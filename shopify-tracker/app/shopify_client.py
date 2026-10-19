"""
Thin wrapper around Shopify's Admin GraphQL API, used to push tracked
products into the CLIENT's own Shopify store (not the source stores being
scraped -- see scraper.py for that side).

Requires a Custom App access token created in the client's Shopify admin
under Settings > Apps and sales channels > Develop apps, with these scopes:
  - read_products, write_products
  - read_inventory, write_inventory

This module is intentionally low-level (one function per API operation).
Higher-level "match this tracked product to the store, then update or
create it" logic lives in shopify_sync.py.
"""
import asyncio
import logging
import re
import random
from dataclasses import dataclass
from typing import Optional

import aiohttp

logger = logging.getLogger("shopify_client")

API_VERSION = "2024-10"
MAX_ATTEMPTS = 3
# THROTTLED errors get their own, more generous retry budget: they're a
# "you WILL succeed if you wait" signal, not a real failure, and bulk
# operations like group product creation are expensive enough (each
# createProduct mutation can cost hundreds of points against Shopify's
# leaky-bucket limiter) to regularly need more than 3 tries when several
# are running concurrently.
THROTTLE_MAX_ATTEMPTS = 6
BASE_BACKOFF_SECONDS = 3
MAX_THROTTLE_BACKOFF_SECONDS = 45
REQUEST_TIMEOUT = 15


class ShopifyAPIError(Exception):
    """Raised when the Admin API returns an error we can't recover from
    (bad credentials, invalid input, etc.) -- as opposed to a transient
    network/rate-limit issue, which is retried automatically instead."""
    pass


@dataclass
class DestVariantMatch:
    product_id: str
    variant_id: str
    inventory_item_id: str


def _graphql_url(shop_domain: str) -> str:
    domain = shop_domain.strip().rstrip("/")
    # Be forgiving of a pasted full URL (https://yourstore.myshopify.com)
    # as well as just the bare domain.
    domain = re.sub(r"^https?://", "", domain, flags=re.IGNORECASE)
    if not domain.endswith(".myshopify.com"):
        # Also forgiving of just "yourstore" instead of the full domain --
        # both are common copy-paste mistakes.
        domain = f"{domain}.myshopify.com"
    return f"https://{domain}/admin/api/{API_VERSION}/graphql.json"


async def get_access_token(
    session: aiohttp.ClientSession, shop_domain: str, client_id: str, client_secret: str,
) -> tuple[str, int]:
    """
    Exchanges an app's Client ID + Client Secret for a usable Admin API
    access token, using Shopify's "client credentials grant" flow -- this
    replaced the old system where a token was just handed to you directly
    in the Shopify admin (that flow was retired for new apps as of Jan 1,
    2026). Only works when the app and the store are in the same Shopify
    organization, which is the normal case when a merchant creates the
    app for their own store.

    Returns (access_token, expires_in_seconds). The token is NOT
    permanent -- see shopify_sync.py for the caching/refresh logic that
    avoids re-requesting one on every single API call.
    """
    domain = shop_domain.strip().rstrip("/")
    domain = re.sub(r"^https?://", "", domain, flags=re.IGNORECASE)
    if not domain.endswith(".myshopify.com"):
        domain = f"{domain}.myshopify.com"

    url = f"https://{domain}/admin/oauth/access_token"
    payload = {
        "client_id": client_id,
        "client_secret": client_secret,
        "grant_type": "client_credentials",
    }

    try:
        async with session.post(
            url, json=payload,
            timeout=aiohttp.ClientTimeout(total=REQUEST_TIMEOUT),
        ) as resp:
            if resp.status == 400:
                body = await resp.text()
                if "shop_not_permitted" in body:
                    raise ShopifyAPIError(
                        "Shopify rejected this token request: the app and the store aren't in the "
                        "same Shopify organization. This usually means the app needs to be installed "
                        "on this specific store from the Dev Dashboard first."
                    )
                raise ShopifyAPIError(f"Shopify rejected the client ID/secret (400): {body}")
            if resp.status == 401:
                raise ShopifyAPIError("Shopify rejected the client ID/secret (401) -- double-check both values.")
            resp.raise_for_status()
            data = await resp.json()
    except (aiohttp.ClientError, asyncio.TimeoutError) as e:
        raise ShopifyAPIError(f"Failed to reach Shopify while requesting an access token: {e}")

    token = data.get("access_token")
    if not token:
        raise ShopifyAPIError(f"Shopify's token response didn't include an access_token: {data}")

    # expires_in is in seconds; Shopify docs note this can be absent for
    # some token types, in which case treat it as a long-lived token and
    # just refresh well before any reasonable assumption of expiry.
    expires_in = int(data.get("expires_in") or 3600)
    return token, expires_in


async def _graphql(
    session: aiohttp.ClientSession,
    shop_domain: str,
    access_token: str,
    query: str,
    variables: Optional[dict] = None,
) -> dict:
    """Runs one GraphQL request against the client's store, with retry on
    rate limiting (Shopify's Admin API uses a leaky-bucket limiter -- a 429
    or THROTTLED response means "slow down", not "this is broken")."""
    url = _graphql_url(shop_domain)
    headers = {
        "X-Shopify-Access-Token": access_token,
        "Content-Type": "application/json",
    }
    payload = {"query": query, "variables": variables or {}}

    attempt = 0
    while True:
        attempt += 1
        try:
            async with session.post(
                url, json=payload, headers=headers,
                timeout=aiohttp.ClientTimeout(total=REQUEST_TIMEOUT),
            ) as resp:
                if resp.status == 401:
                    raise ShopifyAPIError(
                        "Shopify rejected the access token (401) -- check the store domain and token are correct."
                    )
                if resp.status == 429:
                    if attempt >= THROTTLE_MAX_ATTEMPTS:
                        raise ShopifyAPIError(f"Still rate-limited by Shopify after {attempt} attempts (HTTP 429).")
                    retry_after = resp.headers.get("Retry-After")
                    try:
                        wait = float(retry_after) if retry_after else BASE_BACKOFF_SECONDS * (2 ** (attempt - 1))
                    except (TypeError, ValueError):
                        wait = BASE_BACKOFF_SECONDS * (2 ** (attempt - 1))
                    wait = min(MAX_THROTTLE_BACKOFF_SECONDS, max(1.0, wait)) + random.uniform(0, 1)
                    logger.warning(f"Rate limited by Shopify (429), waiting {wait:.1f}s (attempt {attempt}/{THROTTLE_MAX_ATTEMPTS})")
                    await asyncio.sleep(wait)
                    continue

                resp.raise_for_status()
                data = await resp.json()

                errors = data.get("errors")
                if errors:
                    # Throttling shows up as a well-formed 200 response with
                    # a THROTTLED extension code on one of the errors, not
                    # as an HTTP 429 -- this needs its own detection and
                    # retry, checking the actual field Shopify puts it in
                    # (errors[].extensions.code), not just raising outright.
                    is_throttled = any(
                        isinstance(e, dict) and (e.get("extensions") or {}).get("code") == "THROTTLED"
                        for e in errors
                    )
                    if is_throttled:
                        if attempt >= THROTTLE_MAX_ATTEMPTS:
                            raise ShopifyAPIError(f"Still throttled by Shopify after {attempt} attempts: {errors}")
                        # Shopify includes cost.throttleStatus in the response extensions;
                        # use currently available capacity when present, with capped exponential
                        # backoff + jitter as a safe fallback.
                        extensions = data.get("extensions") or {}
                        cost = extensions.get("cost") or {}
                        throttle = cost.get("throttleStatus") or {}
                        available = throttle.get("currentlyAvailable")
                        restore_rate = throttle.get("restoreRate") or 0
                        requested = (cost.get("requestedQueryCost") or 0)
                        if available is not None and restore_rate and requested > available:
                            wait = (requested - available) / restore_rate
                        else:
                            wait = BASE_BACKOFF_SECONDS * (2 ** (attempt - 1))
                        wait = min(MAX_THROTTLE_BACKOFF_SECONDS, max(1.0, wait)) + random.uniform(0, 1)
                        logger.warning(f"Throttled by Shopify (GraphQL THROTTLED), waiting {wait:.1f}s (attempt {attempt}/{THROTTLE_MAX_ATTEMPTS})")
                        await asyncio.sleep(wait)
                        continue
                    raise ShopifyAPIError(f"Shopify GraphQL error: {errors}")

                return data

        except (aiohttp.ClientError, asyncio.TimeoutError) as e:
            if attempt < MAX_ATTEMPTS:
                wait = BASE_BACKOFF_SECONDS * (2 ** (attempt - 1))
                logger.warning(f"Attempt {attempt}/{MAX_ATTEMPTS} failed: {e}. Retrying in {wait}s")
                await asyncio.sleep(wait)
            else:
                raise ShopifyAPIError(f"Failed to reach Shopify after {MAX_ATTEMPTS} attempts: {e}")

    raise ShopifyAPIError("Failed to reach Shopify (rate limited on every attempt)")


async def find_variant_by_sku(
    session: aiohttp.ClientSession, shop_domain: str, access_token: str, sku: str
) -> Optional[DestVariantMatch]:
    """Looks up whether a product with this SKU already exists in the
    client's store. Returns None if there's no match (i.e. this is a
    brand-new product from the client's perspective)."""
    if not sku:
        return None

    query = """
    query FindBySku($query: String!) {
      productVariants(first: 1, query: $query) {
        edges {
          node {
            id
            product { id }
            inventoryItem { id }
          }
        }
      }
    }
    """
    data = await _graphql(session, shop_domain, access_token, query, {"query": f"sku:{sku}"})
    edges = (((data.get("data") or {}).get("productVariants") or {}).get("edges")) or []
    if not edges:
        return None

    node = edges[0]["node"]
    return DestVariantMatch(
        product_id=node["product"]["id"],
        variant_id=node["id"],
        inventory_item_id=node["inventoryItem"]["id"],
    )


async def update_variant_price(
    session: aiohttp.ClientSession, shop_domain: str, access_token: str,
    product_id: str, variant_id: str, price: float,
) -> None:
    mutation = """
    mutation UpdatePrice($productId: ID!, $variants: [ProductVariantsBulkInput!]!) {
      productVariantsBulkUpdate(productId: $productId, variants: $variants) {
        userErrors { field message }
      }
    }
    """
    variables = {
        "productId": product_id,
        "variants": [{"id": variant_id, "price": f"{price:.2f}"}],
    }
    data = await _graphql(session, shop_domain, access_token, mutation, variables)
    errors = (((data.get("data") or {}).get("productVariantsBulkUpdate") or {}).get("userErrors")) or []
    if errors:
        raise ShopifyAPIError(f"Price update failed: {errors}")


async def update_inventory_quantity(
    session: aiohttp.ClientSession, shop_domain: str, access_token: str,
    inventory_item_id: str, location_id: str, quantity: int,
) -> None:
    """Sets absolute on-hand quantity at a given location. location_id has
    to be looked up once per store (see get_primary_location_id) -- most
    stores with a single warehouse only need the one."""
    mutation = """
    mutation SetQty($input: InventorySetQuantitiesInput!) {
      inventorySetQuantities(input: $input) {
        userErrors { field message }
      }
    }
    """
    variables = {
        "input": {
            "reason": "correction",
            "name": "available",
            "ignoreCompareQuantity": True,  # we're setting an absolute value, not comparing against
                                             # a previously-read quantity, so opt out of Shopify's
                                             # optimistic-concurrency check rather than supplying a
                                             # (redundant, and often stale) compareQuantity per entry.
            "quantities": [{
                "inventoryItemId": inventory_item_id,
                "locationId": location_id,
                "quantity": quantity,
            }],
        }
    }
    data = await _graphql(session, shop_domain, access_token, mutation, variables)
    errors = (((data.get("data") or {}).get("inventorySetQuantities") or {}).get("userErrors")) or []
    if errors:
        raise ShopifyAPIError(f"Inventory update failed: {errors}")


async def get_primary_location_id(
    session: aiohttp.ClientSession, shop_domain: str, access_token: str
) -> str:
    """
    Finds the location inventory updates should actually be pushed to --
    NOT just "whichever location the API happens to list first". Shopify's
    `locations(first: N)` has no defined ordering guarantee, and a store's
    "default" location isn't necessarily the one actually wired up to
    fulfill online orders (a merchant can have a default location with
    online fulfillment switched OFF, e.g. while working around Shopify's
    "can't edit this on your current default location" restriction by
    temporarily adding a second placeholder location).

    Pushing quantity to a location that isn't enabled for online
    fulfillment is a silent failure mode: inventorySetQuantities succeeds,
    the number is genuinely correct in Shopify, and the product still
    shows "sold out" on the storefront regardless -- there's no error
    anywhere to point at. So this filters explicitly for
    fulfillsOnlineOrders=true rather than trusting result order, and fails
    loudly (rather than guessing) if that comes back ambiguous.
    """
    query = """
    query {
      locations(first: 10) {
        edges { node { id name fulfillsOnlineOrders } }
      }
    }
    """
    data = await _graphql(session, shop_domain, access_token, query)
    edges = (((data.get("data") or {}).get("locations") or {}).get("edges")) or []
    if not edges:
        raise ShopifyAPIError("Client's store has no fulfillment location set up.")

    fulfilling = [e["node"] for e in edges if e["node"].get("fulfillsOnlineOrders")]

    if not fulfilling:
        names = ", ".join(e["node"]["name"] for e in edges)
        raise ShopifyAPIError(
            "No location on this store is enabled to fulfill online orders "
            f"(checked: {names}). Go to Settings -> Locations in Shopify admin, "
            "open the location that should be the real warehouse, and under "
            "'Fulfillment' turn on 'Use inventory at this location to fulfill "
            "online orders' -- inventory sync will report every product as sold "
            "out on the storefront until this is on for exactly one location."
        )
    if len(fulfilling) > 1:
        names = ", ".join(n["name"] for n in fulfilling)
        raise ShopifyAPIError(
            f"More than one location is enabled to fulfill online orders ({names}) "
            "-- can't tell which one to sync inventory to. Go to Settings -> "
            "Locations in Shopify admin and make sure only the real warehouse "
            "location has 'Use inventory at this location to fulfill online "
            "orders' turned on."
        )
    return fulfilling[0]["id"]


async def create_product(
    session: aiohttp.ClientSession, shop_domain: str, access_token: str,
    title: str, body_html: str, vendor: str, product_type: str, tags: str,
    sku: str, price: float, image_urls: list[str],
    option1_name: str = None, option1_value: str = None,
    option2_name: str = None, option2_value: str = None,
    option3_name: str = None, option3_value: str = None,
) -> DestVariantMatch:
    """Creates a brand-new product (used by the auto-create add-on when a
    source product has no existing match in the client's store).

    Uses productSet rather than productCreate: as of the 2024-10+ Admin API,
    productCreate's ProductInput no longer accepts inline `options` or
    `variants` at all (Shopify split that out) -- it only creates a bare
    product with a default variant, and options/variants need separate
    follow-up mutations. productSet does support setting everything (title,
    options, and variants together) in one call, and is what Shopify
    explicitly recommends for exactly this use case: syncing product data
    in from an external source. See:
    https://shopify.dev/docs/apps/build/graphql/migrate/new-product-model/sync-data
    """
    product_options = []
    variant_option_values = []
    for name, value in (
        (option1_name, option1_value),
        (option2_name, option2_value),
        (option3_name, option3_value),
    ):
        if name and value:
            product_options.append({"name": name, "values": [{"name": value}]})
            variant_option_values.append({"optionName": name, "name": value})

    mutation = """
    mutation CreateProduct($input: ProductSetInput!, $synchronous: Boolean!) {
      productSet(input: $input, synchronous: $synchronous) {
        product {
          id
          variants(first: 1) { nodes { id inventoryItem { id } } }
        }
        userErrors { field message }
      }
    }
    """
    variant_input = {
        "sku": sku or "",
        "price": f"{price:.2f}",
    }
    if variant_option_values:
        variant_input["optionValues"] = variant_option_values

    product_input = {
        "title": title,
        "descriptionHtml": body_html or "",
        "vendor": vendor or "",
        "productType": product_type or "",
        "tags": [t.strip() for t in (tags or "").split(",") if t.strip()],
        "variants": [variant_input],
    }
    if product_options:
        product_input["productOptions"] = product_options
    if image_urls:
        product_input["files"] = [
            {"originalSource": url, "contentType": "IMAGE"} for url in image_urls
        ]

    variables = {"input": product_input, "synchronous": True}
    data = await _graphql(session, shop_domain, access_token, mutation, variables)
    result = (data.get("data") or {}).get("productSet") or {}
    errors = result.get("userErrors") or []
    if errors:
        raise ShopifyAPIError(f"Product creation failed: {errors}")

    product = result.get("product")
    if not product:
        raise ShopifyAPIError("Product creation returned no product (unexpected empty response).")

    variant_nodes = ((product.get("variants") or {}).get("nodes")) or []
    if not variant_nodes:
        raise ShopifyAPIError("Product was created but came back with no variant -- check manually.")
    variant_node = variant_nodes[0]

    return DestVariantMatch(
        product_id=product["id"],
        variant_id=variant_node["id"],
        inventory_item_id=variant_node["inventoryItem"]["id"],
    )


@dataclass
class VariantToCreate:
    """One size/colour variant to create as part of a grouped product.
    sku is what create_product_with_variants uses afterward to match each
    returned Shopify variant back to the source Product row it came from."""
    sku: str
    price: float
    quantity: int
    option1_name: str = None
    option1_value: str = None
    option2_name: str = None
    option2_value: str = None
    option3_name: str = None
    option3_value: str = None


async def create_product_with_variants(
    session: aiohttp.ClientSession, shop_domain: str, access_token: str,
    title: str, body_html: str, vendor: str, product_type: str, tags: str,
    image_urls: list[str], location_id: str, variants: list["VariantToCreate"],
) -> dict[str, "DestVariantMatch"]:
    """
    Creates ONE Shopify product with ALL of its size/colour variants
    together, instead of a separate product per variant. This is what
    keeps e.g. a shirt's XS/S/M/L/XL sizes as one listing with a size
    selector, rather than five separate single-size product pages.

    Returns a dict of {sku: DestVariantMatch}, so the caller (shopify_sync)
    can match each created variant back to the source Product row it came
    from by SKU.
    """
    if not variants:
        raise ShopifyAPIError("create_product_with_variants called with no variants")

    # Collect every distinct (option name -> ordered set of values) across
    # all variants in the group, preserving first-seen order. Shopify needs
    # every value declared up front on productOptions before the variants
    # list can reference them.
    option_names_in_order: list[str] = []
    option_values: dict[str, list[str]] = {}
    for v in variants:
        for name, value in (
            (v.option1_name, v.option1_value),
            (v.option2_name, v.option2_value),
            (v.option3_name, v.option3_value),
        ):
            if not name or not value:
                continue
            if name not in option_values:
                option_values[name] = []
                option_names_in_order.append(name)
            if value not in option_values[name]:
                option_values[name].append(value)

    product_options = [
        {"name": name, "values": [{"name": v} for v in option_values[name]]}
        for name in option_names_in_order
    ]

    variant_inputs = []
    for v in variants:
        option_values_for_variant = []
        for name, value in (
            (v.option1_name, v.option1_value),
            (v.option2_name, v.option2_value),
            (v.option3_name, v.option3_value),
        ):
            if name and value:
                option_values_for_variant.append({"optionName": name, "name": value})

        variant_input = {
            "sku": v.sku or "",
            "price": f"{v.price:.2f}",
        }
        if option_values_for_variant:
            variant_input["optionValues"] = option_values_for_variant
        if location_id:
            # Setting inventory as part of creation (rather than a separate
            # inventorySetQuantities call afterward) sidesteps the
            # ignoreCompareQuantity requirement entirely -- that's only
            # needed when adjusting quantities on an EXISTING inventory
            # item, not when setting the starting quantity at creation time.
            variant_input["inventoryQuantities"] = [{
                "locationId": location_id,
                "name": "available",
                "quantity": v.quantity,
            }]
        variant_inputs.append(variant_input)

    mutation = """
    mutation CreateProduct($input: ProductSetInput!, $synchronous: Boolean!) {
      productSet(input: $input, synchronous: $synchronous) {
        product {
          id
          variants(first: 250) {
            nodes { id sku inventoryItem { id } }
          }
        }
        userErrors { field message }
      }
    }
    """
    product_input = {
        "title": title,
        "descriptionHtml": body_html or "",
        "vendor": vendor or "",
        "productType": product_type or "",
        "tags": [t.strip() for t in (tags or "").split(",") if t.strip()],
        "variants": variant_inputs,
    }
    if product_options:
        product_input["productOptions"] = product_options
    if image_urls:
        product_input["files"] = [
            {"originalSource": url, "contentType": "IMAGE"} for url in image_urls
        ]

    variables = {"input": product_input, "synchronous": True}
    data = await _graphql(session, shop_domain, access_token, mutation, variables)
    result = (data.get("data") or {}).get("productSet") or {}
    errors = result.get("userErrors") or []
    if errors:
        raise ShopifyAPIError(f"Grouped product creation failed: {errors}")

    product = result.get("product")
    if not product:
        raise ShopifyAPIError("Grouped product creation returned no product (unexpected empty response).")

    returned_nodes = ((product.get("variants") or {}).get("nodes")) or []
    matches_by_sku: dict[str, DestVariantMatch] = {}
    for node in returned_nodes:
        sku = node.get("sku")
        if not sku:
            continue
        matches_by_sku[sku] = DestVariantMatch(
            product_id=product["id"],
            variant_id=node["id"],
            inventory_item_id=node["inventoryItem"]["id"],
        )
    return matches_by_sku

async def get_online_store_publication_id(
    session: aiohttp.ClientSession, shop_domain: str, access_token: str,
) -> str:
    """
    Looks up the publication ID for the "Online Store" sales channel on
    this store. Needed because newly created products (via productSet)
    are NOT automatically visible on the storefront -- they exist in the
    admin as Active, but nothing shows on the actual site until they're
    explicitly published to a channel. This is what was silently missing
    before: products looked fine in the admin but were invisible to
    customers.

    Requires the app to have the read_publications scope (write_publications
    is needed for the actual publish call in publish_to_online_store). Most
    stores have exactly one "Online Store" publication by default.
    """
    query = """
    query GetPublications {
      publications(first: 20) {
        nodes { id name }
      }
    }
    """
    data = await _graphql(session, shop_domain, access_token, query)
    nodes = ((data.get("data") or {}).get("publications") or {}).get("nodes") or []
    for node in nodes:
        if node.get("name") == "Online Store":
            return node["id"]
    raise ShopifyAPIError(
        "Couldn't find an \"Online Store\" publication on this store -- "
        "check that the Online Store sales channel is set up, and that the "
        "app has the read_publications scope."
    )


async def publish_to_online_store(
    session: aiohttp.ClientSession, shop_domain: str, access_token: str,
    product_id: str, publication_id: str,
) -> None:
    """
    Publishes a product to the Online Store channel, making it actually
    visible on the storefront. A product being "Active" in the admin is
    NOT the same as being published to a channel -- both are required for
    customers to see it. Safe to call even if the product is already
    published (Shopify's docs confirm this is a no-op, not an error, in
    that case), so this can run on every sync without needing to track
    whether a given product has already been published.

    Requires the app to have the write_publications scope.
    """
    mutation = """
    mutation PublishProduct($id: ID!, $input: [PublicationInput!]!) {
      publishablePublish(id: $id, input: $input) {
        userErrors { field message }
      }
    }
    """
    variables = {"id": product_id, "input": [{"publicationId": publication_id}]}
    data = await _graphql(session, shop_domain, access_token, mutation, variables)
    errors = ((data.get("data") or {}).get("publishablePublish") or {}).get("userErrors") or []
    if errors:
        raise ShopifyAPIError(f"Publishing to Online Store failed: {errors}")


async def unpublish_from_online_store(
    session: aiohttp.ClientSession, shop_domain: str, access_token: str,
    product_id: str, publication_id: str,
) -> None:
    """
    Removes a product from the Online Store channel -- it still exists in
    the admin (Active, all its data intact), it just won't show up
    anywhere on the actual storefront. Used to hide a product once every
    one of its variants is out of stock (see
    shopify_sync._sync_one_group's group-level publish/unpublish decision),
    and undone automatically the next sync after any variant is back in
    stock -- there's no separate "restock" code path, the same decision
    just gets re-evaluated fresh every run. Safe to call even if the
    product is already unpublished (a no-op, not an error), same as
    publish_to_online_store.

    Requires the app to have the write_publications scope (same as publish).
    """
    mutation = """
    mutation UnpublishProduct($id: ID!, $input: [PublicationInput!]!) {
      publishableUnpublish(id: $id, input: $input) {
        userErrors { field message }
      }
    }
    """
    variables = {"id": product_id, "input": [{"publicationId": publication_id}]}
    data = await _graphql(session, shop_domain, access_token, mutation, variables)
    errors = ((data.get("data") or {}).get("publishableUnpublish") or {}).get("userErrors") or []
    if errors:
        raise ShopifyAPIError(f"Unpublishing from Online Store failed: {errors}")


async def get_or_create_collection(
    session: aiohttp.ClientSession, shop_domain: str, access_token: str, title: str,
) -> str:
    """
    Finds an existing custom collection with this exact title, or creates
    one if none exists. Used to give each tracked source store (e.g.
    "Charizma") its own destination collection, so the storefront's
    "Shop by Brand" navigation -- which is just a collection page per
    brand -- actually has products in it. Without this, products were
    created and published fine, but sat with zero collection membership,
    so every brand page showed "No products found" despite the products
    genuinely existing and being live.

    Result isn't cached here -- callers should cache the returned id (see
    Store.dest_collection_id) so this lookup only runs once per store,
    not on every sync.
    """
    search_query = """
    query FindCollection($query: String!) {
      collections(first: 5, query: $query) {
        nodes { id title }
      }
    }
    """
    # Quote the value so Shopify's search syntax treats it as one literal
    # title to match rather than splitting on spaces as separate terms
    # (e.g. an unquoted "Maryum N Maria" would search for three words).
    escaped_title = title.replace('"', '\\"')
    data = await _graphql(session, shop_domain, access_token, search_query, {"query": f'title:"{escaped_title}"'})
    nodes = ((data.get("data") or {}).get("collections") or {}).get("nodes") or []
    for node in nodes:
        if node.get("title") == title:
            return node["id"]

    create_mutation = """
    mutation CreateCollection($input: CollectionInput!) {
      collectionCreate(input: $input) {
        collection { id }
        userErrors { field message }
      }
    }
    """
    data = await _graphql(session, shop_domain, access_token, create_mutation, {"input": {"title": title}})
    result = (data.get("data") or {}).get("collectionCreate") or {}
    errors = result.get("userErrors") or []
    if errors:
        raise ShopifyAPIError(f"Failed to create collection '{title}': {errors}")
    collection = result.get("collection")
    if not collection:
        raise ShopifyAPIError(f"collectionCreate returned no collection for '{title}'")
    return collection["id"]


async def add_product_to_collection(
    session: aiohttp.ClientSession, shop_domain: str, access_token: str,
    collection_id: str, product_id: str,
) -> None:
    """
    Adds a product to a collection. Safe to call even if it's already a
    member -- Shopify just leaves membership as-is rather than erroring
    or duplicating, so this can run on every sync (matched or created)
    rather than needing to track membership separately. That matters
    here specifically: it means the very next sync after this fix ships
    will backfill collection membership for every product already created
    by earlier syncs, not just new ones going forward.
    """
    mutation = """
    mutation AddToCollection($id: ID!, $productIds: [ID!]!) {
      collectionAddProducts(id: $id, productIds: $productIds) {
        userErrors { field message }
      }
    }
    """
    variables = {"id": collection_id, "productIds": [product_id]}
    data = await _graphql(session, shop_domain, access_token, mutation, variables)
    errors = ((data.get("data") or {}).get("collectionAddProducts") or {}).get("userErrors") or []
    if errors:
        raise ShopifyAPIError(f"Adding product to collection failed: {errors}")