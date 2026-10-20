"""
Higher-level orchestration for pushing tracked products into the CLIENT's
own Shopify store. Low-level Admin API calls (create_product,
find_variant_by_sku, price/inventory updates, etc.) live in
shopify_client.py -- this module decides *when* to call them: match
existing products by SKU, group source variants into one destination
product per handle (so sizes/colours end up as variants on ONE listing
instead of separate products), and update vs. create accordingly.

Entry points called from main.py:
- sync_store(db, store_id)              -- used on the scheduled scrape+sync cycle
- sync_store_standalone(store_id, limit) -- used by the manual "sync now" endpoint,
                                             opens its own DB session since it runs
                                             as a bare background task with no
                                             request-scoped session available.
"""
import logging
from datetime import datetime, timezone, timedelta

import aiohttp
from sqlalchemy.orm import Session

from .database import SessionLocal
from .models import Store, Product, AppSettings
from .export import round_to_99, apply_margin, resolve_quantity, _fallback_handle
from . import shopify_client as sc

logger = logging.getLogger("shopify_sync")

# Refresh the cached access token a bit before it actually expires, so a
# sync run doesn't start using a token that dies partway through hundreds
# of sequential API calls.
TOKEN_REFRESH_SAFETY_MARGIN = timedelta(minutes=5)

# In-memory live progress, keyed by store_id -- read by GET
# /stores/{id}/sync/progress so the dashboard can show what's happening
# *during* a sync instead of just waiting blind until it's done. This is
# intentionally NOT persisted to the database: a running sync is a live,
# local, in-process thing, and if the server restarts mid-sync there's no
# "resuming" it anyway -- the next sync just starts fresh. Fine for a
# single-process deployment; would need to move to something shared (e.g.
# the DB, or Redis) if this ever runs across multiple worker processes.
_progress: dict[int, dict] = {}


def get_progress(store_id: int) -> dict:
    """Used by the /sync/progress endpoint. Returns {"status": "idle"} for
    a store that's never synced (or the server just restarted)."""
    return _progress.get(store_id, {"status": "idle"})


def clear_progress(store_id: int) -> None:
    """Drops any in-memory progress entry for this store. Must be called
    when a store is deleted -- otherwise, since SQLite can reuse a
    deleted store's numeric id for a brand-new one, a fresh store can
    silently inherit a stale "running" status left behind by whatever
    the old store's id was last doing (e.g. a sync that never got to
    call _finish_progress because the process restarted mid-run). Safe
    to call even if there's no entry for this id."""
    _progress.pop(store_id, None)


def _init_progress(store_id: int, total_groups: int) -> None:
    _progress[store_id] = {
        "status": "running",
        "total_groups": total_groups,
        "completed_groups": 0,
        "current_handle": None,
        "counts": {"matched": 0, "created": 0, "not_found": 0, "errors": 0},
        "started_at": datetime.now(timezone.utc).isoformat(),
    }


def _advance_progress(store_id: int, handle: str, group_counts: dict) -> None:
    p = _progress.get(store_id)
    if not p:
        return
    p["current_handle"] = handle
    p["completed_groups"] += 1
    for k, v in group_counts.items():
        p["counts"][k] = p["counts"].get(k, 0) + v


def _finish_progress(store_id: int, status: str) -> None:
    p = _progress.get(store_id)
    if p:
        p["status"] = status
        p["finished_at"] = datetime.now(timezone.utc).isoformat()


async def _ensure_access_token(session: aiohttp.ClientSession, store: Store, db: Session) -> str:
    now = datetime.now(timezone.utc)

    # SQLite/SQLAlchemy drops timezone info on the way back out of the
    # database -- shopify_token_expires_at is stored as UTC (see the
    # assignment below) but comes back as a naive datetime, which can't be
    # compared directly against a timezone-aware `now`. Treat a naive value
    # as UTC rather than comparing it as-is.
    expires_at = store.shopify_token_expires_at
    if expires_at is not None and expires_at.tzinfo is None:
        expires_at = expires_at.replace(tzinfo=timezone.utc)

    if store.shopify_access_token and expires_at and expires_at - TOKEN_REFRESH_SAFETY_MARGIN > now:
        return store.shopify_access_token

    token, expires_in = await sc.get_access_token(
        session, store.shopify_domain, store.shopify_client_id, store.shopify_client_secret
    )
    store.shopify_access_token = token
    store.shopify_token_expires_at = now + timedelta(seconds=expires_in)
    db.commit()
    return token


def _is_stale_reference_error(e: Exception) -> bool:
    """True for a Shopify error meaning the dest_* id we have on file no
    longer refers to anything real in the destination store -- almost
    always because the product was deleted there directly (manually in
    the admin, a bulk cleanup, etc), not a transient API hiccup. Matches
    both error shapes actually seen in production: a price/inventory
    update's "Product does not exist" on the productId field, and a
    publish/unpublish or collection call's "Resource does not exist" on
    the id field -- Shopify doesn't use one consistent error here, so
    this matches on the shared wording rather than one exact string."""
    return "does not exist" in str(e).lower()


def _clear_stale_dest_ids(products: list[Product]) -> None:
    """Wipes a product's cached destination ids so the NEXT sync falls
    through to the normal SKU-lookup / re-create path instead of trying
    the exact same now-invalid update again. Without this, a product
    deleted on the Shopify side stays permanently stuck: every sync keeps
    reusing the stale dest_product_id/dest_variant_id forever, fails the
    same way every single time, and never recovers on its own -- which is
    exactly the "Product does not exist" errors repeating night after
    night in the logs for the same SKUs."""
    for p in products:
        p.dest_product_id = None
        p.dest_variant_id = None
        p.dest_inventory_item_id = None
        p.sync_status = "not_found"


def _dest_price(product: Product, fx_rate: float, margin_percent: float = 0.0) -> float:
    """Same charm-pricing + margin convention as the CSV export
    (export.round_to_99 / export.apply_margin) -- prices pushed via the
    API should match what a CSV import would have produced, not raw
    scraped numbers."""
    return round_to_99(apply_margin(product.price * fx_rate, margin_percent))


async def _update_matched_variant(
    session: aiohttp.ClientSession, domain: str, token: str, location_id: str,
    product: Product, fx_rate: float, margin_percent: float = 0.0,
) -> None:
    await sc.update_variant_price(
        session, domain, token,
        product_id=product.dest_product_id,
        variant_id=product.dest_variant_id,
        price=_dest_price(product, fx_rate, margin_percent),
    )
    qty = resolve_quantity(product)
    await sc.update_inventory_quantity(
        session, domain, token,
        inventory_item_id=product.dest_inventory_item_id,
        location_id=location_id,
        quantity=qty,
    )
    # Publishing/unpublishing is decided and applied once per GROUP (one
    # call per Shopify product, not per variant) in _sync_one_group -- see
    # its "any_in_stock" logic. Shopify's publish state lives on the
    # product, not the variant, so doing it here per-variant would just
    # mean the last variant processed wins, which isn't the right rule
    # when some sizes are in stock and others aren't.


async def _match_or_update_one(
    session: aiohttp.ClientSession, domain: str, token: str, location_id: str,
    product: Product, fx_rate: float, db: Session, margin_percent: float = 0.0,
) -> str:
    """Handles a single already-matched variant (dest_* ids already saved
    from a previous sync): just push the current price/stock. Returns the
    sync_status to record. Group-level matching/creation for NOT-yet-matched
    variants, and the group-level publish/unpublish decision, are both
    handled in _sync_one_group, since both have to happen once per product
    (all sizes together), not once per variant."""
    try:
        await _update_matched_variant(session, domain, token, location_id, product, fx_rate, margin_percent)
        product.last_synced_at = datetime.now(timezone.utc)
        product.sync_status = "matched"
        return "matched"
    except sc.ShopifyAPIError as e:
        logger.error(f"Update failed for product {product.id} (SKU {product.sku}): {e}")
        if _is_stale_reference_error(e):
            # The destination product is gone -- don't leave the stale id
            # in place for every future sync to trip over again. This
            # sync still counts as an error (the update genuinely failed
            # this run), but the NEXT sync will treat it as unmatched and
            # re-search/re-create it properly instead of repeating this.
            _clear_stale_dest_ids([product])
        else:
            product.sync_status = "error"
        return "error"


async def _sync_one_group(
    session: aiohttp.ClientSession, domain: str, token: str, location_id: str,
    publication_id: str, collection_id: str, store: Store, variants: list[Product], handle: str,
    fx_rate: float, db: Session,
) -> dict:
    """Handles one product handle's worth of variants end to end: push
    updates for anything already matched, SKU-lookup anything that isn't,
    then create (grouped, published) whatever's still unmatched. Returns
    the counts contributed by this one group, e.g. {"matched": 3,
    "created": 2, "not_found": 0, "errors": 0} -- used both to build the
    overall summary and to report live progress after each group."""
    counts = {"matched": 0, "created": 0, "not_found": 0, "errors": 0}
    margin_percent = store.margin_percent or 0.0

    # Hide this product entirely once every one of its variants is out of
    # stock, and un-hide the moment any variant is back -- computed from
    # every variant we know about (matched or not), and re-evaluated fresh
    # on every sync, so restocking undoes the hide automatically with no
    # separate "un-hide" code path needed.
    any_in_stock = any(p.available for p in variants)

    already_matched = [p for p in variants if p.dest_product_id and p.dest_variant_id and p.dest_inventory_item_id]
    unmatched = [p for p in variants if p not in already_matched]

    # Push current price/stock for anything already matched from a
    # previous sync -- cheap, no search needed.
    for p in already_matched:
        status = await _match_or_update_one(session, domain, token, location_id, p, fx_rate, db, margin_percent)
        counts[status if status in counts else "errors"] += 1
    if already_matched:
        db.commit()

    # For anything not yet matched, check by SKU first -- it may already
    # exist in the store from a manual/CSV import, and we don't want to
    # create a duplicate.
    still_unmatched = []
    for p in unmatched:
        try:
            match = await sc.find_variant_by_sku(session, domain, token, p.sku)
        except sc.ShopifyAPIError as e:
            logger.error(f"SKU lookup failed for product {p.id} (SKU {p.sku}): {e}")
            p.sync_status = "error"
            counts["errors"] += 1
            continue
        if match:
            p.dest_product_id = match.product_id
            p.dest_variant_id = match.variant_id
            p.dest_inventory_item_id = match.inventory_item_id
            status = await _match_or_update_one(session, domain, token, location_id, p, fx_rate, db, margin_percent)
            counts[status if status in counts else "errors"] += 1
        else:
            still_unmatched.append(p)
    db.commit()

    if still_unmatched:
        if not store.auto_create_enabled:
            for p in still_unmatched:
                p.sync_status = "not_found"
            counts["not_found"] += len(still_unmatched)
            db.commit()
        else:
            await _create_group(session, domain, token, location_id, still_unmatched, fx_rate, margin_percent, handle, counts, db)

    # One publish/unpublish call per Shopify product this group touched --
    # Shopify's publish state lives on the PRODUCT, not the variant, so
    # this has to run once per group after all matching/creation above,
    # not per variant (a per-variant publish call would just mean
    # whichever variant was processed last decides visibility, which is
    # wrong once some sizes are in stock and others aren't).
    dest_product_ids = {p.dest_product_id for p in variants if p.dest_product_id}
    for dest_id in dest_product_ids:
        # Every variant currently pointing at this dest_id -- used below to
        # clear ALL of them together if dest_id turns out to be stale,
        # rather than just whichever single one happened to fail first.
        variants_for_this_dest = [p for p in variants if p.dest_product_id == dest_id]

        publish_failed_stale = False
        try:
            if any_in_stock:
                await sc.publish_to_online_store(session, domain, token, dest_id, publication_id)
            else:
                await sc.unpublish_from_online_store(session, domain, token, dest_id, publication_id)
        except sc.ShopifyAPIError as e:
            logger.error(f"Publish/unpublish failed for product {dest_id} (handle {handle}): {e}")
            counts["errors"] += 1
            if _is_stale_reference_error(e):
                publish_failed_stale = True
                _clear_stale_dest_ids(variants_for_this_dest)

        # Independent of publish state -- a product should be categorized
        # under its brand's collection whether or not it's currently in
        # stock, so restocking doesn't need a separate "add to collection"
        # path once it's already a member. Runs unconditionally on every
        # touched product (matched OR created), which is what backfills
        # collection membership for anything created by an earlier sync
        # before this fix existed -- not just products created from here on.
        # Skipped if the publish/unpublish call above already found this
        # dest_id stale and cleared it -- no point hitting the same
        # deleted product with a second doomed call in the same run.
        if not publish_failed_stale:
            try:
                await sc.add_product_to_collection(session, domain, token, collection_id, dest_id)
            except sc.ShopifyAPIError as e:
                logger.error(f"Adding product {dest_id} to collection failed (handle {handle}): {e}")
                counts["errors"] += 1
                if _is_stale_reference_error(e):
                    _clear_stale_dest_ids(variants_for_this_dest)

    return counts


async def _create_group(
    session: aiohttp.ClientSession, domain: str, token: str, location_id: str,
    still_unmatched: list[Product], fx_rate: float, margin_percent: float,
    handle: str, counts: dict, db: Session,
) -> None:
    """Nothing existed for any of these variants -- create ONE new Shopify
    product with all of them as variants together. Split out of
    _sync_one_group so that function's group-level publish/unpublish step
    reads cleanly as one linear sequence rather than being nested inside
    this try/except."""
    first = still_unmatched[0]
    image_urls = []
    if first.all_image_urls:
        image_urls = [u.strip() for u in first.all_image_urls.split(",") if u.strip()]
    variant_specs = [
        sc.VariantToCreate(
            sku=p.sku or "",
            price=_dest_price(p, fx_rate, margin_percent),
            quantity=resolve_quantity(p),
            option1_name=p.option1_name, option1_value=p.option1_value,
            option2_name=p.option2_name, option2_value=p.option2_value,
            option3_name=p.option3_name, option3_value=p.option3_value,
        )
        for p in still_unmatched
    ]
    try:
        created = await sc.create_product_with_variants(
            session, domain, token,
            title=first.title, body_html=first.body_html or "",
            vendor=first.vendor or "", product_type=first.product_type or "",
            tags=first.tags or "", image_urls=image_urls,
            location_id=location_id, variants=variant_specs,
        )
        for p in still_unmatched:
            match = created.get(p.sku)
            if not match:
                p.sync_status = "error"
                counts["errors"] += 1
                continue
            p.dest_product_id = match.product_id
            p.dest_variant_id = match.variant_id
            p.dest_inventory_item_id = match.inventory_item_id
            p.last_synced_at = datetime.now(timezone.utc)
            p.sync_status = "created"
            counts["created"] += 1
    except sc.ShopifyAPIError as e:
        logger.error(f"Group creation failed for handle {handle}: {e}")
        for p in still_unmatched:
            p.sync_status = "error"
        counts["errors"] += len(still_unmatched)
    db.commit()


async def sync_store(
    db: Session,
    store_id: int,
    limit: int | None = None,
    collection_handle: str | None = None,
) -> dict:
    """
    Pushes tracked products for this store into the client's own Shopify store.
    If collection_handle is supplied, only products in that source collection are synced;
    otherwise the whole store is synced:
    - Product rows that already have dest_product_id/dest_variant_id from a
      previous sync just get their price/stock pushed (cheap, no search).
    - Product rows with no dest ids yet get looked up by SKU first, in case
      they already exist in the store from a manual/CSV import -- only if
      that comes back empty AND auto_create_enabled is on do we create a
      brand-new product.
    - Variants are grouped by product handle so e.g. 5 sizes of one design
      become 5 variants on ONE Shopify product, not 5 separate products.
    - Newly created products are explicitly published to the Online Store
      channel -- productSet does not do this automatically, and skipping it
      is what left last night's batch invisible on the storefront despite
      showing Active in the admin.

    Updates _progress[store_id] after every group so GET
    /stores/{id}/sync/progress can show live status while this runs.
    """
    summary = {"matched": 0, "created": 0, "not_found": 0, "errors": 0, "skipped_not_configured": False}

    store = db.query(Store).filter(Store.id == store_id).first()
    if not store:
        return summary

    if not (store.shopify_domain and store.shopify_client_id and store.shopify_client_secret):
        summary["skipped_not_configured"] = True
        store.last_sync_status = "not_configured"
        db.commit()
        return summary

    product_query = db.query(Product).filter(Product.store_id == store_id)
    if collection_handle is not None:
        product_query = product_query.filter(Product.collection_handle == collection_handle)
    products = product_query.all()

    # Group by product handle, preserving first-seen order, same convention
    # as export.build_shopify_csv -- keeps all variants of one design together.
    groups: dict[str, list[Product]] = {}
    for p in products:
        groups.setdefault(_fallback_handle(p), []).append(p)

    if limit is not None:
        # Trim to the first N *groups* (not raw rows) so a "safe first test"
        # sync still creates whole products with all their variants, rather
        # than half-creating one.
        groups = dict(list(groups.items())[:limit])

    domain = store.shopify_domain

    # Refuse to sync with an unresolved FX rate rather than silently
    # falling back to 1.0 -- "or 1.0" here used to mean a store whose
    # currency was never successfully detected (or the target currency
    # settings changed since) would get its raw source-currency prices
    # pushed as if they were already in the target currency, e.g. a real
    # 15,000 PKR item landing on the live Shopify store as "$15,000".
    # fx_rate_to_target IS correctly 1.0 (not None) for a store that
    # genuinely already sells in the target currency -- see
    # scraper.fetch_fx_rate's own from_currency == to_currency check --
    # so this only blocks the actual unresolved case, not a same-currency
    # store working as intended.
    settings = db.query(AppSettings).filter(AppSettings.id == 1).first()
    target_currency = settings.target_currency if settings else "USD"
    if store.currency and store.currency != target_currency and not store.fx_rate_to_target:
        msg = (
            f"Exchange rate for {store.currency} -> {target_currency} hasn't resolved yet -- "
            f"sync refused rather than pushing raw {store.currency} numbers as {target_currency}. "
            f"Should clear up on the next scheduled scrape; if it keeps happening, this store's "
            f"/cart.json may be unreachable or blocked from this server."
        )
        logger.error(f"Sync blocked for store {store_id} ({store.name}): {msg}")
        store.last_sync_status = "error"
        store.last_sync_summary = msg
        store.last_sync_at = datetime.now(timezone.utc)
        db.commit()
        summary["errors"] += 1
        _finish_progress(store_id, "error")
        return summary

    fx_rate = store.fx_rate_to_target or 1.0

    _init_progress(store_id, len(groups))

    async with aiohttp.ClientSession() as session:
        try:
            token = await _ensure_access_token(session, store, db)
            location_id = await sc.get_primary_location_id(session, domain, token)
            publication_id = await sc.get_online_store_publication_id(session, domain, token)

            # Every product synced from this source store gets added to a
            # collection named after the store (e.g. "Charizma") -- without
            # this, products were created and published fine but sat with
            # zero collection membership, so the storefront's per-brand
            # pages ("Shop by Brand" -> Charizma) showed "No products
            # found" despite the products genuinely existing and being
            # live. Resolved once per run and cached on the store row so
            # future syncs skip the lookup entirely.
            collection_id = store.dest_collection_id
            if not collection_id:
                collection_id = await sc.get_or_create_collection(session, domain, token, store.name)
                store.dest_collection_id = collection_id
                db.commit()
        except sc.ShopifyAPIError as e:
            logger.error(f"Sync setup failed for store {store_id} ({store.name}): {e}")
            store.last_sync_status = "error"
            store.last_sync_summary = str(e)
            store.last_sync_at = datetime.now(timezone.utc)
            db.commit()
            summary["errors"] += 1
            _finish_progress(store_id, "error")
            return summary

        for handle, variants in groups.items():
            group_counts = await _sync_one_group(
                session, domain, token, location_id, publication_id, collection_id,
                store, variants, handle, fx_rate, db,
            )
            for k, v in group_counts.items():
                summary[k] += v
            _advance_progress(store_id, handle, group_counts)

    total = summary["matched"] + summary["created"] + summary["not_found"] + summary["errors"]
    store.last_sync_at = datetime.now(timezone.utc)
    store.last_sync_status = "ok" if summary["errors"] == 0 else "error"
    store.last_sync_summary = (
        f"{summary['matched']} updated, {summary['created']} created, "
        f"{summary['not_found']} not found, {summary['errors']} errors (of {total})"
    )
    db.commit()
    _finish_progress(store_id, "done")
    return summary


async def sync_store_standalone(store_id: int, limit: int | None = None) -> dict:
    """Same as sync_store, but opens and closes its own DB session --
    used by the manual /stores/{id}/sync endpoint, which runs as a bare
    background task with no request-scoped session to reuse.

    Unlike the scheduled path in main.py (which logs its own completion
    line), nothing else logs the result of a manual sync -- add it here so
    a manual test run shows up in the terminal instead of finishing
    silently with no visible confirmation either way."""
    db = SessionLocal()
    try:
        summary = await sync_store(db, store_id, limit=limit)
        logger.info(f"Manual sync complete for store {store_id}: {summary}")
        return summary
    except Exception:
        logger.exception(f"Manual sync crashed for store {store_id}")
        _finish_progress(store_id, "error")
        raise
    finally:
        db.close()