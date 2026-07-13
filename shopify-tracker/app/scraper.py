"""
Async Shopify collection scraper.

Improvements over the original script:
- Concurrent requests (asyncio + aiohttp) instead of sequential, with a
  semaphore to avoid hammering any one store.
- Exponential backoff on retries instead of a flat 5s sleep.
- Handles are normalized (stripped/lowercased) to avoid silent 404s from
  typos or stray whitespace, e.g. "unstitched-fabric-1  khjc,".
- Duplicate handles for the same store are skipped instead of scraped twice.
- Every result is tagged with success/error status so the caller (and the
  dashboard) can see exactly which collections failed and why, rather than
  just a printed warning that scrolls off-screen.
"""
import asyncio
import logging
from dataclasses import dataclass, field
from typing import Optional

import aiohttp

logger = logging.getLogger("scraper")

MAX_ATTEMPTS = 3
BASE_BACKOFF_SECONDS = 2
REQUEST_TIMEOUT = 10
MAX_CONCURRENT_REQUESTS = 5


@dataclass
class ScrapeResult:
    store_url: str
    collection_handle: str
    status: str  # "ok", "empty", "error"
    products: list = field(default_factory=list)
    error: Optional[str] = None


def normalize_handle(handle: str) -> str:
    return handle.strip().lower()


def extract_variants(product: dict, collection_handle: str, store_url: str) -> list[dict]:
    """
    Flatten a Shopify product JSON blob into one row per variant.

    Pulls everything needed for a Shopify-import-ready CSV, not just the
    fields needed for the dashboard: body_html, tags, product handle, and
    each variant's option names/values (size, color, etc.) alongside the
    inventory quantity when the store happens to expose it publicly.
    """
    rows = []
    images = product.get("images") or []
    image_url = images[0]["src"] if images else ""

    # Shopify's public /products.json exposes option NAMES at the product
    # level (e.g. ["Size", "Color"]) and each variant carries its own
    # option1/option2/option3 VALUES. Both are needed to rebuild
    # Option1 Name / Option1 Value columns correctly on import.
    option_defs = product.get("options") or []
    option_names = [opt.get("name", f"Option{i+1}") for i, opt in enumerate(option_defs)]
    while len(option_names) < 3:
        option_names.append("")

    tags_raw = product.get("tags") or []
    tags = ", ".join(tags_raw) if isinstance(tags_raw, list) else str(tags_raw)

    handle = product.get("handle", "")
    product_url = f"{store_url.rstrip('/')}/products/{handle}" if handle else ""

    variants = product.get("variants") or [{}]

    for variant in variants:
        rows.append({
            "shopify_product_id": str(product.get("id", "")),
            "variant_id": str(variant.get("id", "")),
            "handle": handle,
            "product_url": product_url,
            "collection_handle": collection_handle,
            "title": product.get("title", "Untitled"),
            "vendor": product.get("vendor", ""),
            "body_html": product.get("body_html", "") or "",
            "tags": tags,
            "sku": variant.get("sku", ""),
            "price": float(variant.get("price", 0) or 0),
            "available": bool(variant.get("available", False)),
            # Public storefront JSON usually does NOT expose exact stock
            # counts (that's an admin-API-only field). If a store happens
            # to include it, we capture it; otherwise it's left as None
            # and the CSV export falls back to a 0/blank policy rather
            # than inventing a number.
            "inventory_qty": variant.get("inventory_quantity"),
            "image_url": image_url,
            "option1_name": option_names[0],
            "option1_value": variant.get("option1") or "",
            "option2_name": option_names[1],
            "option2_value": variant.get("option2") or "",
            "option3_name": option_names[2],
            "option3_value": variant.get("option3") or "",
        })
    return rows


async def fetch_page(session: aiohttp.ClientSession, url: str) -> Optional[dict]:
    for attempt in range(1, MAX_ATTEMPTS + 1):
        try:
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=REQUEST_TIMEOUT)) as resp:
                resp.raise_for_status()
                return await resp.json()
        except (aiohttp.ClientError, asyncio.TimeoutError) as e:
            if attempt < MAX_ATTEMPTS:
                wait = BASE_BACKOFF_SECONDS * (2 ** (attempt - 1))
                logger.warning(f"Attempt {attempt}/{MAX_ATTEMPTS} failed for {url}: {e}. Retrying in {wait}s")
                await asyncio.sleep(wait)
            else:
                logger.error(f"Giving up on {url} after {MAX_ATTEMPTS} attempts: {e}")
                raise
    return None


async def fetch_collection(
    session: aiohttp.ClientSession,
    store_url: str,
    collection_handle: str,
    semaphore: asyncio.Semaphore,
) -> ScrapeResult:
    handle = normalize_handle(collection_handle)
    all_products = []
    page = 1

    async with semaphore:
        try:
            while True:
                url = f"{store_url.rstrip('/')}/collections/{handle}/products.json?page={page}"
                data = await fetch_page(session, url)
                page_products = (data or {}).get("products", [])

                if not page_products:
                    break

                for product in page_products:
                    all_products.extend(extract_variants(product, handle, store_url))

                page += 1

        except (aiohttp.ClientError, asyncio.TimeoutError) as e:
            return ScrapeResult(
                store_url=store_url, collection_handle=handle,
                status="error", error=str(e),
            )

    status = "ok" if all_products else "empty"
    return ScrapeResult(
        store_url=store_url, collection_handle=handle,
        status=status, products=all_products,
    )


async def scrape_store(store_url: str, collection_handles: list[str]) -> list[ScrapeResult]:
    """Scrape all collections for one store concurrently, deduping handles."""
    seen = set()
    unique_handles = []
    for h in collection_handles:
        norm = normalize_handle(h)
        if norm and norm not in seen:
            seen.add(norm)
            unique_handles.append(norm)

    semaphore = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)
    async with aiohttp.ClientSession(headers={"User-Agent": "product-tracker/1.0"}) as session:
        tasks = [
            fetch_collection(session, store_url, handle, semaphore)
            for handle in unique_handles
        ]
        return await asyncio.gather(*tasks)
