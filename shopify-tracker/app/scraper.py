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
import re
from dataclasses import dataclass, field
from datetime import datetime, timezone
import email.utils
from typing import Optional

import aiohttp

logger = logging.getLogger("scraper")

MAX_ATTEMPTS = 3
BASE_BACKOFF_SECONDS = 2
REQUEST_TIMEOUT = 10
# Lowered from 5 -- at concurrency 5, nearly every tracked store was
# getting rate-limited (429) on nearly every collection, nearly every
# night. These are modest-traffic Pakistani storefronts, not built to
# absorb request bursts the way a high-traffic site would be.
MAX_CONCURRENT_REQUESTS = 1
# Separate, more patient handling for 429s specifically: a rate limit is
# fully recoverable given enough of a cooldown (unlike a genuine
# connection failure), so it gets more attempts than MAX_ATTEMPTS and a
# backoff that honors the site's own Retry-After header when it sends one
# (most do) instead of guessing with the same short backoff as everything else.
MAX_RATE_LIMIT_ATTEMPTS = 6
RATE_LIMIT_BACKOFF_SECONDS = 8  # fallback wait when no Retry-After header is present
# Small fixed pacing delay between consecutive page requests to the SAME
# collection, on top of the concurrency cap above -- cheap extra insurance
# against a simple "N requests per second" limiter that the concurrency
# cap alone doesn't fully address.
PAGE_FETCH_DELAY_SECONDS = 1.0
STORE_COUNTRY_HINT = "PK"  # see detect_store_currency() -- forces Pakistan-market localization

# ---------------------------------------------------------------------------
# Live scrape progress + cancellation. Same in-memory-dict-keyed-by-store_id
# pattern already used in shopify_sync.py for sync progress -- read there for
# the reasoning (fine for a single-process deployment; a running scrape
# isn't something that needs to survive a restart anyway).
# ---------------------------------------------------------------------------
_scrape_progress: dict[int, dict] = {}
_cancel_requested: dict[int, bool] = {}


def clear_scrape_progress(store_id: int) -> None:
    """Drops any in-memory scrape progress/cancel-flag entries for this
    store. Must be called when a store is deleted -- see the matching
    clear_progress() in shopify_sync.py for why: a reused store id
    should never inherit a previous store's stale "running" status."""
    _scrape_progress.pop(store_id, None)
    _cancel_requested.pop(store_id, None)


def get_scrape_progress(store_id: int) -> dict:
    """Used by GET /scrape/{id}/progress. {"status": "idle"} if this store
    has never been scraped (or the server just restarted)."""
    return _scrape_progress.get(store_id, {"status": "idle"})


def request_scrape_cancel(store_id: int) -> None:
    """Used by POST /scrape/{id}/stop. Doesn't kill anything mid-flight --
    the running scrape checks this flag between pages/collections and stops
    itself at the next safe point, keeping whatever it already scraped."""
    _cancel_requested[store_id] = True


def _is_cancelled(store_id: int) -> bool:
    return _cancel_requested.get(store_id, False)


def _init_scrape_progress(store_id: int, total_collections: int) -> None:
    _scrape_progress[store_id] = {
        "status": "running",
        "total_collections": total_collections,
        "completed_collections": 0,
        "current_collection": None,
        "products_scraped": 0,
        "started_at": datetime.now(timezone.utc).isoformat(),
    }
    _cancel_requested[store_id] = False


def _update_scrape_progress(store_id: int, collection_handle: str, products_so_far_in_collection: int) -> None:
    p = _scrape_progress.get(store_id)
    if not p:
        return
    p["current_collection"] = collection_handle
    # "products_scraped" is a running total across every collection in this
    # scrape, not just the current one -- rebuilt from each collection's own
    # count each time one of them reports progress.
    per_collection = p.setdefault("_per_collection_counts", {})
    per_collection[collection_handle] = products_so_far_in_collection
    p["products_scraped"] = sum(per_collection.values())


def _advance_scrape_progress(store_id: int, collection_handle: str) -> None:
    p = _scrape_progress.get(store_id)
    if not p:
        return
    p["completed_collections"] += 1


def _finish_scrape_progress(store_id: int, status: str) -> None:
    p = _scrape_progress.get(store_id)
    if p:
        p["status"] = status
        p["finished_at"] = datetime.now(timezone.utc).isoformat()

# Clothing stores like Maria B / Gul Ahmed usually bake colour and
# piece-count into the product title/description rather than exposing them
# as real Shopify options, so there's nothing for the storefront JSON to
# hand us directly. These are best-effort keyword detectors over free text
# — good enough to populate a filterable tag/option, not a source of truth.
KNOWN_COLOURS = [
    "black", "white", "ivory", "off white", "off-white", "cream", "beige",
    "red", "maroon", "crimson", "wine", "burgundy", "pink", "hot pink",
    "fuchsia", "peach", "coral", "orange", "rust", "mustard", "yellow",
    "gold", "green", "emerald", "olive", "mint", "teal", "turquoise",
    "blue", "navy", "royal blue", "sky blue", "cobalt", "purple",
    "lavender", "lilac", "mauve", "magenta", "grey", "gray", "charcoal",
    "silver", "brown", "tan", "chocolate", "copper", "multi", "multicolor",
    "multicolour",
]

# Longer/more specific names first so e.g. "royal blue" matches before "blue".
_COLOUR_PATTERNS = sorted(KNOWN_COLOURS, key=len, reverse=True)

_PIECE_COUNT_RE = re.compile(
    r"\b(1|2|3|one|two|three)[\s-]*(pc|pcs|piece|pieces)\b", re.IGNORECASE
)
_PIECE_WORD_TO_NUM = {"one": "1", "two": "2", "three": "3"}


def _scan_for_colours(text: str) -> Optional[str]:
    """Runs the whole-word colour scan over a single piece of text and
    collapses the result the same way regardless of caller: "Multi
    Colour" if more than one distinct colour (or an explicit
    multi/multicolour word) is found, the single matched colour
    (title-cased) if exactly one is found, or None."""
    working = text.lower()
    found = []
    explicit_multi = False

    # Longest names first (e.g. "royal blue") so a shorter name that's
    # embedded in one we already matched (e.g. "blue") doesn't also get
    # counted as a second, separate colour.
    for colour in _COLOUR_PATTERNS:
        match = re.search(r"\b" + re.escape(colour) + r"\b", working)
        if not match:
            continue
        if colour in ("multi", "multicolor", "multicolour"):
            explicit_multi = True
        else:
            found.append(colour)
        # Blank out the matched span so it can't be re-matched by a
        # shorter colour name nested inside it.
        working = working[:match.start()] + (" " * (match.end() - match.start())) + working[match.end():]

    distinct = list(dict.fromkeys(found))  # de-dupe, keep first-seen order
    if explicit_multi or len(distinct) > 1:
        return "Multi Colour"
    if distinct:
        return distinct[0].title()
    return None


def _detect_colour_from_handle(handle: str) -> Optional[str]:
    """Some stores (e.g. Maria B) encode the real colour as the last
    segment(s) of the product handle, e.g. "sf-ea24-06-off-white" or
    "sf-w25-72-black". Only the trailing token(s) are checked, and only
    against an exact match -- not a substring scan -- so an unrelated
    product code token can't accidentally look like a colour."""
    if not handle:
        return None
    tokens = [t for t in handle.lower().split("-") if t]
    if not tokens:
        return None

    if len(tokens) >= 2:
        last_two = f"{tokens[-2]} {tokens[-1]}"
        if last_two in KNOWN_COLOURS:
            return last_two.title()

    last = tokens[-1]
    if last in ("multi", "multicolor", "multicolour"):
        return "Multi Colour"
    if last in KNOWN_COLOURS:
        return last.title()
    return None


def detect_colour(title: str, handle: str = "") -> Optional[str]:
    """Best-effort colour guess from the title, falling back to the
    product handle if the title has none.

    Deliberately does NOT scan body_html/description text. That used to
    be a fallback here, but real data showed it produces wrong colours:
    marketing copy routinely uses colour words rhetorically ("a
    statement in red", "shades of blush and gold") without describing
    the garment's actual colour, and a plain word-boundary match can't
    tell that apart from a genuine "this dress is red". Title and handle
    are both store-controlled identifiers rather than free prose, so a
    colour word appearing there is far more trustworthy.

    If more than one distinct colour is named in the same haystack (or
    the text explicitly says "multi"/"multicolour"), returns "Multi
    Colour" instead of picking just one. Otherwise returns the single
    matched colour name (title-cased), or None if nothing matched
    anywhere (title or handle).
    """
    from_title = _scan_for_colours(title or "")
    if from_title:
        return from_title
    return _detect_colour_from_handle(handle)


IMPLIED_PIECE_PHRASES = {
    "co-ord": "2", "co ord": "2", "coord": "2", "coordinate set": "2",
    "co-oed": "2", "co oed": "2", "coed set": "2",  # common source-site typo for "co-ord"
    "2-in-1": "2", "twin set": "2",
    "3-in-1": "3",
}

# Gul Ahmed-style titles don't state a piece count directly -- they list
# the garment components instead (e.g. "Shirt And Trouser With Dupatta").
# When nothing else matches, count how many distinct components are named
# and use that as the piece count. Order doesn't matter; a title only
# needs to contain ONE word from a group to count that component once.
TOP_WORDS = {"shirt", "kurti", "kurta", "top", "tunic", "jacket", "waistcoat", "blouse", "jumpsuit", "jump suit", "gown", "dress", "abaya", "kaftan", "coat"}
BOTTOM_WORDS = {"trouser", "trousers", "pant", "pants", "shalwar", "pajama", "pajami", "sharara", "gharara"}
THIRD_WORDS = {"dupatta", "shawl", "scarf", "stole"}


def _count_components(text: str) -> int:
    lowered = text.lower()
    count = 0
    if any(re.search(rf"\b{re.escape(w)}\b", lowered) for w in TOP_WORDS):
        count += 1
    if any(re.search(rf"\b{re.escape(w)}\b", lowered) for w in BOTTOM_WORDS):
        count += 1
    if any(re.search(rf"\b{re.escape(w)}\b", lowered) for w in THIRD_WORDS):
        count += 1
    return count


def detect_piece_count(title: str, product_type: str = "") -> Optional[str]:
    """Best-effort '1/2/3 Piece' guess from title or product type. Tries,
    in order: a literal '3 piece' in the name, a garment term that implies
    a count (e.g. 'Co-ord Set' -> 2 Piece), then falls back to counting
    named garment components (e.g. 'Shirt And Trouser' -> 2 Piece) for
    stores that describe the pieces instead of stating a count."""
    for text in (title or "", product_type or ""):
        match = _PIECE_COUNT_RE.search(text)
        if match:
            num = match.group(1).lower()
            num = _PIECE_WORD_TO_NUM.get(num, num)
            return f"{num} Piece"

        lowered = text.lower()
        for phrase, num in IMPLIED_PIECE_PHRASES.items():
            if phrase in lowered:
                return f"{num} Piece"

    for text in (title or "", product_type or ""):
        count = _count_components(text)
        if count:
            return f"{count} Piece"
    return None
    return None


# Common clothing size tokens. Used to tell a real ready-to-wear/stitched
# item (has actual sizes) apart from an unstitched/fabric-only listing
# (usually has no size option, or a placeholder like "Unstitched"/"Standard").
SIZE_TOKENS = {
    "xxs", "xs", "s", "m", "l", "xl", "xxl", "xxxl", "2xl", "3xl", "4xl",
    "small", "medium", "large",
    "free size", "freesize", "one size",
}


def looks_like_size(value: Optional[str]) -> bool:
    if not value:
        return False
    v = value.strip().lower()
    if v in SIZE_TOKENS:
        return True
    # Numeric dress/waist sizes, e.g. "34", "36", "38-40"
    return bool(re.fullmatch(r"\d{2}(-\d{2})?", v))


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
    all_image_urls = [img["src"] for img in images if img.get("src")]
    image_url = all_image_urls[0] if all_image_urls else ""
    # Comma-joined so it can ride along on the flat per-variant row like the
    # other product-level fields (title, vendor, etc.) — split back out at
    # export time to write one extra CSV row per additional image.
    all_images_joined = ", ".join(all_image_urls)

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

    body_html = product.get("body_html", "") or ""
    title = product.get("title", "Untitled")
    product_type = product.get("product_type", "") or ""

    detected_colour = detect_colour(title, handle)
    piece_count = detect_piece_count(title, product_type)

    # If the source store already exposes colour as a real option (some
    # do), don't overwrite it — just leave the detected value on the row
    # for reference. Otherwise, if there's a free option slot (2 or 3),
    # promote the detected colour into it so it becomes a real, filterable
    # Shopify option on import rather than just buried in the title.
    option_names = list(option_names)  # local copy, may get mutated below
    already_has_colour_option = any(
        (n or "").strip().lower() in ("colour", "color") for n in option_names
    )
    colour_slot = None
    if detected_colour and not already_has_colour_option:
        for i in range(3):
            if not option_names[i]:
                option_names[i] = "Color"  # US spelling to match Shopify convention/theme swatch expectations
                colour_slot = i
                break

    for variant in variants:
        option_values = [
            variant.get("option1") or "",
            variant.get("option2") or "",
            variant.get("option3") or "",
        ]
        if colour_slot is not None and not option_values[colour_slot]:
            option_values[colour_slot] = detected_colour

        rows.append({
            "shopify_product_id": str(product.get("id", "")),
            "variant_id": str(variant.get("id", "")),
            "handle": handle,
            "product_url": product_url,
            "collection_handle": collection_handle,
            "title": title,
            "vendor": product.get("vendor", ""),
            "product_type": product_type,
            "body_html": body_html,
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
            # Same story for weight: "grams" is present on many storefronts
            # but not guaranteed. Left as None (not 0) when absent so the
            # export doesn't claim a false zero-weight item.
            "weight_grams": variant.get("grams"),
            "image_url": image_url,
            "all_image_urls": all_images_joined,
            "option1_name": option_names[0],
            "option1_value": option_values[0],
            "option2_name": option_names[1],
            "option2_value": option_values[1],
            "option3_name": option_names[2],
            "option3_value": option_values[2],
            "detected_colour": detected_colour,
            "piece_count": piece_count,
        })
    return rows


async def detect_store_currency(session: aiohttp.ClientSession, store_url: str) -> Optional[str]:
    """
    Auto-detects a store's real currency instead of relying on it being
    typed in manually on the dashboard.

    Every Shopify storefront exposes /cart.json (even with an empty cart),
    and it always includes the store's actual currency code (e.g. "PKR",
    "AED", "USD") -- this is far more reliable than guessing from prices
    or country, since a store's displayed currency isn't always obvious
    from its URL/TLD.

    Every store this app tracks runs Shopify Markets with region/currency
    selectors (Pakistan, UAE, UK, Bangladesh, etc -- visible on-site as a
    "Region"/"Currency" picker). A cold, cookie-less request to /cart.json
    gets whatever currency Markets auto-selects based on THIS SERVER's own
    IP location -- which, once hosted (Railway/Render/etc, not a Pakistani
    residential IP), is very likely NOT Pakistan. That silently reports
    the wrong currency (e.g. "USD") for a store whose product prices are
    still raw PKR numbers, which downstream treats as a 1:1 conversion --
    turning a real 15,000 PKR item into a literal "$15,000". Forcing
    `?country=PK` is Shopify's own supported override for this (the same
    mechanism their on-site country-selector links use), so it reliably
    gets the store's real Pakistan-market currency no matter where this
    server itself is hosted.
    """
    url = f"{store_url.rstrip('/')}/cart.json?country={STORE_COUNTRY_HINT}"
    try:
        data = await fetch_page(session, url)
    except (aiohttp.ClientError, asyncio.TimeoutError) as e:
        logger.warning(f"Could not auto-detect currency for {store_url}: {e}")
        return None
    currency = (data or {}).get("currency")
    return currency.upper() if currency else None


async def fetch_fx_rate(session: aiohttp.ClientSession, from_currency: str, to_currency: str) -> Optional[float]:
    """
    Fetches how many units of `to_currency` one unit of `from_currency` is
    worth, e.g. fetch_fx_rate(session, "PKR", "USD") -> ~0.0036.

    Used to convert a source store's scraped prices (in ITS currency)
    into the currency your own Shopify store sells in, so imported prices
    are actually correct rather than raw PKR numbers landing in a
    USD-priced store. Uses a free, no-API-key exchange rate service;
    rates don't move fast enough to need anything fancier than a refresh
    once per scrape cycle.
    """
    if from_currency == to_currency:
        return 1.0
    url = f"https://open.er-api.com/v6/latest/{from_currency}"
    try:
        data = await fetch_page(session, url)
    except (aiohttp.ClientError, asyncio.TimeoutError) as e:
        logger.warning(f"Could not fetch FX rate {from_currency}->{to_currency}: {e}")
        return None
    rates = (data or {}).get("rates") or {}
    rate = rates.get(to_currency)
    return float(rate) if rate is not None else None


async def fetch_page(session: aiohttp.ClientSession, url: str) -> Optional[dict]:
    attempt = 0
    rate_limit_attempt = 0
    while True:
        attempt += 1
        try:
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=REQUEST_TIMEOUT)) as resp:
                resp.raise_for_status()
                return await resp.json()
        except aiohttp.ClientResponseError as e:
            if e.status == 429:
                rate_limit_attempt += 1
                if rate_limit_attempt > MAX_RATE_LIMIT_ATTEMPTS:
                    logger.error(f"Giving up on {url} after {MAX_RATE_LIMIT_ATTEMPTS} rate-limited (429) attempts")
                    raise
                # aiohttp's ClientResponseError carries the response's own
                # headers, so the site's own requested cooldown (if it
                # sent one) is used instead of a guess.
                retry_after = e.headers.get("Retry-After") if e.headers else None
                wait = RATE_LIMIT_BACKOFF_SECONDS * (2 ** (rate_limit_attempt - 1))
                if retry_after is not None:
                    try:
                        # Retry-After may be a number of seconds or an HTTP date.
                        wait = max(0.0, float(retry_after))
                    except (TypeError, ValueError):
                        try:
                            retry_at = email.utils.parsedate_to_datetime(retry_after)
                            if retry_at.tzinfo is None:
                                retry_at = retry_at.replace(tzinfo=timezone.utc)
                            wait = max(0.0, (retry_at - datetime.now(timezone.utc)).total_seconds())
                        except (TypeError, ValueError, OverflowError):
                            pass
                # Avoid hot-looping if a server sends a malformed or zero delay.
                wait = max(1.0, wait)
                logger.warning(
                    f"Rate limited (429) on {url}, attempt {rate_limit_attempt}/{MAX_RATE_LIMIT_ATTEMPTS}. "
                    f"Waiting {wait:.1f}s (Retry-After={retry_after})"
                )
                await asyncio.sleep(wait)
                continue
            # Any other HTTP error status falls through to the same
            # generic retry/backoff as a connection error below.
            if attempt < MAX_ATTEMPTS:
                wait = BASE_BACKOFF_SECONDS * (2 ** (attempt - 1))
                logger.warning(f"Attempt {attempt}/{MAX_ATTEMPTS} failed for {url}: {e}. Retrying in {wait}s")
                await asyncio.sleep(wait)
            else:
                logger.error(f"Giving up on {url} after {MAX_ATTEMPTS} attempts: {e}")
                raise
        except (aiohttp.ClientError, asyncio.TimeoutError) as e:
            if attempt < MAX_ATTEMPTS:
                wait = BASE_BACKOFF_SECONDS * (2 ** (attempt - 1))
                logger.warning(f"Attempt {attempt}/{MAX_ATTEMPTS} failed for {url}: {e}. Retrying in {wait}s")
                await asyncio.sleep(wait)
            else:
                logger.error(f"Giving up on {url} after {MAX_ATTEMPTS} attempts: {e}")
                raise


async def fetch_collection(
    session: aiohttp.ClientSession,
    store_url: str,
    collection_handle: str,
    semaphore: asyncio.Semaphore,
    store_id: Optional[int] = None,
) -> ScrapeResult:
    handle = normalize_handle(collection_handle)
    all_products = []
    page = 1
    status = "ok"
    error_msg = None

    async with semaphore:
        try:
            while True:
                # Checked once per page rather than once per product -- a
                # page (up to ~30-50 products) is a short enough unit of
                # work that stopping here is effectively immediate from the
                # user's point of view, without adding a check inside the
                # tight per-product loop below.
                if store_id is not None and _is_cancelled(store_id):
                    status = "cancelled"
                    error_msg = "Stopped by user"
                    break

                # `?country={STORE_COUNTRY_HINT}` for the same reason
                # detect_store_currency() needs it on /cart.json: Shopify
                # Markets can localize a cold request's response based on
                # THIS SERVER's IP, not just the currency label but the
                # actual price numbers too -- without this, store.currency
                # can correctly say "PKR" (cart.json got the override)
                # while prices scraped here quietly come back in a
                # different market's numbers, which then compounds with
                # FX conversion and any margin into a wrong final price.
                url = f"{store_url.rstrip('/')}/collections/{handle}/products.json?page={page}&country={STORE_COUNTRY_HINT}"
                data = await fetch_page(session, url)
                page_products = (data or {}).get("products", [])

                if not page_products:
                    break

                for product in page_products:
                    all_products.extend(extract_variants(product, handle, store_url))

                if store_id is not None:
                    _update_scrape_progress(store_id, handle, len(all_products))

                page += 1
                # Small pacing delay between consecutive page requests to
                # this same collection, on top of the lowered concurrency
                # cap above -- these are modest-traffic storefronts and
                # several were getting 429'd on nearly every collection,
                # nearly every night, at the old concurrency/pacing.
                await asyncio.sleep(PAGE_FETCH_DELAY_SECONDS)

        except (aiohttp.ClientError, asyncio.TimeoutError) as e:
            status = "error"
            error_msg = str(e)
        finally:
            if store_id is not None:
                _advance_scrape_progress(store_id, handle)

    if status == "ok" and not all_products:
        status = "empty"

    return ScrapeResult(
        store_url=store_url, collection_handle=handle,
        status=status, products=all_products, error=error_msg,
    )


async def refresh_store_currency(store_url: str, target_currency: str) -> tuple[Optional[str], Optional[float]]:
    """Convenience wrapper: opens its own session, detects the store's
    real currency, then looks up the conversion rate into target_currency.
    Returns (detected_currency, fx_rate_to_target) -- either can be None
    if detection/lookup failed, in which case the caller should just
    leave the store's existing values alone rather than overwrite them
    with nothing."""
    async with aiohttp.ClientSession(headers={"User-Agent": "product-tracker/1.0"}) as session:
        currency = await detect_store_currency(session, store_url)
        if not currency:
            return None, None
        rate = await fetch_fx_rate(session, currency, target_currency)
        return currency, rate


async def scrape_store(store_url: str, collection_handles: list[str], store_id: Optional[int] = None) -> list[ScrapeResult]:
    """Scrape all collections for one store concurrently, deduping handles.

    Pass store_id to get live progress via get_scrape_progress(store_id)
    and let request_scrape_cancel(store_id) stop the run early -- both are
    no-ops if store_id is omitted (e.g. one-off/manual use)."""
    seen = set()
    unique_handles = []
    for h in collection_handles:
        norm = normalize_handle(h)
        if norm and norm not in seen:
            seen.add(norm)
            unique_handles.append(norm)

    if store_id is not None:
        _init_scrape_progress(store_id, len(unique_handles))

    semaphore = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)
    async with aiohttp.ClientSession(headers={"User-Agent": "product-tracker/1.0"}) as session:
        tasks = [
            fetch_collection(session, store_url, handle, semaphore, store_id=store_id)
            for handle in unique_handles
        ]
        results = await asyncio.gather(*tasks)

    if store_id is not None:
        final_status = "cancelled" if any(r.status == "cancelled" for r in results) else "done"
        _finish_scrape_progress(store_id, final_status)

    return results