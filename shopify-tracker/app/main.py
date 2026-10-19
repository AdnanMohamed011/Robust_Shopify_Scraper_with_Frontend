import asyncio
import logging
from datetime import datetime, timezone, timedelta
from pathlib import Path

from fastapi import FastAPI, Depends, HTTPException, BackgroundTasks, Request
from fastapi.responses import FileResponse, StreamingResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel
from sqlalchemy.orm import Session

from .database import Base, engine, get_db, run_lightweight_migrations
from .models import Store, Collection, Product, PriceHistory, AppSettings
from .scraper import scrape_store, refresh_store_currency, get_scrape_progress, request_scrape_cancel, clear_scrape_progress
from .ingest import ingest_results
from .scheduler import start_scheduler, rebuild_store_jobs, remove_store_jobs, get_status as get_scheduler_status
from .export import build_shopify_csv, filter_sized_only
from . import shopify_sync

logging.basicConfig(level=logging.INFO)

# Resolved relative to this file, not the current working directory --
# "static" alone only works if you launch uvicorn from exactly the right
# folder. Most hosts start the process from wherever they've cloned the
# repo, which isn't guaranteed to be the same working directory you'd get
# running `python -m uvicorn app.main:app` by hand from shopify-tracker/.
STATIC_DIR = Path(__file__).resolve().parent.parent / "static"


Base.metadata.create_all(bind=engine)
run_lightweight_migrations()

app = FastAPI(title="Product Tracker API")


@app.exception_handler(Exception)
async def _unhandled_exception_handler(request: Request, exc: Exception):
    """Without this, any bug that raises something other than an
    HTTPException produces Starlette's default plain-text 500 page --
    which the dashboard's api() helper can't parse as JSON, so the
    request just looks like it silently did nothing rather than showing
    the user any error at all (exactly what happened with the schedule
    save bug). This guarantees every endpoint, including ones added later,
    always returns a JSON body with a `detail` field on failure -- the
    shape the dashboard already knows how to display."""
    logging.error(f"Unhandled error on {request.method} {request.url.path}: {exc}", exc_info=True)
    return JSONResponse(status_code=500, content={"detail": f"Internal error: {exc}"})


@app.on_event("startup")
def on_startup():
    db = next(get_db())
    try:
        stores = [(s.id, s.scrape_times) for s in db.query(Store).all()]
        start_scheduler(stores)
    finally:
        db.close()


# ---------- Schemas ----------

class StoreCreate(BaseModel):
    name: str
    base_url: str
    # No currency field anymore -- it's auto-detected from the store's own
    # /cart.json on the first scrape rather than typed in by hand.


class SettingsUpdate(BaseModel):
    target_currency: str


class MarginUpdate(BaseModel):
    margin_percent: float


class ScheduleUpdate(BaseModel):
    scrape_time: str  # 24h "HH:MM" (UTC), e.g. "10:00"; empty string clears the schedule. Once-daily only, by design -- see update_schedule.


class CollectionCreate(BaseModel):
    handle: str


class ShopifyConnectionUpdate(BaseModel):
    shopify_domain: str
    shopify_client_id: str
    shopify_client_secret: str
    api_sync_enabled: bool = True
    auto_create_enabled: bool = False


# ---------- Concurrency guard ----------


# A real scrape or sync should never legitimately run longer than this.
# _busy_reason uses it to tell a genuinely-running job apart from a
# stale "running" flag left behind by one that never got to report
# finishing (most commonly: the server process restarted mid-run, so
# nothing ever called _finish_progress/_finish_scrape_progress). Without
# this, a stale flag blocks that store's scrape/sync AND its deletion
# forever, since both go through this same check.
_MAX_JOB_DURATION = timedelta(minutes=30)


def _is_stale(progress: dict) -> bool:
    started = progress.get("started_at")
    if not started:
        return False
    try:
        started_at = datetime.fromisoformat(started)
    except ValueError:
        return False
    return datetime.now(timezone.utc) - started_at > _MAX_JOB_DURATION


def _busy_reason(store_id: int) -> str | None:
    """Returns a short, user-facing reason if this store currently has a
    scrape or a sync in progress, else None. Called from every endpoint
    that would otherwise be able to race a running scrape/sync -- adding
    or deleting a collection mid-scrape, deleting the store itself,
    starting a second scrape/sync on top of one that's already going, etc.
    Both scrape and sync progress live in-memory (see scraper.py /
    shopify_sync.py), which is fine for the single-process deployment this
    runs as -- there's exactly one place a "running" status could be true.
    A "running" status older than _MAX_JOB_DURATION is treated as a stale
    leftover rather than a real block -- see _is_stale."""
    scrape_p = get_scrape_progress(store_id)
    if scrape_p.get("status") == "running" and not _is_stale(scrape_p):
        return "A scrape is currently running for this store — wait for it to finish, or stop it first."
    sync_p = shopify_sync.get_progress(store_id)
    if sync_p.get("status") == "running" and not _is_stale(sync_p):
        return "A sync is currently running for this store — wait for it to finish."
    return None


# ---------- Store & collection management ----------

@app.post("/stores")
async def create_store(store: StoreCreate, db: Session = Depends(get_db)):
    base_url = store.base_url.rstrip("/")
    existing = db.query(Store).filter(Store.base_url == base_url).first()
    if existing:
        raise HTTPException(400, "Store already exists")
    new_store = Store(name=store.name, base_url=base_url)
    db.add(new_store)
    db.commit()
    db.refresh(new_store)

    # Detect currency right away rather than waiting for the first
    # collection scrape, so it shows correctly the moment the store is
    # added. Best-effort: if the site is slow/unreachable right now, this
    # just gets picked up on the next scheduled scrape instead.
    settings = _get_or_create_settings(db)
    detected_currency, fx_rate = await refresh_store_currency(base_url, settings.target_currency)
    if detected_currency:
        new_store.currency = detected_currency
    if fx_rate is not None:
        new_store.fx_rate_to_target = fx_rate
        new_store.fx_rate_updated_at = datetime.now(timezone.utc)
    db.commit()
    db.refresh(new_store)
    return new_store


@app.get("/stores")
def list_stores(db: Session = Depends(get_db)):
    return db.query(Store).all()


# ---------- App settings (target currency) ----------

def _get_or_create_settings(db: Session) -> AppSettings:
    settings = db.query(AppSettings).filter(AppSettings.id == 1).first()
    if not settings:
        settings = AppSettings(id=1, target_currency="USD")
        db.add(settings)
        db.commit()
        db.refresh(settings)
    return settings


@app.get("/settings")
def get_settings(db: Session = Depends(get_db)):
    return _get_or_create_settings(db)


@app.put("/settings")
def update_settings(update: SettingsUpdate, db: Session = Depends(get_db)):
    """Sets the currency your own Shopify store sells in. Every tracked
    store's prices get converted into this currency on export and sync --
    change it here rather than per-store."""
    settings = _get_or_create_settings(db)
    settings.target_currency = update.target_currency.strip().upper()
    db.commit()
    db.refresh(settings)
    return settings


@app.put("/stores/{store_id}/margin")
def update_margin(store_id: int, update: MarginUpdate, db: Session = Depends(get_db)):
    """Sets this store's flat percentage markup, applied on top of the
    FX-converted price before the .99 charm-price rounding -- see
    export.apply_margin. Takes effect on the very next export/sync, no
    other steps needed. 0 means a direct price match with no markup."""
    store = db.query(Store).filter(Store.id == store_id).first()
    if not store:
        raise HTTPException(404, "Store not found")
    store.margin_percent = update.margin_percent
    db.commit()
    db.refresh(store)
    return store


@app.put("/stores/{store_id}/schedule")
def update_schedule(store_id: int, update: ScheduleUpdate, db: Session = Depends(get_db)):
    """Sets when this store auto-scrapes (and auto-syncs, collection by
    collection, if sync is enabled -- see run_scrape_for_store_sequential)
    -- independent of every other store's schedule. Deliberately once
    per day, not a list of times -- as the catalog count grows, more
    frequent auto-runs per store multiplies the total load (and overselling
    risk window from stale stock is judged an acceptable tradeoff against
    that, not the other way around) -- so this only ever accepts a single
    HH:MM rather than several. Takes effect immediately (rebuild_store_jobs
    replaces this store's one cron job right now), no redeploy or restart
    needed. An empty string clears the schedule entirely -- this store
    then only runs on manual Scrape Now / Sync Now."""
    store = db.query(Store).filter(Store.id == store_id).first()
    if not store:
        raise HTTPException(404, "Store not found")
    time_str = update.scrape_time.strip()
    if "," in time_str:
        raise HTTPException(400, "Only one time per day is supported for a store's schedule.")
    if time_str:
        try:
            hour_str, minute_str = time_str.split(":")
            hour, minute = int(hour_str), int(minute_str)
            if not (0 <= hour <= 23 and 0 <= minute <= 59):
                raise ValueError
        except ValueError:
            raise HTTPException(400, f"Invalid time {time_str!r} -- use 24h HH:MM, e.g. 10:00")
    store.scrape_times = time_str or None
    db.commit()
    db.refresh(store)
    try:
        rebuild_store_jobs(store_id, store.scrape_times)
    except Exception as e:
        # The DB write above already succeeded (store.scrape_times is
        # correctly saved), so don't roll that back over a scheduling
        # hiccup -- but do surface it clearly rather than a bare 500 with
        # no body, which the dashboard can't show the user anything useful
        # for. Whatever job state exists gets picked up correctly on the
        # next server restart's on_startup pass regardless.
        logging.error(f"rebuild_store_jobs failed for store {store_id}: {e}")
        raise HTTPException(
            500,
            f"Saved, but couldn't apply the new schedule to the running server immediately ({e}). "
            "It will still take effect on the next restart -- or try saving again.",
        )
    return store


@app.post("/stores/{store_id}/collections")
def add_collection(store_id: int, collection: CollectionCreate, db: Session = Depends(get_db)):
    store = db.query(Store).filter(Store.id == store_id).first()
    if not store:
        raise HTTPException(404, "Store not found")
    reason = _busy_reason(store_id)
    if reason:
        raise HTTPException(409, reason)

    handle = collection.handle.strip().lower()
    existing = db.query(Collection).filter(
        Collection.store_id == store_id, Collection.handle == handle
    ).first()
    if existing:
        raise HTTPException(400, "Collection already tracked for this store")

    new_collection = Collection(store_id=store_id, handle=handle)
    db.add(new_collection)
    db.commit()
    db.refresh(new_collection)
    return new_collection


@app.get("/stores/{store_id}/collections")
def list_collections(store_id: int, db: Session = Depends(get_db)):
    return db.query(Collection).filter(Collection.store_id == store_id).all()


@app.delete("/stores/{store_id}")
def delete_store(store_id: int, db: Session = Depends(get_db)):
    store = db.query(Store).filter(Store.id == store_id).first()
    if not store:
        raise HTTPException(404, "Store not found")
    reason = _busy_reason(store_id)
    if reason:
        raise HTTPException(409, reason)
    db.delete(store)  # cascades to its collections and products (see models.py)
    db.commit()
    # Drop any in-memory progress state for this id -- otherwise, since
    # SQLite can reuse a deleted store's numeric id, a future new store
    # could inherit a stale "running" flag from whatever this store's id
    # was last doing. See clear_progress/clear_scrape_progress docstrings.
    shopify_sync.clear_progress(store_id)
    clear_scrape_progress(store_id)
    remove_store_jobs(store_id)
    return {"deleted": True, "store_id": store_id}


@app.delete("/stores/{store_id}/collections/{collection_id}")
def delete_collection(store_id: int, collection_id: int, db: Session = Depends(get_db)):
    reason = _busy_reason(store_id)
    if reason:
        raise HTTPException(409, reason)
    collection = db.query(Collection).filter(
        Collection.store_id == store_id, Collection.id == collection_id
    ).first()
    if not collection:
        raise HTTPException(404, "Collection not found")

    handle = collection.handle
    # Bulk Query.delete() below operates at the SQL level and does NOT
    # trigger the ORM cascade on Product.price_history (that cascade only
    # fires when objects are deleted one at a time through the session,
    # not via a bulk filter().delete()). Deleting products first would
    # leave their price_history rows pointing at now-nonexistent product
    # ids -- harmless on SQLite (foreign keys unenforced) but Postgres
    # correctly rejects it with an IntegrityError -> 500, same failure
    # mode already called out for a different path in models.py. So
    # price_history has to go first, keyed off this collection's own
    # product ids, before the products themselves are removed.
    product_ids = [
        pid for (pid,) in db.query(Product.id).filter(
            Product.store_id == store_id, Product.collection_handle == handle
        )
    ]
    if product_ids:
        db.query(PriceHistory).filter(PriceHistory.product_id.in_(product_ids)).delete(
            synchronize_session=False
        )
        db.query(Product).filter(Product.id.in_(product_ids)).delete(synchronize_session=False)
    db.delete(collection)
    db.commit()
    return {"deleted": True, "collection_id": collection_id}


# ---------- Shopify API sync (pushes tracked products into the CLIENT's own store) ----------

@app.put("/stores/{store_id}/shopify-connection")
def update_shopify_connection(store_id: int, update: ShopifyConnectionUpdate, db: Session = Depends(get_db)):
    """Configures (or updates) the client's own Shopify store connection
    for this tracked store. Needed before api_sync_enabled/auto_create_enabled
    do anything -- without a domain + token, sync just reports 'not configured'."""
    store = db.query(Store).filter(Store.id == store_id).first()
    if not store:
        raise HTTPException(404, "Store not found")

    store.shopify_domain = update.shopify_domain.strip()
    store.shopify_client_id = update.shopify_client_id.strip()
    store.shopify_client_secret = update.shopify_client_secret.strip()
    # Wipe any previously cached token -- if the domain/credentials just
    # changed, the old cached token (if any) is for the wrong app/store and
    # must not be reused. A fresh one gets fetched automatically on the
    # next sync via _ensure_access_token.
    store.shopify_access_token = None
    store.shopify_token_expires_at = None
    store.api_sync_enabled = update.api_sync_enabled
    store.auto_create_enabled = update.auto_create_enabled

    # Also reset every product's saved destination IDs (dest_product_id,
    # dest_variant_id, etc.) whenever the connection is saved. Those IDs
    # point at a specific product in whatever Shopify store was connected
    # PREVIOUSLY (e.g. from testing this feature against a different/dev
    # store before). If they're left in place, the next sync skips the SKU
    # search entirely (see shopify_sync._match_or_update_one) and tries to
    # push straight to those old, now-nonexistent product IDs -- which is
    # exactly "Product does not exist" on every single row, even on a
    # brand new connection's very first sync. Clearing them forces a fresh
    # SKU lookup against whichever store is connected now.
    db.query(Product).filter(Product.store_id == store_id).update({
        Product.dest_product_id: None,
        Product.dest_variant_id: None,
        Product.dest_inventory_item_id: None,
        Product.sync_status: None,
        Product.last_synced_at: None,
    })

    db.commit()
    db.refresh(store)
    return store


@app.post("/stores/{store_id}/sync")
def trigger_sync(store_id: int, background_tasks: BackgroundTasks, limit: int | None = None, db: Session = Depends(get_db)):
    """Manually runs a sync pass right now, rather than waiting for the
    next scheduled scrape+sync cycle -- useful right after configuring the
    connection, to confirm it's actually working before walking away.

    Pass ?limit=N to only sync the first N products -- meant for a safe
    first test (especially with auto-create on) rather than immediately
    running the whole catalog against a freshly-connected store.

    Runs as a background task rather than blocking the request, same as
    /scrape/{store_id} -- a full catalog sync is hundreds of sequential
    Shopify API calls and can take minutes, which isn't safe to hold a
    single HTTP request open for (a dropped connection partway through
    would leave the frontend hanging with no way to know what happened).
    The frontend polls store.last_sync_at/last_sync_status instead."""
    store = db.query(Store).filter(Store.id == store_id).first()
    if not store:
        raise HTTPException(404, "Store not found")
    reason = _busy_reason(store_id)
    if reason:
        raise HTTPException(409, reason)
    background_tasks.add_task(lambda: asyncio.run(shopify_sync.sync_store_standalone(store_id, limit=limit)))
    return {"status": "sync_started", "store_id": store_id, "limit": limit}


@app.get("/stores/{store_id}/sync/progress")
def sync_progress(store_id: int):
    """Live status of an in-flight (or most recently finished) sync --
    which product it's currently on, how many of the total it's gotten
    through, and running counts. Meant to be polled every second or two
    while a sync is running so the dashboard can show real progress
    instead of a blind spinner. Returns {"status": "idle"} if no sync has
    run since the server started."""
    return shopify_sync.get_progress(store_id)


# ---------- Scraping ----------

@app.get("/stores/{store_id}/schedule/status")
def scheduler_status(store_id: int):
    """Per-store schedule status -- replaces the old single global one,
    since every store can now be on its own independent schedule."""
    return get_scheduler_status(store_id)

async def run_scrape_for_store(store_id: int, only_handle: str | None = None):
    """Runs a full scrape + ingest for one store, or just one collection
    within it if only_handle is given. Used by the manual trigger endpoint,
    the per-collection scrape endpoint, and the background scheduler."""
    db = next(get_db())
    try:
        store = db.query(Store).filter(Store.id == store_id).first()
        if not store:
            return
        query = db.query(Collection).filter(
            Collection.store_id == store_id, Collection.active == True  # noqa: E712
        )
        if only_handle:
            query = query.filter(Collection.handle == only_handle.strip().lower())
        collections = query.all()
        handles = [c.handle for c in collections]
        if not handles:
            return

        # Refresh currency + FX rate alongside every scrape rather than only
        # once at store creation -- rates drift over time, and a store's
        # currency itself could theoretically change. Detection failing
        # (site down, unusual store setup) just leaves the store's last
        # known values in place rather than wiping them.
        settings = _get_or_create_settings(db)
        detected_currency, fx_rate = await refresh_store_currency(store.base_url, settings.target_currency)
        if detected_currency:
            store.currency = detected_currency
        if fx_rate is not None:
            store.fx_rate_to_target = fx_rate
            store.fx_rate_updated_at = datetime.now(timezone.utc)
        db.commit()

        results = await scrape_store(store.base_url, handles, store_id=store_id)
        summary = ingest_results(db, store_id, results)
        logging.info(f"Scrape complete for store {store_id} ({store.name}): {summary}")

        # If the user hit Stop partway through, honor that all the way --
        # don't kick off a sync afterwards using a deliberately incomplete
        # scrape. Whatever was scraped before the stop is still ingested
        # above (nothing already fetched is thrown away), just no auto-sync
        # on the back of it this cycle.
        was_cancelled = get_scrape_progress(store_id).get("status") == "cancelled"

        # Push updates into the client's own Shopify store right after
        # ingesting fresh data, so sync runs on the same 6-hour cadence as
        # scraping itself rather than needing a separate schedule.
        if store.api_sync_enabled and not was_cancelled:
            try:
                sync_summary = await shopify_sync.sync_store(db, store_id)
                logging.info(f"Sync complete for store {store_id} ({store.name}): {sync_summary}")
            except Exception as e:
                logging.error(f"Sync failed for store {store_id} ({store.name}): {e}")

        return summary
    finally:
        db.close()


async def run_scrape_for_store_sequential(store_id: int) -> dict:
    """The scheduled-run counterpart to run_scrape_for_store above, used
    only by scheduler.py's per-store cron jobs (manual "Scrape Now"/"Test
    Sync" from the dashboard still go through the regular concurrent
    run_scrape_for_store, which stays fast for a user waiting on it).

    Scrapes and, if sync is enabled, syncs ONE collection at a time,
    sequentially -- so collection A's price/stock updates go out to
    Shopify as soon as A finishes, rather than every collection in the
    store needing to finish scraping before any of them get synced. Slower
    wall-clock time for the whole store, but much lower latency on any
    individual collection's updates, and a slow/large collection can't
    hold up an already-finished small one.
    """
    db = next(get_db())
    try:
        store = db.query(Store).filter(Store.id == store_id).first()
        if not store:
            return {}
        collections = (
            db.query(Collection)
            .filter(Collection.store_id == store_id, Collection.active == True)  # noqa: E712
            .all()
        )
        if not collections:
            return {}

        settings = _get_or_create_settings(db)
        detected_currency, fx_rate = await refresh_store_currency(store.base_url, settings.target_currency)
        if detected_currency:
            store.currency = detected_currency
        if fx_rate is not None:
            store.fx_rate_to_target = fx_rate
            store.fx_rate_updated_at = datetime.now(timezone.utc)
        db.commit()

        totals = {"new": 0, "updated": 0, "unchanged": 0, "errors": 0}
        for coll in collections:
            results = await scrape_store(store.base_url, [coll.handle], store_id=store_id)
            summary = ingest_results(db, store_id, results)
            for key in totals:
                totals[key] += summary.get(key, 0)

            was_cancelled = get_scrape_progress(store_id).get("status") == "cancelled"
            if was_cancelled:
                logging.info(f"Sequential run for store {store_id} stopped early (cancelled) at collection {coll.handle}")
                break

            if store.api_sync_enabled:
                try:
                    sync_summary = await shopify_sync.sync_store(db, store_id, collection_handle=coll.handle)
                    logging.info(
                        f"Sequential sync complete for store {store_id} ({store.name}), "
                        f"collection {coll.handle}: {sync_summary}"
                    )
                except Exception as e:
                    logging.error(f"Sequential sync failed for store {store_id}, collection {coll.handle}: {e}")

        logging.info(f"Sequential run complete for store {store_id} ({store.name}): {totals}")
        return totals
    finally:
        db.close()


@app.post("/scrape/{store_id}")
def trigger_scrape(store_id: int, background_tasks: BackgroundTasks, collection: str | None = None, db: Session = Depends(get_db)):
    store = db.query(Store).filter(Store.id == store_id).first()
    if not store:
        raise HTTPException(404, "Store not found")
    reason = _busy_reason(store_id)
    if reason:
        raise HTTPException(409, reason)

    background_tasks.add_task(lambda: asyncio.run(run_scrape_for_store(store_id, only_handle=collection)))
    return {"status": "scrape_started", "store_id": store_id, "collection": collection}


@app.get("/scrape/{store_id}/progress")
def scrape_progress(store_id: int):
    """Live status of an in-flight (or most recently finished) scrape --
    which collection it's on, how many of the total are done, and a
    running count of products found so far. Polled by the dashboard so it
    can show real progress instead of a blind spinner while a scrape runs.
    Returns {"status": "idle"} if no scrape has run since the server started."""
    return get_scrape_progress(store_id)


@app.post("/scrape/{store_id}/stop")
def stop_scrape(store_id: int, db: Session = Depends(get_db)):
    """Requests that an in-flight scrape stop at the next safe point
    (between pages of a collection) rather than running to completion.
    Whatever was already scraped before stopping is kept -- this isn't a
    rollback, just an early exit. No-op if nothing is currently running."""
    store = db.query(Store).filter(Store.id == store_id).first()
    if not store:
        raise HTTPException(404, "Store not found")
    request_scrape_cancel(store_id)
    return {"status": "stop_requested", "store_id": store_id}


# ---------- Product data ----------

@app.get("/products")
def list_products(store_id: int | None = None, collection: str | None = None, available_only: bool = False, db: Session = Depends(get_db)):
    query = db.query(Product)
    if store_id is not None:
        query = query.filter(Product.store_id == store_id)
    if collection:
        query = query.filter(Product.collection_handle == collection.strip().lower())
    if available_only:
        query = query.filter(Product.available == True)  # noqa: E712
    return query.order_by(Product.last_seen.desc()).limit(500).all()


@app.get("/products/{product_id}/history")
def product_history(product_id: int, db: Session = Depends(get_db)):
    product = db.query(Product).filter(Product.id == product_id).first()
    if not product:
        raise HTTPException(404, "Product not found")
    history = (
        db.query(PriceHistory)
        .filter(PriceHistory.product_id == product_id)
        .order_by(PriceHistory.recorded_at.asc())
        .all()
    )
    return {"product": product, "history": history}


@app.get("/alerts")
def recent_alerts(hours: int = 24, limit: int = 200, db: Session = Depends(get_db)):
    """Recent price drops / restocks — the actual notification feed.
    Capped at `limit` (most recent first) so a busy 24h window across many
    stores doesn't ship an ever-growing payload to the dashboard -- it's a
    live feed, not an archive; older entries are still in price_history in
    the database if ever needed, just not sent here by default."""
    from datetime import timedelta
    cutoff = datetime.now(timezone.utc) - timedelta(hours=hours)
    return (
        db.query(PriceHistory)
        .filter(
            PriceHistory.recorded_at >= cutoff,
            PriceHistory.change_type.in_(["price_drop", "restocked"]),
        )
        .order_by(PriceHistory.recorded_at.desc())
        .limit(limit)
        .all()
    )


# ---------- CSV export (Shopify import format) ----------

@app.get("/export/{store_id}/csv")
def export_csv(
    store_id: int,
    collection: str | None = None,
    available_only: bool = False,
    sized_only: bool = False,
    db: Session = Depends(get_db),
):
    """
    Download a Shopify-import-ready CSV of tracked products.

    Optional filters:
    - collection: only include products from one collection handle
    - available_only: skip out-of-stock items
    - sized_only: skip unstitched/fabric-only listings, keeping only
      products that have a real size variant (ready-to-wear/stitched)
    """
    store = db.query(Store).filter(Store.id == store_id).first()
    if not store:
        raise HTTPException(404, "Store not found")

    # Same guard as the live sync (shopify_sync.sync_store) -- refuse
    # rather than silently exporting raw source-currency numbers as if
    # they were already in the target currency. See that function's
    # comment for the full "thousands of dollars" bug this prevents.
    settings = _get_or_create_settings(db)
    if store.currency and store.currency != settings.target_currency and not store.fx_rate_to_target:
        raise HTTPException(
            409,
            f"Exchange rate for {store.currency} -> {settings.target_currency} hasn't resolved "
            f"yet for this store -- exporting now would show raw {store.currency} numbers as if "
            f"they were {settings.target_currency}. Try Scrape Now to retry detection.",
        )

    query = db.query(Product).filter(Product.store_id == store_id)
    if collection:
        query = query.filter(Product.collection_handle == collection.strip().lower())
    if available_only:
        query = query.filter(Product.available == True)  # noqa: E712

    products = query.all()
    if sized_only:
        products = filter_sized_only(products)
    if not products:
        raise HTTPException(404, "No products found for this store/filter. Run a scrape first.")

    csv_text = build_shopify_csv(products, fx_rate=store.fx_rate_to_target or 1.0, margin_percent=store.margin_percent or 0.0)
    filename = f"{store.name.replace(' ', '_')}_shopify_import.csv"

    return StreamingResponse(
        iter([csv_text]),
        media_type="text/csv",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


# ---------- Dashboard ----------

app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")


@app.get("/")
def dashboard():
    return FileResponse(str(STATIC_DIR / "dashboard.html"))