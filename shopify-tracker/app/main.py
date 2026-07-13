import asyncio
import logging
from datetime import datetime, timezone

from fastapi import FastAPI, Depends, HTTPException, BackgroundTasks
from fastapi.responses import FileResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel
from sqlalchemy.orm import Session

from .database import Base, engine, get_db
from .models import Store, Collection, Product, PriceHistory
from .scraper import scrape_store
from .ingest import ingest_results
from .scheduler import start_scheduler
from .export import build_shopify_csv

logging.basicConfig(level=logging.INFO)

Base.metadata.create_all(bind=engine)

app = FastAPI(title="Product Tracker API")


@app.on_event("startup")
def on_startup():
    start_scheduler()


# ---------- Schemas ----------

class StoreCreate(BaseModel):
    name: str
    base_url: str
    currency: str = "USD"


class CollectionCreate(BaseModel):
    handle: str


# ---------- Store & collection management ----------

@app.post("/stores")
def create_store(store: StoreCreate, db: Session = Depends(get_db)):
    base_url = store.base_url.rstrip("/")
    existing = db.query(Store).filter(Store.base_url == base_url).first()
    if existing:
        raise HTTPException(400, "Store already exists")
    new_store = Store(name=store.name, base_url=base_url, currency=store.currency)
    db.add(new_store)
    db.commit()
    db.refresh(new_store)
    return new_store


@app.get("/stores")
def list_stores(db: Session = Depends(get_db)):
    return db.query(Store).all()


@app.post("/stores/{store_id}/collections")
def add_collection(store_id: int, collection: CollectionCreate, db: Session = Depends(get_db)):
    store = db.query(Store).filter(Store.id == store_id).first()
    if not store:
        raise HTTPException(404, "Store not found")

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


# ---------- Scraping ----------

async def run_scrape_for_store(store_id: int):
    """Runs a full scrape + ingest for one store. Used by both the manual
    trigger endpoint and the background scheduler."""
    db = next(get_db())
    try:
        store = db.query(Store).filter(Store.id == store_id).first()
        if not store:
            return
        collections = db.query(Collection).filter(
            Collection.store_id == store_id, Collection.active == True  # noqa: E712
        ).all()
        handles = [c.handle for c in collections]
        if not handles:
            return

        results = await scrape_store(store.base_url, handles)
        summary = ingest_results(db, store_id, results)
        logging.info(f"Scrape complete for store {store_id} ({store.name}): {summary}")
        return summary
    finally:
        db.close()


@app.post("/scrape/{store_id}")
def trigger_scrape(store_id: int, background_tasks: BackgroundTasks, db: Session = Depends(get_db)):
    store = db.query(Store).filter(Store.id == store_id).first()
    if not store:
        raise HTTPException(404, "Store not found")

    background_tasks.add_task(lambda: asyncio.run(run_scrape_for_store(store_id)))
    return {"status": "scrape_started", "store_id": store_id}


# ---------- Product data ----------

@app.get("/products")
def list_products(store_id: int | None = None, available_only: bool = False, db: Session = Depends(get_db)):
    query = db.query(Product)
    if store_id is not None:
        query = query.filter(Product.store_id == store_id)
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
def recent_alerts(hours: int = 24, db: Session = Depends(get_db)):
    """Recent price drops / restocks — the actual notification feed."""
    from datetime import timedelta
    cutoff = datetime.now(timezone.utc) - timedelta(hours=hours)
    return (
        db.query(PriceHistory)
        .filter(
            PriceHistory.recorded_at >= cutoff,
            PriceHistory.change_type.in_(["price_drop", "restocked"]),
        )
        .order_by(PriceHistory.recorded_at.desc())
        .all()
    )


# ---------- CSV export (Shopify import format) ----------

@app.get("/export/{store_id}/csv")
def export_csv(store_id: int, collection: str | None = None, available_only: bool = False, db: Session = Depends(get_db)):
    """
    Download a Shopify-import-ready CSV of tracked products.

    Optional filters:
    - collection: only include products from one collection handle
    - available_only: skip out-of-stock items
    """
    store = db.query(Store).filter(Store.id == store_id).first()
    if not store:
        raise HTTPException(404, "Store not found")

    query = db.query(Product).filter(Product.store_id == store_id)
    if collection:
        query = query.filter(Product.collection_handle == collection.strip().lower())
    if available_only:
        query = query.filter(Product.available == True)  # noqa: E712

    products = query.all()
    if not products:
        raise HTTPException(404, "No products found for this store/filter. Run a scrape first.")

    csv_text = build_shopify_csv(products)
    filename = f"{store.name.replace(' ', '_')}_shopify_import.csv"

    return StreamingResponse(
        iter([csv_text]),
        media_type="text/csv",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


# ---------- Dashboard ----------

app.mount("/static", StaticFiles(directory="static"), name="static")


@app.get("/")
def dashboard():
    return FileResponse("static/dashboard.html")
