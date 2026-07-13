"""
Takes raw scrape results and reconciles them against the database:
- New variant -> insert Product + an "initial" PriceHistory row.
- Existing variant, price/availability unchanged -> just bump last_seen.
- Existing variant, price or availability changed -> update Product AND
  write a PriceHistory row describing the change. This history is the
  whole point of a tracker vs. a one-off CSV export.
"""
from datetime import datetime, timezone
from sqlalchemy.orm import Session

from .models import Collection, Product, PriceHistory
from .scraper import ScrapeResult


def classify_change(old_price: float, old_available: bool, new_price: float, new_available: bool) -> str | None:
    if old_available and not new_available:
        return "out_of_stock"
    if not old_available and new_available:
        return "restocked"
    if new_price < old_price:
        return "price_drop"
    if new_price > old_price:
        return "price_increase"
    return None


def ingest_results(db: Session, store_id: int, results: list[ScrapeResult]) -> dict:
    summary = {"new": 0, "updated": 0, "unchanged": 0, "errors": 0, "collections": []}
    now = datetime.now(timezone.utc)

    for result in results:
        collection = (
            db.query(Collection)
            .filter(Collection.store_id == store_id, Collection.handle == result.collection_handle)
            .first()
        )
        if collection:
            collection.last_scraped_at = now
            collection.last_scrape_status = result.status

        summary["collections"].append({
            "handle": result.collection_handle,
            "status": result.status,
            "product_count": len(result.products),
            "error": result.error,
        })

        if result.status == "error":
            summary["errors"] += 1
            continue

        for row in result.products:
            if not row["variant_id"]:
                continue

            existing = (
                db.query(Product)
                .filter(Product.store_id == store_id, Product.variant_id == row["variant_id"])
                .first()
            )

            if existing is None:
                new_product = Product(
                    store_id=store_id,
                    collection_handle=row["collection_handle"],
                    shopify_product_id=row["shopify_product_id"],
                    variant_id=row["variant_id"],
                    handle=row["handle"],
                    product_url=row["product_url"],
                    title=row["title"],
                    vendor=row["vendor"],
                    body_html=row["body_html"],
                    tags=row["tags"],
                    sku=row["sku"],
                    image_url=row["image_url"],
                    price=row["price"],
                    available=row["available"],
                    inventory_qty=row["inventory_qty"],
                    option1_name=row["option1_name"], option1_value=row["option1_value"],
                    option2_name=row["option2_name"], option2_value=row["option2_value"],
                    option3_name=row["option3_name"], option3_value=row["option3_value"],
                    first_seen=now,
                    last_seen=now,
                )
                db.add(new_product)
                db.flush()  # get new_product.id
                db.add(PriceHistory(
                    product_id=new_product.id,
                    price=row["price"],
                    available=row["available"],
                    change_type="initial",
                    recorded_at=now,
                ))
                summary["new"] += 1
            else:
                change = classify_change(existing.price, existing.available, row["price"], row["available"])
                existing.last_seen = now
                # Descriptive fields can change (a store edits a title, adds
                # tags, swaps the main image) without it being a "price
                # change" worth logging in history — just keep these current.
                existing.title = row["title"]
                existing.product_url = row["product_url"]
                existing.body_html = row["body_html"]
                existing.tags = row["tags"]
                existing.image_url = row["image_url"]
                existing.inventory_qty = row["inventory_qty"]
                if change:
                    existing.price = row["price"]
                    existing.available = row["available"]
                    db.add(PriceHistory(
                        product_id=existing.id,
                        price=row["price"],
                        available=row["available"],
                        change_type=change,
                        recorded_at=now,
                    ))
                    summary["updated"] += 1
                else:
                    summary["unchanged"] += 1

    db.commit()
    return summary
