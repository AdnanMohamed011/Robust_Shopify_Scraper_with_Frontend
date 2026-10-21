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
    summary = {"new": 0, "updated": 0, "unchanged": 0, "errors": 0, "vanished": 0, "collections": []}
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

        # A blocked/unavailable collection with no products is a skipped
        # attempt, not a fatal ingest error. Partial results are still useful:
        # ingest the rows we got, but never reconcile unseen products as gone
        # unless the collection was fully fetched (ok/empty).
        if result.status in ("error", "blocked", "unavailable") and not result.products:
            if result.status == "error":
                summary["errors"] += 1
            continue

        seen_variant_ids = set()

        for row in result.products:
            if not row["variant_id"]:
                continue
            seen_variant_ids.add(row["variant_id"])

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
                    product_type=row["product_type"],
                    body_html=row["body_html"],
                    tags=row["tags"],
                    sku=row["sku"],
                    image_url=row["image_url"],
                    all_image_urls=row["all_image_urls"],
                    price=row["price"],
                    available=row["available"],
                    inventory_qty=row["inventory_qty"],
                    weight_grams=row["weight_grams"],
                    option1_name=row["option1_name"], option1_value=row["option1_value"],
                    option2_name=row["option2_name"], option2_value=row["option2_value"],
                    option3_name=row["option3_name"], option3_value=row["option3_value"],
                    detected_colour=row.get("detected_colour"),
                    piece_count=row.get("piece_count"),
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
                existing.all_image_urls = row["all_image_urls"]
                existing.inventory_qty = row["inventory_qty"]
                existing.product_type = row["product_type"]
                existing.weight_grams = row["weight_grams"]
                existing.option1_name = row["option1_name"]
                existing.option1_value = row["option1_value"]
                existing.option2_name = row["option2_name"]
                existing.option2_value = row["option2_value"]
                existing.option3_name = row["option3_name"]
                existing.option3_value = row["option3_value"]
                existing.detected_colour = row.get("detected_colour")
                existing.piece_count = row.get("piece_count")
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

        # Anything previously tracked in this collection but NOT present in
        # this scrape has genuinely disappeared from the source (permanently
        # discontinued, removed, etc) rather than just "still there,
        # unchanged". Mark it out of stock -- same as any other stock
        # change -- rather than leaving it frozen showing stale "in stock"
        # data forever. This NEVER deletes the Product row or touches
        # anything on the destination Shopify store directly: it just
        # updates available/inventory_qty here, so the next regular sync
        # naturally pushes the same "sold out" update it would for any
        # other out-of-stock item -- consistent with the tool never
        # deleting listings, only ever updating stock status.
        #
        # Only runs for a collection that scraped successfully this round
        # ("ok" or "empty", not "error") -- a failed/timed-out scrape must
        # never be read as "everything in this collection vanished", which
        # would wrongly mark a whole collection's worth of real stock as
        # sold out over a transient network hiccup.
        if result.status not in ("ok", "empty"):
            # Partial/blocked pages are not a complete inventory snapshot.
            # Preserve previously tracked stock state and retry on a later run.
            continue

        vanished_query = db.query(Product).filter(
            Product.store_id == store_id,
            Product.collection_handle == result.collection_handle,
            Product.available == True,  # noqa: E712 -- only touch rows that would actually change
        )
        if seen_variant_ids:
            vanished_query = vanished_query.filter(~Product.variant_id.in_(seen_variant_ids))
        vanished = vanished_query.all()
        for product in vanished:
            product.available = False
            product.inventory_qty = 0
            db.add(PriceHistory(
                product_id=product.id,
                price=product.price,
                available=False,
                change_type="out_of_stock",
                recorded_at=now,
            ))
            summary["vanished"] += 1

    db.commit()
    return summary