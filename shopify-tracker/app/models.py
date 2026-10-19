from datetime import datetime, timezone
from sqlalchemy import (
    Column, Integer, String, Float, Boolean, DateTime, ForeignKey, UniqueConstraint
)
from sqlalchemy.orm import relationship
from .database import Base


def utcnow():
    return datetime.now(timezone.utc)


class AppSettings(Base):
    """Single-row table (id is always 1) for app-wide settings that don't
    belong to any one store -- currently just the currency your own
    Shopify store sells in, which every tracked store's prices get
    converted into before export/sync."""
    __tablename__ = "app_settings"

    id = Column(Integer, primary_key=True, default=1)
    target_currency = Column(String, nullable=False, default="USD")


class Store(Base):
    __tablename__ = "stores"

    id = Column(Integer, primary_key=True, index=True)
    name = Column(String, nullable=False)
    base_url = Column(String, nullable=False, unique=True)
    # Auto-detected from the store's own /cart.json on each scrape (see
    # scraper.detect_store_currency) rather than typed in manually --
    # "USD" here is just the pre-detection placeholder for a brand new store.
    currency = Column(String, nullable=False, default="USD")
    fx_rate_to_target = Column(Float, nullable=True)  # multiply a scraped price by this to get it into your store's currency
    fx_rate_updated_at = Column(DateTime, nullable=True)
    created_at = Column(DateTime, default=utcnow)

    # Flat percentage markup applied on top of the FX-converted price,
    # e.g. 15.0 turns a converted $100 into $115 before the .99 charm-price
    # rounding. 0.0 (the default) is a straight pass-through of the source
    # price, exactly like every export/sync before this field existed --
    # existing stores get 0.0 automatically when this column is added
    # (see database.run_lightweight_migrations), so nothing changes in
    # price for anyone until a margin is actually set. Editable per store,
    # any time, from the dashboard -- applies to both the CSV export and
    # the live Shopify sync, since both already funnel through the same
    # round_to_99/apply_margin pricing helpers in export.py.
    margin_percent = Column(Float, nullable=False, default=0.0)

    # Cached id of this store's dedicated destination collection (named
    # after the store itself, e.g. "Charizma") -- every product synced
    # from this source store gets added to it, which is what makes the
    # storefront's "Shop by Brand" pages (one collection per brand) show
    # anything at all. Looked up/created once via
    # shopify_client.get_or_create_collection and cached here so every
    # sync doesn't re-search for it -- same pattern as the cached access
    # token below. Null until the first sync for this store runs.
    dest_collection_id = Column(String, nullable=True)

    # --- Client's OWN Shopify store connection (the destination store
    # these tracked products get pushed INTO) -- separate from base_url
    # above, which is the SOURCE store being tracked/scraped. ---
    shopify_domain = Column(String, nullable=True)  # e.g. "yourstore.myshopify.com"
    # As of Jan 1 2026, Shopify retired the old flow where a Custom App
    # handed you a ready-to-use access token directly in the admin. New
    # apps (via the Dev Dashboard) instead give you a Client ID + Client
    # Secret, and the access token has to be requested programmatically
    # via the "client credentials grant" (see shopify_client.get_access_token)
    # -- and it expires, so it needs periodic refreshing rather than being
    # stored once and reused forever.
    shopify_client_id = Column(String, nullable=True)
    shopify_client_secret = Column(String, nullable=True)
    shopify_access_token = Column(String, nullable=True)  # cached token fetched via the client credentials grant
    shopify_token_expires_at = Column(DateTime, nullable=True)  # refresh once we're within a safety margin of this
    api_sync_enabled = Column(Boolean, default=False)  # push price/stock updates to matched products
    auto_create_enabled = Column(Boolean, default=False)  # also create brand-new products that have no match yet
    last_sync_at = Column(DateTime, nullable=True)
    last_sync_status = Column(String, nullable=True)  # "ok", "error", "not_configured"
    last_sync_summary = Column(String, nullable=True)  # short human-readable result, e.g. "12 updated, 2 created, 1 error"

    # A single 24h "HH:MM" time (UTC), e.g. "10:00" -- once daily, when
    # this store's automatic scrape+sync runs, independent of every other
    # store's own time (see scheduler.py's rebuild_store_jobs). Once per
    # day only, by design (see main.py's update_schedule) -- kept as a
    # string rather than a Time column since scheduler.py's parser
    # already works on "HH:MM" text either way. Null/empty means this
    # store has no automatic schedule at all -- manual Scrape Now / Sync
    # Now only.
    scrape_times = Column(String, nullable=True)

    collections = relationship("Collection", back_populates="store", cascade="all, delete-orphan")
    products = relationship("Product", back_populates="store", cascade="all, delete-orphan")


class Collection(Base):
    __tablename__ = "collections"

    id = Column(Integer, primary_key=True, index=True)
    store_id = Column(Integer, ForeignKey("stores.id"), nullable=False)
    handle = Column(String, nullable=False)
    active = Column(Boolean, default=True)
    last_scraped_at = Column(DateTime, nullable=True)
    last_scrape_status = Column(String, nullable=True)  # "ok", "error", "empty"

    store = relationship("Store", back_populates="collections")

    __table_args__ = (UniqueConstraint("store_id", "handle", name="uq_store_collection"),)


class Product(Base):
    """
    Current known state of a single product variant (one SKU).
    Historical changes live in PriceHistory, not here.
    """
    __tablename__ = "products"

    id = Column(Integer, primary_key=True, index=True)
    store_id = Column(Integer, ForeignKey("stores.id"), nullable=False)
    collection_handle = Column(String, nullable=False)

    shopify_product_id = Column(String, nullable=False)
    variant_id = Column(String, nullable=False)
    handle = Column(String, nullable=True)  # Shopify product handle - groups variant rows in the CSV
    product_url = Column(String, nullable=True)  # direct link to the product page on the source store

    title = Column(String, nullable=False)
    vendor = Column(String, nullable=True)
    product_type = Column(String, nullable=True)  # Shopify's "Type" field - free text, e.g. "Unstitched Suit"
    body_html = Column(String, nullable=True)
    tags = Column(String, nullable=True)
    sku = Column(String, nullable=True)
    image_url = Column(String, nullable=True)  # first/primary image, used for the dashboard thumbnail
    all_image_urls = Column(String, nullable=True)  # every image on the product, comma-separated, used for CSV export

    price = Column(Float, nullable=False)
    available = Column(Boolean, default=False)
    inventory_qty = Column(Integer, nullable=True)  # None if the store doesn't expose it publicly
    weight_grams = Column(Float, nullable=True)  # None if the store doesn't expose it publicly

    option1_name = Column(String, nullable=True)
    option1_value = Column(String, nullable=True)
    option2_name = Column(String, nullable=True)
    option2_value = Column(String, nullable=True)
    option3_name = Column(String, nullable=True)
    option3_value = Column(String, nullable=True)

    # Detected from title/description text since most source stores don't
    # expose these as real Shopify options — see scraper.py's
    # detect_colour()/detect_piece_count(). None when nothing was matched.
    detected_colour = Column(String, nullable=True)
    piece_count = Column(String, nullable=True)  # e.g. "1 Piece", "2 Piece", "3 Piece"

    first_seen = Column(DateTime, default=utcnow)
    last_seen = Column(DateTime, default=utcnow)

    # --- Sync state against the CLIENT's own Shopify store (destination),
    # set once this product has been matched (by SKU) or created there via
    # the Admin API. Kept so repeat syncs are cheap updates instead of a
    # fresh SKU search every time, and so the dashboard can show sync status. ---
    dest_product_id = Column(String, nullable=True)  # this product's id on the CLIENT's store
    dest_variant_id = Column(String, nullable=True)
    dest_inventory_item_id = Column(String, nullable=True)  # needed to push stock qty via Shopify's inventory API
    last_synced_at = Column(DateTime, nullable=True)
    sync_status = Column(String, nullable=True)  # "matched", "created", "not_found", "error", or None if sync's never run

    store = relationship("Store", back_populates="products")
    # cascade="all, delete-orphan" makes the ORM delete a product's price
    # history rows FIRST whenever the product itself is deleted (e.g. via
    # delete_store's cascade, or a direct product delete) -- without this,
    # deleting a product leaves its price_history rows pointing at a
    # product id that no longer exists. SQLite silently allows that
    # (foreign keys aren't enforced by default), which is why this never
    # showed up locally, but Postgres (used on Railway) enforces the
    # foreign key and rejects the delete outright with a 500.
    price_history = relationship("PriceHistory", back_populates="product", cascade="all, delete-orphan")

    __table_args__ = (
        UniqueConstraint("store_id", "variant_id", name="uq_store_variant"),
    )


class PriceHistory(Base):
    """
    One row is written only when a product's price or availability
    actually changes — not on every scrape. This is what makes
    'price dropped' / 'back in stock' alerts possible.
    """
    __tablename__ = "price_history"

    id = Column(Integer, primary_key=True, index=True)
    product_id = Column(Integer, ForeignKey("products.id"), nullable=False)
    product = relationship("Product", back_populates="price_history")

    price = Column(Float, nullable=False)
    available = Column(Boolean, nullable=False)
    change_type = Column(String, nullable=False)  # "initial", "price_drop", "price_increase", "restocked", "out_of_stock"
    recorded_at = Column(DateTime, default=utcnow)