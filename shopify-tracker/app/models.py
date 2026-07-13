from datetime import datetime, timezone
from sqlalchemy import (
    Column, Integer, String, Float, Boolean, DateTime, ForeignKey, UniqueConstraint
)
from sqlalchemy.orm import relationship
from .database import Base


def utcnow():
    return datetime.now(timezone.utc)


class Store(Base):
    __tablename__ = "stores"

    id = Column(Integer, primary_key=True, index=True)
    name = Column(String, nullable=False)
    base_url = Column(String, nullable=False, unique=True)
    currency = Column(String, nullable=False, default="USD")  # e.g. "USD", "AED", "PKR" - you set this per store since Shopify's public JSON doesn't expose it
    created_at = Column(DateTime, default=utcnow)

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
    body_html = Column(String, nullable=True)
    tags = Column(String, nullable=True)
    sku = Column(String, nullable=True)
    image_url = Column(String, nullable=True)

    price = Column(Float, nullable=False)
    available = Column(Boolean, default=False)
    inventory_qty = Column(Integer, nullable=True)  # None if the store doesn't expose it publicly

    option1_name = Column(String, nullable=True)
    option1_value = Column(String, nullable=True)
    option2_name = Column(String, nullable=True)
    option2_value = Column(String, nullable=True)
    option3_name = Column(String, nullable=True)
    option3_value = Column(String, nullable=True)

    first_seen = Column(DateTime, default=utcnow)
    last_seen = Column(DateTime, default=utcnow)

    store = relationship("Store", back_populates="products")

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

    price = Column(Float, nullable=False)
    available = Column(Boolean, nullable=False)
    change_type = Column(String, nullable=False)  # "initial", "price_drop", "price_increase", "restocked", "out_of_stock"
    recorded_at = Column(DateTime, default=utcnow)
