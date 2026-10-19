"""
One-time migration: copies everything from your local tracker.db (SQLite)
into a Postgres database (e.g. Railway's), preserving all rows exactly as
they are -- including Shopify sync state (dest_product_id, sync_status,
etc.) so nothing has to be re-matched/re-created on the live store.

Run this from your PC (same folder as your other project files), AFTER:
  1. You've added a PostgreSQL database in Railway
  2. You've copied its connection string (Railway's Postgres service ->
     "Connect" tab -> the "Postgres Connection URL", NOT the internal one --
     you need the public one since this runs from your PC, not from inside
     Railway's network)

Usage:
    pip install psycopg2-binary --break-system-packages
    python migrate_to_postgres.py "postgresql://user:pass@host:port/railway"

This is safe to re-run: it skips any row that already exists in Postgres
(matched by primary key), so running it twice won't create duplicates or
crash on a second attempt.
"""
import sys

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

# Import your actual models so the exact same table structure is used on
# both sides -- avoids any risk of the destination schema drifting from
# what your app actually expects.
sys.path.insert(0, ".")
from app.models import Base, AppSettings, Store, Collection, Product, PriceHistory  # noqa: E402

# Order matters: parents before children, so foreign keys never point at a
# row that doesn't exist yet on the Postgres side.
TABLES_IN_ORDER = [AppSettings, Store, Collection, Product, PriceHistory]


def migrate(sqlite_path: str, postgres_url: str) -> None:
    sqlite_engine = create_engine(f"sqlite:///{sqlite_path}")
    postgres_engine = create_engine(postgres_url)

    # Creates any missing tables on the Postgres side -- safe/no-op if
    # they're already there (same pattern as run_lightweight_migrations
    # uses for SQLite).
    Base.metadata.create_all(bind=postgres_engine)

    SqliteSession = sessionmaker(bind=sqlite_engine)
    PostgresSession = sessionmaker(bind=postgres_engine)

    sqlite_db = SqliteSession()
    postgres_db = PostgresSession()

    try:
        for model in TABLES_IN_ORDER:
            rows = sqlite_db.query(model).all()
            copied = 0
            skipped = 0
            for row in rows:
                exists = postgres_db.get(model, row.id)
                if exists:
                    skipped += 1
                    continue
                # Copy every column's value across, preserving the original
                # primary key so foreign keys in child tables still resolve
                # correctly (Product.store_id, PriceHistory.product_id, etc).
                data = {
                    col.name: getattr(row, col.name)
                    for col in model.__table__.columns
                }
                postgres_db.add(model(**data))
                copied += 1
            postgres_db.commit()
            print(f"{model.__tablename__}: copied {copied}, skipped {skipped} (already present)")

        print("\nMigration complete.")
    finally:
        sqlite_db.close()
        postgres_db.close()


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python migrate_to_postgres.py \"<postgres_connection_url>\"")
        sys.exit(1)

    migrate(sqlite_path="tracker.db", postgres_url=sys.argv[1])