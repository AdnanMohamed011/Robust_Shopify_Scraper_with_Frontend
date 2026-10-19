"""
Database connection setup.

Uses SQLite by default for local development. Set the DATABASE_URL
environment variable to a Postgres connection string (e.g.
postgresql://user:pass@host/db) for production hosting -- SQLite's file
lives on local disk, which most hosts (Railway, Render, Fly.io) wipe on
every redeploy/restart since their filesystems are ephemeral. Postgres is
what actually persists your tracked products across deploys.
"""
import logging
import os
from sqlalchemy import create_engine, inspect, text
from sqlalchemy.orm import sessionmaker, declarative_base

logger = logging.getLogger("database")

DATABASE_URL = os.environ.get("DATABASE_URL", "sqlite:///./tracker.db")

# Some hosts (Railway, Heroku-style providers) still hand out URLs starting
# with "postgres://" -- SQLAlchemy 1.4+ requires the "postgresql://" scheme
# and will raise on the old form, so normalize it here rather than making
# every deploy target remember to fix it.
if DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql://", 1)

# Explicitly use the psycopg (v3) driver rather than SQLAlchemy's default of
# psycopg2 for a plain "postgresql://" URL. psycopg2-binary's wheel doesn't
# reliably bundle libpq on minimal hosting images (Railway's included) --
# raises "libpq.so.5: cannot open shared object file" at import time even
# though the package installed fine. psycopg[binary] avoids that failure
# mode entirely, hence requirements.txt uses it instead.
if DATABASE_URL.startswith("postgresql://"):
    DATABASE_URL = DATABASE_URL.replace("postgresql://", "postgresql+psycopg://", 1)

# check_same_thread is a SQLite-only connect arg -- Postgres doesn't
# recognize it and create_engine() would error if we passed it regardless
# of DB backend, so only apply it when we're actually on SQLite.
connect_args = {"check_same_thread": False} if DATABASE_URL.startswith("sqlite") else {}

engine = create_engine(DATABASE_URL, connect_args=connect_args)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


def run_lightweight_migrations():
    """
    Base.metadata.create_all() (called at import time in main.py) only
    creates tables that don't exist yet — it never adds new columns to a
    table that's already there. Since this is SQLite with no real
    migration tool (Alembic etc.), this walks each model's columns and
    ALTERs in any that are missing from the actual database file, so
    schema changes (like adding detected_colour/piece_count) don't break
    on an existing tracker.db. Safe to run on every startup: it's a no-op
    once the columns already exist.
    """
    inspector = inspect(engine)
    with engine.begin() as conn:
        for table in Base.metadata.sorted_tables:
            if not inspector.has_table(table.name):
                continue  # brand-new table, create_all() already handles it
            existing_columns = {col["name"] for col in inspector.get_columns(table.name)}
            for column in table.columns:
                if column.name in existing_columns:
                    continue
                col_type = column.type.compile(engine.dialect)
                logger.info(f"Migrating: adding column {table.name}.{column.name} ({col_type})")
                conn.execute(text(f'ALTER TABLE "{table.name}" ADD COLUMN "{column.name}" {col_type}'))