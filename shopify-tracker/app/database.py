"""
Database connection setup.

Uses SQLite for simplicity — swap the URL for a Postgres connection string
(e.g. postgresql://user:pass@host/db) when you're ready for real multi-tenant
production use. SQLAlchemy makes that a one-line change.
"""
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, declarative_base

DATABASE_URL = "sqlite:///./tracker.db"

engine = create_engine(
    DATABASE_URL, connect_args={"check_same_thread": False}
)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
