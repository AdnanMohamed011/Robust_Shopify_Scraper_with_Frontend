"""
One-time fix for a real gap in the SQLite -> Postgres migration: copying
rows over with their original IDs preserved (needed so foreign keys still
line up) does NOT update Postgres's internal "what ID comes next"
sequence counter for each table. That counter was left wherever it was
before the migration (low), while the actual data now goes much higher --
so the very next INSERT Postgres tries to auto-assign an ID for collides
with a row that already exists. That's the exact "duplicate key value
violates unique constraint" crash you hit when adding a new store.

This resets every table's sequence to (current max id) + 1, so new rows
get assigned IDs that don't collide with anything already migrated. Safe
to run more than once -- it's idempotent, just recalculates the same
correct value each time.

Usage:
    python fix_sequences.py "postgresql://your-connection-string"
"""
import sys

from sqlalchemy import create_engine, text

TABLES = ["app_settings", "stores", "collections", "products", "price_history"]


def fix_sequences(postgres_url: str) -> None:
    engine = create_engine(postgres_url)
    with engine.begin() as conn:
        for table in TABLES:
            result = conn.execute(text(
                f"SELECT setval("
                f"  pg_get_serial_sequence('{table}', 'id'), "
                f"  COALESCE((SELECT MAX(id) FROM {table}), 1)"
                f")"
            ))
            new_value = result.scalar()
            print(f"{table}: sequence reset to {new_value}")
    print("\nDone -- new rows in any table will now get IDs above what's already there.")


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python fix_sequences.py \"<postgres_connection_url>\"")
        sys.exit(1)
    fix_sequences(sys.argv[1])