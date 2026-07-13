"""
Runs scrapes automatically on a schedule instead of you manually running
a script. Default: every 6 hours, for every store in the database.
"""
import asyncio
import logging
from apscheduler.schedulers.background import BackgroundScheduler

logger = logging.getLogger("scheduler")

SCRAPE_INTERVAL_HOURS = 6

_scheduler = BackgroundScheduler()
_started = False


def _run_all_stores():
    # Imported here to avoid a circular import with main.py at module load time.
    from .database import SessionLocal
    from .models import Store
    from .main import run_scrape_for_store

    db = SessionLocal()
    try:
        store_ids = [s.id for s in db.query(Store).all()]
    finally:
        db.close()

    for store_id in store_ids:
        try:
            asyncio.run(run_scrape_for_store(store_id))
        except Exception as e:
            logger.error(f"Scheduled scrape failed for store {store_id}: {e}")


def start_scheduler():
    global _started
    if _started:
        return
    _scheduler.add_job(_run_all_stores, "interval", hours=SCRAPE_INTERVAL_HOURS, id="scrape_all")
    _scheduler.start()
    _started = True
    logger.info(f"Scheduler started: scraping every {SCRAPE_INTERVAL_HOURS}h")
