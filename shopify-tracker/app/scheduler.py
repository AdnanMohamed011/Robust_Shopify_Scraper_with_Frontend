"""
Runs each store's scrape+sync automatically once a day, at its OWN time,
rather than one shared interval for every store. Each store has its own
single clock time (Store.scrape_times, e.g. "10:00", UTC) -- kept as a
comma-splittable string for backward compatibility with older multi-time
values, but main.py's update_schedule only ever writes one entry now
(once-daily only, by design -- see its docstring). A store with no
scrape_times set has no automatic schedule at all (manual Scrape Now /
Sync Now only).
"""
import logging
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger

logger = logging.getLogger("scheduler")

# Pinned explicitly rather than left to APScheduler's default (the
# server's local system timezone via tzlocal) -- Railway containers
# usually default to UTC anyway, but "usually" isn't a guarantee, and the
# dashboard tells users these times are UTC. Without pinning this, a
# schedule that looked correct in testing could silently run at the wrong
# time (or, worse, look like it never ran at all) the moment the
# underlying host's timezone assumption changed.
_scheduler = BackgroundScheduler(timezone="UTC")
_started = False


def _job_id(store_id: int, index: int) -> str:
    return f"store_{store_id}_time_{index}"


def parse_scrape_times(scrape_times: str | None) -> list[tuple[int, int]]:
    """Turns "10:00,22:00" into [(10, 0), (22, 0)], skipping/logging any
    entry that isn't valid HH:MM rather than raising -- one bad entry
    shouldn't take a store's whole schedule down. Empty/None -> []."""
    parsed = []
    for raw in (scrape_times or "").split(","):
        raw = raw.strip()
        if not raw:
            continue
        try:
            hour_str, minute_str = raw.split(":")
            hour, minute = int(hour_str), int(minute_str)
            if not (0 <= hour <= 23 and 0 <= minute <= 59):
                raise ValueError
            parsed.append((hour, minute))
        except ValueError:
            logger.warning(f"Skipping invalid scrape_times entry: {raw!r}")
    return parsed


def _run_store_scheduled(store_id: int):
    # Imported here to avoid a circular import with main.py at module load
    # time. run_scrape_for_store_sequential (not the plain/concurrent
    # run_scrape_for_store used by the manual "Scrape Now" button) scrapes
    # and syncs one collection at a time, so a collection's updates go out
    # to Shopify as soon as that collection is done, not after every other
    # collection in the store finishes scraping too.
    import asyncio
    from .main import run_scrape_for_store_sequential

    try:
        asyncio.run(run_scrape_for_store_sequential(store_id))
    except Exception as e:
        logger.error(f"Scheduled run failed for store {store_id}: {e}")


def rebuild_store_jobs(store_id: int, scrape_times: str | None) -> None:
    """(Re)creates this store's scheduled jobs from scratch based on its
    current scrape_times -- removes whatever jobs it had before (if any)
    first, so this is safe to call both the first time a store gets a
    schedule and every time it's edited afterward. Call this any time
    Store.scrape_times changes (see main.py's store-schedule endpoint),
    not just at startup, so an edit takes effect immediately rather than
    needing a redeploy."""
    # Remove this store's existing jobs (however many there were) before
    # adding the new set -- covers a store's time count changing (e.g.
    # going from 2 times to 1) as well as the times themselves changing.
    for job in _scheduler.get_jobs():
        if job.id.startswith(f"store_{store_id}_time_"):
            _scheduler.remove_job(job.id)

    for index, (hour, minute) in enumerate(parse_scrape_times(scrape_times)):
        _scheduler.add_job(
            _run_store_scheduled,
            CronTrigger(hour=hour, minute=minute, timezone="UTC"),
            args=[store_id],
            id=_job_id(store_id, index),
            replace_existing=True,
        )
    logger.info(f"Rebuilt schedule for store {store_id}: {scrape_times or '(none)'}")


def start_scheduler(stores: list[tuple[int, str | None]]) -> None:
    """Starts the scheduler (once) and registers every store's jobs from
    its stored scrape_times. stores is a list of (store_id, scrape_times)
    pairs -- passed in from main.py's startup handler rather than this
    module querying the DB itself, same reasoning as before: keeps
    scheduler.py from needing its own DB session at import time."""
    global _started
    if _started:
        return
    _scheduler.start()
    _started = True
    for store_id, scrape_times in stores:
        rebuild_store_jobs(store_id, scrape_times)
    logger.info(f"Scheduler started for {len(stores)} store(s)")


def remove_store_jobs(store_id: int) -> None:
    """Called when a store is deleted, so its jobs don't keep firing
    against a store_id that no longer exists."""
    for job in _scheduler.get_jobs():
        if job.id.startswith(f"store_{store_id}_time_"):
            _scheduler.remove_job(job.id)


def get_status(store_id: int) -> dict:
    """Used by the dashboard to show this store's own schedule status --
    replaces the old single global status, since every store can now be
    on a different schedule."""
    jobs = [j for j in _scheduler.get_jobs() if j.id.startswith(f"store_{store_id}_time_")]
    next_runs = sorted(j.next_run_time for j in jobs if j.next_run_time)
    return {
        "active": _started and len(jobs) > 0,
        "scheduled_times": len(jobs),
        "next_run_at": next_runs[0].isoformat() if next_runs else None,
    }