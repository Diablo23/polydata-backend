"""APScheduler setup for recurring crawler sync and analytics refresh."""

import asyncio
import logging

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.interval import IntervalTrigger

from app.config import get_settings
from app.crawler.sync import run_sync

logger = logging.getLogger(__name__)
settings = get_settings()


async def _sync_job() -> None:
    """Wrapper for the sync job so exceptions don't kill the scheduler."""
    try:
        stats = await run_sync()
        logger.info("Scheduled sync result: %s", stats)
    except Exception:
        logger.exception("Scheduled sync failed")


def create_scheduler() -> AsyncIOScheduler:
    """Create and configure the scheduler (does NOT start it)."""
    scheduler = AsyncIOScheduler()

    # Market sync — every N minutes
    scheduler.add_job(
        _sync_job,
        trigger=IntervalTrigger(minutes=settings.sync_interval_minutes),
        id="market_sync",
        name="Polymarket market sync",
        replace_existing=True,
    )

    return scheduler


async def start_scheduler() -> None:
    """Start the scheduler and run forever."""
    scheduler = create_scheduler()
    scheduler.start()
    logger.info(
        "Scheduler started — sync every %d minutes",
        settings.sync_interval_minutes,
    )
    try:
        # Keep the event loop alive
        while True:
            await asyncio.sleep(3600)
    except (KeyboardInterrupt, SystemExit):
        scheduler.shutdown()
        logger.info("Scheduler shut down")
