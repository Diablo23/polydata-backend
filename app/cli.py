"""CLI commands for managing the crawler and database.

Usage:
    python -m app.cli backfill          # Full historical backfill
    python -m app.cli backfill --skip-prices  # Backfill markets only, skip price history
    python -m app.cli sync              # Run one sync cycle
    python -m app.cli start-scheduler   # Start recurring sync
    python -m app.cli stats             # Print DB stats
"""

import argparse
import asyncio
import logging
import sys

from sqlalchemy import func, select

from app.database import async_session_factory
from app.models import Event, Market, PriceSnapshot, Tag


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("cli")


async def cmd_backfill(skip_prices: bool = False) -> None:
    from app.crawler.backfill import run_backfill

    logger.info("Starting full backfill%s...", " (skip prices)" if skip_prices else "")
    stats = await run_backfill(skip_prices=skip_prices)
    logger.info("Backfill complete: %s", stats)


async def cmd_sync() -> None:
    from app.crawler.sync import run_sync

    logger.info("Running one sync cycle...")
    stats = await run_sync()
    logger.info("Sync complete: %s", stats)


async def cmd_start_scheduler() -> None:
    from app.scheduler import start_scheduler

    await start_scheduler()


async def cmd_stats() -> None:
    async with async_session_factory() as session:
        total = (await session.execute(select(func.count(Market.id)))).scalar() or 0
        closed = (
            await session.execute(
                select(func.count(Market.id)).where(Market.is_closed.is_(True))
            )
        ).scalar() or 0
        active = (
            await session.execute(
                select(func.count(Market.id)).where(Market.is_active.is_(True))
            )
        ).scalar() or 0
        resolved_no = (
            await session.execute(
                select(func.count(Market.id)).where(Market.resolved_to_no.is_(True))
            )
        ).scalar() or 0
        resolved_yes = (
            await session.execute(
                select(func.count(Market.id)).where(
                    Market.resolved_to_no.is_(False),
                    Market.winning_outcome.isnot(None),
                )
            )
        ).scalar() or 0
        snapshots = (
            await session.execute(select(func.count(PriceSnapshot.id)))
        ).scalar() or 0
        events = (
            await session.execute(select(func.count(Event.id)))
        ).scalar() or 0
        tags = (
            await session.execute(select(func.count(Tag.id)))
        ).scalar() or 0
        total_vol = (
            await session.execute(
                select(func.sum(Market.volume)).where(Market.is_closed.is_(True))
            )
        ).scalar() or 0

    no_rate = (resolved_no / closed * 100) if closed > 0 else 0

    print()
    print("=" * 50)
    print("  NOTHING EVER HAPPENS — DATABASE STATS")
    print("=" * 50)
    print(f"  TOTAL MARKETS ........... {total:>10,}")
    print(f"  CLOSED (RESOLVED) ....... {closed:>10,}")
    print(f"  ACTIVE .................. {active:>10,}")
    print(f"  RESOLVED YES ............ {resolved_yes:>10,}")
    print(f"  RESOLVED NO ............. {resolved_no:>10,}")
    print(f"  NO RATE ................. {no_rate:>9.1f}%")
    print(f"  TOTAL VOLUME ............ ${total_vol:>12,.0f}")
    print(f"  PRICE SNAPSHOTS ......... {snapshots:>10,}")
    print(f"  EVENTS .................. {events:>10,}")
    print(f"  TAGS .................... {tags:>10,}")
    print("=" * 50)
    print()


def main() -> None:
    parser = argparse.ArgumentParser(
        prog="python -m app.cli",
        description="Nothing Ever Happens — CLI tools",
    )
    sub = parser.add_subparsers(dest="command")

    bf = sub.add_parser("backfill", help="Run full historical backfill")
    bf.add_argument(
        "--skip-prices",
        action="store_true",
        help="Skip price history backfill (faster)",
    )

    sub.add_parser("sync", help="Run one sync cycle")
    sub.add_parser("start-scheduler", help="Start recurring sync scheduler")
    sub.add_parser("stats", help="Print database statistics")

    args = parser.parse_args()

    if args.command is None:
        parser.print_help()
        sys.exit(1)

    if args.command == "backfill":
        asyncio.run(cmd_backfill(skip_prices=args.skip_prices))
    elif args.command == "sync":
        asyncio.run(cmd_sync())
    elif args.command == "start-scheduler":
        asyncio.run(cmd_start_scheduler())
    elif args.command == "stats":
        asyncio.run(cmd_stats())


if __name__ == "__main__":
    main()
