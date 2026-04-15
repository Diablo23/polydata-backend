"""Historical backfill — crawl all resolved markets and their price histories."""

import asyncio
import logging
from typing import Any

import httpx
from sqlalchemy import select, text
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import get_settings
from app.crawler import clob, gamma
from app.database import async_session_factory
from app.models import Event, Market, PriceSnapshot, Tag

logger = logging.getLogger(__name__)
settings = get_settings()

# Concurrency limiter for price history fetches
PRICE_SEMAPHORE = asyncio.Semaphore(5)


def _market_upsert_values(m: dict[str, Any]) -> dict[str, Any]:
    """Extract columns for an upsert from a parsed market dict."""
    return {
        "id": m["id"],
        "question": m["question"],
        "condition_id": m.get("condition_id"),
        "slug": m.get("slug"),
        "outcomes": m.get("outcomes"),
        "outcome_prices": m.get("outcome_prices"),
        "volume": m.get("volume", 0),
        "liquidity": m.get("liquidity", 0),
        "start_date": m.get("start_date"),
        "end_date": m.get("end_date"),
        "closed_time": m.get("closed_time"),
        "category": m.get("category"),
        "resolution_source": m.get("resolution_source"),
        "description": m.get("description"),
        "is_active": m.get("is_active", False),
        "is_closed": m.get("is_closed", False),
        "is_archived": m.get("is_archived", False),
        "clob_token_ids": m.get("clob_token_ids"),
        "event_id": m.get("event_id"),
        "winning_outcome": m.get("winning_outcome"),
        "resolved_to_no": m.get("resolved_to_no"),
        "raw_data": m.get("raw_data"),
    }


async def upsert_markets(
    session: AsyncSession, markets: list[dict[str, Any]]
) -> int:
    """Bulk upsert markets into DB. Returns count inserted/updated."""
    if not markets:
        return 0

    values = [_market_upsert_values(m) for m in markets]
    stmt = pg_insert(Market).values(values)
    stmt = stmt.on_conflict_do_update(
        index_elements=["id"],
        set_={
            "question": stmt.excluded.question,
            "condition_id": stmt.excluded.condition_id,
            "slug": stmt.excluded.slug,
            "outcomes": stmt.excluded.outcomes,
            "outcome_prices": stmt.excluded.outcome_prices,
            "volume": stmt.excluded.volume,
            "liquidity": stmt.excluded.liquidity,
            "start_date": stmt.excluded.start_date,
            "end_date": stmt.excluded.end_date,
            "closed_time": stmt.excluded.closed_time,
            "category": stmt.excluded.category,
            "resolution_source": stmt.excluded.resolution_source,
            "description": stmt.excluded.description,
            "is_active": stmt.excluded.is_active,
            "is_closed": stmt.excluded.is_closed,
            "is_archived": stmt.excluded.is_archived,
            "clob_token_ids": stmt.excluded.clob_token_ids,
            "event_id": stmt.excluded.event_id,
            "winning_outcome": stmt.excluded.winning_outcome,
            "resolved_to_no": stmt.excluded.resolved_to_no,
            "raw_data": stmt.excluded.raw_data,
            "updated_at": text("NOW()"),
        },
    )
    await session.execute(stmt)
    return len(values)


async def upsert_events(
    session: AsyncSession, events: list[dict[str, Any]]
) -> int:
    """Bulk upsert events."""
    if not events:
        return 0

    values = [
        {
            "id": e["id"],
            "title": e.get("title"),
            "slug": e.get("slug"),
            "description": e.get("description"),
            "is_active": e.get("is_active", False),
            "is_closed": e.get("is_closed", False),
            "start_date": e.get("start_date"),
            "end_date": e.get("end_date"),
            "volume": e.get("volume", 0),
            "liquidity": e.get("liquidity", 0),
        }
        for e in events
    ]
    stmt = pg_insert(Event).values(values)
    stmt = stmt.on_conflict_do_update(
        index_elements=["id"],
        set_={
            "title": stmt.excluded.title,
            "slug": stmt.excluded.slug,
            "description": stmt.excluded.description,
            "is_active": stmt.excluded.is_active,
            "is_closed": stmt.excluded.is_closed,
            "start_date": stmt.excluded.start_date,
            "end_date": stmt.excluded.end_date,
            "volume": stmt.excluded.volume,
            "liquidity": stmt.excluded.liquidity,
        },
    )
    await session.execute(stmt)
    return len(values)


async def upsert_tags(session: AsyncSession, tags: list[dict[str, Any]]) -> int:
    """Bulk upsert tags."""
    if not tags:
        return 0

    values = [
        {"id": t["id"], "label": t.get("label"), "slug": t.get("slug")}
        for t in tags
    ]
    stmt = pg_insert(Tag).values(values)
    stmt = stmt.on_conflict_do_update(
        index_elements=["id"],
        set_={"label": stmt.excluded.label, "slug": stmt.excluded.slug},
    )
    await session.execute(stmt)
    return len(values)


async def backfill_price_history(
    http: httpx.AsyncClient,
    session: AsyncSession,
    market_id: str,
    condition_id: str,
) -> int:
    """Fetch and store price history for one market. Returns snapshot count."""
    async with PRICE_SEMAPHORE:
        snapshots = await clob.fetch_price_history(http, condition_id)

    if not snapshots:
        return 0

    # Check if we already have snapshots for this market
    existing = await session.execute(
        select(PriceSnapshot.id)
        .where(PriceSnapshot.market_id == market_id)
        .limit(1)
    )
    if existing.scalar_one_or_none() is not None:
        logger.debug("Skipping price history for %s — already exists", market_id)
        return 0

    objects = [
        PriceSnapshot(
            market_id=market_id,
            timestamp=s["timestamp"],
            yes_price=s["yes_price"],
            no_price=s["no_price"],
            source="clob_history",
        )
        for s in snapshots
    ]
    session.add_all(objects)
    return len(objects)


async def run_backfill(skip_prices: bool = False) -> dict[str, int]:
    """Execute the full historical backfill.

    1. Fetch & upsert all closed events
    2. Fetch & upsert all tags
    3. Fetch & upsert all closed markets
    4. Backfill price histories for markets with a condition_id

    Returns counts of items processed.
    """
    stats: dict[str, int] = {
        "events": 0,
        "tags": 0,
        "markets": 0,
        "price_snapshots": 0,
        "price_errors": 0,
    }

    async with httpx.AsyncClient() as http:
        # ── Tags (fast, do first) ───────────────────────────────
        logger.info("Backfilling tags...")
        tags = await gamma.fetch_tags(http)
        async with async_session_factory() as session:
            stats["tags"] = await upsert_tags(session, tags)
            await session.commit()
        logger.info("Upserted %d tags", stats["tags"])

        # ── Markets (the important part) ────────────────────────
        logger.info("Backfilling closed markets...")
        markets = await gamma.fetch_all_closed_markets(http)
        # Batch upsert in chunks of 500
        chunk_size = 500
        for i in range(0, len(markets), chunk_size):
            chunk = markets[i : i + chunk_size]
            async with async_session_factory() as session:
                count = await upsert_markets(session, chunk)
                await session.commit()
                stats["markets"] += count
        logger.info("Upserted %d markets", stats["markets"])

        # ── Price histories ─────────────────────────────────────
        if not skip_prices:
            logger.info("Backfilling price histories...")
            markets_with_cid = [
                m for m in markets if m.get("condition_id")
            ]
            for i, m in enumerate(markets_with_cid):
                try:
                    async with async_session_factory() as session:
                        count = await backfill_price_history(
                            http, session, m["id"], m["condition_id"]
                        )
                        await session.commit()
                        stats["price_snapshots"] += count
                except Exception:
                    stats["price_errors"] += 1
                    logger.exception(
                        "Price history failed for market %s", m["id"]
                    )

                if (i + 1) % 100 == 0:
                    logger.info(
                        "Price history progress: %d/%d markets",
                        i + 1, len(markets_with_cid),
                    )
            logger.info(
                "Price history done: %d snapshots, %d errors",
                stats["price_snapshots"], stats["price_errors"],
            )

        # ── Events (capped — not critical) ─────────────────────
        logger.info("Backfilling events (max 5000)...")
        events: list[dict[str, Any]] = []
        offset = 0
        while offset < 5000:
            page = await gamma.fetch_closed_events(http, limit=100, offset=offset)
            if not page:
                break
            events.extend(page)
            offset += 100
        async with async_session_factory() as session:
            stats["events"] = await upsert_events(session, events)
            await session.commit()
        logger.info("Upserted %d events", stats["events"])

    return stats
