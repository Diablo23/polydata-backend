"""Ongoing sync — poll for new/updated markets every N minutes."""

import logging
from typing import Any

import httpx
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import get_settings
from app.crawler import clob, gamma
from app.crawler.backfill import (
    backfill_price_history,
    upsert_events,
    upsert_markets,
    upsert_tags,
)
from app.database import get_session_factory
from app.models import Market

logger = logging.getLogger(__name__)
settings = get_settings()


async def _get_tracked_active_ids(session: AsyncSession) -> set[str]:
    """Return IDs of markets we currently track as active."""
    result = await session.execute(
        select(Market.id).where(Market.is_active.is_(True))
    )
    return {row[0] for row in result.all()}


async def run_sync() -> dict[str, Any]:
    """Execute one sync cycle.

    1. Fetch latest active markets from Gamma
    2. Upsert them (detects status changes via is_closed)
    3. Fetch recently closed markets (first 2 pages)
    4. For newly-resolved markets, backfill price history
    5. Re-sync tags

    Returns a summary dict.
    """
    stats: dict[str, Any] = {
        "active_fetched": 0,
        "closed_fetched": 0,
        "newly_resolved": 0,
        "price_snapshots": 0,
        "errors": [],
    }

    async with httpx.AsyncClient() as http:
        # ── Fetch current active markets ────────────────────────
        all_active: list[dict[str, Any]] = []
        offset = 0
        while True:
            page = await gamma.fetch_active_markets(http, limit=100, offset=offset)
            if not page:
                break
            all_active.extend(page)
            offset += 100
            if len(page) < 100:
                break
        stats["active_fetched"] = len(all_active)

        # Get IDs that we previously tracked as active
        async with get_session_factory()() as session:
            prev_active_ids = await _get_tracked_active_ids(session)

        # Upsert active markets
        async with get_session_factory()() as session:
            await upsert_markets(session, all_active)
            await session.commit()

        # ── Fetch recently closed markets (first 2 pages) ──────
        closed_recent: list[dict[str, Any]] = []
        for page_offset in (0, 100):
            page = await gamma.fetch_closed_markets(http, limit=100, offset=page_offset)
            if not page:
                break
            closed_recent.extend(page)
        stats["closed_fetched"] = len(closed_recent)

        # Upsert closed markets
        async with get_session_factory()() as session:
            await upsert_markets(session, closed_recent)
            await session.commit()

        # ── Detect newly resolved markets ───────────────────────
        current_active_ids = {m["id"] for m in all_active}
        closed_market_ids = {m["id"] for m in closed_recent}

        # Markets that were active before but aren't now → newly resolved
        newly_resolved_ids = (prev_active_ids - current_active_ids) & closed_market_ids
        # Also include closed markets we haven't seen before
        newly_resolved_markets = [
            m for m in closed_recent
            if m["id"] in newly_resolved_ids or (
                m.get("is_closed") and m["id"] not in prev_active_ids
            )
        ]
        # De-duplicate
        seen: set[str] = set()
        unique_resolved: list[dict[str, Any]] = []
        for m in newly_resolved_markets:
            if m["id"] not in seen and m.get("condition_id"):
                seen.add(m["id"])
                unique_resolved.append(m)

        stats["newly_resolved"] = len(unique_resolved)

        # ── Backfill price history for newly resolved ───────────
        for m in unique_resolved:
            try:
                async with get_session_factory()() as session:
                    count = await backfill_price_history(
                        http, session, m["id"], m["condition_id"]
                    )
                    await session.commit()
                    stats["price_snapshots"] += count
            except Exception as exc:
                stats["errors"].append({"market_id": m["id"], "error": str(exc)})
                logger.exception("Price backfill failed for %s", m["id"])

        # ── Re-sync tags ────────────────────────────────────────
        try:
            tags = await gamma.fetch_tags(http)
            async with get_session_factory()() as session:
                await upsert_tags(session, tags)
                await session.commit()
        except Exception as exc:
            logger.warning("Tag sync failed: %s", exc)

    logger.info(
        "Sync complete: %d active, %d closed, %d newly resolved, %d snapshots",
        stats["active_fetched"],
        stats["closed_fetched"],
        stats["newly_resolved"],
        stats["price_snapshots"],
    )
    return stats
