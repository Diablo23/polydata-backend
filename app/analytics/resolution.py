"""Resolution statistics — overall and by-category YES/NO rates, volume stats."""

import logging
from typing import Any

from sqlalchemy import case, func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models import Market

logger = logging.getLogger(__name__)


async def compute_overview(session: AsyncSession) -> dict[str, Any]:
    """Compute aggregate resolution stats across all resolved markets."""

    base = select(
        func.count(Market.id).label("total"),
        func.count(case((Market.resolved_to_no.is_(False), 1))).label("resolved_yes"),
        func.count(case((Market.resolved_to_no.is_(True), 1))).label("resolved_no"),
        func.sum(Market.volume).label("total_volume"),
        func.avg(Market.volume).label("avg_volume"),
        func.count(
            case((Market.volume > 100_000, 1))
        ).label("markets_above_100k"),
    ).where(
        Market.is_closed.is_(True),
        Market.winning_outcome.isnot(None),
    )

    row = (await session.execute(base)).one()

    total = row.total or 0
    resolved_yes = row.resolved_yes or 0
    resolved_no = row.resolved_no or 0
    total_volume = float(row.total_volume or 0)
    avg_volume = float(row.avg_volume or 0)
    markets_above_100k = row.markets_above_100k or 0

    no_rate = (resolved_no / total * 100) if total > 0 else 0
    yes_rate = (resolved_yes / total * 100) if total > 0 else 0

    # Median volume — use try/except in case percentile_cont isn't supported
    median_volume = 0.0
    try:
        median_q = select(
            func.percentile_cont(0.5).within_group(Market.volume).label("median")
        ).where(
            Market.is_closed.is_(True),
            Market.winning_outcome.isnot(None),
        )
        median_row = (await session.execute(median_q)).one()
        median_volume = float(median_row.median or 0)
    except Exception:
        logger.warning("percentile_cont failed, using 0 for median")

    return {
        "total_markets": total,
        "resolved_yes": resolved_yes,
        "resolved_no": resolved_no,
        "no_rate": round(no_rate, 1),
        "yes_rate": round(yes_rate, 1),
        "total_volume": round(total_volume, 2),
        "avg_volume": round(avg_volume, 2),
        "median_volume": round(median_volume, 2),
        "markets_above_100k": markets_above_100k,
    }


async def compute_by_category(
    session: AsyncSession,
    volume_min: float | None = None,
) -> list[dict[str, Any]]:
    """Compute resolution stats grouped by category."""

    filters = [
        Market.is_closed.is_(True),
        Market.winning_outcome.isnot(None),
        Market.category.isnot(None),
    ]
    if volume_min is not None:
        filters.append(Market.volume >= volume_min)

    q = (
        select(
            Market.category,
            func.count(Market.id).label("total"),
            func.count(case((Market.resolved_to_no.is_(False), 1))).label("resolved_yes"),
            func.count(case((Market.resolved_to_no.is_(True), 1))).label("resolved_no"),
            func.avg(Market.volume).label("avg_volume"),
        )
        .where(*filters)
        .group_by(Market.category)
    )

    rows = (await session.execute(q)).all()

    results: list[dict[str, Any]] = []
    for r in rows:
        total = r.total or 0
        resolved_no = r.resolved_no or 0
        no_rate = (resolved_no / total * 100) if total > 0 else 0
        results.append({
            "category": r.category,
            "total": total,
            "resolved_yes": r.resolved_yes or 0,
            "resolved_no": resolved_no,
            "no_rate": round(no_rate, 1),
            "avg_volume": round(float(r.avg_volume or 0), 2),
        })

    # Sort by NO rate descending
    results.sort(key=lambda x: x["no_rate"], reverse=True)
    return results


async def get_recent_resolutions(
    session: AsyncSession,
    limit: int = 20,
) -> list[dict[str, Any]]:
    """Get the most recently resolved markets."""
    q = (
        select(Market)
        .where(
            Market.is_closed.is_(True),
            Market.winning_outcome.isnot(None),
        )
        .order_by(Market.closed_time.desc().nullslast())
        .limit(limit)
    )
    rows = (await session.execute(q)).scalars().all()

    return [
        {
            "id": m.id,
            "question": m.question,
            "slug": m.slug,
            "winning_outcome": m.winning_outcome,
            "resolved_to_no": m.resolved_to_no,
            "volume": m.volume,
            "closed_time": m.closed_time.isoformat() if m.closed_time else None,
            "category": m.category,
        }
        for m in rows
    ]
