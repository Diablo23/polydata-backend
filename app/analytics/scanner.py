"""NO opportunity scanner — finds active markets where NO appears undervalued.

For each active market, computes expected value of buying NO based on
the historical NO resolution rate for that category.
"""

import logging
from typing import Any

from sqlalchemy import Float, case, cast, func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models import Market

logger = logging.getLogger(__name__)


async def _get_category_no_rates(
    session: AsyncSession,
) -> dict[str, float]:
    """Compute historical NO resolution rate per category."""
    q = (
        select(
            Market.category,
            func.count(Market.id).label("total"),
            func.count(case((Market.resolved_to_no.is_(True), 1))).label("no_count"),
        )
        .where(
            Market.is_closed.is_(True),
            Market.winning_outcome.isnot(None),
            Market.category.isnot(None),
        )
        .group_by(Market.category)
    )
    rows = (await session.execute(q)).all()

    rates: dict[str, float] = {}
    for r in rows:
        total = r.total or 0
        if total > 0:
            rates[r.category] = (r.no_count or 0) / total

    return rates


async def _get_global_no_rate(session: AsyncSession) -> float:
    """Compute global NO rate as fallback for unknown categories."""
    q = select(
        func.count(Market.id).label("total"),
        func.count(case((Market.resolved_to_no.is_(True), 1))).label("no_count"),
    ).where(
        Market.is_closed.is_(True),
        Market.winning_outcome.isnot(None),
    )
    row = (await session.execute(q)).one()
    total = row.total or 0
    if total == 0:
        return 0.733  # fallback default
    return (row.no_count or 0) / total


async def scan_no_opportunities(
    session: AsyncSession,
    max_no_price: float = 0.60,
    min_volume: float = 5000,
    category: str | None = None,
    sort_by: str = "expected_value",  # "expected_value", "volume", "end_date"
    limit: int = 50,
) -> list[dict[str, Any]]:
    """Find active markets where NO might be undervalued.

    EV = (category_no_rate × $1.00) - no_price
    A positive EV suggests the NO price is lower than historically justified.
    """
    # Get category NO rates
    cat_rates = await _get_category_no_rates(session)
    global_no_rate = await _get_global_no_rate(session)

    # Fetch active markets
    filters = [
        Market.is_active.is_(True),
        Market.is_closed.is_(False),
        Market.outcome_prices.isnot(None),
        Market.outcomes.isnot(None),
    ]
    if min_volume > 0:
        filters.append(Market.volume >= min_volume)
    if category:
        filters.append(Market.category == category)

    q = (
        select(Market)
        .where(*filters)
        .order_by(Market.volume.desc())
        .limit(500)  # Pre-limit before EV filtering
    )
    rows = (await session.execute(q)).scalars().all()

    opportunities: list[dict[str, Any]] = []
    for m in rows:
        if not m.outcomes or not m.outcome_prices:
            continue
        if len(m.outcomes) < 2 or len(m.outcome_prices) < 2:
            continue

        # Find NO price
        try:
            no_idx = [o.lower() for o in m.outcomes].index("no")
            no_price = m.outcome_prices[no_idx]
        except (ValueError, IndexError):
            # If no "No" outcome, skip
            continue

        if no_price > max_no_price:
            continue

        # Get category NO rate (or global fallback)
        cat_no_rate = cat_rates.get(m.category, global_no_rate) if m.category else global_no_rate

        # EV = (probability of NO × payout) - cost
        # Payout for NO winning = $1.00
        ev = (cat_no_rate * 1.0) - no_price

        opportunities.append({
            "id": m.id,
            "question": m.question,
            "slug": m.slug,
            "no_price": round(no_price, 4),
            "category": m.category,
            "category_no_rate": round(cat_no_rate * 100, 1),
            "expected_value": round(ev, 4),
            "volume": m.volume,
            "end_date": m.end_date.isoformat() if m.end_date else None,
        })

    # Sort
    if sort_by == "volume":
        opportunities.sort(key=lambda x: x["volume"], reverse=True)
    elif sort_by == "end_date":
        opportunities.sort(
            key=lambda x: x["end_date"] or "9999", reverse=False
        )
    else:  # expected_value
        opportunities.sort(key=lambda x: x["expected_value"], reverse=True)

    return opportunities[:limit]
