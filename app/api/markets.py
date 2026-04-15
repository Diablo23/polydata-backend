"""Markets API routes — list, search, detail, price history."""

from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import func, or_, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.database import get_session
from app.models import Market, MarketTag, PriceSnapshot, Tag

router = APIRouter(prefix="/api/v1", tags=["markets"])


@router.get("/markets/resolved")
async def list_resolved_markets(
    category: str | None = Query(None),
    outcome: str | None = Query(None, description="ALL, YES, or NO"),
    volume_min: float | None = Query(None),
    volume_max: float | None = Query(None),
    start_date: str | None = Query(None, description="ISO date string"),
    end_date: str | None = Query(None, description="ISO date string"),
    search: str | None = Query(None, description="Search in question text"),
    sort: str = Query("volume", description="volume, closed_time, question"),
    order: str = Query("desc", description="asc or desc"),
    limit: int = Query(50, ge=1, le=200),
    offset: int = Query(0, ge=0),
    session: AsyncSession = Depends(get_session),
) -> dict[str, Any]:
    """Paginated list of resolved markets with filtering and sorting."""

    filters = [
        Market.is_closed.is_(True),
        Market.winning_outcome.isnot(None),
    ]

    if category:
        filters.append(Market.category == category)
    if outcome and outcome.upper() != "ALL":
        if outcome.upper() == "YES":
            filters.append(Market.resolved_to_no.is_(False))
        elif outcome.upper() == "NO":
            filters.append(Market.resolved_to_no.is_(True))
    if volume_min is not None:
        filters.append(Market.volume >= volume_min)
    if volume_max is not None:
        filters.append(Market.volume <= volume_max)
    if start_date:
        filters.append(Market.closed_time >= start_date)
    if end_date:
        filters.append(Market.closed_time <= end_date)
    if search:
        filters.append(Market.question.ilike(f"%{search}%"))

    # Count
    count_q = select(func.count(Market.id)).where(*filters)
    total = (await session.execute(count_q)).scalar() or 0

    # Sort
    sort_col = {
        "volume": Market.volume,
        "closed_time": Market.closed_time,
        "question": Market.question,
    }.get(sort, Market.volume)

    if order.lower() == "asc":
        sort_expr = sort_col.asc().nullslast()
    else:
        sort_expr = sort_col.desc().nullslast()

    q = (
        select(Market)
        .where(*filters)
        .order_by(sort_expr)
        .limit(limit)
        .offset(offset)
    )
    rows = (await session.execute(q)).scalars().all()

    items = [
        {
            "id": m.id,
            "question": m.question,
            "slug": m.slug,
            "outcome_prices": m.outcome_prices,
            "volume": m.volume,
            "category": m.category,
            "is_closed": m.is_closed,
            "closed_time": m.closed_time.isoformat() if m.closed_time else None,
            "winning_outcome": m.winning_outcome,
            "resolved_to_no": m.resolved_to_no,
        }
        for m in rows
    ]

    return {
        "items": items,
        "total": total,
        "limit": limit,
        "offset": offset,
        "has_more": (offset + limit) < total,
    }


@router.get("/markets/{market_id}")
async def get_market(
    market_id: str,
    session: AsyncSession = Depends(get_session),
) -> dict[str, Any]:
    """Single market with full details."""
    # Try by ID first, then by slug
    q = select(Market).where(
        or_(Market.id == market_id, Market.slug == market_id)
    )
    market = (await session.execute(q)).scalar_one_or_none()

    if market is None:
        raise HTTPException(status_code=404, detail="Market not found")

    return {
        "id": market.id,
        "question": market.question,
        "condition_id": market.condition_id,
        "slug": market.slug,
        "outcomes": market.outcomes,
        "outcome_prices": market.outcome_prices,
        "volume": market.volume,
        "liquidity": market.liquidity,
        "start_date": market.start_date.isoformat() if market.start_date else None,
        "end_date": market.end_date.isoformat() if market.end_date else None,
        "closed_time": market.closed_time.isoformat() if market.closed_time else None,
        "category": market.category,
        "resolution_source": market.resolution_source,
        "description": market.description,
        "is_active": market.is_active,
        "is_closed": market.is_closed,
        "is_archived": market.is_archived,
        "event_id": market.event_id,
        "winning_outcome": market.winning_outcome,
        "resolved_to_no": market.resolved_to_no,
        "created_at": market.created_at.isoformat() if market.created_at else None,
        "updated_at": market.updated_at.isoformat() if market.updated_at else None,
    }


@router.get("/markets/{market_id}/price-history")
async def get_price_history(
    market_id: str,
    session: AsyncSession = Depends(get_session),
) -> list[dict[str, Any]]:
    """Price history for a market."""
    # Resolve slug to ID if needed
    mkt = (
        await session.execute(
            select(Market.id).where(
                or_(Market.id == market_id, Market.slug == market_id)
            )
        )
    ).scalar_one_or_none()

    if mkt is None:
        raise HTTPException(status_code=404, detail="Market not found")

    q = (
        select(PriceSnapshot)
        .where(PriceSnapshot.market_id == mkt)
        .order_by(PriceSnapshot.timestamp.asc())
    )
    rows = (await session.execute(q)).scalars().all()

    return [
        {
            "timestamp": s.timestamp.isoformat(),
            "yes_price": s.yes_price,
            "no_price": s.no_price,
        }
        for s in rows
    ]


@router.get("/tags")
async def list_tags(
    session: AsyncSession = Depends(get_session),
) -> list[dict[str, Any]]:
    """All tags/categories with market counts."""
    q = (
        select(
            Tag.id,
            Tag.label,
            Tag.slug,
            func.count(MarketTag.market_id).label("market_count"),
        )
        .outerjoin(MarketTag, MarketTag.tag_id == Tag.id)
        .group_by(Tag.id, Tag.label, Tag.slug)
        .order_by(func.count(MarketTag.market_id).desc())
    )
    rows = (await session.execute(q)).all()

    return [
        {
            "id": r.id,
            "label": r.label,
            "slug": r.slug,
            "market_count": r.market_count or 0,
        }
        for r in rows
    ]


@router.get("/categories")
async def list_categories(
    session: AsyncSession = Depends(get_session),
) -> list[dict[str, Any]]:
    """Distinct categories from markets (simpler than tags)."""
    q = (
        select(
            Market.category,
            func.count(Market.id).label("market_count"),
        )
        .where(Market.category.isnot(None))
        .group_by(Market.category)
        .order_by(func.count(Market.id).desc())
    )
    rows = (await session.execute(q)).all()

    return [
        {"category": r.category, "market_count": r.market_count}
        for r in rows
    ]
