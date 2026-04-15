"""Scanner API routes — NO opportunity finder for active markets."""

from typing import Any

from fastapi import APIRouter, Depends, Query
from sqlalchemy.ext.asyncio import AsyncSession

from app.analytics.scanner import scan_no_opportunities
from app.database import get_session

router = APIRouter(prefix="/api/v1/scanner", tags=["scanner"])


@router.get("/no-opportunities")
async def no_opportunities(
    max_no_price: float = Query(0.60, description="Maximum NO price to consider"),
    min_volume: float = Query(5000, description="Minimum market volume"),
    category: str | None = Query(None, description="Category filter"),
    sort_by: str = Query(
        "expected_value",
        description="Sort by: expected_value, volume, end_date",
    ),
    limit: int = Query(50, ge=1, le=200),
    session: AsyncSession = Depends(get_session),
) -> list[dict[str, Any]]:
    """Find active markets where NO appears undervalued based on
    historical category resolution rates."""
    return await scan_no_opportunities(
        session,
        max_no_price=max_no_price,
        min_volume=min_volume,
        category=category,
        sort_by=sort_by,
        limit=limit,
    )
