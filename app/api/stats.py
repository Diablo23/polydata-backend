"""Stats API routes — overview, by-category, calibration, bias."""

from typing import Any

from fastapi import APIRouter, Depends, Query
from sqlalchemy.ext.asyncio import AsyncSession

from app.analytics.bias import compute_bias, compute_extreme_bias
from app.analytics.cache import get_or_compute
from app.analytics.calibration import compute_brier_score, compute_calibration
from app.analytics.resolution import (
    compute_by_category,
    compute_overview,
    get_recent_resolutions,
)
from app.database import get_session

router = APIRouter(prefix="/api/v1/stats", tags=["stats"])


@router.get("/overview")
async def overview(
    session: AsyncSession = Depends(get_session),
) -> dict[str, Any]:
    """Overall resolution statistics."""
    stats = await get_or_compute(
        session, "overview", compute_overview, max_age_minutes=15
    )

    # Attach Brier score
    brier = await get_or_compute(
        session,
        "brier_overall",
        lambda s: compute_brier_score(s, horizon="7d"),
        max_age_minutes=15,
    )
    if isinstance(stats, dict):
        stats["brier_score"] = brier.get("brier_score") if isinstance(brier, dict) else None
    return stats


@router.get("/by-category")
async def by_category(
    volume_min: float | None = Query(None, description="Minimum volume filter"),
    session: AsyncSession = Depends(get_session),
) -> list[dict[str, Any]]:
    """Resolution stats grouped by category."""
    params = {"volume_min": volume_min}

    async def _compute(s: AsyncSession) -> list:
        return await compute_by_category(s, volume_min=volume_min)

    return await get_or_compute(
        session, "by_category", _compute, parameters=params, max_age_minutes=15
    )


@router.get("/calibration")
async def calibration(
    horizon: str = Query("7d", description="Time horizon: 30d, 7d, 1d, 12h, 4h"),
    volume_min: float | None = Query(None, description="Minimum volume filter"),
    category: str | None = Query(None, description="Category filter"),
    session: AsyncSession = Depends(get_session),
) -> list[dict[str, Any]]:
    """Calibration curve data — predicted vs actual resolution rates."""
    params = {"horizon": horizon, "volume_min": volume_min, "category": category}

    async def _compute(s: AsyncSession) -> list:
        return await compute_calibration(
            s, horizon=horizon, volume_min=volume_min, category=category
        )

    return await get_or_compute(
        session, "calibration", _compute, parameters=params, max_age_minutes=15
    )


@router.get("/bias")
async def bias(
    price_band: str | None = Query(None, description="e.g. '90-100' for favorites"),
    horizon: str = Query("7d"),
    volume_min: float | None = Query(None),
    category: str | None = Query(None),
    session: AsyncSession = Depends(get_session),
) -> list[dict[str, Any]]:
    """Favorite-longshot bias data."""
    params = {
        "price_band": price_band,
        "horizon": horizon,
        "volume_min": volume_min,
        "category": category,
    }

    async def _compute(s: AsyncSession) -> list:
        return await compute_bias(
            s,
            price_band=price_band,
            horizon=horizon,
            volume_min=volume_min,
            category=category,
        )

    return await get_or_compute(
        session, "bias", _compute, parameters=params, max_age_minutes=15
    )


@router.get("/bias/extremes")
async def bias_extremes(
    horizon: str = Query("7d"),
    volume_min: float | None = Query(None),
    session: AsyncSession = Depends(get_session),
) -> dict[str, Any]:
    """Quick summary of favorite-longshot bias at the extremes."""
    params = {"horizon": horizon, "volume_min": volume_min}

    async def _compute(s: AsyncSession) -> dict:
        return await compute_extreme_bias(s, horizon=horizon, volume_min=volume_min)

    return await get_or_compute(
        session, "bias_extremes", _compute, parameters=params, max_age_minutes=15
    )


@router.get("/recent-resolutions")
async def recent_resolutions(
    limit: int = Query(20, ge=1, le=100),
    session: AsyncSession = Depends(get_session),
) -> list[dict[str, Any]]:
    """Most recently resolved markets (not cached — always fresh)."""
    return await get_recent_resolutions(session, limit=limit)
