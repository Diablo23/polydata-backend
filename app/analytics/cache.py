"""Analytics caching layer — stores and retrieves pre-computed analytics."""

import json
import logging
from datetime import datetime, timedelta, timezone
from typing import Any

from sqlalchemy import delete, select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import get_settings
from app.models import AnalyticsCache

logger = logging.getLogger(__name__)
settings = get_settings()


def _params_key(params: dict[str, Any] | None) -> dict | None:
    """Normalise parameters dict for consistent cache keys."""
    if not params:
        return None
    # Sort keys and remove None values for consistency
    return {k: v for k, v in sorted(params.items()) if v is not None} or None


async def get_cached(
    session: AsyncSession,
    metric_name: str,
    parameters: dict[str, Any] | None = None,
    max_age_minutes: int | None = None,
) -> dict[str, Any] | None:
    """Retrieve a cached analytics result.

    Returns None if not found or if older than max_age_minutes.
    """
    if max_age_minutes is None:
        max_age_minutes = settings.analytics_refresh_minutes

    params = _params_key(parameters)

    q = select(AnalyticsCache).where(
        AnalyticsCache.metric_name == metric_name,
    )

    if params is not None:
        # JSONB comparison
        q = q.where(AnalyticsCache.parameters == params)
    else:
        q = q.where(AnalyticsCache.parameters.is_(None))

    q = q.order_by(AnalyticsCache.computed_at.desc()).limit(1)

    row = (await session.execute(q)).scalar_one_or_none()
    if row is None:
        return None

    # Check freshness
    cutoff = datetime.now(timezone.utc) - timedelta(minutes=max_age_minutes)
    if row.computed_at and row.computed_at.replace(tzinfo=timezone.utc) < cutoff:
        logger.debug("Cache stale for %s (computed %s)", metric_name, row.computed_at)
        return None

    return row.result


async def set_cached(
    session: AsyncSession,
    metric_name: str,
    result: dict[str, Any] | list,
    parameters: dict[str, Any] | None = None,
) -> None:
    """Store a computed analytics result in cache."""
    params = _params_key(parameters)

    # Delete old entries for this metric+params combo
    del_q = delete(AnalyticsCache).where(
        AnalyticsCache.metric_name == metric_name,
    )
    if params is not None:
        del_q = del_q.where(AnalyticsCache.parameters == params)
    else:
        del_q = del_q.where(AnalyticsCache.parameters.is_(None))

    await session.execute(del_q)

    # Insert new
    entry = AnalyticsCache(
        metric_name=metric_name,
        parameters=params,
        result=result if isinstance(result, dict) else {"data": result},
        computed_at=datetime.now(timezone.utc),
    )
    session.add(entry)


async def get_or_compute(
    session: AsyncSession,
    metric_name: str,
    compute_fn,
    parameters: dict[str, Any] | None = None,
    max_age_minutes: int | None = None,
) -> Any:
    """Get from cache or compute + cache.

    compute_fn should be an async callable that takes (session) and returns
    the result to cache.
    """
    cached = await get_cached(session, metric_name, parameters, max_age_minutes)
    if cached is not None:
        logger.debug("Cache hit for %s", metric_name)
        # Unwrap {"data": [...]} wrapper if present
        if isinstance(cached, dict) and "data" in cached and len(cached) == 1:
            return cached["data"]
        return cached

    logger.debug("Cache miss for %s — computing", metric_name)
    result = await compute_fn(session)

    await set_cached(session, metric_name, result, parameters)
    await session.commit()

    return result
