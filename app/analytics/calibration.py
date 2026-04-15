"""Calibration curves and Brier score computation.

For each resolved market, we look at its YES price at a given time horizon
before resolution and compare the implied probability to the actual outcome.
"""

import logging
from datetime import timedelta
from typing import Any

from sqlalchemy import Float, and_, case, cast, func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models import Market, PriceSnapshot

logger = logging.getLogger(__name__)

# Map horizon strings to timedeltas
HORIZONS: dict[str, timedelta] = {
    "30d": timedelta(days=30),
    "7d": timedelta(days=7),
    "1d": timedelta(days=1),
    "12h": timedelta(hours=12),
    "4h": timedelta(hours=4),
}

BANDS = [
    (0.0, 0.1, "0-10%"),
    (0.1, 0.2, "10-20%"),
    (0.2, 0.3, "20-30%"),
    (0.3, 0.4, "30-40%"),
    (0.4, 0.5, "40-50%"),
    (0.5, 0.6, "50-60%"),
    (0.6, 0.7, "60-70%"),
    (0.7, 0.8, "70-80%"),
    (0.8, 0.9, "80-90%"),
    (0.9, 1.0, "90-100%"),
]


async def _get_prices_at_horizon(
    session: AsyncSession,
    horizon: str,
    volume_min: float | None = None,
    category: str | None = None,
) -> list[dict[str, Any]]:
    """For each resolved market, find the YES price closest to the given
    time horizon before resolution.

    Returns list of {market_id, yes_price, resolved_yes (bool)}.
    """
    delta = HORIZONS.get(horizon, timedelta(days=7))

    # Build filters for resolved markets
    market_filters = [
        Market.is_closed.is_(True),
        Market.winning_outcome.isnot(None),
        Market.closed_time.isnot(None),
        Market.condition_id.isnot(None),
    ]
    if volume_min is not None:
        market_filters.append(Market.volume >= volume_min)
    if category is not None:
        market_filters.append(Market.category == category)

    # Subquery: target timestamp = closed_time - horizon
    # We want the snapshot closest to that target
    # Strategy: for each market, find the snapshot with timestamp closest to
    # (closed_time - delta), but not after closed_time

    # Step 1: Get eligible markets
    markets_q = select(
        Market.id,
        Market.closed_time,
        Market.resolved_to_no,
    ).where(*market_filters)

    market_rows = (await session.execute(markets_q)).all()

    results: list[dict[str, Any]] = []

    # Step 2: For each market, find closest snapshot to target time
    # Batch this to avoid N+1 — use a window function approach
    # For large datasets, this is done in batches
    batch_size = 500
    for i in range(0, len(market_rows), batch_size):
        batch = market_rows[i : i + batch_size]
        market_ids = [r.id for r in batch]
        market_map = {r.id: r for r in batch}

        # Get all snapshots for these markets in one query
        snap_q = (
            select(
                PriceSnapshot.market_id,
                PriceSnapshot.timestamp,
                PriceSnapshot.yes_price,
            )
            .where(PriceSnapshot.market_id.in_(market_ids))
            .order_by(PriceSnapshot.market_id, PriceSnapshot.timestamp)
        )
        snap_rows = (await session.execute(snap_q)).all()

        # Group snapshots by market
        snaps_by_market: dict[str, list] = {}
        for s in snap_rows:
            snaps_by_market.setdefault(s.market_id, []).append(s)

        for mid, mkt in market_map.items():
            snaps = snaps_by_market.get(mid, [])
            if not snaps or mkt.closed_time is None:
                continue

            target_time = mkt.closed_time - delta

            # Find snapshot closest to target_time (but before closed_time)
            best = None
            best_diff = None
            for s in snaps:
                if s.timestamp > mkt.closed_time:
                    continue
                diff = abs((s.timestamp - target_time).total_seconds())
                if best_diff is None or diff < best_diff:
                    best_diff = diff
                    best = s

            if best is not None and best.yes_price is not None:
                results.append({
                    "market_id": mid,
                    "yes_price": best.yes_price,
                    "resolved_yes": not mkt.resolved_to_no,
                })

    return results


async def compute_calibration(
    session: AsyncSession,
    horizon: str = "7d",
    volume_min: float | None = None,
    category: str | None = None,
) -> list[dict[str, Any]]:
    """Compute calibration curve data.

    Groups markets by their YES price at the given horizon into 10% bands,
    then compares predicted probability to actual resolution rate.
    """
    prices = await _get_prices_at_horizon(session, horizon, volume_min, category)

    if not prices:
        return []

    # Bucket into bands
    band_data: dict[str, dict[str, Any]] = {}
    for band_start, band_end, band_label in BANDS:
        band_data[band_label] = {
            "band": band_label,
            "band_start": band_start,
            "band_end": band_end,
            "markets": [],
            "yes_count": 0,
        }

    for p in prices:
        price = p["yes_price"]
        # Clamp to [0, 1]
        price = max(0.0, min(1.0, price))

        for band_start, band_end, band_label in BANDS:
            # Include 1.0 in the last band
            if band_start <= price < band_end or (band_end == 1.0 and price == 1.0):
                band_data[band_label]["markets"].append(p)
                if p["resolved_yes"]:
                    band_data[band_label]["yes_count"] += 1
                break

    # Compute stats per band
    results: list[dict[str, Any]] = []
    for band_start, band_end, band_label in BANDS:
        bd = band_data[band_label]
        market_count = len(bd["markets"])
        if market_count == 0:
            continue

        predicted_rate = (band_start + band_end) / 2  # midpoint
        actual_rate = bd["yes_count"] / market_count
        deviation = actual_rate - predicted_rate

        # Brier score for this band
        brier_sum = sum(
            (m["yes_price"] - (1.0 if m["resolved_yes"] else 0.0)) ** 2
            for m in bd["markets"]
        )
        brier_score = brier_sum / market_count

        results.append({
            "band": band_label,
            "band_start": band_start,
            "band_end": band_end,
            "market_count": market_count,
            "predicted_rate": round(predicted_rate * 100, 1),
            "actual_rate": round(actual_rate * 100, 1),
            "deviation": round(deviation * 100, 1),
            "brier_score": round(brier_score, 4),
        })

    return results


async def compute_brier_score(
    session: AsyncSession,
    horizon: str = "7d",
    volume_min: float | None = None,
    category: str | None = None,
) -> dict[str, Any]:
    """Compute aggregate Brier score.

    Brier = mean( (predicted - actual)^2 )
    Lower is better; 0 = perfect, 0.25 = coin flip.
    """
    prices = await _get_prices_at_horizon(session, horizon, volume_min, category)

    if not prices:
        return {"brier_score": None, "market_count": 0}

    brier_sum = sum(
        (p["yes_price"] - (1.0 if p["resolved_yes"] else 0.0)) ** 2
        for p in prices
    )
    brier_score = brier_sum / len(prices)

    return {
        "brier_score": round(brier_score, 4),
        "market_count": len(prices),
    }


async def compute_calibration_from_final_prices(
    session: AsyncSession,
    volume_min: float | None = None,
    category: str | None = None,
) -> list[dict[str, Any]]:
    """Compute calibration using the FINAL outcome_prices stored on the market.

    This is a faster alternative that doesn't require price_snapshots —
    it uses the last known outcome_prices before resolution.
    Useful as a fallback when price history hasn't been backfilled.
    """
    filters = [
        Market.is_closed.is_(True),
        Market.winning_outcome.isnot(None),
        Market.outcome_prices.isnot(None),
    ]
    if volume_min is not None:
        filters.append(Market.volume >= volume_min)
    if category is not None:
        filters.append(Market.category == category)

    q = select(
        Market.id,
        Market.outcome_prices,
        Market.resolved_to_no,
    ).where(*filters)

    rows = (await session.execute(q)).all()

    # Use the same band logic but with stored prices
    # Note: outcome_prices at resolution are [1,0] or [0,1], so this
    # won't give useful calibration. This method is mainly for
    # computing overall NO rates and quick stats.
    # Real calibration needs price_snapshots at a pre-resolution horizon.

    return []  # Placeholder — use compute_calibration() with snapshots
