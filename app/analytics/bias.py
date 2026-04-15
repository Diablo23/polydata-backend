"""Favorite-longshot bias detection.

Compares the implied probability (from market prices) against actual
resolution rates to quantify systematic mispricing at the extremes.
"""

import logging
from typing import Any

from sqlalchemy.ext.asyncio import AsyncSession

from app.analytics.calibration import BANDS, _get_prices_at_horizon

logger = logging.getLogger(__name__)


async def compute_bias(
    session: AsyncSession,
    price_band: str | None = None,
    horizon: str = "7d",
    volume_min: float | None = None,
    category: str | None = None,
) -> list[dict[str, Any]]:
    """Compute favorite-longshot bias for each probability band.

    For each band:
    - implied_probability = midpoint of the band
    - actual_yes_rate = fraction that actually resolved YES
    - bias = actual_yes_rate - implied_probability
      - Positive bias → underbets (resolve YES more than price implies)
      - Negative bias → overbets / overconfidence (favorites win less often)

    The classic favorite-longshot bias shows:
    - High-probability bands (>90%) with NEGATIVE bias (overconfidence)
    - Low-probability bands (<10%) with POSITIVE bias (longshots win more)
    """
    prices = await _get_prices_at_horizon(session, horizon, volume_min, category)

    if not prices:
        return []

    # Filter to specific band if requested
    target_bands = BANDS
    if price_band:
        # Parse "90-100" format
        try:
            parts = price_band.replace("%", "").split("-")
            low = float(parts[0]) / 100
            high = float(parts[1]) / 100
            target_bands = [(low, high, price_band)]
        except (ValueError, IndexError):
            pass

    # Bucket markets into bands
    results: list[dict[str, Any]] = []
    for band_start, band_end, band_label in target_bands:
        band_markets = [
            p for p in prices
            if band_start <= max(0.0, min(1.0, p["yes_price"])) < band_end
            or (band_end == 1.0 and p["yes_price"] >= 1.0)
        ]

        if not band_markets:
            continue

        market_count = len(band_markets)
        yes_count = sum(1 for m in band_markets if m["resolved_yes"])
        actual_yes_rate = yes_count / market_count
        implied_probability = (band_start + band_end) / 2
        bias = actual_yes_rate - implied_probability

        results.append({
            "band": band_label,
            "market_count": market_count,
            "implied_probability": round(implied_probability * 100, 1),
            "actual_yes_rate": round(actual_yes_rate * 100, 1),
            "bias": round(bias * 100, 1),
        })

    return results


async def compute_extreme_bias(
    session: AsyncSession,
    horizon: str = "7d",
    volume_min: float | None = None,
) -> dict[str, Any]:
    """Quick summary of bias at the extremes — the key insight.

    Returns bias for favorites (>90%) and longshots (<10%).
    """
    all_bias = await compute_bias(session, horizon=horizon, volume_min=volume_min)

    favorites = next((b for b in all_bias if b["band"] == "90-100%"), None)
    longshots = next((b for b in all_bias if b["band"] == "0-10%"), None)

    return {
        "favorites": favorites,
        "longshots": longshots,
        "interpretation": {
            "favorites_overconfident": (
                favorites is not None and favorites["bias"] < 0
            ),
            "longshots_underbet": (
                longshots is not None and longshots["bias"] > 0
            ),
        },
    }
