"""CLOB API client — orderbook data and price history."""

import asyncio
import logging
from datetime import datetime, timezone
from typing import Any

import httpx

from app.config import get_settings

logger = logging.getLogger(__name__)
settings = get_settings()

BASE_URL = settings.clob_api_url
DELAY = settings.crawler_request_delay_ms / 1000
MAX_RETRIES = settings.crawler_max_retries


async def _request(
    client: httpx.AsyncClient,
    path: str,
    params: dict[str, Any] | None = None,
) -> Any:
    """GET with retry + exponential backoff."""
    url = f"{BASE_URL}{path}"
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = await client.get(url, params=params, timeout=30)
            resp.raise_for_status()
            await asyncio.sleep(DELAY)
            return resp.json()
        except (httpx.HTTPStatusError, httpx.RequestError) as exc:
            wait = 2 ** (attempt - 1)
            logger.warning(
                "CLOB %s attempt %d/%d failed: %s — retrying in %ds",
                path, attempt, MAX_RETRIES, exc, wait,
            )
            if attempt == MAX_RETRIES:
                raise
            await asyncio.sleep(wait)


async def fetch_price_history(
    client: httpx.AsyncClient,
    condition_id: str,
    interval: str = "max",
    fidelity: int = 60,
) -> list[dict[str, Any]]:
    """Fetch historical price data for a market.

    Returns list of {"timestamp": datetime, "yes_price": float, "no_price": float}.
    The CLOB API returns {t: unix_timestamp, p: price} for the YES token.
    """
    try:
        data = await _request(
            client,
            "/prices-history",
            {
                "market": condition_id,
                "interval": interval,
                "fidelity": fidelity,
            },
        )
    except Exception:
        logger.error("Failed to fetch price history for %s", condition_id)
        return []

    if not data or "history" not in data:
        # Some responses return the array directly, some wrap in {"history": [...]}
        if isinstance(data, list):
            history = data
        else:
            return []
    else:
        history = data["history"]

    snapshots: list[dict[str, Any]] = []
    for point in history:
        try:
            ts = point.get("t")
            price = float(point.get("p", 0))
            if ts is None:
                continue
            # t can be unix seconds (int) or ISO string
            if isinstance(ts, (int, float)):
                timestamp = datetime.fromtimestamp(ts, tz=timezone.utc)
            else:
                timestamp = datetime.fromisoformat(str(ts).replace("Z", "+00:00"))
            snapshots.append({
                "timestamp": timestamp,
                "yes_price": price,
                "no_price": round(1.0 - price, 6),
            })
        except (ValueError, TypeError) as exc:
            logger.debug("Skipping malformed price point: %s (%s)", point, exc)
            continue

    return snapshots


async def fetch_current_price(
    client: httpx.AsyncClient,
    clob_token_id: str,
) -> float | None:
    """Fetch current price for a single token."""
    try:
        data = await _request(client, "/price", {"token_id": clob_token_id})
        if data and "price" in data:
            return float(data["price"])
    except Exception:
        logger.error("Failed to fetch price for token %s", clob_token_id)
    return None
