"""Data API client — user positions, trades, activity, leaderboards.

This module is a placeholder for Phase 2+ features (wallet analytics,
trade history, holder data).  The endpoints below require no auth.
"""

import asyncio
import logging
from typing import Any

import httpx

from app.config import get_settings

logger = logging.getLogger(__name__)
settings = get_settings()

BASE_URL = settings.data_api_url
DELAY = settings.crawler_request_delay_ms / 1000
MAX_RETRIES = settings.crawler_max_retries


async def _request(
    client: httpx.AsyncClient,
    path: str,
    params: dict[str, Any] | None = None,
) -> Any:
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
                "DataAPI %s attempt %d/%d failed: %s", path, attempt, MAX_RETRIES, exc
            )
            if attempt == MAX_RETRIES:
                raise
            await asyncio.sleep(wait)


async def fetch_trades(
    client: httpx.AsyncClient, condition_id: str
) -> list[dict[str, Any]]:
    """Fetch trade history for a market."""
    data = await _request(client, "/trades", {"market": condition_id})
    return data if isinstance(data, list) else []


async def fetch_holders(
    client: httpx.AsyncClient, condition_id: str
) -> list[dict[str, Any]]:
    """Fetch top holders of a market."""
    data = await _request(client, "/holders", {"conditionId": condition_id})
    return data if isinstance(data, list) else []
