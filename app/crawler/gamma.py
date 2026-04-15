"""Gamma API client — market discovery, metadata, events, tags."""

import asyncio
import json
import logging
from typing import Any

import httpx

from app.config import get_settings

logger = logging.getLogger(__name__)
settings = get_settings()

BASE_URL = settings.gamma_api_url
DELAY = settings.crawler_request_delay_ms / 1000  # convert ms → seconds
MAX_RETRIES = settings.crawler_max_retries
PAGE_SIZE = settings.crawler_page_size


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
                "Gamma %s attempt %d/%d failed: %s — retrying in %ds",
                path, attempt, MAX_RETRIES, exc, wait,
            )
            if attempt == MAX_RETRIES:
                raise
            await asyncio.sleep(wait)


def _parse_json_string(val: str | list | None) -> list | None:
    """Parse a JSON-encoded string field (outcomes, outcomePrices, clobTokenIds)."""
    if val is None:
        return None
    if isinstance(val, list):
        return val
    try:
        return json.loads(val)
    except (json.JSONDecodeError, TypeError):
        return None


def parse_market(raw: dict[str, Any]) -> dict[str, Any]:
    """Normalise a raw Gamma market object into our DB-friendly shape."""
    outcomes = _parse_json_string(raw.get("outcomes"))
    outcome_prices_raw = _parse_json_string(raw.get("outcomePrices"))
    clob_token_ids = _parse_json_string(raw.get("clobTokenIds"))

    # Convert price strings to floats
    outcome_prices: list[float] | None = None
    if outcome_prices_raw:
        try:
            outcome_prices = [float(p) for p in outcome_prices_raw]
        except (ValueError, TypeError):
            outcome_prices = None

    # Determine winning outcome
    winning_outcome: str | None = None
    resolved_to_no: bool | None = None
    is_closed = bool(raw.get("closed", False))

    if is_closed and outcome_prices and outcomes and len(outcomes) >= 2:
        # The outcome whose price is at or near 1.0 is the winner
        if outcome_prices[0] >= 0.99:
            winning_outcome = outcomes[0]
        elif len(outcome_prices) > 1 and outcome_prices[1] >= 0.99:
            winning_outcome = outcomes[1]
        elif outcome_prices[0] <= 0.01 and len(outcome_prices) > 1:
            winning_outcome = outcomes[1]
        elif len(outcome_prices) > 1 and outcome_prices[1] <= 0.01:
            winning_outcome = outcomes[0]

        if winning_outcome is not None:
            resolved_to_no = winning_outcome.lower() == "no"

    return {
        "id": str(raw["id"]),
        "question": raw.get("question", ""),
        "condition_id": raw.get("conditionId"),
        "slug": raw.get("slug"),
        "outcomes": outcomes,
        "outcome_prices": outcome_prices,
        "volume": float(raw.get("volumeNum", 0) or 0),
        "liquidity": float(raw.get("liquidityNum", 0) or 0),
        "start_date": raw.get("startDate"),
        "end_date": raw.get("endDate"),
        "closed_time": raw.get("closedTime"),
        "category": raw.get("category"),
        "resolution_source": raw.get("resolutionSource"),
        "description": raw.get("description"),
        "is_active": bool(raw.get("active", False)),
        "is_closed": is_closed,
        "is_archived": bool(raw.get("archived", False)),
        "clob_token_ids": clob_token_ids,
        "event_id": raw.get("eventId") and str(raw["eventId"]),
        "winning_outcome": winning_outcome,
        "resolved_to_no": resolved_to_no,
        "raw_data": raw,
    }


def parse_event(raw: dict[str, Any]) -> dict[str, Any]:
    """Normalise a raw Gamma event object."""
    return {
        "id": str(raw["id"]),
        "title": raw.get("title"),
        "slug": raw.get("slug"),
        "description": raw.get("description"),
        "is_active": bool(raw.get("active", False)),
        "is_closed": bool(raw.get("closed", False)),
        "start_date": raw.get("startDate"),
        "end_date": raw.get("endDate"),
        "volume": float(raw.get("volume", 0) or 0),
        "liquidity": float(raw.get("liquidity", 0) or 0),
    }


# ── Public fetchers ─────────────────────────────────────────────


async def fetch_closed_markets(
    client: httpx.AsyncClient,
    limit: int = PAGE_SIZE,
    offset: int = 0,
) -> list[dict[str, Any]]:
    """Fetch a page of closed (resolved) markets."""
    data = await _request(
        client, "/markets", {"closed": "true", "limit": limit, "offset": offset}
    )
    if not data:
        return []
    return [parse_market(m) for m in data]


async def fetch_active_markets(
    client: httpx.AsyncClient,
    limit: int = PAGE_SIZE,
    offset: int = 0,
) -> list[dict[str, Any]]:
    """Fetch a page of active (live) markets."""
    data = await _request(
        client,
        "/markets",
        {"active": "true", "closed": "false", "limit": limit, "offset": offset},
    )
    if not data:
        return []
    return [parse_market(m) for m in data]


async def fetch_all_closed_markets(
    client: httpx.AsyncClient,
) -> list[dict[str, Any]]:
    """Paginate through ALL closed markets."""
    all_markets: list[dict[str, Any]] = []
    offset = 0
    while True:
        page = await fetch_closed_markets(client, limit=PAGE_SIZE, offset=offset)
        if not page:
            break
        all_markets.extend(page)
        logger.info("Fetched %d closed markets (offset=%d)", len(all_markets), offset)
        offset += PAGE_SIZE
    return all_markets


async def fetch_closed_events(
    client: httpx.AsyncClient,
    limit: int = PAGE_SIZE,
    offset: int = 0,
) -> list[dict[str, Any]]:
    """Fetch a page of closed events."""
    data = await _request(
        client, "/events", {"closed": "true", "limit": limit, "offset": offset}
    )
    if not data:
        return []
    return [parse_event(e) for e in data]


async def fetch_all_closed_events(
    client: httpx.AsyncClient,
) -> list[dict[str, Any]]:
    """Paginate through ALL closed events."""
    all_events: list[dict[str, Any]] = []
    offset = 0
    while True:
        page = await fetch_closed_events(client, limit=PAGE_SIZE, offset=offset)
        if not page:
            break
        all_events.extend(page)
        logger.info("Fetched %d closed events (offset=%d)", len(all_events), offset)
        offset += PAGE_SIZE
    return all_events


async def fetch_tags(client: httpx.AsyncClient) -> list[dict[str, Any]]:
    """Fetch all available tags/categories."""
    data = await _request(client, "/tags")
    if not data:
        return []
    return [
        {"id": str(t["id"]), "label": t.get("label"), "slug": t.get("slug")}
        for t in data
    ]
