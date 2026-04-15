"""Pydantic schemas for API request/response validation."""

from datetime import datetime

from pydantic import BaseModel, Field


# ── Market schemas ──────────────────────────────────────────────


class MarketSummary(BaseModel):
    id: str
    question: str
    slug: str | None = None
    outcome_prices: list[float] | None = None
    volume: float = 0
    category: str | None = None
    is_closed: bool = False
    closed_time: datetime | None = None
    winning_outcome: str | None = None
    resolved_to_no: bool | None = None

    model_config = {"from_attributes": True}


class MarketDetail(MarketSummary):
    condition_id: str | None = None
    outcomes: list[str] | None = None
    liquidity: float = 0
    start_date: datetime | None = None
    end_date: datetime | None = None
    resolution_source: str | None = None
    description: str | None = None
    is_active: bool = True
    is_archived: bool = False
    event_id: str | None = None
    created_at: datetime | None = None
    updated_at: datetime | None = None


class PricePoint(BaseModel):
    timestamp: datetime
    yes_price: float | None = None
    no_price: float | None = None

    model_config = {"from_attributes": True}


class MarketWithPriceHistory(MarketDetail):
    price_history: list[PricePoint] = []


# ── Stats schemas ───────────────────────────────────────────────


class OverviewStats(BaseModel):
    total_markets: int
    resolved_yes: int
    resolved_no: int
    no_rate: float
    yes_rate: float
    avg_volume: float
    median_volume: float
    total_volume: float
    markets_above_100k: int
    brier_score: float | None = None


class CategoryStats(BaseModel):
    category: str
    total: int
    resolved_yes: int
    resolved_no: int
    no_rate: float
    avg_volume: float
    brier_score: float | None = None


class CalibrationBand(BaseModel):
    band: str
    band_start: float
    band_end: float
    market_count: int
    predicted_rate: float
    actual_rate: float
    deviation: float
    brier_score: float


class BiasResult(BaseModel):
    band: str
    market_count: int
    implied_probability: float
    actual_yes_rate: float
    bias: float


# ── Scanner schemas ─────────────────────────────────────────────


class NoOpportunity(BaseModel):
    id: str
    question: str
    slug: str | None = None
    no_price: float
    category: str | None = None
    category_no_rate: float
    expected_value: float
    volume: float
    end_date: datetime | None = None


# ── Pagination ──────────────────────────────────────────────────


class PaginatedResponse(BaseModel):
    items: list
    total: int
    limit: int
    offset: int
    has_more: bool


class MarketListResponse(BaseModel):
    items: list[MarketSummary]
    total: int
    limit: int
    offset: int
    has_more: bool


# ── Tags ────────────────────────────────────────────────────────


class TagWithCount(BaseModel):
    id: str
    label: str | None = None
    slug: str | None = None
    market_count: int = 0

    model_config = {"from_attributes": True}
