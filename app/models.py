"""SQLAlchemy ORM models for the Nothing Ever Happens database."""

from datetime import datetime

from sqlalchemy import (
    Boolean,
    Float,
    ForeignKey,
    Index,
    String,
    Text,
    func,
)
from sqlalchemy.dialects.postgresql import ARRAY, JSONB, TIMESTAMP
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.database import Base


class Event(Base):
    __tablename__ = "events"

    id: Mapped[str] = mapped_column(String, primary_key=True)
    title: Mapped[str | None] = mapped_column(Text)
    slug: Mapped[str | None] = mapped_column(String, unique=True)
    description: Mapped[str | None] = mapped_column(Text)
    is_active: Mapped[bool] = mapped_column(Boolean, default=True)
    is_closed: Mapped[bool] = mapped_column(Boolean, default=False)
    start_date: Mapped[datetime | None] = mapped_column(TIMESTAMP(timezone=True))
    end_date: Mapped[datetime | None] = mapped_column(TIMESTAMP(timezone=True))
    volume: Mapped[float] = mapped_column(Float, default=0)
    liquidity: Mapped[float] = mapped_column(Float, default=0)
    created_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True), server_default=func.now()
    )

    # Relationships
    markets: Mapped[list["Market"]] = relationship("Market", back_populates="event")


class Market(Base):
    __tablename__ = "markets"

    id: Mapped[str] = mapped_column(String, primary_key=True)
    question: Mapped[str] = mapped_column(Text, nullable=False)
    condition_id: Mapped[str | None] = mapped_column(String, index=True)
    slug: Mapped[str | None] = mapped_column(String, unique=True)
    outcomes: Mapped[list[str] | None] = mapped_column(ARRAY(String))
    outcome_prices: Mapped[list[float] | None] = mapped_column(ARRAY(Float))
    volume: Mapped[float] = mapped_column(Float, default=0)
    liquidity: Mapped[float] = mapped_column(Float, default=0)
    start_date: Mapped[datetime | None] = mapped_column(TIMESTAMP(timezone=True))
    end_date: Mapped[datetime | None] = mapped_column(TIMESTAMP(timezone=True))
    closed_time: Mapped[datetime | None] = mapped_column(TIMESTAMP(timezone=True))
    category: Mapped[str | None] = mapped_column(String)
    resolution_source: Mapped[str | None] = mapped_column(Text)
    description: Mapped[str | None] = mapped_column(Text)
    is_active: Mapped[bool] = mapped_column(Boolean, default=True)
    is_closed: Mapped[bool] = mapped_column(Boolean, default=False)
    is_archived: Mapped[bool] = mapped_column(Boolean, default=False)
    clob_token_ids: Mapped[list[str] | None] = mapped_column(ARRAY(String))
    event_id: Mapped[str | None] = mapped_column(
        String, ForeignKey("events.id"), index=True
    )
    winning_outcome: Mapped[str | None] = mapped_column(String)
    resolved_to_no: Mapped[bool | None] = mapped_column(Boolean)
    created_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True), server_default=func.now()
    )
    updated_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True), server_default=func.now(), onupdate=func.now()
    )
    raw_data: Mapped[dict | None] = mapped_column(JSONB)

    # Relationships
    event: Mapped[Event | None] = relationship("Event", back_populates="markets", foreign_keys=[event_id])
    price_snapshots: Mapped[list["PriceSnapshot"]] = relationship(
        "PriceSnapshot", back_populates="market", cascade="all, delete-orphan"
    )
    tags: Mapped[list["Tag"]] = relationship(
        "Tag", secondary="market_tags", back_populates="markets"
    )

    # Note: event_id FK constraint added via migration to avoid circular issues
    # with Alembic autogenerate. The relationship still works.


class PriceSnapshot(Base):
    __tablename__ = "price_snapshots"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    market_id: Mapped[str] = mapped_column(
        String, ForeignKey("markets.id"), index=True, nullable=False
    )
    timestamp: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True), nullable=False
    )
    yes_price: Mapped[float | None] = mapped_column(Float)
    no_price: Mapped[float | None] = mapped_column(Float)
    source: Mapped[str] = mapped_column(String, default="clob_history")
    created_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True), server_default=func.now()
    )

    # Relationships
    market: Mapped[Market] = relationship("Market", back_populates="price_snapshots")

    __table_args__ = (
        Index("ix_price_snapshots_market_timestamp", "market_id", "timestamp"),
    )


class Tag(Base):
    __tablename__ = "tags"

    id: Mapped[str] = mapped_column(String, primary_key=True)
    label: Mapped[str | None] = mapped_column(Text)
    slug: Mapped[str | None] = mapped_column(String)

    # Relationships
    markets: Mapped[list[Market]] = relationship(
        "Market", secondary="market_tags", back_populates="tags"
    )


class MarketTag(Base):
    __tablename__ = "market_tags"

    market_id: Mapped[str] = mapped_column(
        String, ForeignKey("markets.id"), primary_key=True
    )
    tag_id: Mapped[str] = mapped_column(
        String, ForeignKey("tags.id"), primary_key=True
    )


class AnalyticsCache(Base):
    __tablename__ = "analytics_cache"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    metric_name: Mapped[str] = mapped_column(String, nullable=False)
    parameters: Mapped[dict | None] = mapped_column(JSONB)
    result: Mapped[dict] = mapped_column(JSONB, nullable=False)
    computed_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True), server_default=func.now()
    )

    __table_args__ = (
        Index("ix_analytics_cache_metric_params", "metric_name", "parameters"),
    )
