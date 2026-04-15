"""initial schema

Revision ID: 001_initial
Revises:
Create Date: 2026-04-15
"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "001_initial"
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # ── events ──────────────────────────────────────────────────
    op.create_table(
        "events",
        sa.Column("id", sa.String(), primary_key=True),
        sa.Column("title", sa.Text()),
        sa.Column("slug", sa.String(), unique=True),
        sa.Column("description", sa.Text()),
        sa.Column("is_active", sa.Boolean(), server_default=sa.text("true")),
        sa.Column("is_closed", sa.Boolean(), server_default=sa.text("false")),
        sa.Column("start_date", sa.TIMESTAMP(timezone=True)),
        sa.Column("end_date", sa.TIMESTAMP(timezone=True)),
        sa.Column("volume", sa.Float(), server_default=sa.text("0")),
        sa.Column("liquidity", sa.Float(), server_default=sa.text("0")),
        sa.Column(
            "created_at",
            sa.TIMESTAMP(timezone=True),
            server_default=sa.func.now(),
        ),
    )

    # ── tags ────────────────────────────────────────────────────
    op.create_table(
        "tags",
        sa.Column("id", sa.String(), primary_key=True),
        sa.Column("label", sa.Text()),
        sa.Column("slug", sa.String()),
    )

    # ── markets ─────────────────────────────────────────────────
    op.create_table(
        "markets",
        sa.Column("id", sa.String(), primary_key=True),
        sa.Column("question", sa.Text(), nullable=False),
        sa.Column("condition_id", sa.String(), index=True),
        sa.Column("slug", sa.String(), unique=True),
        sa.Column("outcomes", postgresql.ARRAY(sa.String())),
        sa.Column("outcome_prices", postgresql.ARRAY(sa.Float())),
        sa.Column("volume", sa.Float(), server_default=sa.text("0")),
        sa.Column("liquidity", sa.Float(), server_default=sa.text("0")),
        sa.Column("start_date", sa.TIMESTAMP(timezone=True)),
        sa.Column("end_date", sa.TIMESTAMP(timezone=True)),
        sa.Column("closed_time", sa.TIMESTAMP(timezone=True)),
        sa.Column("category", sa.String()),
        sa.Column("resolution_source", sa.Text()),
        sa.Column("description", sa.Text()),
        sa.Column("is_active", sa.Boolean(), server_default=sa.text("true")),
        sa.Column("is_closed", sa.Boolean(), server_default=sa.text("false")),
        sa.Column("is_archived", sa.Boolean(), server_default=sa.text("false")),
        sa.Column("clob_token_ids", postgresql.ARRAY(sa.String())),
        sa.Column(
            "event_id",
            sa.String(),
            sa.ForeignKey("events.id", ondelete="SET NULL"),
            index=True,
        ),
        sa.Column("winning_outcome", sa.String()),
        sa.Column("resolved_to_no", sa.Boolean()),
        sa.Column(
            "created_at",
            sa.TIMESTAMP(timezone=True),
            server_default=sa.func.now(),
        ),
        sa.Column(
            "updated_at",
            sa.TIMESTAMP(timezone=True),
            server_default=sa.func.now(),
        ),
        sa.Column("raw_data", postgresql.JSONB()),
    )

    # ── price_snapshots ─────────────────────────────────────────
    op.create_table(
        "price_snapshots",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column(
            "market_id",
            sa.String(),
            sa.ForeignKey("markets.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("timestamp", sa.TIMESTAMP(timezone=True), nullable=False),
        sa.Column("yes_price", sa.Float()),
        sa.Column("no_price", sa.Float()),
        sa.Column("source", sa.String(), server_default=sa.text("'clob_history'")),
        sa.Column(
            "created_at",
            sa.TIMESTAMP(timezone=True),
            server_default=sa.func.now(),
        ),
    )
    op.create_index(
        "ix_price_snapshots_market_timestamp",
        "price_snapshots",
        ["market_id", "timestamp"],
    )

    # ── market_tags ─────────────────────────────────────────────
    op.create_table(
        "market_tags",
        sa.Column(
            "market_id",
            sa.String(),
            sa.ForeignKey("markets.id", ondelete="CASCADE"),
            primary_key=True,
        ),
        sa.Column(
            "tag_id",
            sa.String(),
            sa.ForeignKey("tags.id", ondelete="CASCADE"),
            primary_key=True,
        ),
    )

    # ── analytics_cache ─────────────────────────────────────────
    op.create_table(
        "analytics_cache",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("metric_name", sa.String(), nullable=False),
        sa.Column("parameters", postgresql.JSONB()),
        sa.Column("result", postgresql.JSONB(), nullable=False),
        sa.Column(
            "computed_at",
            sa.TIMESTAMP(timezone=True),
            server_default=sa.func.now(),
        ),
    )
    op.create_index(
        "ix_analytics_cache_metric_params",
        "analytics_cache",
        ["metric_name", "parameters"],
    )


def downgrade() -> None:
    op.drop_table("analytics_cache")
    op.drop_table("market_tags")
    op.drop_table("price_snapshots")
    op.drop_table("markets")
    op.drop_table("tags")
    op.drop_table("events")
