"""Application configuration via environment variables."""

import os
from functools import lru_cache

from pydantic_settings import BaseSettings


def _make_async_url(raw: str) -> str:
    """Convert any postgres URL to asyncpg format."""
    for prefix in ("postgresql+asyncpg://", "postgresql+psycopg2://", "postgresql://", "postgres://"):
        if raw.startswith(prefix):
            raw = raw[len(prefix):]
            break
    return f"postgresql+asyncpg://{raw}"


def _make_sync_url(raw: str) -> str:
    """Convert any postgres URL to psycopg2 format."""
    for prefix in ("postgresql+asyncpg://", "postgresql+psycopg2://", "postgresql://", "postgres://"):
        if raw.startswith(prefix):
            raw = raw[len(prefix):]
            break
    return f"postgresql+psycopg2://{raw}"


class Settings(BaseSettings):
    # Raw DATABASE_URL from Railway (or .env)
    database_url: str = "postgresql://postgres:postgres@localhost:5432/neh"
    database_url_sync: str = ""
    db_pool_size: int = 10
    db_max_overflow: int = 20

    # Polymarket APIs
    gamma_api_url: str = "https://gamma-api.polymarket.com"
    clob_api_url: str = "https://clob.polymarket.com"
    data_api_url: str = "https://data-api.polymarket.com"

    # Crawler settings
    crawler_request_delay_ms: int = 50
    crawler_max_retries: int = 3
    crawler_page_size: int = 100
    sync_interval_minutes: int = 5
    analytics_refresh_minutes: int = 15

    # API settings
    api_rate_limit_per_minute: int = 100

    # App
    app_name: str = "Nothing Ever Happens"
    debug: bool = False
    port: int = 8000

    model_config = {"env_file": ".env", "env_file_encoding": "utf-8"}

    @property
    def async_db_url(self) -> str:
        return _make_async_url(self.database_url)

    @property
    def sync_db_url(self) -> str:
        if self.database_url_sync:
            return _make_sync_url(self.database_url_sync)
        return _make_sync_url(self.database_url)


@lru_cache
def get_settings() -> Settings:
    return Settings()
