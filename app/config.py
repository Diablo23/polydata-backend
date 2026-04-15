"""Application configuration via environment variables."""

import os
from functools import lru_cache

from pydantic_settings import BaseSettings


def _derive_db_urls() -> tuple[str, str]:
    """Derive async and sync database URLs from DATABASE_URL env var.
    
    Railway sets DATABASE_URL as postgresql://user:pass@host:port/db
    We need:
      - postgresql+asyncpg://... for async SQLAlchemy
      - postgresql+psycopg2://... for Alembic/sync
    """
    raw = os.environ.get("DATABASE_URL", "postgresql://postgres:postgres@localhost:5432/neh")
    # Handle various prefixes Railway might use
    base = raw
    for prefix in ("postgresql://", "postgres://", "postgresql+asyncpg://", "postgresql+psycopg2://"):
        if base.startswith(prefix):
            base = base[len(prefix):]
            break
    async_url = f"postgresql+asyncpg://{base}"
    sync_url = f"postgresql+psycopg2://{base}"
    return async_url, sync_url


class Settings(BaseSettings):
    # Database — auto-derived from DATABASE_URL
    database_url: str = ""
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
    cors_allow_origins: list[str] = ["*"]

    # App
    app_name: str = "Nothing Ever Happens"
    debug: bool = False
    port: int = 8000

    model_config = {"env_file": ".env", "env_file_encoding": "utf-8"}

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        if not self.database_url or "localhost" in self.database_url:
            async_url, sync_url = _derive_db_urls()
            self.database_url = async_url
            self.database_url_sync = sync_url


@lru_cache
def get_settings() -> Settings:
    return Settings()
