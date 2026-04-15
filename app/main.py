"""FastAPI application entry point."""

import time
from collections import defaultdict
from typing import Any

from fastapi import FastAPI, Request, Response
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from app.api.markets import router as markets_router
from app.api.scanner import router as scanner_router
from app.api.stats import router as stats_router
from app.config import get_settings

settings = get_settings()

app = FastAPI(
    title=settings.app_name,
    description="Polymarket resolution outcome database and analytics platform",
    version="1.0.0",
)

# ── CORS ────────────────────────────────────────────────────────
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ── Rate limiting (in-memory) ───────────────────────────────────
_rate_limit_store: dict[str, list[float]] = defaultdict(list)
RATE_LIMIT = settings.api_rate_limit_per_minute
RATE_WINDOW = 60  # seconds


@app.middleware("http")
async def rate_limit_middleware(request: Request, call_next: Any) -> Response:
    """Simple per-IP rate limiter."""
    if not request.url.path.startswith("/api/"):
        return await call_next(request)

    client_ip = request.client.host if request.client else "unknown"
    now = time.time()

    _rate_limit_store[client_ip] = [
        t for t in _rate_limit_store[client_ip] if now - t < RATE_WINDOW
    ]

    if len(_rate_limit_store[client_ip]) >= RATE_LIMIT:
        retry_after = int(RATE_WINDOW - (now - _rate_limit_store[client_ip][0]))
        return JSONResponse(
            status_code=429,
            content={"detail": "Rate limit exceeded. Try again later."},
            headers={"Retry-After": str(max(1, retry_after))},
        )

    _rate_limit_store[client_ip].append(now)
    return await call_next(request)


# ── Register routers ───────────────────────────────────────────
app.include_router(stats_router)
app.include_router(markets_router)
app.include_router(scanner_router)


# ── Health / root ──────────────────────────────────────────────
@app.get("/health")
async def health_check() -> dict[str, str]:
    return {"status": "ok", "service": settings.app_name}


@app.get("/")
async def root() -> dict[str, str]:
    return {
        "name": settings.app_name,
        "version": "1.0.0",
        "docs": "/docs",
        "api": "/api/v1/stats/overview",
    }
