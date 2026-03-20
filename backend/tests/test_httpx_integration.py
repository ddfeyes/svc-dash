"""httpx integration tests for 12 FastAPI endpoints.

Tests use httpx.AsyncClient + ASGITransport against a minimal FastAPI app
(router-only, no background WebSocket collectors or pollers).
DB is an isolated temp SQLite file initialised from conftest.py.

Endpoints under test:
  1.  GET /api/symbols
  2.  GET /api/health
  3.  GET /api/ws-stats
  4.  GET /api/ohlcv
  5.  GET /api/trades/recent
  6.  GET /api/tape-speed-tps
  7.  GET /api/aggressor-streak
  8.  GET /api/volume-profile
  9.  GET /api/oi/history
  10. GET /api/funding/history
  11. GET /api/liquidations/recent
  12. GET /api/tape-speed
"""

import os
import sys
import tempfile

import httpx
import pytest

# Set env vars before any backend imports so storage.DB_PATH picks them up.
os.environ.setdefault(
    "DB_PATH", os.path.join(tempfile.mkdtemp(), "test_httpx_integration.db")
)
os.environ.setdefault("SYMBOL_BINANCE", "BANANAS31USDT")
os.environ.setdefault("SYMBOL_BYBIT", "BANANAS31USDT")

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from storage import init_db  # noqa: E402  (must come after env-var setup)

SYMBOL = "BANANAS31USDT"


# ── shared helpers ────────────────────────────────────────────────────────────


def _make_app():
    """Minimal FastAPI app: router only, no lifespan background tasks."""
    from fastapi import FastAPI
    from api import router

    app = FastAPI()
    app.include_router(router)
    return app


def _client(app=None):
    """Return an httpx.AsyncClient backed by ASGITransport."""
    return httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app or _make_app()),
        base_url="http://test",
    )


# ── 1. GET /api/symbols ───────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_symbols_returns_200():
    await init_db()
    async with _client() as c:
        r = await c.get("/api/symbols")
    assert r.status_code == 200


@pytest.mark.asyncio
async def test_symbols_status_ok():
    await init_db()
    async with _client() as c:
        r = await c.get("/api/symbols")
    assert r.json()["status"] == "ok"


@pytest.mark.asyncio
async def test_symbols_returns_list_with_test_symbol():
    await init_db()
    async with _client() as c:
        r = await c.get("/api/symbols")
    body = r.json()
    assert isinstance(body["symbols"], list)
    assert len(body["symbols"]) > 0
    assert SYMBOL in body["symbols"]


# ── 2. GET /api/health ────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_health_returns_200():
    await init_db()
    async with _client() as c:
        r = await c.get("/api/health")
    assert r.status_code == 200


@pytest.mark.asyncio
async def test_health_status_ok():
    await init_db()
    async with _client() as c:
        r = await c.get("/api/health")
    assert r.json()["status"] == "ok"


@pytest.mark.asyncio
async def test_health_schema():
    await init_db()
    async with _client() as c:
        r = await c.get("/api/health")
    data = r.json()
    assert "db_size_mb" in data
    assert "record_counts" in data
    assert "symbols" in data
    assert "symbol_count" in data
    assert isinstance(data["record_counts"], dict)


@pytest.mark.asyncio
async def test_health_record_counts_contains_core_tables():
    await init_db()
    async with _client() as c:
        r = await c.get("/api/health")
    counts = r.json()["record_counts"]
    for table in ("trades", "open_interest", "funding_rate", "liquidations"):
        assert table in counts, f"Missing table key: {table}"
        assert isinstance(counts[table], int)


# ── 3. GET /api/ws-stats ──────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_ws_stats_returns_200():
    await init_db()
    async with _client() as c:
        r = await c.get("/api/ws-stats")
    assert r.status_code == 200


@pytest.mark.asyncio
async def test_ws_stats_status_ok():
    await init_db()
    async with _client() as c:
        r = await c.get("/api/ws-stats")
    assert r.json()["status"] == "ok"


@pytest.mark.asyncio
async def test_ws_stats_schema():
    await init_db()
    async with _client() as c:
        r = await c.get("/api/ws-stats")
    data = r.json()
    assert "connections" in data
    assert "messages_per_sec" in data
    assert "uptime_sec" in data
    assert "total_messages" in data


@pytest.mark.asyncio
async def test_ws_stats_numeric_types():
    await init_db()
    async with _client() as c:
        r = await c.get("/api/ws-stats")
    data = r.json()
    assert isinstance(data["connections"], int) and data["connections"] >= 0
    assert isinstance(data["messages_per_sec"], (int, float))
    assert data["uptime_sec"] > 0


# ── 4. GET /api/ohlcv ─────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_ohlcv_returns_200():
    await init_db()
    async with _client() as c:
        r = await c.get("/api/ohlcv")
    assert r.status_code == 200


@pytest.mark.asyncio
async def test_ohlcv_status_ok():
    await init_db()
    async with _client() as c:
        r = await c.get("/api/ohlcv")
    assert r.json()["status"] == "ok"


@pytest.mark.asyncio
async def test_ohlcv_schema():
    await init_db()
    async with _client() as c:
        r = await c.get("/api/ohlcv")
    data = r.json()
    assert "symbol" in data
    assert "interval" in data
    assert "data" in data
    assert "count" in data
    assert isinstance(data["data"], list)
    assert data["count"] == len(data["data"])


@pytest.mark.asyncio
async def test_ohlcv_with_symbol_param():
    await init_db()
    async with _client() as c:
        r = await c.get(f"/api/ohlcv?symbol={SYMBOL}")
    assert r.status_code == 200
    assert r.json()["symbol"] == SYMBOL


@pytest.mark.asyncio
async def test_ohlcv_interval_param_reflected():
    await init_db()
    async with _client() as c:
        r = await c.get("/api/ohlcv?interval=300&window=7200")
    assert r.status_code == 200
    assert r.json()["interval"] == 300


@pytest.mark.asyncio
async def test_ohlcv_invalid_interval_too_small_returns_422():
    """interval < 10 must be rejected (ge=10 constraint)."""
    await init_db()
    async with _client() as c:
        r = await c.get("/api/ohlcv?interval=5")
    assert r.status_code == 422


# ── 5. GET /api/trades/recent ─────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_trades_recent_returns_200():
    await init_db()
    async with _client() as c:
        r = await c.get("/api/trades/recent")
    assert r.status_code == 200


@pytest.mark.asyncio
async def test_trades_recent_status_ok():
    await init_db()
    async with _client() as c:
        r = await c.get("/api/trades/recent")
    assert r.json()["status"] == "ok"


@pytest.mark.asyncio
async def test_trades_recent_schema():
    await init_db()
    async with _client() as c:
        r = await c.get("/api/trades/recent")
    data = r.json()
    assert "data" in data
    assert "count" in data
    assert isinstance(data["data"], list)
    assert data["count"] == len(data["data"])


@pytest.mark.asyncio
async def test_trades_recent_with_limit_param():
    await init_db()
    async with _client() as c:
        r = await c.get("/api/trades/recent?limit=10")
    assert r.status_code == 200


@pytest.mark.asyncio
async def test_trades_recent_limit_exceeded_returns_422():
    """limit > 1000 must be rejected (le=1000 constraint)."""
    await init_db()
    async with _client() as c:
        r = await c.get("/api/trades/recent?limit=9999")
    assert r.status_code == 422


@pytest.mark.asyncio
async def test_trades_recent_with_symbol_param():
    await init_db()
    async with _client() as c:
        r = await c.get(f"/api/trades/recent?symbol={SYMBOL}")
    assert r.status_code == 200


# ── 6. GET /api/tape-speed-tps ────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_tape_speed_tps_returns_200():
    await init_db()
    async with _client() as c:
        r = await c.get("/api/tape-speed-tps")
    assert r.status_code == 200


@pytest.mark.asyncio
async def test_tape_speed_tps_status_ok():
    await init_db()
    async with _client() as c:
        r = await c.get("/api/tape-speed-tps")
    assert r.json()["status"] == "ok"


@pytest.mark.asyncio
async def test_tape_speed_tps_has_symbol_field():
    await init_db()
    async with _client() as c:
        r = await c.get("/api/tape-speed-tps")
    assert "symbol" in r.json()


@pytest.mark.asyncio
async def test_tape_speed_tps_with_symbol_param():
    await init_db()
    async with _client() as c:
        r = await c.get(f"/api/tape-speed-tps?symbol={SYMBOL}")
    assert r.status_code == 200
    assert r.json()["symbol"] == SYMBOL


# ── 7. GET /api/aggressor-streak ─────────────────────────────────────────────


@pytest.mark.asyncio
async def test_aggressor_streak_returns_200():
    await init_db()
    async with _client() as c:
        r = await c.get("/api/aggressor-streak")
    assert r.status_code == 200


@pytest.mark.asyncio
async def test_aggressor_streak_status_ok():
    await init_db()
    async with _client() as c:
        r = await c.get("/api/aggressor-streak")
    assert r.json()["status"] == "ok"


@pytest.mark.asyncio
async def test_aggressor_streak_has_symbol_field():
    await init_db()
    async with _client() as c:
        r = await c.get("/api/aggressor-streak")
    assert "symbol" in r.json()


@pytest.mark.asyncio
async def test_aggressor_streak_with_symbol_param():
    await init_db()
    async with _client() as c:
        r = await c.get(f"/api/aggressor-streak?symbol={SYMBOL}")
    assert r.status_code == 200
    assert r.json()["symbol"] == SYMBOL


@pytest.mark.asyncio
async def test_aggressor_streak_threshold_param():
    await init_db()
    async with _client() as c:
        r = await c.get("/api/aggressor-streak?threshold=75.0&alert_streak=5")
    assert r.status_code == 200


@pytest.mark.asyncio
async def test_aggressor_streak_threshold_too_low_returns_422():
    """threshold < 50 must be rejected (ge=50.0 constraint)."""
    await init_db()
    async with _client() as c:
        r = await c.get("/api/aggressor-streak?threshold=20.0")
    assert r.status_code == 422


# ── 8. GET /api/volume-profile ────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_volume_profile_returns_200():
    await init_db()
    async with _client() as c:
        r = await c.get("/api/volume-profile")
    assert r.status_code == 200


@pytest.mark.asyncio
async def test_volume_profile_status_ok():
    await init_db()
    async with _client() as c:
        r = await c.get("/api/volume-profile")
    assert r.json()["status"] == "ok"


@pytest.mark.asyncio
async def test_volume_profile_has_symbol_field():
    await init_db()
    async with _client() as c:
        r = await c.get("/api/volume-profile")
    assert "symbol" in r.json()


@pytest.mark.asyncio
async def test_volume_profile_with_symbol_param():
    await init_db()
    async with _client() as c:
        r = await c.get(f"/api/volume-profile?symbol={SYMBOL}")
    assert r.status_code == 200
    assert r.json()["symbol"] == SYMBOL


@pytest.mark.asyncio
async def test_volume_profile_bins_param():
    await init_db()
    async with _client() as c:
        r = await c.get("/api/volume-profile?bins=20&window=1800")
    assert r.status_code == 200


@pytest.mark.asyncio
async def test_volume_profile_bins_too_many_returns_422():
    """bins > 200 must be rejected (le=200 constraint)."""
    await init_db()
    async with _client() as c:
        r = await c.get("/api/volume-profile?bins=999")
    assert r.status_code == 422


# ── 9. GET /api/oi/history ────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_oi_history_returns_200():
    await init_db()
    async with _client() as c:
        r = await c.get("/api/oi/history")
    assert r.status_code == 200


@pytest.mark.asyncio
async def test_oi_history_status_ok():
    await init_db()
    async with _client() as c:
        r = await c.get("/api/oi/history")
    assert r.json()["status"] == "ok"


@pytest.mark.asyncio
async def test_oi_history_schema():
    await init_db()
    async with _client() as c:
        r = await c.get("/api/oi/history")
    data = r.json()
    assert "data" in data
    assert "count" in data
    assert isinstance(data["data"], list)


@pytest.mark.asyncio
async def test_oi_history_with_symbol_param():
    await init_db()
    async with _client() as c:
        r = await c.get(f"/api/oi/history?symbol={SYMBOL}")
    assert r.status_code == 200


@pytest.mark.asyncio
async def test_oi_history_limit_exceeded_returns_422():
    """limit > 2000 must be rejected (le=2000 constraint)."""
    await init_db()
    async with _client() as c:
        r = await c.get("/api/oi/history?limit=99999")
    assert r.status_code == 422


# ── 10. GET /api/funding/history ─────────────────────────────────────────────


@pytest.mark.asyncio
async def test_funding_history_returns_200():
    await init_db()
    async with _client() as c:
        r = await c.get("/api/funding/history")
    assert r.status_code == 200


@pytest.mark.asyncio
async def test_funding_history_status_ok():
    await init_db()
    async with _client() as c:
        r = await c.get("/api/funding/history")
    assert r.json()["status"] == "ok"


@pytest.mark.asyncio
async def test_funding_history_schema():
    await init_db()
    async with _client() as c:
        r = await c.get("/api/funding/history")
    data = r.json()
    assert "data" in data
    assert "count" in data
    assert isinstance(data["data"], list)


@pytest.mark.asyncio
async def test_funding_history_with_symbol_param():
    await init_db()
    async with _client() as c:
        r = await c.get(f"/api/funding/history?symbol={SYMBOL}")
    assert r.status_code == 200


@pytest.mark.asyncio
async def test_funding_history_limit_exceeded_returns_422():
    """limit > 1000 must be rejected (le=1000 constraint)."""
    await init_db()
    async with _client() as c:
        r = await c.get("/api/funding/history?limit=99999")
    assert r.status_code == 422


# ── 11. GET /api/liquidations/recent ─────────────────────────────────────────


@pytest.mark.asyncio
async def test_liquidations_recent_returns_200():
    await init_db()
    async with _client() as c:
        r = await c.get("/api/liquidations/recent")
    assert r.status_code == 200


@pytest.mark.asyncio
async def test_liquidations_recent_status_ok():
    await init_db()
    async with _client() as c:
        r = await c.get("/api/liquidations/recent")
    assert r.json()["status"] == "ok"


@pytest.mark.asyncio
async def test_liquidations_recent_schema():
    await init_db()
    async with _client() as c:
        r = await c.get("/api/liquidations/recent")
    data = r.json()
    assert "data" in data
    assert "count" in data
    assert isinstance(data["data"], list)
    assert data["count"] == len(data["data"])


@pytest.mark.asyncio
async def test_liquidations_recent_with_symbol_param():
    await init_db()
    async with _client() as c:
        r = await c.get(f"/api/liquidations/recent?symbol={SYMBOL}")
    assert r.status_code == 200


@pytest.mark.asyncio
async def test_liquidations_recent_limit_exceeded_returns_422():
    """limit > 500 must be rejected (le=500 constraint)."""
    await init_db()
    async with _client() as c:
        r = await c.get("/api/liquidations/recent?limit=9999")
    assert r.status_code == 422


# ── 12. GET /api/tape-speed ───────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_tape_speed_returns_200():
    await init_db()
    async with _client() as c:
        r = await c.get("/api/tape-speed")
    assert r.status_code == 200


@pytest.mark.asyncio
async def test_tape_speed_status_ok():
    await init_db()
    async with _client() as c:
        r = await c.get("/api/tape-speed")
    assert r.json()["status"] == "ok"


@pytest.mark.asyncio
async def test_tape_speed_has_symbol_field():
    await init_db()
    async with _client() as c:
        r = await c.get("/api/tape-speed")
    assert "symbol" in r.json()


@pytest.mark.asyncio
async def test_tape_speed_with_symbol_param():
    await init_db()
    async with _client() as c:
        r = await c.get(f"/api/tape-speed?symbol={SYMBOL}")
    assert r.status_code == 200
    assert r.json()["symbol"] == SYMBOL


@pytest.mark.asyncio
async def test_tape_speed_window_param():
    await init_db()
    async with _client() as c:
        r = await c.get("/api/tape-speed?window=3600&bucket=120")
    assert r.status_code == 200


@pytest.mark.asyncio
async def test_tape_speed_invalid_window_too_small_returns_422():
    """window < 60 must be rejected (ge=60 constraint)."""
    await init_db()
    async with _client() as c:
        r = await c.get("/api/tape-speed?window=30")
    assert r.status_code == 422
