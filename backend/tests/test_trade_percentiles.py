"""Tests for GET /api/trade-size-percentiles endpoint."""
import asyncio
import os
import sys
import tempfile
import time

import pytest

os.environ.setdefault(
    "DB_PATH", os.path.join(tempfile.mkdtemp(), "test_trade_pct.db")
)
os.environ.setdefault("SYMBOL_BINANCE", "BANANAS31USDT")
os.environ.setdefault("SYMBOL_BYBIT", "BANANAS31USDT")

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from storage import init_db, get_db


# ── Helpers ───────────────────────────────────────────────────────────────────


def _make_client():
    from fastapi import FastAPI
    from fastapi.testclient import TestClient
    import api as api_module

    app = FastAPI()
    app.include_router(api_module.router)
    return TestClient(app)


async def _insert_trades(n: int = 20, symbol: str = "BANANAS31USDT"):
    """Insert n trades with sizes 1..n into the test DB."""
    await init_db()
    db = await get_db()
    ts = time.time()
    for i in range(1, n + 1):
        await db.execute(
            "INSERT INTO trades (ts, exchange, symbol, price, qty, side) VALUES (?, ?, ?, ?, ?, ?)",
            (ts - i, "binance", symbol, 0.5, float(i), "buy"),
        )
    await db.commit()
    await db.close()


# ── Tests ─────────────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_trade_percentiles_endpoint_exists():
    """GET /api/trade-size-percentiles should return 200."""
    await init_db()
    client = _make_client()
    r = client.get("/api/trade-size-percentiles")
    assert r.status_code == 200


@pytest.mark.asyncio
async def test_trade_percentiles_response_shape_with_no_data():
    """Empty DB should return null percentiles and sample_size=0."""
    await init_db()
    client = _make_client()
    r = client.get("/api/trade-size-percentiles?symbol=BANANAS31USDT")
    data = r.json()
    assert data["status"] == "ok"
    assert "symbol" in data
    assert "sample_size" in data
    assert "p50" in data
    assert "p75" in data
    assert "p90" in data
    assert "p95" in data
    assert "p99" in data
    assert "median_usd" in data


@pytest.mark.asyncio
async def test_trade_percentiles_empty_returns_nulls():
    """With no trades, all percentiles are None."""
    await init_db()
    client = _make_client()
    # Use a symbol with no trades
    r = client.get("/api/trade-size-percentiles?symbol=BANANAS31USDT")
    data = r.json()
    if data["sample_size"] == 0:
        assert data["p50"] is None
        assert data["p99"] is None
        assert data["median_usd"] is None


@pytest.mark.asyncio
async def test_trade_percentiles_with_data():
    """After inserting trades, percentiles should be non-null."""
    await _insert_trades(50)
    client = _make_client()
    r = client.get("/api/trade-size-percentiles?symbol=BANANAS31USDT")
    data = r.json()
    assert data["sample_size"] > 0
    assert data["p50"] is not None
    assert data["p75"] is not None
    assert data["p90"] is not None
    assert data["p95"] is not None
    assert data["p99"] is not None
    assert data["median_usd"] is not None


@pytest.mark.asyncio
async def test_trade_percentiles_ordering():
    """p50 <= p75 <= p90 <= p95 <= p99."""
    await _insert_trades(100)
    client = _make_client()
    data = client.get("/api/trade-size-percentiles?symbol=BANANAS31USDT").json()
    if data["sample_size"] > 0:
        assert data["p50"] <= data["p75"]
        assert data["p75"] <= data["p90"]
        assert data["p90"] <= data["p95"]
        assert data["p95"] <= data["p99"]


@pytest.mark.asyncio
async def test_trade_percentiles_sample_size_capped_at_1000():
    """sample_size should never exceed 1000 (last 1000 trades)."""
    await _insert_trades(100)
    client = _make_client()
    data = client.get("/api/trade-size-percentiles?symbol=BANANAS31USDT").json()
    assert data["sample_size"] <= 1000


@pytest.mark.asyncio
async def test_trade_percentiles_symbol_in_response():
    """Response should echo back the requested symbol."""
    await init_db()
    client = _make_client()
    data = client.get("/api/trade-size-percentiles?symbol=BANANAS31USDT").json()
    assert data["symbol"] == "BANANAS31USDT"


@pytest.mark.asyncio
async def test_trade_percentiles_status_ok():
    """status field must equal 'ok'."""
    await init_db()
    client = _make_client()
    data = client.get("/api/trade-size-percentiles").json()
    assert data["status"] == "ok"


@pytest.mark.asyncio
async def test_trade_percentiles_median_usd_positive():
    """median_usd should be positive when trades exist."""
    await _insert_trades(20)
    client = _make_client()
    data = client.get("/api/trade-size-percentiles?symbol=BANANAS31USDT").json()
    if data["sample_size"] > 0:
        assert data["median_usd"] > 0


@pytest.mark.asyncio
async def test_trade_percentiles_p50_is_median():
    """For a uniform distribution 1..N, p50 should be near the midpoint."""
    await _insert_trades(100)
    client = _make_client()
    data = client.get("/api/trade-size-percentiles?symbol=BANANAS31USDT").json()
    if data["sample_size"] > 0 and data["p50"] is not None:
        # midpoint of 1..100 ≈ 50
        assert 40.0 <= data["p50"] <= 60.0


@pytest.mark.asyncio
async def test_trade_percentiles_p99_gte_p50():
    """p99 must always be >= p50."""
    await _insert_trades(50)
    client = _make_client()
    data = client.get("/api/trade-size-percentiles?symbol=BANANAS31USDT").json()
    if data["sample_size"] > 0:
        assert data["p99"] >= data["p50"]


@pytest.mark.asyncio
async def test_calc_percentile_helper():
    """_calc_percentile on known data returns expected values."""
    from api import _calc_percentile

    vals = list(range(1, 101))  # 1..100
    # p50 of 1..100 = 50.5
    p50 = _calc_percentile(vals, 50)
    assert abs(p50 - 50.5) < 0.5
    # p100 = last element
    p100 = _calc_percentile(vals, 100)
    assert p100 == 100.0
    # p0 = first element
    p0 = _calc_percentile(vals, 0)
    assert p0 == 1.0


@pytest.mark.asyncio
async def test_calc_percentile_single_element():
    """_calc_percentile on a 1-element list always returns that element."""
    from api import _calc_percentile

    assert _calc_percentile([42.0], 50) == 42.0
    assert _calc_percentile([42.0], 99) == 42.0


@pytest.mark.asyncio
async def test_calc_percentile_empty_returns_zero():
    """_calc_percentile on empty list returns 0.0."""
    from api import _calc_percentile

    assert _calc_percentile([], 50) == 0.0


@pytest.mark.asyncio
async def test_trade_percentiles_json_content_type():
    """Response Content-Type should be application/json."""
    await init_db()
    client = _make_client()
    r = client.get("/api/trade-size-percentiles")
    assert "application/json" in r.headers.get("content-type", "")


@pytest.mark.asyncio
async def test_trade_percentiles_default_symbol_used():
    """Without ?symbol param the endpoint should still return status ok."""
    await init_db()
    client = _make_client()
    r = client.get("/api/trade-size-percentiles")
    assert r.status_code == 200
    assert r.json()["status"] == "ok"
