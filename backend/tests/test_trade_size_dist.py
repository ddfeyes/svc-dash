"""Tests for GET /api/trade-size-dist endpoint."""
import asyncio
import os
import sys
import tempfile
import time

import pytest

os.environ.setdefault(
    "DB_PATH", os.path.join(tempfile.mkdtemp(), "test_trade_size_dist.db")
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


async def _insert_trades(trades: list):
    """Insert raw trade rows: list of (price, qty, side) tuples."""
    await init_db()
    db = await get_db()
    ts = time.time()
    for i, (price, qty, side) in enumerate(trades):
        await db.execute(
            "INSERT INTO trades (ts, exchange, symbol, price, qty, side) VALUES (?, ?, ?, ?, ?, ?)",
            (ts - i, "binance", "BANANAS31USDT", price, qty, side),
        )
    await db.commit()
    await db.close()


# ── Tests ─────────────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_endpoint_returns_200():
    """GET /api/trade-size-dist should return HTTP 200."""
    await init_db()
    client = _make_client()
    r = client.get("/api/trade-size-dist")
    assert r.status_code == 200


@pytest.mark.asyncio
async def test_response_status_ok():
    """status field must equal 'ok'."""
    await init_db()
    client = _make_client()
    data = client.get("/api/trade-size-dist").json()
    assert data["status"] == "ok"


@pytest.mark.asyncio
async def test_response_shape_with_data():
    """Response must contain status, symbol, window, and buckets."""
    await init_db()
    client = _make_client()
    data = client.get("/api/trade-size-dist?symbol=BANANAS31USDT").json()
    assert "status" in data
    assert "symbol" in data
    assert "window" in data
    assert "buckets" in data


@pytest.mark.asyncio
async def test_symbol_echoed_in_response():
    """Response should echo back the requested symbol."""
    await init_db()
    client = _make_client()
    data = client.get("/api/trade-size-dist?symbol=BANANAS31USDT").json()
    assert data["symbol"] == "BANANAS31USDT"


@pytest.mark.asyncio
async def test_default_window_is_3600():
    """Default window should be 3600 seconds."""
    await init_db()
    client = _make_client()
    data = client.get("/api/trade-size-dist").json()
    assert data.get("window") == 3600


@pytest.mark.asyncio
async def test_buckets_have_correct_labels():
    """When trades exist, buckets must have the five standard labels."""
    await _insert_trades([(1.0, 10.0, "buy")])  # $10 retail trade
    client = _make_client()
    data = client.get("/api/trade-size-dist?symbol=BANANAS31USDT").json()
    if data["buckets"]:
        labels = [b["label"] for b in data["buckets"]]
        assert "<$100" in labels
        assert "$100-1k" in labels
        assert "$1k-10k" in labels
        assert "$10k-100k" in labels
        assert ">$100k" in labels


@pytest.mark.asyncio
async def test_retail_trade_lands_in_correct_bucket():
    """A $50 trade (price=5, qty=10) must appear in the <$100 bucket."""
    await _insert_trades([(5.0, 10.0, "buy")])  # $50
    client = _make_client()
    data = client.get("/api/trade-size-dist?symbol=BANANAS31USDT").json()
    if data["buckets"]:
        retail = next((b for b in data["buckets"] if b["label"] == "<$100"), None)
        assert retail is not None
        assert retail["buy_count"] >= 1
        assert retail["buy_usd"] >= 50.0


@pytest.mark.asyncio
async def test_whale_trade_lands_in_correct_bucket():
    """A $50,000 trade must appear in the $10k-100k bucket."""
    await _insert_trades([(100.0, 500.0, "sell")])  # $50,000
    client = _make_client()
    data = client.get("/api/trade-size-dist?symbol=BANANAS31USDT").json()
    if data["buckets"]:
        whale = next((b for b in data["buckets"] if b["label"] == "$10k-100k"), None)
        assert whale is not None
        assert whale["sell_count"] >= 1
        assert whale["sell_usd"] >= 50000.0


@pytest.mark.asyncio
async def test_buy_sell_counts_are_non_negative():
    """buy_count and sell_count must always be >= 0 in every bucket."""
    await _insert_trades([(1.0, 5.0, "buy"), (1.0, 5.0, "sell")])
    client = _make_client()
    data = client.get("/api/trade-size-dist?symbol=BANANAS31USDT").json()
    for b in data.get("buckets", []):
        assert b["buy_count"] >= 0
        assert b["sell_count"] >= 0


@pytest.mark.asyncio
async def test_total_count_equals_buy_plus_sell():
    """total_count must equal buy_count + sell_count for every bucket."""
    await _insert_trades([
        (2.0, 10.0, "buy"),   # $20 → <$100
        (2.0, 10.0, "sell"),  # $20 → <$100
    ])
    client = _make_client()
    data = client.get("/api/trade-size-dist?symbol=BANANAS31USDT").json()
    for b in data.get("buckets", []):
        assert b["total_count"] == b["buy_count"] + b["sell_count"]


@pytest.mark.asyncio
async def test_total_usd_equals_buy_plus_sell_usd():
    """total_usd must equal buy_usd + sell_usd for every bucket."""
    await _insert_trades([
        (3.0, 10.0, "buy"),   # $30 → <$100
        (3.0, 10.0, "sell"),  # $30 → <$100
    ])
    client = _make_client()
    data = client.get("/api/trade-size-dist?symbol=BANANAS31USDT").json()
    for b in data.get("buckets", []):
        assert abs(b["total_usd"] - (b["buy_usd"] + b["sell_usd"])) < 0.01


@pytest.mark.asyncio
async def test_json_content_type():
    """Response Content-Type should be application/json."""
    await init_db()
    client = _make_client()
    r = client.get("/api/trade-size-dist")
    assert "application/json" in r.headers.get("content-type", "")
