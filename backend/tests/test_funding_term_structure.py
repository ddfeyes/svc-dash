"""Tests for funding rate term structure analysis (issue #105).

TDD approach: all tests written first, then implementation.
"""

import asyncio
import os
import tempfile
import time
import pytest
import json

import sys
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from storage import init_db, get_db
from metrics import compute_funding_term_structure


async def setup_fresh_db():
    """Clear all funding_rate data from the test database."""
    await init_db()
    db = await get_db()
    await db.execute("DELETE FROM funding_rate")
    await db.commit()
    await db.close()


# ── Basic Functionality ────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_empty_funding_data():
    """With no funding history, should return zero rates and neutral shape."""
    await setup_fresh_db()
    result = await compute_funding_term_structure(symbol="BTCUSDT")
    
    assert result is not None
    assert "rates" in result
    assert "shape" in result
    assert "exhaustion_score" in result
    assert "trend" in result
    
    assert result["rates"]["d1"] == 0.0
    assert result["rates"]["d7"] == 0.0
    assert result["rates"]["d30"] == 0.0
    assert result["shape"] == "flat"
    assert result["exhaustion_score"] == 0.0


@pytest.mark.asyncio
async def test_single_funding_rate():
    """With one funding rate, should handle gracefully."""
    await setup_fresh_db()
    db = await get_db()
    
    ts = time.time()
    await db.execute(
        "INSERT INTO funding_rate (ts, exchange, symbol, rate) VALUES (?, ?, ?, ?)",
        (ts, "binance", "BTCUSDT", 0.0001)
    )
    await db.commit()
    await db.close()
    
    result = await compute_funding_term_structure(symbol="BTCUSDT")
    
    assert result["rates"]["d1"] == 0.0001
    assert isinstance(result["rates"]["d7"], float)
    assert isinstance(result["rates"]["d30"], float)


@pytest.mark.asyncio
async def test_normal_funding_curve():
    """Normal upward curve: d1 < d7 < d30 (contango, longer-dated higher)."""
    await setup_fresh_db()
    db = await get_db()
    
    ts = time.time()
    # Contango: recent low, older high
    rates = [
        (ts - 86400 * 30, "binance", "BTCUSDT", 0.0003),   # old: 0.03%
        (ts - 86400 * 7, "binance", "BTCUSDT", 0.0002),    # mid: 0.02%
        (ts - 3600, "binance", "BTCUSDT", 0.0001),         # recent: 0.01%
        (ts - 7200, "binance", "BTCUSDT", 0.00011),
        (ts - 10800, "binance", "BTCUSDT", 0.00012),
    ]
    for r in rates:
        await db.execute(
            "INSERT INTO funding_rate (ts, exchange, symbol, rate) VALUES (?, ?, ?, ?)",
            r
        )
    await db.commit()
    await db.close()
    
    result = await compute_funding_term_structure(symbol="BTCUSDT")
    
    assert result["shape"] == "normal"


@pytest.mark.asyncio
async def test_inverted_funding_curve():
    """Inverted curve: d1 > d7 > d30 (backwardation, recent higher)."""
    await setup_fresh_db()
    db = await get_db()
    
    ts = time.time()
    # Backwardation: recent high, older low
    rates = [
        (ts - 86400 * 30, "binance", "BTCUSDT", 0.00001),   # old: low
        (ts - 86400 * 25, "binance", "BTCUSDT", 0.000015),
        (ts - 86400 * 20, "binance", "BTCUSDT", 0.00002),
        (ts - 86400 * 15, "binance", "BTCUSDT", 0.000025),
        (ts - 86400 * 10, "binance", "BTCUSDT", 0.0001),
        (ts - 86400 * 8, "binance", "BTCUSDT", 0.00012),
        (ts - 86400, "binance", "BTCUSDT", 0.0004),         # recent: high
        (ts - 43200, "binance", "BTCUSDT", 0.0005),
        (ts - 3600, "binance", "BTCUSDT", 0.0006),
    ]
    for r in rates:
        await db.execute(
            "INSERT INTO funding_rate (ts, exchange, symbol, rate) VALUES (?, ?, ?, ?)",
            r
        )
    await db.commit()
    await db.close()
    
    result = await compute_funding_term_structure(symbol="BTCUSDT")
    
    assert result["shape"] == "inverted"


@pytest.mark.asyncio
async def test_flat_funding_curve():
    """Flat curve: all rates approximately equal."""
    await setup_fresh_db()
    db = await get_db()
    
    ts = time.time()
    # All same value
    rates = [
        (ts - 86400 * 30, "binance", "BTCUSDT", 0.0001),
        (ts - 86400 * 20, "binance", "BTCUSDT", 0.0001),
        (ts - 86400 * 10, "binance", "BTCUSDT", 0.0001),
        (ts - 86400 * 7, "binance", "BTCUSDT", 0.0001),
        (ts - 86400 * 3, "binance", "BTCUSDT", 0.0001),
        (ts - 3600, "binance", "BTCUSDT", 0.0001),
    ]
    for r in rates:
        await db.execute(
            "INSERT INTO funding_rate (ts, exchange, symbol, rate) VALUES (?, ?, ?, ?)",
            r
        )
    await db.commit()
    await db.close()
    
    result = await compute_funding_term_structure(symbol="BTCUSDT")
    
    assert result["shape"] == "flat"


# ── Exhaustion Score ────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_exhaustion_score_positive_extreme():
    """Extreme positive funding indicates long exhaustion."""
    await setup_fresh_db()
    db = await get_db()
    
    ts = time.time()
    rates = [
        (ts - 86400, "binance", "BTCUSDT", 0.001),    # 0.1%
        (ts - 43200, "binance", "BTCUSDT", 0.0015),   # 0.15%
        (ts - 100, "binance", "BTCUSDT", 0.002),      # 0.2%
    ]
    for r in rates:
        await db.execute(
            "INSERT INTO funding_rate (ts, exchange, symbol, rate) VALUES (?, ?, ?, ?)",
            r
        )
    await db.commit()
    await db.close()
    
    result = await compute_funding_term_structure(symbol="BTCUSDT")
    
    assert result["exhaustion_score"] > 0.5


@pytest.mark.asyncio
async def test_exhaustion_score_negative_extreme():
    """Extreme negative funding indicates short exhaustion."""
    await setup_fresh_db()
    db = await get_db()
    
    ts = time.time()
    rates = [
        (ts - 86400, "binance", "BTCUSDT", -0.001),   # -0.1%
        (ts - 43200, "binance", "BTCUSDT", -0.0015),  # -0.15%
        (ts - 100, "binance", "BTCUSDT", -0.002),     # -0.2%
    ]
    for r in rates:
        await db.execute(
            "INSERT INTO funding_rate (ts, exchange, symbol, rate) VALUES (?, ?, ?, ?)",
            r
        )
    await db.commit()
    await db.close()
    
    result = await compute_funding_term_structure(symbol="BTCUSDT")
    
    assert result["exhaustion_score"] > 0.5


@pytest.mark.asyncio
async def test_exhaustion_score_neutral():
    """Near-zero funding should have low exhaustion."""
    await setup_fresh_db()
    db = await get_db()
    
    ts = time.time()
    rates = [
        (ts - 86400, "binance", "BTCUSDT", 0.000001),
        (ts - 43200, "binance", "BTCUSDT", 0.000002),
        (ts - 100, "binance", "BTCUSDT", -0.000001),
    ]
    for r in rates:
        await db.execute(
            "INSERT INTO funding_rate (ts, exchange, symbol, rate) VALUES (?, ?, ?, ?)",
            r
        )
    await db.commit()
    await db.close()
    
    result = await compute_funding_term_structure(symbol="BTCUSDT")
    
    assert result["exhaustion_score"] < 0.1


# ── Trend Detection ────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_trend_uptrend():
    """Rates moving from negative to positive = uptrend."""
    await setup_fresh_db()
    db = await get_db()
    
    ts = time.time()
    rates = [
        (ts - 86400, "binance", "BTCUSDT", -0.0003),
        (ts - 43200, "binance", "BTCUSDT", -0.0001),
        (ts - 3600, "binance", "BTCUSDT", 0.0001),
    ]
    for r in rates:
        await db.execute(
            "INSERT INTO funding_rate (ts, exchange, symbol, rate) VALUES (?, ?, ?, ?)",
            r
        )
    await db.commit()
    await db.close()
    
    result = await compute_funding_term_structure(symbol="BTCUSDT")
    
    assert result["trend"] in ["up", "neutral"]


@pytest.mark.asyncio
async def test_trend_downtrend():
    """Rates moving from positive to negative = downtrend."""
    await setup_fresh_db()
    db = await get_db()
    
    ts = time.time()
    rates = [
        (ts - 86400, "binance", "BTCUSDT", 0.0003),
        (ts - 43200, "binance", "BTCUSDT", 0.0001),
        (ts - 3600, "binance", "BTCUSDT", -0.0001),
    ]
    for r in rates:
        await db.execute(
            "INSERT INTO funding_rate (ts, exchange, symbol, rate) VALUES (?, ?, ?, ?)",
            r
        )
    await db.commit()
    await db.close()
    
    result = await compute_funding_term_structure(symbol="BTCUSDT")
    
    assert result["trend"] in ["down", "neutral"]


# ── Edge Cases ────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_zero_rates():
    """All zero rates."""
    await setup_fresh_db()
    db = await get_db()
    
    ts = time.time()
    for i in range(10):
        await db.execute(
            "INSERT INTO funding_rate (ts, exchange, symbol, rate) VALUES (?, ?, ?, ?)",
            (ts - 86400 + i * 3600, "binance", "BTCUSDT", 0.0)
        )
    await db.commit()
    await db.close()
    
    result = await compute_funding_term_structure(symbol="BTCUSDT")
    
    assert result["rates"]["d1"] == 0.0
    assert result["rates"]["d7"] == 0.0
    assert result["rates"]["d30"] == 0.0
    assert result["exhaustion_score"] == 0.0


@pytest.mark.asyncio
async def test_multiple_symbols():
    """Different symbols computed independently."""
    await setup_fresh_db()
    db = await get_db()
    
    ts = time.time()
    for sym, rate in [("BTCUSDT", 0.0001), ("ETHUSDT", 0.0002)]:
        for i in range(3):
            await db.execute(
                "INSERT INTO funding_rate (ts, exchange, symbol, rate) VALUES (?, ?, ?, ?)",
                (ts - 86400 + i * 43200, "binance", sym, rate)
            )
    await db.commit()
    await db.close()
    
    btc_result = await compute_funding_term_structure(symbol="BTCUSDT")
    eth_result = await compute_funding_term_structure(symbol="ETHUSDT")
    
    # Should be different
    assert btc_result is not None
    assert eth_result is not None
    assert btc_result["rates"]["d1"] != eth_result["rates"]["d1"]


@pytest.mark.asyncio
async def test_response_time_single_symbol():
    """Single symbol should respond < 200ms."""
    await setup_fresh_db()
    db = await get_db()
    
    ts = time.time()
    for i in range(100):
        for exchange in ["binance", "bybit"]:
            await db.execute(
                "INSERT INTO funding_rate (ts, exchange, symbol, rate) VALUES (?, ?, ?, ?)",
                (ts - i * 3600, exchange, "BTCUSDT", 0.0001 + 0.00001 * i)
            )
    await db.commit()
    await db.close()
    
    import time as timer
    start = timer.time()
    result = await compute_funding_term_structure(symbol="BTCUSDT")
    elapsed = timer.time() - start
    
    assert elapsed < 0.2  # <200ms


@pytest.mark.asyncio
async def test_mixed_sign_rates():
    """Rates with mixed positive and negative."""
    await setup_fresh_db()
    db = await get_db()
    
    ts = time.time()
    rates = [
        (ts - 86400, "binance", "BTCUSDT", 0.0002),
        (ts - 43200, "binance", "BTCUSDT", -0.0001),
        (ts - 3600, "binance", "BTCUSDT", 0.00015),
    ]
    for r in rates:
        await db.execute(
            "INSERT INTO funding_rate (ts, exchange, symbol, rate) VALUES (?, ?, ?, ?)",
            r
        )
    await db.commit()
    await db.close()
    
    result = await compute_funding_term_structure(symbol="BTCUSDT")
    
    assert isinstance(result["exhaustion_score"], float)
    assert 0 <= result["exhaustion_score"] <= 1.0
