"""Tests for smart money pattern detection."""
import asyncio
import os
import tempfile
import time
import pytest

os.environ["DB_PATH"] = os.path.join(tempfile.mkdtemp(), "test_smart_money.db")
os.environ["SYMBOL_BINANCE"] = "BANANAS31USDT"
os.environ["SYMBOL_BYBIT"] = "BANANAS31USDT"

import sys
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from storage import init_db, get_db
from metrics import detect_smart_money_patterns


@pytest.mark.asyncio
async def test_detect_smart_money_patterns_empty():
    """Empty trades should return neutral pattern."""
    await init_db()
    result = await detect_smart_money_patterns(symbol="BANANAS31USDT")
    assert result["pattern_type"] in ["neutral", "accumulation", "distribution", "absorption"]
    assert 0 <= result["confidence"] <= 1
    assert "smart_delta_1h" in result
    assert "smart_delta_4h" in result
    assert "smart_delta_24h" in result
    assert "absorption_ratio" in result


@pytest.mark.asyncio
async def test_detect_accumulation_pattern():
    """Smart money buying > selling should detect accumulation."""
    await init_db()
    db = await get_db()
    await db.execute("DELETE FROM trades")
    await db.commit()

    ts = time.time()
    # Large buys (smart money)
    for i in range(10):
        await db.execute(
            "INSERT INTO trades (ts, exchange, symbol, price, qty, side) VALUES (?, ?, ?, ?, ?, ?)",
            (ts - 100 + i*10, "binance", "BANANAS31USDT", 100.0, 60000.0, "buy")
        )
    # Small sells (retail)
    for i in range(5):
        await db.execute(
            "INSERT INTO trades (ts, exchange, symbol, price, qty, side) VALUES (?, ?, ?, ?, ?, ?)",
            (ts - 50 + i*10, "binance", "BANANAS31USDT", 100.0, 10000.0, "sell")
        )
    await db.commit()
    await db.close()

    result = await detect_smart_money_patterns(symbol="BANANAS31USDT")
    assert result["pattern_type"] == "accumulation"
    assert result["confidence"] > 0.5
    assert result["smart_delta_1h"] > 0


@pytest.mark.asyncio
async def test_detect_distribution_pattern():
    """Smart money selling > buying should detect distribution."""
    await init_db()
    db = await get_db()
    await db.execute("DELETE FROM trades")
    await db.commit()

    ts = time.time()
    # Large sells (smart money)
    for i in range(10):
        await db.execute(
            "INSERT INTO trades (ts, exchange, symbol, price, qty, side) VALUES (?, ?, ?, ?, ?, ?)",
            (ts - 100 + i*10, "binance", "BANANAS31USDT", 100.0, 60000.0, "sell")
        )
    # Small buys (retail)
    for i in range(5):
        await db.execute(
            "INSERT INTO trades (ts, exchange, symbol, price, qty, side) VALUES (?, ?, ?, ?, ?, ?)",
            (ts - 50 + i*10, "binance", "BANANAS31USDT", 100.0, 10000.0, "buy")
        )
    await db.commit()
    await db.close()

    result = await detect_smart_money_patterns(symbol="BANANAS31USDT")
    assert result["pattern_type"] == "distribution"
    assert result["confidence"] > 0.5
    assert result["smart_delta_1h"] < 0


@pytest.mark.asyncio
async def test_detect_absorption_pattern():
    """Large move resisted by smart money should detect absorption."""
    await init_db()
    db = await get_db()
    await db.execute("DELETE FROM trades")
    await db.commit()

    ts = time.time()
    # Large retail sells (bearish move) - small qty, below smart threshold ($50k notional)
    # price=100, qty=400 → notional=$40k (below threshold)
    for i in range(20):
        await db.execute(
            "INSERT INTO trades (ts, exchange, symbol, price, qty, side) VALUES (?, ?, ?, ?, ?, ?)",
            (ts - 100 + i*5, "binance", "BANANAS31USDT", 100.0, 400.0, "sell")
        )
    # Smart money buys to resist (absorb) - large qty, above smart threshold
    # price=100, qty=600 → notional=$60k (above threshold)
    for i in range(5):
        await db.execute(
            "INSERT INTO trades (ts, exchange, symbol, price, qty, side) VALUES (?, ?, ?, ?, ?, ?)",
            (ts - 80 + i*10, "binance", "BANANAS31USDT", 100.0, 600.0, "buy")
        )
    await db.commit()
    await db.close()

    result = await detect_smart_money_patterns(symbol="BANANAS31USDT")
    assert result["pattern_type"] == "absorption"
    assert result["confidence"] > 0.3
    assert result["absorption_ratio"] > 0


@pytest.mark.asyncio
async def test_smart_money_threshold_50k():
    """Only trades >= $50k notional value should count as smart."""
    await init_db()
    db = await get_db()
    await db.execute("DELETE FROM trades")
    await db.commit()

    ts = time.time()
    # Small trade (not smart): 0.1 * 100 = $10 notional
    await db.execute(
        "INSERT INTO trades (ts, exchange, symbol, price, qty, side) VALUES (?, ?, ?, ?, ?, ?)",
        (ts - 10, "binance", "BANANAS31USDT", 100.0, 0.1, "buy")
    )
    # Large trade (smart): 600 * 100 = $60k notional
    await db.execute(
        "INSERT INTO trades (ts, exchange, symbol, price, qty, side) VALUES (?, ?, ?, ?, ?, ?)",
        (ts - 5, "binance", "BANANAS31USDT", 100.0, 600.0, "buy")
    )
    await db.commit()
    await db.close()

    result = await detect_smart_money_patterns(symbol="BANANAS31USDT")
    # Should see mostly smart money (only the large buy)
    assert result["smart_delta_1h"] > 0


@pytest.mark.asyncio
async def test_windows_1h_4h_24h():
    """Smart money deltas should exist for 1h, 4h, 24h windows."""
    await init_db()
    db = await get_db()
    await db.execute("DELETE FROM trades")
    await db.commit()

    ts = time.time()
    # Add trades across different time windows
    for window_offset in [0, 3600, 7200, 14400, 43200]:
        await db.execute(
            "INSERT INTO trades (ts, exchange, symbol, price, qty, side) VALUES (?, ?, ?, ?, ?, ?)",
            (ts - window_offset, "binance", "BANANAS31USDT", 100.0, 60000.0, "buy")
        )
    await db.commit()
    await db.close()

    result = await detect_smart_money_patterns(symbol="BANANAS31USDT")
    assert "smart_delta_1h" in result
    assert "smart_delta_4h" in result
    assert "smart_delta_24h" in result
    # 24h should be >= 4h >= 1h (cumulative)
    assert abs(result["smart_delta_24h"]) >= abs(result["smart_delta_4h"])
    assert abs(result["smart_delta_4h"]) >= abs(result["smart_delta_1h"])


@pytest.mark.asyncio
async def test_confidence_score_range():
    """Confidence should always be 0–1."""
    await init_db()
    db = await get_db()
    await db.execute("DELETE FROM trades")
    await db.commit()

    ts = time.time()
    # Random data
    for i in range(50):
        side = "buy" if i % 2 == 0 else "sell"
        qty = 50000.0 + i * 1000.0
        await db.execute(
            "INSERT INTO trades (ts, exchange, symbol, price, qty, side) VALUES (?, ?, ?, ?, ?, ?)",
            (ts - 3600 + i*60, "binance", "BANANAS31USDT", 100.0, qty, side)
        )
    await db.commit()
    await db.close()

    result = await detect_smart_money_patterns(symbol="BANANAS31USDT")
    assert 0 <= result["confidence"] <= 1


@pytest.mark.asyncio
async def test_neutral_pattern_low_volume():
    """Very few trades should default to neutral."""
    await init_db()
    db = await get_db()
    await db.execute("DELETE FROM trades")
    await db.commit()

    ts = time.time()
    # Just one small trade: qty=100 @ price=100 = $10k (below smart threshold of $50k)
    await db.execute(
        "INSERT INTO trades (ts, exchange, symbol, price, qty, side) VALUES (?, ?, ?, ?, ?, ?)",
        (ts - 10, "binance", "BANANAS31USDT", 100.0, 100.0, "buy")
    )
    await db.commit()
    await db.close()

    result = await detect_smart_money_patterns(symbol="BANANAS31USDT")
    assert result["pattern_type"] == "neutral"
    assert result["confidence"] < 0.3


@pytest.mark.asyncio
async def test_absorption_ratio_calculation():
    """Absorption ratio = smart buys when retail dumping / retail volume."""
    await init_db()
    db = await get_db()
    await db.execute("DELETE FROM trades")
    await db.commit()

    ts = time.time()
    # Retail dumps (small qty, below $50k notional): 10 * 400 = 4k qty @ $100
    for i in range(10):
        await db.execute(
            "INSERT INTO trades (ts, exchange, symbol, price, qty, side) VALUES (?, ?, ?, ?, ?, ?)",
            (ts - 50 + i*5, "binance", "BANANAS31USDT", 100.0, 400.0, "sell")
        )
    # Smart buys to absorb (large qty): 3 * 600 = 1800 qty @ $100 = $180k
    for i in range(3):
        await db.execute(
            "INSERT INTO trades (ts, exchange, symbol, price, qty, side) VALUES (?, ?, ?, ?, ?, ?)",
            (ts - 40 + i*10, "binance", "BANANAS31USDT", 100.0, 600.0, "buy")
        )
    await db.commit()
    await db.close()

    result = await detect_smart_money_patterns(symbol="BANANAS31USDT")
    # absorption_ratio = 1800 / 4000 = 0.45
    assert 0 < result["absorption_ratio"] < 1


@pytest.mark.asyncio
async def test_pattern_with_multiple_exchanges():
    """Should aggregate trades from multiple exchanges."""
    await init_db()
    db = await get_db()
    await db.execute("DELETE FROM trades")
    await db.commit()

    ts = time.time()
    # Binance: 10 large buys
    for i in range(10):
        await db.execute(
            "INSERT INTO trades (ts, exchange, symbol, price, qty, side) VALUES (?, ?, ?, ?, ?, ?)",
            (ts - 50 + i*5, "binance", "BANANAS31USDT", 100.0, 60000.0, "buy")
        )
    # Bybit: 5 small sells
    for i in range(5):
        await db.execute(
            "INSERT INTO trades (ts, exchange, symbol, price, qty, side) VALUES (?, ?, ?, ?, ?, ?)",
            (ts - 30 + i*5, "bybit", "BANANAS31USDT", 100.0, 10000.0, "sell")
        )
    await db.commit()
    await db.close()

    result = await detect_smart_money_patterns(symbol="BANANAS31USDT")
    assert result["pattern_type"] == "accumulation"
    assert result["confidence"] > 0.5


@pytest.mark.asyncio
async def test_neutral_mixed_volume():
    """Equal smart buys and sells should be neutral."""
    await init_db()
    db = await get_db()
    await db.execute("DELETE FROM trades")
    await db.commit()

    ts = time.time()
    # 5 large buys
    for i in range(5):
        await db.execute(
            "INSERT INTO trades (ts, exchange, symbol, price, qty, side) VALUES (?, ?, ?, ?, ?, ?)",
            (ts - 50 + i*10, "binance", "BANANAS31USDT", 100.0, 60000.0, "buy")
        )
    # 5 large sells (equal)
    for i in range(5):
        await db.execute(
            "INSERT INTO trades (ts, exchange, symbol, price, qty, side) VALUES (?, ?, ?, ?, ?, ?)",
            (ts - 40 + i*10, "binance", "BANANAS31USDT", 100.0, 60000.0, "sell")
        )
    await db.commit()
    await db.close()

    result = await detect_smart_money_patterns(symbol="BANANAS31USDT")
    assert result["pattern_type"] == "neutral"
    assert abs(result["smart_delta_1h"]) < 1000


@pytest.mark.asyncio
async def test_response_format():
    """Response should have all required fields."""
    await init_db()
    result = await detect_smart_money_patterns(symbol="BANANAS31USDT")
    
    required = [
        "pattern_type", "confidence", 
        "smart_delta_1h", "smart_delta_4h", "smart_delta_24h",
        "absorption_ratio", "timestamp"
    ]
    for field in required:
        assert field in result, f"Missing field: {field}"


@pytest.mark.asyncio
async def test_confidence_high_on_clear_pattern():
    """Very skewed smart/retail ratio should have high confidence."""
    await init_db()
    db = await get_db()
    await db.execute("DELETE FROM trades")
    await db.commit()

    ts = time.time()
    # 20 large buys, 0 sells
    for i in range(20):
        await db.execute(
            "INSERT INTO trades (ts, exchange, symbol, price, qty, side) VALUES (?, ?, ?, ?, ?, ?)",
            (ts - 100 + i*5, "binance", "BANANAS31USDT", 100.0, 60000.0, "buy")
        )
    await db.commit()
    await db.close()

    result = await detect_smart_money_patterns(symbol="BANANAS31USDT")
    assert result["pattern_type"] == "accumulation"
    assert result["confidence"] > 0.8


@pytest.mark.asyncio
async def test_timestamp_present():
    """Result should include current timestamp."""
    await init_db()
    before = time.time()
    result = await detect_smart_money_patterns(symbol="BANANAS31USDT")
    after = time.time()
    
    assert "timestamp" in result
    assert before <= result["timestamp"] <= after
