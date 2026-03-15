"""Tests for metrics computation."""
import asyncio
import os
import tempfile
import time
import pytest

os.environ["DB_PATH"] = os.path.join(tempfile.mkdtemp(), "test_metrics.db")
os.environ["SYMBOL_BINANCE"] = "BANANAS31USDT"
os.environ["SYMBOL_BYBIT"] = "BANANAS31USDT"

import sys
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from storage import init_db, get_db
from metrics import compute_cvd, compute_volume_imbalance


@pytest.mark.asyncio
async def test_cvd_empty():
    """CVD with no trades should return empty list."""
    await init_db()
    result = await compute_cvd(window_seconds=3600, symbol="BANANAS31USDT")
    assert isinstance(result, list)


@pytest.mark.asyncio
async def test_volume_imbalance_empty():
    """Volume imbalance with no trades should return zeros."""
    await init_db()
    result = await compute_volume_imbalance(window_seconds=60, symbol="BANANAS31USDT")
    assert result["total_volume"] == 0
    assert result["imbalance"] == 0


@pytest.mark.asyncio
async def test_cvd_with_trades():
    """CVD should accumulate correctly with buy/sell trades."""
    await init_db()
    db = await get_db()
    
    ts = time.time()
    trades = [
        (ts - 10, "binance", "BANANAS31USDT", 0.01, 100.0, "buy"),
        (ts - 5, "binance", "BANANAS31USDT", 0.01, 50.0, "sell"),
        (ts - 1, "binance", "BANANAS31USDT", 0.01, 75.0, "buy"),
    ]
    for t in trades:
        await db.execute(
            "INSERT INTO trades (ts, exchange, symbol, price, qty, side) VALUES (?, ?, ?, ?, ?, ?)", t
        )
    await db.commit()
    await db.close()
    
    result = await compute_cvd(window_seconds=3600, symbol="BANANAS31USDT")
    assert len(result) == 3
    # CVD = +100 - 50 + 75 = +125
    assert result[-1]["cvd"] == 125.0


@pytest.mark.asyncio
async def test_volume_imbalance_with_trades():
    """Volume imbalance should reflect buy/sell ratio."""
    await init_db()
    db = await get_db()
    
    ts = time.time()
    await db.execute(
        "INSERT INTO trades (ts, exchange, symbol, price, qty, side) VALUES (?, ?, ?, ?, ?, ?)",
        (ts - 5, "binance", "BANANAS31USDT", 0.01, 200.0, "buy")
    )
    await db.execute(
        "INSERT INTO trades (ts, exchange, symbol, price, qty, side) VALUES (?, ?, ?, ?, ?, ?)",
        (ts - 3, "binance", "BANANAS31USDT", 0.01, 100.0, "sell")
    )
    await db.commit()
    await db.close()
    
    result = await compute_volume_imbalance(window_seconds=60, symbol="BANANAS31USDT")
    assert result["buy_volume"] >= 200.0
    assert result["sell_volume"] >= 100.0
    assert result["total_volume"] > 0
    # imbalance should be positive (more buys than sells)
    assert result["imbalance"] > 0
