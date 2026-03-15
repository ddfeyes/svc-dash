"""Tests for storage layer."""
import asyncio
import os
import tempfile
import time
import pytest

# Set test DB before importing storage
os.environ["DB_PATH"] = os.path.join(tempfile.mkdtemp(), "test_storage.db")
os.environ["SYMBOL_BINANCE"] = "BANANAS31USDT"
os.environ["SYMBOL_BYBIT"] = "BANANAS31USDT"

import sys
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from storage import init_db, get_db


@pytest.mark.asyncio
async def test_init_db_creates_tables():
    """init_db should create all required tables without error."""
    await init_db()
    db = await get_db()
    # Check tables exist
    cursor = await db.execute(
        "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
    )
    tables = [row[0] for row in await cursor.fetchall()]
    await db.close()

    assert "trades" in tables
    assert "orderbook_snapshots" in tables
    assert "open_interest" in tables


@pytest.mark.asyncio
async def test_insert_and_read_trade():
    """Should insert a trade and read it back."""
    await init_db()
    db = await get_db()
    
    ts = time.time()
    await db.execute(
        "INSERT INTO trades (ts, exchange, symbol, price, qty, side) VALUES (?, ?, ?, ?, ?, ?)",
        (ts, "binance", "BANANAS31USDT", 0.01, 100.0, "buy")
    )
    await db.commit()
    
    cursor = await db.execute("SELECT * FROM trades WHERE symbol = ?", ("BANANAS31USDT",))
    rows = await cursor.fetchall()
    await db.close()
    
    assert len(rows) >= 1
    assert rows[-1]["price"] == 0.01
    assert rows[-1]["side"] == "buy"


@pytest.mark.asyncio
async def test_insert_open_interest():
    """Should insert OI data and read it back."""
    await init_db()
    db = await get_db()
    
    ts = time.time()
    await db.execute(
        "INSERT INTO open_interest (ts, exchange, symbol, oi_value) VALUES (?, ?, ?, ?)",
        (ts, "binance", "BANANAS31USDT", 50000000.0)
    )
    await db.commit()
    
    cursor = await db.execute("SELECT * FROM open_interest WHERE symbol = ?", ("BANANAS31USDT",))
    rows = await cursor.fetchall()
    await db.close()
    
    assert len(rows) >= 1
    assert rows[-1]["oi_value"] == 50000000.0


@pytest.mark.asyncio
async def test_orderbook_snapshot():
    """Should insert and read orderbook snapshot."""
    await init_db()
    db = await get_db()
    
    ts = time.time()
    await db.execute(
        """INSERT INTO orderbook_snapshots 
           (ts, exchange, symbol, bids, asks, best_bid, best_ask, mid_price, spread, bid_volume, ask_volume, imbalance) 
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (ts, "binance", "BANANAS31USDT", "[]", "[]", 0.01, 0.0101, 0.01005, 0.1, 1000.0, 900.0, 0.05)
    )
    await db.commit()
    
    cursor = await db.execute("SELECT * FROM orderbook_snapshots WHERE symbol = ?", ("BANANAS31USDT",))
    rows = await cursor.fetchall()
    await db.close()
    
    assert len(rows) >= 1
    assert rows[-1]["spread"] == 0.1
