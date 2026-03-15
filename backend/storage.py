"""SQLite storage layer using aiosqlite — multi-symbol."""
import aiosqlite
import asyncio
import os
import time
from typing import Any, Dict, List, Optional

DB_PATH = os.getenv("DB_PATH", "data/bananas31.db")


async def get_db() -> aiosqlite.Connection:
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    db = await aiosqlite.connect(DB_PATH)
    db.row_factory = aiosqlite.Row
    await db.execute("PRAGMA journal_mode=WAL")
    await db.execute("PRAGMA synchronous=NORMAL")
    return db


async def init_db():
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("PRAGMA journal_mode=WAL")
        await db.execute("PRAGMA synchronous=NORMAL")

        await db.execute("""
            CREATE TABLE IF NOT EXISTS orderbook_snapshots (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ts REAL NOT NULL,
                exchange TEXT NOT NULL,
                symbol TEXT NOT NULL,
                bids TEXT NOT NULL,
                asks TEXT NOT NULL,
                best_bid REAL,
                best_ask REAL,
                mid_price REAL,
                spread REAL,
                bid_volume REAL,
                ask_volume REAL,
                imbalance REAL
            )
        """)

        await db.execute("""
            CREATE TABLE IF NOT EXISTS trades (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ts REAL NOT NULL,
                exchange TEXT NOT NULL,
                symbol TEXT NOT NULL,
                price REAL NOT NULL,
                qty REAL NOT NULL,
                side TEXT NOT NULL,
                trade_id TEXT
            )
        """)

        await db.execute("""
            CREATE TABLE IF NOT EXISTS open_interest (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ts REAL NOT NULL,
                exchange TEXT NOT NULL,
                symbol TEXT NOT NULL,
                oi_value REAL NOT NULL,
                oi_contracts REAL
            )
        """)

        await db.execute("""
            CREATE TABLE IF NOT EXISTS funding_rate (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ts REAL NOT NULL,
                exchange TEXT NOT NULL,
                symbol TEXT NOT NULL,
                rate REAL NOT NULL,
                next_funding_ts REAL
            )
        """)

        await db.execute("""
            CREATE TABLE IF NOT EXISTS liquidations (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ts REAL NOT NULL,
                exchange TEXT NOT NULL,
                symbol TEXT NOT NULL,
                side TEXT NOT NULL,
                price REAL NOT NULL,
                qty REAL NOT NULL,
                value REAL
            )
        """)

        # Indexes for time-range queries
        await db.execute("CREATE INDEX IF NOT EXISTS idx_ob_ts ON orderbook_snapshots(ts)")
        await db.execute("CREATE INDEX IF NOT EXISTS idx_ob_sym ON orderbook_snapshots(symbol, ts)")
        await db.execute("CREATE INDEX IF NOT EXISTS idx_trades_ts ON trades(ts)")
        await db.execute("CREATE INDEX IF NOT EXISTS idx_trades_sym ON trades(symbol, ts)")
        await db.execute("CREATE INDEX IF NOT EXISTS idx_oi_ts ON open_interest(ts)")
        await db.execute("CREATE INDEX IF NOT EXISTS idx_oi_sym ON open_interest(symbol, ts)")
        await db.execute("CREATE INDEX IF NOT EXISTS idx_fr_ts ON funding_rate(ts)")
        await db.execute("CREATE INDEX IF NOT EXISTS idx_fr_sym ON funding_rate(symbol, ts)")
        await db.execute("CREATE INDEX IF NOT EXISTS idx_liq_ts ON liquidations(ts)")
        await db.execute("CREATE INDEX IF NOT EXISTS idx_liq_sym ON liquidations(symbol, ts)")

        await db.commit()


async def insert_orderbook(exchange: str, symbol: str, bids: list, asks: list):
    import json
    ts = time.time()
    best_bid = float(bids[0][0]) if bids else None
    best_ask = float(asks[0][0]) if asks else None
    mid_price = (best_bid + best_ask) / 2 if best_bid and best_ask else None
    spread = (best_ask - best_bid) if best_bid and best_ask else None

    bid_vol = sum(float(b[1]) for b in bids[:10])
    ask_vol = sum(float(a[1]) for a in asks[:10])
    imbalance = (bid_vol - ask_vol) / (bid_vol + ask_vol) if (bid_vol + ask_vol) > 0 else 0

    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""
            INSERT INTO orderbook_snapshots
            (ts, exchange, symbol, bids, asks, best_bid, best_ask, mid_price, spread, bid_volume, ask_volume, imbalance)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
        """, (ts, exchange, symbol,
              json.dumps(bids[:20]), json.dumps(asks[:20]),
              best_bid, best_ask, mid_price, spread,
              bid_vol, ask_vol, imbalance))
        await db.commit()


async def insert_trade(exchange: str, symbol: str, price: float, qty: float, side: str, trade_id: str = None):
    ts = time.time()
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""
            INSERT INTO trades (ts, exchange, symbol, price, qty, side, trade_id)
            VALUES (?,?,?,?,?,?,?)
        """, (ts, exchange, symbol, price, qty, side, trade_id))
        await db.commit()


async def insert_oi(exchange: str, symbol: str, oi_value: float, oi_contracts: float = None):
    ts = time.time()
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""
            INSERT INTO open_interest (ts, exchange, symbol, oi_value, oi_contracts)
            VALUES (?,?,?,?,?)
        """, (ts, exchange, symbol, oi_value, oi_contracts))
        await db.commit()


async def insert_funding(exchange: str, symbol: str, rate: float, next_ts: float = None):
    ts = time.time()
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""
            INSERT INTO funding_rate (ts, exchange, symbol, rate, next_funding_ts)
            VALUES (?,?,?,?,?)
        """, (ts, exchange, symbol, rate, next_ts))
        await db.commit()


async def insert_liquidation(exchange: str, symbol: str, side: str, price: float, qty: float):
    ts = time.time()
    value = price * qty
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""
            INSERT INTO liquidations (ts, exchange, symbol, side, price, qty, value)
            VALUES (?,?,?,?,?,?,?)
        """, (ts, exchange, symbol, side, price, qty, value))
        await db.commit()


def _build_query(base: str, filters: list, order: str, limit: int) -> tuple:
    """Helper to build parameterized queries."""
    params = []
    where_parts = []
    for col, val in filters:
        if val is not None:
            where_parts.append(f"{col} = ?")
            params.append(val)
    if where_parts:
        base += " WHERE " + " AND ".join(where_parts)
    base += f" {order} LIMIT ?"
    params.append(limit)
    return base, params


def _build_query_with_since(base: str, since: float, symbol: Optional[str], order: str, limit: int) -> tuple:
    params = [since]
    q = base + " WHERE ts > ?"
    if symbol:
        q += " AND symbol = ?"
        params.append(symbol)
    q += f" {order} LIMIT ?"
    params.append(limit)
    return q, params


async def get_latest_orderbook(
    exchange: str = None,
    symbol: str = None,
    limit: int = 1
) -> List[Dict]:
    q = "SELECT * FROM orderbook_snapshots"
    params = []
    where = []
    if exchange:
        where.append("exchange = ?")
        params.append(exchange)
    if symbol:
        where.append("symbol = ?")
        params.append(symbol)
    if where:
        q += " WHERE " + " AND ".join(where)
    q += " ORDER BY ts DESC LIMIT ?"
    params.append(limit)
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(q, params) as cur:
            rows = await cur.fetchall()
            return [dict(r) for r in rows]


async def get_recent_trades(
    limit: int = 100,
    since: float = None,
    symbol: str = None,
) -> List[Dict]:
    since = since or (time.time() - 300)
    q, params = _build_query_with_since(
        "SELECT * FROM trades", since, symbol, "ORDER BY ts DESC", limit
    )
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(q, params) as cur:
            rows = await cur.fetchall()
            return [dict(r) for r in rows]


async def get_oi_history(
    limit: int = 300,
    since: float = None,
    symbol: str = None,
) -> List[Dict]:
    since = since or (time.time() - 3600)
    q, params = _build_query_with_since(
        "SELECT * FROM open_interest", since, symbol, "ORDER BY ts ASC", limit
    )
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(q, params) as cur:
            rows = await cur.fetchall()
            return [dict(r) for r in rows]


async def get_funding_history(
    limit: int = 100,
    since: float = None,
    symbol: str = None,
) -> List[Dict]:
    since = since or (time.time() - 86400)
    q, params = _build_query_with_since(
        "SELECT * FROM funding_rate", since, symbol, "ORDER BY ts ASC", limit
    )
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(q, params) as cur:
            rows = await cur.fetchall()
            return [dict(r) for r in rows]


async def get_recent_liquidations(
    limit: int = 50,
    since: float = None,
    symbol: str = None,
) -> List[Dict]:
    since = since or (time.time() - 3600)
    q, params = _build_query_with_since(
        "SELECT * FROM liquidations", since, symbol, "ORDER BY ts DESC", limit
    )
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(q, params) as cur:
            rows = await cur.fetchall()
            return [dict(r) for r in rows]


async def get_trades_for_volume_profile(since: float, symbol: str = None, tick_size: float = None) -> List[Dict]:
    """
    Return aggregated volume per price level for volume profile.
    tick_size controls price resolution (default: auto-detected from data).
    If tick_size is None, first fetches price range to pick an appropriate resolution.
    """
    params: list = [since]
    sym_filter = ""
    if symbol:
        sym_filter = " AND symbol = ?"
        params.append(symbol)

    if tick_size is None:
        # Auto-detect: get avg price to pick tick size
        range_q = f"SELECT AVG(price) as avg_price FROM trades WHERE ts > ?{sym_filter}"
        async with aiosqlite.connect(DB_PATH) as db:
            db.row_factory = aiosqlite.Row
            async with db.execute(range_q, params) as cur:
                row = await cur.fetchone()
                avg_price = row["avg_price"] if row and row["avg_price"] else 1.0

        # Pick tick size as ~0.05% of avg price, rounded to a nice number
        import math
        magnitude = 10 ** math.floor(math.log10(avg_price * 0.0005))
        tick_size = round(avg_price * 0.0005 / magnitude) * magnitude
        tick_size = max(tick_size, 1e-8)

    q = f"""
        SELECT
            ROUND(price / ?) * ? AS price_level,
            SUM(qty) AS volume
        FROM trades
        WHERE ts > ?{sym_filter}
        GROUP BY price_level
        ORDER BY price_level ASC
    """
    # Rebuild params with tick_size first (used twice in ROUND), then since, then optional symbol
    tick_params: list = [tick_size, tick_size, since]
    if symbol:
        tick_params.append(symbol)

    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(q, tick_params) as cur:
            rows = await cur.fetchall()
            return [dict(r) for r in rows], tick_size


async def get_orderbook_history(
    limit: int = 60,
    symbol: str = None,
    exchange: str = None,
) -> List[Dict]:
    """Get recent orderbook snapshots for heatmap."""
    params = []
    q = "SELECT ts, bids, asks, mid_price FROM orderbook_snapshots"
    where = []
    if symbol:
        where.append("symbol = ?")
        params.append(symbol)
    if exchange:
        where.append("exchange = ?")
        params.append(exchange)
    if where:
        q += " WHERE " + " AND ".join(where)
    q += " ORDER BY ts DESC LIMIT ?"
    params.append(limit)
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(q, params) as cur:
            rows = await cur.fetchall()
            return [dict(r) for r in reversed(rows)]  # chronological


async def get_trades_for_cvd(since: float, symbol: str = None) -> List[Dict]:
    params: list = [since]
    q = "SELECT ts, price, qty, side FROM trades WHERE ts > ?"
    if symbol:
        q += " AND symbol = ?"
        params.append(symbol)
    q += " ORDER BY ts ASC"
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(q, params) as cur:
            rows = await cur.fetchall()
            return [dict(r) for r in rows]


async def get_orderbook_snapshots_for_heatmap(
    symbol: str, since: float, sample_interval: int = 10
) -> List[Dict]:
    """Get orderbook snapshots sampled every sample_interval seconds for heatmap."""
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        q = """
            SELECT ts, bids, asks, mid_price
            FROM orderbook_snapshots
            WHERE ts > ? AND symbol = ?
            ORDER BY ts ASC
        """
        async with db.execute(q, [since, symbol]) as cur:
            rows = await cur.fetchall()
            rows = [dict(r) for r in rows]

    if not rows:
        return []

    # Sample every sample_interval seconds
    sampled = []
    last_ts = None
    for row in rows:
        if last_ts is None or row["ts"] - last_ts >= sample_interval:
            sampled.append(row)
            last_ts = row["ts"]

    return sampled


async def cleanup_old_data(max_age_seconds: int = 86400 * 7):
    cutoff = time.time() - max_age_seconds
    async with aiosqlite.connect(DB_PATH) as db:
        for table in ["trades", "open_interest", "funding_rate", "liquidations"]:
            await db.execute(f"DELETE FROM {table} WHERE ts < ?", (cutoff,))
        # Keep orderbook more recent (1 hour)
        await db.execute("DELETE FROM orderbook_snapshots WHERE ts < ?", (time.time() - 3600,))
        await db.commit()
