"""SQLite storage layer using aiosqlite — multi-symbol."""
import aiosqlite
import asyncio
import json
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

        # Dedicated spread history table for faster queries / less IO
        await db.execute("""
            CREATE TABLE IF NOT EXISTS spread_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ts REAL NOT NULL,
                symbol TEXT NOT NULL,
                exchange TEXT NOT NULL DEFAULT 'binance',
                spread_pct REAL NOT NULL,
                spread_bps REAL NOT NULL,
                spread_abs REAL,
                bid REAL,
                ask REAL,
                mid REAL,
                bid_vol REAL,
                ask_vol REAL
            )
        """)
        await db.execute("CREATE INDEX IF NOT EXISTS idx_spread_ts  ON spread_history(ts)")
        await db.execute("CREATE INDEX IF NOT EXISTS idx_spread_sym ON spread_history(symbol, ts)")

        await db.execute("""
            CREATE TABLE IF NOT EXISTS alert_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ts REAL NOT NULL,
                symbol TEXT NOT NULL,
                alert_type TEXT NOT NULL,
                severity TEXT NOT NULL,
                description TEXT NOT NULL,
                data TEXT
            )
        """)
        await db.execute("CREATE INDEX IF NOT EXISTS idx_alert_ts ON alert_history(ts)")
        await db.execute("CREATE INDEX IF NOT EXISTS idx_alert_sym ON alert_history(symbol, ts)")

        await db.execute("""
            CREATE TABLE IF NOT EXISTS whale_trades (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ts REAL NOT NULL,
                symbol TEXT NOT NULL,
                price REAL NOT NULL,
                qty REAL NOT NULL,
                side TEXT NOT NULL,
                value_usd REAL NOT NULL,
                exchange TEXT DEFAULT 'binance'
            )
        """)
        await db.execute("CREATE INDEX IF NOT EXISTS idx_whale_ts ON whale_trades(ts)")
        await db.execute("CREATE INDEX IF NOT EXISTS idx_whale_sym ON whale_trades(symbol, ts)")

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

    spread_pct = (spread / mid_price * 100) if (spread and mid_price) else None
    spread_bps = round(spread_pct * 100, 4) if spread_pct is not None else None

    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""
            INSERT INTO orderbook_snapshots
            (ts, exchange, symbol, bids, asks, best_bid, best_ask, mid_price, spread, bid_volume, ask_volume, imbalance)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
        """, (ts, exchange, symbol,
              json.dumps(bids[:20]), json.dumps(asks[:20]),
              best_bid, best_ask, mid_price, spread,
              bid_vol, ask_vol, imbalance))
        # Also write to dedicated spread_history table for fast tracker queries
        if spread_pct is not None:
            await db.execute("""
                INSERT INTO spread_history
                (ts, symbol, exchange, spread_pct, spread_bps, spread_abs, bid, ask, mid, bid_vol, ask_vol)
                VALUES (?,?,?,?,?,?,?,?,?,?,?)
            """, (ts, symbol, exchange, round(spread_pct, 6), spread_bps,
                  round(spread, 8) if spread else None,
                  best_bid, best_ask, mid_price, bid_vol, ask_vol))
        await db.commit()


async def insert_spread(
    symbol: str, exchange: str,
    spread_pct: float, spread_bps: float,
    spread_abs: float = None,
    bid: float = None, ask: float = None, mid: float = None,
    bid_vol: float = None, ask_vol: float = None,
):
    """Explicit spread insert (for external callers)."""
    ts = time.time()
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""
            INSERT INTO spread_history
            (ts, symbol, exchange, spread_pct, spread_bps, spread_abs, bid, ask, mid, bid_vol, ask_vol)
            VALUES (?,?,?,?,?,?,?,?,?,?,?)
        """, (ts, symbol, exchange, spread_pct, spread_bps, spread_abs, bid, ask, mid, bid_vol, ask_vol))
        await db.commit()


async def get_spread_history(
    symbol: str,
    since: float = None,
    window: int = 3600,
    exchange: str = None,
    limit: int = 2000,
) -> List[Dict]:
    """Fetch spread history from dedicated table."""
    since = since or (time.time() - window)
    params: list = [since, symbol]
    q = "SELECT ts, symbol, exchange, spread_pct, spread_bps, spread_abs, bid, ask, mid, bid_vol, ask_vol FROM spread_history WHERE ts > ? AND symbol = ?"
    if exchange:
        q += " AND exchange = ?"
        params.append(exchange)
    q += " ORDER BY ts ASC LIMIT ?"
    params.append(limit)
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(q, params) as cur:
            rows = await cur.fetchall()
            return [dict(r) for r in rows]


async def get_spread_stats(symbol: str, window: int = 1800, exchange: str = None) -> Dict:
    """Return current, avg, max spread and alert status."""
    since = time.time() - window
    rows = await get_spread_history(symbol=symbol, since=since, exchange=exchange, limit=5000)
    if not rows:
        return {"symbol": symbol, "count": 0}
    bps_vals = [r["spread_bps"] for r in rows if r["spread_bps"] is not None]
    pct_vals  = [r["spread_pct"] for r in rows if r["spread_pct"] is not None]
    if not bps_vals:
        return {"symbol": symbol, "count": 0}
    current_bps = bps_vals[-1]
    current_pct = pct_vals[-1] if pct_vals else None
    avg_bps = sum(bps_vals) / len(bps_vals)
    max_bps = max(bps_vals)
    min_bps = min(bps_vals)
    p95_bps = sorted(bps_vals)[int(len(bps_vals) * 0.95)]
    alert = None
    # Alert: current > 0.5% spread OR > 2x avg
    if current_pct is not None and current_pct > 0.5:
        alert = {"level": "high", "reason": "spread_pct_threshold",
                 "message": f"Spread {current_pct:.4f}% exceeds 0.5% threshold",
                 "current_pct": round(current_pct, 4), "current_bps": round(current_bps, 2)}
    elif avg_bps > 0 and current_bps > avg_bps * 2:
        alert = {"level": "medium", "reason": "spread_widening",
                 "message": f"Spread widened: {current_bps:.1f} bps (avg {avg_bps:.1f} bps, {current_bps/avg_bps:.1f}x)",
                 "current_bps": round(current_bps, 2), "avg_bps": round(avg_bps, 2),
                 "ratio": round(current_bps / avg_bps, 2)}
    latest = rows[-1]
    return {
        "symbol": symbol,
        "ts": latest["ts"],
        "current_pct": round(current_pct, 6) if current_pct is not None else None,
        "current_bps": round(current_bps, 2),
        "avg_bps": round(avg_bps, 2),
        "max_bps": round(max_bps, 2),
        "min_bps": round(min_bps, 2),
        "p95_bps": round(p95_bps, 2),
        "bid": latest.get("bid"),
        "ask": latest.get("ask"),
        "mid": latest.get("mid"),
        "bid_vol": latest.get("bid_vol"),
        "ask_vol": latest.get("ask_vol"),
        "count": len(rows),
        "window_s": window,
        "alert": alert,
    }


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
            SUM(qty) AS volume,
            SUM(CASE WHEN side IN ('buy','Buy') THEN qty ELSE 0 END) AS buy_vol,
            SUM(CASE WHEN side NOT IN ('buy','Buy') THEN qty ELSE 0 END) AS sell_vol
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


async def get_ohlcv(
    interval_seconds: int = 60,
    window_seconds: int = 3600,
    symbol: str = None,
) -> List[Dict]:
    """Compute OHLCV candles from trades table using SQLite integer bucketing."""
    since = time.time() - window_seconds
    interval = max(1, interval_seconds)

    sym_filter = ""
    params: list = [since, interval, interval]
    if symbol:
        sym_filter = " AND symbol = ?"
        params.append(symbol)

    q = f"""
        SELECT
            CAST(ts / ? AS INTEGER) * ? AS bucket,
            MIN(ts)   AS ts_open,
            MAX(ts)   AS ts_close,
            price     AS open_price,
            price     AS close_price,
            MAX(price) AS high,
            MIN(price) AS low,
            SUM(qty)  AS volume,
            SUM(CASE WHEN side = 'buy'  THEN qty ELSE 0 END) AS buy_volume,
            SUM(CASE WHEN side = 'sell' THEN qty ELSE 0 END) AS sell_volume,
            COUNT(*)  AS trade_count
        FROM trades
        WHERE ts > ?{sym_filter}
        GROUP BY bucket
        ORDER BY bucket ASC
    """
    # reorder params: interval (x2 for bucket calc) first, then since, then symbol
    full_params: list = [interval, interval, since]
    if symbol:
        full_params.append(symbol)

    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        # We need open/close as first/last prices — get them in a separate pass
        async with db.execute(q, full_params) as cur:
            rows = await cur.fetchall()
            buckets = [dict(r) for r in rows]

    if not buckets:
        return []

    # Fetch first/last price per bucket for true open/close
    # Use window functions if SQLite supports them (3.25+), otherwise fallback
    q2 = f"""
        SELECT
            CAST(ts / ? AS INTEGER) * ? AS bucket,
            FIRST_VALUE(price) OVER (PARTITION BY CAST(ts / ? AS INTEGER) ORDER BY ts ASC)  AS open,
            LAST_VALUE(price)  OVER (PARTITION BY CAST(ts / ? AS INTEGER) ORDER BY ts ASC
                ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS close
        FROM trades
        WHERE ts > ?{sym_filter}
        GROUP BY bucket
    """
    try:
        oc_params: list = [interval, interval, interval, interval, since]
        if symbol:
            oc_params.append(symbol)
        async with aiosqlite.connect(DB_PATH) as db:
            db.row_factory = aiosqlite.Row
            async with db.execute(q2, oc_params) as cur:
                oc_rows = await cur.fetchall()
                oc_map = {r["bucket"]: (r["open"], r["close"]) for r in oc_rows}
    except Exception:
        oc_map = {}

    # Compute cumulative VWAP across all candles
    cum_pv = 0.0
    cum_vol = 0.0
    result = []
    for b in buckets:
        bucket = b["bucket"]
        o, c = oc_map.get(bucket, (b["open_price"], b["close_price"]))
        typical_price = (b["high"] + b["low"] + c) / 3.0
        vol = b["volume"] or 0
        cum_pv  += typical_price * vol
        cum_vol += vol
        vwap = cum_pv / cum_vol if cum_vol > 0 else None
        result.append({
            "ts": bucket,
            "open": o,
            "high": b["high"],
            "low": b["low"],
            "close": c,
            "volume": b["volume"],
            "buy_volume": b["buy_volume"],
            "sell_volume": b["sell_volume"],
            "trade_count": b["trade_count"],
            "vwap": round(vwap, 8) if vwap else None,
        })
    return result


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


async def insert_alert(symbol: str, alert_type: str, severity: str, description: str, data: dict = None):
    """Persist a fired alert to history."""
    import json
    ts = time.time()
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""
            INSERT INTO alert_history (ts, symbol, alert_type, severity, description, data)
            VALUES (?,?,?,?,?,?)
        """, (ts, symbol, alert_type, severity, description, json.dumps(data or {})))
        await db.commit()


async def get_alert_history(
    limit: int = 100,
    since: float = None,
    symbol: str = None,
    alert_type: str = None,
) -> List[Dict]:
    """Fetch recent alert history."""
    since = since or (time.time() - 86400)
    params: list = [since]
    q = "SELECT * FROM alert_history WHERE ts > ?"
    if symbol:
        q += " AND symbol = ?"
        params.append(symbol)
    if alert_type:
        q += " AND alert_type = ?"
        params.append(alert_type)
    q += " ORDER BY ts DESC LIMIT ?"
    params.append(limit)
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(q, params) as cur:
            rows = await cur.fetchall()
            return [dict(r) for r in rows]


async def insert_pattern(symbol: str, pattern_type: str, confidence: float, signals: dict, description: str):
    """Persist a detected market pattern to history."""
    import json
    ts = time.time()
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""
            CREATE TABLE IF NOT EXISTS pattern_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ts REAL NOT NULL,
                symbol TEXT NOT NULL,
                pattern_type TEXT NOT NULL,
                confidence REAL NOT NULL,
                signals TEXT NOT NULL,
                description TEXT NOT NULL
            )
        """)
        await db.execute(
            "INSERT INTO pattern_history (ts, symbol, pattern_type, confidence, signals, description) VALUES (?,?,?,?,?,?)",
            (ts, symbol, pattern_type, confidence, json.dumps(signals), description)
        )
        await db.commit()


async def get_pattern_history(
    limit: int = 100,
    since: float = None,
    symbol: str = None,
    pattern_type: str = None,
) -> List[Dict]:
    """Fetch recent pattern detections."""
    import json
    since = since or (time.time() - 86400)
    params: list = [since]
    q = "SELECT * FROM pattern_history WHERE ts > ?"
    if symbol:
        q += " AND symbol = ?"
        params.append(symbol)
    if pattern_type:
        q += " AND pattern_type = ?"
        params.append(pattern_type)
    q += " ORDER BY ts DESC LIMIT ?"
    params.append(limit)
    # Ensure table exists
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""
            CREATE TABLE IF NOT EXISTS pattern_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ts REAL NOT NULL,
                symbol TEXT NOT NULL,
                pattern_type TEXT NOT NULL,
                confidence REAL NOT NULL,
                signals TEXT NOT NULL,
                description TEXT NOT NULL
            )
        """)
        db.row_factory = aiosqlite.Row
        async with db.execute(q, params) as cur:
            rows = await cur.fetchall()
            result = []
            for r in rows:
                d = dict(r)
                try:
                    d["signals"] = json.loads(d["signals"])
                except Exception:
                    pass
                result.append(d)
            return result


async def cleanup_old_data(max_age_seconds: int = 86400 * 7):
    cutoff = time.time() - max_age_seconds
    async with aiosqlite.connect(DB_PATH) as db:
        for table in ["trades", "open_interest", "funding_rate", "liquidations"]:
            await db.execute(f"DELETE FROM {table} WHERE ts < ?", (cutoff,))
        # Keep orderbook only last 30 minutes (enough for heatmap)
        await db.execute("DELETE FROM orderbook_snapshots WHERE ts < ?", (time.time() - 1800,))
        # Keep spread history 4 hours (1s resolution → ~14400 rows/symbol → manageable)
        await db.execute("DELETE FROM spread_history WHERE ts < ?", (time.time() - 14400,))
        # Keep alert history 30 days
        await db.execute("DELETE FROM alert_history WHERE ts < ?", (time.time() - 86400 * 30,))
        await db.commit()
    # Reclaim space (VACUUM requires its own connection)
    async with aiosqlite.connect(DB_PATH) as db2:
        await db2.execute("VACUUM")
        await db2.execute("ANALYZE")


async def insert_whale_trade(symbol: str, price: float, qty: float, side: str, value_usd: float, exchange: str = "binance"):
    """Log a whale trade (value > threshold) to persistent storage."""
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "INSERT INTO whale_trades (ts, symbol, price, qty, side, value_usd, exchange) VALUES (?,?,?,?,?,?,?)",
            (time.time(), symbol, price, qty, side, value_usd, exchange)
        )
        await db.commit()


async def get_whale_trades(limit: int = 100, since: float = None, symbol: str = None, min_usd: float = 50000) -> list:
    """Fetch recent whale trades, optionally filtered by symbol and time window."""
    params = [min_usd]
    q = "SELECT * FROM whale_trades WHERE value_usd >= ?"
    if since:
        q += " AND ts > ?"
        params.append(since)
    if symbol:
        q += " AND symbol = ?"
        params.append(symbol)
    q += " ORDER BY ts DESC LIMIT ?"
    params.append(limit)
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(q, params) as cur:
            rows = await cur.fetchall()
            return [dict(r) for r in rows]


async def insert_phase_snapshot(symbol: str, phase: str, confidence: float, signals: dict, composite_score: float = None):
    """Store a periodic market phase snapshot for historical replay."""
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            """CREATE TABLE IF NOT EXISTS phase_snapshots (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ts REAL NOT NULL,
                symbol TEXT NOT NULL,
                phase TEXT NOT NULL,
                confidence REAL,
                composite_score REAL,
                signals TEXT
            )"""
        )
        await db.execute(
            "INSERT INTO phase_snapshots (ts, symbol, phase, confidence, composite_score, signals) VALUES (?,?,?,?,?,?)",
            (time.time(), symbol, phase, confidence, composite_score,
             json.dumps(signals) if signals else None)
        )
        await db.commit()


async def get_phase_snapshots(
    symbol: str = None,
    since: float = None,
    until: float = None,
    limit: int = 200,
) -> list:
    """Fetch phase snapshots for historical replay."""
    async with aiosqlite.connect(DB_PATH) as db:
        # Ensure table exists
        await db.execute(
            """CREATE TABLE IF NOT EXISTS phase_snapshots (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ts REAL NOT NULL,
                symbol TEXT NOT NULL,
                phase TEXT NOT NULL,
                confidence REAL,
                composite_score REAL,
                signals TEXT
            )"""
        )
        params = []
        q = "SELECT * FROM phase_snapshots WHERE 1=1"
        if symbol:
            q += " AND symbol = ?"
            params.append(symbol)
        if since:
            q += " AND ts >= ?"
            params.append(since)
        if until:
            q += " AND ts <= ?"
            params.append(until)
        q += " ORDER BY ts ASC LIMIT ?"
        params.append(limit)
        db.row_factory = aiosqlite.Row
        async with db.execute(q, params) as cur:
            rows = await cur.fetchall()
            result = []
            for r in rows:
                d = dict(r)
                if d.get("signals"):
                    try:
                        d["signals"] = json.loads(d["signals"])
                    except Exception:
                        pass
                result.append(d)
            return result


async def get_data_freshness() -> Dict:
    """Return last update timestamps per symbol per data type."""
    tables = {
        "trades": "trades",
        "oi": "open_interest",
        "funding": "funding_rate",
        "liquidations": "liquidations",
    }
    result = {}
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        for key, table in tables.items():
            try:
                async with db.execute(
                    f"SELECT symbol, MAX(ts) as last_ts FROM {table} GROUP BY symbol"
                ) as cur:
                    rows = await cur.fetchall()
                    for r in rows:
                        sym = r["symbol"]
                        if sym not in result:
                            result[sym] = {}
                        result[sym][key] = r["last_ts"]
            except Exception:
                pass
    now = time.time()
    # Add age_seconds
    for sym in result:
        for key in list(result[sym].keys()):
            ts = result[sym][key]
            result[sym][f"{key}_age"] = round(now - ts, 1) if ts else None
    return result
