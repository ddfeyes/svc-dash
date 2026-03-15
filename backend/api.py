"""FastAPI REST endpoints — multi-symbol."""
import asyncio
import json
import time
from typing import Optional, Set

from fastapi import APIRouter, Query, WebSocket, WebSocketDisconnect
from fastapi.responses import JSONResponse

from collectors import get_symbols
from storage import (
    get_latest_orderbook,
    get_recent_trades,
    get_oi_history,
    get_funding_history,
    get_recent_liquidations,
    get_orderbook_snapshots_for_heatmap,
    get_ohlcv,
)
from metrics import (
    compute_cvd,
    compute_volume_imbalance,
    compute_oi_momentum,
    classify_market_phase,
    compute_volume_profile,
    detect_delta_divergence,
    detect_large_trades,
    detect_oi_spike,
    detect_liquidation_cascade,
)

router = APIRouter(prefix="/api")

# ── WebSocket connection manager ─────────────────────────────────────────────

class ConnectionManager:
    def __init__(self):
        self._connections: dict[str, Set[WebSocket]] = {}  # symbol -> set of ws

    async def connect(self, ws: WebSocket, symbol: str):
        await ws.accept()
        if symbol not in self._connections:
            self._connections[symbol] = set()
        self._connections[symbol].add(ws)

    def disconnect(self, ws: WebSocket, symbol: str):
        if symbol in self._connections:
            self._connections[symbol].discard(ws)

    async def broadcast(self, symbol: str, data: dict):
        conns = self._connections.get(symbol, set()).copy()
        dead = set()
        for ws in conns:
            try:
                await ws.send_text(json.dumps(data))
            except Exception:
                dead.add(ws)
        for ws in dead:
            self._connections.get(symbol, set()).discard(ws)


manager = ConnectionManager()


@router.get("/symbols")
async def list_symbols():
    """Return all tracked symbols."""
    return {"status": "ok", "symbols": get_symbols()}


@router.get("/orderbook/latest")
async def orderbook_latest(
    exchange: Optional[str] = None,
    symbol: Optional[str] = None,
    limit: int = Query(default=1, le=20)
):
    data = await get_latest_orderbook(exchange=exchange, symbol=symbol, limit=limit)
    return {"status": "ok", "data": data, "count": len(data)}


@router.get("/trades/recent")
async def trades_recent(
    limit: int = Query(default=100, le=1000),
    since: Optional[float] = None,
    symbol: Optional[str] = None,
):
    data = await get_recent_trades(limit=limit, since=since, symbol=symbol)
    return {"status": "ok", "data": data, "count": len(data)}


@router.get("/oi/history")
async def oi_history(
    limit: int = Query(default=300, le=2000),
    since: Optional[float] = None,
    symbol: Optional[str] = None,
):
    data = await get_oi_history(limit=limit, since=since, symbol=symbol)
    return {"status": "ok", "data": data, "count": len(data)}


@router.get("/funding/history")
async def funding_history(
    limit: int = Query(default=100, le=1000),
    since: Optional[float] = None,
    symbol: Optional[str] = None,
):
    data = await get_funding_history(limit=limit, since=since, symbol=symbol)
    return {"status": "ok", "data": data, "count": len(data)}


@router.get("/liquidations/recent")
async def liquidations_recent(
    limit: int = Query(default=50, le=500),
    since: Optional[float] = None,
    symbol: Optional[str] = None,
):
    data = await get_recent_liquidations(limit=limit, since=since, symbol=symbol)
    return {"status": "ok", "data": data, "count": len(data)}


@router.get("/cvd/history")
async def cvd_history(
    window: int = Query(default=3600, le=86400),
    symbol: Optional[str] = None,
):
    syms = get_symbols()
    target = symbol if symbol and symbol in syms else syms[0]
    data = await compute_cvd(window_seconds=window, symbol=target)
    return {"status": "ok", "data": data, "count": len(data)}


@router.get("/volume-profile")
async def volume_profile(
    window: int = Query(default=3600, le=86400),
    bins: int = Query(default=50, le=200),
    symbol: Optional[str] = None,
):
    syms = get_symbols()
    target = symbol if symbol and symbol in syms else syms[0]
    data = await compute_volume_profile(window_seconds=window, bins=bins, symbol=target)
    return {"status": "ok", "symbol": target, **data}


@router.get("/market-depth")
async def market_depth(symbol: Optional[str] = None):
    """
    Returns cumulative bid/ask depth curve from latest orderbook snapshot.
    """
    syms = get_symbols()
    target = symbol if symbol and symbol in syms else syms[0]
    ob = await get_latest_orderbook(symbol=target, limit=1)
    if not ob:
        return {"status": "ok", "symbol": target, "bids": [], "asks": [], "mid_price": None}

    import json as _json
    row = ob[0]
    try:
        raw_bids = _json.loads(row.get("bids", "[]"))
        raw_asks = _json.loads(row.get("asks", "[]"))
    except Exception:
        raw_bids, raw_asks = [], []

    # Build cumulative depth
    cum_bid = 0.0
    depth_bids = []
    for p, q in sorted([[float(x[0]), float(x[1])] for x in raw_bids], key=lambda x: x[0], reverse=True):
        cum_bid += q
        depth_bids.append({"price": p, "qty": round(q, 6), "cum_qty": round(cum_bid, 6)})

    cum_ask = 0.0
    depth_asks = []
    for p, q in sorted([[float(x[0]), float(x[1])] for x in raw_asks], key=lambda x: x[0]):
        cum_ask += q
        depth_asks.append({"price": p, "qty": round(q, 6), "cum_qty": round(cum_ask, 6)})

    return {
        "status": "ok",
        "symbol": target,
        "mid_price": row.get("mid_price"),
        "bids": depth_bids,
        "asks": depth_asks,
        "ts": row.get("ts"),
    }


@router.get("/liq-cascade")
async def liq_cascade(
    window: int = Query(default=60, le=600),
    threshold_usd: float = Query(default=50000, le=10000000),
    symbol: Optional[str] = None,
):
    syms = get_symbols()
    target = symbol if symbol and symbol in syms else syms[0]
    data = await detect_liquidation_cascade(window_seconds=window, threshold_usd=threshold_usd, symbol=target)
    return {"status": "ok", "symbol": target, **data}


@router.get("/oi-spike")
async def oi_spike(
    window: int = Query(default=300, le=3600),
    threshold: float = Query(default=3.0, le=50.0),
    symbol: Optional[str] = None,
):
    syms = get_symbols()
    target = symbol if symbol and symbol in syms else syms[0]
    data = await detect_oi_spike(window_seconds=window, threshold_pct=threshold, symbol=target)
    return {"status": "ok", "symbol": target, **data}


@router.get("/delta-divergence")
async def delta_divergence(
    window: int = Query(default=300, le=3600),
    symbol: Optional[str] = None,
):
    syms = get_symbols()
    target = symbol if symbol and symbol in syms else syms[0]
    data = await detect_delta_divergence(window_seconds=window, symbol=target)
    return {"status": "ok", "symbol": target, **data}


@router.get("/large-trades")
async def large_trades(
    window: int = Query(default=300, le=3600),
    min_usd: float = Query(default=10000, le=1000000),
    symbol: Optional[str] = None,
):
    syms = get_symbols()
    target = symbol if symbol and symbol in syms else syms[0]
    data = await detect_large_trades(window_seconds=window, min_usd=min_usd, symbol=target)
    return {"status": "ok", "symbol": target, **data}


@router.websocket("/ws/{symbol}")
async def websocket_endpoint(ws: WebSocket, symbol: str):
    """
    WebSocket: streams real-time summary every 1s for a given symbol.
    Message format: {"type": "summary", "data": {...}}
    """
    syms = get_symbols()
    if symbol not in syms:
        symbol = syms[0] if syms else "BANANAS31USDT"

    await manager.connect(ws, symbol)
    try:
        while True:
            try:
                phase_task = classify_market_phase(symbol=symbol)
                vol_task = compute_volume_imbalance(window_seconds=60, symbol=symbol)
                oi_task = compute_oi_momentum(window_seconds=300, symbol=symbol)

                phase, vol_imb, oi_mom = await asyncio.gather(phase_task, vol_task, oi_task)

                ob = await get_latest_orderbook(symbol=symbol, limit=1)
                price = ob[0].get("mid_price") if ob else None
                spread = ob[0].get("spread") if ob else None
                imbalance = ob[0].get("imbalance") if ob else None

                funding = await get_funding_history(limit=2, symbol=symbol)
                latest_funding = {}
                for row in funding:
                    latest_funding[row["exchange"]] = row["rate"]

                # Parse raw orderbook for depth
                raw_bids, raw_asks = [], []
                if ob:
                    try:
                        raw_bids = json.loads(ob[0].get("bids", "[]"))
                        raw_asks = json.loads(ob[0].get("asks", "[]"))
                    except Exception:
                        pass

                cum_bid, depth_bids = 0.0, []
                for p, q in sorted([[float(x[0]), float(x[1])] for x in raw_bids], key=lambda x: x[0], reverse=True):
                    cum_bid += q
                    depth_bids.append([round(float(p), 8), round(cum_bid, 6)])

                cum_ask, depth_asks = 0.0, []
                for p, q in sorted([[float(x[0]), float(x[1])] for x in raw_asks], key=lambda x: x[0]):
                    cum_ask += q
                    depth_asks.append([round(float(p), 8), round(cum_ask, 6)])

                msg = {
                    "type": "summary",
                    "ts": time.time(),
                    "symbol": symbol,
                    "price": price,
                    "spread": spread,
                    "orderbook_imbalance": imbalance,
                    "phase": phase,
                    "volume_imbalance": vol_imb,
                    "oi_momentum": oi_mom,
                    "funding_rates": latest_funding,
                    "depth_bids": depth_bids,
                    "depth_asks": depth_asks,
                    "ob_bids": raw_bids[:10],
                    "ob_asks": raw_asks[:10],
                }
                await ws.send_text(json.dumps(msg))

                # Also check for client pings (non-blocking)
                try:
                    data = await asyncio.wait_for(ws.receive_text(), timeout=0.01)
                except asyncio.TimeoutError:
                    pass

                await asyncio.sleep(1.0)

            except WebSocketDisconnect:
                break
            except Exception as e:
                try:
                    await ws.send_text(json.dumps({"type": "error", "message": str(e)}))
                except Exception:
                    break
                await asyncio.sleep(2.0)
    finally:
        manager.disconnect(ws, symbol)


@router.get("/export/{metric}")
async def export_csv(
    metric: str,
    symbol: Optional[str] = None,
    window: int = Query(default=3600, le=86400),
):
    """Export metric data as CSV. metric: trades|oi|funding|liquidations|cvd"""
    import csv
    import io
    from fastapi.responses import StreamingResponse

    syms = get_symbols()
    target = symbol if symbol and symbol in syms else syms[0]
    since = time.time() - window

    if metric == "trades":
        rows = await get_recent_trades(limit=10000, since=since, symbol=target)
        fields = ["ts", "exchange", "symbol", "price", "qty", "side"]
    elif metric == "oi":
        rows = await get_oi_history(limit=10000, since=since, symbol=target)
        fields = ["ts", "exchange", "symbol", "oi_value"]
    elif metric == "funding":
        rows = await get_funding_history(limit=10000, since=since, symbol=target)
        fields = ["ts", "exchange", "symbol", "rate", "next_funding_ts"]
    elif metric == "liquidations":
        rows = await get_recent_liquidations(limit=10000, since=since, symbol=target)
        fields = ["ts", "exchange", "symbol", "side", "price", "qty", "value"]
    elif metric == "cvd":
        rows = await compute_cvd(window_seconds=window, symbol=target)
        fields = ["ts", "price", "cvd", "delta"]
    else:
        return JSONResponse({"error": "Unknown metric. Use: trades|oi|funding|liquidations|cvd"}, status_code=400)

    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=fields, extrasaction="ignore")
    writer.writeheader()
    writer.writerows(rows)
    output.seek(0)

    filename = f"{target}_{metric}_{int(time.time())}.csv"
    return StreamingResponse(
        iter([output.getvalue()]),
        media_type="text/csv",
        headers={"Content-Disposition": f"attachment; filename={filename}"}
    )


@router.get("/orderbook-heatmap")
async def orderbook_heatmap(
    symbol: Optional[str] = None,
    minutes: int = Query(default=5, le=30),
):
    """
    Returns cumulative orderbook volume binned over time for heatmap visualization.
    Y-axis: price levels (±0.5% from mid-price, 20 bins)
    X-axis: time (sampled every 10s)
    """
    import json as _json
    import math

    syms = get_symbols()
    target = symbol if symbol and symbol in syms else syms[0]
    since = time.time() - minutes * 60

    snapshots = await get_orderbook_snapshots_for_heatmap(target, since, sample_interval=10)

    if not snapshots:
        return {
            "status": "ok",
            "symbol": target,
            "mid_price": None,
            "timestamps": [],
            "bid_cumsum": [],
            "ask_cumsum": [],
            "price_levels": [],
        }

    NUM_BINS = 20
    RANGE_PCT = 0.005  # ±0.5%

    # Use latest mid_price as reference
    ref_mid = next((s["mid_price"] for s in reversed(snapshots) if s["mid_price"]), None)
    if not ref_mid:
        return {"status": "ok", "symbol": target, "mid_price": None,
                "timestamps": [], "bid_cumsum": [], "ask_cumsum": [], "price_levels": []}

    ref_mid = float(ref_mid)
    price_low = ref_mid * (1 - RANGE_PCT)
    price_high = ref_mid * (1 + RANGE_PCT)
    bin_size = (price_high - price_low) / NUM_BINS

    # Build price level centers
    price_levels = [round(price_low + (i + 0.5) * bin_size, 8) for i in range(NUM_BINS)]

    timestamps = []
    bid_cumsum = []  # list of NUM_BINS arrays
    ask_cumsum = []

    for snap in snapshots:
        ts = snap["ts"]
        mid = snap["mid_price"]
        if not mid:
            continue

        try:
            raw_bids = _json.loads(snap["bids"] or "[]")
            raw_asks = _json.loads(snap["asks"] or "[]")
        except Exception:
            continue

        # Initialize bins
        bid_bins = [0.0] * NUM_BINS
        ask_bins = [0.0] * NUM_BINS

        for p, q in raw_bids:
            p, q = float(p), float(q)
            if price_low <= p <= price_high:
                idx = min(int((p - price_low) / bin_size), NUM_BINS - 1)
                bid_bins[idx] += q

        for p, q in raw_asks:
            p, q = float(p), float(q)
            if price_low <= p <= price_high:
                idx = min(int((p - price_low) / bin_size), NUM_BINS - 1)
                ask_bins[idx] += q

        # Convert to cumulative (from mid outward)
        mid_bin = min(int((float(mid) - price_low) / bin_size), NUM_BINS - 1)
        mid_bin = max(0, mid_bin)

        # Bids: cumulate downward from mid
        bid_cum = [0.0] * NUM_BINS
        running = 0.0
        for i in range(mid_bin, -1, -1):
            running += bid_bins[i]
            bid_cum[i] = running

        # Asks: cumulate upward from mid
        ask_cum = [0.0] * NUM_BINS
        running = 0.0
        for i in range(mid_bin, NUM_BINS):
            running += ask_bins[i]
            ask_cum[i] = running

        timestamps.append(ts)
        bid_cumsum.append([round(v, 4) for v in bid_cum])
        ask_cumsum.append([round(v, 4) for v in ask_cum])

    return {
        "status": "ok",
        "symbol": target,
        "mid_price": ref_mid,
        "timestamps": timestamps,
        "bid_cumsum": bid_cumsum,    # list[time][bin]
        "ask_cumsum": ask_cumsum,    # list[time][bin]
        "price_levels": price_levels,
    }


@router.get("/trade-flow-heatmap")
async def trade_flow_heatmap(
    symbol: Optional[str] = None,
    minutes: int = Query(default=30, le=120),
    bins: int = Query(default=20, le=50),
    time_buckets: int = Query(default=60, le=120),
):
    """
    Returns trade flow (actual executed trades) binned by time×price.
    Shows buy/sell pressure zones.
    """
    import math

    syms = get_symbols()
    target = symbol if symbol and symbol in syms else syms[0]
    since = time.time() - minutes * 60

    trades = await get_recent_trades(limit=50000, since=since, symbol=target)
    if not trades:
        return {"status": "ok", "symbol": target, "timestamps": [], "price_levels": [],
                "buy_vol": [], "sell_vol": [], "mid_price": None}

    prices = [t["price"] for t in trades if t.get("price")]
    if not prices:
        return {"status": "ok", "symbol": target, "timestamps": [], "price_levels": [],
                "buy_vol": [], "sell_vol": [], "mid_price": None}

    p_low  = min(prices)
    p_high = max(prices)
    p_rng  = p_high - p_low
    if p_rng < 1e-12:
        p_rng = p_low * 0.01

    bin_size = p_rng / bins
    ts_start = since
    ts_end   = time.time()
    ts_range = ts_end - ts_start
    bucket_size = ts_range / time_buckets

    # price_levels: center of each price bin
    price_levels = [round(p_low + (i + 0.5) * bin_size, 8) for i in range(bins)]

    # Initialize grids: [time_bucket][price_bin]
    buy_grid  = [[0.0] * bins for _ in range(time_buckets)]
    sell_grid = [[0.0] * bins for _ in range(time_buckets)]

    for t in trades:
        ts   = t.get("ts", 0)
        p    = t.get("price", 0)
        qty  = t.get("qty", 0)
        side = t.get("side", "")
        val  = p * qty  # USD value

        t_idx = min(int((ts - ts_start) / bucket_size), time_buckets - 1)
        p_idx = min(int((p - p_low) / bin_size), bins - 1)

        if side == "buy":
            buy_grid[t_idx][p_idx]  += val
        else:
            sell_grid[t_idx][p_idx] += val

    # Timestamps for x-axis labels
    timestamps = [round(ts_start + (i + 0.5) * bucket_size) for i in range(time_buckets)]
    mid_price = prices[-1] if prices else None

    return {
        "status": "ok",
        "symbol": target,
        "mid_price": mid_price,
        "timestamps": timestamps,
        "price_levels": price_levels,
        "buy_vol": buy_grid,
        "sell_vol": sell_grid,
    }


@router.get("/ohlcv")
async def ohlcv(
    interval: int = Query(default=60, ge=10, le=3600, description="Candle interval in seconds"),
    window: int = Query(default=3600, le=86400, description="Lookback window in seconds"),
    symbol: Optional[str] = None,
):
    """OHLCV candles from trade data."""
    syms = get_symbols()
    target = symbol if symbol and symbol in syms else syms[0]
    data = await get_ohlcv(interval_seconds=interval, window_seconds=window, symbol=target)
    return {"status": "ok", "symbol": target, "interval": interval, "data": data, "count": len(data)}


@router.get("/multi-summary")
async def multi_summary():
    """Quick stats for all tracked symbols — for overview bar."""
    syms = get_symbols()
    results = {}
    for sym in syms:
        try:
            ob = await get_latest_orderbook(symbol=sym, limit=1)
            price = ob[0].get("mid_price") if ob else None

            # Quick CVD delta
            cvd_data = await compute_cvd(window_seconds=300, symbol=sym)
            cvd_delta = 0
            if cvd_data and len(cvd_data) >= 2:
                cvd_delta = cvd_data[-1]["cvd"] - cvd_data[0]["cvd"]

            # Funding
            funding = await get_funding_history(limit=2, symbol=sym)
            rates = {r["exchange"]: r["rate"] for r in funding}
            avg_funding = sum(rates.values()) / len(rates) if rates else 0

            # Latest OI momentum
            oi_mom = await compute_oi_momentum(window_seconds=300, symbol=sym)
            oi_pct = oi_mom.get("avg_pct_change", 0)

            results[sym] = {
                "price": price,
                "cvd_delta": round(cvd_delta, 0),
                "funding": round(avg_funding, 8),
                "oi_pct": round(oi_pct, 4),
            }
        except Exception as e:
            results[sym] = {"error": str(e)}
    return {"status": "ok", "symbols": results}


@router.get("/metrics/summary")
async def metrics_summary(symbol: Optional[str] = None):
    syms = get_symbols()
    target = symbol if symbol and symbol in syms else syms[0]

    import asyncio
    phase_task = classify_market_phase(symbol=target)
    vol_task = compute_volume_imbalance(window_seconds=60, symbol=target)
    oi_task = compute_oi_momentum(window_seconds=300, symbol=target)

    phase, vol_imb, oi_mom = await asyncio.gather(phase_task, vol_task, oi_task)

    # Latest price from orderbook
    ob = await get_latest_orderbook(symbol=target, limit=1)
    price = ob[0].get("mid_price") if ob else None
    spread = ob[0].get("spread") if ob else None
    imbalance = ob[0].get("imbalance") if ob else None

    # Latest funding
    funding = await get_funding_history(limit=2, symbol=target)
    latest_funding = {}
    for row in funding:
        latest_funding[row["exchange"]] = row["rate"]

    return {
        "status": "ok",
        "ts": time.time(),
        "symbol": target,
        "price": price,
        "spread": spread,
        "orderbook_imbalance": imbalance,
        "phase": phase,
        "volume_imbalance": vol_imb,
        "oi_momentum": oi_mom,
        "funding_rates": latest_funding,
    }
