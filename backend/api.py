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
    insert_alert,
    get_alert_history,
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
    detect_volume_spike,
    detect_liquidation_cascade,
    detect_funding_extreme,
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
    data = await compute_volume_profile(symbol=target, window_seconds=window, bins=bins)
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


@router.get("/funding-extreme")
async def funding_extreme(
    symbol: Optional[str] = None,
    threshold_pct: float = Query(default=0.1, le=5.0),
):
    syms = get_symbols()
    target = symbol if symbol and symbol in syms else syms[0]
    data = await detect_funding_extreme(symbol=target, threshold_pct=threshold_pct)
    return {"status": "ok", "symbol": target, **data}


@router.get("/volume-spike")
async def volume_spike(
    window: int = Query(default=30, le=300),
    baseline: int = Query(default=300, le=3600),
    symbol: Optional[str] = None,
):
    syms = get_symbols()
    target = symbol if symbol and symbol in syms else syms[0]
    from metrics import detect_volume_spike as _dvs
    data = await _dvs(window_seconds=window, baseline_seconds=baseline, symbol=target)
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
                next_funding_ts = None
                for row in funding:
                    latest_funding[row["exchange"]] = row["rate"]
                    if row.get("next_funding_ts") and (next_funding_ts is None or row["next_funding_ts"] > next_funding_ts):
                        next_funding_ts = row["next_funding_ts"]

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

                # Recent trades (last 2s) for tape
                recent_trades = await get_recent_trades(
                    limit=50, since=time.time() - 2.5, symbol=symbol
                )
                # Serialize: only fields needed by tape
                tape_trades = [
                    {"ts": t["ts"], "price": t["price"], "qty": t["qty"], "side": t["side"]}
                    for t in recent_trades
                ]

                # Check & persist alerts (every WS tick, but only save on trigger)
                alert_tasks = await asyncio.gather(
                    detect_delta_divergence(window_seconds=300, symbol=symbol),
                    detect_oi_spike(window_seconds=300, threshold_pct=3.0, symbol=symbol),
                    detect_liquidation_cascade(window_seconds=60, threshold_usd=50000, symbol=symbol),
                    detect_volume_spike(window_seconds=30, baseline_seconds=300, symbol=symbol),
                    detect_funding_extreme(symbol=symbol, threshold_pct=0.1),
                    return_exceptions=True,
                )
                div_result, oi_result, liq_result, vol_result, funding_ex_result = alert_tasks

                fired_alerts = []
                if isinstance(div_result, dict) and div_result.get("divergence") not in ("none", None):
                    sev = "high" if div_result.get("severity", 0) > 0.5 else "medium"
                    fired_alerts.append(("delta_divergence", sev, div_result.get("description", ""), div_result))
                if isinstance(oi_result, dict) and oi_result.get("spike"):
                    fired_alerts.append(("oi_spike", "high", oi_result.get("description", ""), oi_result))
                if isinstance(liq_result, dict) and liq_result.get("cascade"):
                    fired_alerts.append(("liq_cascade", "critical", liq_result.get("description", ""), liq_result))
                if isinstance(vol_result, dict) and vol_result.get("spike"):
                    fired_alerts.append(("volume_spike", "medium", vol_result.get("description", ""), vol_result))
                if isinstance(funding_ex_result, dict) and funding_ex_result.get("extreme"):
                    fired_alerts.append(("funding_extreme", "high", funding_ex_result.get("description", ""), funding_ex_result))

                # Phase change detection
                if not hasattr(ws, "_last_phase"):
                    ws._last_phase = {}
                prev_phase = ws._last_phase.get(symbol)
                cur_phase = phase.get("phase") if isinstance(phase, dict) else None
                if cur_phase and prev_phase and cur_phase != prev_phase:
                    phase_desc = f"Phase change: {prev_phase} → {cur_phase} (conf: {phase.get('confidence', 0):.0%})"
                    fired_alerts.append(("phase_change", "medium", phase_desc, {
                        "from": prev_phase, "to": cur_phase,
                        "confidence": phase.get("confidence"),
                        "signals": phase.get("signals"),
                    }))
                if cur_phase:
                    ws._last_phase[symbol] = cur_phase

                # Deduplicate: only save if no same-type alert in last 60s
                # funding_extreme uses 300s cooldown (fires constantly otherwise)
                cooldowns = {"funding_extreme": 300, "phase_change": 120}
                for a_type, sev, desc, data in fired_alerts:
                    if not hasattr(ws, "_last_alert_ts"):
                        ws._last_alert_ts = {}
                    last = ws._last_alert_ts.get(a_type, 0)
                    cooldown = cooldowns.get(a_type, 60)
                    if time.time() - last > cooldown:
                        await insert_alert(symbol, a_type, sev, desc, data)
                        ws._last_alert_ts[a_type] = time.time()

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
                    "next_funding_ts": next_funding_ts,
                    "funding_extreme": funding_ex_result if isinstance(funding_ex_result, dict) else None,
                    "depth_bids": depth_bids,
                    "depth_asks": depth_asks,
                    "ob_bids": raw_bids[:10],
                    "ob_asks": raw_asks[:10],
                    "recent_trades": tape_trades,
                    "active_alerts": [{"type": a, "severity": s, "description": d} for a, s, d, _ in fired_alerts],
                    # Inline alert details so frontend can update without REST polling
                    "oi_spike": oi_result if isinstance(oi_result, dict) else None,
                    "vol_spike": vol_result if isinstance(vol_result, dict) else None,
                    "liq_cascade": liq_result if isinstance(liq_result, dict) else None,
                    "delta_divergence": div_result if isinstance(div_result, dict) else None,
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


@router.get("/alerts")
async def alert_history_endpoint(
    symbol: Optional[str] = None,
    alert_type: Optional[str] = None,
    limit: int = Query(default=100, le=500),
    since: Optional[float] = None,
):
    """Return persisted alert history."""
    data = await get_alert_history(limit=limit, since=since, symbol=symbol, alert_type=alert_type)
    return {"status": "ok", "data": data, "count": len(data)}


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


async def _sym_summary(sym: str) -> dict:
    """Gather all quick stats for one symbol in parallel."""
    try:
        ob_task       = get_latest_orderbook(symbol=sym, limit=1)
        cvd_task      = compute_cvd(window_seconds=300, symbol=sym)
        funding_task  = get_funding_history(limit=2, symbol=sym)
        oi_task       = compute_oi_momentum(window_seconds=300, symbol=sym)
        candles_task  = get_ohlcv(interval_seconds=3600, window_seconds=86400, symbol=sym)

        ob, cvd_data, funding, oi_mom, candles_24h = await asyncio.gather(
            ob_task, cvd_task, funding_task, oi_task, candles_task,
            return_exceptions=True,
        )

        price = ob[0].get("mid_price") if isinstance(ob, list) and ob else None

        cvd_delta = 0
        if isinstance(cvd_data, list) and len(cvd_data) >= 2:
            cvd_delta = cvd_data[-1]["cvd"] - cvd_data[0]["cvd"]

        avg_funding = 0
        if isinstance(funding, list) and funding:
            rates = [r["rate"] for r in funding]
            avg_funding = sum(rates) / len(rates)

        oi_pct = oi_mom.get("avg_pct_change", 0) if isinstance(oi_mom, dict) else 0

        change_24h = 0.0
        high_24h = None
        low_24h = None
        if isinstance(candles_24h, list) and candles_24h:
            open_24h  = candles_24h[0]["open"]
            close_24h = candles_24h[-1]["close"]
            if open_24h:
                change_24h = (close_24h - open_24h) / open_24h * 100
            high_24h = max(c["high"] for c in candles_24h)
            low_24h  = min(c["low"]  for c in candles_24h)

        return {
            "price": price,
            "cvd_delta": round(cvd_delta, 0),
            "funding": round(avg_funding, 8),
            "oi_pct": round(oi_pct, 4),
            "change_24h": round(change_24h, 4),
            "high_24h": high_24h,
            "low_24h": low_24h,
        }
    except Exception as e:
        return {"error": str(e)}


@router.get("/multi-summary")
async def multi_summary():
    """Quick stats for all tracked symbols — for overview bar."""
    syms = get_symbols()
    summaries = await asyncio.gather(*[_sym_summary(sym) for sym in syms], return_exceptions=True)
    results = {}
    for sym, summary in zip(syms, summaries):
        results[sym] = summary if isinstance(summary, dict) else {"error": str(summary)}
    return {"status": "ok", "symbols": results}


@router.get("/momentum")
async def momentum_table():
    """Price momentum for all symbols: 1h, 4h, 24h change%."""
    syms = get_symbols()

    async def sym_momentum(sym: str):
        try:
            windows = [3600, 14400, 86400]  # 1h, 4h, 24h
            tasks = [get_ohlcv(interval_seconds=300, window_seconds=w, symbol=sym) for w in windows]
            results = await asyncio.gather(*tasks, return_exceptions=True)
            pcts = {}
            for w, candles in zip(["1h", "4h", "24h"], results):
                if isinstance(candles, list) and len(candles) >= 2:
                    o = candles[0]["open"]
                    c = candles[-1]["close"]
                    pcts[w] = round((c - o) / o * 100, 4) if o else 0
                else:
                    pcts[w] = None
            return sym, pcts
        except Exception as e:
            return sym, {"error": str(e)}

    pairs = await asyncio.gather(*[sym_momentum(sym) for sym in syms])
    return {"status": "ok", "symbols": {sym: pcts for sym, pcts in pairs}}


@router.get("/correlations")
async def price_correlations(window: int = Query(default=3600, le=86400)):
    """
    Pearson correlation matrix between all tracked symbols based on 1-min OHLCV close prices.
    Returns matrix[sym_a][sym_b] = correlation coefficient (-1 to 1).
    """
    syms = get_symbols()
    if len(syms) < 2:
        return {"status": "ok", "matrix": {}, "window": window}

    # Fetch 1-min candles for all symbols in parallel
    candle_tasks = [get_ohlcv(interval_seconds=60, window_seconds=window, symbol=sym) for sym in syms]
    all_candles = await asyncio.gather(*candle_tasks, return_exceptions=True)

    # Build time-indexed price series
    price_series = {}
    for sym, candles in zip(syms, all_candles):
        if isinstance(candles, list) and candles:
            price_series[sym] = {c["ts"]: c["close"] for c in candles}

    if len(price_series) < 2:
        return {"status": "ok", "matrix": {}, "window": window}

    # Find common timestamps
    all_ts = set.intersection(*[set(v.keys()) for v in price_series.values()])
    sorted_ts = sorted(all_ts)

    if len(sorted_ts) < 5:
        return {"status": "ok", "matrix": {}, "window": window, "note": "Insufficient data"}

    # Build aligned price arrays
    aligned = {sym: [price_series[sym][ts] for ts in sorted_ts] for sym in price_series}

    def pearson(xs, ys):
        n = len(xs)
        if n < 2:
            return 0
        mx = sum(xs) / n
        my = sum(ys) / n
        num = sum((x - mx) * (y - my) for x, y in zip(xs, ys))
        dx = (sum((x - mx) ** 2 for x in xs)) ** 0.5
        dy = (sum((y - my) ** 2 for y in ys)) ** 0.5
        if dx == 0 or dy == 0:
            return 0
        return round(num / (dx * dy), 4)

    matrix = {}
    for sym_a in price_series:
        matrix[sym_a] = {}
        for sym_b in price_series:
            if sym_a == sym_b:
                matrix[sym_a][sym_b] = 1.0
            else:
                matrix[sym_a][sym_b] = pearson(aligned[sym_a], aligned[sym_b])

    return {
        "status": "ok",
        "matrix": matrix,
        "symbols": list(price_series.keys()),
        "data_points": len(sorted_ts),
        "window": window,
    }


@router.get("/health")
async def health_check():
    """Backend health: DB size, record counts, uptime."""
    import os
    from storage import DB_PATH
    try:
        db_size = os.path.getsize(DB_PATH) if os.path.exists(DB_PATH) else 0
        async with __import__('aiosqlite').connect(DB_PATH) as db:
            counts = {}
            for table in ["trades", "open_interest", "funding_rate", "liquidations", "orderbook_snapshots", "alert_history"]:
                async with db.execute(f"SELECT COUNT(*) FROM {table}") as cur:
                    row = await cur.fetchone()
                    counts[table] = row[0] if row else 0
        syms = get_symbols()
        return {
            "status": "ok",
            "db_size_mb": round(db_size / 1024 / 1024, 2),
            "record_counts": counts,
            "symbols": syms,
            "symbol_count": len(syms),
        }
    except Exception as e:
        return {"status": "error", "error": str(e)}


@router.get("/stats")
async def symbol_stats(symbol: Optional[str] = None):
    """24h price stats: open, high, low, close, volume, change%."""
    syms = get_symbols()
    target = symbol if symbol and symbol in syms else syms[0]

    # Get 24h candles
    candles = await get_ohlcv(interval_seconds=3600, window_seconds=86400, symbol=target)
    if not candles:
        return {"status": "ok", "symbol": target, "stats": None}

    open_price = candles[0]["open"]
    close_price = candles[-1]["close"]
    high = max(c["high"] for c in candles)
    low = min(c["low"] for c in candles)
    volume = sum(c["volume"] for c in candles)
    buy_volume = sum(c["buy_volume"] for c in candles)
    sell_volume = sum(c["sell_volume"] for c in candles)
    change_pct = ((close_price - open_price) / open_price * 100) if open_price else 0

    return {
        "status": "ok",
        "symbol": target,
        "stats": {
            "open": open_price,
            "high": high,
            "low": low,
            "close": close_price,
            "change_pct": round(change_pct, 4),
            "volume": round(volume, 2),
            "buy_volume": round(buy_volume, 2),
            "sell_volume": round(sell_volume, 2),
            "candles": len(candles),
        }
    }


@router.get("/trade-size-dist")
async def trade_size_distribution(
    symbol: Optional[str] = None,
    window: int = Query(default=3600, le=86400),
):
    """
    Trade size distribution by USD bucket: <$100, $100-$1k, $1k-$10k, $10k-$100k, >$100k.
    Returns buy/sell breakdown per bucket.
    """
    syms = get_symbols()
    target = symbol if symbol and symbol in syms else syms[0]
    since = time.time() - window

    trades = await get_recent_trades(limit=100000, since=since, symbol=target)
    if not trades:
        return {"status": "ok", "symbol": target, "buckets": []}

    buckets = [
        {"label": "<$100",     "min": 0,       "max": 100,    "buy_count": 0, "sell_count": 0, "buy_usd": 0, "sell_usd": 0},
        {"label": "$100-1k",   "min": 100,     "max": 1000,   "buy_count": 0, "sell_count": 0, "buy_usd": 0, "sell_usd": 0},
        {"label": "$1k-10k",   "min": 1000,    "max": 10000,  "buy_count": 0, "sell_count": 0, "buy_usd": 0, "sell_usd": 0},
        {"label": "$10k-100k", "min": 10000,   "max": 100000, "buy_count": 0, "sell_count": 0, "buy_usd": 0, "sell_usd": 0},
        {"label": ">$100k",    "min": 100000,  "max": 1e18,   "buy_count": 0, "sell_count": 0, "buy_usd": 0, "sell_usd": 0},
    ]

    for t in trades:
        val = t["price"] * t["qty"]
        is_buy = t["side"] in ("buy", "Buy")
        for b in buckets:
            if b["min"] <= val < b["max"]:
                if is_buy:
                    b["buy_count"] += 1
                    b["buy_usd"] += val
                else:
                    b["sell_count"] += 1
                    b["sell_usd"] += val
                break

    for b in buckets:
        b["buy_usd"]  = round(b["buy_usd"], 2)
        b["sell_usd"] = round(b["sell_usd"], 2)
        b["total_usd"] = round(b["buy_usd"] + b["sell_usd"], 2)
        b["total_count"] = b["buy_count"] + b["sell_count"]

    return {"status": "ok", "symbol": target, "window": window, "buckets": buckets}


@router.get("/support-resistance")
async def support_resistance(
    symbol: Optional[str] = None,
    window: int = Query(default=3600, le=86400),
    sensitivity: float = Query(default=0.003, le=0.05, description="Min price diff to count as new level (as fraction)")
):
    """
    Auto-detect support/resistance levels from local price extrema in trade data.
    Uses peak-finding on 1-min OHLCV data.
    Returns sorted list of price levels with strength (touch count).
    """
    syms = get_symbols()
    target = symbol if symbol and symbol in syms else syms[0]

    candles = await get_ohlcv(interval_seconds=60, window_seconds=window, symbol=target)
    if len(candles) < 10:
        return {"status": "ok", "symbol": target, "levels": []}

    highs  = [c["high"]  for c in candles]
    lows   = [c["low"]   for c in candles]
    closes = [c["close"] for c in candles]

    def find_peaks(series, is_max: bool):
        peaks = []
        for i in range(2, len(series) - 2):
            if is_max:
                if series[i] > series[i-1] and series[i] > series[i-2] and \
                   series[i] > series[i+1] and series[i] > series[i+2]:
                    peaks.append(series[i])
            else:
                if series[i] < series[i-1] and series[i] < series[i-2] and \
                   series[i] < series[i+1] and series[i] < series[i+2]:
                    peaks.append(series[i])
        return peaks

    resistance_peaks = find_peaks(highs, is_max=True)
    support_troughs  = find_peaks(lows, is_max=False)

    current_price = closes[-1] if closes else 0

    # Cluster nearby levels within sensitivity range
    def cluster(levels, sens):
        if not levels:
            return []
        levels_sorted = sorted(levels)
        clusters = []
        cur_cluster = [levels_sorted[0]]
        for p in levels_sorted[1:]:
            if (p - cur_cluster[-1]) / max(cur_cluster[-1], 1e-12) < sens:
                cur_cluster.append(p)
            else:
                clusters.append(cur_cluster)
                cur_cluster = [p]
        clusters.append(cur_cluster)
        # Return center price + touch count
        return [{"price": round(sum(c)/len(c), 8), "touches": len(c)} for c in clusters]

    resistance_levels = cluster(resistance_peaks, sensitivity)
    support_levels    = cluster(support_troughs, sensitivity)

    # Sort by touches (strength) and annotate type
    all_levels = []
    for r in resistance_levels:
        all_levels.append({**r, "type": "resistance", "distance_pct": round((r["price"] - current_price) / current_price * 100, 4) if current_price else 0})
    for s in support_levels:
        all_levels.append({**s, "type": "support", "distance_pct": round((s["price"] - current_price) / current_price * 100, 4) if current_price else 0})

    # Sort by proximity to current price
    all_levels.sort(key=lambda x: abs(x["distance_pct"]))

    return {
        "status": "ok",
        "symbol": target,
        "current_price": current_price,
        "levels": all_levels[:20],  # top 20 closest levels
        "window": window,
    }


@router.get("/microstructure")
async def microstructure(
    symbol: Optional[str] = None,
    window: int = Query(default=300, le=3600),
):
    """
    Market microstructure stats: avg trade size, aggressor ratio, trades/min,
    buy/sell count ratio, tick rule pressure.
    """
    syms = get_symbols()
    target = symbol if symbol and symbol in syms else syms[0]
    since = time.time() - window

    trades = await get_recent_trades(limit=100000, since=since, symbol=target)
    if not trades:
        return {"status": "ok", "symbol": target, "data": None}

    total = len(trades)
    buy_trades  = [t for t in trades if t["side"] in ("buy", "Buy")]
    sell_trades = [t for t in trades if t["side"] not in ("buy", "Buy")]

    buy_count  = len(buy_trades)
    sell_count = len(sell_trades)
    buy_vol    = sum(t["qty"] * t["price"] for t in buy_trades)
    sell_vol   = sum(t["qty"] * t["price"] for t in sell_trades)

    avg_trade_usd = (buy_vol + sell_vol) / total if total > 0 else 0
    avg_buy_usd   = buy_vol / buy_count  if buy_count > 0 else 0
    avg_sell_usd  = sell_vol / sell_count if sell_count > 0 else 0

    trades_per_min = total / (window / 60) if window > 0 else 0
    aggressor_ratio = buy_count / total if total > 0 else 0.5  # >0.5 = buyer aggressor dominant

    # Large trades (>$5k) breakdown
    large_threshold = 5000
    large_buy  = sum(1 for t in buy_trades  if t["qty"] * t["price"] >= large_threshold)
    large_sell = sum(1 for t in sell_trades if t["qty"] * t["price"] >= large_threshold)
    large_total = large_buy + large_sell

    return {
        "status": "ok",
        "symbol": target,
        "window": window,
        "data": {
            "total_trades": total,
            "buy_count": buy_count,
            "sell_count": sell_count,
            "aggressor_ratio": round(aggressor_ratio, 4),
            "trades_per_min": round(trades_per_min, 2),
            "avg_trade_usd": round(avg_trade_usd, 2),
            "avg_buy_usd": round(avg_buy_usd, 2),
            "avg_sell_usd": round(avg_sell_usd, 2),
            "large_buy_count": large_buy,
            "large_sell_count": large_sell,
            "large_total": large_total,
            "large_threshold_usd": large_threshold,
            "buy_usd": round(buy_vol, 2),
            "sell_usd": round(sell_vol, 2),
        }
    }


@router.get("/pivots")
async def pivot_levels(symbol: Optional[str] = None):
    """
    Classic pivot points from previous day's OHLC.
    PP = (H + L + C) / 3
    R1 = 2*PP - L, S1 = 2*PP - H
    R2 = PP + (H - L), S2 = PP - (H - L)
    R3 = H + 2*(PP - L), S3 = L - 2*(H - PP)
    """
    syms = get_symbols()
    target = symbol if symbol and symbol in syms else syms[0]

    # Try to get previous day candles; fall back to all available data
    candles_48h = await get_ohlcv(interval_seconds=3600, window_seconds=48 * 3600, symbol=target)
    if not candles_48h:
        return {"status": "ok", "symbol": target, "pivots": None, "note": "Insufficient data"}

    # Previous day = candles from 48h ago to 24h ago (or all if not enough)
    now = time.time()
    cutoff_start = now - 48 * 3600
    cutoff_end   = now - 24 * 3600
    prev_day = [c for c in candles_48h if cutoff_start <= c["ts"] <= cutoff_end]
    if not prev_day:
        # Fallback: use first half or all candles if < 4h
        half = max(1, len(candles_48h) // 2)
        prev_day = candles_48h[:half] if len(candles_48h) >= 4 else candles_48h

    ph = max(c["high"] for c in prev_day)
    pl = min(c["low"]  for c in prev_day)
    pc = prev_day[-1]["close"]

    pp = (ph + pl + pc) / 3
    r1 = 2 * pp - pl
    s1 = 2 * pp - ph
    r2 = pp + (ph - pl)
    s2 = pp - (ph - pl)
    r3 = ph + 2 * (pp - pl)
    s3 = pl - 2 * (ph - pp)

    def rnd(v): return round(v, 8)

    return {
        "status": "ok",
        "symbol": target,
        "pivots": {
            "pp": rnd(pp),
            "r1": rnd(r1), "r2": rnd(r2), "r3": rnd(r3),
            "s1": rnd(s1), "s2": rnd(s2), "s3": rnd(s3),
        },
        "prev_day": {"high": rnd(ph), "low": rnd(pl), "close": rnd(pc)},
    }


@router.get("/session")
async def session_stats(symbol: Optional[str] = None):
    """
    Intraday trading session stats: current candle, session H/L, VWAP,
    buy/sell ratio, total liq, trade count. Useful as a session summary card.
    """
    syms = get_symbols()
    target = symbol if symbol and symbol in syms else syms[0]
    now = time.time()

    # Session = last 8h (rough Asia/EU/US session overlap)
    session_start = now - 8 * 3600

    candles_1h, trades_1h, liqs_1h = await asyncio.gather(
        get_ohlcv(interval_seconds=300, window_seconds=8 * 3600, symbol=target),
        get_recent_trades(limit=100000, since=session_start, symbol=target),
        get_recent_liquidations(limit=10000, since=session_start, symbol=target),
    )

    # Session H/L/VWAP
    if candles_1h:
        s_high = max(c["high"] for c in candles_1h)
        s_low  = min(c["low"]  for c in candles_1h)
        s_open = candles_1h[0]["open"]
        s_close = candles_1h[-1]["close"]
        s_change = ((s_close - s_open) / s_open * 100) if s_open else 0
        # VWAP from candles
        cum_pv = sum(((c["high"] + c["low"] + c["close"]) / 3) * c["volume"] for c in candles_1h)
        cum_v  = sum(c["volume"] for c in candles_1h)
        vwap = cum_pv / cum_v if cum_v > 0 else None
    else:
        s_high = s_low = s_open = s_close = vwap = None
        s_change = 0

    # Buy/sell breakdown
    buy_vol = sum(t["qty"] * t["price"] for t in trades_1h if t["side"] in ("buy", "Buy"))
    sell_vol = sum(t["qty"] * t["price"] for t in trades_1h if t["side"] not in ("buy", "Buy"))
    total_vol = buy_vol + sell_vol
    buy_pct = (buy_vol / total_vol * 100) if total_vol > 0 else 50

    # Liquidation totals
    liq_long  = sum(l["value"] or 0 for l in liqs_1h if l["side"] != "buy")  # long liq = sell side
    liq_short = sum(l["value"] or 0 for l in liqs_1h if l["side"] == "buy")  # short liq = buy side
    liq_total = liq_long + liq_short

    return {
        "status": "ok",
        "symbol": target,
        "session_hours": 8,
        "session": {
            "high": s_high,
            "low": s_low,
            "open": s_open,
            "close": s_close,
            "change_pct": round(s_change, 4),
            "vwap": round(vwap, 8) if vwap else None,
            "buy_usd": round(buy_vol, 2),
            "sell_usd": round(sell_vol, 2),
            "buy_pct": round(buy_pct, 2),
            "trade_count": len(trades_1h),
            "liq_total_usd": round(liq_total, 2),
            "liq_long_usd": round(liq_long, 2),
            "liq_short_usd": round(liq_short, 2),
        }
    }


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
