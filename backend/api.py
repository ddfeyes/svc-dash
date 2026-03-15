"""FastAPI REST endpoints — multi-symbol."""
import asyncio
import json
import time
from typing import Optional, Set
import aiosqlite

from fastapi import APIRouter, Query, WebSocket, WebSocketDisconnect
from fastapi.responses import JSONResponse

from collectors import get_symbols
import storage
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
    get_whale_trades,
    insert_pattern,
    get_pattern_history,
    get_phase_snapshots,
    get_data_freshness,
    get_spread_history,
    get_spread_stats,
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
    detect_cvd_momentum,
    compute_market_regime,
    detect_accumulation_distribution_pattern,
    detect_cross_symbol_oi_spike,
    detect_funding_arbitrage,
    compute_vwap_deviation,
    fetch_oi_mcap_ratio,
    predict_liquidation_cascade,
    detect_funding_divergence,
    compute_oi_concentration,
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


class AlertManager:
    """Fan-out alert events to /ws/alerts subscribers."""
    def __init__(self):
        self._clients: Set[WebSocket] = set()

    async def connect(self, ws: WebSocket):
        await ws.accept()
        self._clients.add(ws)

    def disconnect(self, ws: WebSocket):
        self._clients.discard(ws)

    async def broadcast(self, alert: dict):
        dead = set()
        for ws in self._clients.copy():
            try:
                await ws.send_text(json.dumps(alert))
            except Exception:
                dead.add(ws)
        for ws in dead:
            self._clients.discard(ws)


alert_manager = AlertManager()


@router.websocket("/ws/alerts")
async def ws_alerts(ws: WebSocket):
    """WebSocket: real-time alert push. Sends {type:'alert', ts, symbol, alert_type, severity, description}."""
    await alert_manager.connect(ws)
    try:
        # Send recent alerts on connect
        recent = await get_alert_history(limit=20)
        for a in recent:
            await ws.send_text(json.dumps({"type": "alert_history", **a}))
        # Keep alive
        while True:
            try:
                await asyncio.wait_for(ws.receive_text(), timeout=30)
            except asyncio.TimeoutError:
                await ws.send_text(json.dumps({"type": "ping"}))
    except WebSocketDisconnect:
        alert_manager.disconnect(ws)
    except Exception:
        alert_manager.disconnect(ws)


@router.get("/symbols")
async def list_symbols():
    """Return all tracked symbols."""
    return {"status": "ok", "symbols": get_symbols()}


@router.get("/freshness")
async def data_freshness():
    """Return last update timestamps per symbol per data type."""
    data = await get_data_freshness()
    return {"status": "ok", "freshness": data}


@router.get("/stats/summary")
async def stats_summary():
    """24h aggregate stats for all symbols."""
    import aiosqlite
    from storage import DB_PATH
    since_24h = time.time() - 86400
    result = {}
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        # Trades aggregates
        async with db.execute(
            """SELECT symbol,
                      COUNT(*) as trade_count,
                      SUM(price * qty) as volume_usd,
                      SUM(CASE WHEN side='buy' THEN price*qty ELSE 0 END) as buy_vol,
                      SUM(CASE WHEN side='sell' THEN price*qty ELSE 0 END) as sell_vol,
                      MIN(price) as price_low,
                      MAX(price) as price_high,
                      AVG(price) as price_avg
               FROM trades WHERE ts >= ? GROUP BY symbol""",
            (since_24h,)
        ) as cur:
            for r in await cur.fetchall():
                sym = r["symbol"]
                if sym not in result:
                    result[sym] = {}
                result[sym]["trades_24h"] = r["trade_count"]
                result[sym]["volume_usd_24h"] = round(r["volume_usd"] or 0, 2)
                result[sym]["buy_vol_24h"] = round(r["buy_vol"] or 0, 2)
                result[sym]["sell_vol_24h"] = round(r["sell_vol"] or 0, 2)
                result[sym]["price_low_24h"] = r["price_low"]
                result[sym]["price_high_24h"] = r["price_high"]
                result[sym]["price_avg_24h"] = round(r["price_avg"] or 0, 6)
                bv = r["buy_vol"] or 0
                sv = r["sell_vol"] or 0
                total = bv + sv
                result[sym]["cvd_ratio_24h"] = round((bv - sv) / total, 4) if total > 0 else 0
        # Liquidations aggregates
        async with db.execute(
            """SELECT symbol,
                      COUNT(*) as liq_count,
                      SUM(value) as liq_value_usd,
                      SUM(CASE WHEN side='buy' THEN value ELSE 0 END) as long_liqs,
                      SUM(CASE WHEN side='sell' THEN value ELSE 0 END) as short_liqs
               FROM liquidations WHERE ts >= ? GROUP BY symbol""",
            (since_24h,)
        ) as cur:
            for r in await cur.fetchall():
                sym = r["symbol"]
                if sym not in result:
                    result[sym] = {}
                result[sym]["liqs_24h"] = r["liq_count"]
                result[sym]["liq_usd_24h"] = round(r["liq_value_usd"] or 0, 2)
                result[sym]["long_liqs_usd_24h"] = round(r["long_liqs"] or 0, 2)
                result[sym]["short_liqs_usd_24h"] = round(r["short_liqs"] or 0, 2)
        # Latest OI
        async with db.execute(
            """SELECT symbol, oi_value as oi_latest
               FROM open_interest
               WHERE ts = (SELECT MAX(ts) FROM open_interest o2 WHERE o2.symbol = open_interest.symbol)
               GROUP BY symbol"""
        ) as cur:
            for r in await cur.fetchall():
                sym = r["symbol"]
                if sym not in result:
                    result[sym] = {}
                result[sym]["oi_latest"] = r["oi_latest"]
        # Funding latest
        async with db.execute(
            """SELECT symbol, rate as funding_latest
               FROM funding_rate
               WHERE ts = (SELECT MAX(ts) FROM funding_rate f2 WHERE f2.symbol = funding_rate.symbol)
               GROUP BY symbol"""
        ) as cur:
            for r in await cur.fetchall():
                sym = r["symbol"]
                if sym not in result:
                    result[sym] = {}
                result[sym]["funding_latest"] = r["funding_latest"]
        # Whale count 24h
        async with db.execute(
            """SELECT symbol, COUNT(*) as whale_count, SUM(value_usd) as whale_vol
               FROM whale_trades WHERE ts >= ? GROUP BY symbol""",
            (since_24h,)
        ) as cur:
            for r in await cur.fetchall():
                sym = r["symbol"]
                if sym not in result:
                    result[sym] = {}
                result[sym]["whale_trades_24h"] = r["whale_count"]
                result[sym]["whale_vol_usd_24h"] = round(r["whale_vol"] or 0, 2)

    return {"status": "ok", "since": since_24h, "summary": result}


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


@router.get("/cascade-predictor")
async def cascade_predictor_endpoint(
    symbol: Optional[str] = None,
    oi_window: int = Query(default=120, le=600),
    oi_threshold: float = Query(default=2.0),
    sr_proximity: float = Query(default=0.5),
):
    """Liquidation cascade predictor: OI buildup + price near key level → warning."""
    syms = get_symbols()
    target = symbol if symbol and symbol in syms else syms[0]
    data = await predict_liquidation_cascade(
        symbol=target,
        oi_window=oi_window,
        oi_threshold_pct=oi_threshold,
        sr_proximity_pct=sr_proximity,
    )
    return {"status": "ok", "symbol": target, **data}


@router.get("/oi-mcap")
async def oi_mcap_endpoint(symbol: Optional[str] = None):
    """Open Interest / Market Cap ratio — leverage risk signal."""
    syms = get_symbols()
    target = symbol if symbol and symbol in syms else syms[0]
    data = await fetch_oi_mcap_ratio(symbol=target)
    return {"status": "ok", "symbol": target, **data}


@router.get("/vwap-deviation")
async def vwap_deviation_endpoint(
    symbol: Optional[str] = None,
    window: int = Query(default=3600, le=86400),
):
    """VWAP deviation signal: how far current price is from VWAP (%)."""
    syms = get_symbols()
    target = symbol if symbol and symbol in syms else syms[0]
    data = await compute_vwap_deviation(window_seconds=window, symbol=target)
    return {"status": "ok", "symbol": target, **data}


@router.get("/funding-arb")
async def funding_arb_endpoint(
    symbol: Optional[str] = None,
    threshold_bps: float = Query(default=5.0, ge=0.1, le=100.0),
):
    """Funding arbitrage signal: Binance vs Bybit rate divergence."""
    syms = get_symbols()
    target = symbol if symbol and symbol in syms else syms[0]
    data = await detect_funding_arbitrage(symbol=target, threshold_bps=threshold_bps)
    return {"status": "ok", "symbol": target, **data}


@router.get("/oi-concentration")
async def oi_concentration_endpoint(
    symbol: Optional[str] = None,
    window: int = Query(default=3600, ge=300, le=86400),
    buckets: int = Query(default=10, ge=5, le=50),
):
    """OI concentration: % of OI change in densest price range bucket."""
    from collectors import get_symbols
    syms = get_symbols()
    target = symbol if symbol and symbol in syms else syms[0]
    data = await compute_oi_concentration(symbol=target, window_seconds=window, n_buckets=buckets)
    return {"status": "ok", "symbol": target, **data}


@router.get("/funding-divergence")
async def funding_divergence_endpoint(
    focus: Optional[str] = None,
    multiplier: float = Query(default=2.0, ge=1.0, le=20.0),
):
    """Funding rate divergence: focus symbol vs average of peers."""
    from collectors import get_symbols
    syms = get_symbols()
    target = focus if focus and focus in syms else syms[0]
    data = await detect_funding_divergence(focus_symbol=target, divergence_multiplier=multiplier)
    return {"status": "ok", **data}


@router.get("/cvd-momentum")
async def cvd_momentum_endpoint(
    symbol: Optional[str] = None,
    window: int = Query(default=60, le=300),
):
    syms = get_symbols()
    target = symbol if symbol and symbol in syms else syms[0]
    data = await detect_cvd_momentum(window_seconds=window, symbol=target)
    return {"status": "ok", "symbol": target, **data}


@router.get("/market-regime")
async def market_regime_endpoint(
    symbol: Optional[str] = None,
):
    syms = get_symbols()
    target = symbol if symbol and symbol in syms else syms[0]
    data = await compute_market_regime(symbol=target)
    return {"status": "ok", **data}


@router.get("/market-regime/all")
async def market_regime_all():
    """Composite regime score for all tracked symbols."""
    syms = get_symbols()
    results = await asyncio.gather(*[compute_market_regime(symbol=s) for s in syms])
    return {"status": "ok", "symbols": {s: r for s, r in zip(syms, results)}}


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


@router.get("/whale-history")
async def whale_history(
    limit: int = Query(default=100, le=500),
    since: Optional[float] = None,
    symbol: Optional[str] = None,
    min_usd: float = Query(default=50000, le=10000000),
    window: int = Query(default=3600, description="Seconds back to fetch if since not specified"),
):
    """Fetch persisted whale trades (single trade > min_usd USD)."""
    if since is None:
        since = time.time() - window
    trades = await get_whale_trades(limit=limit, since=since, symbol=symbol, min_usd=min_usd)
    buy_vol = sum(t["value_usd"] for t in trades if t["side"] in ("buy", "Buy"))
    sell_vol = sum(t["value_usd"] for t in trades if t["side"] not in ("buy", "Buy"))
    return {
        "status": "ok",
        "count": len(trades),
        "trades": trades,
        "total_buy_usd": round(buy_vol, 2),
        "total_sell_usd": round(sell_vol, 2),
        "whale_threshold_usd": min_usd,
    }


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
    _tick_count = 0
    _cached_regime = None
    try:
        while True:
            try:
                _tick_count += 1
                phase_task = classify_market_phase(symbol=symbol)
                vol_task = compute_volume_imbalance(window_seconds=60, symbol=symbol)
                oi_task = compute_oi_momentum(window_seconds=300, symbol=symbol)

                phase, vol_imb, oi_mom = await asyncio.gather(phase_task, vol_task, oi_task)

                # Regime is expensive, compute every 5 ticks (~5s)
                if _tick_count % 5 == 1:
                    _cached_regime = await compute_market_regime(symbol=symbol)

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
                    detect_funding_arbitrage(symbol=symbol, threshold_bps=5.0),
                    compute_vwap_deviation(window_seconds=3600, symbol=symbol),
                    predict_liquidation_cascade(symbol=symbol, oi_window=120, oi_threshold_pct=2.0, sr_proximity_pct=0.5),
                    return_exceptions=True,
                )
                div_result, oi_result, liq_result, vol_result, funding_ex_result, funding_arb_result, vwap_dev_result, cascade_pred_result = alert_tasks

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
                if isinstance(funding_arb_result, dict) and funding_arb_result.get("arb"):
                    fired_alerts.append(("funding_arb", "medium", funding_arb_result.get("description", ""), funding_arb_result))
                # VWAP deviation: alert only on strong deviation
                if isinstance(vwap_dev_result, dict) and vwap_dev_result.get("strength") == "strong":
                    fired_alerts.append(("vwap_deviation", "medium", vwap_dev_result.get("description", ""), vwap_dev_result))
                # Cascade predictor: alert on high_risk
                if isinstance(cascade_pred_result, dict) and cascade_pred_result.get("high_risk"):
                    sev = "critical" if cascade_pred_result.get("level") == "cascading" else "high"
                    fired_alerts.append(("cascade_predictor", sev, cascade_pred_result.get("description", ""), cascade_pred_result))

                # Spread widening alert (check every tick, 0.5% threshold)
                try:
                    if ob and ob[0].get("best_bid") and ob[0].get("best_ask") and ob[0].get("mid_price"):
                        _bid = ob[0]["best_bid"]
                        _ask = ob[0]["best_ask"]
                        _mid = ob[0]["mid_price"]
                        _spread_pct = (_ask - _bid) / _mid * 100 if _mid > 0 else 0
                        SPREAD_ALERT_THRESHOLD = 0.5  # %
                        if _spread_pct > SPREAD_ALERT_THRESHOLD:
                            fired_alerts.append((
                                "spread_alert",
                                "high",
                                f"Spread widened to {_spread_pct:.4f}% ({_spread_pct*100:.2f} bps) — threshold {SPREAD_ALERT_THRESHOLD}%",
                                {"spread_pct": round(_spread_pct, 6), "spread_bps": round(_spread_pct * 100, 4),
                                 "bid": _bid, "ask": _ask, "mid": _mid},
                            ))
                except Exception:
                    pass

                # Cross-symbol correlated OI spike (only check from BANANAS31 WS to avoid duplicate)
                cross_sym_result = None
                if symbol == get_symbols()[0]:  # only run once per tick cycle from primary symbol
                    try:
                        all_syms = get_symbols()
                        cross_sym_result = await detect_cross_symbol_oi_spike(
                            symbols=all_syms,
                            window_seconds=300,
                            threshold_pct=2.5,
                            min_correlated=2,
                        )
                        if isinstance(cross_sym_result, dict) and cross_sym_result.get("correlated"):
                            fired_alerts.append((
                                "cross_symbol_oi_spike",
                                "high",
                                cross_sym_result.get("description", ""),
                                cross_sym_result,
                            ))
                    except Exception:
                        pass

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
                cooldowns = {"funding_extreme": 300, "phase_change": 120, "funding_arb": 180, "vwap_deviation": 120, "cascade_predictor": 90, "spread_alert": 60}
                for a_type, sev, desc, data in fired_alerts:
                    if not hasattr(ws, "_last_alert_ts"):
                        ws._last_alert_ts = {}
                    last = ws._last_alert_ts.get(a_type, 0)
                    cooldown = cooldowns.get(a_type, 60)
                    if time.time() - last > cooldown:
                        await insert_alert(symbol, a_type, sev, desc, data)
                        ws._last_alert_ts[a_type] = time.time()
                        await alert_manager.broadcast({
                            "type": "alert",
                            "ts": time.time(),
                            "symbol": symbol,
                            "alert_type": a_type,
                            "severity": sev,
                            "description": desc,
                        })

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
                    "cross_symbol_oi_spike": cross_sym_result if isinstance(cross_sym_result, dict) else None,
                    "funding_arb": funding_arb_result if isinstance(funding_arb_result, dict) else None,
                    "vwap_deviation": vwap_dev_result if isinstance(vwap_dev_result, dict) else None,
                    "cascade_predictor": cascade_pred_result if isinstance(cascade_pred_result, dict) else None,
                    "market_regime": _cached_regime,
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


@router.get("/cross-symbol-oi")
async def cross_symbol_oi_endpoint(
    window: int = Query(default=300, le=3600),
    threshold: float = Query(default=2.5),
    min_correlated: int = Query(default=2),
):
    """Check if multiple symbols are simultaneously spiking OI (correlated OI alert)."""
    syms = get_symbols()
    result = await detect_cross_symbol_oi_spike(
        symbols=syms,
        window_seconds=window,
        threshold_pct=threshold,
        min_correlated=min_correlated,
    )
    return {"status": "ok", "data": result}


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
    elif metric == "whales":
        rows = await get_whale_trades(symbol=target, limit=10000)
        fields = ["ts", "exchange", "symbol", "price", "qty", "side", "value_usd"]
    elif metric == "patterns":
        rows = await get_pattern_history(symbol=target, limit=10000)
        fields = ["ts", "symbol", "pattern_type", "confidence", "description"]
    elif metric == "phases":
        rows = await get_phase_snapshots(symbol=target, limit=10000)
        fields = ["ts", "symbol", "phase", "confidence", "composite_score"]
    elif metric == "alerts":
        rows = await get_alert_history(symbol=target, limit=10000)
        fields = ["ts", "symbol", "alert_type", "severity", "description"]
    else:
        return JSONResponse({"error": "Unknown metric. Use: trades|oi|funding|liquidations|cvd|whales|patterns|phases|alerts"}, status_code=400)

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
        candles_1h_task = get_ohlcv(interval_seconds=300, window_seconds=3600, symbol=sym)

        ob, cvd_data, funding, oi_mom, candles_24h, candles_1h = await asyncio.gather(
            ob_task, cvd_task, funding_task, oi_task, candles_task, candles_1h_task,
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
        change_1h  = 0.0
        high_24h = None
        low_24h = None
        if isinstance(candles_24h, list) and candles_24h:
            open_24h  = candles_24h[0]["open"]
            close_24h = candles_24h[-1]["close"]
            if open_24h:
                change_24h = (close_24h - open_24h) / open_24h * 100
            high_24h = max(c["high"] for c in candles_24h)
            low_24h  = min(c["low"]  for c in candles_24h)
        if isinstance(candles_1h, list) and len(candles_1h) >= 2:
            o1h = candles_1h[0]["open"]
            c1h = candles_1h[-1]["close"]
            if o1h:
                change_1h = (c1h - o1h) / o1h * 100

        return {
            "price": price,
            "cvd_delta": round(cvd_delta, 0),
            "funding": round(avg_funding, 8),
            "oi_pct": round(oi_pct, 4),
            "change_24h": round(change_24h, 4),
            "change_1h": round(change_1h, 4),
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


@router.get("/oi-delta")
async def oi_delta_candles(
    interval: int = Query(default=300, ge=60, le=3600),
    window: int = Query(default=3600, le=86400),
    symbol: Optional[str] = None,
    exchange: str = Query(default="binance"),
):
    """OI change per candle as histogram data. interval=candle size (s), window=lookback (s)."""
    target = symbol or get_symbols()[0]
    since = time.time() - window
    rows = await get_oi_history(limit=10000, since=since, symbol=target)
    if not rows:
        return {"status": "ok", "symbol": target, "interval": interval, "candles": []}

    # Filter to one exchange
    filtered = [r for r in rows if r.get("exchange") == exchange]
    if not filtered:
        filtered = rows  # fallback to all

    rows = filtered

    # Bucket rows into candles
    candles: dict[int, list] = {}
    for r in rows:
        ts = r.get("ts", 0)
        bucket = int(ts // interval) * interval
        if bucket not in candles:
            candles[bucket] = []
        candles[bucket].append(r.get("oi_value") or r.get("open_interest") or 0)

    result = []
    for bucket in sorted(candles):
        vals = candles[bucket]
        if len(vals) >= 2:
            oi_change = vals[-1] - vals[0]
        elif len(vals) == 1:
            oi_change = 0.0
        else:
            continue
        result.append({"ts": bucket, "oi_change": round(oi_change, 2), "oi_end": round(vals[-1], 2)})

    return {"status": "ok", "symbol": target, "interval": interval, "candles": result}


@router.get("/trade-count-rate")
async def trade_count_rate(
    interval: int = Query(default=60, ge=10, le=300),
    window: int = Query(default=1800, le=7200),
    symbol: Optional[str] = None,
):
    """Trades per minute bucketed by interval over the last `window` seconds.
    Returns list of {ts, trades_count, trades_per_min} for area chart rendering."""
    target = symbol or get_symbols()[0]
    since = time.time() - window
    # fetch raw trades
    db_path = storage.DB_PATH
    q = "SELECT ts FROM trades WHERE ts > ? AND symbol = ? ORDER BY ts ASC"
    async with aiosqlite.connect(db_path) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(q, (since, target)) as cur:
            rows = await cur.fetchall()

    if not rows:
        return {"status": "ok", "symbol": target, "interval": interval, "buckets": []}

    # Bucket into intervals
    buckets: dict[int, int] = {}
    for r in rows:
        ts = r["ts"]
        bucket = int(ts // interval) * interval
        buckets[bucket] = buckets.get(bucket, 0) + 1

    # Fill gaps and compute trades_per_min
    now_bucket = int(time.time() // interval) * interval
    start_bucket = int(since // interval) * interval
    result = []
    b = start_bucket
    while b <= now_bucket:
        count = buckets.get(b, 0)
        tpm = round(count * (60 / interval), 2)
        result.append({"ts": b, "trades_count": count, "trades_per_min": tpm})
        b += interval

    return {"status": "ok", "symbol": target, "interval": interval, "window": window, "buckets": result}


@router.get("/spread-history")
async def spread_history(
    window: int = Query(default=1800, le=86400),
    symbol: Optional[str] = None,
    exchange: str = Query(default="binance"),
):
    """Bid-ask spread history from orderbook_snapshots. Returns spread in bps and spread %.
    Also returns alert if current spread is >2x the 30-min average."""
    target = symbol or get_symbols()[0]
    since = time.time() - window
    db_path = storage.DB_PATH
    q = """SELECT ts, best_bid, best_ask, spread, mid_price
           FROM orderbook_snapshots
           WHERE ts > ? AND symbol = ? AND exchange = ?
           ORDER BY ts ASC"""
    async with aiosqlite.connect(db_path) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(q, (since, target, exchange)) as cur:
            rows = await cur.fetchall()

    if not rows:
        # try without exchange filter
        q2 = """SELECT ts, best_bid, best_ask, spread, mid_price
                FROM orderbook_snapshots
                WHERE ts > ? AND symbol = ?
                ORDER BY ts ASC"""
        async with aiosqlite.connect(db_path) as db:
            db.row_factory = aiosqlite.Row
            async with db.execute(q2, (since, target)) as cur:
                rows = await cur.fetchall()

    if not rows:
        return {"status": "ok", "symbol": target, "data": [], "alert": None}

    data = []
    for r in rows:
        mid = r["mid_price"] or ((r["best_bid"] or 0) + (r["best_ask"] or 0)) / 2
        sp = r["spread"] or 0
        sp_pct = round((sp / mid) * 100, 4) if mid > 0 else 0
        sp_bps = round(sp_pct * 100, 2)
        data.append({"ts": r["ts"], "spread": round(sp, 6), "spread_pct": sp_pct, "spread_bps": sp_bps})

    # Alert: current spread vs 30min average
    alert = None
    if len(data) >= 2:
        current = data[-1]["spread_bps"]
        avg = sum(d["spread_bps"] for d in data) / len(data)
        if avg > 0 and current > avg * 2:
            alert = {
                "level": "high",
                "message": f"Spread widened: {current:.1f} bps (avg {avg:.1f} bps, {current/avg:.1f}x)",
                "current_bps": round(current, 2),
                "avg_bps": round(avg, 2),
                "ratio": round(current / avg, 2),
            }

    return {"status": "ok", "symbol": target, "data": data, "alert": alert}


# ─── Spread Tracker ──────────────────────────────────────────────────────────

@router.get("/spread-tracker")
async def spread_tracker(
    symbol: Optional[str] = Query(default=None),
    window: int = Query(default=1800, ge=300, le=7200, description="History window in seconds (default 30min)"),
    exchange: Optional[str] = Query(default=None),
    threshold_pct: float = Query(default=0.5, description="Alert threshold for spread % (default 0.5%)"),
):
    """
    Bid-ask spread tracker: current spread + historical series + alert status.
    - spread_pct = (ask - bid) / mid * 100
    - Stores in dedicated spread_history table (written by insert_orderbook)
    - Returns alert when spread_pct > threshold_pct OR current > 2x avg
    """
    from storage import get_spread_history, get_spread_stats, DB_PATH
    syms = [symbol.upper()] if symbol else get_symbols()
    result = {}

    for sym in syms:
        stats = await get_spread_stats(sym, window=window, exchange=exchange)
        history = await get_spread_history(sym, window=window, exchange=exchange, limit=1000)

        # Override alert threshold if custom
        alert = stats.get("alert")
        current_pct = stats.get("current_pct")
        current_bps = stats.get("current_bps")
        avg_bps = stats.get("avg_bps", 0)
        if current_pct is not None and current_pct > threshold_pct and (alert is None or alert.get("level") != "high"):
            alert = {
                "level": "high",
                "reason": "spread_pct_threshold",
                "message": f"Spread {current_pct:.4f}% exceeds {threshold_pct}% threshold",
                "current_pct": round(current_pct, 4),
                "current_bps": round(current_bps, 2) if current_bps else None,
            }

        result[sym] = {
            **stats,
            "alert": alert,
            "threshold_pct": threshold_pct,
            "history": [
                {"ts": r["ts"], "spread_pct": r["spread_pct"], "spread_bps": r["spread_bps"],
                 "bid_vol": r.get("bid_vol"), "ask_vol": r.get("ask_vol")}
                for r in history
            ],
        }

    if symbol:
        return {"status": "ok", "ts": time.time(), **result.get(symbol.upper(), {})}
    return {"status": "ok", "ts": time.time(), "symbols": result}


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


@router.get("/max-drawdown")
async def max_drawdown_endpoint(
    symbol: Optional[str] = None,
    window: int = Query(default=3600, le=86400),
):
    """Peak-to-trough max drawdown over the last window_seconds."""
    from metrics import compute_max_drawdown
    # Always fetch all symbols so frontend can look up by name
    data = await compute_max_drawdown(window_seconds=window, symbol=None)
    # If a specific symbol was requested and we got no data for it, try with filter
    if symbol and symbol not in data:
        sym_data = await compute_max_drawdown(window_seconds=window, symbol=symbol)
        if sym_data:
            data.update(sym_data)
    return {"status": "ok", "symbols": data, "window_seconds": window}


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


@router.get("/pattern")
async def pattern_live(symbol: Optional[str] = None):
    """Live accumulation/distribution footprint for a symbol."""
    syms = get_symbols()
    target = symbol if symbol and symbol in syms else syms[0]
    data = await detect_accumulation_distribution_pattern(symbol=target)
    return {"status": "ok", **data}


@router.get("/pattern/all")
async def pattern_all():
    """Live pattern for all tracked symbols."""
    syms = get_symbols()
    results = await asyncio.gather(*[
        detect_accumulation_distribution_pattern(symbol=s) for s in syms
    ])
    return {"status": "ok", "symbols": {s: r for s, r in zip(syms, results)}}


@router.get("/pattern-history")
async def pattern_history_endpoint(
    symbol: Optional[str] = None,
    pattern_type: Optional[str] = None,
    limit: int = Query(default=100, le=500),
    since: Optional[float] = None,
):
    """Return persisted pattern detection history."""
    data = await get_pattern_history(limit=limit, since=since, symbol=symbol, pattern_type=pattern_type)
    return {"status": "ok", "data": data, "count": len(data)}


@router.get("/phase-history")
async def phase_history_endpoint(
    symbol: Optional[str] = None,
    since: Optional[float] = None,
    until: Optional[float] = None,
    limit: int = Query(default=200, le=1000),
    window_hours: float = Query(default=1.0, le=24.0),
):
    """
    Return historical phase snapshots for timeline replay.
    If since is not provided, defaults to last window_hours.
    """
    import time as _time
    syms = get_symbols()
    target = symbol if symbol and symbol in syms else syms[0]
    if since is None:
        since = _time.time() - window_hours * 3600
    data = await get_phase_snapshots(symbol=target, since=since, until=until, limit=limit)
    return {"status": "ok", "symbol": target, "data": data, "count": len(data)}


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


@router.get("/atr")
async def atr_endpoint(
    symbol: Optional[str] = None,
    period: int = Query(default=14, le=100),
    interval: int = Query(default=60, le=3600),
):
    """ATR(n) for the given symbol, using 1-min (or specified) candles."""
    syms = get_symbols()
    target = symbol if symbol and symbol in syms else syms[0]
    candles = await get_ohlcv(interval_seconds=interval, window_seconds=period * interval * 3, symbol=target)
    if len(candles) < period + 1:
        return {"status": "ok", "symbol": target, "atr": None, "atr_pct": None, "period": period}

    highs  = [c["high"]  for c in candles]
    lows   = [c["low"]   for c in candles]
    closes = [c["close"] for c in candles]

    trs = []
    for i in range(1, len(candles)):
        tr = max(highs[i] - lows[i], abs(highs[i] - closes[i-1]), abs(lows[i] - closes[i-1]))
        trs.append(tr)

    # Wilder smoothed ATR
    atr = sum(trs[:period]) / period
    for i in range(period, len(trs)):
        atr = (atr * (period - 1) + trs[i]) / period

    last_close = closes[-1]
    atr_pct = atr / last_close * 100 if last_close else None

    return {
        "status": "ok",
        "symbol": target,
        "atr": round(atr, 8),
        "atr_pct": round(atr_pct, 4) if atr_pct else None,
        "last_close": last_close,
        "period": period,
        "interval_seconds": interval,
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


@router.get("/funding-heatmap")
async def funding_heatmap(
    hours: int = Query(24, ge=1, le=168),
    buckets: int = Query(24, ge=6, le=72),
):
    """Funding rate extremes heatmap: symbol × time bucket grid.
    
    Returns a 2D grid where each cell = average funding rate for
    (symbol, time_bucket). Used to spot funding rate extremes over time.
    """
    symbols = get_symbols()
    now = time.time()
    since = now - hours * 3600
    bucket_size = (hours * 3600) / buckets

    # Fetch all funding data for all symbols in range
    all_data: dict[str, list] = {}
    for sym in symbols:
        rows = await get_funding_history(limit=10000, since=since, symbol=sym)
        all_data[sym] = rows

    # Build bucket timestamps
    bucket_starts = [since + i * bucket_size for i in range(buckets)]

    # Aggregate: for each symbol × bucket, compute average rate
    grid = {}
    global_min = float("inf")
    global_max = float("-inf")

    for sym in symbols:
        grid[sym] = []
        rows = all_data[sym]
        for b_idx in range(buckets):
            b_start = bucket_starts[b_idx]
            b_end = b_start + bucket_size
            # All rows in this bucket
            in_bucket = [r["rate"] for r in rows if b_start <= r["ts"] < b_end]
            if in_bucket:
                avg = sum(in_bucket) / len(in_bucket)
                count = len(in_bucket)
            else:
                avg = None
                count = 0
            grid[sym].append({"ts": b_start, "rate": avg, "count": count})
            if avg is not None:
                if avg < global_min:
                    global_min = avg
                if avg > global_max:
                    global_max = avg

    if global_min == float("inf"):
        global_min = 0.0
    if global_max == float("-inf"):
        global_max = 0.0

    # Compute extremes: max absolute rate per symbol
    extremes = {}
    for sym in symbols:
        rates = [c["rate"] for c in grid[sym] if c["rate"] is not None]
        if rates:
            max_abs = max(abs(r) for r in rates)
            latest = rates[-1] if rates else 0
            extremes[sym] = {"max_abs": round(max_abs * 100, 6), "latest": round(latest * 100, 6)}
        else:
            extremes[sym] = {"max_abs": 0.0, "latest": 0.0}

    return {
        "status": "ok",
        "ts": now,
        "hours": hours,
        "buckets": buckets,
        "bucket_size_seconds": bucket_size,
        "symbols": symbols,
        "bucket_starts": bucket_starts,
        "grid": grid,
        "global_min": global_min,
        "global_max": global_max,
        "extremes": extremes,
    }


# ── Liquidation Pressure Score ────────────────────────────────────────────────

@router.get("/liq-pressure")
async def liq_pressure(
    symbol: Optional[str] = Query(default=None),
    window: int = Query(default=300, ge=30, le=3600, description="Window seconds for OI velocity"),
    liq_window: int = Query(default=120, ge=30, le=1800, description="Window seconds for liq volume"),
):
    """
    Liquidation Pressure Score (0-100):
    Combines recent liquidation volume + OI velocity into a single 0-100 score.
    High = extreme pressure (potential cascade), Low = calm.
    """
    symbols = [symbol] if symbol else get_symbols()
    now = time.time()
    result = {}

    for sym in symbols:
        # 1) Liquidation volume score (0-50 points)
        liq_data = await get_recent_liquidations(limit=1000, since=now - liq_window, symbol=sym)
        liq_usd = sum(r.get("value", 0) or 0 for r in liq_data)

        # Normalize: $0 → 0pts, $100k → 25pts, $500k → 50pts (logarithmic)
        import math
        if liq_usd <= 0:
            liq_score = 0.0
        else:
            # log scale: ln(liq_usd/1000) / ln(500) * 50, clamp 0-50
            liq_score = min(50.0, max(0.0, math.log(liq_usd / 1000 + 1) / math.log(501) * 50))

        # Long vs short breakdown
        long_liq = sum(r.get("value", 0) or 0 for r in liq_data if r.get("side") == "sell")
        short_liq = sum(r.get("value", 0) or 0 for r in liq_data if r.get("side") == "buy")

        # 2) OI velocity score (0-50 points)
        oi_mom = await compute_oi_momentum(window_seconds=window, symbol=sym)
        oi_pct = abs(oi_mom.get("avg_pct_change", 0))

        # Normalize: 0% → 0pts, 1% → 25pts, 3%+ → 50pts
        if oi_pct <= 0:
            oi_score = 0.0
        else:
            oi_score = min(50.0, oi_pct / 3.0 * 50)

        total_score = round(liq_score + oi_score, 1)

        # Severity label
        if total_score >= 80:
            level = "critical"
        elif total_score >= 60:
            level = "high"
        elif total_score >= 35:
            level = "medium"
        elif total_score >= 15:
            level = "low"
        else:
            level = "calm"

        result[sym] = {
            "score": total_score,
            "level": level,
            "liq_score": round(liq_score, 1),
            "oi_score": round(oi_score, 1),
            "liq_usd": round(liq_usd, 2),
            "long_liq_usd": round(long_liq, 2),
            "short_liq_usd": round(short_liq, 2),
            "oi_pct_change": round(oi_mom.get("avg_pct_change", 0), 4),
            "liq_window_s": liq_window,
            "oi_window_s": window,
        }

    return {"status": "ok", "ts": now, "symbols": result}


# ── Price Velocity Indicator ──────────────────────────────────────────────────

@router.get("/price-velocity")
async def price_velocity(
    symbol: Optional[str] = Query(default=None),
    short_window: int = Query(default=10, ge=5, le=60, description="Short window (seconds) for instant velocity"),
    long_window: int = Query(default=60, ge=15, le=300, description="Long window (seconds) for trend velocity"),
):
    """
    Price velocity: rate of price change in $/second (and %/second).
    Returns instant velocity (short_window) + trend velocity (long_window).
    Also returns a normalized -100 to +100 score for a speedometer needle.
    """
    symbols = [symbol] if symbol else get_symbols()
    now = time.time()
    result = {}

    for sym in symbols:
        # Fetch trades for long window
        trades = await get_recent_trades(limit=2000, since=now - long_window, symbol=sym)
        if not trades:
            result[sym] = {
                "instant_velocity": 0.0,
                "trend_velocity": 0.0,
                "instant_pct_per_sec": 0.0,
                "trend_pct_per_sec": 0.0,
                "score": 0,
                "direction": "flat",
                "price_now": None,
            }
            continue

        # Sort by timestamp
        trades.sort(key=lambda t: t.get("ts", 0))

        now_ts = trades[-1]["ts"]
        price_now = trades[-1].get("price", 0) or 0

        # Instant velocity: short window
        cutoff_short = now_ts - short_window
        short_trades = [t for t in trades if t.get("ts", 0) >= cutoff_short]
        if len(short_trades) >= 2:
            p_start = short_trades[0]["price"] or 0
            p_end = short_trades[-1]["price"] or 0
            dt = short_trades[-1]["ts"] - short_trades[0]["ts"]
            inst_vel = (p_end - p_start) / dt if dt > 0 else 0.0
            inst_pct = (inst_vel / p_start * 100) if p_start else 0.0
        else:
            inst_vel = 0.0
            inst_pct = 0.0

        # Trend velocity: long window
        p_start_long = trades[0]["price"] or 0
        p_end_long = trades[-1]["price"] or 0
        dt_long = trades[-1]["ts"] - trades[0]["ts"]
        trend_vel = (p_end_long - p_start_long) / dt_long if dt_long > 0 else 0.0
        trend_pct = (trend_vel / p_start_long * 100) if p_start_long else 0.0

        # Normalize to -100..+100 score based on %/sec (cap at ±0.1%/sec)
        MAX_PCT_PER_SEC = 0.05  # 0.05%/s = extreme
        score = max(-100, min(100, round(inst_pct / MAX_PCT_PER_SEC * 100)))

        direction = "up" if score > 5 else ("down" if score < -5 else "flat")

        result[sym] = {
            "instant_velocity": round(inst_vel, 8),
            "trend_velocity": round(trend_vel, 8),
            "instant_pct_per_sec": round(inst_pct, 6),
            "trend_pct_per_sec": round(trend_pct, 6),
            "score": score,
            "direction": direction,
            "price_now": round(price_now, 8),
            "short_window_s": short_window,
            "long_window_s": long_window,
        }

    return {"status": "ok", "ts": now, "symbols": result}


# ── CVD New-High Divergence ───────────────────────────────────────────────────

@router.get("/cvd-divergence")
async def cvd_divergence_endpoint(
    symbol: Optional[str] = Query(default=None),
    window: int = Query(default=300, ge=60, le=3600, description="Lookback window in seconds"),
):
    """
    CVD New-High/Low Divergence detector.
    Bearish: price makes new high in window but CVD does not confirm → sell pressure hidden.
    Bullish: price makes new low but CVD does not confirm → buy pressure hidden.
    Returns badge-level signal per symbol.
    """
    symbols = [symbol] if symbol else get_symbols()
    now = time.time()
    result = {}

    for sym in symbols:
        trades = await get_recent_trades(limit=5000, since=now - window, symbol=sym)
        if len(trades) < 20:
            result[sym] = {"signal": "none", "description": "Insufficient data", "severity": 0}
            continue

        trades.sort(key=lambda t: t.get("ts", 0))

        # Split into two halves: first half vs second half
        mid = len(trades) // 2
        first_half = trades[:mid]
        second_half = trades[mid:]

        def price_high(ts): return max((t.get("price", 0) or 0) for t in ts)
        def price_low(ts):  return min((t.get("price", 0) or 0) for t in ts)
        def cvd_sum(ts):
            s = 0
            for t in ts:
                v = (t.get("price", 0) or 0) * (t.get("qty", 0) or 0)
                s += v if t.get("side") == "buy" else -v
            return s

        p_high_1 = price_high(first_half)
        p_high_2 = price_high(second_half)
        p_low_1  = price_low(first_half)
        p_low_2  = price_low(second_half)
        cvd_1    = cvd_sum(first_half)
        cvd_2    = cvd_sum(second_half)

        # Price change %
        price_latest = second_half[-1].get("price", 0) or 0
        price_oldest = first_half[0].get("price", 0) or 0
        price_pct = (price_latest - price_oldest) / price_oldest * 100 if price_oldest else 0

        # CVD change (normalized by median trade value)
        median_val = sorted([abs((t.get("price",0) or 0)*(t.get("qty",0) or 0)) for t in trades])[len(trades)//2]
        cvd_delta = cvd_2 - cvd_1
        cvd_norm = cvd_delta / (median_val * len(trades) / 2) if median_val else 0  # -1 to 1

        # Bearish divergence: price makes higher high, CVD makes lower high
        bearish_div = p_high_2 > p_high_1 * 1.001 and cvd_2 < cvd_1 * 0.9
        # Bullish divergence: price makes lower low, CVD makes higher low
        bullish_div = p_low_2 < p_low_1 * 0.999 and cvd_2 > cvd_1 * 1.1

        # Also classic: price trend vs CVD trend
        if not bearish_div and not bullish_div:
            if price_pct > 0.1 and cvd_norm < -0.05:
                bearish_div = True
            elif price_pct < -0.1 and cvd_norm > 0.05:
                bullish_div = True

        if bearish_div:
            severity = min(3, max(1, int(abs(price_pct) / 0.2 + 1)))
            result[sym] = {
                "signal": "bearish",
                "description": f"🔻 Bearish div: price +{price_pct:.2f}% but CVD not confirming",
                "severity": severity,
                "price_pct": round(price_pct, 3),
                "cvd_norm": round(cvd_norm, 4),
                "price_high_1": round(p_high_1, 8),
                "price_high_2": round(p_high_2, 8),
            }
        elif bullish_div:
            severity = min(3, max(1, int(abs(price_pct) / 0.2 + 1)))
            result[sym] = {
                "signal": "bullish",
                "description": f"🟢 Bullish div: price {price_pct:.2f}% but CVD not confirming",
                "severity": severity,
                "price_pct": round(price_pct, 3),
                "cvd_norm": round(cvd_norm, 4),
                "price_low_1": round(p_low_1, 8),
                "price_low_2": round(p_low_2, 8),
            }
        else:
            result[sym] = {
                "signal": "none",
                "description": "No CVD divergence detected",
                "severity": 0,
                "price_pct": round(price_pct, 3),
                "cvd_norm": round(cvd_norm, 4),
            }

    return {"status": "ok", "ts": now, "window_s": window, "symbols": result}


# ── Trade Momentum Burst Detector ────────────────────────────────────────────

@router.get("/trade-bursts")
async def trade_bursts(
    symbol: Optional[str] = Query(default=None),
    window: int = Query(default=60, ge=10, le=300, description="Total lookback window in seconds"),
    burst_window: int = Query(default=5, ge=1, le=30, description="Burst detection window in seconds"),
    threshold: int = Query(default=10, ge=3, le=100, description="Min trades in burst_window to count as burst"),
):
    """
    Detect trade momentum bursts: periods with >threshold trades in burst_window seconds.
    Returns detected bursts + current rate (trades/sec in last 10s).
    """
    symbols = [symbol] if symbol else get_symbols()
    now = time.time()
    result = {}

    for sym in symbols:
        trades = await get_recent_trades(limit=5000, since=now - window, symbol=sym)
        if not trades:
            result[sym] = {"burst_active": False, "burst_count": 0, "rate_now": 0.0, "bursts": []}
            continue

        trades.sort(key=lambda t: t.get("ts", 0))
        timestamps = [t["ts"] for t in trades]

        # Sliding window burst detection
        bursts = []
        i = 0
        while i < len(timestamps):
            ts_start = timestamps[i]
            ts_end = ts_start + burst_window
            j = i
            while j < len(timestamps) and timestamps[j] <= ts_end:
                j += 1
            count = j - i
            if count >= threshold:
                # Collect burst info
                burst_trades = trades[i:j]
                buy_vol  = sum((t.get("price",0) or 0)*(t.get("qty",0) or 0) for t in burst_trades if t.get("side")=="buy")
                sell_vol = sum((t.get("price",0) or 0)*(t.get("qty",0) or 0) for t in burst_trades if t.get("side")=="sell")
                direction = "buy" if buy_vol > sell_vol else "sell"
                bursts.append({
                    "ts_start": round(ts_start, 2),
                    "ts_end": round(timestamps[j-1], 2),
                    "trade_count": count,
                    "rate_per_sec": round(count / burst_window, 2),
                    "buy_vol": round(buy_vol, 2),
                    "sell_vol": round(sell_vol, 2),
                    "direction": direction,
                })
                i = j  # skip past this burst
            else:
                i += 1

        # Current rate: trades in last 10 seconds
        recent_ts = now - 10
        recent_count = sum(1 for t in timestamps if t >= recent_ts)
        rate_now = round(recent_count / 10, 2)

        # Is a burst active right now?
        burst_active = False
        if timestamps:
            last_10s = [t for t in timestamps if t >= now - burst_window]
            burst_active = len(last_10s) >= threshold

        # Latest burst
        latest_burst = bursts[-1] if bursts else None

        result[sym] = {
            "burst_active": burst_active,
            "burst_count": len(bursts),
            "rate_now": rate_now,
            "current_burst_trades": len([t for t in timestamps if t >= now - burst_window]),
            "bursts": bursts[-5:],  # last 5 bursts
            "latest_burst": latest_burst,
        }

    return {"status": "ok", "ts": now, "window_s": window, "burst_window_s": burst_window, "threshold": threshold, "symbols": result}


# ── Cumulative Funding Cost Tracker ─────────────────────────────────────────

@router.get("/funding-cost")
async def funding_cost(
    symbol: Optional[str] = Query(default=None),
    session_hours: float = Query(default=8.0, ge=0.1, le=168, description="Session duration in hours (how long you've been in the trade)"),
    position_usd: float = Query(default=10000.0, ge=1, description="Position size in USD"),
    side: str = Query(default="long", description="Position side: long or short"),
):
    """
    Cumulative funding cost since session open.
    - Fetches all funding rate samples in the window
    - Funding is paid every 8h: sums up intervals * rate * position
    - Returns cost per symbol, direction, and rate trend
    """
    symbols = [symbol] if symbol else get_symbols()
    now = time.time()
    since = now - session_hours * 3600
    result = {}

    for sym in symbols:
        funding_rows = await get_funding_history(limit=10000, since=since, symbol=sym)
        if not funding_rows:
            result[sym] = {
                "total_cost_usd": 0.0,
                "total_cost_pct": 0.0,
                "intervals_counted": 0,
                "avg_rate": 0.0,
                "latest_rate": 0.0,
                "favorable": False,
                "description": "No funding data in window",
            }
            continue

        # Group by exchange and compute per-interval costs
        # Funding rates are sampled ~every 8h; each sample represents one funding payment
        # We'll use the actual rate samples weighted by time intervals between them
        by_exchange = {}
        for r in funding_rows:
            ex = r["exchange"]
            if ex not in by_exchange:
                by_exchange[ex] = []
            by_exchange[ex].append(r)

        all_costs = []
        FUNDING_INTERVAL = 28800  # 8h in seconds
        for ex, rows in by_exchange.items():
            rows.sort(key=lambda r: r["ts"])
            # Deduplicate: snap each row to its 8h funding slot (ts // 28800)
            seen_slots = {}
            for row in rows:
                slot = int(row["ts"] // FUNDING_INTERVAL)
                # Keep the latest reading for each slot
                if slot not in seen_slots or row["ts"] > seen_slots[slot]["ts"]:
                    seen_slots[slot] = row
            deduped = sorted(seen_slots.values(), key=lambda r: r["ts"])

            # Each deduped row = one funding payment
            for row in deduped:
                rate = row.get("rate", 0) or 0
                # Cost: for long, pay if rate > 0; receive if rate < 0
                if side == "long":
                    cost = rate * position_usd
                else:
                    cost = -rate * position_usd  # short is opposite
                all_costs.append(cost)

        total_cost = sum(all_costs)
        # Count unique 8h funding intervals elapsed
        intervals_counted = len(set(int(r["ts"] // FUNDING_INTERVAL) for r in funding_rows))

        # Latest rate
        latest = sorted(funding_rows, key=lambda r: r["ts"])[-1]
        latest_rate = latest.get("rate", 0) or 0

        # Average rate across all samples
        rates = [r.get("rate", 0) or 0 for r in funding_rows]
        avg_rate = sum(rates) / len(rates) if rates else 0

        # Favorable = we're receiving funding (negative cost)
        favorable = total_cost < 0

        result[sym] = {
            "total_cost_usd": round(total_cost, 4),
            "total_cost_pct": round(total_cost / position_usd * 100, 4),
            "intervals_counted": intervals_counted,
            "avg_rate": round(avg_rate * 100, 6),
            "avg_rate_pct": round(avg_rate * 100, 4),
            "latest_rate": round(latest_rate, 8),
            "latest_rate_pct": round(latest_rate * 100, 4),
            "favorable": favorable,
            "side": side,
            "position_usd": position_usd,
            "session_hours": session_hours,
            "description": (
                f"{'📥 Receiving' if favorable else '📤 Paying'} ${abs(total_cost):.4f} "
                f"({'−' if favorable else '+'}{ abs(total_cost/position_usd*100):.4f}%) "
                f"over {session_hours:.1f}h"
            ),
        }

    return {"status": "ok", "ts": now, "symbols": result}


# ── Rolling Max Drawdown ──────────────────────────────────────────────────────

@router.get("/max-drawdown")
async def max_drawdown(
    symbol: Optional[str] = Query(default=None),
    window: int = Query(default=3600, ge=300, le=86400, description="Lookback window in seconds"),
):
    """
    Rolling maximum drawdown: peak-to-trough price drop in the last `window` seconds.
    Returns: max_drawdown_pct (negative = down), peak price, trough price, times.
    """
    symbols = [symbol] if symbol else get_symbols()
    now = time.time()
    result = {}

    for sym in symbols:
        trades = await get_recent_trades(limit=10000, since=now - window, symbol=sym)
        if len(trades) < 3:
            result[sym] = {
                "max_drawdown_pct": 0.0,
                "max_runup_pct": 0.0,
                "peak_price": None,
                "trough_price": None,
                "current_price": None,
                "current_dd_pct": 0.0,
            }
            continue

        trades.sort(key=lambda t: t.get("ts", 0))
        prices = [(t["ts"], t.get("price", 0) or 0) for t in trades if (t.get("price") or 0) > 0]
        if len(prices) < 3:
            continue

        # Compute max drawdown using O(n) running peak approach
        peak = prices[0][1]
        trough = prices[0][1]
        peak_ts = prices[0][0]
        trough_ts = prices[0][0]
        max_dd = 0.0
        max_dd_peak = peak
        max_dd_trough = trough
        max_dd_peak_ts = peak_ts
        max_dd_trough_ts = trough_ts

        cur_peak = prices[0][1]
        cur_peak_ts = prices[0][0]

        for ts, p in prices[1:]:
            if p > cur_peak:
                cur_peak = p
                cur_peak_ts = ts
            dd = (p - cur_peak) / cur_peak * 100 if cur_peak else 0
            if dd < max_dd:
                max_dd = dd
                max_dd_peak = cur_peak
                max_dd_trough = p
                max_dd_peak_ts = cur_peak_ts
                max_dd_trough_ts = ts

        # Max run-up (low-to-high)
        trough_run = prices[0][1]
        trough_run_ts = prices[0][0]
        max_runup = 0.0
        max_ru_trough = trough_run
        max_ru_peak = trough_run

        for ts, p in prices[1:]:
            if p < trough_run:
                trough_run = p
                trough_run_ts = ts
            ru = (p - trough_run) / trough_run * 100 if trough_run else 0
            if ru > max_runup:
                max_runup = ru
                max_ru_trough = trough_run
                max_ru_peak = p

        # Current drawdown from recent peak
        recent_peak = max(p for _, p in prices[-100:])  # last ~100 trades
        current_price = prices[-1][1]
        current_dd = (current_price - recent_peak) / recent_peak * 100 if recent_peak else 0

        result[sym] = {
            "max_drawdown_pct": round(max_dd, 4),
            "max_runup_pct": round(max_runup, 4),
            "peak_price": round(max_dd_peak, 8),
            "trough_price": round(max_dd_trough, 8),
            "peak_ts": round(max_dd_peak_ts, 2),
            "trough_ts": round(max_dd_trough_ts, 2),
            "current_price": round(current_price, 8),
            "current_dd_pct": round(current_dd, 4),
            "recent_peak": round(recent_peak, 8),
            "window_s": window,
        }

    return {"status": "ok", "ts": now, "symbols": result}


# ---------------------------------------------------------------------------
# OB Wall Strength Decay Tracker
# ---------------------------------------------------------------------------

@router.get("/ob-wall-decay")
async def ob_wall_decay(
    symbol: str = Query("BANANAS31USDT"),
    window: int = Query(300, description="Lookback window in seconds (default 5min)"),
    wall_threshold_pct: float = Query(0.5, description="Min % of total side volume to be a wall"),
    price_cluster_pct: float = Query(0.05, description="Price cluster range % for wall detection"),
):
    """
    OB wall strength decay tracker.
    Detects significant bid/ask walls in the orderbook over time and tracks
    how their volume changes across snapshots in the past window seconds.
    Returns: time series of top bid wall and ask wall strengths.
    """
    now = time.time()
    since = now - window
    target = symbol.upper()

    snapshots = await get_orderbook_snapshots_for_heatmap(target, since, sample_interval=5)

    if not snapshots:
        return {"status": "ok", "symbol": target, "window_s": window, "series": [], "decay": {}}

    def detect_walls(levels: list, total_volume: float, cluster_pct: float, threshold_pct: float):
        if not levels or total_volume == 0:
            return []
        levels_sorted = sorted(levels, key=lambda x: float(x[0]))
        walls = []
        i = 0
        while i < len(levels_sorted):
            base_price = float(levels_sorted[i][0])
            cluster_vol = float(levels_sorted[i][1])
            j = i + 1
            while j < len(levels_sorted):
                p = float(levels_sorted[j][0])
                if abs(p - base_price) / base_price * 100 <= cluster_pct:
                    cluster_vol += float(levels_sorted[j][1])
                    j += 1
                else:
                    break
            pct = cluster_vol / total_volume * 100
            if pct >= threshold_pct:
                walls.append({
                    "price": round(base_price, 8),
                    "volume": round(cluster_vol, 2),
                    "pct_of_side": round(pct, 2),
                })
            i = j
        walls.sort(key=lambda x: x["volume"], reverse=True)
        return walls[:3]

    series = []
    for snap in snapshots:
        ts = snap["ts"]
        try:
            bids = json.loads(snap["bids"]) if isinstance(snap["bids"], str) else snap["bids"]
            asks = json.loads(snap["asks"]) if isinstance(snap["asks"], str) else snap["asks"]
        except Exception:
            continue

        total_bid_vol = sum(float(b[1]) for b in bids) if bids else 0
        total_ask_vol = sum(float(a[1]) for a in asks) if asks else 0

        bid_walls = detect_walls(bids, total_bid_vol, price_cluster_pct, wall_threshold_pct)
        ask_walls = detect_walls(asks, total_ask_vol, price_cluster_pct, wall_threshold_pct)

        top_bid = bid_walls[0] if bid_walls else None
        top_ask = ask_walls[0] if ask_walls else None

        series.append({
            "ts": round(ts, 2),
            "mid_price": snap.get("mid_price"),
            "top_bid_wall": top_bid,
            "top_ask_wall": top_ask,
            "top_bid_vol": round(top_bid["volume"], 2) if top_bid else 0,
            "top_ask_vol": round(top_ask["volume"], 2) if top_ask else 0,
            "top_bid_pct": round(top_bid["pct_of_side"], 2) if top_bid else 0,
            "top_ask_pct": round(top_ask["pct_of_side"], 2) if top_ask else 0,
        })

    decay_info = {}
    if len(series) >= 2:
        first = series[0]
        last = series[-1]
        bid_decay = None
        ask_decay = None
        if first["top_bid_vol"] > 0:
            bid_decay = round((last["top_bid_vol"] - first["top_bid_vol"]) / first["top_bid_vol"] * 100, 2)
        if first["top_ask_vol"] > 0:
            ask_decay = round((last["top_ask_vol"] - first["top_ask_vol"]) / first["top_ask_vol"] * 100, 2)
        decay_info = {
            "bid_wall_decay_pct": bid_decay,
            "ask_wall_decay_pct": ask_decay,
            "window_s": window,
            "snapshots": len(series),
            "bid_trend": "weakening" if bid_decay is not None and bid_decay < -5 else "strengthening" if bid_decay is not None and bid_decay > 5 else "stable",
            "ask_trend": "weakening" if ask_decay is not None and ask_decay < -5 else "strengthening" if ask_decay is not None and ask_decay > 5 else "stable",
        }

    return {
        "status": "ok",
        "symbol": target,
        "ts": now,
        "decay": decay_info,
        "series": series,
    }


@router.get("/flow-imbalance")
async def flow_imbalance(
    symbol: str = Query("BANANAS31USDT"),
    window: int = Query(3600, description="Window in seconds (default 1h)"),
    bucket_size: int = Query(60, description="Bucket size in seconds (default 1m)"),
):
    """
    Trade flow imbalance ratio chart.
    Returns rolling buy/sell volume ratio as time series.
    Ratio = buy_vol / (buy_vol + sell_vol), range [0..1].
    0.5 = balanced, >0.5 = buy-dominant, <0.5 = sell-dominant.
    """
    target = symbol.upper()
    now = time.time()

    candles = await get_ohlcv(interval_seconds=bucket_size, window_seconds=window, symbol=target)

    series = []
    for c in candles:
        buy_vol = c.get("buy_volume", 0) or 0
        sell_vol = c.get("sell_volume", 0) or 0
        total = buy_vol + sell_vol
        ratio = round(buy_vol / total, 4) if total > 0 else None
        series.append({
            "ts": c["ts"],
            "buy_vol": round(buy_vol, 4),
            "sell_vol": round(sell_vol, 4),
            "total_vol": round(total, 4),
            "ratio": ratio,
            "label": "buy" if ratio is not None and ratio > 0.55 else "sell" if ratio is not None and ratio < 0.45 else "neutral",
        })

    # Rolling 5-bucket average ratio
    window_size = 5
    for i, s in enumerate(series):
        slice_ = [x["ratio"] for x in series[max(0, i - window_size + 1):i + 1] if x["ratio"] is not None]
        s["ratio_ma5"] = round(sum(slice_) / len(slice_), 4) if slice_ else None

    # Summary stats
    valid_ratios = [s["ratio"] for s in series if s["ratio"] is not None]
    summary = {}
    if valid_ratios:
        avg_ratio = sum(valid_ratios) / len(valid_ratios)
        total_buy = sum(s["buy_vol"] for s in series)
        total_sell = sum(s["sell_vol"] for s in series)
        summary = {
            "avg_ratio": round(avg_ratio, 4),
            "total_buy_vol": round(total_buy, 4),
            "total_sell_vol": round(total_sell, 4),
            "bias": "buy" if avg_ratio > 0.55 else "sell" if avg_ratio < 0.45 else "neutral",
            "bias_strength": round(abs(avg_ratio - 0.5) * 200, 1),  # 0-100 scale
            "buckets": len(series),
        }

    return {
        "status": "ok",
        "symbol": target,
        "window_s": window,
        "bucket_size_s": bucket_size,
        "ts": now,
        "summary": summary,
        "series": series,
    }


# ---------------------------------------------------------------------------
# Volatility Regime Detector
# ---------------------------------------------------------------------------
@router.get("/volatility-regime")
async def volatility_regime_endpoint(
    symbol: Optional[str] = None,
    period: int = Query(default=14, ge=5, le=50),
    lookback_periods: int = Query(default=100, ge=20, le=500),
):
    """
    Classify price action volatility as low/medium/high using ATR percentile.

    Computes ATR(period) on 1-min candles over lookback_periods windows,
    then classifies current ATR vs its own historical distribution.
    - percentile < 33 → LOW volatility
    - percentile 33–66 → MEDIUM volatility
    - percentile > 66 → HIGH volatility
    """
    syms = get_symbols()
    target = symbol if symbol and symbol in syms else syms[0]

    # Fetch enough candles for lookback_periods + period ATR windows
    total_candles_needed = lookback_periods + period + 5
    window_seconds = total_candles_needed * 60  # 1-min candles

    candles = await get_ohlcv(interval_seconds=60, window_seconds=window_seconds, symbol=target)

    if len(candles) < period + 10:
        return {
            "status": "ok",
            "symbol": target,
            "regime": "unknown",
            "regime_label": "Unknown",
            "percentile": None,
            "current_atr_pct": None,
            "atr_history": [],
            "note": "insufficient data",
        }

    highs  = [c["high"]  for c in candles]
    lows   = [c["low"]   for c in candles]
    closes = [c["close"] for c in candles]

    # Compute TR for each candle
    trs = []
    for i in range(1, len(candles)):
        tr = max(highs[i] - lows[i], abs(highs[i] - closes[i-1]), abs(lows[i] - closes[i-1]))
        trs.append(tr)

    # Compute rolling ATR values (Wilder) for each window ending point
    atr_values = []  # list of (atr_pct, close_price)

    # Warm up: compute first ATR at index period-1 of trs
    if len(trs) >= period:
        atr = sum(trs[:period]) / period
        close_price = closes[period]
        atr_pct = atr / close_price * 100 if close_price else 0
        atr_values.append(atr_pct)

        for i in range(period, len(trs)):
            atr = (atr * (period - 1) + trs[i]) / period
            close_price = closes[i + 1] if i + 1 < len(closes) else closes[i]
            atr_pct = atr / close_price * 100 if close_price else 0
            atr_values.append(atr_pct)

    if not atr_values:
        return {
            "status": "ok",
            "symbol": target,
            "regime": "unknown",
            "regime_label": "Unknown",
            "percentile": None,
            "current_atr_pct": None,
            "atr_history": [],
        }

    current_atr_pct = atr_values[-1]
    sorted_atrs = sorted(atr_values)
    n = len(sorted_atrs)

    # Compute percentile of current ATR within its own history
    rank = sum(1 for v in sorted_atrs if v <= current_atr_pct)
    percentile = (rank / n) * 100

    if percentile < 33:
        regime = "low"
        regime_label = "Low Volatility"
        regime_color = "#26a69a"  # teal
    elif percentile < 67:
        regime = "medium"
        regime_label = "Medium Volatility"
        regime_color = "#ffb74d"  # amber
    else:
        regime = "high"
        regime_label = "High Volatility"
        regime_color = "#ef5350"  # red

    # Return last 60 ATR values for sparkline
    history_slice = atr_values[-60:]
    p33 = sorted_atrs[int(n * 0.33)]
    p67 = sorted_atrs[int(n * 0.67)]

    return {
        "status": "ok",
        "symbol": target,
        "regime": regime,
        "regime_label": regime_label,
        "regime_color": regime_color,
        "percentile": round(percentile, 1),
        "current_atr_pct": round(current_atr_pct, 4),
        "p33_atr_pct": round(p33, 4),
        "p67_atr_pct": round(p67, 4),
        "atr_history": [round(v, 4) for v in history_slice],
        "period": period,
        "lookback_periods": lookback_periods,
        "candles_used": len(candles),
    }


@router.get("/volatility-regime/all")
async def volatility_regime_all(period: int = Query(default=14, ge=5, le=50)):
    """Volatility regime for all tracked symbols."""
    import asyncio

    async def _fetch(sym: str):
        return await volatility_regime_endpoint(symbol=sym, period=int(period), lookback_periods=100)

    syms = get_symbols()
    results = await asyncio.gather(*[_fetch(s) for s in syms], return_exceptions=True)
    out = {}
    for s, r in zip(syms, results):
        if isinstance(r, Exception):
            out[s] = {"regime": "unknown", "error": str(r)}
        else:
            out[s] = r
    return {"status": "ok", "symbols": out}
