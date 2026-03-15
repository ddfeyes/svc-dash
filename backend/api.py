"""FastAPI REST endpoints — multi-symbol."""
import time
from typing import Optional

from fastapi import APIRouter, Query
from fastapi.responses import JSONResponse

from collectors import get_symbols
from storage import (
    get_latest_orderbook,
    get_recent_trades,
    get_oi_history,
    get_funding_history,
    get_recent_liquidations,
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
)

router = APIRouter(prefix="/api")


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
    symbol: Optional[str] = Query(default=None),
    timeframe: int = Query(default=3600, le=86400, description="Lookback window in seconds"),
):
    """
    Volume Profile for a symbol over the given timeframe.
    Returns POC (Point of Control), VAH (Value Area High), VAL (Value Area Low),
    and the full price/volume profile with 0.01 price resolution.
    """
    syms = get_symbols()
    target = symbol if symbol and symbol in syms else syms[0]
    data = await compute_volume_profile(symbol=target, timeframe_seconds=timeframe)
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
