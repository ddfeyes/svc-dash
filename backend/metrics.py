"""Computed metrics: CVD, volume imbalance, OI momentum, phase classifier — multi-symbol."""
import time
from typing import Dict, List, Optional

from storage import (
    get_trades_for_cvd,
    get_oi_history,
    get_latest_orderbook,
    get_funding_history,
)


async def compute_cvd(window_seconds: int = 3600, symbol: str = None) -> List[Dict]:
    """Cumulative Volume Delta over the last window."""
    since = time.time() - window_seconds
    trades = await get_trades_for_cvd(since, symbol=symbol)

    cvd = 0.0
    result = []
    for t in trades:
        delta = t["qty"] if t["side"] in ("buy", "Buy") else -t["qty"]
        cvd += delta
        result.append({
            "ts": t["ts"],
            "price": t["price"],
            "cvd": round(cvd, 6),
            "delta": round(delta, 6),
        })

    # Downsample to ~300 points for frontend
    if len(result) > 300:
        step = len(result) // 300
        result = result[::step]

    return result


async def compute_volume_imbalance(window_seconds: int = 60, symbol: str = None) -> Dict:
    """Buy vs sell volume ratio over window."""
    since = time.time() - window_seconds
    trades = await get_trades_for_cvd(since, symbol=symbol)

    buy_vol = sum(t["qty"] for t in trades if t["side"] in ("buy", "Buy"))
    sell_vol = sum(t["qty"] for t in trades if t["side"] in ("sell", "Sell"))
    total = buy_vol + sell_vol

    imbalance = (buy_vol - sell_vol) / total if total > 0 else 0

    return {
        "buy_volume": round(buy_vol, 4),
        "sell_volume": round(sell_vol, 4),
        "total_volume": round(total, 4),
        "imbalance": round(imbalance, 4),
        "window_seconds": window_seconds,
    }


async def compute_oi_momentum(window_seconds: int = 300, symbol: str = None) -> Dict:
    """OI rate of change over window."""
    since = time.time() - window_seconds
    oi_data = await get_oi_history(limit=500, since=since, symbol=symbol)

    if len(oi_data) < 2:
        return {"momentum": 0, "oi_start": None, "oi_end": None, "pct_change": 0}

    exchanges = {}
    for row in oi_data:
        ex = row["exchange"]
        if ex not in exchanges:
            exchanges[ex] = []
        exchanges[ex].append(row["oi_value"])

    results = {}
    for ex, values in exchanges.items():
        if len(values) >= 2:
            start, end = values[0], values[-1]
            pct = ((end - start) / start * 100) if start else 0
            results[ex] = {
                "oi_start": round(start, 2),
                "oi_end": round(end, 2),
                "pct_change": round(pct, 4),
                "momentum": round(end - start, 2),
            }

    all_pct = [v["pct_change"] for v in results.values()]
    avg_pct = sum(all_pct) / len(all_pct) if all_pct else 0

    return {
        "exchanges": results,
        "avg_pct_change": round(avg_pct, 4),
        "window_seconds": window_seconds,
    }


async def classify_market_phase(symbol: str = None) -> Dict:
    """
    Phase classifier based on OI momentum + price change + CVD.

    Phases:
    - Accumulation: price flat/down, OI up, CVD slightly positive
    - Distribution: price flat/up, OI up, CVD slightly negative or diverging
    - Markup: price up, OI up, CVD strongly positive
    - Markdown: price down, OI up or down, CVD strongly negative
    """
    cvd_data = await compute_cvd(window_seconds=300, symbol=symbol)
    oi_mom = await compute_oi_momentum(window_seconds=300, symbol=symbol)
    ob_data = await get_latest_orderbook(symbol=symbol, limit=2)

    price_change_pct = 0.0
    if len(ob_data) >= 2:
        p1 = ob_data[0].get("mid_price") or 0
        p2 = ob_data[-1].get("mid_price") or 0
        if p2:
            price_change_pct = ((p1 - p2) / p2) * 100

    cvd_delta = 0.0
    if len(cvd_data) >= 2:
        cvd_delta = cvd_data[-1]["cvd"] - cvd_data[0]["cvd"]

    oi_pct = oi_mom.get("avg_pct_change", 0)

    phase = "Unknown"
    confidence = 0.5

    if price_change_pct > 0.1 and cvd_delta > 0 and oi_pct > 0:
        phase = "Markup"
        confidence = min(0.95, 0.6 + abs(price_change_pct) * 0.1 + abs(cvd_delta) * 0.01)
    elif price_change_pct < -0.1 and cvd_delta < 0:
        phase = "Markdown"
        confidence = min(0.95, 0.6 + abs(price_change_pct) * 0.1 + abs(cvd_delta) * 0.01)
    elif abs(price_change_pct) <= 0.1 and oi_pct > 0 and cvd_delta >= 0:
        phase = "Accumulation"
        confidence = 0.55 + oi_pct * 0.5
    elif abs(price_change_pct) <= 0.1 and oi_pct > 0 and cvd_delta < 0:
        phase = "Distribution"
        confidence = 0.55 + oi_pct * 0.5
    elif oi_pct < -0.05:
        phase = "Markdown" if cvd_delta < 0 else "Accumulation"
        confidence = 0.5

    confidence = min(0.99, max(0.1, confidence))

    return {
        "phase": phase,
        "confidence": round(confidence, 3),
        "signals": {
            "price_change_pct": round(price_change_pct, 4),
            "cvd_delta": round(cvd_delta, 4),
            "oi_pct_change": round(oi_pct, 4),
        },
        "description": _phase_description(phase),
    }


def _phase_description(phase: str) -> str:
    return {
        "Accumulation": "Smart money buying quietly; price consolidating with rising OI",
        "Distribution": "Smart money selling into strength; OI rising but CVD diverging",
        "Markup": "Sustained uptrend with strong buying pressure and rising OI",
        "Markdown": "Sustained downtrend with selling pressure",
        "Unknown": "Insufficient data to classify market phase",
    }.get(phase, "")
