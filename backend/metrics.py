"""Computed metrics: CVD, volume imbalance, OI momentum, phase classifier — multi-symbol."""
import time
from typing import Dict, List, Optional

from storage import (
    get_trades_for_cvd,
    get_trades_for_volume_profile,
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


async def detect_oi_spike(window_seconds: int = 300, threshold_pct: float = 3.0, symbol: str = None) -> Dict:
    """
    OI spike detector: if OI changes >threshold_pct in window, alert.
    """
    since = time.time() - window_seconds
    oi_data = await get_oi_history(limit=500, since=since, symbol=symbol)

    if len(oi_data) < 2:
        return {"spike": False, "exchanges": {}, "description": "Insufficient OI data"}

    exchanges = {}
    for row in oi_data:
        ex = row["exchange"]
        if ex not in exchanges:
            exchanges[ex] = []
        exchanges[ex].append({"ts": row["ts"], "oi": row["oi_value"]})

    results = {}
    spikes = []
    for ex, rows in exchanges.items():
        if len(rows) < 2:
            continue
        oi_start = rows[0]["oi"]
        oi_end = rows[-1]["oi"]
        if oi_start == 0:
            continue
        pct = (oi_end - oi_start) / oi_start * 100
        direction = "up" if pct > 0 else "down"
        is_spike = abs(pct) >= threshold_pct
        results[ex] = {
            "oi_start": round(oi_start, 2),
            "oi_end": round(oi_end, 2),
            "pct_change": round(pct, 4),
            "direction": direction,
            "spike": is_spike,
        }
        if is_spike:
            spikes.append(f"{ex}: OI {direction} {abs(pct):.2f}%")

    overall_spike = len(spikes) > 0
    description = " | ".join(spikes) if spikes else "OI stable"

    return {
        "spike": overall_spike,
        "exchanges": results,
        "description": description,
        "threshold_pct": threshold_pct,
        "window_seconds": window_seconds,
    }


async def detect_delta_divergence(window_seconds: int = 300, symbol: str = None) -> Dict:
    """
    Delta divergence: price moving up but CVD moving down (or vice versa).
    Returns severity: none | weak | strong
    """
    since = time.time() - window_seconds
    trades = await get_trades_for_cvd(since, symbol=symbol)
    ob = await get_latest_orderbook(symbol=symbol, limit=5)

    if len(trades) < 20 or len(ob) < 2:
        return {"divergence": "none", "severity": 0, "description": "Insufficient data"}

    # Split into two halves
    mid = len(trades) // 2
    first_half = trades[:mid]
    second_half = trades[mid:]

    def cvd_of(ts_list):
        c = 0.0
        for t in ts_list:
            c += t["qty"] if t["side"] in ("buy", "Buy") else -t["qty"]
        return c

    cvd1 = cvd_of(first_half)
    cvd2 = cvd_of(second_half)
    cvd_change = cvd2 - cvd1  # positive = CVD rising

    # Price change
    p_start = ob[-1].get("mid_price") or 0
    p_end = ob[0].get("mid_price") or 0
    price_change = p_end - p_start if p_start else 0
    price_pct = (price_change / p_start * 100) if p_start else 0

    # Normalize CVD change relative to total volume
    total_vol = sum(t["qty"] for t in trades) or 1
    cvd_norm = cvd_change / total_vol  # -1 to +1

    divergence = "none"
    severity = 0
    description = "No divergence"

    if price_pct > 0.05 and cvd_norm < -0.05:
        divergence = "bearish"
        severity = min(1.0, abs(price_pct) * 0.3 + abs(cvd_norm) * 0.7)
        description = f"⚠ Price up {price_pct:.2f}% but CVD falling — bearish divergence"
    elif price_pct < -0.05 and cvd_norm > 0.05:
        divergence = "bullish"
        severity = min(1.0, abs(price_pct) * 0.3 + abs(cvd_norm) * 0.7)
        description = f"⚠ Price down {abs(price_pct):.2f}% but CVD rising — bullish divergence"

    return {
        "divergence": divergence,
        "severity": round(severity, 3),
        "price_change_pct": round(price_pct, 4),
        "cvd_norm": round(cvd_norm, 4),
        "description": description,
        "window_seconds": window_seconds,
    }


async def detect_large_trades(window_seconds: int = 300, min_usd: float = 10000, symbol: str = None) -> Dict:
    """
    Detect large individual trades > min_usd.
    """
    since = time.time() - window_seconds
    trades = await get_trades_for_cvd(since, symbol=symbol)

    large = []
    for t in trades:
        value = t["price"] * t["qty"]
        if value >= min_usd:
            large.append({
                "ts": t["ts"],
                "price": t["price"],
                "qty": t["qty"],
                "side": t["side"],
                "value_usd": round(value, 2),
            })

    large.sort(key=lambda x: x["value_usd"], reverse=True)

    buy_vol = sum(l["value_usd"] for l in large if l["side"] in ("buy", "Buy"))
    sell_vol = sum(l["value_usd"] for l in large if l["side"] not in ("buy", "Buy"))

    return {
        "count": len(large),
        "trades": large[:20],  # top 20 by size
        "total_buy_usd": round(buy_vol, 2),
        "total_sell_usd": round(sell_vol, 2),
        "min_usd_threshold": min_usd,
        "window_seconds": window_seconds,
    }


async def compute_volume_profile(symbol: str, timeframe_seconds: int = 3600) -> dict:
    """
    Volume Profile: POC, VAH, VAL over the last timeframe_seconds.

    - POC (Point of Control): price level with highest traded volume
    - Value Area: price range containing 70% of total volume
    - VAH (Value Area High): upper bound of value area
    - VAL (Value Area Low): lower bound of value area

    Returns dict with poc_price, poc_volume, vah, val, and full profile list.
    """
    since = time.time() - timeframe_seconds
    rows, tick_size = await get_trades_for_volume_profile(since, symbol=symbol)

    if not rows:
        return {
            "poc_price": None,
            "poc_volume": None,
            "vah": None,
            "val": None,
            "profile": [],
            "total_volume": 0,
            "timeframe_seconds": timeframe_seconds,
            "tick_size": None,
        }

    # Determine display precision from tick_size
    import math
    decimals = max(0, -int(math.floor(math.log10(tick_size)))) + 1 if tick_size > 0 else 2

    # Build profile list sorted by price
    profile = [{"price": row["price_level"], "volume": row["volume"]} for row in rows]
    total_volume = sum(p["volume"] for p in profile)

    # POC: level with maximum volume
    poc = max(profile, key=lambda x: x["volume"])
    poc_price = poc["price"]
    poc_volume = poc["volume"]

    # Value Area: 70% of total volume centered around POC
    value_area_target = total_volume * 0.70

    # Start value area at POC, expand outward (higher/lower) one level at a time
    # taking the side with greater volume each step
    poc_idx = next(i for i, p in enumerate(profile) if p["price"] == poc_price)
    lo_idx = poc_idx
    hi_idx = poc_idx
    accumulated = poc_volume

    while accumulated < value_area_target:
        # Candidate volumes above and below current bounds
        can_go_up = hi_idx + 1 < len(profile)
        can_go_down = lo_idx - 1 >= 0

        if not can_go_up and not can_go_down:
            break

        vol_up = profile[hi_idx + 1]["volume"] if can_go_up else -1
        vol_down = profile[lo_idx - 1]["volume"] if can_go_down else -1

        if vol_up >= vol_down:
            hi_idx += 1
            accumulated += vol_up
        else:
            lo_idx -= 1
            accumulated += vol_down

    vah = profile[hi_idx]["price"]
    val = profile[lo_idx]["price"]

    return {
        "poc_price": round(poc_price, decimals),
        "poc_volume": round(poc_volume, 6),
        "vah": round(vah, decimals),
        "val": round(val, decimals),
        "total_volume": round(total_volume, 6),
        "value_area_pct": round(accumulated / total_volume * 100, 2) if total_volume else 0,
        "tick_size": tick_size,
        "profile": [{"price": round(p["price"], decimals), "volume": round(p["volume"], 6)} for p in profile],
        "timeframe_seconds": timeframe_seconds,
    }


def _phase_description(phase: str) -> str:
    return {
        "Accumulation": "Smart money buying quietly; price consolidating with rising OI",
        "Distribution": "Smart money selling into strength; OI rising but CVD diverging",
        "Markup": "Sustained uptrend with strong buying pressure and rising OI",
        "Markdown": "Sustained downtrend with selling pressure",
        "Unknown": "Insufficient data to classify market phase",
    }.get(phase, "")
