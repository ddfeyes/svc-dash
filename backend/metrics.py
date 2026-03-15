"""Computed metrics: CVD, volume imbalance, OI momentum, phase classifier — multi-symbol."""
import asyncio
import time
from typing import Dict, List, Optional

from storage import (
    get_trades_for_cvd,
    get_trades_for_volume_profile,
    get_oi_history,
    get_latest_orderbook,
    get_funding_history,
    get_recent_trades,
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


_phase_history: dict = {}  # per-symbol rolling history for smoothing

async def classify_market_phase(symbol: str = None) -> Dict:
    """
    Phase classifier v2: multi-window lookback + confidence smoothing.

    Uses 3 windows (1min, 5min, 15min) and weights signals by recency.
    Applies exponential smoothing to confidence to reduce noise.
    """
    # Gather signals at multiple timeframes
    windows = [60, 300, 900]
    weights = [0.5, 0.3, 0.2]

    tasks = [
        compute_cvd(window_seconds=w, symbol=symbol) for w in windows
    ] + [
        compute_oi_momentum(window_seconds=w, symbol=symbol) for w in windows
    ]
    results = await asyncio.gather(*[asyncio.create_task(t) for t in tasks], return_exceptions=True)

    cvd_results = results[:3]
    oi_results = results[3:]

    ob_data = await get_latest_orderbook(symbol=symbol, limit=1)

    # Price change using trade prices from CVD data (spans the full window)
    def price_change_from_cvd(data):
        if isinstance(data, Exception) or len(data) < 2:
            return 0.0
        p_now = data[-1].get("price") or 0
        p_old = data[0].get("price") or 0
        if not p_old:
            return 0.0
        return (p_now - p_old) / p_old * 100

    # Short-term (1min) and broad (15min) price change
    price_pct_now   = price_change_from_cvd(cvd_results[0])   # 1min window
    price_pct_broad = price_change_from_cvd(cvd_results[2])   # 15min window

    # CVD deltas
    def cvd_delta_of(data):
        if isinstance(data, Exception) or len(data) < 2:
            return 0.0
        return data[-1]["cvd"] - data[0]["cvd"]

    cvd_deltas = [cvd_delta_of(r) for r in cvd_results]
    oi_pcts = []
    for r in oi_results:
        if isinstance(r, Exception):
            oi_pcts.append(0.0)
        else:
            oi_pcts.append(r.get("avg_pct_change", 0))

    # Weighted aggregation
    def weighted(vals, ws):
        total_w = sum(ws[:len(vals)])
        return sum(v * w for v, w in zip(vals, ws)) / total_w if total_w > 0 else 0

    w_cvd = weighted(cvd_deltas, weights)
    w_oi = weighted(oi_pcts, weights)
    w_price = price_pct_now * 0.6 + price_pct_broad * 0.4

    # Normalize CVD by estimating total volume (rough)
    cvd_norm = 0.0
    if cvd_results[0] and not isinstance(cvd_results[0], Exception) and len(cvd_results[0]) > 1:
        total_abs = sum(abs(p.get("delta", 0)) for p in cvd_results[0])
        cvd_norm = w_cvd / total_abs if total_abs > 0 else 0

    # Classification
    phase = "Unknown"
    raw_conf = 0.5

    price_up = w_price > 0.05
    price_dn = w_price < -0.05
    price_flat = not price_up and not price_dn
    oi_up = w_oi > 0.01
    oi_dn = w_oi < -0.01
    cvd_pos = cvd_norm > 0.02
    cvd_neg = cvd_norm < -0.02

    if price_up and cvd_pos and oi_up:
        phase = "Markup"
        raw_conf = 0.6 + abs(w_price) * 0.08 + abs(cvd_norm) * 0.3
    elif price_up and cvd_pos:
        phase = "Markup"
        raw_conf = 0.55 + abs(w_price) * 0.06 + abs(cvd_norm) * 0.2
    elif price_dn and cvd_neg:
        phase = "Markdown"
        raw_conf = 0.6 + abs(w_price) * 0.08 + abs(cvd_norm) * 0.3
    elif price_dn and not cvd_neg:
        phase = "Markdown"
        raw_conf = 0.5 + abs(w_price) * 0.06
    elif price_flat and oi_up and cvd_pos:
        phase = "Accumulation"
        raw_conf = 0.55 + abs(w_oi) * 2 + abs(cvd_norm) * 0.3
    elif price_flat and oi_up and cvd_neg:
        phase = "Distribution"
        raw_conf = 0.55 + abs(w_oi) * 2 + abs(cvd_norm) * 0.3
    elif price_flat and cvd_pos:
        # Buying pressure on flat price = accumulation
        phase = "Accumulation"
        raw_conf = 0.5 + abs(cvd_norm) * 0.2
    elif price_flat and cvd_neg:
        # Selling pressure on flat price = distribution
        phase = "Distribution"
        raw_conf = 0.5 + abs(cvd_norm) * 0.2
    elif price_up and cvd_neg:
        phase = "Distribution"
        raw_conf = 0.5 + abs(cvd_norm) * 0.4
    elif price_dn and cvd_pos:
        phase = "Accumulation"
        raw_conf = 0.5 + abs(cvd_norm) * 0.3
    elif oi_dn:
        phase = "Markdown" if w_price < 0 else "Distribution"
        raw_conf = 0.45
    else:
        phase = "Accumulation"
        raw_conf = 0.4

    raw_conf = min(0.99, max(0.1, raw_conf))

    # Exponential smoothing on confidence using per-symbol rolling history
    global _phase_history
    sym_key = symbol or "__default__"
    if sym_key not in _phase_history:
        _phase_history[sym_key] = []
    _phase_history[sym_key].append({"phase": phase, "conf": raw_conf})
    if len(_phase_history[sym_key]) > 10:
        _phase_history[sym_key] = _phase_history[sym_key][-10:]

    # Smooth: recent phases that agree boost confidence
    hist = _phase_history[sym_key]
    same_phase_count = sum(1 for h in hist if h["phase"] == phase)
    smooth_conf = raw_conf * 0.7 + (same_phase_count / len(hist)) * 0.3

    return {
        "phase": phase,
        "confidence": round(min(0.99, smooth_conf), 3),
        "signals": {
            "price_change_pct": round(w_price, 4),
            "cvd_delta": round(w_cvd, 4),
            "cvd_norm": round(cvd_norm, 4),
            "oi_pct_change": round(w_oi, 4),
        },
        "description": _phase_description(phase),
        "lookback_windows": windows,
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


async def detect_liquidation_cascade(window_seconds: int = 60, threshold_usd: float = 50000, symbol: str = None) -> Dict:
    """
    Liquidation cascade: detect bursts of liquidations > threshold_usd in window.
    A cascade is when liquidation value spikes >threshold_usd in 60s.
    """
    since = time.time() - window_seconds
    from storage import get_recent_liquidations
    liqs = await get_recent_liquidations(limit=500, since=since, symbol=symbol)

    if not liqs:
        return {"cascade": False, "total_usd": 0, "buy_usd": 0, "sell_usd": 0, "description": "No liquidations"}

    buy_usd = sum(l.get("value", 0) for l in liqs if l.get("side") == "buy")
    sell_usd = sum(l.get("value", 0) for l in liqs if l.get("side") != "buy")
    total_usd = buy_usd + sell_usd

    cascade = total_usd >= threshold_usd
    dominant = "longs" if sell_usd > buy_usd else "shorts"
    dominant_val = max(buy_usd, sell_usd)

    if cascade:
        description = f"🚨 Cascade: ${total_usd:,.0f} liquidated in {window_seconds}s ({dominant} dominant: ${dominant_val:,.0f})"
    else:
        description = f"${total_usd:,.0f} liquidated (threshold: ${threshold_usd:,.0f})"

    # Bucket by 10s for sparkline
    buckets = {}
    for l in liqs:
        bucket = int((l["ts"] - since) / 10)
        if bucket not in buckets:
            buckets[bucket] = 0.0
        buckets[bucket] += l.get("value", 0)

    sparkline = [round(buckets.get(i, 0), 2) for i in range(int(window_seconds / 10))]

    return {
        "cascade": cascade,
        "total_usd": round(total_usd, 2),
        "buy_usd": round(buy_usd, 2),
        "sell_usd": round(sell_usd, 2),
        "count": len(liqs),
        "dominant": dominant,
        "description": description,
        "sparkline": sparkline,
        "threshold_usd": threshold_usd,
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


async def compute_volume_profile(symbol: str, window_seconds: int = 3600, bins: int = 50) -> dict:
    """
    Volume Profile: POC, VAH, VAL over the last window_seconds.

    - POC (Point of Control): price level with highest traded volume
    - Value Area: price range containing 70% of total volume
    - VAH (Value Area High): upper bound of value area
    - VAL (Value Area Low): lower bound of value area

    Returns dict with poc, vah, val, bins[], value_area_pct.
    """
    since = time.time() - window_seconds
    rows, tick_size = await get_trades_for_volume_profile(since, symbol=symbol)

    if not rows:
        return {
            "poc": None,
            "vah": None,
            "val": None,
            "bins": [],
            "total_volume": 0,
            "value_area_pct": 70,
            "window_seconds": window_seconds,
            "tick_size": None,
        }

    # Determine display precision from tick_size
    import math
    decimals = max(0, -int(math.floor(math.log10(tick_size)))) + 1 if tick_size > 0 else 6

    # Build profile list sorted by price (include buy/sell split)
    raw_profile = [
        {
            "price": row["price_level"],
            "volume": row["volume"],
            "buy_vol": row.get("buy_vol", 0) or 0,
            "sell_vol": row.get("sell_vol", 0) or 0,
        }
        for row in rows
    ]

    # Downsample to `bins` buckets if we have more raw levels
    if len(raw_profile) > bins and bins > 0:
        p_low  = raw_profile[0]["price"]
        p_high = raw_profile[-1]["price"]
        p_rng  = p_high - p_low
        bin_size = p_rng / bins if p_rng > 0 else 1

        bin_map: dict = {}
        for entry in raw_profile:
            b_idx = min(bins - 1, int((entry["price"] - p_low) / bin_size))
            center = round(p_low + (b_idx + 0.5) * bin_size, decimals)
            if center not in bin_map:
                bin_map[center] = {"price": center, "volume": 0.0, "buy_vol": 0.0, "sell_vol": 0.0}
            bin_map[center]["volume"]   += entry["volume"]
            bin_map[center]["buy_vol"]  += entry["buy_vol"]
            bin_map[center]["sell_vol"] += entry["sell_vol"]

        profile = sorted(bin_map.values(), key=lambda x: x["price"])
    else:
        profile = raw_profile

    total_volume = sum(p["volume"] for p in profile)

    # POC: level with maximum volume
    poc_entry = max(profile, key=lambda x: x["volume"])
    poc_price = poc_entry["price"]
    poc_volume = poc_entry["volume"]

    # Value Area: 70% of total volume centered around POC
    value_area_target = total_volume * 0.70

    poc_idx = next(i for i, p in enumerate(profile) if p["price"] == poc_price)
    lo_idx = poc_idx
    hi_idx = poc_idx
    accumulated = poc_volume

    while accumulated < value_area_target:
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
        "poc": round(poc_price, decimals),
        "poc_volume": round(poc_volume, 6),
        "vah": round(vah, decimals),
        "val": round(val, decimals),
        "total_volume": round(total_volume, 6),
        "value_area_pct": round(accumulated / total_volume * 100, 2) if total_volume else 0,
        "tick_size": tick_size,
        "bins": [
            {
                "price": round(p["price"], decimals),
                "volume": round(p["volume"], 6),
                "buy_vol": round(p["buy_vol"], 6),
                "sell_vol": round(p["sell_vol"], 6),
            }
            for p in profile
        ],
        "window_seconds": window_seconds,
    }


async def detect_funding_extreme(symbol: str = None, threshold_pct: float = 0.1) -> Dict:
    """
    Detect extreme funding rates (>threshold_pct% or <-threshold_pct%).
    Extreme funding = squeeze risk: shorts squeezed if funding very positive,
    longs squeezed if funding very negative.
    """
    funding = await get_funding_history(limit=4, symbol=symbol)
    if not funding:
        return {"extreme": False, "rates": {}, "description": "No funding data", "direction": None}

    rates = {}
    for row in funding:
        ex = row["exchange"]
        if ex not in rates:
            rates[ex] = row["rate"]

    avg_rate = sum(rates.values()) / len(rates) if rates else 0
    threshold = threshold_pct / 100.0

    extreme = abs(avg_rate) >= threshold
    direction = None
    if extreme:
        direction = "long_squeeze" if avg_rate > 0 else "short_squeeze"

    if extreme:
        pct_str = f"{avg_rate * 100:+.4f}%"
        if direction == "long_squeeze":
            desc = f"⚡ Funding extreme {pct_str} — longs paying, short squeeze risk"
        else:
            desc = f"⚡ Funding extreme {pct_str} — shorts paying, long squeeze risk"
    else:
        desc = f"Funding normal ({avg_rate * 100:+.4f}%)"

    return {
        "extreme": extreme,
        "avg_rate": round(avg_rate, 8),
        "avg_rate_pct": round(avg_rate * 100, 6),
        "rates": {ex: round(r, 8) for ex, r in rates.items()},
        "direction": direction,
        "description": desc,
        "threshold_pct": threshold_pct,
    }


async def detect_cvd_momentum(window_seconds: int = 60, symbol: str = None) -> Dict:
    """
    CVD momentum: rate of change of CVD in last window.
    Returns: cvd_rate (CVD/s), direction, intensity (0-1), and acceleration.
    Useful for catching momentum shifts early.
    """
    since = time.time() - window_seconds
    trades = await get_trades_for_cvd(since, symbol=symbol)

    if len(trades) < 5:
        return {"cvd_rate": 0, "direction": "neutral", "intensity": 0, "acceleration": 0}

    # Split into early/late half for acceleration
    half = len(trades) // 2
    early_trades = trades[:half]
    late_trades  = trades[half:]

    def cvd_of(ts_list):
        c = 0.0
        for t in ts_list:
            c += t["price"] * t["qty"] if t["side"] in ("buy", "Buy") else -(t["price"] * t["qty"])
        return c

    total_vol_usd = sum(t["price"] * t["qty"] for t in trades)
    early_cvd = cvd_of(early_trades)
    late_cvd  = cvd_of(late_trades)
    total_cvd = early_cvd + late_cvd

    # Rate = CVD USD per second
    span = max(trades[-1]["ts"] - trades[0]["ts"], 1)
    cvd_rate = total_cvd / span

    # Intensity = abs(total_cvd) / total_vol_usd
    intensity = min(1.0, abs(total_cvd) / total_vol_usd) if total_vol_usd > 0 else 0

    # Acceleration: is late_cvd larger than early_cvd (momentum increasing)?
    acceleration = late_cvd - early_cvd
    accel_norm = acceleration / total_vol_usd if total_vol_usd > 0 else 0

    direction = "bullish" if total_cvd > 0 else "bearish" if total_cvd < 0 else "neutral"

    return {
        "cvd_rate": round(cvd_rate, 2),
        "cvd_total_usd": round(total_cvd, 2),
        "direction": direction,
        "intensity": round(intensity, 4),
        "acceleration": round(accel_norm, 4),
        "accelerating": abs(late_cvd) > abs(early_cvd),
        "window_seconds": window_seconds,
    }


async def detect_volume_spike(window_seconds: int = 30, baseline_seconds: int = 300, symbol: str = None) -> Dict:
    """
    Volume spike: compare recent window volume vs baseline average.
    Returns spike if recent/baseline ratio > 3x.
    """
    now = time.time()
    recent_trades = await get_trades_for_cvd(now - window_seconds, symbol=symbol)
    baseline_trades = await get_trades_for_cvd(now - baseline_seconds, symbol=symbol)

    recent_vol = sum(t["qty"] * t["price"] for t in recent_trades)
    # Baseline per-period average
    n_periods = baseline_seconds / window_seconds
    baseline_per_period = sum(t["qty"] * t["price"] for t in baseline_trades) / n_periods if n_periods > 0 else 0

    ratio = recent_vol / baseline_per_period if baseline_per_period > 0 else 0
    spike = ratio >= 3.0

    buy_vol = sum(t["qty"] * t["price"] for t in recent_trades if t["side"] in ("buy", "Buy"))
    sell_vol = sum(t["qty"] * t["price"] for t in recent_trades if t["side"] not in ("buy", "Buy"))
    dominant = "buy" if buy_vol >= sell_vol else "sell"
    dominant_pct = (max(buy_vol, sell_vol) / recent_vol * 100) if recent_vol > 0 else 0

    return {
        "spike": spike,
        "ratio": round(ratio, 2),
        "recent_usd": round(recent_vol, 2),
        "baseline_usd_per_period": round(baseline_per_period, 2),
        "dominant": dominant,
        "dominant_pct": round(dominant_pct, 1),
        "description": (
            f"🌊 Vol spike {ratio:.1f}x normal (${recent_vol:,.0f} in {window_seconds}s, {dominant}-dominant)"
            if spike else
            f"Volume normal ({ratio:.1f}x)"
        ),
        "window_seconds": window_seconds,
    }


def _phase_description(phase: str) -> str:
    return {
        "Accumulation": "Smart money buying quietly; price consolidating with rising OI",
        "Distribution": "Smart money selling into strength; OI rising but CVD diverging",
        "Markup": "Sustained uptrend with strong buying pressure and rising OI",
        "Markdown": "Sustained downtrend with selling pressure",
        "Unknown": "Insufficient data to classify market phase",
    }.get(phase, "")


async def detect_accumulation_distribution_pattern(symbol: str = None) -> Dict:
    """
    ML-style accumulation/distribution footprint detector.
    
    Accumulation signals:
    - OI rising + CVD positive (buyers adding longs)
    - Large buy trades clustering near lows
    - Low sell volume on dips (weak selling pressure)
    - Funding near zero or negative (shorts paying longs)
    
    Distribution signals:
    - OI rising + CVD negative (sellers adding shorts)  
    - Large sell trades clustering near highs
    - Low buy volume on rallies (weak buying interest)
    - Funding positive and rising (longs paying)
    
    Returns pattern type, confidence (0-1), and component signals.
    """
    now = time.time()
    since_5m  = now - 300
    since_15m = now - 900
    since_1h  = now - 3600

    # Gather inputs in parallel
    oi_5m, oi_15m, cvd_5m, cvd_15m, vol_imb_5m, vol_imb_15m, funding, ob = await asyncio.gather(
        compute_oi_momentum(window_seconds=300,  symbol=symbol),
        compute_oi_momentum(window_seconds=900,  symbol=symbol),
        compute_cvd(window_seconds=300,  symbol=symbol),
        compute_cvd(window_seconds=900,  symbol=symbol),
        compute_volume_imbalance(window_seconds=300, symbol=symbol),
        compute_volume_imbalance(window_seconds=900, symbol=symbol),
        get_funding_history(limit=4, symbol=symbol),
        get_latest_orderbook(symbol=symbol, limit=1),
    )

    # --- Signal extraction ---

    # 1. OI trend (positive = rising)
    oi_5m_pct  = oi_5m.get("avg_pct_change", 0)
    oi_15m_pct = oi_15m.get("avg_pct_change", 0)
    oi_rising  = oi_5m_pct > 0.5 or oi_15m_pct > 0.3

    # 2. CVD direction and end delta
    cvd_5m_end  = cvd_5m[-1]["cvd"]  if cvd_5m  else 0
    cvd_5m_start = cvd_5m[0]["cvd"] if cvd_5m  else 0
    cvd_15m_end  = cvd_15m[-1]["cvd"] if cvd_15m else 0
    cvd_15m_start = cvd_15m[0]["cvd"] if cvd_15m else 0
    cvd_5m_delta  = cvd_5m_end  - cvd_5m_start
    cvd_15m_delta = cvd_15m_end - cvd_15m_start
    cvd_positive = cvd_5m_delta > 0 and cvd_15m_delta > 0
    cvd_negative = cvd_5m_delta < 0 and cvd_15m_delta < 0

    # 3. Volume imbalance
    imb_5m  = vol_imb_5m.get("imbalance", 0)   # -1 to 1
    imb_15m = vol_imb_15m.get("imbalance", 0)
    buy_dominant  = imb_5m > 0.1 and imb_15m > 0.05
    sell_dominant = imb_5m < -0.1 and imb_15m < -0.05

    # 4. Funding rate analysis
    avg_funding = 0.0
    if funding:
        rates = [r["rate"] for r in funding]
        avg_funding = sum(rates) / len(rates)
    funding_negative = avg_funding < -0.01   # shorts paying
    funding_positive = avg_funding > 0.01    # longs paying
    funding_rising   = len(funding) >= 2 and funding[-1]["rate"] > funding[0]["rate"]

    # 5. OB imbalance (bid > ask = buy pressure at top of book)
    ob_imb = ob[0].get("imbalance", 0) if ob else 0
    ob_bid_heavy = ob_imb > 0.1
    ob_ask_heavy = ob_imb < -0.1

    # --- Pattern scoring ---
    accum_score = 0.0
    distrib_score = 0.0
    signals = {}

    # Accumulation: OI rising + CVD buying + buy dominant + funding low/negative
    if oi_rising:
        accum_score += 0.2 if oi_5m_pct > 0 else 0.1
        distrib_score += 0.15  # OI rising is shared signal
        signals["oi_rising"] = True
    
    if cvd_positive:
        accum_score += 0.25
        signals["cvd_buying"] = True
    elif cvd_negative:
        distrib_score += 0.25
        signals["cvd_selling"] = True

    if buy_dominant:
        accum_score += 0.2
        signals["buy_volume_dominant"] = True
    elif sell_dominant:
        distrib_score += 0.2
        signals["sell_volume_dominant"] = True

    if funding_negative:
        accum_score += 0.15  # smart money long while shorts pay
        signals["funding_negative"] = True
    elif funding_positive and funding_rising:
        distrib_score += 0.15  # longs overextended
        signals["funding_positive_rising"] = True

    if ob_bid_heavy:
        accum_score += 0.1
        signals["ob_bid_wall"] = True
    elif ob_ask_heavy:
        distrib_score += 0.1
        signals["ob_ask_wall"] = True

    # Add raw values for context
    signals["oi_5m_pct"]   = round(oi_5m_pct, 4)
    signals["oi_15m_pct"]  = round(oi_15m_pct, 4)
    signals["cvd_5m_delta"]  = round(cvd_5m_delta, 4)
    signals["cvd_15m_delta"] = round(cvd_15m_delta, 4)
    signals["vol_imb_5m"]  = round(imb_5m, 4)
    signals["vol_imb_15m"] = round(imb_15m, 4)
    signals["avg_funding"]  = round(avg_funding, 6)
    signals["ob_imbalance"] = round(ob_imb, 4)

    # Clamp scores
    accum_score  = min(1.0, accum_score)
    distrib_score = min(1.0, distrib_score)

    # Determine pattern
    THRESHOLD = 0.35
    if accum_score >= distrib_score and accum_score >= THRESHOLD:
        pattern = "accumulation"
        confidence = round(accum_score, 3)
        description = (
            f"Accumulation footprint (conf={confidence:.0%}): "
            f"OI+{oi_5m_pct:+.2f}%, CVD Δ{cvd_5m_delta:+.2f}, "
            f"imb={imb_5m:+.2f}, funding={avg_funding:.4f}%"
        )
    elif distrib_score > accum_score and distrib_score >= THRESHOLD:
        pattern = "distribution"
        confidence = round(distrib_score, 3)
        description = (
            f"Distribution footprint (conf={confidence:.0%}): "
            f"OI+{oi_5m_pct:+.2f}%, CVD Δ{cvd_5m_delta:+.2f}, "
            f"imb={imb_5m:+.2f}, funding={avg_funding:.4f}%"
        )
    else:
        # Below threshold — noise/balanced
        pattern = "balanced"
        confidence = round(max(accum_score, distrib_score), 3)
        description = f"No clear accumulation/distribution (max_conf={confidence:.0%})"

    return {
        "pattern":      pattern,
        "confidence":   confidence,
        "accum_score":  round(accum_score, 3),
        "distrib_score": round(distrib_score, 3),
        "description":  description,
        "signals":      signals,
        "symbol":       symbol,
        "ts":           now,
    }


async def compute_market_regime(symbol: str = None) -> Dict:
    """
    Composite market regime score combining all signals.
    Returns a score from -100 (extreme bear) to +100 (extreme bull)
    with a confidence level and actionable summary.
    """
    syms = [symbol] if symbol else None

    # Gather all signals
    phase_data = await classify_market_phase(symbol=symbol)
    cvd_mom    = await detect_cvd_momentum(window_seconds=60, symbol=symbol)
    cvd_mom5   = await detect_cvd_momentum(window_seconds=300, symbol=symbol)
    vol_imb    = await compute_volume_imbalance(window_seconds=60, symbol=symbol)
    oi_mom     = await compute_oi_momentum(window_seconds=300, symbol=symbol)
    delta_div  = await detect_delta_divergence(window_seconds=300, symbol=symbol)

    score = 0
    weights = {}

    # Phase: ±30
    phase = phase_data.get("phase", "Unknown")
    phase_conf = phase_data.get("confidence", 0.5)
    phase_map = {"Accumulation": 20, "Markup": 30, "Bull Trend": 30,
                 "Distribution": -20, "Markdown": -30, "Bear Trend": -30, "Balanced": 0, "Unknown": 0}
    phase_score = phase_map.get(phase, 0) * phase_conf
    score += phase_score
    weights["phase"] = round(phase_score, 1)

    # CVD momentum 1min: ±20
    cvd_dir = 1 if cvd_mom.get("direction") == "bullish" else -1 if cvd_mom.get("direction") == "bearish" else 0
    cvd_score = cvd_dir * cvd_mom.get("intensity", 0) * 20
    if cvd_mom.get("accelerating"):
        cvd_score *= 1.3
    score += cvd_score
    weights["cvd_1m"] = round(cvd_score, 1)

    # CVD momentum 5min: ±15
    cvd5_dir = 1 if cvd_mom5.get("direction") == "bullish" else -1 if cvd_mom5.get("direction") == "bearish" else 0
    cvd5_score = cvd5_dir * cvd_mom5.get("intensity", 0) * 15
    score += cvd5_score
    weights["cvd_5m"] = round(cvd5_score, 1)

    # Volume imbalance: ±15
    imb = vol_imb.get("imbalance", 0)  # -1 to 1
    imb_score = imb * 15
    score += imb_score
    weights["vol_imb"] = round(imb_score, 1)

    # OI momentum: ±10 (OI rising = more conviction)
    oi_pct = 0
    for ex_data in oi_mom.get("exchanges", {}).values():
        oi_pct += ex_data.get("pct_change", 0)
    oi_pct = max(-5, min(5, oi_pct))
    oi_score = oi_pct * 2  # ±10
    score += oi_score
    weights["oi"] = round(oi_score, 1)

    # Delta divergence: −10 (divergence = warning, direction-adjusted)
    if delta_div.get("divergence"):
        sev = delta_div.get("severity", 1)
        div_score = -sev * 5 * (-1 if delta_div.get("cvd_direction") == "bullish" else 1)
        score += div_score
        weights["divergence"] = round(div_score, 1)

    # Clamp
    score = max(-100, min(100, score))

    # Regime label
    if score >= 60:    regime = "Strong Bull"
    elif score >= 30:  regime = "Bull"
    elif score >= 10:  regime = "Mild Bull"
    elif score > -10:  regime = "Neutral"
    elif score > -30:  regime = "Mild Bear"
    elif score > -60:  regime = "Bear"
    else:              regime = "Strong Bear"

    # Action hint
    if score >= 30:    action = "Long bias"
    elif score >= 10:  action = "Cautious long"
    elif score > -10:  action = "Wait / range trade"
    elif score > -30:  action = "Cautious short"
    else:              action = "Short bias"

    return {
        "score": round(score, 1),
        "regime": regime,
        "action": action,
        "phase": phase,
        "phase_confidence": phase_conf,
        "weights": weights,
        "symbol": symbol,
    }


async def detect_cross_symbol_oi_spike(
    symbols: List[str],
    window_seconds: int = 300,
    threshold_pct: float = 2.5,
    min_correlated: int = 2,
) -> Dict:
    """
    Inter-symbol correlation alert: detect when multiple symbols spike OI simultaneously.
    If >= min_correlated symbols show OI spike within the same window → fire alert.
    """
    since = time.time() - window_seconds

    spikes = {}
    for sym in symbols:
        oi_data = await get_oi_history(limit=200, since=since, symbol=sym)
        if len(oi_data) < 2:
            spikes[sym] = {"spike": False, "pct_change": 0.0, "reason": "insufficient data"}
            continue

        # group by exchange, take first exchange we find with data
        by_exchange = {}
        for row in oi_data:
            ex = row["exchange"]
            by_exchange.setdefault(ex, []).append(row)

        best_pct = 0.0
        best_ex = None
        for ex, rows in by_exchange.items():
            if len(rows) < 2:
                continue
            oi_start = rows[0]["oi_value"]
            oi_end = rows[-1]["oi_value"]
            if oi_start == 0:
                continue
            pct = (oi_end - oi_start) / oi_start * 100
            if abs(pct) > abs(best_pct):
                best_pct = pct
                best_ex = ex

        is_spike = abs(best_pct) >= threshold_pct
        spikes[sym] = {
            "spike": is_spike,
            "pct_change": round(best_pct, 4),
            "exchange": best_ex,
            "direction": "up" if best_pct > 0 else "down",
        }

    spiking_syms = [s for s, v in spikes.items() if v["spike"]]
    correlated = len(spiking_syms) >= min_correlated

    # Check directional agreement (most go same way = stronger signal)
    if correlated:
        directions = [spikes[s]["direction"] for s in spiking_syms]
        dominant = max(set(directions), key=directions.count)
        agree_pct = directions.count(dominant) / len(directions) * 100
    else:
        dominant = None
        agree_pct = 0.0

    description = ""
    if correlated:
        parts = [f"{s} OI {spikes[s]['direction']} {abs(spikes[s]['pct_change']):.2f}%" for s in spiking_syms]
        description = f"Correlated OI spike: {', '.join(parts)} | direction agreement {agree_pct:.0f}%"
    else:
        description = f"No correlated OI spike (only {len(spiking_syms)}/{len(symbols)} symbols spiking)"

    return {
        "correlated": correlated,
        "spiking_symbols": spiking_syms,
        "dominant_direction": dominant,
        "direction_agreement_pct": round(agree_pct, 1),
        "all_symbols": spikes,
        "description": description,
        "threshold_pct": threshold_pct,
        "window_seconds": window_seconds,
    }


async def detect_funding_arbitrage(
    symbol: str = None,
    threshold_bps: float = 5.0,
) -> Dict:
    """
    Funding arbitrage signal: compare Binance vs Bybit funding rates.
    If |binance_rate - bybit_rate| >= threshold_bps (basis points), flag as arbitrage opportunity.
    threshold_bps: divergence threshold in basis points (1 bp = 0.0001%)
    """
    funding = await get_funding_history(limit=4, symbol=symbol)
    if not funding:
        return {
            "arb": False,
            "binance": None,
            "bybit": None,
            "divergence_bps": 0.0,
            "description": "No funding data",
            "signal": None,
        }

    rates = {}
    for row in funding:
        ex = row["exchange"].lower()
        if ex not in rates:
            rates[ex] = row["rate"]

    binance = rates.get("binance")
    bybit = rates.get("bybit")

    if binance is None or bybit is None:
        available = list(rates.keys())
        return {
            "arb": False,
            "binance": binance,
            "bybit": bybit,
            "divergence_bps": 0.0,
            "description": f"Only have data for: {available}",
            "signal": None,
        }

    # divergence in basis points (1 bps = 0.01% = 0.0001)
    div_raw = binance - bybit  # positive = Binance higher
    div_bps = div_raw * 10000  # convert to bps

    arb = abs(div_bps) >= threshold_bps

    signal = None
    description = ""
    if arb:
        if div_bps > 0:
            signal = "binance_high"
            description = (
                f"⚡ Funding arb: Binance {binance*100:+.4f}% >> Bybit {bybit*100:+.4f}% "
                f"(+{div_bps:.2f} bps) — longs costly on Binance"
            )
        else:
            signal = "bybit_high"
            description = (
                f"⚡ Funding arb: Bybit {bybit*100:+.4f}% >> Binance {binance*100:+.4f}% "
                f"({div_bps:.2f} bps) — longs costly on Bybit"
            )
    else:
        description = (
            f"Funding aligned: Binance {binance*100:+.4f}% / Bybit {bybit*100:+.4f}% "
            f"(div {div_bps:+.2f} bps)"
        )

    return {
        "arb": arb,
        "binance": round(binance, 8),
        "bybit": round(bybit, 8),
        "binance_pct": round(binance * 100, 6),
        "bybit_pct": round(bybit * 100, 6),
        "divergence_bps": round(div_bps, 4),
        "threshold_bps": threshold_bps,
        "signal": signal,
        "description": description,
    }


async def compute_vwap_deviation(window_seconds: int = 3600, symbol: str = None) -> Dict:
    """
    Compute VWAP for the given window and return deviation of current price from VWAP.
    VWAP deviation = (price - vwap) / vwap * 100 (%)
    Also classifies signal: above/below, strength (weak/moderate/strong).
    """
    since = time.time() - window_seconds
    trades = await get_recent_trades(since=since, symbol=symbol)

    if not trades:
        return {
            "vwap": None,
            "price": None,
            "deviation_pct": None,
            "signal": "no_data",
            "strength": None,
            "description": "No trade data",
        }

    # Compute VWAP: sum(price * qty) / sum(qty)
    cum_pv = 0.0
    cum_v = 0.0
    latest_price = None
    for t in trades:
        p = float(t.get("price", 0) or 0)
        q = float(t.get("qty", 0) or 0)
        if p > 0 and q > 0:
            cum_pv += p * q
            cum_v += q
            latest_price = p

    if cum_v == 0 or latest_price is None:
        return {
            "vwap": None,
            "price": None,
            "deviation_pct": None,
            "signal": "insufficient_data",
            "strength": None,
            "description": "Insufficient trade data",
        }

    vwap = cum_pv / cum_v
    deviation_pct = (latest_price - vwap) / vwap * 100

    # Classify
    abs_dev = abs(deviation_pct)
    if abs_dev < 0.1:
        strength = "flat"
    elif abs_dev < 0.3:
        strength = "weak"
    elif abs_dev < 0.8:
        strength = "moderate"
    else:
        strength = "strong"

    signal = "above_vwap" if deviation_pct > 0 else "below_vwap"

    if deviation_pct > 0:
        desc = f"Price {deviation_pct:+.3f}% above VWAP ({vwap:.6f}) — {strength}"
    else:
        desc = f"Price {deviation_pct:+.3f}% below VWAP ({vwap:.6f}) — {strength}"

    return {
        "vwap": round(vwap, 8),
        "price": round(latest_price, 8),
        "deviation_pct": round(deviation_pct, 4),
        "signal": signal,
        "strength": strength,
        "description": desc,
        "window_seconds": window_seconds,
        "trade_count": len(trades),
    }
