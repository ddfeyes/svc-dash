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
