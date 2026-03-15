"""Computed metrics: CVD, volume imbalance, OI momentum, phase classifier — multi-symbol."""
import asyncio
import json
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


# ── CVD helpers ───────────────────────────────────────────────────────────────

def _cvd_delta(trade: dict, use_usd: bool = False) -> float:
    """Signed volume delta for a single trade.

    Uses ``is_buyer_aggressor`` (authoritative taker-side field) with graceful
    fallback to ``side`` string for legacy rows that predate the field.

    Args:
        trade:    dict with at minimum ``qty`` and optionally ``is_buyer_aggressor``,
                  ``side``, ``price``.
        use_usd:  if True return ``price * qty`` (USD notional); otherwise ``qty``
                  (base currency, default for CVD charts).

    Returns:
        +vol if buyer initiated, -vol if seller initiated, 0 if unknown.
    """
    is_buyer_agg = trade.get("is_buyer_aggressor")

    if is_buyer_agg is not None:
        buyer_initiated = bool(is_buyer_agg)
    else:
        side = (trade.get("side") or "").lower()
        if side == "buy":
            buyer_initiated = True
        elif side == "sell":
            buyer_initiated = False
        else:
            return 0.0  # unknown — do not corrupt CVD with noise

    vol = float(trade.get("price", 0)) * float(trade["qty"]) if use_usd else float(trade["qty"])
    return vol if buyer_initiated else -vol


def compute_cvd_from_trades(trades: List[dict]) -> List[dict]:
    """Pure CVD accumulation over an ordered list of trade dicts.

    Extracted from compute_cvd() so it can be unit-tested without a DB.
    Returns list of {ts, price, cvd, delta}.
    """
    cvd = 0.0
    result = []
    for t in trades:
        delta = _cvd_delta(t)
        cvd += delta
        result.append({
            "ts": t["ts"],
            "price": t["price"],
            "cvd": round(cvd, 6),
            "delta": round(delta, 6),
        })
    return result


async def compute_cvd(window_seconds: int = 3600, symbol: str = None) -> List[Dict]:
    """Cumulative Volume Delta over the last window."""
    since = time.time() - window_seconds
    trades = await get_trades_for_cvd(since, symbol=symbol)

    result = compute_cvd_from_trades(trades)

    # Downsample to ~300 points for frontend
    if len(result) > 300:
        step = len(result) // 300
        result = result[::step]

    return result


def compute_depth_ratio(snapshot: dict, levels: int = 5) -> Optional[float]:
    """Compute bid/ask depth ratio from the top N price levels of an orderbook snapshot.

    Args:
        snapshot: dict with ``bids`` and ``asks`` (JSON string or list of [price, qty]).
        levels:   number of best price levels to sum (default 5).

    Returns:
        bid_depth / ask_depth, or None if data is invalid / ask depth is zero.
    """
    try:
        raw_bids = snapshot.get("bids")
        raw_asks = snapshot.get("asks")
        if raw_bids is None or raw_asks is None:
            return None

        bids = json.loads(raw_bids) if isinstance(raw_bids, str) else raw_bids
        asks = json.loads(raw_asks) if isinstance(raw_asks, str) else raw_asks
    except (ValueError, TypeError):
        return None

    try:
        bid_depth = sum(float(b[1]) for b in bids[:levels])
        ask_depth = sum(float(a[1]) for a in asks[:levels])
    except (IndexError, TypeError, ValueError):
        return None

    if ask_depth == 0:
        return None if bid_depth > 0 else 0.0
    return round(bid_depth / ask_depth, 6)


def compute_depth_ratio_series(snapshots: List[dict], levels: int = 5) -> List[dict]:
    """Map compute_depth_ratio over an ordered list of orderbook snapshots.

    Snapshots with None ratio (empty asks, malformed data) are excluded.
    Returns list of {ts, ratio} sorted by ts ascending.
    """
    result = []
    for snap in sorted(snapshots, key=lambda s: s["ts"]):
        ratio = compute_depth_ratio(snap, levels=levels)
        if ratio is not None:
            result.append({"ts": snap["ts"], "ratio": ratio})
    return result


def compute_liq_heatmap(
    liqs: List[dict],
    time_bucket: int = 300,
    price_bins: int = 20,
) -> dict:
    """Aggregate liquidations into a 2D heatmap grid (pure function, testable).

    Args:
        liqs:        list of liquidation dicts with ts, price, qty, side, value
        time_bucket: seconds per time column (default 5 min)
        price_bins:  number of price rows

    Returns:
        {
          "cells": [{ts_bucket, price_bucket, price_mid, total_usd, long_usd, short_usd, count}],
          "price_min", "price_max", "price_step", "time_bucket"
        }

    Side semantics: side="sell" = long liquidated; side="buy" = short liquidated.
    """
    if not liqs:
        return {"cells": [], "price_min": 0.0, "price_max": 0.0,
                "price_step": 0.0, "time_bucket": time_bucket}

    prices = [float(l["price"]) for l in liqs]
    price_min = min(prices)
    price_max = max(prices)

    # Guard against zero-range (all same price): expand slightly
    if price_max == price_min:
        price_min *= 0.999
        price_max *= 1.001
    price_step = (price_max - price_min) / price_bins

    # Aggregate into cells keyed by (ts_bucket, price_bucket_index)
    cells: dict = {}
    for l in liqs:
        ts_b = int(l["ts"] // time_bucket) * time_bucket
        p_idx = int((float(l["price"]) - price_min) / price_step)
        p_idx = max(0, min(price_bins - 1, p_idx))  # clamp to [0, price_bins-1]

        key = (ts_b, p_idx)
        if key not in cells:
            cells[key] = {"ts_bucket": ts_b, "price_bucket": p_idx,
                          "price_mid": price_min + (p_idx + 0.5) * price_step,
                          "total_usd": 0.0, "long_usd": 0.0, "short_usd": 0.0, "count": 0}

        usd = float(l.get("value") or float(l["price"]) * float(l["qty"]))
        side = (l.get("side") or "").lower()

        cells[key]["total_usd"] += usd
        cells[key]["count"] += 1
        if side == "sell":      # sell liquidation = long position forced closed
            cells[key]["long_usd"] += usd
        elif side == "buy":     # buy liquidation = short position forced closed
            cells[key]["short_usd"] += usd

    sorted_cells = sorted(cells.values(), key=lambda c: (c["ts_bucket"], c["price_bucket"]))
    return {
        "cells": sorted_cells,
        "price_min": round(price_min, 8),
        "price_max": round(price_max, 8),
        "price_step": round(price_step, 8),
        "time_bucket": time_bucket,
    }


async def compute_volume_imbalance(window_seconds: int = 60, symbol: str = None) -> Dict:
    """Buy vs sell volume ratio over window."""
    since = time.time() - window_seconds
    trades = await get_trades_for_cvd(since, symbol=symbol)

    buy_vol = sum(t["qty"] for t in trades if _cvd_delta(t) > 0)
    sell_vol = sum(t["qty"] for t in trades if _cvd_delta(t) < 0)
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
        return sum(_cvd_delta(t) for t in ts_list)

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
        return sum(_cvd_delta(t, use_usd=True) for t in ts_list)

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


# CoinGecko symbol → coin id mapping (extend as needed)
_COINGECKO_IDS = {
    "BANANAS31USDT": "banana",   # likely id; fallback graceful
    "COSUSDT": "contentos",
    "DEXEUSDT": "dexe",
    "LYNUSDT": "lynex",
}

async def fetch_oi_mcap_ratio(symbol: str = None) -> Dict:
    """
    Fetch open interest (from DB) and market cap (from CoinGecko free API).
    Returns OI/Mcap ratio as a signal.
    High OI/Mcap (>15-20%) = elevated leverage risk.
    """
    import httpx

    # Get latest OI from DB
    oi_data = await get_oi_history(limit=2, symbol=symbol)
    if not oi_data:
        return {"error": "No OI data", "ratio_pct": None}

    # Take the most recent OI value
    latest_oi_row = oi_data[0] if oi_data else None
    if not latest_oi_row:
        return {"error": "No OI row", "ratio_pct": None}

    oi_contracts = latest_oi_row.get("oi_contracts") or latest_oi_row.get("oi_value", 0)

    # Get price to compute OI in USD
    price_data = await get_oi_history(limit=2, symbol=symbol)
    # We'll get price from latest orderbook
    ob = await get_latest_orderbook(symbol=symbol, limit=1)
    price = None
    if ob and isinstance(ob, list) and ob[0]:
        price = ob[0].get("mid_price") or ob[0].get("best_bid")

    oi_usd = (oi_contracts * price) if (oi_contracts and price) else None

    # Fetch market cap from CoinGecko (free, no key)
    coin_id = None
    if symbol:
        for k, v in _COINGECKO_IDS.items():
            if k.upper() == symbol.upper():
                coin_id = v
                break

    mcap = None
    mcap_error = None
    if coin_id:
        try:
            async with httpx.AsyncClient(timeout=5.0) as client:
                r = await client.get(
                    f"https://api.coingecko.com/api/v3/coins/{coin_id}",
                    params={"localization": "false", "tickers": "false",
                            "market_data": "true", "community_data": "false",
                            "developer_data": "false"},
                    headers={"Accept": "application/json"}
                )
                if r.status_code == 200:
                    data = r.json()
                    mcap = data.get("market_data", {}).get("market_cap", {}).get("usd")
        except Exception as e:
            mcap_error = str(e)

    ratio_pct = None
    signal = None
    description = ""
    if oi_usd and mcap and mcap > 0:
        ratio_pct = (oi_usd / mcap) * 100
        if ratio_pct < 5:
            signal = "low"
            description = f"OI/Mcap {ratio_pct:.2f}% — low leverage"
        elif ratio_pct < 15:
            signal = "moderate"
            description = f"OI/Mcap {ratio_pct:.2f}% — moderate leverage"
        elif ratio_pct < 30:
            signal = "high"
            description = f"⚠️ OI/Mcap {ratio_pct:.2f}% — elevated leverage risk"
        else:
            signal = "extreme"
            description = f"🚨 OI/Mcap {ratio_pct:.2f}% — extreme leverage, squeeze risk"

    return {
        "oi_contracts": round(oi_contracts, 2) if oi_contracts else None,
        "oi_usd": round(oi_usd, 2) if oi_usd else None,
        "price": round(price, 8) if price else None,
        "mcap_usd": mcap,
        "coin_id": coin_id,
        "ratio_pct": round(ratio_pct, 4) if ratio_pct is not None else None,
        "signal": signal,
        "description": description,
        "mcap_error": mcap_error,
    }


async def predict_liquidation_cascade(
    symbol: str = None,
    oi_window: int = 120,
    oi_threshold_pct: float = 2.0,
    sr_proximity_pct: float = 0.5,
) -> Dict:
    """
    Pre-cascade warning: OI rising fast AND price approaching key S/R level.
    Logic:
    1. OI change > oi_threshold_pct in last oi_window seconds → high leverage buildup
    2. Price within sr_proximity_pct% of a key support/resistance level → approaching trigger
    3. If both conditions met → cascade risk HIGH
    Also considers: current liquidation activity + funding direction
    """
    import asyncio as _asyncio

    # Gather: OI momentum, latest OI, latest price, liquidations
    oi_mom_task = compute_oi_momentum(window_seconds=oi_window, symbol=symbol)
    liq_task    = detect_liquidation_cascade(window_seconds=60, symbol=symbol)
    funding_task = get_funding_history(limit=2, symbol=symbol)
    ob_task     = get_latest_orderbook(symbol=symbol, limit=1)

    oi_mom, recent_liq, funding, ob = await _asyncio.gather(
        oi_mom_task, liq_task, funding_task, ob_task,
        return_exceptions=True
    )

    # Current price
    price = None
    if isinstance(ob, list) and ob:
        price = ob[0].get("mid_price") or ob[0].get("best_bid")

    # OI momentum
    oi_pct = 0.0
    oi_direction = None
    if isinstance(oi_mom, dict):
        oi_pct = abs(oi_mom.get("pct_change", 0) or 0)
        oi_direction = oi_mom.get("direction")

    oi_building = oi_pct >= oi_threshold_pct

    # Funding direction (positive = longs paying, squeeze risk for longs)
    funding_direction = None
    avg_funding = 0.0
    if isinstance(funding, list) and funding:
        rates = [r["rate"] for r in funding if r.get("rate") is not None]
        avg_funding = sum(rates) / len(rates) if rates else 0
        funding_direction = "long_pay" if avg_funding > 0 else "short_pay"

    # Price proximity to S/R — compute simple S/R from OI history + price
    # Use price levels from recent OI + orderbook walls as proxy S/R
    near_key_level = False
    closest_level = None
    closest_dist_pct = None

    if price and isinstance(ob, list) and ob:
        raw_bids = []
        raw_asks = []
        try:
            import json as _json
            bids_raw = ob[0].get("bids")
            asks_raw = ob[0].get("asks")
            if isinstance(bids_raw, str):
                raw_bids = _json.loads(bids_raw)
            elif isinstance(bids_raw, list):
                raw_bids = bids_raw
            if isinstance(asks_raw, str):
                raw_asks = _json.loads(asks_raw)
            elif isinstance(asks_raw, list):
                raw_asks = raw_asks
        except Exception:
            pass

        # Find large bid/ask walls (> 3x average size) as key levels
        if raw_bids and raw_asks:
            all_levels = []
            bid_qtys = [float(q) for _, q in raw_bids[:20]]
            ask_qtys = [float(q) for _, q in raw_asks[:20]]
            avg_bid = sum(bid_qtys) / len(bid_qtys) if bid_qtys else 0
            avg_ask = sum(ask_qtys) / len(ask_qtys) if ask_qtys else 0

            for p, q in raw_bids[:20]:
                if float(q) > avg_bid * 2.5:
                    all_levels.append(float(p))
            for p, q in raw_asks[:20]:
                if float(q) > avg_ask * 2.5:
                    all_levels.append(float(p))

            if all_levels and price:
                dists = [(abs(lv - price) / price * 100, lv) for lv in all_levels]
                dists.sort()
                if dists:
                    closest_dist_pct, closest_level = dists[0]
                    near_key_level = closest_dist_pct <= sr_proximity_pct

    # Already cascading?
    already_cascading = isinstance(recent_liq, dict) and recent_liq.get("cascade")

    # Composite risk
    risk_factors = []
    if oi_building:
        risk_factors.append(f"OI +{oi_pct:.1f}% ({oi_direction or '?'})")
    if near_key_level:
        risk_factors.append(f"price {closest_dist_pct:.3f}% from wall @ {closest_level:.7f}")
    if already_cascading:
        risk_factors.append("active cascade")
    if abs(avg_funding) > 0.001:
        risk_factors.append(f"funding {avg_funding*100:+.4f}%")

    high_risk = oi_building and near_key_level
    if already_cascading:
        level = "cascading"
        description = f"🚨 CASCADE ACTIVE: {'; '.join(risk_factors)}"
    elif high_risk:
        level = "high"
        description = f"⚠️ Cascade risk HIGH: {'; '.join(risk_factors)}"
    elif oi_building:
        level = "building"
        description = f"🟡 OI building ({oi_pct:.1f}%), not yet near key level"
    elif near_key_level:
        level = "watch"
        description = f"👁 Near key level ({closest_dist_pct:.3f}%), OI stable"
    else:
        level = "low"
        description = "Cascade risk low — OI stable, price not near walls"

    return {
        "level": level,
        "high_risk": high_risk or already_cascading,
        "oi_building": oi_building,
        "oi_pct_change": round(oi_pct, 4),
        "oi_direction": oi_direction,
        "near_key_level": near_key_level,
        "closest_level": round(closest_level, 8) if closest_level else None,
        "closest_dist_pct": round(closest_dist_pct, 4) if closest_dist_pct is not None else None,
        "avg_funding": round(avg_funding, 8),
        "funding_direction": funding_direction,
        "already_cascading": already_cascading,
        "description": description,
        "risk_factors": risk_factors,
    }


async def compute_max_drawdown(window_seconds: int = 3600, symbol: str = None) -> Dict:
    """
    Compute peak-to-trough max drawdown and max run-up over last `window_seconds`.
    Returns per-symbol dict with fields expected by the frontend.
    """
    import storage
    since = time.time() - window_seconds
    trades = await storage.get_recent_trades(since=since, symbol=symbol, limit=10000)

    symbols_data: Dict[str, List] = {}
    for t in trades:
        sym = t["symbol"]
        if sym not in symbols_data:
            symbols_data[sym] = []
        symbols_data[sym].append((t["ts"], float(t["price"])))

    results = {}
    for sym, pts in symbols_data.items():
        if not pts:
            results[sym] = {
                "max_drawdown_pct": 0.0,
                "max_runup_pct": 0.0,
                "current_dd_pct": 0.0,
                "peak_price": None,
                "trough_price": None,
                "recent_peak": None,
                "current_price": None,
                "samples": 0,
            }
            continue
        pts.sort(key=lambda x: x[0])
        prices = [p for _, p in pts]

        # Max drawdown: peak-to-trough
        peak = prices[0]
        max_dd = 0.0
        peak_price = prices[0]
        trough_price = prices[0]

        for price in prices[1:]:
            if price > peak:
                peak = price
            dd = (peak - price) / peak * 100 if peak > 0 else 0.0
            if dd > max_dd:
                max_dd = dd
                peak_price = peak
                trough_price = price

        # Max run-up: trough-to-peak (forward)
        trough = prices[0]
        max_ru = 0.0
        for price in prices[1:]:
            if price < trough:
                trough = price
            ru = (price - trough) / trough * 100 if trough > 0 else 0.0
            if ru > max_ru:
                max_ru = ru

        # Current drawdown from recent peak (last 10% of window)
        recent_slice = prices[max(0, len(prices) - max(10, len(prices) // 10)):]
        recent_peak = max(recent_slice) if recent_slice else prices[-1]
        current_price = prices[-1]
        current_dd = (recent_peak - current_price) / recent_peak * 100 if recent_peak > 0 else 0.0

        results[sym] = {
            "max_drawdown_pct": -round(max_dd, 4),  # negative = drawdown
            "max_runup_pct": round(max_ru, 4),
            "current_dd_pct": -round(current_dd, 4),
            "peak_price": round(peak_price, 8),
            "trough_price": round(trough_price, 8),
            "recent_peak": round(recent_peak, 8),
            "current_price": round(current_price, 8),
            "window_seconds": window_seconds,
            "samples": len(prices),
        }

    return results


async def detect_funding_divergence(focus_symbol: str = "BANANAS31USDT", divergence_multiplier: float = 2.0) -> Dict:
    """
    Funding rate divergence alert: when focus_symbol's funding rate diverges
    >divergence_multiplier x from the average of the other symbols.
    Returns alert details + per-symbol rates.
    """
    from collectors import get_symbols
    all_syms = get_symbols()
    if focus_symbol not in all_syms:
        all_syms = [focus_symbol] + all_syms

    # Gather latest funding rate per symbol
    sym_rates = {}
    for sym in all_syms:
        rows = await get_funding_history(limit=2, symbol=sym)
        if rows:
            # Use first row (most recent)
            sym_rates[sym] = rows[0]["rate"]

    if len(sym_rates) < 2:
        return {
            "divergence": False,
            "focus": focus_symbol,
            "focus_rate": sym_rates.get(focus_symbol),
            "peer_avg": None,
            "ratio": None,
            "description": "Insufficient data",
            "severity": "info",
        }

    focus_rate = sym_rates.get(focus_symbol)
    if focus_rate is None:
        return {
            "divergence": False,
            "focus": focus_symbol,
            "focus_rate": None,
            "peer_avg": None,
            "ratio": None,
            "description": f"No data for {focus_symbol}",
            "severity": "info",
        }

    peers = {s: r for s, r in sym_rates.items() if s != focus_symbol}
    if not peers:
        return {
            "divergence": False,
            "focus": focus_symbol,
            "focus_rate": round(focus_rate * 100, 6),
            "peer_avg": None,
            "ratio": None,
            "description": "No peer symbols",
            "severity": "info",
        }

    peer_avg = sum(peers.values()) / len(peers)
    focus_pct = focus_rate * 100
    peer_avg_pct = peer_avg * 100

    # Compute divergence ratio: how many times larger in absolute terms
    if abs(peer_avg) < 1e-9:
        # Peer avg near zero — check if focus is significant
        ratio = abs(focus_rate) / 0.0001 if abs(focus_rate) > 1e-9 else 0.0
    else:
        ratio = abs(focus_rate) / abs(peer_avg)

    diverged = ratio >= divergence_multiplier
    same_sign = (focus_rate >= 0) == (peer_avg >= 0)

    if diverged:
        direction_note = "same direction" if same_sign else "OPPOSITE direction"
        severity = "high" if ratio >= 3.0 else "medium"
        desc = (
            f"🚨 Funding divergence: {focus_symbol} at {focus_pct:+.4f}% vs peer avg {peer_avg_pct:+.4f}% "
            f"(ratio {ratio:.1f}x, {direction_note})"
        )
    else:
        severity = "info"
        desc = (
            f"Funding normal: {focus_symbol} at {focus_pct:+.4f}% vs peer avg {peer_avg_pct:+.4f}% "
            f"(ratio {ratio:.1f}x)"
        )

    return {
        "divergence": diverged,
        "focus": focus_symbol,
        "focus_rate_pct": round(focus_pct, 6),
        "peer_avg_pct": round(peer_avg_pct, 6),
        "ratio": round(ratio, 2),
        "same_sign": same_sign,
        "severity": severity,
        "description": desc,
        "rates": {s: round(r * 100, 6) for s, r in sym_rates.items()},
        "threshold_multiplier": divergence_multiplier,
    }


async def compute_oi_concentration(symbol: str = None, window_seconds: int = 3600, n_buckets: int = 10) -> Dict:
    """
    OI concentration metric: % of total OI change in the densest price range bucket.
    
    Method:
    1. Get OI history + trade prices over window
    2. Compute price range (min, max) over window
    3. Divide range into n_buckets equal buckets
    4. For each OI sample, assign it to the price bucket closest to that timestamp
    5. Sum |OI delta| per bucket, find the bucket with highest concentration
    6. Return concentration% = top_bucket / total * 100
    """
    import time
    from storage import get_oi_history, get_recent_trades
    now = time.time()
    since = now - window_seconds

    oi_rows, trade_rows = await asyncio.gather(
        get_oi_history(limit=2000, since=since, symbol=symbol),
        get_recent_trades(limit=5000, since=since, symbol=symbol),
    )

    if not oi_rows or len(oi_rows) < 2:
        return {
            "concentration_pct": None,
            "top_bucket_range": None,
            "n_buckets": n_buckets,
            "description": "Insufficient OI data",
            "window_seconds": window_seconds,
        }

    if not trade_rows:
        return {
            "concentration_pct": None,
            "top_bucket_range": None,
            "n_buckets": n_buckets,
            "description": "No trade price data",
            "window_seconds": window_seconds,
        }

    # Build price timeline
    trade_rows.sort(key=lambda x: x["ts"])
    prices_ts = [(r["ts"], r["price"]) for r in trade_rows if r.get("price")]
    if not prices_ts:
        return {"concentration_pct": None, "top_bucket_range": None, "n_buckets": n_buckets,
                "description": "No price data", "window_seconds": window_seconds}

    all_prices = [p for _, p in prices_ts]
    price_min = min(all_prices)
    price_max = max(all_prices)
    price_range = price_max - price_min

    if price_range < 1e-10:
        return {
            "concentration_pct": 100.0,
            "top_bucket_range": [price_min, price_max],
            "n_buckets": n_buckets,
            "description": "Price range near zero — all OI in single bucket",
            "window_seconds": window_seconds,
        }

    bucket_size = price_range / n_buckets

    def get_bucket(price: float) -> int:
        idx = int((price - price_min) / bucket_size)
        return max(0, min(n_buckets - 1, idx))

    # Build price interpolation for OI timestamps
    def interp_price(ts: float) -> float:
        """Return the nearest trade price for a given timestamp."""
        lo, hi = 0, len(prices_ts) - 1
        while lo < hi:
            mid = (lo + hi) // 2
            if prices_ts[mid][0] < ts:
                lo = mid + 1
            else:
                hi = mid
        return prices_ts[lo][1]

    # Compute |OI delta| per bucket
    oi_rows.sort(key=lambda x: x["ts"])
    bucket_oi = [0.0] * n_buckets

    for i in range(1, len(oi_rows)):
        prev = oi_rows[i - 1]
        curr = oi_rows[i]
        # Skip if different symbol/exchange
        if prev.get("symbol") != curr.get("symbol"):
            continue
        delta = abs((curr.get("oi_value") or 0) - (prev.get("oi_value") or 0))
        if delta < 1e-6:
            continue
        price_at_ts = interp_price(curr["ts"])
        b = get_bucket(price_at_ts)
        bucket_oi[b] += delta

    total_oi = sum(bucket_oi)
    if total_oi < 1e-6:
        return {
            "concentration_pct": None,
            "top_bucket_range": None,
            "n_buckets": n_buckets,
            "description": "No OI changes in window",
            "window_seconds": window_seconds,
        }

    top_bucket_idx = bucket_oi.index(max(bucket_oi))
    top_bucket_oi = bucket_oi[top_bucket_idx]
    concentration_pct = top_bucket_oi / total_oi * 100

    bucket_low = price_min + top_bucket_idx * bucket_size
    bucket_high = bucket_low + bucket_size

    # Intensity rating
    if concentration_pct >= 60:
        intensity = "high"
        label = "🔥"
    elif concentration_pct >= 40:
        intensity = "medium"
        label = "⚡"
    else:
        intensity = "low"
        label = "💧"

    desc = (
        f"{label} OI concentration {concentration_pct:.1f}% in price range "
        f"{bucket_low:.4f}–{bucket_high:.4f} ({intensity})"
    )

    return {
        "concentration_pct": round(concentration_pct, 2),
        "top_bucket_range": [round(bucket_low, 8), round(bucket_high, 8)],
        "top_bucket_idx": top_bucket_idx,
        "n_buckets": n_buckets,
        "intensity": intensity,
        "description": desc,
        "price_range": [round(price_min, 8), round(price_max, 8)],
        "bucket_oi": [round(v, 2) for v in bucket_oi],
        "total_oi_delta": round(total_oi, 2),
        "window_seconds": window_seconds,
    }


async def compute_vpin(symbol: str = None, window_seconds: int = 1800, n_buckets: int = 50) -> Dict:
    """
    VPIN (Volume-synchronized Probability of Informed Trading) approximation.
    
    Classic VPIN: divide total volume into equal-sized volume buckets, 
    in each bucket compute |buy_vol - sell_vol| / bucket_vol.
    VPIN = average of these ratios over last N buckets.
    
    High VPIN (>0.5) → high toxicity / informed trading → adverse selection risk.
    Low VPIN (<0.2) → mostly noise trading.
    """
    from storage import get_trades_for_cvd
    import time
    since = time.time() - window_seconds
    trades = await get_trades_for_cvd(since=since, symbol=symbol)

    if not trades or len(trades) < 10:
        return {
            "vpin": None,
            "toxicity": "insufficient_data",
            "description": "Not enough trades for VPIN",
            "n_buckets_used": 0,
            "window_seconds": window_seconds,
        }

    # Sort by time
    trades.sort(key=lambda t: t["ts"])

    # Compute total volume
    total_vol = sum(t["qty"] for t in trades)
    if total_vol < 1e-10:
        return {
            "vpin": None,
            "toxicity": "insufficient_data",
            "description": "Zero volume",
            "n_buckets_used": 0,
            "window_seconds": window_seconds,
        }

    bucket_vol = total_vol / n_buckets

    # Fill buckets
    vpin_buckets = []
    buy_vol = 0.0
    sell_vol = 0.0
    current_bucket_vol = 0.0

    for t in trades:
        qty = t["qty"]
        is_buy = _cvd_delta(t) > 0

        remaining = qty
        while remaining > 0:
            space = bucket_vol - current_bucket_vol
            fill = min(remaining, space)
            if is_buy:
                buy_vol += fill
            else:
                sell_vol += fill
            current_bucket_vol += fill
            remaining -= fill

            if current_bucket_vol >= bucket_vol - 1e-12:
                # Bucket complete
                bv = buy_vol + sell_vol
                if bv > 0:
                    vpin_buckets.append(abs(buy_vol - sell_vol) / bv)
                buy_vol = 0.0
                sell_vol = 0.0
                current_bucket_vol = 0.0

    # Add partial last bucket if meaningful
    if current_bucket_vol > bucket_vol * 0.3:
        bv = buy_vol + sell_vol
        if bv > 0:
            vpin_buckets.append(abs(buy_vol - sell_vol) / bv)

    if not vpin_buckets:
        return {
            "vpin": None,
            "toxicity": "insufficient_data",
            "description": "Could not form volume buckets",
            "n_buckets_used": 0,
            "window_seconds": window_seconds,
        }

    vpin = sum(vpin_buckets) / len(vpin_buckets)

    # Toxicity classification
    if vpin >= 0.6:
        toxicity = "extreme"
        label = "🔴"
        desc_suffix = "extreme toxicity — informed flow dominant"
    elif vpin >= 0.45:
        toxicity = "high"
        label = "🟠"
        desc_suffix = "high toxicity — elevated adverse selection"
    elif vpin >= 0.3:
        toxicity = "moderate"
        label = "🟡"
        desc_suffix = "moderate toxicity — mixed flow"
    else:
        toxicity = "low"
        label = "🟢"
        desc_suffix = "low toxicity — noise-dominated"

    desc = f"{label} VPIN={vpin:.3f} — {desc_suffix}"

    # Rolling VPIN series for trend (last 10 buckets vs first 10)
    trend = "stable"
    if len(vpin_buckets) >= 20:
        recent = sum(vpin_buckets[-10:]) / 10
        earlier = sum(vpin_buckets[:10]) / 10
        if recent > earlier * 1.2:
            trend = "rising"
        elif recent < earlier * 0.8:
            trend = "falling"

    return {
        "vpin": round(vpin, 4),
        "toxicity": toxicity,
        "trend": trend,
        "description": desc,
        "n_buckets_used": len(vpin_buckets),
        "bucket_volume": round(bucket_vol, 4),
        "total_volume": round(total_vol, 4),
        "window_seconds": window_seconds,
        "series": [round(v, 4) for v in vpin_buckets[-20:]],  # last 20 buckets for sparkline
    }


async def compute_realized_vs_implied_vol(symbol: str = None, window_seconds: int = 3600, candle_size: int = 60) -> Dict:
    """
    Realized vs implied volatility comparison.
    
    Realized vol: annualized std dev of log returns over window, computed from 1-min candles.
    Implied vol (proxy): ATR(14) normalized by price × sqrt(annualization factor).
    
    Convergence signal: when realized vol > implied vol proxy → market moving faster than expected.
    Divergence signal: when realized vol << implied vol proxy → market calmer than expected.
    """
    import math
    import time
    from storage import get_recent_trades
    
    since = time.time() - window_seconds
    trades = await get_recent_trades(limit=10000, since=since, symbol=symbol)
    
    if not trades or len(trades) < 20:
        return {
            "realized_vol_pct": None,
            "implied_vol_pct": None,
            "vol_ratio": None,
            "signal": "insufficient_data",
            "description": "Not enough data",
            "window_seconds": window_seconds,
        }
    
    trades.sort(key=lambda t: t["ts"])
    
    # Build candles of candle_size seconds
    candles = {}
    for t in trades:
        bucket = int(t["ts"] // candle_size) * candle_size
        p = t.get("price", 0)
        q = t.get("qty", 0)
        if p <= 0:
            continue
        if bucket not in candles:
            candles[bucket] = {"open": p, "high": p, "low": p, "close": p, "volume": q}
        else:
            c = candles[bucket]
            c["high"] = max(c["high"], p)
            c["low"] = min(c["low"], p)
            c["close"] = p
            c["volume"] += q
    
    sorted_candles = sorted(candles.items())
    if len(sorted_candles) < 5:
        return {
            "realized_vol_pct": None,
            "implied_vol_pct": None,
            "vol_ratio": None,
            "signal": "insufficient_data",
            "description": "Too few candles",
            "window_seconds": window_seconds,
        }
    
    closes = [c["close"] for _, c in sorted_candles]
    
    # Realized vol: std dev of log returns, annualized
    log_returns = []
    for i in range(1, len(closes)):
        if closes[i - 1] > 0 and closes[i] > 0:
            lr = math.log(closes[i] / closes[i - 1])
            log_returns.append(lr)
    
    if len(log_returns) < 3:
        return {
            "realized_vol_pct": None,
            "implied_vol_pct": None,
            "vol_ratio": None,
            "signal": "insufficient_data",
            "description": "Insufficient returns",
            "window_seconds": window_seconds,
        }
    
    n = len(log_returns)
    mean_r = sum(log_returns) / n
    variance = sum((r - mean_r) ** 2 for r in log_returns) / (n - 1)
    std_dev = math.sqrt(variance)
    
    # Annualize: candles per year = (365 * 24 * 3600) / candle_size
    candles_per_year = (365 * 24 * 3600) / candle_size
    realized_vol = std_dev * math.sqrt(candles_per_year)
    realized_vol_pct = realized_vol * 100
    
    # ATR-implied vol proxy: ATR(14) / price → normalize to per-candle, then annualize
    highs = [c["high"] for _, c in sorted_candles]
    lows = [c["low"] for _, c in sorted_candles]
    
    # True ranges
    trs = []
    for i in range(1, len(sorted_candles)):
        prev_close = sorted_candles[i - 1][1]["close"]
        high = highs[i]
        low = lows[i]
        tr = max(high - low, abs(high - prev_close), abs(low - prev_close))
        trs.append(tr)
    
    if not trs:
        implied_vol_pct = None
    else:
        period = min(14, len(trs))
        atr = sum(trs[:period]) / period
        current_price = closes[-1]
        if current_price > 0:
            atr_pct_per_candle = atr / current_price
            implied_vol_pct = atr_pct_per_candle * math.sqrt(candles_per_year) * 100
        else:
            implied_vol_pct = None
    
    # Signal
    if implied_vol_pct is None or implied_vol_pct < 1e-6:
        signal = "no_implied"
        desc = f"Realized vol: {realized_vol_pct:.1f}% (annualized, implied unavailable)"
        vol_ratio = None
    else:
        vol_ratio = realized_vol_pct / implied_vol_pct
        if vol_ratio >= 1.3:
            signal = "realized_high"
            emoji = "🔥"
            desc = f"{emoji} Realized {realized_vol_pct:.1f}% > Implied {implied_vol_pct:.1f}% (ratio {vol_ratio:.2f}x) — market moving faster than expected"
        elif vol_ratio <= 0.7:
            signal = "realized_low"
            emoji = "😴"
            desc = f"{emoji} Realized {realized_vol_pct:.1f}% < Implied {implied_vol_pct:.1f}% (ratio {vol_ratio:.2f}x) — market calmer than expected"
        else:
            signal = "converged"
            emoji = "⚖️"
            desc = f"{emoji} Realized {realized_vol_pct:.1f}% ≈ Implied {implied_vol_pct:.1f}% (ratio {vol_ratio:.2f}x) — converged"
    
    return {
        "realized_vol_pct": round(realized_vol_pct, 2),
        "implied_vol_pct": round(implied_vol_pct, 2) if implied_vol_pct is not None else None,
        "vol_ratio": round(vol_ratio, 3) if vol_ratio is not None else None,
        "signal": signal,
        "description": desc,
        "n_candles": len(sorted_candles),
        "n_returns": len(log_returns),
        "candle_size": candle_size,
        "window_seconds": window_seconds,
    }


def _compute_rsi(closes: list, period: int = 14) -> list:
    """Compute RSI series from close prices. Returns list of RSI values."""
    if len(closes) < period + 1:
        return []
    
    gains = []
    losses = []
    for i in range(1, len(closes)):
        diff = closes[i] - closes[i - 1]
        gains.append(max(diff, 0))
        losses.append(max(-diff, 0))
    
    if len(gains) < period:
        return []
    
    avg_gain = sum(gains[:period]) / period
    avg_loss = sum(losses[:period]) / period
    
    rsi_values = []
    for i in range(period, len(gains)):
        if avg_loss < 1e-12:
            rsi = 100.0
        else:
            rs = avg_gain / avg_loss
            rsi = 100 - (100 / (1 + rs))
        rsi_values.append(rsi)
        
        avg_gain = (avg_gain * (period - 1) + gains[i]) / period
        avg_loss = (avg_loss * (period - 1) + losses[i]) / period
    
    return rsi_values


async def compute_mtf_rsi_divergence(symbol: str = None, rsi_period: int = 14) -> Dict:
    """
    Multi-timeframe RSI divergence detector.
    
    Computes RSI on 5m and 1h candles from recent trade data.
    Detects:
    - Bullish divergence: price lower low, RSI higher low
    - Bearish divergence: price higher high, RSI lower high
    
    Returns RSI values for each timeframe + divergence signal.
    """
    import time
    from storage import get_recent_trades
    
    now = time.time()
    # Need 4h of data for 1h RSI (need rsi_period + extra candles)
    since_4h = now - 4 * 3600
    since_5m = now - 5 * 60 * (rsi_period + 10)  # enough 5m candles
    
    # Fetch enough trades for both timeframes
    fetch_since = min(since_4h, since_5m)
    trades = await get_recent_trades(limit=20000, since=fetch_since, symbol=symbol)
    
    if not trades or len(trades) < 30:
        return {
            "rsi_5m": None, "rsi_1h": None,
            "divergence": None,
            "description": "Insufficient data",
        }
    
    trades.sort(key=lambda t: t["ts"])
    
    def build_candles(trades_list, candle_size: int) -> list:
        """Build OHLCV candles and return list of (ts, open, high, low, close)."""
        buckets = {}
        for t in trades_list:
            p = t.get("price", 0)
            if p <= 0:
                continue
            b = int(t["ts"] // candle_size) * candle_size
            if b not in buckets:
                buckets[b] = {"open": p, "high": p, "low": p, "close": p}
            else:
                c = buckets[b]
                c["high"] = max(c["high"], p)
                c["low"] = min(c["low"], p)
                c["close"] = p
        return sorted(buckets.items())
    
    candles_5m = build_candles(trades, 300)   # 5 min
    candles_1h = build_candles(trades, 3600)  # 60 min
    
    closes_5m = [c["close"] for _, c in candles_5m]
    closes_1h = [c["close"] for _, c in candles_1h]
    
    rsi_5m_series = _compute_rsi(closes_5m, rsi_period)
    rsi_1h_series = _compute_rsi(closes_1h, rsi_period)
    
    rsi_5m_current = rsi_5m_series[-1] if rsi_5m_series else None
    rsi_1h_current = rsi_1h_series[-1] if rsi_1h_series else None
    
    # Divergence detection: compare last 2 peaks/troughs
    def detect_divergence(prices: list, rsi_vals: list, lookback: int = 5):
        """Detect bull/bear divergence in last lookback points."""
        if len(prices) < lookback + 2 or len(rsi_vals) < lookback + 2:
            return None
        
        recent_prices = prices[-lookback:]
        recent_rsi = rsi_vals[-lookback:]
        
        # Bearish: price makes higher high, RSI makes lower high
        price_max_idx = recent_prices.index(max(recent_prices))
        if price_max_idx > 0:
            prev_max_price = max(recent_prices[:price_max_idx])
            prev_max_rsi = max(recent_rsi[:price_max_idx])
            cur_price = recent_prices[-1]
            cur_rsi = recent_rsi[-1]
            if cur_price > prev_max_price and cur_rsi < prev_max_rsi:
                return "bearish"
        
        # Bullish: price makes lower low, RSI makes higher low
        price_min_idx = recent_prices.index(min(recent_prices))
        if price_min_idx > 0:
            prev_min_price = min(recent_prices[:price_min_idx])
            prev_min_rsi = min(recent_rsi[:price_min_idx])
            cur_price = recent_prices[-1]
            cur_rsi = recent_rsi[-1]
            if cur_price < prev_min_price and cur_rsi > prev_min_rsi:
                return "bullish"
        
        return None
    
    div_5m = detect_divergence(closes_5m, rsi_5m_series) if rsi_5m_series else None
    div_1h = detect_divergence(closes_1h, rsi_1h_series) if rsi_1h_series else None
    
    # Determine overall signal
    if div_5m == div_1h and div_5m is not None:
        convergence = "strong"
        divergence = div_5m
    elif div_5m or div_1h:
        convergence = "weak"
        divergence = div_5m or div_1h
    else:
        convergence = None
        divergence = None
    
    # RSI zones
    def rsi_zone(rsi):
        if rsi is None:
            return "unknown"
        if rsi >= 70:
            return "overbought"
        elif rsi <= 30:
            return "oversold"
        else:
            return "neutral"
    
    zone_5m = rsi_zone(rsi_5m_current)
    zone_1h = rsi_zone(rsi_1h_current)
    
    # Description
    if divergence == "bearish" and convergence == "strong":
        desc = f"🐻 STRONG bearish RSI divergence (5m+1h) — price higher, RSI lower"
        severity = "high"
    elif divergence == "bullish" and convergence == "strong":
        desc = f"🐂 STRONG bullish RSI divergence (5m+1h) — price lower, RSI higher"
        severity = "high"
    elif divergence == "bearish":
        tf = "5m" if div_5m else "1h"
        desc = f"🐻 Bearish RSI divergence ({tf}) — price higher, RSI lower"
        severity = "medium"
    elif divergence == "bullish":
        tf = "5m" if div_5m else "1h"
        desc = f"🐂 Bullish RSI divergence ({tf}) — price lower, RSI higher"
        severity = "medium"
    else:
        r5 = f"{rsi_5m_current:.1f}" if rsi_5m_current is not None else "?"
        r1 = f"{rsi_1h_current:.1f}" if rsi_1h_current is not None else "?"
        desc = f"No divergence — RSI 5m:{r5} ({zone_5m}) / 1h:{r1} ({zone_1h})"
        severity = "info"
    
    return {
        "rsi_5m": round(rsi_5m_current, 2) if rsi_5m_current is not None else None,
        "rsi_1h": round(rsi_1h_current, 2) if rsi_1h_current is not None else None,
        "zone_5m": zone_5m,
        "zone_1h": zone_1h,
        "divergence": divergence,
        "convergence": convergence,
        "divergence_5m": div_5m,
        "divergence_1h": div_1h,
        "severity": severity,
        "description": desc,
        "rsi_5m_series": [round(v, 2) for v in rsi_5m_series[-20:]],
        "rsi_1h_series": [round(v, 2) for v in rsi_1h_series[-20:]],
        "n_candles_5m": len(candles_5m),
        "n_candles_1h": len(candles_1h),
    }


async def compute_aggressor_ratio_series(
    symbol: str = None, 
    window_seconds: int = 1800,  # 30m total window
    bucket_size: int = 60,       # 1m buckets
) -> Dict:
    """
    Trade aggressor ratio time series: % buy-initiated trades per time bucket.
    
    Aggressor = taker side: if side='buy', buyer was aggressor (market buy order).
    Returns time series of buy% over 30m in 1m buckets.
    
    Signal:
    - >70% buyers → strong buy aggression
    - <30% buyers → strong sell aggression
    """
    import time
    from storage import get_recent_trades
    
    since = time.time() - window_seconds
    trades = await get_recent_trades(limit=20000, since=since, symbol=symbol)
    
    if not trades:
        return {
            "series": [], "current_ratio": None,
            "description": "No data", "signal": "no_data",
        }
    
    # Build buckets
    buckets = {}
    for t in trades:
        b = int(t["ts"] // bucket_size) * bucket_size
        side = (t.get("side") or "").lower()
        if b not in buckets:
            buckets[b] = {"buy": 0, "sell": 0, "total": 0}
        buckets[b]["total"] += 1
        if side in ("buy",):
            buckets[b]["buy"] += 1
        else:
            buckets[b]["sell"] += 1
    
    sorted_buckets = sorted(buckets.items())
    series = []
    for ts, c in sorted_buckets:
        total = c["total"]
        buy_ratio = c["buy"] / total if total > 0 else 0.5
        series.append({
            "ts": ts,
            "buy_pct": round(buy_ratio * 100, 2),
            "sell_pct": round((1 - buy_ratio) * 100, 2),
            "total": total,
            "buy": c["buy"],
            "sell": c["sell"],
        })
    
    if not series:
        return {
            "series": [], "current_ratio": None,
            "description": "No buckets", "signal": "no_data",
        }
    
    # Current ratio from last bucket + recent weighted
    last = series[-1]
    current_pct = last["buy_pct"]
    
    # Rolling average of last 5 buckets for smoother signal
    recent = series[-5:]
    total_trades = sum(b["total"] for b in recent)
    total_buy = sum(b["buy"] for b in recent)
    rolling_pct = (total_buy / total_trades * 100) if total_trades > 0 else 50.0
    
    if rolling_pct >= 70:
        signal = "strong_buy_aggression"
        emoji = "🟢"
        desc = f"{emoji} Strong buy aggression: {rolling_pct:.1f}% buyers (30m rolling)"
    elif rolling_pct >= 60:
        signal = "mild_buy_aggression"
        emoji = "🟡"
        desc = f"{emoji} Mild buy aggression: {rolling_pct:.1f}% buyers"
    elif rolling_pct <= 30:
        signal = "strong_sell_aggression"
        emoji = "🔴"
        desc = f"{emoji} Strong sell aggression: {rolling_pct:.1f}% buyers ({(100 - rolling_pct):.1f}% sellers)"
    elif rolling_pct <= 40:
        signal = "mild_sell_aggression"
        emoji = "🟡"
        desc = f"{emoji} Mild sell aggression: {rolling_pct:.1f}% buyers"
    else:
        signal = "balanced"
        emoji = "⚪"
        desc = f"{emoji} Balanced: {rolling_pct:.1f}% buyers"
    
    return {
        "series": series,
        "current_ratio": round(current_pct, 2),
        "rolling_buy_pct": round(rolling_pct, 2),
        "rolling_sell_pct": round(100 - rolling_pct, 2),
        "signal": signal,
        "description": desc,
        "n_buckets": len(series),
        "window_seconds": window_seconds,
        "bucket_size": bucket_size,
    }


async def compute_kalman_price(
    symbol: str = None,
    window_seconds: int = 1800,
    process_noise: float = 1e-5,
    measurement_noise: float = 1e-3,
) -> Dict:
    """
    Kalman filter smoothed price vs raw price.
    
    1D Kalman filter for price smoothing:
    - State: [price, velocity]
    - Process noise Q controls how much we trust the model
    - Measurement noise R controls how much we trust raw price
    
    Returns:
    - Smoothed price series
    - Current smoothed vs raw deviation
    - Noise ratio (signal for micro-structure noise detection)
    """
    import time
    import math
    from storage import get_recent_trades
    
    since = time.time() - window_seconds
    trades = await get_recent_trades(limit=10000, since=since, symbol=symbol)
    
    if not trades or len(trades) < 10:
        return {
            "smoothed_price": None,
            "raw_price": None,
            "deviation_pct": None,
            "noise_ratio": None,
            "description": "Insufficient data",
            "series": [],
        }
    
    trades.sort(key=lambda t: t["ts"])
    prices = [(t["ts"], t["price"]) for t in trades if t.get("price", 0) > 0]
    
    if not prices:
        return {
            "smoothed_price": None,
            "raw_price": None,
            "deviation_pct": None,
            "noise_ratio": None,
            "description": "No valid prices",
            "series": [],
        }
    
    # 1D Kalman filter: state = price estimate
    # x_k = x_{k-1} + w_k  (constant model)
    # z_k = x_k + v_k
    # P: estimate covariance, K: Kalman gain
    
    x = prices[0][1]  # initial estimate = first price
    P = 1.0           # initial covariance
    Q = process_noise   # process noise
    R = measurement_noise  # measurement noise
    
    smoothed = []
    raw_list = []
    
    for ts, z in prices:
        # Predict
        x_pred = x
        P_pred = P + Q
        
        # Update
        K = P_pred / (P_pred + R)
        x = x_pred + K * (z - x_pred)
        P = (1 - K) * P_pred
        
        smoothed.append((ts, x))
        raw_list.append(z)
    
    # Downsample for response (max 200 points)
    step = max(1, len(smoothed) // 200)
    series = [
        {"ts": int(ts), "raw": round(raw_list[i], 8), "smooth": round(sm, 8)}
        for i, (ts, sm) in enumerate(smoothed)
        if i % step == 0
    ]
    
    current_raw = prices[-1][1]
    current_smooth = smoothed[-1][1]
    deviation_pct = (current_raw - current_smooth) / current_smooth * 100 if current_smooth > 0 else 0
    
    # Noise ratio: std of (raw - smoothed) / mean price
    residuals = [r - s for r, (_, s) in zip(raw_list, smoothed)]
    if len(residuals) > 1:
        mean_r = sum(residuals) / len(residuals)
        std_r = math.sqrt(sum((r - mean_r) ** 2 for r in residuals) / len(residuals))
        mean_price = sum(raw_list) / len(raw_list)
        noise_ratio = std_r / mean_price if mean_price > 0 else 0
    else:
        noise_ratio = 0
    
    # Signal
    abs_dev = abs(deviation_pct)
    if abs_dev >= 0.5:
        signal = "high_noise"
        emoji = "🔴"
        desc = f"{emoji} High microstructure noise: raw {abs_dev:.3f}% from Kalman smooth {'above' if deviation_pct > 0 else 'below'}"
    elif abs_dev >= 0.1:
        signal = "moderate_noise"
        emoji = "🟡"
        desc = f"{emoji} Moderate noise: raw {deviation_pct:+.3f}% vs smooth"
    else:
        signal = "low_noise"
        emoji = "🟢"
        desc = f"{emoji} Low noise: raw ≈ smooth (Δ {deviation_pct:+.4f}%)"
    
    return {
        "smoothed_price": round(current_smooth, 8),
        "raw_price": round(current_raw, 8),
        "deviation_pct": round(deviation_pct, 6),
        "noise_ratio": round(noise_ratio * 100, 6),  # as percentage
        "signal": signal,
        "description": desc,
        "series": series,
        "n_points": len(prices),
        "process_noise": process_noise,
        "measurement_noise": measurement_noise,
    }


async def compute_ob_pressure_gradient(
    symbol: str = None,
    window_seconds: int = 600,
    bucket_size: int = 60,
    depth_levels: int = 10,
) -> Dict:
    """
    Order book pressure gradient: rate of change of bid/ask imbalance per minute.
    
    For each OB snapshot: imbalance = (bid_vol - ask_vol) / (bid_vol + ask_vol)
    Gradient = imbalance[t] - imbalance[t-1]
    
    Positive gradient → increasing bid pressure
    Negative gradient → increasing ask pressure (selling)
    """
    import json
    import time
    from storage import get_orderbook_history
    
    ob_rows = await get_orderbook_history(limit=200, symbol=symbol)
    
    if not ob_rows or len(ob_rows) < 2:
        return {
            "gradient": None,
            "current_imbalance": None,
            "description": "Insufficient OB data",
            "series": [],
        }
    
    # Compute imbalance per snapshot
    def compute_imbalance(row: dict) -> float:
        """Compute bid/ask volume imbalance from OB snapshot."""
        try:
            bids = json.loads(row.get("bids", "[]")) if isinstance(row.get("bids"), str) else (row.get("bids") or [])
            asks = json.loads(row.get("asks", "[]")) if isinstance(row.get("asks"), str) else (row.get("asks") or [])
        except Exception:
            return 0.0
        
        bid_vol = sum(float(b[1]) for b in bids[:depth_levels] if len(b) >= 2)
        ask_vol = sum(float(a[1]) for a in asks[:depth_levels] if len(a) >= 2)
        total = bid_vol + ask_vol
        if total < 1e-12:
            return 0.0
        return (bid_vol - ask_vol) / total
    
    # Build imbalance time series bucketed by minute
    buckets = {}
    for row in ob_rows:
        ts = row["ts"]
        imb = compute_imbalance(row)
        b = int(ts // bucket_size) * bucket_size
        if b not in buckets:
            buckets[b] = []
        buckets[b].append(imb)
    
    sorted_buckets = sorted(buckets.items())
    imb_series = [(ts, sum(vals) / len(vals)) for ts, vals in sorted_buckets if vals]
    
    if len(imb_series) < 2:
        return {
            "gradient": None,
            "current_imbalance": round(imb_series[-1][1] if imb_series else 0, 4),
            "description": "Insufficient bucketed data",
            "series": [{"ts": int(ts), "imbalance": round(v, 4)} for ts, v in imb_series],
        }
    
    # Compute gradients (imbalance change per bucket)
    gradients = []
    for i in range(1, len(imb_series)):
        grad = imb_series[i][1] - imb_series[i - 1][1]
        gradients.append((imb_series[i][0], grad))
    
    current_imbalance = imb_series[-1][1]
    current_gradient = gradients[-1][1] if gradients else 0
    
    # Rolling gradient (last 3 buckets)
    recent_grads = [g for _, g in gradients[-3:]]
    avg_gradient = sum(recent_grads) / len(recent_grads) if recent_grads else 0
    
    # Signal
    if avg_gradient >= 0.05:
        signal = "strong_bid_pressure"
        emoji = "🔼"
        desc = f"{emoji} Strong bid pressure building: gradient +{avg_gradient:.3f}/min"
    elif avg_gradient >= 0.02:
        signal = "mild_bid_pressure"
        emoji = "📈"
        desc = f"{emoji} Mild bid pressure: gradient +{avg_gradient:.3f}/min"
    elif avg_gradient <= -0.05:
        signal = "strong_ask_pressure"
        emoji = "🔽"
        desc = f"{emoji} Strong ask pressure building: gradient {avg_gradient:.3f}/min"
    elif avg_gradient <= -0.02:
        signal = "mild_ask_pressure"
        emoji = "📉"
        desc = f"{emoji} Mild ask pressure: gradient {avg_gradient:.3f}/min"
    else:
        signal = "neutral"
        emoji = "➡️"
        desc = f"{emoji} Neutral pressure: gradient {avg_gradient:+.4f}/min, imbalance {current_imbalance:+.3f}"
    
    # Build response series
    series = []
    for i, (ts, imb) in enumerate(imb_series):
        grad = gradients[i - 1][1] if i > 0 else 0
        series.append({"ts": int(ts), "imbalance": round(imb, 4), "gradient": round(grad, 4)})
    
    return {
        "gradient": round(current_gradient, 4),
        "avg_gradient": round(avg_gradient, 4),
        "current_imbalance": round(current_imbalance, 4),
        "signal": signal,
        "description": desc,
        "series": series,
        "n_buckets": len(imb_series),
        "window_seconds": window_seconds,
        "bucket_size": bucket_size,
    }
