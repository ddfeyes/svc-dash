"""
Whale Wallet Flow Tracker — Wave 23 Task 3 (Issue #117).

Simulates large wallet movements (>$1M notional) to track accumulation/distribution
patterns. Computes net inflow/outflow over 7-day rolling window, derives accumulation
score, and signals trend direction.

Data source: seeded mock (deterministic per symbol, no live API).
"""
import math
import random
import time
from typing import List, Dict, Optional

# Valid flow signal values
FLOW_SIGNALS = ("accumulating", "neutral", "distributing")

# Threshold for signal classification
ACCUMULATION_HIGH_THRESHOLD = 70
DISTRIBUTION_LOW_THRESHOLD = 30

# Minimum trade size in USD (>100 BTC equiv)
MIN_TRADE_USD = 1_000_000  # $1M minimum whale trade
MAX_TRADE_USD = 50_000_000  # $50M max per single trade

# 7-day window in seconds
WINDOW_7D = 7 * 86400

# Average interval between whale trades: 2-6 hours per spec
MIN_INTERVAL_SEC = 2 * 3600   # 2 hours
MAX_INTERVAL_SEC = 6 * 3600   # 6 hours


def _symbol_seed(symbol: str) -> int:
    """Deterministic seed from symbol string."""
    return sum(ord(c) * (i + 1) for i, c in enumerate(symbol))


def generate_whale_trades(symbol: str = "BTCUSDT") -> List[Dict]:
    """
    Generate seeded mock whale trades for a 7-day window.

    Returns list of trade dicts:
        ts (float): unix timestamp
        direction (str): "inflow" | "outflow"
        amount_usd (float): trade notional in USD
        symbol (str): trading pair
    """
    seed = _symbol_seed(symbol)
    rng = random.Random(seed)
    now = time.time()
    start = now - WINDOW_7D

    trades = []
    ts = start
    while ts < now:
        interval = rng.uniform(MIN_INTERVAL_SEC, MAX_INTERVAL_SEC)
        ts += interval
        if ts >= now:
            break

        direction = rng.choices(
            ["inflow", "outflow"],
            # Slightly more inflow than outflow for bullish bias (realistic)
            weights=[0.55, 0.45],
        )[0]
        amount_usd = round(rng.uniform(MIN_TRADE_USD, MAX_TRADE_USD), 2)
        trades.append(
            {
                "ts": round(ts, 3),
                "direction": direction,
                "amount_usd": amount_usd,
                "symbol": symbol,
            }
        )

    return trades


def compute_inflow_outflow(trades: List[Dict]) -> Dict:
    """
    Aggregate inflow/outflow from trade list.

    Returns:
        whale_inflow_7d: total USD inflow over 7 days
        whale_outflow_7d: total USD outflow over 7 days
        daily_buckets: list of 7 dicts {day, inflow, outflow, net}
                       day=0 is today, day=6 is 7 days ago
    """
    now = time.time()

    # 7 daily buckets: bucket[i] = day i (0=today, 6=oldest)
    buckets = [{"day": i, "inflow": 0.0, "outflow": 0.0, "net": 0.0} for i in range(7)]

    total_inflow = 0.0
    total_outflow = 0.0

    for trade in trades:
        age_sec = now - trade["ts"]
        if age_sec < 0 or age_sec > WINDOW_7D:
            continue  # outside window
        bucket_idx = min(int(age_sec / 86400), 6)
        amt = trade["amount_usd"]
        if trade["direction"] == "inflow":
            buckets[bucket_idx]["inflow"] += amt
            total_inflow += amt
        else:
            buckets[bucket_idx]["outflow"] += amt
            total_outflow += amt

    # Compute net per bucket and round
    for b in buckets:
        b["inflow"] = round(b["inflow"], 2)
        b["outflow"] = round(b["outflow"], 2)
        b["net"] = round(b["inflow"] - b["outflow"], 2)

    return {
        "whale_inflow_7d": round(total_inflow, 2),
        "whale_outflow_7d": round(total_outflow, 2),
        "daily_buckets": buckets,
    }


def compute_accumulation_score(inflow: float, outflow: float) -> float:
    """
    Accumulation score: ratio of net inflow to total volume, scaled to 0–100.

    Formula: score = (inflow / total_volume) * 100, clamped [0, 100].
    Zero total volume returns neutral 50.
    """
    total = inflow + outflow
    if total <= 0:
        return 50.0
    score = (inflow / total) * 100.0
    return round(max(0.0, min(100.0, score)), 2)


def compute_flow_signal(accumulation_score: float) -> str:
    """
    Classify flow signal from accumulation score.

    >70  => "accumulating"
    <30  => "distributing"
    else => "neutral"
    """
    if accumulation_score >= ACCUMULATION_HIGH_THRESHOLD:
        return "accumulating"
    if accumulation_score <= DISTRIBUTION_LOW_THRESHOLD:
        return "distributing"
    return "neutral"


def compute_trend_7d(daily_buckets: List[Dict]) -> float:
    """
    Linear regression slope over 7-day net flow series.

    Returns slope (USD/day). Positive = net flow increasing (more accumulation).
    """
    n = len(daily_buckets)
    if n < 2:
        return 0.0

    # x = day index (0 = oldest, n-1 = today)
    # Note: bucket[0] = today (most recent), bucket[6] = oldest
    # Reverse so x=0 is oldest
    nets = [b["net"] for b in reversed(daily_buckets)]
    x_vals = list(range(n))

    x_mean = sum(x_vals) / n
    y_mean = sum(nets) / n

    numerator = sum((x - x_mean) * (y - y_mean) for x, y in zip(x_vals, nets))
    denominator = sum((x - x_mean) ** 2 for x in x_vals)

    if denominator == 0:
        return 0.0
    slope = numerator / denominator
    return round(slope, 2)


def compute_whale_flow(symbol: Optional[str] = None) -> Dict:
    """
    Main entry point: compute full whale wallet flow tracker result.

    Returns dict with:
        symbol: trading pair
        whale_inflow_7d: total inflow USD (7 days)
        whale_outflow_7d: total outflow USD (7 days)
        net_flow_bps: net flow in basis points of total volume
        accumulation_score: 0-100 (100 = pure accumulation)
        flow_signal: "accumulating" | "neutral" | "distributing"
        trend_7d: linear slope of daily net flow (USD/day)
        daily_buckets: [{day, inflow, outflow, net}] × 7
    """
    sym = symbol or "BTCUSDT"
    trades = generate_whale_trades(sym)
    io = compute_inflow_outflow(trades)

    inflow = io["whale_inflow_7d"]
    outflow = io["whale_outflow_7d"]
    total = inflow + outflow

    # Net flow in basis points: (inflow - outflow) / total_volume * 10000
    if total > 0:
        net_flow_bps = round((inflow - outflow) / total * 10000, 2)
    else:
        net_flow_bps = 0.0

    accumulation_score = compute_accumulation_score(inflow, outflow)
    flow_signal = compute_flow_signal(accumulation_score)
    trend_7d = compute_trend_7d(io["daily_buckets"])

    return {
        "symbol": sym,
        "whale_inflow_7d": inflow,
        "whale_outflow_7d": outflow,
        "net_flow_bps": net_flow_bps,
        "accumulation_score": accumulation_score,
        "flow_signal": flow_signal,
        "trend_7d": trend_7d,
        "daily_buckets": io["daily_buckets"],
    }
