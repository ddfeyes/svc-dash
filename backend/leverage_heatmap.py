"""
Leverage Ratio Heatmap — Wave 24 Task 5 (Issue #129).

Computes OI/MCap leverage ratios across major crypto assets (BTC, ETH, SOL, BNB),
assigns risk signals and heatmap colors, and returns 30-day history.

Data source: seeded mock (deterministic per symbol, no live API).
"""
import random
from typing import Dict, List, Optional

# Supported cross-market assets
ASSETS = ("BTC", "ETH", "SOL", "BNB")

# Valid risk signals
RISK_SIGNALS = ("high", "medium", "low")

# Valid trend values
TRENDS = ("rising", "falling", "stable")

# Valid heatmap colors
HEATMAP_COLORS = ("red", "orange", "yellow", "green")

# Leverage ratio thresholds for risk classification
HIGH_LEVERAGE_THRESHOLD = 1.4
MEDIUM_LEVERAGE_THRESHOLD = 1.0

# Heatmap color thresholds
RED_THRESHOLD = 1.5
ORANGE_THRESHOLD = 1.2
YELLOW_THRESHOLD = 0.9

# Base OI and market cap values (approximate real-world scale, USD)
_BASE_OI = {
    "BTC": 18_500_000_000,
    "ETH": 9_200_000_000,
    "SOL": 3_100_000_000,
    "BNB": 1_400_000_000,
}

_BASE_MCAP = {
    "BTC": 12_000_000_000,
    "ETH": 7_500_000_000,
    "SOL": 2_800_000_000,
    "BNB": 1_200_000_000,
}

_SECTOR = "crypto_perps"

_DESCRIPTION = (
    "Leverage Ratio (OI/MCap) measures open interest relative to market cap. "
    "High ratios signal elevated leverage and potential cascade risk. "
    "Values above 1.4 are high risk; above 1.0 are medium risk."
)


def _symbol_seed(symbol: str) -> int:
    """Deterministic seed from symbol string."""
    return sum(ord(c) * (i + 1) for i, c in enumerate(symbol))


def compute_asset_leverage(asset: str, rng: random.Random) -> Dict:
    """
    Compute leverage ratio data for a single asset.

    Returns dict with oi_usd, leverage_ratio, percentile_rank (placeholder 0),
    risk_signal, risk_score, trend, heatmap_color, history_30d.
    """
    base_oi = _BASE_OI.get(asset, 1_000_000_000)
    base_mcap = _BASE_MCAP.get(asset, 1_000_000_000)

    # Add seeded noise (±20%)
    oi_usd = round(base_oi * rng.uniform(0.80, 1.20))
    mcap_usd = round(base_mcap * rng.uniform(0.80, 1.20))

    leverage_ratio = round(oi_usd / mcap_usd, 3)

    risk_signal = _classify_risk_signal(leverage_ratio)
    risk_score = _compute_risk_score(leverage_ratio)
    trend = _classify_trend(rng)
    heatmap_color = _leverage_to_color(leverage_ratio)
    history_30d = _generate_history_30d(asset, leverage_ratio, rng)

    return {
        "oi_usd": oi_usd,
        "leverage_ratio": leverage_ratio,
        "percentile_rank": 0,  # filled in after ranking all assets
        "risk_signal": risk_signal,
        "risk_score": risk_score,
        "trend": trend,
        "heatmap_color": heatmap_color,
        "history_30d": history_30d,
    }


def _classify_risk_signal(leverage_ratio: float) -> str:
    """Classify risk signal from leverage ratio."""
    if leverage_ratio >= HIGH_LEVERAGE_THRESHOLD:
        return "high"
    if leverage_ratio >= MEDIUM_LEVERAGE_THRESHOLD:
        return "medium"
    return "low"


def _compute_risk_score(leverage_ratio: float) -> float:
    """
    Risk score 0–100 proportional to leverage ratio.
    Clamped: 0 at leverage=0, 100 at leverage>=2.0.
    """
    score = min(100.0, (leverage_ratio / 2.0) * 100.0)
    return round(max(0.0, score), 1)


def _classify_trend(rng: random.Random) -> str:
    """Seeded random trend selection."""
    return rng.choices(TRENDS, weights=[0.4, 0.3, 0.3])[0]


def _leverage_to_color(leverage_ratio: float) -> str:
    """Map leverage ratio to heatmap color."""
    if leverage_ratio >= RED_THRESHOLD:
        return "red"
    if leverage_ratio >= ORANGE_THRESHOLD:
        return "orange"
    if leverage_ratio >= YELLOW_THRESHOLD:
        return "yellow"
    return "green"


def _generate_history_30d(
    asset: str, current_ratio: float, rng: random.Random
) -> List[Dict]:
    """
    Generate 30-day leverage ratio history ending at current_ratio.

    Returns list of {date, leverage_ratio} dicts, oldest first.
    """
    from datetime import date, timedelta

    history = []
    today = date(2026, 3, 16)
    ratio = current_ratio * rng.uniform(0.85, 0.95)  # start slightly lower

    for day_offset in range(30, 0, -1):
        d = today - timedelta(days=day_offset)
        # Random walk toward current_ratio
        delta = rng.uniform(-0.03, 0.04)
        ratio = round(max(0.1, ratio + delta), 3)
        history.append({"date": str(d), "leverage_ratio": ratio})

    return history


def assign_percentile_ranks(assets_data: Dict[str, Dict]) -> Dict[str, Dict]:
    """
    Assign percentile_rank (0–100) to each asset based on leverage_ratio.

    Highest leverage_ratio gets rank 100, lowest gets 0.
    """
    sorted_assets = sorted(
        assets_data.keys(),
        key=lambda a: assets_data[a]["leverage_ratio"],
    )
    n = len(sorted_assets)
    for rank_idx, asset in enumerate(sorted_assets):
        if n == 1:
            pct = 100
        else:
            pct = round(rank_idx / (n - 1) * 100)
        assets_data[asset]["percentile_rank"] = pct
    return assets_data


def compute_leverage_ratio_heatmap(symbol: Optional[str] = None) -> Dict:
    """
    Main entry point: compute leverage ratio heatmap across BTC/ETH/SOL/BNB.

    Returns:
        assets: dict of asset -> {oi_usd, leverage_ratio, percentile_rank,
                                   risk_signal, risk_score, trend,
                                   heatmap_color, history_30d}
        sector: str
        description: str
    """
    seed = _symbol_seed(symbol or "BTCUSDT")
    rng = random.Random(seed)

    assets_data: Dict[str, Dict] = {}
    for asset in ASSETS:
        asset_rng = random.Random(seed + _symbol_seed(asset))
        assets_data[asset] = compute_asset_leverage(asset, asset_rng)

    assign_percentile_ranks(assets_data)

    return {
        "assets": assets_data,
        "sector": _SECTOR,
        "description": _DESCRIPTION,
    }
