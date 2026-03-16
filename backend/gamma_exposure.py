"""
Options Gamma Exposure (GEX) — Wave 23 Task 4 (Issue #118).

Simulates a synthetic options chain for crypto assets and computes:
- Net dealer gamma per strike (dealer is short calls, long puts by convention)
- Zero-gamma flip point (strike where cumulative net gamma crosses zero)
- GEX signal: pinning | amplifying | neutral
- Positive/negative gamma zones

Data source: seeded mock options chain — deterministic per symbol, no live API.
"""
import math
import random
from typing import List, Dict, Optional

# ── Constants ──────────────────────────────────────────────────────────────────

# Valid GEX signal values
GEX_SIGNALS = ("pinning", "amplifying", "neutral")

# Pinning threshold: if abs(total_net_gex) < this fraction of max OI * gamma → "pinning"
PINNING_THRESHOLD_RATIO = 0.05

# Default spot prices per symbol for seeding
DEFAULT_SPOTS = {
    "BTCUSDT": 65_000.0,
    "ETHUSDT": 3_200.0,
    "SOLUSDT": 150.0,
    "BNBUSDT": 550.0,
}
DEFAULT_SPOT = 65_000.0  # fallback

# Strike offsets from spot (as fractions): ±5%, ±10%, ±15%, ±20%
STRIKE_OFFSETS = [-0.20, -0.15, -0.10, -0.05, 0.00, 0.05, 0.10, 0.15, 0.20]

# Open Interest distribution weights (peak near ATM)
# Weights correspond to STRIKE_OFFSETS indices
OI_WEIGHTS = [0.05, 0.10, 0.20, 0.30, 0.40, 0.30, 0.20, 0.10, 0.05]

# Base OI per contract (will be scaled by weight + noise)
BASE_OI = 10_000  # total open interest units to distribute

# Gamma per contract (normalized, higher near ATM)
# In practice, gamma ~ N(0, sigma)/S for near-ATM; we simulate relative gamma
BASE_GAMMA = 0.001  # base gamma per contract at ATM

# GEX signal threshold
AMPLIFYING_THRESHOLD_RATIO = 0.15  # dominant negative GEX fraction


def _symbol_seed(symbol: str) -> int:
    """Deterministic seed from symbol string."""
    return sum(ord(c) * (i + 1) for i, c in enumerate(symbol)) + 42


def _get_spot(symbol: str) -> float:
    """Return simulated spot price for a symbol."""
    return DEFAULT_SPOTS.get(symbol.upper(), DEFAULT_SPOT)


def generate_options_chain(symbol: str = "BTCUSDT") -> Dict:
    """
    Generate a seeded mock options chain for the given symbol.

    Returns dict with:
        spot (float): current spot price
        strikes (list[float]): option strike prices
        calls (list[dict]): call option data per strike (oi, gamma)
        puts (list[dict]): put option data per strike (oi, gamma)

    Dealer convention: dealers are short calls (sell calls to hedgers) and long puts.
    Net dealer gamma per strike = -call_gamma * call_oi + put_gamma * put_oi
    """
    seed = _symbol_seed(symbol)
    rng = random.Random(seed)
    spot = _get_spot(symbol)

    strikes = []
    calls = []
    puts = []

    for i, offset in enumerate(STRIKE_OFFSETS):
        strike = round(spot * (1 + offset), 2)
        strikes.append(strike)

        weight = OI_WEIGHTS[i]
        noise_call = rng.uniform(0.7, 1.3)
        noise_put = rng.uniform(0.7, 1.3)

        call_oi = int(BASE_OI * weight * noise_call)
        put_oi = int(BASE_OI * weight * noise_put)

        # Gamma decays with moneyness (higher near ATM)
        moneyness_dist = abs(offset)
        gamma_factor = math.exp(-8 * moneyness_dist * moneyness_dist)
        base_g = BASE_GAMMA * gamma_factor

        call_gamma = base_g * rng.uniform(0.85, 1.15)
        put_gamma = base_g * rng.uniform(0.85, 1.15)

        calls.append({"strike": strike, "oi": call_oi, "gamma": call_gamma})
        puts.append({"strike": strike, "oi": put_oi, "gamma": put_gamma})

    return {
        "symbol": symbol,
        "spot": spot,
        "strikes": strikes,
        "calls": calls,
        "puts": puts,
    }


def compute_net_dealer_gamma(chain: Dict) -> List[Dict]:
    """
    Compute net dealer gamma per strike.

    Dealer convention:
        - Dealers are short calls → dealer gamma = -call_gamma * call_oi
        - Dealers are long puts  → dealer gamma = +put_gamma * put_oi
        - Net dealer gamma per strike = put_gamma * put_oi - call_gamma * call_oi

    Returns list of dicts with:
        strike (float)
        call_gamma_exposure (float)
        put_gamma_exposure (float)
        net_dealer_gamma (float): positive = net long gamma (pinning), negative = net short (amplifying)
    """
    results = []
    calls_by_strike = {c["strike"]: c for c in chain["calls"]}
    puts_by_strike = {p["strike"]: p for p in chain["puts"]}

    for strike in chain["strikes"]:
        call = calls_by_strike.get(strike, {"oi": 0, "gamma": 0.0})
        put = puts_by_strike.get(strike, {"oi": 0, "gamma": 0.0})

        call_gex = call["gamma"] * call["oi"]
        put_gex = put["gamma"] * put["oi"]
        net = put_gex - call_gex

        results.append({
            "strike": strike,
            "call_gamma_exposure": call_gex,
            "put_gamma_exposure": put_gex,
            "net_dealer_gamma": net,
        })

    return results


def find_flip_point(net_gamma_by_strike: List[Dict]) -> Optional[float]:
    """
    Find the zero-gamma flip point: the strike level where cumulative net dealer gamma
    transitions from positive to negative (or vice versa).

    Returns the interpolated strike price of the flip, or None if no crossing exists.

    Algorithm:
        - Build cumulative sum of net_dealer_gamma sorted by strike ascending
        - Find two consecutive strikes where cumulative sum crosses zero
        - Interpolate between them
    """
    if not net_gamma_by_strike:
        return None

    sorted_data = sorted(net_gamma_by_strike, key=lambda x: x["strike"])

    cumulative = 0.0
    prev_cum = None
    prev_strike = None

    for entry in sorted_data:
        cumulative += entry["net_dealer_gamma"]
        strike = entry["strike"]

        if prev_cum is not None:
            # Check for zero crossing
            if (prev_cum < 0 and cumulative >= 0) or (prev_cum >= 0 and cumulative < 0):
                # Linear interpolation: find where cumsum = 0
                # prev_cum + t * (cumulative - prev_cum) = 0
                # t = -prev_cum / (cumulative - prev_cum)
                delta = cumulative - prev_cum
                if abs(delta) > 1e-15:
                    t = -prev_cum / delta
                    flip = prev_strike + t * (strike - prev_strike)
                    return round(flip, 2)

        prev_cum = cumulative
        prev_strike = strike

    # No crossing found — return lowest strike if all negative, highest if all positive
    if cumulative < 0:
        return sorted_data[0]["strike"]
    else:
        return sorted_data[-1]["strike"]


def compute_gex_signal(
    net_gamma_by_strike: List[Dict],
    total_net_gex: float,
    pinning_threshold: Optional[float] = None,
) -> str:
    """
    Classify GEX signal:
        - "pinning":    abs(total_net_gex) is small (market pinned near flip point)
        - "amplifying": dominant negative net gamma (dealers short gamma → amplify moves)
        - "neutral":    mixed / moderate

    Args:
        net_gamma_by_strike: list from compute_net_dealer_gamma
        total_net_gex: sum of all net_dealer_gamma values
        pinning_threshold: override threshold; auto-computed if None

    Returns one of GEX_SIGNALS.
    """
    if not net_gamma_by_strike:
        return "neutral"

    # Compute max absolute gamma contribution for threshold reference
    max_abs = max(abs(e["net_dealer_gamma"]) for e in net_gamma_by_strike)
    if max_abs == 0:
        return "neutral"

    if pinning_threshold is None:
        pinning_threshold = max_abs * PINNING_THRESHOLD_RATIO * len(net_gamma_by_strike)

    if abs(total_net_gex) < pinning_threshold:
        return "pinning"

    # Negative total → dealers net short gamma → amplifying moves
    if total_net_gex < -pinning_threshold * AMPLIFYING_THRESHOLD_RATIO:
        return "amplifying"

    return "neutral"


def compute_gamma_zones(
    net_gamma_by_strike: List[Dict],
    flip_point: Optional[float],
    spot: float,
) -> Dict:
    """
    Compute positive and negative gamma zones relative to flip point and spot.

    Returns:
        positive_gamma_zone: dict with min/max strikes where net_dealer_gamma > 0
        negative_gamma_zone: dict with min/max strikes where net_dealer_gamma < 0
    """
    pos_strikes = [e["strike"] for e in net_gamma_by_strike if e["net_dealer_gamma"] > 0]
    neg_strikes = [e["strike"] for e in net_gamma_by_strike if e["net_dealer_gamma"] < 0]

    pos_zone = {
        "min": min(pos_strikes) if pos_strikes else None,
        "max": max(pos_strikes) if pos_strikes else None,
        "strikes": sorted(pos_strikes),
    }
    neg_zone = {
        "min": min(neg_strikes) if neg_strikes else None,
        "max": max(neg_strikes) if neg_strikes else None,
        "strikes": sorted(neg_strikes),
    }

    return {
        "positive_gamma_zone": pos_zone,
        "negative_gamma_zone": neg_zone,
    }


def compute_gamma_exposure(symbol: str = "BTCUSDT") -> Dict:
    """
    Main entry point: compute full GEX analysis for a symbol.

    Returns dict with:
        symbol (str)
        spot (float)
        strikes (list[float])
        net_gamma_by_strike (list[dict]): per-strike gamma data
        flip_point (float|None): zero-gamma flip level
        positive_gamma_zone (dict): zone above flip point
        negative_gamma_zone (dict): zone below flip point
        total_net_gex (float): sum of all net dealer gamma
        gex_signal (str): pinning | amplifying | neutral
        timestamp (float): unix epoch
    """
    import time

    chain = generate_options_chain(symbol)
    net_gamma = compute_net_dealer_gamma(chain)
    total_net_gex = sum(e["net_dealer_gamma"] for e in net_gamma)
    flip_point = find_flip_point(net_gamma)
    signal = compute_gex_signal(net_gamma, total_net_gex)
    zones = compute_gamma_zones(net_gamma, flip_point, chain["spot"])

    return {
        "symbol": symbol,
        "spot": chain["spot"],
        "strikes": chain["strikes"],
        "net_gamma_by_strike": [
            {
                "strike": e["strike"],
                "net_dealer_gamma": round(e["net_dealer_gamma"], 6),
                "call_gamma_exposure": round(e["call_gamma_exposure"], 6),
                "put_gamma_exposure": round(e["put_gamma_exposure"], 6),
            }
            for e in net_gamma
        ],
        "flip_point": flip_point,
        "positive_gamma_zone": zones["positive_gamma_zone"],
        "negative_gamma_zone": zones["negative_gamma_zone"],
        "total_net_gex": round(total_net_gex, 6),
        "gex_signal": signal,
        "timestamp": time.time(),
    }
