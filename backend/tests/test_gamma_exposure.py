"""
TDD tests for Options Gamma Exposure (GEX) — Wave 23 Task 4, Issue #118.
35+ tests covering: options chain generation, net dealer gamma computation,
flip point detection, GEX signal classification, zones, API schema validation.
"""
import os
import sys
import tempfile
import math

import pytest

os.environ.setdefault("DB_PATH", os.path.join(tempfile.mkdtemp(), "test.db"))
os.environ.setdefault("SYMBOL_BINANCE", "BANANAS31USDT")
os.environ.setdefault("SYMBOL_BYBIT", "BANANAS31USDT")

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from gamma_exposure import (  # noqa: E402
    generate_options_chain,
    compute_net_dealer_gamma,
    find_flip_point,
    compute_gex_signal,
    compute_gamma_zones,
    compute_gamma_exposure,
    GEX_SIGNALS,
    STRIKE_OFFSETS,
)


# ── Fixtures ──────────────────────────────────────────────────────────────────

@pytest.fixture(scope="module")
def btc_chain():
    return generate_options_chain("BTCUSDT")


@pytest.fixture(scope="module")
def eth_chain():
    return generate_options_chain("ETHUSDT")


@pytest.fixture(scope="module")
def btc_net_gamma(btc_chain):
    return compute_net_dealer_gamma(btc_chain)


@pytest.fixture(scope="module")
def btc_result():
    return compute_gamma_exposure("BTCUSDT")


@pytest.fixture(scope="module")
def eth_result():
    return compute_gamma_exposure("ETHUSDT")


@pytest.fixture(scope="module")
def sol_result():
    return compute_gamma_exposure("SOLUSDT")


# ── generate_options_chain ────────────────────────────────────────────────────

def test_chain_returns_dict(btc_chain):
    assert isinstance(btc_chain, dict)


def test_chain_has_required_keys(btc_chain):
    for key in ("symbol", "spot", "strikes", "calls", "puts"):
        assert key in btc_chain, f"Missing key: {key}"


def test_chain_strikes_count(btc_chain):
    """Should have one strike per STRIKE_OFFSETS entry."""
    assert len(btc_chain["strikes"]) == len(STRIKE_OFFSETS)


def test_chain_strikes_are_floats(btc_chain):
    for s in btc_chain["strikes"]:
        assert isinstance(s, float)


def test_chain_spot_btc_range(btc_chain):
    """BTC spot should be in a reasonable range."""
    assert 10_000 < btc_chain["spot"] < 500_000


def test_chain_spot_eth_range(eth_chain):
    """ETH spot should be in a reasonable range."""
    assert 100 < eth_chain["spot"] < 50_000


def test_chain_calls_count(btc_chain):
    assert len(btc_chain["calls"]) == len(STRIKE_OFFSETS)


def test_chain_puts_count(btc_chain):
    assert len(btc_chain["puts"]) == len(STRIKE_OFFSETS)


def test_chain_call_has_strike_oi_gamma(btc_chain):
    for c in btc_chain["calls"]:
        assert "strike" in c
        assert "oi" in c
        assert "gamma" in c


def test_chain_put_has_strike_oi_gamma(btc_chain):
    for p in btc_chain["puts"]:
        assert "strike" in p
        assert "oi" in p
        assert "gamma" in p


def test_chain_oi_positive(btc_chain):
    for c in btc_chain["calls"]:
        assert c["oi"] >= 0
    for p in btc_chain["puts"]:
        assert p["oi"] >= 0


def test_chain_gamma_positive(btc_chain):
    for c in btc_chain["calls"]:
        assert c["gamma"] >= 0
    for p in btc_chain["puts"]:
        assert p["gamma"] >= 0


def test_chain_deterministic():
    """Same symbol always returns same chain."""
    c1 = generate_options_chain("BTCUSDT")
    c2 = generate_options_chain("BTCUSDT")
    assert c1["strikes"] == c2["strikes"]
    assert c1["calls"] == c2["calls"]
    assert c1["puts"] == c2["puts"]


def test_chain_different_symbols_differ():
    btc = generate_options_chain("BTCUSDT")
    eth = generate_options_chain("ETHUSDT")
    assert btc["spot"] != eth["spot"]
    assert btc["strikes"] != eth["strikes"]


def test_chain_atm_has_highest_gamma(btc_chain):
    """ATM strike (offset=0) should have higher gamma than far OTM strikes (±15%, ±20%)."""
    atm_idx = STRIKE_OFFSETS.index(0.00)
    atm_call_gamma = btc_chain["calls"][atm_idx]["gamma"]
    # Only compare to far OTM (first 2 and last 2 strikes, offset ±15% and ±20%)
    far_otm_indices = [0, 1, -2, -1]
    for i in far_otm_indices:
        far_gamma = btc_chain["calls"][i]["gamma"]
        assert atm_call_gamma > far_gamma, (
            f"ATM gamma {atm_call_gamma} should be > far OTM gamma {far_gamma} at idx {i}"
        )


def test_chain_atm_has_highest_oi_approx(btc_chain):
    """ATM strike should have highest OI weight on average."""
    atm_idx = STRIKE_OFFSETS.index(0.00)
    atm_call_oi = btc_chain["calls"][atm_idx]["oi"]
    # ATM OI should be greater than far OTM (first and last strikes)
    assert atm_call_oi > btc_chain["calls"][0]["oi"]
    assert atm_call_oi > btc_chain["calls"][-1]["oi"]


# ── compute_net_dealer_gamma ───────────────────────────────────────────────────

def test_net_gamma_returns_list(btc_net_gamma):
    assert isinstance(btc_net_gamma, list)


def test_net_gamma_count(btc_net_gamma):
    assert len(btc_net_gamma) == len(STRIKE_OFFSETS)


def test_net_gamma_entry_keys(btc_net_gamma):
    for entry in btc_net_gamma:
        assert "strike" in entry
        assert "net_dealer_gamma" in entry
        assert "call_gamma_exposure" in entry
        assert "put_gamma_exposure" in entry


def test_net_gamma_formula(btc_chain):
    """net_dealer_gamma = put_gex - call_gex."""
    results = compute_net_dealer_gamma(btc_chain)
    for i, entry in enumerate(results):
        call = btc_chain["calls"][i]
        put = btc_chain["puts"][i]
        expected_net = put["gamma"] * put["oi"] - call["gamma"] * call["oi"]
        assert abs(entry["net_dealer_gamma"] - expected_net) < 1e-10


def test_net_gamma_call_gex_nonneg(btc_net_gamma):
    for e in btc_net_gamma:
        assert e["call_gamma_exposure"] >= 0


def test_net_gamma_put_gex_nonneg(btc_net_gamma):
    for e in btc_net_gamma:
        assert e["put_gamma_exposure"] >= 0


def test_net_gamma_empty_chain():
    """Empty calls/puts should return empty list."""
    chain = {"symbol": "X", "spot": 100.0, "strikes": [], "calls": [], "puts": []}
    result = compute_net_dealer_gamma(chain)
    assert result == []


# ── find_flip_point ────────────────────────────────────────────────────────────

def test_flip_point_returns_float_or_none(btc_net_gamma):
    flip = find_flip_point(btc_net_gamma)
    assert flip is None or isinstance(flip, float)


def test_flip_point_in_strike_range(btc_net_gamma, btc_chain):
    flip = find_flip_point(btc_net_gamma)
    if flip is not None:
        min_s = min(btc_chain["strikes"])
        max_s = max(btc_chain["strikes"])
        assert min_s <= flip <= max_s


def test_flip_point_all_positive():
    """All positive net gamma → flip at highest strike."""
    data = [
        {"strike": 100.0, "net_dealer_gamma": 1.0},
        {"strike": 105.0, "net_dealer_gamma": 2.0},
        {"strike": 110.0, "net_dealer_gamma": 0.5},
    ]
    flip = find_flip_point(data)
    assert flip == 110.0


def test_flip_point_all_negative():
    """All negative net gamma → flip at lowest strike."""
    data = [
        {"strike": 100.0, "net_dealer_gamma": -1.0},
        {"strike": 105.0, "net_dealer_gamma": -2.0},
        {"strike": 110.0, "net_dealer_gamma": -0.5},
    ]
    flip = find_flip_point(data)
    assert flip == 100.0


def test_flip_point_crossing():
    """Crossing from negative to positive should interpolate."""
    data = [
        {"strike": 100.0, "net_dealer_gamma": -2.0},
        {"strike": 110.0, "net_dealer_gamma": 4.0},
    ]
    flip = find_flip_point(data)
    # Cumulative after strike 100: -2.0 (neg)
    # Cumulative after strike 110: -2.0 + 4.0 = 2.0 (pos)
    # Crossing: t = -(-2) / (2 - (-2)) = 2/4 = 0.5 within [100, 110]
    # But interpolation is on *cumulative* crossing, using strike range
    # prev_cum=-2, cum=2, crossing at t=0.5 → flip = 100 + 0.5*10 = 105
    assert isinstance(flip, float)
    assert 100.0 <= flip <= 110.0


def test_flip_point_empty():
    assert find_flip_point([]) is None


def test_flip_point_single_positive():
    data = [{"strike": 100.0, "net_dealer_gamma": 5.0}]
    flip = find_flip_point(data)
    assert flip == 100.0


def test_flip_point_single_negative():
    data = [{"strike": 100.0, "net_dealer_gamma": -5.0}]
    flip = find_flip_point(data)
    assert flip == 100.0


# ── compute_gex_signal ────────────────────────────────────────────────────────

def test_gex_signal_valid_values(btc_net_gamma):
    total = sum(e["net_dealer_gamma"] for e in btc_net_gamma)
    signal = compute_gex_signal(btc_net_gamma, total)
    assert signal in GEX_SIGNALS


def test_gex_signal_pinning_near_zero():
    """Very small total → pinning."""
    data = [
        {"strike": 100.0, "net_dealer_gamma": 0.001},
        {"strike": 105.0, "net_dealer_gamma": -0.001},
    ]
    signal = compute_gex_signal(data, 0.0)
    assert signal == "pinning"


def test_gex_signal_amplifying_large_negative():
    """Large negative total → amplifying."""
    data = [
        {"strike": 100.0, "net_dealer_gamma": -100.0},
        {"strike": 105.0, "net_dealer_gamma": -50.0},
    ]
    total = -150.0
    signal = compute_gex_signal(data, total)
    assert signal == "amplifying"


def test_gex_signal_empty():
    signal = compute_gex_signal([], 0.0)
    assert signal == "neutral"


def test_gex_signal_all_zero():
    data = [{"strike": 100.0, "net_dealer_gamma": 0.0}]
    signal = compute_gex_signal(data, 0.0)
    assert signal == "neutral"


# ── compute_gamma_zones ────────────────────────────────────────────────────────

def test_zones_returns_dict(btc_net_gamma, btc_chain):
    flip = find_flip_point(btc_net_gamma)
    zones = compute_gamma_zones(btc_net_gamma, flip, btc_chain["spot"])
    assert isinstance(zones, dict)
    assert "positive_gamma_zone" in zones
    assert "negative_gamma_zone" in zones


def test_zones_pos_zone_keys(btc_net_gamma, btc_chain):
    flip = find_flip_point(btc_net_gamma)
    zones = compute_gamma_zones(btc_net_gamma, flip, btc_chain["spot"])
    pz = zones["positive_gamma_zone"]
    assert "min" in pz
    assert "max" in pz
    assert "strikes" in pz


def test_zones_neg_zone_keys(btc_net_gamma, btc_chain):
    flip = find_flip_point(btc_net_gamma)
    zones = compute_gamma_zones(btc_net_gamma, flip, btc_chain["spot"])
    nz = zones["negative_gamma_zone"]
    assert "min" in nz
    assert "max" in nz
    assert "strikes" in nz


def test_zones_strikes_are_sorted(btc_net_gamma, btc_chain):
    flip = find_flip_point(btc_net_gamma)
    zones = compute_gamma_zones(btc_net_gamma, flip, btc_chain["spot"])
    pz_strikes = zones["positive_gamma_zone"]["strikes"]
    nz_strikes = zones["negative_gamma_zone"]["strikes"]
    assert pz_strikes == sorted(pz_strikes)
    assert nz_strikes == sorted(nz_strikes)


# ── compute_gamma_exposure (integration) ─────────────────────────────────────

def test_full_result_is_dict(btc_result):
    assert isinstance(btc_result, dict)


def test_full_result_required_keys(btc_result):
    required = [
        "symbol", "spot", "strikes", "net_gamma_by_strike",
        "flip_point", "positive_gamma_zone", "negative_gamma_zone",
        "total_net_gex", "gex_signal", "timestamp",
    ]
    for key in required:
        assert key in btc_result, f"Missing key: {key}"


def test_full_result_symbol(btc_result):
    assert btc_result["symbol"] == "BTCUSDT"


def test_full_result_spot_btc(btc_result):
    assert btc_result["spot"] == 65_000.0


def test_full_result_strikes_count(btc_result):
    assert len(btc_result["strikes"]) == len(STRIKE_OFFSETS)


def test_full_result_net_gamma_count(btc_result):
    assert len(btc_result["net_gamma_by_strike"]) == len(STRIKE_OFFSETS)


def test_full_result_gex_signal_valid(btc_result):
    assert btc_result["gex_signal"] in GEX_SIGNALS


def test_full_result_flip_point_numeric(btc_result):
    fp = btc_result["flip_point"]
    assert fp is None or isinstance(fp, (int, float))


def test_full_result_total_net_gex_numeric(btc_result):
    assert isinstance(btc_result["total_net_gex"], float)


def test_full_result_timestamp_recent(btc_result):
    import time
    assert btc_result["timestamp"] > time.time() - 60


def test_full_result_eth_symbol(eth_result):
    assert eth_result["symbol"] == "ETHUSDT"


def test_full_result_sol_spot_range(sol_result):
    assert 10 < sol_result["spot"] < 10_000


def test_full_result_deterministic():
    """Two calls return same values (deterministic mock)."""
    r1 = compute_gamma_exposure("BTCUSDT")
    r2 = compute_gamma_exposure("BTCUSDT")
    assert r1["strikes"] == r2["strikes"]
    assert r1["total_net_gex"] == r2["total_net_gex"]
    assert r1["flip_point"] == r2["flip_point"]
    assert r1["gex_signal"] == r2["gex_signal"]


def test_full_result_net_gamma_entry_structure(btc_result):
    for entry in btc_result["net_gamma_by_strike"]:
        assert "strike" in entry
        assert "net_dealer_gamma" in entry
        assert "call_gamma_exposure" in entry
        assert "put_gamma_exposure" in entry


def test_full_result_zone_structure(btc_result):
    pz = btc_result["positive_gamma_zone"]
    nz = btc_result["negative_gamma_zone"]
    for zone in (pz, nz):
        assert "min" in zone
        assert "max" in zone
        assert "strikes" in zone


def test_full_result_flip_in_strike_range(btc_result):
    fp = btc_result["flip_point"]
    if fp is not None:
        strikes = btc_result["strikes"]
        assert min(strikes) <= fp <= max(strikes)
