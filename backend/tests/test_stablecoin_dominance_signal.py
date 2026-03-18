"""Tests for compute_stablecoin_dominance_signal() — Wave 26.

60+ tests covering all required keys, value ranges, structural invariants,
signal/trend consistency, breakdown validation, and determinism.
"""

import asyncio
import sys
import os

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from metrics import compute_stablecoin_dominance_signal

REQUIRED_KEYS = {
    "stablecoin_dominance_pct",
    "dominance_trend",
    "total_stablecoin_supply_usd",
    "supply_growth_7d",
    "supply_growth_30d",
    "signal",
    "signal_strength",
    "breakdown",
    "historical_dominance",
    "timestamp",
}

DOMINANCE_TRENDS = {"increasing", "decreasing", "stable"}
SIGNALS = {"risk-on", "risk-off", "neutral"}
BREAKDOWN_KEYS = {"USDT", "USDC", "DAI", "other"}
HISTORICAL_ENTRY_KEYS = {"date", "pct"}


def run(coro):
    return asyncio.run(coro)


# ── Fixtures ──────────────────────────────────────────────────────────────────


@pytest.fixture(scope="module")
def result():
    return run(compute_stablecoin_dominance_signal())


@pytest.fixture(scope="module")
def result2():
    return run(compute_stablecoin_dominance_signal())


# ── Return type ───────────────────────────────────────────────────────────────


def test_returns_dict(result):
    assert isinstance(result, dict)


# ── Required keys present ─────────────────────────────────────────────────────


def test_has_stablecoin_dominance_pct(result):
    assert "stablecoin_dominance_pct" in result


def test_has_dominance_trend(result):
    assert "dominance_trend" in result


def test_has_total_stablecoin_supply_usd(result):
    assert "total_stablecoin_supply_usd" in result


def test_has_supply_growth_7d(result):
    assert "supply_growth_7d" in result


def test_has_supply_growth_30d(result):
    assert "supply_growth_30d" in result


def test_has_signal(result):
    assert "signal" in result


def test_has_signal_strength(result):
    assert "signal_strength" in result


def test_has_breakdown(result):
    assert "breakdown" in result


def test_has_historical_dominance(result):
    assert "historical_dominance" in result


def test_has_timestamp(result):
    assert "timestamp" in result


def test_all_required_keys_present(result):
    assert REQUIRED_KEYS.issubset(result.keys())


# ── Type checks ───────────────────────────────────────────────────────────────


def test_stablecoin_dominance_pct_is_float(result):
    assert isinstance(result["stablecoin_dominance_pct"], float)


def test_dominance_trend_is_str(result):
    assert isinstance(result["dominance_trend"], str)


def test_total_stablecoin_supply_usd_is_numeric(result):
    assert isinstance(result["total_stablecoin_supply_usd"], (int, float))


def test_supply_growth_7d_is_float(result):
    assert isinstance(result["supply_growth_7d"], float)


def test_supply_growth_30d_is_float(result):
    assert isinstance(result["supply_growth_30d"], float)


def test_signal_is_str(result):
    assert isinstance(result["signal"], str)


def test_signal_strength_is_float(result):
    assert isinstance(result["signal_strength"], float)


def test_breakdown_is_dict(result):
    assert isinstance(result["breakdown"], dict)


def test_historical_dominance_is_list(result):
    assert isinstance(result["historical_dominance"], list)


def test_timestamp_is_str(result):
    assert isinstance(result["timestamp"], str)


# ── Value ranges ──────────────────────────────────────────────────────────────


def test_stablecoin_dominance_pct_range(result):
    assert 0.0 <= result["stablecoin_dominance_pct"] <= 100.0


def test_total_supply_usd_positive(result):
    assert result["total_stablecoin_supply_usd"] > 0


def test_total_supply_usd_reasonable(result):
    # Stablecoin supply should be in billions range (> $100B)
    assert result["total_stablecoin_supply_usd"] > 100_000_000_000


def test_supply_growth_7d_range(result):
    assert -50.0 <= result["supply_growth_7d"] <= 50.0


def test_supply_growth_30d_range(result):
    assert -50.0 <= result["supply_growth_30d"] <= 50.0


def test_signal_strength_range(result):
    assert 0.0 <= result["signal_strength"] <= 1.0


# ── Enum validation ───────────────────────────────────────────────────────────


def test_dominance_trend_valid(result):
    assert result["dominance_trend"] in DOMINANCE_TRENDS


def test_signal_valid(result):
    assert result["signal"] in SIGNALS


# ── Signal / trend consistency ────────────────────────────────────────────────


def test_signal_risk_on_when_trend_decreasing(result):
    if result["dominance_trend"] == "decreasing":
        assert result["signal"] == "risk-on"


def test_signal_risk_off_when_trend_increasing(result):
    if result["dominance_trend"] == "increasing":
        assert result["signal"] == "risk-off"


def test_signal_neutral_when_trend_stable(result):
    if result["dominance_trend"] == "stable":
        assert result["signal"] == "neutral"


# ── Breakdown structure ───────────────────────────────────────────────────────


def test_breakdown_has_usdt(result):
    assert "USDT" in result["breakdown"]


def test_breakdown_has_usdc(result):
    assert "USDC" in result["breakdown"]


def test_breakdown_has_dai(result):
    assert "DAI" in result["breakdown"]


def test_breakdown_has_other(result):
    assert "other" in result["breakdown"]


def test_breakdown_all_keys_present(result):
    assert BREAKDOWN_KEYS.issubset(result["breakdown"].keys())


def test_breakdown_usdt_is_float(result):
    assert isinstance(result["breakdown"]["USDT"], float)


def test_breakdown_usdc_is_float(result):
    assert isinstance(result["breakdown"]["USDC"], float)


def test_breakdown_dai_is_float(result):
    assert isinstance(result["breakdown"]["DAI"], float)


def test_breakdown_other_is_float(result):
    assert isinstance(result["breakdown"]["other"], float)


def test_breakdown_usdt_range(result):
    assert 0.0 < result["breakdown"]["USDT"] < 1.0


def test_breakdown_usdc_range(result):
    assert 0.0 < result["breakdown"]["USDC"] < 1.0


def test_breakdown_dai_range(result):
    assert 0.0 < result["breakdown"]["DAI"] < 1.0


def test_breakdown_other_range(result):
    assert 0.0 <= result["breakdown"]["other"] < 1.0


def test_breakdown_sum_approx_one(result):
    total = sum(result["breakdown"].values())
    assert abs(total - 1.0) < 0.01


def test_breakdown_usdt_dominant(result):
    # USDT is typically the largest stablecoin
    bd = result["breakdown"]
    assert bd["USDT"] > bd["DAI"]


# ── Historical dominance structure ────────────────────────────────────────────


def test_historical_dominance_non_empty(result):
    assert len(result["historical_dominance"]) > 0


def test_historical_dominance_length_at_least_30(result):
    assert len(result["historical_dominance"]) >= 30


def test_historical_dominance_entry_has_date(result):
    for entry in result["historical_dominance"]:
        assert "date" in entry


def test_historical_dominance_entry_has_pct(result):
    for entry in result["historical_dominance"]:
        assert "pct" in entry


def test_historical_dominance_all_keys(result):
    for entry in result["historical_dominance"]:
        assert HISTORICAL_ENTRY_KEYS.issubset(entry.keys())


def test_historical_dominance_dates_are_strings(result):
    for entry in result["historical_dominance"]:
        assert isinstance(entry["date"], str)


def test_historical_dominance_dates_format(result):
    for entry in result["historical_dominance"]:
        parts = entry["date"].split("-")
        assert len(parts) == 3
        assert len(parts[0]) == 4  # year


def test_historical_dominance_pcts_are_float(result):
    for entry in result["historical_dominance"]:
        assert isinstance(entry["pct"], (float, int))


def test_historical_dominance_pcts_in_range(result):
    for entry in result["historical_dominance"]:
        assert 0.0 <= entry["pct"] <= 100.0


def test_historical_dominance_dates_ascending(result):
    dates = [h["date"] for h in result["historical_dominance"]]
    assert dates == sorted(dates)


def test_historical_dominance_last_entry_date(result):
    # Last entry should be today (2026-03-17 fixed for determinism)
    last_date = result["historical_dominance"][-1]["date"]
    assert last_date == "2026-03-17"


def test_historical_dominance_last_entry_matches_current(result):
    last_pct = result["historical_dominance"][-1]["pct"]
    assert last_pct == result["stablecoin_dominance_pct"]


# ── Timestamp format ──────────────────────────────────────────────────────────


def test_timestamp_is_iso_format(result):
    ts = result["timestamp"]
    assert "T" in ts
    assert ts.endswith("Z") or "+" in ts or len(ts) >= 19


def test_timestamp_has_date_part(result):
    ts = result["timestamp"]
    date_part = ts.split("T")[0]
    parts = date_part.split("-")
    assert len(parts) == 3
    assert len(parts[0]) == 4  # year


# ── Determinism ───────────────────────────────────────────────────────────────


def test_determinism_stablecoin_dominance_pct(result, result2):
    assert result["stablecoin_dominance_pct"] == result2["stablecoin_dominance_pct"]


def test_determinism_dominance_trend(result, result2):
    assert result["dominance_trend"] == result2["dominance_trend"]


def test_determinism_total_supply_usd(result, result2):
    assert (
        result["total_stablecoin_supply_usd"] == result2["total_stablecoin_supply_usd"]
    )


def test_determinism_supply_growth_7d(result, result2):
    assert result["supply_growth_7d"] == result2["supply_growth_7d"]


def test_determinism_supply_growth_30d(result, result2):
    assert result["supply_growth_30d"] == result2["supply_growth_30d"]


def test_determinism_signal(result, result2):
    assert result["signal"] == result2["signal"]


def test_determinism_signal_strength(result, result2):
    assert result["signal_strength"] == result2["signal_strength"]


def test_determinism_breakdown_usdt(result, result2):
    assert result["breakdown"]["USDT"] == result2["breakdown"]["USDT"]


def test_determinism_breakdown_usdc(result, result2):
    assert result["breakdown"]["USDC"] == result2["breakdown"]["USDC"]


def test_determinism_historical_dominance_dates(result, result2):
    dates1 = [h["date"] for h in result["historical_dominance"]]
    dates2 = [h["date"] for h in result2["historical_dominance"]]
    assert dates1 == dates2


def test_determinism_historical_dominance_pcts(result, result2):
    pcts1 = [h["pct"] for h in result["historical_dominance"]]
    pcts2 = [h["pct"] for h in result2["historical_dominance"]]
    assert pcts1 == pcts2
