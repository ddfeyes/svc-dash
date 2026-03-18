"""Tests for compute_volatility_regime_forecast (Wave 26, Issue 158).

≥60 tests covering response shape, field types/ranges, business-logic invariants,
and deterministic reproducibility (seed 20260328).
"""

import asyncio
import re
import pytest
from metrics import compute_volatility_regime_forecast

# ── Helpers ───────────────────────────────────────────────────────────────────

_VALID_REGIMES = {"low_vol", "normal", "transitioning", "high_vol"}


def run(coro):
    return asyncio.run(coro)


@pytest.fixture(scope="module")
def result():
    return run(compute_volatility_regime_forecast())


@pytest.fixture(scope="module")
def result2():
    return run(compute_volatility_regime_forecast())


# ── Return type ───────────────────────────────────────────────────────────────


def test_returns_dict(result):
    assert isinstance(result, dict)


# ── Top-level keys ────────────────────────────────────────────────────────────


def test_has_current_regime(result):
    assert "current_regime" in result


def test_has_forecast_regime_7d(result):
    assert "forecast_regime_7d" in result


def test_has_forecast_regime_30d(result):
    assert "forecast_regime_30d" in result


def test_has_realized_vol_7d(result):
    assert "realized_vol_7d" in result


def test_has_realized_vol_30d(result):
    assert "realized_vol_30d" in result


def test_has_iv_index(result):
    assert "iv_index" in result


def test_has_vol_risk_premium(result):
    assert "vol_risk_premium" in result


def test_has_regime_confidence(result):
    assert "regime_confidence" in result


def test_has_regime_history(result):
    assert "regime_history" in result


def test_has_vol_compression_ratio(result):
    assert "vol_compression_ratio" in result


def test_has_timestamp(result):
    assert "timestamp" in result


# ── current_regime ────────────────────────────────────────────────────────────


def test_current_regime_is_str(result):
    assert isinstance(result["current_regime"], str)


def test_current_regime_valid_value(result):
    assert result["current_regime"] in _VALID_REGIMES


def test_current_regime_not_empty(result):
    assert len(result["current_regime"]) > 0


# ── forecast_regime_7d ────────────────────────────────────────────────────────


def test_forecast_regime_7d_is_str(result):
    assert isinstance(result["forecast_regime_7d"], str)


def test_forecast_regime_7d_valid_value(result):
    assert result["forecast_regime_7d"] in _VALID_REGIMES


# ── forecast_regime_30d ───────────────────────────────────────────────────────


def test_forecast_regime_30d_is_str(result):
    assert isinstance(result["forecast_regime_30d"], str)


def test_forecast_regime_30d_valid_value(result):
    assert result["forecast_regime_30d"] in _VALID_REGIMES


# ── realized_vol_7d ───────────────────────────────────────────────────────────


def test_realized_vol_7d_is_float(result):
    assert isinstance(result["realized_vol_7d"], float)


def test_realized_vol_7d_positive(result):
    assert result["realized_vol_7d"] > 0.0


def test_realized_vol_7d_range(result):
    assert 0.0 < result["realized_vol_7d"] <= 200.0


# ── realized_vol_30d ──────────────────────────────────────────────────────────


def test_realized_vol_30d_is_float(result):
    assert isinstance(result["realized_vol_30d"], float)


def test_realized_vol_30d_positive(result):
    assert result["realized_vol_30d"] > 0.0


def test_realized_vol_30d_range(result):
    assert 0.0 < result["realized_vol_30d"] <= 200.0


# ── iv_index ──────────────────────────────────────────────────────────────────


def test_iv_index_is_float(result):
    assert isinstance(result["iv_index"], float)


def test_iv_index_positive(result):
    assert result["iv_index"] > 0.0


def test_iv_index_range(result):
    assert 0.0 < result["iv_index"] <= 300.0


# ── vol_risk_premium ──────────────────────────────────────────────────────────


def test_vol_risk_premium_is_float(result):
    assert isinstance(result["vol_risk_premium"], float)


def test_vol_risk_premium_computed(result):
    """VRP must equal iv_index minus realized_vol_7d (rounded to 1dp)."""
    expected = round(result["iv_index"] - result["realized_vol_7d"], 1)
    assert abs(result["vol_risk_premium"] - expected) < 0.05


# ── regime_confidence ─────────────────────────────────────────────────────────


def test_regime_confidence_is_float(result):
    assert isinstance(result["regime_confidence"], float)


def test_regime_confidence_in_0_1_range(result):
    assert 0.0 <= result["regime_confidence"] <= 1.0


def test_regime_confidence_above_0_5(result):
    assert result["regime_confidence"] >= 0.5


# ── vol_compression_ratio ─────────────────────────────────────────────────────


def test_vol_compression_ratio_is_float(result):
    assert isinstance(result["vol_compression_ratio"], float)


def test_vol_compression_ratio_positive(result):
    assert result["vol_compression_ratio"] > 0.0


def test_vol_compression_ratio_computed(result):
    """VCR must equal realized_vol_7d / realized_vol_30d (rounded to 2dp)."""
    expected = round(
        result["realized_vol_7d"] / max(result["realized_vol_30d"], 0.01), 2
    )
    assert abs(result["vol_compression_ratio"] - expected) < 0.01


# ── timestamp ─────────────────────────────────────────────────────────────────


def test_timestamp_is_str(result):
    assert isinstance(result["timestamp"], str)


def test_timestamp_not_empty(result):
    assert len(result["timestamp"]) > 0


def test_timestamp_format(result):
    assert re.match(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z", result["timestamp"])


# ── regime_history ────────────────────────────────────────────────────────────


def test_regime_history_is_list(result):
    assert isinstance(result["regime_history"], list)


def test_regime_history_length_30(result):
    assert len(result["regime_history"]) == 30


def test_regime_history_entries_are_dicts(result):
    for entry in result["regime_history"]:
        assert isinstance(entry, dict)


def test_regime_history_entry_has_date(result):
    for entry in result["regime_history"]:
        assert "date" in entry


def test_regime_history_entry_has_regime(result):
    for entry in result["regime_history"]:
        assert "regime" in entry


def test_regime_history_entry_has_rv7d(result):
    for entry in result["regime_history"]:
        assert "rv7d" in entry


def test_regime_history_date_is_str(result):
    for entry in result["regime_history"]:
        assert isinstance(entry["date"], str)


def test_regime_history_date_format(result):
    for entry in result["regime_history"]:
        assert re.match(r"\d{4}-\d{2}-\d{2}$", entry["date"])


def test_regime_history_regime_is_str(result):
    for entry in result["regime_history"]:
        assert isinstance(entry["regime"], str)


def test_regime_history_regime_valid(result):
    for entry in result["regime_history"]:
        assert entry["regime"] in _VALID_REGIMES


def test_regime_history_rv7d_is_float(result):
    for entry in result["regime_history"]:
        assert isinstance(entry["rv7d"], float)


def test_regime_history_rv7d_positive(result):
    for entry in result["regime_history"]:
        assert entry["rv7d"] > 0.0


def test_regime_history_dates_ascending(result):
    dates = [e["date"] for e in result["regime_history"]]
    assert dates == sorted(dates)


def test_regime_history_entry_has_3_keys(result):
    for entry in result["regime_history"]:
        assert len(entry) == 3


# ── Determinism ───────────────────────────────────────────────────────────────


def test_deterministic_current_regime(result, result2):
    assert result["current_regime"] == result2["current_regime"]


def test_deterministic_forecast_7d(result, result2):
    assert result["forecast_regime_7d"] == result2["forecast_regime_7d"]


def test_deterministic_forecast_30d(result, result2):
    assert result["forecast_regime_30d"] == result2["forecast_regime_30d"]


def test_deterministic_realized_vol_7d(result, result2):
    assert result["realized_vol_7d"] == result2["realized_vol_7d"]


def test_deterministic_realized_vol_30d(result, result2):
    assert result["realized_vol_30d"] == result2["realized_vol_30d"]


def test_deterministic_iv_index(result, result2):
    assert result["iv_index"] == result2["iv_index"]


def test_deterministic_vol_risk_premium(result, result2):
    assert result["vol_risk_premium"] == result2["vol_risk_premium"]


def test_deterministic_regime_confidence(result, result2):
    assert result["regime_confidence"] == result2["regime_confidence"]


def test_deterministic_vol_compression_ratio(result, result2):
    assert result["vol_compression_ratio"] == result2["vol_compression_ratio"]


def test_deterministic_regime_history_rv_values(result, result2):
    rv1 = [e["rv7d"] for e in result["regime_history"]]
    rv2 = [e["rv7d"] for e in result2["regime_history"]]
    assert rv1 == rv2


def test_deterministic_regime_history_regimes(result, result2):
    r1 = [e["regime"] for e in result["regime_history"]]
    r2 = [e["regime"] for e in result2["regime_history"]]
    assert r1 == r2
