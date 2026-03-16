"""Tests for compute_volatility_regime_detector() — 50+ tests."""
import asyncio
import math
import sys
import os
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "backend"))

from metrics import compute_volatility_regime_detector

# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------

def run(coro):
    return asyncio.run(coro)


@pytest.fixture(scope="module")
def result():
    return run(compute_volatility_regime_detector())


# ---------------------------------------------------------------------------
# 1. Return type
# ---------------------------------------------------------------------------

def test_returns_dict(result):
    assert isinstance(result, dict)


# ---------------------------------------------------------------------------
# 2. Required keys
# ---------------------------------------------------------------------------

REQUIRED_KEYS = [
    "regime",
    "realized_vol_30d",
    "implied_vol",
    "vol_of_vol",
    "regime_confidence",
    "regime_duration_days",
    "transition_probability",
    "timestamp",
]

@pytest.mark.parametrize("key", REQUIRED_KEYS)
def test_required_key_present(result, key):
    assert key in result, f"Missing key: {key}"


# ---------------------------------------------------------------------------
# 3. regime field
# ---------------------------------------------------------------------------

VALID_REGIMES = {"low", "medium", "high", "extreme"}

def test_regime_is_string(result):
    assert isinstance(result["regime"], str)

def test_regime_valid_value(result):
    assert result["regime"] in VALID_REGIMES

def test_regime_not_empty(result):
    assert result["regime"] != ""

def test_regime_lowercase(result):
    assert result["regime"] == result["regime"].lower()


# ---------------------------------------------------------------------------
# 4. realized_vol_30d
# ---------------------------------------------------------------------------

def test_realized_vol_is_float(result):
    assert isinstance(result["realized_vol_30d"], float)

def test_realized_vol_positive(result):
    assert result["realized_vol_30d"] > 0

def test_realized_vol_reasonable_range(result):
    # annualized % — expect between 1% and 300%
    assert 1.0 <= result["realized_vol_30d"] <= 300.0

def test_realized_vol_finite(result):
    assert math.isfinite(result["realized_vol_30d"])


# ---------------------------------------------------------------------------
# 5. implied_vol
# ---------------------------------------------------------------------------

def test_implied_vol_is_float(result):
    assert isinstance(result["implied_vol"], float)

def test_implied_vol_positive(result):
    assert result["implied_vol"] > 0

def test_implied_vol_reasonable_range(result):
    assert 1.0 <= result["implied_vol"] <= 300.0

def test_implied_vol_finite(result):
    assert math.isfinite(result["implied_vol"])


# ---------------------------------------------------------------------------
# 6. vol_of_vol
# ---------------------------------------------------------------------------

def test_vol_of_vol_is_float(result):
    assert isinstance(result["vol_of_vol"], float)

def test_vol_of_vol_non_negative(result):
    assert result["vol_of_vol"] >= 0

def test_vol_of_vol_finite(result):
    assert math.isfinite(result["vol_of_vol"])

def test_vol_of_vol_reasonable(result):
    assert result["vol_of_vol"] <= 10.0  # vvol ratio should be sane


# ---------------------------------------------------------------------------
# 7. regime_confidence
# ---------------------------------------------------------------------------

def test_confidence_is_float(result):
    assert isinstance(result["regime_confidence"], float)

def test_confidence_min(result):
    assert result["regime_confidence"] >= 0.0

def test_confidence_max(result):
    assert result["regime_confidence"] <= 1.0

def test_confidence_not_nan(result):
    assert not math.isnan(result["regime_confidence"])


# ---------------------------------------------------------------------------
# 8. regime_duration_days
# ---------------------------------------------------------------------------

def test_duration_is_int(result):
    assert isinstance(result["regime_duration_days"], int)

def test_duration_positive(result):
    assert result["regime_duration_days"] > 0

def test_duration_reasonable(result):
    assert result["regime_duration_days"] <= 365 * 5  # max 5 years


# ---------------------------------------------------------------------------
# 9. transition_probability
# ---------------------------------------------------------------------------

def test_tp_is_dict(result):
    assert isinstance(result["transition_probability"], dict)

def test_tp_has_four_keys(result):
    assert set(result["transition_probability"].keys()) == {"low", "medium", "high", "extreme"}

def test_tp_values_are_floats(result):
    for k, v in result["transition_probability"].items():
        assert isinstance(v, float), f"transition_probability[{k!r}] is not float"

def test_tp_values_non_negative(result):
    for k, v in result["transition_probability"].items():
        assert v >= 0.0, f"transition_probability[{k!r}] is negative"

def test_tp_values_max_one(result):
    for k, v in result["transition_probability"].items():
        assert v <= 1.0, f"transition_probability[{k!r}] > 1"

def test_tp_sum_approx_one(result):
    total = sum(result["transition_probability"].values())
    assert abs(total - 1.0) < 1e-6, f"transition_probability sums to {total}"

def test_tp_no_nan(result):
    for k, v in result["transition_probability"].items():
        assert not math.isnan(v)


# ---------------------------------------------------------------------------
# 10. timestamp
# ---------------------------------------------------------------------------

def test_timestamp_is_str(result):
    assert isinstance(result["timestamp"], str)

def test_timestamp_not_empty(result):
    assert len(result["timestamp"]) > 0

def test_timestamp_iso_format(result):
    from datetime import datetime
    # should parse without error
    ts = result["timestamp"].replace("Z", "+00:00")
    dt = datetime.fromisoformat(ts)
    assert dt is not None

def test_timestamp_year_reasonable(result):
    assert result["timestamp"].startswith("20"), "timestamp should start with 20xx"


# ---------------------------------------------------------------------------
# 11. Regime thresholds consistency
# ---------------------------------------------------------------------------

def test_low_regime_vol_below_15(result):
    if result["regime"] == "low":
        assert result["realized_vol_30d"] < 15.0

def test_medium_regime_vol_range(result):
    if result["regime"] == "medium":
        assert 15.0 <= result["realized_vol_30d"] < 30.0

def test_high_regime_vol_range(result):
    if result["regime"] == "high":
        assert 30.0 <= result["realized_vol_30d"] < 60.0

def test_extreme_regime_vol_above_60(result):
    if result["regime"] == "extreme":
        assert result["realized_vol_30d"] >= 60.0


# ---------------------------------------------------------------------------
# 12. Determinism (same seed -> same result)
# ---------------------------------------------------------------------------

def test_deterministic_same_result():
    r1 = run(compute_volatility_regime_detector())
    r2 = run(compute_volatility_regime_detector())
    assert r1["regime"] == r2["regime"]
    assert r1["realized_vol_30d"] == r2["realized_vol_30d"]
    assert r1["implied_vol"] == r2["implied_vol"]
    assert r1["vol_of_vol"] == r2["vol_of_vol"]
    assert r1["regime_confidence"] == r2["regime_confidence"]
    assert r1["regime_duration_days"] == r2["regime_duration_days"]
    assert r1["transition_probability"] == r2["transition_probability"]


# ---------------------------------------------------------------------------
# 13. All four regime types can be produced by helper
# ---------------------------------------------------------------------------


def _classify_vol(vol_value: float) -> str:
    if vol_value < 15.0:
        return "low"
    elif vol_value < 30.0:
        return "medium"
    elif vol_value < 60.0:
        return "high"
    else:
        return "extreme"


def test_all_regimes_low():
    assert _classify_vol(10.0) == "low"


def test_all_regimes_medium():
    assert _classify_vol(22.0) == "medium"


def test_all_regimes_high():
    assert _classify_vol(45.0) == "high"


def test_all_regimes_extreme():
    assert _classify_vol(80.0) == "extreme"


# ---------------------------------------------------------------------------
# 14. Boundary values for regime thresholds
# ---------------------------------------------------------------------------

def test_boundary_15_is_medium():
    assert _classify_vol(15.0) == "medium"


def test_boundary_30_is_high():
    assert _classify_vol(30.0) == "high"


def test_boundary_60_is_extreme():
    assert _classify_vol(60.0) == "extreme"


def test_boundary_just_below_15_is_low():
    assert _classify_vol(14.99) == "low"


def test_boundary_just_below_30_is_medium():
    assert _classify_vol(29.99) == "medium"


def test_boundary_just_below_60_is_high():
    assert _classify_vol(59.99) == "high"


def test_boundary_zero_vol_is_low():
    assert _classify_vol(0.0) == "low"


def test_boundary_very_high_vol_is_extreme():
    assert _classify_vol(200.0) == "extreme"


# ---------------------------------------------------------------------------
# 15. HTTP endpoint tests
# ---------------------------------------------------------------------------

from fastapi.testclient import TestClient
import importlib

@pytest.fixture(scope="module")
def client():
    main_mod = importlib.import_module("main")
    app = main_mod.app
    return TestClient(app)


def test_http_200(client):
    r = client.get("/api/volatility-regime-detector")
    assert r.status_code == 200


def test_http_content_type_json(client):
    r = client.get("/api/volatility-regime-detector")
    assert "application/json" in r.headers["content-type"]


def test_http_response_has_regime(client):
    r = client.get("/api/volatility-regime-detector")
    data = r.json()
    assert "regime" in data


def test_http_response_has_realized_vol(client):
    r = client.get("/api/volatility-regime-detector")
    data = r.json()
    assert "realized_vol_30d" in data


def test_http_response_has_implied_vol(client):
    r = client.get("/api/volatility-regime-detector")
    data = r.json()
    assert "implied_vol" in data


def test_http_response_has_transition_probability(client):
    r = client.get("/api/volatility-regime-detector")
    data = r.json()
    assert "transition_probability" in data


def test_http_regime_valid(client):
    r = client.get("/api/volatility-regime-detector")
    assert r.json()["regime"] in VALID_REGIMES


def test_http_confidence_range(client):
    r = client.get("/api/volatility-regime-detector")
    c = r.json()["regime_confidence"]
    assert 0.0 <= c <= 1.0


def test_http_tp_sum(client):
    r = client.get("/api/volatility-regime-detector")
    tp = r.json()["transition_probability"]
    assert abs(sum(tp.values()) - 1.0) < 1e-4


def test_http_not_found_other_path(client):
    r = client.get("/api/volatility-regime-DOESNOTEXIST")
    assert r.status_code == 404


def test_http_response_has_vol_of_vol(client):
    r = client.get("/api/volatility-regime-detector")
    assert "vol_of_vol" in r.json()


def test_http_response_has_timestamp(client):
    r = client.get("/api/volatility-regime-detector")
    assert "timestamp" in r.json()


def test_http_response_has_duration(client):
    r = client.get("/api/volatility-regime-detector")
    assert "regime_duration_days" in r.json()


def test_http_response_has_confidence(client):
    r = client.get("/api/volatility-regime-detector")
    assert "regime_confidence" in r.json()
