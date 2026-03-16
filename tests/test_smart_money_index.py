"""Tests for Smart Money Index (feat/smart-money-index, issue #96)."""

import asyncio
import pytest
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "backend"))

from metrics import compute_smart_money_index


def run(coro):
    return asyncio.get_event_loop().run_until_complete(coro)


@pytest.fixture(scope="module")
def result():
    return run(compute_smart_money_index())


# ── 1. Return type ────────────────────────────────────────────────────────────

def test_returns_dict(result):
    assert isinstance(result, dict)


def test_not_none(result):
    assert result is not None


# ── 2. Top-level keys ────────────────────────────────────────────────────────

def test_has_smi_score(result):
    assert "smi_score" in result


def test_has_institutional_flow(result):
    assert "institutional_flow" in result


def test_has_retail_flow(result):
    assert "retail_flow" in result


def test_has_divergence(result):
    assert "divergence" in result


def test_has_signal(result):
    assert "signal" in result


def test_has_components(result):
    assert "components" in result


def test_has_timestamp(result):
    assert "timestamp" in result


def test_exactly_seven_keys(result):
    assert len(result) == 7


# ── 3. smi_score ─────────────────────────────────────────────────────────────

def test_smi_score_is_float(result):
    assert isinstance(result["smi_score"], float)


def test_smi_score_ge_minus_one(result):
    assert result["smi_score"] >= -1.0


def test_smi_score_le_plus_one(result):
    assert result["smi_score"] <= 1.0


def test_smi_score_not_nan(result):
    import math
    assert not math.isnan(result["smi_score"])


def test_smi_score_not_inf(result):
    import math
    assert math.isfinite(result["smi_score"])


# ── 4. institutional_flow ────────────────────────────────────────────────────

def test_institutional_flow_is_float(result):
    assert isinstance(result["institutional_flow"], float)


def test_institutional_flow_not_nan(result):
    import math
    assert not math.isnan(result["institutional_flow"])


def test_institutional_flow_finite(result):
    import math
    assert math.isfinite(result["institutional_flow"])


# ── 5. retail_flow ───────────────────────────────────────────────────────────

def test_retail_flow_is_float(result):
    assert isinstance(result["retail_flow"], float)


def test_retail_flow_not_nan(result):
    import math
    assert not math.isnan(result["retail_flow"])


def test_retail_flow_finite(result):
    import math
    assert math.isfinite(result["retail_flow"])


# ── 6. divergence ────────────────────────────────────────────────────────────

def test_divergence_is_float(result):
    assert isinstance(result["divergence"], float)


def test_divergence_not_nan(result):
    import math
    assert not math.isnan(result["divergence"])


def test_divergence_finite(result):
    import math
    assert math.isfinite(result["divergence"])


def test_divergence_equals_institutional_minus_retail(result):
    expected = round(result["institutional_flow"] - result["retail_flow"], 2)
    assert result["divergence"] == expected


# ── 7. signal ────────────────────────────────────────────────────────────────

def test_signal_is_str(result):
    assert isinstance(result["signal"], str)


def test_signal_valid_value(result):
    assert result["signal"] in ("accumulation", "distribution", "neutral")


def test_signal_accumulation_when_high_smi(result):
    if result["smi_score"] > 0.2:
        assert result["signal"] == "accumulation"


def test_signal_distribution_when_low_smi(result):
    if result["smi_score"] < -0.2:
        assert result["signal"] == "distribution"


def test_signal_neutral_when_mid_smi(result):
    if -0.2 <= result["smi_score"] <= 0.2:
        assert result["signal"] == "neutral"


def test_signal_not_empty(result):
    assert len(result["signal"]) > 0


# ── 8. components dict ───────────────────────────────────────────────────────

def test_components_is_dict(result):
    assert isinstance(result["components"], dict)


def test_components_has_block_ratio(result):
    assert "block_ratio" in result["components"]


def test_components_has_oi_skew(result):
    assert "oi_skew" in result["components"]


def test_components_has_futures_basis(result):
    assert "futures_basis" in result["components"]


def test_components_has_whale_accumulation(result):
    assert "whale_accumulation" in result["components"]


def test_components_exactly_four_keys(result):
    assert len(result["components"]) == 4


def test_block_ratio_is_float(result):
    assert isinstance(result["components"]["block_ratio"], float)


def test_oi_skew_is_float(result):
    assert isinstance(result["components"]["oi_skew"], float)


def test_futures_basis_is_float(result):
    assert isinstance(result["components"]["futures_basis"], float)


def test_whale_accumulation_is_float(result):
    assert isinstance(result["components"]["whale_accumulation"], float)


def test_block_ratio_non_negative(result):
    assert result["components"]["block_ratio"] >= 0.0


def test_block_ratio_le_one(result):
    assert result["components"]["block_ratio"] <= 1.0


def test_whale_accumulation_finite(result):
    import math
    assert math.isfinite(result["components"]["whale_accumulation"])


def test_oi_skew_finite(result):
    import math
    assert math.isfinite(result["components"]["oi_skew"])


def test_futures_basis_finite(result):
    import math
    assert math.isfinite(result["components"]["futures_basis"])


# ── 9. timestamp ─────────────────────────────────────────────────────────────

def test_timestamp_is_str(result):
    assert isinstance(result["timestamp"], str)


def test_timestamp_not_empty(result):
    assert len(result["timestamp"]) > 0


def test_timestamp_iso_format(result):
    from datetime import datetime
    ts = result["timestamp"]
    ts_clean = ts.replace("Z", "+00:00")
    try:
        datetime.fromisoformat(ts_clean)
        parsed = True
    except ValueError:
        parsed = False
    assert parsed, f"timestamp '{ts}' is not ISO 8601"


def test_timestamp_contains_T(result):
    assert "T" in result["timestamp"]


# ── 10. Determinism ──────────────────────────────────────────────────────────

def test_deterministic_smi_score():
    r1 = run(compute_smart_money_index())
    r2 = run(compute_smart_money_index())
    assert r1["smi_score"] == r2["smi_score"]


def test_deterministic_institutional_flow():
    r1 = run(compute_smart_money_index())
    r2 = run(compute_smart_money_index())
    assert r1["institutional_flow"] == r2["institutional_flow"]


def test_deterministic_retail_flow():
    r1 = run(compute_smart_money_index())
    r2 = run(compute_smart_money_index())
    assert r1["retail_flow"] == r2["retail_flow"]


def test_deterministic_signal():
    r1 = run(compute_smart_money_index())
    r2 = run(compute_smart_money_index())
    assert r1["signal"] == r2["signal"]


def test_deterministic_components():
    r1 = run(compute_smart_money_index())
    r2 = run(compute_smart_money_index())
    assert r1["components"] == r2["components"]


def test_deterministic_divergence():
    r1 = run(compute_smart_money_index())
    r2 = run(compute_smart_money_index())
    assert r1["divergence"] == r2["divergence"]


# ── 11. HTTP endpoint ────────────────────────────────────────────────────────

@pytest.fixture(scope="module")
def client():
    from fastapi import FastAPI
    from fastapi.testclient import TestClient
    from api import router

    mini_app = FastAPI()
    mini_app.include_router(router)
    return TestClient(mini_app)


def test_endpoint_status_200(client):
    resp = client.get("/api/smart-money-index")
    assert resp.status_code == 200


def test_endpoint_content_type_json(client):
    resp = client.get("/api/smart-money-index")
    assert "application/json" in resp.headers["content-type"]


def test_endpoint_returns_smi_score(client):
    data = client.get("/api/smart-money-index").json()
    assert "smi_score" in data


def test_endpoint_returns_signal(client):
    data = client.get("/api/smart-money-index").json()
    assert data["signal"] in ("accumulation", "distribution", "neutral")


def test_endpoint_returns_components(client):
    data = client.get("/api/smart-money-index").json()
    assert isinstance(data["components"], dict)
    assert "block_ratio" in data["components"]


def test_endpoint_smi_score_range(client):
    data = client.get("/api/smart-money-index").json()
    assert -1.0 <= data["smi_score"] <= 1.0


def test_endpoint_timestamp_present(client):
    data = client.get("/api/smart-money-index").json()
    assert "timestamp" in data
    assert len(data["timestamp"]) > 0


def test_endpoint_returns_institutional_flow(client):
    data = client.get("/api/smart-money-index").json()
    assert "institutional_flow" in data


def test_endpoint_returns_retail_flow(client):
    data = client.get("/api/smart-money-index").json()
    assert "retail_flow" in data


def test_endpoint_returns_divergence(client):
    data = client.get("/api/smart-money-index").json()
    assert "divergence" in data


def test_endpoint_deterministic(client):
    r1 = client.get("/api/smart-money-index").json()
    r2 = client.get("/api/smart-money-index").json()
    assert r1["smi_score"] == r2["smi_score"]
    assert r1["signal"] == r2["signal"]


def test_endpoint_all_keys(client):
    data = client.get("/api/smart-money-index").json()
    for key in ["smi_score", "institutional_flow", "retail_flow", "divergence", "signal", "components", "timestamp"]:
        assert key in data


def test_endpoint_components_all_subkeys(client):
    data = client.get("/api/smart-money-index").json()
    for key in ["block_ratio", "oi_skew", "futures_basis", "whale_accumulation"]:
        assert key in data["components"]
