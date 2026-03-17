"""Tests for compute_liquidation_cascade_risk() — Wave 26, Task 2.

60+ tests covering all required keys, value ranges, structural invariants,
determinism, trigger zones, leverage concentration, historical cascades,
and structural checks (route, HTML card, JS functions).
"""
import asyncio
import inspect
import os
import re
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from metrics import compute_liquidation_cascade_risk

REQUIRED_KEYS = {
    "cascade_risk_score",
    "risk_level",
    "leverage_concentration",
    "trigger_zones",
    "cascade_threshold_distance_pct",
    "historical_cascades",
    "sentiment_amplifier",
    "timestamp",
}

VALID_RISK_LEVELS = {"low", "moderate", "elevated", "critical"}
TRIGGER_ZONE_KEYS = {"price_level", "estimated_liquidations_usd", "direction"}
HISTORICAL_CASCADE_KEYS = {"date", "drop_pct", "liquidated_usd"}
VALID_DIRECTIONS = {"long", "short"}
LEVERAGE_TIERS = {"2x", "5x", "10x", "20x+"}


def run(coro):
    return asyncio.run(coro)


# ── Fixtures ───────────────────────────────────────────────────────────────────


@pytest.fixture(scope="module")
def result():
    return run(compute_liquidation_cascade_risk())


@pytest.fixture(scope="module")
def result2():
    return run(compute_liquidation_cascade_risk())


# ── Return type ────────────────────────────────────────────────────────────────


def test_returns_dict(result):
    assert isinstance(result, dict)


# ── Required keys ──────────────────────────────────────────────────────────────


def test_has_cascade_risk_score(result):
    assert "cascade_risk_score" in result


def test_has_risk_level(result):
    assert "risk_level" in result


def test_has_leverage_concentration(result):
    assert "leverage_concentration" in result


def test_has_trigger_zones(result):
    assert "trigger_zones" in result


def test_has_cascade_threshold_distance_pct(result):
    assert "cascade_threshold_distance_pct" in result


def test_has_historical_cascades(result):
    assert "historical_cascades" in result


def test_has_sentiment_amplifier(result):
    assert "sentiment_amplifier" in result


def test_has_timestamp(result):
    assert "timestamp" in result


def test_all_required_keys_present(result):
    assert REQUIRED_KEYS.issubset(result.keys())


# ── cascade_risk_score ─────────────────────────────────────────────────────────


def test_cascade_risk_score_is_float(result):
    assert isinstance(result["cascade_risk_score"], float)


def test_cascade_risk_score_gte_0(result):
    assert result["cascade_risk_score"] >= 0.0


def test_cascade_risk_score_lte_100(result):
    assert result["cascade_risk_score"] <= 100.0


def test_cascade_risk_score_positive(result):
    assert result["cascade_risk_score"] > 0.0


# ── risk_level ─────────────────────────────────────────────────────────────────


def test_risk_level_is_str(result):
    assert isinstance(result["risk_level"], str)


def test_risk_level_valid(result):
    assert result["risk_level"] in VALID_RISK_LEVELS


def test_risk_level_consistent_with_score(result):
    score = result["cascade_risk_score"]
    level = result["risk_level"]
    if score >= 75.0:
        assert level == "critical"
    elif score >= 50.0:
        assert level == "elevated"
    elif score >= 25.0:
        assert level == "moderate"
    else:
        assert level == "low"


# ── leverage_concentration ─────────────────────────────────────────────────────


def test_leverage_concentration_is_dict(result):
    assert isinstance(result["leverage_concentration"], dict)


def test_leverage_concentration_has_all_tiers(result):
    assert LEVERAGE_TIERS.issubset(result["leverage_concentration"].keys())


def test_leverage_concentration_2x_is_float(result):
    assert isinstance(result["leverage_concentration"]["2x"], float)


def test_leverage_concentration_5x_is_float(result):
    assert isinstance(result["leverage_concentration"]["5x"], float)


def test_leverage_concentration_10x_is_float(result):
    assert isinstance(result["leverage_concentration"]["10x"], float)


def test_leverage_concentration_20x_is_float(result):
    assert isinstance(result["leverage_concentration"]["20x+"], float)


def test_leverage_concentration_all_non_negative(result):
    for tier, val in result["leverage_concentration"].items():
        assert val >= 0.0, f"Tier {tier} has negative value {val}"


def test_leverage_concentration_all_lte_1(result):
    for tier, val in result["leverage_concentration"].items():
        assert val <= 1.0, f"Tier {tier} has value > 1: {val}"


def test_leverage_concentration_sums_to_1(result):
    total = sum(result["leverage_concentration"].values())
    assert abs(total - 1.0) < 0.01, f"Sum {total} not close to 1.0"


# ── trigger_zones ──────────────────────────────────────────────────────────────


def test_trigger_zones_is_list(result):
    assert isinstance(result["trigger_zones"], list)


def test_trigger_zones_nonempty(result):
    assert len(result["trigger_zones"]) > 0


def test_trigger_zones_has_5_items(result):
    assert len(result["trigger_zones"]) == 5


def test_trigger_zones_are_dicts(result):
    for zone in result["trigger_zones"]:
        assert isinstance(zone, dict)


def test_trigger_zones_have_required_keys(result):
    for zone in result["trigger_zones"]:
        assert TRIGGER_ZONE_KEYS.issubset(zone.keys()), f"Missing keys in {zone}"


def test_trigger_zones_price_level_is_int(result):
    for zone in result["trigger_zones"]:
        assert isinstance(zone["price_level"], int)


def test_trigger_zones_price_level_positive(result):
    for zone in result["trigger_zones"]:
        assert zone["price_level"] > 0


def test_trigger_zones_estimated_liquidations_is_int(result):
    for zone in result["trigger_zones"]:
        assert isinstance(zone["estimated_liquidations_usd"], int)


def test_trigger_zones_estimated_liquidations_positive(result):
    for zone in result["trigger_zones"]:
        assert zone["estimated_liquidations_usd"] > 0


def test_trigger_zones_direction_valid(result):
    for zone in result["trigger_zones"]:
        assert zone["direction"] in VALID_DIRECTIONS


# ── cascade_threshold_distance_pct ────────────────────────────────────────────


def test_cascade_threshold_distance_pct_is_float(result):
    assert isinstance(result["cascade_threshold_distance_pct"], float)


def test_cascade_threshold_distance_pct_non_negative(result):
    assert result["cascade_threshold_distance_pct"] >= 0.0


def test_cascade_threshold_distance_pct_reasonable(result):
    assert result["cascade_threshold_distance_pct"] <= 20.0


# ── historical_cascades ────────────────────────────────────────────────────────


def test_historical_cascades_is_list(result):
    assert isinstance(result["historical_cascades"], list)


def test_historical_cascades_has_10_items(result):
    assert len(result["historical_cascades"]) == 10


def test_historical_cascades_are_dicts(result):
    for c in result["historical_cascades"]:
        assert isinstance(c, dict)


def test_historical_cascades_have_required_keys(result):
    for c in result["historical_cascades"]:
        assert HISTORICAL_CASCADE_KEYS.issubset(c.keys()), f"Missing keys in {c}"


def test_historical_cascades_date_is_str(result):
    for c in result["historical_cascades"]:
        assert isinstance(c["date"], str)


def test_historical_cascades_date_nonempty(result):
    for c in result["historical_cascades"]:
        assert len(c["date"]) > 0


def test_historical_cascades_date_iso_format(result):
    iso_re = re.compile(r"^\d{4}-\d{2}-\d{2}$")
    for c in result["historical_cascades"]:
        assert iso_re.match(c["date"]), f"Date not ISO: {c['date']}"


def test_historical_cascades_drop_pct_is_float(result):
    for c in result["historical_cascades"]:
        assert isinstance(c["drop_pct"], float)


def test_historical_cascades_drop_pct_positive(result):
    for c in result["historical_cascades"]:
        assert c["drop_pct"] > 0.0


def test_historical_cascades_drop_pct_reasonable(result):
    for c in result["historical_cascades"]:
        assert 5.0 <= c["drop_pct"] <= 35.0


def test_historical_cascades_liquidated_usd_is_int(result):
    for c in result["historical_cascades"]:
        assert isinstance(c["liquidated_usd"], int)


def test_historical_cascades_liquidated_usd_positive(result):
    for c in result["historical_cascades"]:
        assert c["liquidated_usd"] > 0


# ── sentiment_amplifier ────────────────────────────────────────────────────────


def test_sentiment_amplifier_is_float(result):
    assert isinstance(result["sentiment_amplifier"], float)


def test_sentiment_amplifier_positive(result):
    assert result["sentiment_amplifier"] > 0.0


def test_sentiment_amplifier_in_range(result):
    assert 0.5 <= result["sentiment_amplifier"] <= 3.0


# ── timestamp ─────────────────────────────────────────────────────────────────


def test_timestamp_is_str(result):
    assert isinstance(result["timestamp"], str)


def test_timestamp_nonempty(result):
    assert len(result["timestamp"]) > 0


def test_timestamp_iso_format(result):
    ts = result["timestamp"]
    assert "T" in ts, f"Timestamp missing 'T': {ts}"


# ── Determinism ────────────────────────────────────────────────────────────────


def test_determinism_cascade_risk_score(result, result2):
    assert result["cascade_risk_score"] == result2["cascade_risk_score"]


def test_determinism_risk_level(result, result2):
    assert result["risk_level"] == result2["risk_level"]


def test_determinism_leverage_concentration(result, result2):
    assert result["leverage_concentration"] == result2["leverage_concentration"]


def test_determinism_trigger_zones(result, result2):
    assert result["trigger_zones"] == result2["trigger_zones"]


def test_determinism_cascade_threshold_distance(result, result2):
    assert result["cascade_threshold_distance_pct"] == result2["cascade_threshold_distance_pct"]


def test_determinism_historical_cascades_dates(result, result2):
    dates1 = [c["date"] for c in result["historical_cascades"]]
    dates2 = [c["date"] for c in result2["historical_cascades"]]
    assert dates1 == dates2


def test_determinism_historical_cascades_drop_pct(result, result2):
    drops1 = [c["drop_pct"] for c in result["historical_cascades"]]
    drops2 = [c["drop_pct"] for c in result2["historical_cascades"]]
    assert drops1 == drops2


def test_determinism_sentiment_amplifier(result, result2):
    assert result["sentiment_amplifier"] == result2["sentiment_amplifier"]


# ── Async / function shape ─────────────────────────────────────────────────────


def test_function_is_async():
    assert inspect.iscoroutinefunction(compute_liquidation_cascade_risk)


def test_asyncio_run_works():
    data = asyncio.run(compute_liquidation_cascade_risk())
    assert isinstance(data, dict)


# ── Structural / integration checks ───────────────────────────────────────────


def test_route_registered_in_api_py():
    api_path = os.path.join(os.path.dirname(__file__), "..", "api.py")
    content = open(api_path).read()
    assert "/liquidation-cascade-risk" in content, "/liquidation-cascade-risk route missing from api.py"


def test_html_card_exists():
    html_path = os.path.join(
        os.path.dirname(__file__), "..", "..", "frontend", "index.html"
    )
    content = open(html_path).read()
    assert "liquidation-cascade-risk-card" in content, "liquidation-cascade-risk-card missing from index.html"


def test_js_render_function_exists():
    js_path = os.path.join(
        os.path.dirname(__file__), "..", "..", "frontend", "app.js"
    )
    content = open(js_path).read()
    assert "renderLiquidationCascadeRisk" in content, "renderLiquidationCascadeRisk missing from app.js"


def test_js_fetch_function_exists():
    js_path = os.path.join(
        os.path.dirname(__file__), "..", "..", "frontend", "app.js"
    )
    content = open(js_path).read()
    assert "fetchLiquidationCascadeRisk" in content, "fetchLiquidationCascadeRisk missing from app.js"


def test_js_api_call_exists():
    js_path = os.path.join(
        os.path.dirname(__file__), "..", "..", "frontend", "app.js"
    )
    content = open(js_path).read()
    assert "/liquidation-cascade-risk" in content, "/liquidation-cascade-risk call missing from app.js"


def test_metrics_import_in_api_py():
    api_path = os.path.join(os.path.dirname(__file__), "..", "api.py")
    content = open(api_path).read()
    assert "compute_liquidation_cascade_risk" in content, "compute_liquidation_cascade_risk not imported in api.py"
