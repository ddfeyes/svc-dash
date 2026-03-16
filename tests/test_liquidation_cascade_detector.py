"""
Unit / smoke tests for /api/liquidation-cascade-detector.

Liquidation cascade detector — simulates real-time cascade detection
for BTC/ETH/SOL/BNB with probabilistic modeling.

Fields returned:
  - cascade_probability: float [0, 1]
  - time_to_cascade_minutes: float > 0
  - support_levels: list of floats (price levels)
  - total_liquidated_usd: float >= 0
  - cascade_chain: list of dicts with {asset, amount, time}
  - liq_velocity: float >= 0 (USD/minute)
  - exchanges: list of str
  - regime: str in {"calm", "building", "cascade", "peak"}

Uses random.seed(20260316) for deterministic data.
"""

import os
import sys
import asyncio
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "backend"))

_ROOT = os.path.join(os.path.dirname(__file__), "..")

VALID_REGIMES = {"calm", "building", "cascade", "peak"}
VALID_ASSETS = {"BTC", "ETH", "SOL", "BNB"}


def _html() -> str:
    with open(os.path.join(_ROOT, "frontend", "index.html"), encoding="utf-8") as f:
        return f.read()


def _js() -> str:
    with open(os.path.join(_ROOT, "frontend", "app.js"), encoding="utf-8") as f:
        return f.read()


def _run(coro):
    return asyncio.get_event_loop().run_until_complete(coro)


@pytest.fixture(scope="module")
def result():
    from metrics import compute_liquidation_cascade_detector
    return _run(compute_liquidation_cascade_detector())


@pytest.fixture(scope="module")
def result2():
    """Second call – to verify determinism."""
    from metrics import compute_liquidation_cascade_detector
    return _run(compute_liquidation_cascade_detector())


# ── Field presence ────────────────────────────────────────────────────────────

class TestFieldPresence:
    def test_has_cascade_probability(self, result):
        assert "cascade_probability" in result

    def test_has_time_to_cascade_minutes(self, result):
        assert "time_to_cascade_minutes" in result

    def test_has_support_levels(self, result):
        assert "support_levels" in result

    def test_has_total_liquidated_usd(self, result):
        assert "total_liquidated_usd" in result

    def test_has_cascade_chain(self, result):
        assert "cascade_chain" in result

    def test_has_liq_velocity(self, result):
        assert "liq_velocity" in result

    def test_has_exchanges(self, result):
        assert "exchanges" in result

    def test_has_regime(self, result):
        assert "regime" in result

    def test_all_required_fields_present(self, result):
        required = {
            "cascade_probability", "time_to_cascade_minutes",
            "support_levels", "total_liquidated_usd",
            "cascade_chain", "liq_velocity", "exchanges", "regime",
        }
        assert required.issubset(result.keys())


# ── Type correctness ──────────────────────────────────────────────────────────

class TestTypes:
    def test_cascade_probability_is_float(self, result):
        assert isinstance(result["cascade_probability"], (int, float))

    def test_time_to_cascade_minutes_is_float(self, result):
        assert isinstance(result["time_to_cascade_minutes"], (int, float))

    def test_support_levels_is_list(self, result):
        assert isinstance(result["support_levels"], list)

    def test_total_liquidated_usd_is_numeric(self, result):
        assert isinstance(result["total_liquidated_usd"], (int, float))

    def test_cascade_chain_is_list(self, result):
        assert isinstance(result["cascade_chain"], list)

    def test_liq_velocity_is_numeric(self, result):
        assert isinstance(result["liq_velocity"], (int, float))

    def test_exchanges_is_list(self, result):
        assert isinstance(result["exchanges"], list)

    def test_regime_is_str(self, result):
        assert isinstance(result["regime"], str)

    def test_support_levels_contain_numerics(self, result):
        for lvl in result["support_levels"]:
            assert isinstance(lvl, (int, float))

    def test_exchanges_contain_strings(self, result):
        for ex in result["exchanges"]:
            assert isinstance(ex, str)


# ── Value ranges ──────────────────────────────────────────────────────────────

class TestValueRanges:
    def test_cascade_probability_in_0_1(self, result):
        p = result["cascade_probability"]
        assert 0.0 <= p <= 1.0

    def test_cascade_probability_not_negative(self, result):
        assert result["cascade_probability"] >= 0.0

    def test_cascade_probability_not_above_1(self, result):
        assert result["cascade_probability"] <= 1.0

    def test_time_to_cascade_minutes_positive(self, result):
        assert result["time_to_cascade_minutes"] > 0

    def test_total_liquidated_usd_non_negative(self, result):
        assert result["total_liquidated_usd"] >= 0

    def test_liq_velocity_non_negative(self, result):
        assert result["liq_velocity"] >= 0

    def test_support_levels_non_empty(self, result):
        assert len(result["support_levels"]) > 0

    def test_support_levels_all_positive(self, result):
        for lvl in result["support_levels"]:
            assert lvl > 0, f"Support level {lvl} should be positive"

    def test_exchanges_non_empty(self, result):
        assert len(result["exchanges"]) > 0

    def test_cascade_chain_non_empty(self, result):
        assert len(result["cascade_chain"]) > 0


# ── Regime validation ─────────────────────────────────────────────────────────

class TestRegime:
    def test_regime_valid_value(self, result):
        assert result["regime"] in VALID_REGIMES

    def test_regime_not_empty_string(self, result):
        assert result["regime"] != ""

    def test_regime_calm_in_valid_set(self):
        assert "calm" in VALID_REGIMES

    def test_regime_building_in_valid_set(self):
        assert "building" in VALID_REGIMES

    def test_regime_cascade_in_valid_set(self):
        assert "cascade" in VALID_REGIMES

    def test_regime_peak_in_valid_set(self):
        assert "peak" in VALID_REGIMES

    def test_regime_is_one_of_four(self, result):
        assert result["regime"] in {"calm", "building", "cascade", "peak"}


# ── Cascade chain structure ───────────────────────────────────────────────────

class TestCascadeChain:
    def test_cascade_chain_entries_have_asset(self, result):
        for entry in result["cascade_chain"]:
            assert "asset" in entry

    def test_cascade_chain_entries_have_amount(self, result):
        for entry in result["cascade_chain"]:
            assert "amount" in entry

    def test_cascade_chain_entries_have_time(self, result):
        for entry in result["cascade_chain"]:
            assert "time" in entry

    def test_cascade_chain_assets_are_valid(self, result):
        for entry in result["cascade_chain"]:
            assert entry["asset"] in VALID_ASSETS, f"Unknown asset: {entry['asset']}"

    def test_cascade_chain_amounts_positive(self, result):
        for entry in result["cascade_chain"]:
            assert entry["amount"] > 0, f"Amount should be positive: {entry['amount']}"

    def test_cascade_chain_time_numeric(self, result):
        for entry in result["cascade_chain"]:
            assert isinstance(entry["time"], (int, float))

    def test_cascade_chain_assets_are_strings(self, result):
        for entry in result["cascade_chain"]:
            assert isinstance(entry["asset"], str)

    def test_cascade_chain_amounts_are_numeric(self, result):
        for entry in result["cascade_chain"]:
            assert isinstance(entry["amount"], (int, float))

    def test_cascade_chain_no_missing_keys(self, result):
        required_keys = {"asset", "amount", "time"}
        for entry in result["cascade_chain"]:
            assert required_keys.issubset(entry.keys())


# ── Determinism ───────────────────────────────────────────────────────────────

class TestDeterminism:
    def test_cascade_probability_deterministic(self, result, result2):
        assert result["cascade_probability"] == result2["cascade_probability"]

    def test_time_to_cascade_deterministic(self, result, result2):
        assert result["time_to_cascade_minutes"] == result2["time_to_cascade_minutes"]

    def test_regime_deterministic(self, result, result2):
        assert result["regime"] == result2["regime"]

    def test_total_liquidated_deterministic(self, result, result2):
        assert result["total_liquidated_usd"] == result2["total_liquidated_usd"]

    def test_support_levels_deterministic(self, result, result2):
        assert result["support_levels"] == result2["support_levels"]

    def test_exchanges_deterministic(self, result, result2):
        assert sorted(result["exchanges"]) == sorted(result2["exchanges"])

    def test_liq_velocity_deterministic(self, result, result2):
        assert result["liq_velocity"] == result2["liq_velocity"]


# ── HTTP endpoint (structural) ────────────────────────────────────────────────

class TestHTTPEndpoint:
    def test_route_registered_in_api(self):
        api_path = os.path.join(_ROOT, "backend", "api.py")
        with open(api_path, encoding="utf-8") as f:
            content = f.read()
        assert "liquidation-cascade-detector" in content

    def test_endpoint_calls_compute_function(self):
        api_path = os.path.join(_ROOT, "backend", "api.py")
        with open(api_path, encoding="utf-8") as f:
            content = f.read()
        assert "compute_liquidation_cascade_detector" in content

    def test_metrics_function_exists(self):
        metrics_path = os.path.join(_ROOT, "backend", "metrics.py")
        with open(metrics_path, encoding="utf-8") as f:
            content = f.read()
        assert "compute_liquidation_cascade_detector" in content

    def test_function_is_async(self):
        metrics_path = os.path.join(_ROOT, "backend", "metrics.py")
        with open(metrics_path, encoding="utf-8") as f:
            content = f.read()
        assert "async def compute_liquidation_cascade_detector" in content


# ── HTML card ────────────────────────────────────────────────────────────────

class TestHTMLCard:
    def test_html_has_liquidation_cascade_detector_title(self):
        html = _html()
        assert "Liquidation Cascade Detector" in html

    def test_html_has_liq_cascade_detector_id(self):
        html = _html()
        assert "liq-cascade-detector" in html

    def test_html_has_progress_element(self):
        html = _html()
        assert "progress" in html.lower()

    def test_html_has_regime_reference(self):
        html = _html()
        assert "regime" in html.lower()

    def test_html_has_support_levels_reference(self):
        html = _html()
        assert "support_levels" in html or "support-levels" in html or "supportLevels" in html

    def test_html_has_total_liquidated_reference(self):
        html = _html()
        assert (
            "total_liquidated" in html
            or "totalLiquidated" in html
            or "total-liquidated" in html
            or "TOTAL LIQUIDATED" in html
        )

    def test_js_has_render_function(self):
        js = _js()
        assert "renderLiqCascadeDetector" in js

    def test_js_calls_api_endpoint(self):
        js = _js()
        assert "liquidation-cascade-detector" in js


# ── Additional edge case / coverage tests ─────────────────────────────────────

class TestAdditional:
    def test_result_is_dict(self, result):
        assert isinstance(result, dict)

    def test_cascade_chain_all_assets_from_valid_set(self, result):
        assets = {e["asset"] for e in result["cascade_chain"]}
        assert assets.issubset(VALID_ASSETS)

    def test_support_levels_all_positive(self, result):
        for lvl in result["support_levels"]:
            assert lvl > 0

    def test_total_liquidated_is_finite(self, result):
        import math
        assert math.isfinite(result["total_liquidated_usd"])

    def test_liq_velocity_is_finite(self, result):
        import math
        assert math.isfinite(result["liq_velocity"])

    def test_cascade_probability_is_finite(self, result):
        import math
        assert math.isfinite(result["cascade_probability"])

    def test_time_to_cascade_is_finite(self, result):
        import math
        assert math.isfinite(result["time_to_cascade_minutes"])

    def test_exchanges_are_non_empty_strings(self, result):
        for ex in result["exchanges"]:
            assert len(ex) > 0

    def test_result_has_at_least_8_keys(self, result):
        assert len(result) >= 8

    def test_cascade_chain_at_least_4_entries(self, result):
        # 4 assets × min 2 events = at least 8 entries
        assert len(result["cascade_chain"]) >= 4

    def test_support_levels_at_least_3(self, result):
        assert len(result["support_levels"]) >= 3
