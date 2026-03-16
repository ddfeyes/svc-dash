"""
Unit / smoke tests for /api/perpetual-basis.

Perpetual futures basis tracker:
  - Basis = (perp_price - spot_price) / spot_price * 100 (%)
  - Annualized basis rate  = basis_pct × (365 × 24 / funding_interval_hours)
  - Funding annualized %   = avg_funding_rate × 3 × 365 × 100  (3 payouts/day)
  - Carry signal:  basis > threshold → positive_carry (short perp / long spot)
                   basis < -threshold → negative_carry (long perp / short spot)
                   |basis| ≤ threshold → neutral
  - Carry strength: 0-100 score based on basis magnitude
  - Carry action:  text recommendation derived from carry signal
  - Basis z-score: current basis vs rolling history

Covers:
  - _pb_basis_pct
  - _pb_annualized_from_price
  - _pb_funding_annualized
  - _pb_carry_signal
  - _pb_carry_strength
  - _pb_carry_action
  - _pb_basis_zscore
  - Response shape / key validation
  - History list structure
  - Route, HTML card, JS function, JS API call
"""

import sys
import os
import time
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "backend"))

from metrics import (
    _pb_basis_pct,
    _pb_annualized_from_price,
    _pb_funding_annualized,
    _pb_carry_signal,
    _pb_carry_strength,
    _pb_carry_action,
    _pb_basis_zscore,
)

# ---------------------------------------------------------------------------
# Sample response fixture
# ---------------------------------------------------------------------------

NOW = 1_700_000_000.0

SAMPLE_RESPONSE = {
    "symbol": "BANANAS31USDT",
    "perp_price": 1.0050,
    "spot_price": 1.0000,
    "basis_pct": 0.50,
    "annualized_basis_pct": 547.5,
    "carry_signal": "positive_carry",
    "carry_strength": 62.5,
    "carry_action": "short_perp_long_spot",
    "basis_zscore": 1.2,
    "funding_rate": 0.0001,
    "funding_annualized_pct": 10.95,
    "history": [
        {"ts": NOW - 3600, "basis_pct": 0.30, "funding_rate": 0.00008},
        {"ts": NOW - 1800, "basis_pct": 0.40, "funding_rate": 0.00009},
        {"ts": NOW,        "basis_pct": 0.50, "funding_rate": 0.0001},
    ],
    "description": "Positive carry: perp trades above spot — short perp / long spot",
}


# ===========================================================================
# 1. _pb_basis_pct
# ===========================================================================

class TestPbBasisPct:
    def test_zero_when_perp_equals_spot(self):
        assert _pb_basis_pct(1.0, 1.0) == pytest.approx(0.0, abs=1e-6)

    def test_positive_when_perp_above_spot(self):
        assert _pb_basis_pct(101.0, 100.0) > 0

    def test_negative_when_perp_below_spot(self):
        assert _pb_basis_pct(99.0, 100.0) < 0

    def test_correct_formula(self):
        # (105 - 100) / 100 * 100 = 5.0%
        result = _pb_basis_pct(105.0, 100.0)
        assert result == pytest.approx(5.0, rel=1e-4)

    def test_small_difference_precision(self):
        # (100.01 - 100.00) / 100.00 * 100 = 0.01%
        result = _pb_basis_pct(100.01, 100.00)
        assert result == pytest.approx(0.01, rel=1e-3)

    def test_zero_spot_returns_zero(self):
        assert _pb_basis_pct(100.0, 0.0) == 0.0

    def test_returns_float(self):
        assert isinstance(_pb_basis_pct(1.005, 1.0), float)


# ===========================================================================
# 2. _pb_annualized_from_price
# ===========================================================================

class TestPbAnnualizedFromPrice:
    def test_zero_basis_returns_zero(self):
        assert _pb_annualized_from_price(0.0) == pytest.approx(0.0, abs=1e-6)

    def test_positive_basis_positive_annualized(self):
        assert _pb_annualized_from_price(0.5) > 0

    def test_negative_basis_negative_annualized(self):
        assert _pb_annualized_from_price(-0.5) < 0

    def test_correct_formula_8h_interval(self):
        # 0.5% basis × 365 × 24 / 8 = 547.5%
        result = _pb_annualized_from_price(0.5, funding_interval_hours=8)
        assert result == pytest.approx(547.5, rel=1e-3)

    def test_custom_interval_24h(self):
        # 1% × 365 × 24 / 24 = 365%
        result = _pb_annualized_from_price(1.0, funding_interval_hours=24)
        assert result == pytest.approx(365.0, rel=1e-3)

    def test_returns_float(self):
        assert isinstance(_pb_annualized_from_price(0.5), float)


# ===========================================================================
# 3. _pb_funding_annualized
# ===========================================================================

class TestPbFundingAnnualized:
    def test_zero_rate_returns_zero(self):
        assert _pb_funding_annualized(0.0) == pytest.approx(0.0, abs=1e-6)

    def test_positive_rate_returns_positive(self):
        assert _pb_funding_annualized(0.0001) > 0

    def test_negative_rate_returns_negative(self):
        assert _pb_funding_annualized(-0.0001) < 0

    def test_correct_formula(self):
        # 0.0001 × 3 × 365 × 100 = 10.95%
        result = _pb_funding_annualized(0.0001)
        assert result == pytest.approx(10.95, rel=1e-3)

    def test_returns_float(self):
        assert isinstance(_pb_funding_annualized(0.0001), float)


# ===========================================================================
# 4. _pb_carry_signal
# ===========================================================================

class TestPbCarrySignal:
    def test_positive_basis_above_threshold_is_positive_carry(self):
        assert _pb_carry_signal(0.5) == "positive_carry"

    def test_negative_basis_below_threshold_is_negative_carry(self):
        assert _pb_carry_signal(-0.5) == "negative_carry"

    def test_zero_basis_is_neutral(self):
        assert _pb_carry_signal(0.0) == "neutral"

    def test_small_positive_below_threshold_is_neutral(self):
        # threshold defaults to 0.1%
        assert _pb_carry_signal(0.05) == "neutral"

    def test_small_negative_above_neg_threshold_is_neutral(self):
        assert _pb_carry_signal(-0.05) == "neutral"

    def test_exactly_at_threshold_is_positive_carry(self):
        # at exactly threshold → positive_carry
        result = _pb_carry_signal(0.1, threshold=0.1)
        assert result == "positive_carry"


# ===========================================================================
# 5. _pb_carry_strength
# ===========================================================================

class TestPbCarryStrength:
    def test_zero_basis_returns_low_score(self):
        assert _pb_carry_strength(0.0) < 30

    def test_large_positive_basis_returns_high_score(self):
        assert _pb_carry_strength(5.0) > 70

    def test_large_negative_basis_returns_high_score(self):
        # strength is magnitude-based (abs)
        assert _pb_carry_strength(-5.0) > 70

    def test_result_in_0_100_range(self):
        for v in (0.0, 0.1, 0.5, 1.0, 5.0, 10.0, -5.0):
            s = _pb_carry_strength(v)
            assert 0 <= s <= 100, f"strength out of range for basis={v}"

    def test_returns_float(self):
        assert isinstance(_pb_carry_strength(0.5), float)

    def test_positive_and_negative_same_magnitude_equal_strength(self):
        assert _pb_carry_strength(1.0) == pytest.approx(
            _pb_carry_strength(-1.0), rel=1e-4
        )


# ===========================================================================
# 6. _pb_carry_action
# ===========================================================================

class TestPbCarryAction:
    def test_positive_carry_action(self):
        assert _pb_carry_action("positive_carry") == "short_perp_long_spot"

    def test_negative_carry_action(self):
        assert _pb_carry_action("negative_carry") == "long_perp_short_spot"

    def test_neutral_action(self):
        assert _pb_carry_action("neutral") == "no_trade"

    def test_returns_string(self):
        assert isinstance(_pb_carry_action("positive_carry"), str)


# ===========================================================================
# 7. _pb_basis_zscore
# ===========================================================================

class TestPbBasisZscore:
    def test_empty_history_returns_zero(self):
        assert _pb_basis_zscore(0.5, []) == 0.0

    def test_single_item_returns_zero(self):
        assert _pb_basis_zscore(0.5, [{"basis_pct": 0.5}]) == 0.0

    def test_current_at_mean_returns_near_zero(self):
        history = [{"basis_pct": 1.0}, {"basis_pct": 1.0}, {"basis_pct": 1.0}]
        assert _pb_basis_zscore(1.0, history) == pytest.approx(0.0, abs=0.01)

    def test_above_mean_returns_positive(self):
        history = [{"basis_pct": 0.0}, {"basis_pct": 1.0}]
        result = _pb_basis_zscore(2.0, history)
        assert result > 0

    def test_below_mean_returns_negative(self):
        history = [{"basis_pct": 1.0}, {"basis_pct": 2.0}]
        result = _pb_basis_zscore(0.0, history)
        assert result < 0

    def test_uniform_history_returns_zero(self):
        history = [{"basis_pct": 0.5}] * 10
        assert _pb_basis_zscore(0.5, history) == pytest.approx(0.0, abs=0.01)

    def test_returns_float(self):
        history = [{"basis_pct": 0.3}, {"basis_pct": 0.7}]
        assert isinstance(_pb_basis_zscore(0.5, history), float)


# ===========================================================================
# 8. History list structure
# ===========================================================================

class TestPbBasisHistory:
    def test_history_is_list(self):
        assert isinstance(SAMPLE_RESPONSE["history"], list)

    def test_history_items_have_ts(self):
        for item in SAMPLE_RESPONSE["history"]:
            assert "ts" in item

    def test_history_items_have_basis_pct(self):
        for item in SAMPLE_RESPONSE["history"]:
            assert "basis_pct" in item
            assert isinstance(item["basis_pct"], (int, float))

    def test_history_items_have_funding_rate(self):
        for item in SAMPLE_RESPONSE["history"]:
            assert "funding_rate" in item

    def test_history_sorted_by_ts_ascending(self):
        tss = [h["ts"] for h in SAMPLE_RESPONSE["history"]]
        assert tss == sorted(tss)

    def test_history_length_greater_than_zero(self):
        assert len(SAMPLE_RESPONSE["history"]) > 0


# ===========================================================================
# 9. SAMPLE_RESPONSE structure
# ===========================================================================

class TestSampleResponseShape:
    def test_has_symbol(self):
        assert "symbol" in SAMPLE_RESPONSE

    def test_has_perp_price(self):
        assert "perp_price" in SAMPLE_RESPONSE
        assert SAMPLE_RESPONSE["perp_price"] > 0

    def test_has_spot_price_field(self):
        # may be None if spot market unavailable
        assert "spot_price" in SAMPLE_RESPONSE

    def test_has_basis_pct(self):
        assert "basis_pct" in SAMPLE_RESPONSE
        assert isinstance(SAMPLE_RESPONSE["basis_pct"], (int, float))

    def test_has_annualized_basis_pct(self):
        assert "annualized_basis_pct" in SAMPLE_RESPONSE

    def test_carry_signal_valid(self):
        assert SAMPLE_RESPONSE["carry_signal"] in (
            "positive_carry", "negative_carry", "neutral"
        )

    def test_carry_strength_in_range(self):
        s = SAMPLE_RESPONSE["carry_strength"]
        assert 0 <= s <= 100

    def test_carry_action_valid(self):
        assert SAMPLE_RESPONSE["carry_action"] in (
            "short_perp_long_spot", "long_perp_short_spot", "no_trade"
        )

    def test_has_basis_zscore(self):
        assert "basis_zscore" in SAMPLE_RESPONSE
        assert isinstance(SAMPLE_RESPONSE["basis_zscore"], (int, float))

    def test_has_funding_rate(self):
        assert "funding_rate" in SAMPLE_RESPONSE

    def test_has_funding_annualized_pct(self):
        assert "funding_annualized_pct" in SAMPLE_RESPONSE

    def test_has_history_list(self):
        assert isinstance(SAMPLE_RESPONSE["history"], list)

    def test_has_description(self):
        assert "description" in SAMPLE_RESPONSE
        assert isinstance(SAMPLE_RESPONSE["description"], str)


# ===========================================================================
# 10. Structural tests
# ===========================================================================

class TestStructural:
    def test_route_registered_in_api_py(self):
        api_path = os.path.join(os.path.dirname(__file__), "..", "backend", "api.py")
        content = open(api_path).read()
        assert "/perpetual-basis" in content, "/perpetual-basis route missing from api.py"

    def test_html_card_exists(self):
        html_path = os.path.join(os.path.dirname(__file__), "..", "frontend", "index.html")
        content = open(html_path).read()
        assert "card-perpetual-basis" in content, "card-perpetual-basis missing from index.html"

    def test_js_render_function_exists(self):
        js_path = os.path.join(os.path.dirname(__file__), "..", "frontend", "app.js")
        content = open(js_path).read()
        assert "renderPerpetualBasis" in content, "renderPerpetualBasis missing from app.js"

    def test_js_api_call_to_endpoint(self):
        js_path = os.path.join(os.path.dirname(__file__), "..", "frontend", "app.js")
        content = open(js_path).read()
        assert "/perpetual-basis" in content, "/perpetual-basis call missing from app.js"
