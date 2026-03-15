"""Tests for compute_market_microstructure_score — composite 0-100 market quality score."""
import math
import pytest
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'backend'))

from metrics import compute_market_microstructure_score

# ── helpers ───────────────────────────────────────────────────────────────────

PERFECT = dict(spread_bps=0.5, depth_usd=5_000_000.0, trade_rate=10.0, noise_ratio=0.0)
TERRIBLE = dict(spread_bps=50.0, depth_usd=0.0, trade_rate=0.0, noise_ratio=1.0)
# All components at their geometric / arithmetic midpoint → each score = 50.0
MIDPOINT = dict(
    spread_bps=25.25,                              # arithmetic midpoint of 0.5..50.0
    depth_usd=math.sqrt(10_000.0 * 5_000_000.0),  # geometric mean
    trade_rate=math.sqrt(0.01 * 10.0),             # geometric mean
    noise_ratio=0.5,                               # arithmetic midpoint of 0..1
)


def _score(**kwargs):
    return compute_market_microstructure_score(**kwargs)


# ── TestStructure ─────────────────────────────────────────────────────────────

class TestStructure:
    def test_returns_dict(self):
        assert isinstance(_score(**PERFECT), dict)

    def test_has_score_key(self):
        assert "score" in _score(**PERFECT)

    def test_has_grade_key(self):
        assert "grade" in _score(**PERFECT)

    def test_has_label_key(self):
        assert "label" in _score(**PERFECT)

    def test_has_components_key(self):
        assert "components" in _score(**PERFECT)

    def test_has_weights_key(self):
        assert "weights" in _score(**PERFECT)

    def test_score_is_float(self):
        assert isinstance(_score(**PERFECT)["score"], float)

    def test_score_range_0_to_100_perfect(self):
        r = _score(**PERFECT)
        assert 0.0 <= r["score"] <= 100.0

    def test_score_range_0_to_100_terrible(self):
        r = _score(**TERRIBLE)
        assert 0.0 <= r["score"] <= 100.0

    def test_components_has_four_keys(self):
        c = _score(**PERFECT)["components"]
        assert set(c.keys()) == {"spread", "depth", "trade_rate", "noise"}

    def test_each_component_has_score_value_weight(self):
        c = _score(**PERFECT)["components"]
        for name in ("spread", "depth", "trade_rate", "noise"):
            assert "score" in c[name], f"{name} missing 'score'"
            assert "value" in c[name], f"{name} missing 'value'"
            assert "weight" in c[name], f"{name} missing 'weight'"

    def test_component_scores_in_0_100_range(self):
        c = _score(**MIDPOINT)["components"]
        for name in ("spread", "depth", "trade_rate", "noise"):
            assert 0.0 <= c[name]["score"] <= 100.0

    def test_weights_sum_to_1(self):
        w = _score(**PERFECT)["weights"]
        assert abs(sum(w.values()) - 1.0) < 1e-9

    def test_grade_is_string(self):
        assert isinstance(_score(**PERFECT)["grade"], str)

    def test_label_is_string(self):
        assert isinstance(_score(**PERFECT)["label"], str)


# ── TestSpreadScore ───────────────────────────────────────────────────────────

class TestSpreadScore:
    def test_min_spread_gives_100(self):
        r = _score(spread_bps=0.5, depth_usd=1e6, trade_rate=1.0, noise_ratio=0.0)
        assert r["components"]["spread"]["score"] == 100.0

    def test_max_spread_gives_0(self):
        r = _score(spread_bps=50.0, depth_usd=1e6, trade_rate=1.0, noise_ratio=0.0)
        assert r["components"]["spread"]["score"] == 0.0

    def test_spread_midpoint_gives_50(self):
        r = _score(spread_bps=25.25, depth_usd=1e6, trade_rate=1.0, noise_ratio=0.0)
        assert abs(r["components"]["spread"]["score"] - 50.0) < 1e-6

    def test_spread_below_min_clamped_to_100(self):
        r = _score(spread_bps=0.1, depth_usd=1e6, trade_rate=1.0, noise_ratio=0.0)
        assert r["components"]["spread"]["score"] == 100.0

    def test_spread_above_max_clamped_to_0(self):
        r = _score(spread_bps=100.0, depth_usd=1e6, trade_rate=1.0, noise_ratio=0.0)
        assert r["components"]["spread"]["score"] == 0.0

    def test_spread_value_stored_in_component(self):
        r = _score(spread_bps=7.5, depth_usd=1e6, trade_rate=1.0, noise_ratio=0.0)
        assert r["components"]["spread"]["value"] == 7.5


# ── TestDepthScore ────────────────────────────────────────────────────────────

class TestDepthScore:
    def test_zero_depth_gives_0(self):
        r = _score(spread_bps=5.0, depth_usd=0.0, trade_rate=1.0, noise_ratio=0.0)
        assert r["components"]["depth"]["score"] == 0.0

    def test_min_depth_gives_0(self):
        r = _score(spread_bps=5.0, depth_usd=10_000.0, trade_rate=1.0, noise_ratio=0.0)
        assert r["components"]["depth"]["score"] == 0.0

    def test_max_depth_gives_100(self):
        r = _score(spread_bps=5.0, depth_usd=5_000_000.0, trade_rate=1.0, noise_ratio=0.0)
        assert r["components"]["depth"]["score"] == 100.0

    def test_geometric_mean_depth_gives_50(self):
        depth = math.sqrt(10_000.0 * 5_000_000.0)
        r = _score(spread_bps=5.0, depth_usd=depth, trade_rate=1.0, noise_ratio=0.0)
        assert abs(r["components"]["depth"]["score"] - 50.0) < 1e-6

    def test_depth_below_min_gives_0(self):
        r = _score(spread_bps=5.0, depth_usd=5_000.0, trade_rate=1.0, noise_ratio=0.0)
        assert r["components"]["depth"]["score"] == 0.0

    def test_depth_above_max_clamped_to_100(self):
        r = _score(spread_bps=5.0, depth_usd=10_000_000.0, trade_rate=1.0, noise_ratio=0.0)
        assert r["components"]["depth"]["score"] == 100.0


# ── TestTradeRateScore ────────────────────────────────────────────────────────

class TestTradeRateScore:
    def test_zero_rate_gives_0(self):
        r = _score(spread_bps=5.0, depth_usd=1e6, trade_rate=0.0, noise_ratio=0.0)
        assert r["components"]["trade_rate"]["score"] == 0.0

    def test_min_rate_gives_0(self):
        r = _score(spread_bps=5.0, depth_usd=1e6, trade_rate=0.01, noise_ratio=0.0)
        assert r["components"]["trade_rate"]["score"] == 0.0

    def test_max_rate_gives_100(self):
        r = _score(spread_bps=5.0, depth_usd=1e6, trade_rate=10.0, noise_ratio=0.0)
        assert r["components"]["trade_rate"]["score"] == 100.0

    def test_geometric_mean_rate_gives_50(self):
        rate = math.sqrt(0.01 * 10.0)
        r = _score(spread_bps=5.0, depth_usd=1e6, trade_rate=rate, noise_ratio=0.0)
        assert abs(r["components"]["trade_rate"]["score"] - 50.0) < 1e-6

    def test_rate_below_min_gives_0(self):
        r = _score(spread_bps=5.0, depth_usd=1e6, trade_rate=0.005, noise_ratio=0.0)
        assert r["components"]["trade_rate"]["score"] == 0.0

    def test_rate_above_max_clamped_to_100(self):
        r = _score(spread_bps=5.0, depth_usd=1e6, trade_rate=100.0, noise_ratio=0.0)
        assert r["components"]["trade_rate"]["score"] == 100.0


# ── TestNoiseScore ────────────────────────────────────────────────────────────

class TestNoiseScore:
    def test_zero_noise_gives_100(self):
        r = _score(spread_bps=5.0, depth_usd=1e6, trade_rate=1.0, noise_ratio=0.0)
        assert r["components"]["noise"]["score"] == 100.0

    def test_max_noise_gives_0(self):
        r = _score(spread_bps=5.0, depth_usd=1e6, trade_rate=1.0, noise_ratio=1.0)
        assert r["components"]["noise"]["score"] == 0.0

    def test_noise_midpoint_gives_50(self):
        r = _score(spread_bps=5.0, depth_usd=1e6, trade_rate=1.0, noise_ratio=0.5)
        assert abs(r["components"]["noise"]["score"] - 50.0) < 1e-6

    def test_noise_above_1_clamped_to_0(self):
        r = _score(spread_bps=5.0, depth_usd=1e6, trade_rate=1.0, noise_ratio=2.0)
        assert r["components"]["noise"]["score"] == 0.0

    def test_noise_below_0_clamped_to_100(self):
        r = _score(spread_bps=5.0, depth_usd=1e6, trade_rate=1.0, noise_ratio=-0.5)
        assert r["components"]["noise"]["score"] == 100.0


# ── TestCompositeScore ────────────────────────────────────────────────────────

class TestCompositeScore:
    def test_all_perfect_gives_100(self):
        assert _score(**PERFECT)["score"] == 100.0

    def test_all_terrible_gives_0(self):
        assert _score(**TERRIBLE)["score"] == 0.0

    def test_all_midpoint_gives_50(self):
        assert abs(_score(**MIDPOINT)["score"] - 50.0) < 0.01

    def test_custom_weights_spread_only(self):
        w = {"spread": 1, "depth": 0, "trade_rate": 0, "noise": 0}
        r = _score(spread_bps=0.5, depth_usd=0.0, trade_rate=0.0, noise_ratio=1.0, weights=w)
        assert r["score"] == 100.0

    def test_custom_weights_noise_only(self):
        w = {"spread": 0, "depth": 0, "trade_rate": 0, "noise": 1}
        r = _score(spread_bps=50.0, depth_usd=0.0, trade_rate=0.0, noise_ratio=0.0, weights=w)
        assert r["score"] == 100.0

    def test_custom_weights_normalized_proportionally(self):
        # Scaling all weights by same factor must not change the score
        w1 = {"spread": 1, "depth": 1, "trade_rate": 1, "noise": 1}
        w2 = {"spread": 3, "depth": 3, "trade_rate": 3, "noise": 3}
        r1 = _score(**MIDPOINT, weights=w1)
        r2 = _score(**MIDPOINT, weights=w2)
        assert abs(r1["score"] - r2["score"]) < 1e-9

    def test_score_increases_with_better_spread(self):
        r_good = _score(spread_bps=1.0,  depth_usd=1e6, trade_rate=1.0, noise_ratio=0.5)
        r_bad  = _score(spread_bps=20.0, depth_usd=1e6, trade_rate=1.0, noise_ratio=0.5)
        assert r_good["score"] > r_bad["score"]

    def test_score_increases_with_more_depth(self):
        r_deep    = _score(spread_bps=5.0, depth_usd=2_000_000.0, trade_rate=1.0, noise_ratio=0.5)
        r_shallow = _score(spread_bps=5.0, depth_usd=50_000.0,    trade_rate=1.0, noise_ratio=0.5)
        assert r_deep["score"] > r_shallow["score"]

    def test_score_increases_with_higher_trade_rate(self):
        r_active = _score(spread_bps=5.0, depth_usd=1e6, trade_rate=5.0, noise_ratio=0.5)
        r_slow   = _score(spread_bps=5.0, depth_usd=1e6, trade_rate=0.1, noise_ratio=0.5)
        assert r_active["score"] > r_slow["score"]

    def test_score_decreases_with_more_noise(self):
        r_clean = _score(spread_bps=5.0, depth_usd=1e6, trade_rate=1.0, noise_ratio=0.1)
        r_noisy = _score(spread_bps=5.0, depth_usd=1e6, trade_rate=1.0, noise_ratio=0.9)
        assert r_clean["score"] > r_noisy["score"]


# ── TestGradeLabel ────────────────────────────────────────────────────────────

def _score_at(target: float):
    """Return result with composite == target by isolating spread component."""
    # spread_score = 100*(50-spread)/(50-0.5) = target
    # → spread = 50 - target*(50-0.5)/100
    spread = 50.0 - target * (50.0 - 0.5) / 100.0
    w = {"spread": 1, "depth": 0, "trade_rate": 0, "noise": 0}
    return _score(spread_bps=spread, depth_usd=0.0, trade_rate=0.0, noise_ratio=0.0, weights=w)


class TestGradeLabel:
    def test_grade_A_at_80(self):
        r = _score_at(80.0)
        assert r["grade"] == "A"
        assert r["label"] == "excellent"

    def test_grade_A_at_100(self):
        assert _score(**PERFECT)["grade"] == "A"

    def test_grade_B_at_60(self):
        r = _score_at(60.0)
        assert r["grade"] == "B"
        assert r["label"] == "good"

    def test_grade_B_just_below_A(self):
        assert _score_at(79.0)["grade"] == "B"

    def test_grade_C_at_40(self):
        r = _score_at(40.0)
        assert r["grade"] == "C"
        assert r["label"] == "fair"

    def test_grade_C_just_below_B(self):
        assert _score_at(59.0)["grade"] == "C"

    def test_grade_D_at_20(self):
        r = _score_at(20.0)
        assert r["grade"] == "D"
        assert r["label"] == "poor"

    def test_grade_D_just_below_C(self):
        assert _score_at(39.0)["grade"] == "D"

    def test_grade_F_at_0(self):
        r = _score(**TERRIBLE)
        assert r["grade"] == "F"
        assert r["label"] == "very poor"

    def test_grade_F_just_below_D(self):
        assert _score_at(10.0)["grade"] == "F"
        assert _score_at(10.0)["label"] == "very poor"


# ── TestEdgeCases ─────────────────────────────────────────────────────────────

class TestEdgeCases:
    def test_negative_depth_same_as_zero(self):
        r1 = _score(spread_bps=5.0, depth_usd=-1000.0, trade_rate=1.0, noise_ratio=0.5)
        r2 = _score(spread_bps=5.0, depth_usd=0.0,     trade_rate=1.0, noise_ratio=0.5)
        assert r1["score"] == r2["score"]

    def test_negative_trade_rate_same_as_zero(self):
        r1 = _score(spread_bps=5.0, depth_usd=1e6, trade_rate=-1.0, noise_ratio=0.5)
        r2 = _score(spread_bps=5.0, depth_usd=1e6, trade_rate=0.0,  noise_ratio=0.5)
        assert r1["score"] == r2["score"]

    def test_all_extreme_high_clamped_to_100(self):
        r = _score(spread_bps=-100.0, depth_usd=1e9, trade_rate=1000.0, noise_ratio=-10.0)
        assert r["score"] == 100.0

    def test_component_values_reflect_inputs(self):
        r = _score(spread_bps=7.5, depth_usd=300_000.0, trade_rate=2.5, noise_ratio=0.3)
        c = r["components"]
        assert c["spread"]["value"] == 7.5
        assert c["depth"]["value"] == 300_000.0
        assert c["trade_rate"]["value"] == 2.5
        assert c["noise"]["value"] == 0.3

    def test_default_weights_match_expected_values(self):
        w = _score(**PERFECT)["weights"]
        assert abs(w["spread"] - 0.35) < 1e-9
        assert abs(w["depth"] - 0.30) < 1e-9
        assert abs(w["trade_rate"] - 0.20) < 1e-9
        assert abs(w["noise"] - 0.15) < 1e-9
