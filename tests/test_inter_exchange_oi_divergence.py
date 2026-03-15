"""Tests for compute_inter_exchange_oi_divergence."""
import pytest
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'backend'))

from metrics import compute_inter_exchange_oi_divergence

# ── helpers ───────────────────────────────────────────────────────────────────

def _snap(ts, oi):
    return {"ts": float(ts), "oi_value": float(oi)}


def _run(oi_by_exchange, **kwargs):
    return compute_inter_exchange_oi_divergence(oi_by_exchange, **kwargs)


# Pre-built scenarios (verified by hand)

# All three exchanges up exactly 5% → zero deviation
SAME_UP_5 = {
    "binance": [_snap(0, 100), _snap(60, 105)],   # +5.0%
    "bybit":   [_snap(0, 200), _snap(60, 210)],   # +5.0%
    "okx":     [_snap(0, 300), _snap(60, 315)],   # +5.0%
}

# Binance/Bybit +8%, OKX -3%
# mean = (8+8-3)/3 = 4.333…%, okx dev = -3-4.333 = -7.333% → divergence + opposing
OPPOSING = {
    "binance": [_snap(0, 100), _snap(60, 108)],   # +8%
    "bybit":   [_snap(0, 200), _snap(60, 216)],   # +8%
    "okx":     [_snap(0, 300), _snap(60, 291)],   # -3%
}

# Binance +10%, Bybit +2% — same direction, divergence_pct=4% (> 3% threshold, < 6%)
SAME_DIR_LOW = {
    "binance": [_snap(0, 100), _snap(60, 110)],   # +10%
    "bybit":   [_snap(0, 200), _snap(60, 204)],   # +2%
}

# Binance +14%, Bybit +1% — same direction, divergence_pct=6.5% (>= 2×threshold=6)
SAME_DIR_MEDIUM = {
    "binance": [_snap(0, 100), _snap(60, 114)],   # +14%
    "bybit":   [_snap(0, 200), _snap(60, 202)],   # +1%
}


# ── TestStructure ─────────────────────────────────────────────────────────────

class TestStructure:
    def test_returns_dict(self):
        assert isinstance(_run(SAME_UP_5), dict)

    def test_has_divergence_key(self):
        assert "divergence" in _run(SAME_UP_5)

    def test_has_divergence_pct_key(self):
        assert "divergence_pct" in _run(SAME_UP_5)

    def test_has_mean_pct_change_key(self):
        assert "mean_pct_change" in _run(SAME_UP_5)

    def test_has_diverging_exchange_key(self):
        assert "diverging_exchange" in _run(SAME_UP_5)

    def test_has_opposing_key(self):
        assert "opposing" in _run(SAME_UP_5)

    def test_has_severity_key(self):
        assert "severity" in _run(SAME_UP_5)

    def test_has_alert_key(self):
        assert "alert" in _run(SAME_UP_5)

    def test_has_exchange_count_key(self):
        assert "exchange_count" in _run(SAME_UP_5)

    def test_has_exchanges_key(self):
        assert "exchanges" in _run(SAME_UP_5)

    def test_has_description_key(self):
        assert "description" in _run(SAME_UP_5)

    def test_has_min_divergence_pct_key(self):
        assert "min_divergence_pct" in _run(SAME_UP_5)

    def test_exchange_count_matches_valid_input(self):
        assert _run(SAME_UP_5)["exchange_count"] == 3

    def test_per_exchange_has_pct_change(self):
        r = _run(SAME_UP_5)
        for ex in ("binance", "bybit", "okx"):
            assert "pct_change" in r["exchanges"][ex]

    def test_per_exchange_has_latest_oi(self):
        r = _run(SAME_UP_5)
        for ex in ("binance", "bybit", "okx"):
            assert "latest_oi" in r["exchanges"][ex]

    def test_per_exchange_has_deviation(self):
        r = _run(SAME_UP_5)
        for ex in ("binance", "bybit", "okx"):
            assert "deviation" in r["exchanges"][ex]

    def test_per_exchange_has_direction(self):
        r = _run(SAME_UP_5)
        for ex in ("binance", "bybit", "okx"):
            assert "direction" in r["exchanges"][ex]

    def test_alert_equals_divergence(self):
        r1 = _run(SAME_UP_5)
        r2 = _run(OPPOSING)
        assert r1["alert"] == r1["divergence"]
        assert r2["alert"] == r2["divergence"]

    def test_description_is_string(self):
        assert isinstance(_run(SAME_UP_5)["description"], str)
        assert isinstance(_run(OPPOSING)["description"], str)


# ── TestNoDivergence ──────────────────────────────────────────────────────────

class TestNoDivergence:
    def test_all_same_pct_no_divergence(self):
        assert _run(SAME_UP_5)["divergence"] is False

    def test_all_same_pct_divergence_pct_zero(self):
        assert _run(SAME_UP_5)["divergence_pct"] == 0.0

    def test_all_same_pct_severity_none(self):
        assert _run(SAME_UP_5)["severity"] == "none"

    def test_all_same_pct_diverging_exchange_none(self):
        assert _run(SAME_UP_5)["diverging_exchange"] is None

    def test_small_deviations_below_threshold(self):
        # binance +2%, bybit +1% → mean=1.5%, max_dev=0.5% < 3%
        data = {
            "binance": [_snap(0, 100), _snap(60, 102)],
            "bybit":   [_snap(0, 100), _snap(60, 101)],
        }
        assert _run(data)["divergence"] is False

    def test_below_threshold_no_alert(self):
        data = {
            "binance": [_snap(0, 100), _snap(60, 102)],
            "bybit":   [_snap(0, 100), _snap(60, 101)],
        }
        assert _run(data)["alert"] is False


# ── TestPctChange ─────────────────────────────────────────────────────────────

class TestPctChange:
    def test_up_10_pct(self):
        data = {
            "binance": [_snap(0, 100), _snap(60, 110)],
            "bybit":   [_snap(0, 100), _snap(60, 110)],
        }
        r = _run(data)
        assert abs(r["exchanges"]["binance"]["pct_change"] - 10.0) < 1e-6

    def test_down_5_pct(self):
        data = {
            "binance": [_snap(0, 100), _snap(60, 95)],
            "bybit":   [_snap(0, 100), _snap(60, 95)],
        }
        assert abs(_run(data)["exchanges"]["binance"]["pct_change"] - (-5.0)) < 1e-6

    def test_flat_zero_pct(self):
        data = {
            "binance": [_snap(0, 100), _snap(60, 100)],
            "bybit":   [_snap(0, 100), _snap(60, 100)],
        }
        assert _run(data)["exchanges"]["binance"]["pct_change"] == 0.0

    def test_uses_first_and_last_snapshot_ignores_middle(self):
        # Middle snapshot (200) is irrelevant — only first (100) and last (110) matter
        data = {
            "binance": [_snap(0, 100), _snap(30, 200), _snap(60, 110)],
            "bybit":   [_snap(0, 100), _snap(60, 110)],
        }
        assert abs(_run(data)["exchanges"]["binance"]["pct_change"] - 10.0) < 1e-6

    def test_latest_oi_is_last_snapshot_value(self):
        data = {
            "binance": [_snap(0, 100), _snap(60, 107.5)],
            "bybit":   [_snap(0, 100), _snap(60, 107.5)],
        }
        assert abs(_run(data)["exchanges"]["binance"]["latest_oi"] - 107.5) < 1e-6

    def test_exchange_with_single_snapshot_excluded(self):
        data = {
            "binance": [_snap(0, 100)],               # only 1 → excluded
            "bybit":   [_snap(0, 200), _snap(60, 210)],
            "okx":     [_snap(0, 300), _snap(60, 315)],
        }
        r = _run(data)
        assert r["exchange_count"] == 2
        assert "binance" not in r["exchanges"]

    def test_zero_starting_oi_gives_zero_pct_change(self):
        data = {
            "binance": [_snap(0, 0), _snap(60, 100)],   # start=0 → pct=0
            "bybit":   [_snap(0, 200), _snap(60, 210)],
        }
        assert _run(data)["exchanges"]["binance"]["pct_change"] == 0.0


# ── TestDivergence ────────────────────────────────────────────────────────────

class TestDivergence:
    def test_divergence_detected(self):
        assert _run(OPPOSING)["divergence"] is True

    def test_diverging_exchange_identified(self):
        # OKX has the biggest absolute deviation in OPPOSING scenario
        assert _run(OPPOSING)["diverging_exchange"] == "okx"

    def test_divergence_pct_correct(self):
        # OPPOSING: mean=(8+8-3)/3=4.333%, okx_dev=|-3-4.333|=7.333%
        r = _run(OPPOSING)
        expected = abs(-3.0 - (8.0 + 8.0 - 3.0) / 3)
        assert abs(r["divergence_pct"] - expected) < 0.001

    def test_mean_pct_change_correct(self):
        r = _run(OPPOSING)
        expected = (8.0 + 8.0 - 3.0) / 3
        assert abs(r["mean_pct_change"] - expected) < 0.001

    def test_custom_threshold_triggers_at_higher_value(self):
        # SAME_DIR_LOW has divergence_pct=4% > default 3% → True with 3%, False with 5%
        assert _run(SAME_DIR_LOW)["divergence"] is True
        assert _run(SAME_DIR_LOW, min_divergence_pct=5.0)["divergence"] is False

    def test_exactly_at_threshold_triggers(self):
        # binance +7%, bybit +1%: mean=4%, deviations=3.0% exactly = threshold
        data = {
            "binance": [_snap(0, 100), _snap(60, 107)],
            "bybit":   [_snap(0, 100), _snap(60, 101)],
        }
        assert _run(data)["divergence"] is True

    def test_deviations_sum_near_zero(self):
        # Deviations sum to ≈0 (mean property); tolerance accounts for 4dp rounding
        r = _run(OPPOSING)
        total = sum(v["deviation"] for v in r["exchanges"].values())
        assert abs(total) < 1e-3

    def test_min_divergence_pct_stored_in_result(self):
        assert _run(SAME_UP_5, min_divergence_pct=5.0)["min_divergence_pct"] == 5.0


# ── TestOpposing ──────────────────────────────────────────────────────────────

class TestOpposing:
    def test_opposing_true_when_up_and_down_present(self):
        assert _run(OPPOSING)["opposing"] is True

    def test_opposing_false_when_all_same_sign(self):
        assert _run(SAME_DIR_LOW)["opposing"] is False

    def test_opposing_false_when_all_flat(self):
        data = {
            "binance": [_snap(0, 100), _snap(60, 100)],
            "bybit":   [_snap(0, 100), _snap(60, 100)],
        }
        assert _run(data)["opposing"] is False

    def test_opposing_true_two_exchanges_opposite_directions(self):
        data = {
            "binance": [_snap(0, 100), _snap(60, 110)],  # +10%
            "bybit":   [_snap(0, 100), _snap(60,  90)],  # -10%
        }
        assert _run(data)["opposing"] is True

    def test_opposing_false_all_positive_including_small(self):
        data = {
            "binance": [_snap(0, 100), _snap(60, 110)],   # +10%
            "bybit":   [_snap(0, 100), _snap(60, 100.1)], # +0.1%
        }
        assert _run(data)["opposing"] is False


# ── TestSeverity ──────────────────────────────────────────────────────────────

class TestSeverity:
    def test_no_divergence_severity_none(self):
        assert _run(SAME_UP_5)["severity"] == "none"

    def test_opposing_divergence_severity_high(self):
        assert _run(OPPOSING)["severity"] == "high"

    def test_same_dir_low_severity_low(self):
        # divergence_pct=4%, threshold=3%, not opposing, 4% < 2*3%=6%
        assert _run(SAME_DIR_LOW)["severity"] == "low"

    def test_same_dir_medium_severity_medium(self):
        # divergence_pct=6.5%, threshold=3%, not opposing, 6.5% >= 2*3%=6%
        assert _run(SAME_DIR_MEDIUM)["severity"] == "medium"

    def test_opposing_and_small_divergence_still_high(self):
        # opposing=True always means severity="high" when divergence=True
        data = {
            "binance": [_snap(0, 100), _snap(60, 104)],   # +4%
            "bybit":   [_snap(0, 100), _snap(60,  98)],   # -2%
        }
        # mean=1%, deviations=3% each, divergence_pct=3% >= threshold=3%
        # opposing=True → severity="high"
        r = _run(data)
        assert r["divergence"] is True
        assert r["opposing"] is True
        assert r["severity"] == "high"

    def test_below_threshold_always_none(self):
        data = {
            "binance": [_snap(0, 100), _snap(60, 102)],
            "bybit":   [_snap(0, 100), _snap(60, 101)],
        }
        assert _run(data)["severity"] == "none"


# ── TestEdgeCases ─────────────────────────────────────────────────────────────

class TestEdgeCases:
    def test_empty_input(self):
        r = _run({})
        assert r["divergence"] is False
        assert r["exchange_count"] == 0

    def test_single_exchange_only(self):
        data = {"binance": [_snap(0, 100), _snap(60, 110)]}
        r = _run(data)
        assert r["divergence"] is False

    def test_all_single_snapshots_insufficient(self):
        data = {
            "binance": [_snap(0, 100)],
            "bybit":   [_snap(0, 200)],
        }
        r = _run(data)
        assert r["divergence"] is False
        assert r["exchange_count"] == 0

    def test_direction_up(self):
        data = {
            "binance": [_snap(0, 100), _snap(60, 110)],
            "bybit":   [_snap(0, 100), _snap(60, 110)],
        }
        assert _run(data)["exchanges"]["binance"]["direction"] == "up"

    def test_direction_down(self):
        data = {
            "binance": [_snap(0, 100), _snap(60, 90)],
            "bybit":   [_snap(0, 100), _snap(60, 90)],
        }
        assert _run(data)["exchanges"]["binance"]["direction"] == "down"

    def test_direction_flat(self):
        data = {
            "binance": [_snap(0, 100), _snap(60, 100)],
            "bybit":   [_snap(0, 100), _snap(60, 100)],
        }
        assert _run(data)["exchanges"]["binance"]["direction"] == "flat"

    def test_two_exchanges_only_sufficient(self):
        data = {
            "binance": [_snap(0, 100), _snap(60, 110)],
            "bybit":   [_snap(0, 100), _snap(60, 110)],
        }
        r = _run(data)
        assert r["exchange_count"] == 2
        assert isinstance(r["divergence"], bool)
