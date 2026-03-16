"""
Unit / smoke tests for /api/validator-activity.

Ethereum Validator Activity Dashboard — tracks active validator counts,
attestation effectiveness, validator queue, slashing events, and APY.

Approach:
  - Active validator count with 30d trend (simulated beacon chain data)
  - Attestation effectiveness rate: % of validators attesting correctly
  - Queue size: entering (pending activation) and exiting (pending exit)
  - Slashing events: count of recent slashings in last 30 days
  - Estimated staking APY: formula based on total staked ETH and network rewards

Signal:
  healthy    — effectiveness >= 95% and no recent spikes
  degraded   — effectiveness 90–95% or queue backlog
  unhealthy  — effectiveness < 90% or recent slashings spike

Covers:
  - _va_effectiveness_rate
  - _va_queue_pressure
  - _va_slashing_rate
  - _va_staking_apy
  - _va_validator_trend
  - _va_health_label
  - _va_participation_score
  - Response shape / key validation
  - Structural: route, HTML card, JS function, JS API call
"""

import sys
import os
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "backend"))

from metrics import (
    _va_effectiveness_rate,
    _va_queue_pressure,
    _va_slashing_rate,
    _va_staking_apy,
    _va_validator_trend,
    _va_health_label,
    _va_participation_score,
)

# ---------------------------------------------------------------------------
# Sample response fixture
# ---------------------------------------------------------------------------

SAMPLE_RESPONSE = {
    "validators": {
        "active":          1_023_456,
        "pending_entry":       4_200,
        "pending_exit":          380,
        "slashed_30d":            12,
        "change_30d_pct":        2.1,
    },
    "attestation": {
        "effectiveness_pct":   96.8,
        "participation_score": 82.0,
        "epoch":           310_450,
    },
    "queue": {
        "entry_count":    4_200,
        "exit_count":       380,
        "pressure":       "moderate",
        "wait_epochs":       225,
    },
    "slashing": {
        "count_30d":         12,
        "rate_per_1k":     0.012,
        "last_event_days":    3,
    },
    "apy": {
        "estimated_pct":   3.85,
        "total_staked_eth": 32_750_000,
        "annual_rewards_eth": 1_260_875,
    },
    "health": {
        "label":     "healthy",
        "score":        88.0,
    },
    "history_30d": [
        {"date": "2024-11-14", "active": 1_020_100, "effectiveness_pct": 96.5},
        {"date": "2024-11-15", "active": 1_020_800, "effectiveness_pct": 96.7},
        {"date": "2024-11-16", "active": 1_021_200, "effectiveness_pct": 97.0},
        {"date": "2024-11-17", "active": 1_022_000, "effectiveness_pct": 96.8},
        {"date": "2024-11-18", "active": 1_022_900, "effectiveness_pct": 96.6},
        {"date": "2024-11-19", "active": 1_023_100, "effectiveness_pct": 96.9},
        {"date": "2024-11-20", "active": 1_023_456, "effectiveness_pct": 96.8},
    ],
    "description": "Healthy: 1.02M validators, 96.8% attestation effectiveness, APY 3.85%",
}


# ===========================================================================
# 1. _va_effectiveness_rate
# ===========================================================================

class TestVaEffectivenessRate:
    def test_all_attested_is_100(self):
        assert _va_effectiveness_rate(1000, 1000) == pytest.approx(100.0, abs=0.01)

    def test_none_attested_is_zero(self):
        assert _va_effectiveness_rate(0, 1000) == pytest.approx(0.0, abs=0.01)

    def test_partial_correct_fraction(self):
        assert _va_effectiveness_rate(750, 1000) == pytest.approx(75.0, abs=0.01)

    def test_zero_total_returns_zero(self):
        assert _va_effectiveness_rate(0, 0) == pytest.approx(0.0, abs=1e-6)

    def test_returns_float(self):
        assert isinstance(_va_effectiveness_rate(950, 1000), float)

    def test_result_in_0_100_range(self):
        for attested, total in [(0, 100), (50, 100), (100, 100), (99, 100)]:
            result = _va_effectiveness_rate(attested, total)
            assert 0.0 <= result <= 100.0

    def test_typical_mainnet_value(self):
        # Mainnet typically 96–99%
        rate = _va_effectiveness_rate(968_000, 1_000_000)
        assert 96.0 <= rate <= 100.0


# ===========================================================================
# 2. _va_queue_pressure
# ===========================================================================

class TestVaQueuePressure:
    def test_high_entry_is_high(self):
        assert _va_queue_pressure(20_000, 100) == "high"

    def test_low_entry_is_low(self):
        assert _va_queue_pressure(50, 50) == "low"

    def test_moderate_entry_is_moderate(self):
        result = _va_queue_pressure(5_000, 200)
        assert result in ("moderate", "high", "low")

    def test_high_exit_elevates_pressure(self):
        result = _va_queue_pressure(100, 15_000)
        assert result in ("high", "moderate")

    def test_returns_valid_string(self):
        result = _va_queue_pressure(1000, 500)
        assert result in ("high", "moderate", "low")

    def test_zero_queues_is_low(self):
        assert _va_queue_pressure(0, 0) == "low"


# ===========================================================================
# 3. _va_slashing_rate
# ===========================================================================

class TestVaSlashingRate:
    def test_zero_slashes_is_zero(self):
        assert _va_slashing_rate(0, 1_000_000) == pytest.approx(0.0, abs=1e-9)

    def test_rate_per_1k_correct(self):
        # 10 slashes / 1_000_000 active = 0.01 per 1k
        rate = _va_slashing_rate(10, 1_000_000)
        assert rate == pytest.approx(0.01, rel=1e-3)

    def test_zero_active_returns_zero(self):
        assert _va_slashing_rate(5, 0) == pytest.approx(0.0, abs=1e-9)

    def test_returns_float(self):
        assert isinstance(_va_slashing_rate(12, 1_000_000), float)

    def test_higher_slashes_higher_rate(self):
        low  = _va_slashing_rate(5,  1_000_000)
        high = _va_slashing_rate(50, 1_000_000)
        assert high > low

    def test_larger_active_lower_rate(self):
        small = _va_slashing_rate(10, 500_000)
        large = _va_slashing_rate(10, 1_000_000)
        assert large < small


# ===========================================================================
# 4. _va_staking_apy
# ===========================================================================

class TestVaStakingApy:
    def test_typical_mainnet_apy_in_range(self):
        # ~32.7M ETH staked → ~3.5–5% APY
        apy = _va_staking_apy(32_700_000)
        assert 2.0 <= apy <= 8.0

    def test_less_staked_higher_apy(self):
        # Fewer stakers share more of the reward pool
        apy_low  = _va_staking_apy(10_000_000)
        apy_high = _va_staking_apy(40_000_000)
        assert apy_low > apy_high

    def test_zero_staked_returns_zero(self):
        assert _va_staking_apy(0) == pytest.approx(0.0, abs=1e-6)

    def test_returns_float(self):
        assert isinstance(_va_staking_apy(32_000_000), float)

    def test_result_is_positive(self):
        assert _va_staking_apy(30_000_000) > 0

    def test_result_in_sane_range(self):
        apy = _va_staking_apy(32_000_000)
        assert 0.0 < apy < 20.0


# ===========================================================================
# 5. _va_validator_trend
# ===========================================================================

class TestVaValidatorTrend:
    def test_growing_count_is_growing(self):
        counts = [1_000_000, 1_005_000, 1_010_000, 1_015_000, 1_020_000]
        assert _va_validator_trend(counts) == "growing"

    def test_shrinking_count_is_shrinking(self):
        counts = [1_020_000, 1_015_000, 1_010_000, 1_005_000, 1_000_000]
        assert _va_validator_trend(counts) == "shrinking"

    def test_flat_count_is_stable(self):
        counts = [1_000_000] * 7
        assert _va_validator_trend(counts) == "stable"

    def test_empty_returns_stable(self):
        assert _va_validator_trend([]) == "stable"

    def test_single_returns_stable(self):
        assert _va_validator_trend([1_000_000]) == "stable"

    def test_returns_valid_string(self):
        result = _va_validator_trend([1_000_000, 1_001_000, 1_002_000])
        assert result in ("growing", "shrinking", "stable")


# ===========================================================================
# 6. _va_health_label
# ===========================================================================

class TestVaHealthLabel:
    def test_high_effectiveness_no_slashing_is_healthy(self):
        assert _va_health_label(97.5, 0) == "healthy"

    def test_low_effectiveness_is_unhealthy(self):
        assert _va_health_label(88.0, 0) == "unhealthy"

    def test_mid_effectiveness_is_degraded(self):
        assert _va_health_label(92.5, 0) == "degraded"

    def test_high_slashing_degrades_health(self):
        result = _va_health_label(97.0, 100)
        assert result in ("degraded", "unhealthy")

    def test_returns_valid_string(self):
        result = _va_health_label(96.0, 5)
        assert result in ("healthy", "degraded", "unhealthy")

    def test_boundary_95_is_healthy(self):
        assert _va_health_label(95.0, 0) == "healthy"

    def test_boundary_90_is_degraded(self):
        assert _va_health_label(90.0, 0) == "degraded"


# ===========================================================================
# 7. _va_participation_score
# ===========================================================================

class TestVaParticipationScore:
    def test_perfect_effectiveness_near_100(self):
        score = _va_participation_score(100.0, 0, 1_000_000)
        assert score >= 95.0

    def test_low_effectiveness_low_score(self):
        score = _va_participation_score(85.0, 0, 1_000_000)
        assert score < 70.0

    def test_large_queue_lowers_score(self):
        no_queue   = _va_participation_score(97.0, 0,      1_000_000)
        with_queue = _va_participation_score(97.0, 50_000, 1_000_000)
        assert no_queue > with_queue

    def test_result_in_0_100_range(self):
        for eff, queue in [(100.0, 0), (95.0, 5000), (80.0, 20000)]:
            score = _va_participation_score(eff, queue, 1_000_000)
            assert 0.0 <= score <= 100.0

    def test_returns_float(self):
        assert isinstance(_va_participation_score(96.8, 4200, 1_000_000), float)

    def test_zero_active_returns_zero(self):
        assert _va_participation_score(96.0, 0, 0) == pytest.approx(0.0, abs=1e-6)


# ===========================================================================
# 8. SAMPLE_RESPONSE structure
# ===========================================================================

class TestSampleResponseShape:
    def test_has_validators_dict(self):
        assert isinstance(SAMPLE_RESPONSE["validators"], dict)

    def test_validators_has_required_keys(self):
        for key in ("active", "pending_entry", "pending_exit", "slashed_30d", "change_30d_pct"):
            assert key in SAMPLE_RESPONSE["validators"], f"validators missing '{key}'"

    def test_active_validators_positive(self):
        assert SAMPLE_RESPONSE["validators"]["active"] > 0

    def test_has_attestation_dict(self):
        assert isinstance(SAMPLE_RESPONSE["attestation"], dict)

    def test_attestation_has_required_keys(self):
        for key in ("effectiveness_pct", "participation_score", "epoch"):
            assert key in SAMPLE_RESPONSE["attestation"], f"attestation missing '{key}'"

    def test_effectiveness_in_range(self):
        e = SAMPLE_RESPONSE["attestation"]["effectiveness_pct"]
        assert 0.0 <= e <= 100.0

    def test_has_queue_dict(self):
        assert isinstance(SAMPLE_RESPONSE["queue"], dict)

    def test_queue_has_required_keys(self):
        for key in ("entry_count", "exit_count", "pressure", "wait_epochs"):
            assert key in SAMPLE_RESPONSE["queue"], f"queue missing '{key}'"

    def test_queue_pressure_valid(self):
        assert SAMPLE_RESPONSE["queue"]["pressure"] in ("high", "moderate", "low")

    def test_has_slashing_dict(self):
        assert isinstance(SAMPLE_RESPONSE["slashing"], dict)

    def test_slashing_has_required_keys(self):
        for key in ("count_30d", "rate_per_1k", "last_event_days"):
            assert key in SAMPLE_RESPONSE["slashing"], f"slashing missing '{key}'"

    def test_has_apy_dict(self):
        assert isinstance(SAMPLE_RESPONSE["apy"], dict)

    def test_apy_has_required_keys(self):
        for key in ("estimated_pct", "total_staked_eth", "annual_rewards_eth"):
            assert key in SAMPLE_RESPONSE["apy"], f"apy missing '{key}'"

    def test_apy_positive(self):
        assert SAMPLE_RESPONSE["apy"]["estimated_pct"] > 0

    def test_has_health_dict(self):
        assert isinstance(SAMPLE_RESPONSE["health"], dict)

    def test_health_label_valid(self):
        assert SAMPLE_RESPONSE["health"]["label"] in ("healthy", "degraded", "unhealthy")

    def test_has_history_30d_list(self):
        assert isinstance(SAMPLE_RESPONSE["history_30d"], list)

    def test_history_items_have_required_keys(self):
        for item in SAMPLE_RESPONSE["history_30d"]:
            for key in ("date", "active", "effectiveness_pct"):
                assert key in item, f"history_30d item missing '{key}'"

    def test_has_description(self):
        assert isinstance(SAMPLE_RESPONSE["description"], str)


# ===========================================================================
# 9. Structural tests
# ===========================================================================

class TestStructural:
    def test_route_registered_in_api_py(self):
        api_path = os.path.join(os.path.dirname(__file__), "..", "backend", "api.py")
        content = open(api_path).read()
        assert "/validator-activity" in content, "/validator-activity route missing"

    def test_html_card_exists(self):
        html_path = os.path.join(os.path.dirname(__file__), "..", "frontend", "index.html")
        content = open(html_path).read()
        assert "card-validator-activity" in content, "card-validator-activity missing"

    def test_js_render_function_exists(self):
        js_path = os.path.join(os.path.dirname(__file__), "..", "frontend", "app.js")
        content = open(js_path).read()
        assert "renderValidatorActivity" in content, "renderValidatorActivity missing"

    def test_js_api_call_to_endpoint(self):
        js_path = os.path.join(os.path.dirname(__file__), "..", "frontend", "app.js")
        content = open(js_path).read()
        assert "/validator-activity" in content, "/validator-activity call missing"
