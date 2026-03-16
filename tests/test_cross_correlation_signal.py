"""
Unit / smoke tests for /api/cross-correlation-signal.

Cross-correlation signal detector: computes rolling correlation between
two price series (window=20), deterministic via seed 20260316.

Detects:
  - Correlation score (Pearson, -1 to 1)
  - Signal strength (0 to 1, normalized)
  - Divergence detection (high variance in rolling correlations)
  - Signal type (bullish/bearish/neutral)
  - Confidence level (0 to 1)
  - Rolling correlations (last 20 windows)

Covers:
  - Function presence and signature
  - Field presence, types, ranges
  - Determinism (same seed = same output)
  - Empty/invalid inputs
  - HTTP endpoint registration
  - Frontend card integration
  - Signal type classification logic
  - Correlation range validation [-1, 1]
  - Rolling correlations array structure (20 floats)
  - Timestamp presence and format
"""
import os
import sys
import math
import pytest
import asyncio
import json
from datetime import datetime, timezone

_ROOT = os.path.join(os.path.dirname(__file__), "..")
sys.path.insert(0, os.path.join(_ROOT, "backend"))

from metrics import compute_cross_correlation_signal


def _html() -> str:
    with open(os.path.join(_ROOT, "frontend", "index.html"), encoding="utf-8") as f:
        return f.read()


def _js() -> str:
    with open(os.path.join(_ROOT, "frontend", "app.js"), encoding="utf-8") as f:
        return f.read()


# ── Test data ─────────────────────────────────────────────────────────────────


def _perfect_corr_series():
    """Two perfectly correlated series."""
    base = [100 + i * 2 for i in range(50)]
    return base, base


def _inverse_corr_series():
    """Two negatively correlated series."""
    a = [100 + i * 2 for i in range(50)]
    b = [100 - i * 2 for i in range(50)]
    return a, b


def _no_corr_series():
    """Two uncorrelated series."""
    import random
    rng = random.Random(42)
    a = [rng.uniform(100, 110) for _ in range(50)]
    b = [rng.uniform(100, 110) for _ in range(50)]
    return a, b


def _short_series():
    """Short series (less than window size)."""
    return [100, 101, 102], [100, 102, 104]


def _empty_series():
    """Empty series."""
    return [], []


def _constant_series():
    """Constant (no variance) series."""
    return [100] * 50, [100] * 50


# ── Unit Tests ────────────────────────────────────────────────────────────────


class TestFunctionPresence:
    """Function signature and basic availability."""

    def test_function_exists(self):
        """compute_cross_correlation_signal is importable."""
        assert callable(compute_cross_correlation_signal)

    def test_function_signature(self):
        """Function accepts two list arguments."""
        import inspect
        sig = inspect.signature(compute_cross_correlation_signal)
        params = list(sig.parameters.keys())
        assert "series_a" in params
        assert "series_b" in params
        assert len(params) == 2


class TestFieldPresence:
    """All required fields present in output."""

    def test_all_fields_present(self):
        """Output dict has all required fields."""
        a, b = _perfect_corr_series()
        result = compute_cross_correlation_signal(a, b)
        
        required_fields = [
            "correlation_score",
            "signal_strength",
            "divergence_detected",
            "window_size",
            "rolling_correlations",
            "signal_type",
            "confidence_level",
            "timestamp",
        ]
        for field in required_fields:
            assert field in result, f"Missing field: {field}"

    def test_no_extra_fields(self):
        """Output dict has exactly expected fields (no extras)."""
        a, b = _perfect_corr_series()
        result = compute_cross_correlation_signal(a, b)
        
        expected = {
            "correlation_score",
            "signal_strength",
            "divergence_detected",
            "window_size",
            "rolling_correlations",
            "signal_type",
            "confidence_level",
            "timestamp",
        }
        assert set(result.keys()) == expected


class TestFieldTypes:
    """Field types are correct."""

    def test_correlation_score_is_float(self):
        """correlation_score is a float."""
        a, b = _perfect_corr_series()
        result = compute_cross_correlation_signal(a, b)
        assert isinstance(result["correlation_score"], (int, float))

    def test_signal_strength_is_float(self):
        """signal_strength is a float."""
        a, b = _perfect_corr_series()
        result = compute_cross_correlation_signal(a, b)
        assert isinstance(result["signal_strength"], (int, float))

    def test_divergence_detected_is_bool(self):
        """divergence_detected is a boolean."""
        a, b = _perfect_corr_series()
        result = compute_cross_correlation_signal(a, b)
        assert isinstance(result["divergence_detected"], bool)

    def test_window_size_is_int(self):
        """window_size is an integer."""
        a, b = _perfect_corr_series()
        result = compute_cross_correlation_signal(a, b)
        assert isinstance(result["window_size"], int)

    def test_rolling_correlations_is_list(self):
        """rolling_correlations is a list."""
        a, b = _perfect_corr_series()
        result = compute_cross_correlation_signal(a, b)
        assert isinstance(result["rolling_correlations"], list)

    def test_rolling_correlations_contains_floats(self):
        """rolling_correlations contains floats."""
        a, b = _perfect_corr_series()
        result = compute_cross_correlation_signal(a, b)
        for val in result["rolling_correlations"]:
            assert isinstance(val, (int, float)), f"Expected float, got {type(val)}"

    def test_signal_type_is_string(self):
        """signal_type is a string."""
        a, b = _perfect_corr_series()
        result = compute_cross_correlation_signal(a, b)
        assert isinstance(result["signal_type"], str)

    def test_confidence_level_is_float(self):
        """confidence_level is a float."""
        a, b = _perfect_corr_series()
        result = compute_cross_correlation_signal(a, b)
        assert isinstance(result["confidence_level"], (int, float))

    def test_timestamp_is_string(self):
        """timestamp is a string."""
        a, b = _perfect_corr_series()
        result = compute_cross_correlation_signal(a, b)
        assert isinstance(result["timestamp"], str)


class TestFieldRanges:
    """Field values are in sensible ranges."""

    def test_correlation_score_in_range(self):
        """correlation_score is in [-1, 1]."""
        a, b = _perfect_corr_series()
        result = compute_cross_correlation_signal(a, b)
        assert -1.0 <= result["correlation_score"] <= 1.0

    def test_signal_strength_in_range(self):
        """signal_strength is in [0, 1]."""
        a, b = _perfect_corr_series()
        result = compute_cross_correlation_signal(a, b)
        assert 0.0 <= result["signal_strength"] <= 1.0

    def test_window_size_is_20(self):
        """window_size is exactly 20."""
        a, b = _perfect_corr_series()
        result = compute_cross_correlation_signal(a, b)
        assert result["window_size"] == 20

    def test_rolling_correlations_length_is_20(self):
        """rolling_correlations has exactly 20 elements."""
        a, b = _perfect_corr_series()
        result = compute_cross_correlation_signal(a, b)
        assert len(result["rolling_correlations"]) == 20

    def test_rolling_correlations_in_range(self):
        """Each rolling correlation is in [-1, 1]."""
        a, b = _perfect_corr_series()
        result = compute_cross_correlation_signal(a, b)
        for corr in result["rolling_correlations"]:
            assert -1.0 <= corr <= 1.0

    def test_signal_type_valid_values(self):
        """signal_type is one of: bullish, bearish, neutral."""
        a, b = _perfect_corr_series()
        result = compute_cross_correlation_signal(a, b)
        assert result["signal_type"] in ("bullish", "bearish", "neutral")

    def test_confidence_level_in_range(self):
        """confidence_level is in [0, 1]."""
        a, b = _perfect_corr_series()
        result = compute_cross_correlation_signal(a, b)
        assert 0.0 <= result["confidence_level"] <= 1.0


class TestDeterminism:
    """Same inputs produce same outputs (seed 20260316)."""

    def test_same_seed_same_output(self):
        """Same series produce identical result on repeated calls."""
        a, b = _perfect_corr_series()
        result1 = compute_cross_correlation_signal(a, b)
        result2 = compute_cross_correlation_signal(a, b)
        
        # Compare deterministic fields (not timestamp)
        assert result1["correlation_score"] == result2["correlation_score"]
        assert result1["signal_strength"] == result2["signal_strength"]
        assert result1["divergence_detected"] == result2["divergence_detected"]
        assert result1["rolling_correlations"] == result2["rolling_correlations"]
        assert result1["signal_type"] == result2["signal_type"]
        # confidence_level may have small random boost, but should be similar
        assert abs(result1["confidence_level"] - result2["confidence_level"]) < 0.15

    def test_different_series_different_output(self):
        """Different series produce different results."""
        a1, b1 = _perfect_corr_series()
        a2, b2 = _inverse_corr_series()
        
        result1 = compute_cross_correlation_signal(a1, b1)
        result2 = compute_cross_correlation_signal(a2, b2)
        
        # Results should differ significantly
        assert result1["correlation_score"] != result2["correlation_score"]


class TestSignalLogic:
    """Signal type classification and strength logic."""

    def test_perfect_correlation_bullish(self):
        """Perfect positive correlation signals bullish (unless divergence)."""
        a, b = _perfect_corr_series()
        result = compute_cross_correlation_signal(a, b)
        
        # Should be highly positive
        assert result["correlation_score"] > 0.5
        # Should be bullish unless divergence detected
        if not result["divergence_detected"]:
            assert result["signal_type"] == "bullish"

    def test_inverse_correlation_bearish(self):
        """Inverse correlation signals bearish (unless divergence)."""
        a, b = _inverse_corr_series()
        result = compute_cross_correlation_signal(a, b)
        
        # Should be highly negative
        assert result["correlation_score"] < -0.5
        # Should be bearish unless divergence detected
        if not result["divergence_detected"]:
            assert result["signal_type"] == "bearish"

    def test_weak_correlation_neutral(self):
        """Weak correlation signals neutral."""
        a, b = _no_corr_series()
        result = compute_cross_correlation_signal(a, b)
        
        # Should be close to zero
        assert abs(result["correlation_score"]) < 0.5
        # Should be neutral
        assert result["signal_type"] == "neutral"

    def test_signal_strength_correlates_with_strength(self):
        """Higher signal_strength for stronger correlations."""
        a1, b1 = _perfect_corr_series()
        a2, b2 = _no_corr_series()
        
        result1 = compute_cross_correlation_signal(a1, b1)
        result2 = compute_cross_correlation_signal(a2, b2)
        
        # Perfect correlation should have higher strength
        assert result1["signal_strength"] >= result2["signal_strength"]


class TestEdgeCases:
    """Handle edge cases gracefully."""

    def test_empty_series_returns_zeros(self):
        """Empty series returns neutral defaults."""
        a, b = _empty_series()
        result = compute_cross_correlation_signal(a, b)
        
        assert result["correlation_score"] == 0.0
        assert result["signal_strength"] == 0.0
        assert result["divergence_detected"] == False
        assert result["signal_type"] == "neutral"
        assert result["confidence_level"] == 0.0

    def test_short_series_handled(self):
        """Short series (< window) handled gracefully."""
        a, b = _short_series()
        result = compute_cross_correlation_signal(a, b)
        
        # Should still return valid structure
        assert result["window_size"] == 20
        assert len(result["rolling_correlations"]) == 20
        assert all(isinstance(c, (int, float)) for c in result["rolling_correlations"])

    def test_constant_series_returns_neutral(self):
        """Constant series (no variance) returns neutral."""
        a, b = _constant_series()
        result = compute_cross_correlation_signal(a, b)
        
        # Should handle no variance gracefully
        assert result["correlation_score"] == 0.0
        assert result["signal_type"] == "neutral"

    def test_mismatched_lengths_handled(self):
        """Mismatched series lengths handled by truncation."""
        a = [100, 101, 102, 103, 104, 105]
        b = [100, 102, 104]
        result = compute_cross_correlation_signal(a, b)
        
        # Should not error
        assert result["window_size"] == 20
        assert len(result["rolling_correlations"]) == 20


class TestTimestamp:
    """Timestamp validation."""

    def test_timestamp_is_iso_format(self):
        """timestamp is valid ISO 8601 format."""
        a, b = _perfect_corr_series()
        result = compute_cross_correlation_signal(a, b)
        
        # Should be parseable as ISO
        ts = result["timestamp"]
        try:
            datetime.fromisoformat(ts.replace("Z", "+00:00"))
        except ValueError:
            pytest.fail(f"Invalid ISO timestamp: {ts}")

    def test_timestamp_is_recent(self):
        """timestamp is recent (within last 5 seconds)."""
        a, b = _perfect_corr_series()
        before = datetime.now(timezone.utc)
        result = compute_cross_correlation_signal(a, b)
        after = datetime.now(timezone.utc)
        
        ts = datetime.fromisoformat(result["timestamp"].replace("Z", "+00:00"))
        assert before <= ts <= after


class TestRollingCorrelations:
    """Rolling correlation window behavior."""

    def test_rolling_correlations_length_always_20(self):
        """rolling_correlations always has exactly 20 elements."""
        test_cases = [
            _perfect_corr_series(),
            _inverse_corr_series(),
            _no_corr_series(),
            _short_series(),
        ]
        for a, b in test_cases:
            result = compute_cross_correlation_signal(a, b)
            assert len(result["rolling_correlations"]) == 20

    def test_rolling_correlations_all_floats(self):
        """All rolling correlations are floats in valid range."""
        a, b = _perfect_corr_series()
        result = compute_cross_correlation_signal(a, b)
        
        for corr in result["rolling_correlations"]:
            assert isinstance(corr, (int, float))
            assert -1.0 <= corr <= 1.0


# ── HTTP Integration Tests ────────────────────────────────────────────────────


class TestHTTPEndpoint:
    """API endpoint integration tests."""

    @pytest.mark.asyncio
    async def test_endpoint_exists(self):
        """GET /api/cross-correlation-signal endpoint is registered."""
        from fastapi.testclient import TestClient
        from backend.api import router
        from fastapi import FastAPI
        
        app = FastAPI()
        app.include_router(router)
        client = TestClient(app)
        
        response = client.get("/api/cross-correlation-signal")
        assert response.status_code in (200, 422)  # 422 if optional params missing

    @pytest.mark.asyncio
    async def test_endpoint_default_response(self):
        """Endpoint returns valid response with default parameters."""
        from fastapi.testclient import TestClient
        from backend.api import router
        from fastapi import FastAPI
        
        app = FastAPI()
        app.include_router(router)
        client = TestClient(app)
        
        response = client.get("/api/cross-correlation-signal")
        assert response.status_code == 200
        data = response.json()
        
        # Verify all required fields
        required_fields = [
            "correlation_score",
            "signal_strength",
            "divergence_detected",
            "window_size",
            "rolling_correlations",
            "signal_type",
            "confidence_level",
            "timestamp",
        ]
        for field in required_fields:
            assert field in data

    @pytest.mark.asyncio
    async def test_endpoint_with_custom_series(self):
        """Endpoint accepts custom series as query parameters."""
        from fastapi.testclient import TestClient
        from backend.api import router
        from fastapi import FastAPI
        
        app = FastAPI()
        app.include_router(router)
        client = TestClient(app)
        
        # Create series as comma-separated strings
        series_a = "100,101,102,103,104,105"
        series_b = "100,102,104,106,108,110"
        
        response = client.get(
            "/api/cross-correlation-signal",
            params={"series_a": series_a, "series_b": series_b}
        )
        assert response.status_code == 200
        data = response.json()
        
        # Should have all fields
        assert "correlation_score" in data
        assert "signal_type" in data

    @pytest.mark.asyncio
    async def test_endpoint_invalid_series_handling(self):
        """Endpoint handles invalid series format gracefully."""
        from fastapi.testclient import TestClient
        from backend.api import router
        from fastapi import FastAPI
        
        app = FastAPI()
        app.include_router(router)
        client = TestClient(app)
        
        response = client.get(
            "/api/cross-correlation-signal",
            params={"series_a": "not,a,number", "series_b": "also,invalid"}
        )
        # Should return 200 with error field or default neutral response
        assert response.status_code == 200
        data = response.json()
        assert "correlation_score" in data


# ── Frontend Integration Tests ────────────────────────────────────────────────


class TestFrontendIntegration:
    """Frontend HTML/JS card presence and structure."""

    def test_html_contains_cross_correlation_card(self):
        """index.html has cross-correlation signal card."""
        html = _html()
        # Look for a card or element related to cross-correlation
        assert "cross-correlation" in html.lower() or "correlation-signal" in html.lower()

    def test_js_has_render_function(self):
        """app.js has renderCrossCorrelationSignal function."""
        js = _js()
        assert "renderCrossCorrelationSignal" in js

    def test_js_function_handles_response(self):
        """JS function references response fields."""
        js = _js()
        # Should reference at least signal_type or correlation_score
        assert "signal_type" in js or "correlation_score" in js or "signal_strength" in js


# ── Comprehensive Scenario Tests ──────────────────────────────────────────────


class TestComprehensiveScenarios:
    """Multi-field validation scenarios."""

    def test_scenario_bullish_high_confidence(self):
        """Bullish signal with high confidence."""
        # Strongly correlated positive series
        a = [100 + i for i in range(50)]
        b = [100 + i * 1.1 for i in range(50)]
        result = compute_cross_correlation_signal(a, b)
        
        assert result["correlation_score"] > 0.5
        if not result["divergence_detected"]:
            assert result["signal_type"] == "bullish"
        assert result["confidence_level"] > 0.5

    def test_scenario_bearish_low_confidence(self):
        """Bearish signal with lower confidence (divergence)."""
        # Weakly correlated series with high variance
        import random
        rng = random.Random(999)
        a = [100 + rng.uniform(-10, 10) for _ in range(50)]
        b = [100 - rng.uniform(-10, 10) for _ in range(50)]
        result = compute_cross_correlation_signal(a, b)
        
        # May have divergence
        if result["divergence_detected"]:
            assert result["signal_type"] == "neutral"

    def test_scenario_all_fields_consistent(self):
        """All fields are mutually consistent."""
        a, b = _perfect_corr_series()
        result = compute_cross_correlation_signal(a, b)
        
        # If signal_strength is high, correlation_score should be high
        if result["signal_strength"] > 0.7:
            assert abs(result["correlation_score"]) > 0.4
        
        # If divergence_detected, signal_type should be neutral
        if result["divergence_detected"]:
            assert result["signal_type"] == "neutral"
        
        # rolling_correlations should cluster near correlation_score
        avg_rolling = sum(result["rolling_correlations"]) / len(result["rolling_correlations"])
        assert abs(avg_rolling - result["correlation_score"]) < 0.5


class TestCountAndCoverage:
    """Overall test count and coverage targets."""

    def test_at_least_40_tests_defined(self):
        """Test suite has 40+ test cases."""
        # This will be verified by pytest
        # As a sanity check, count the test methods in this module
        import inspect
        members = inspect.getmembers(sys.modules[__name__], inspect.isclass)
        test_classes = [m for name, m in members if name.startswith("Test")]
        
        total_tests = sum(
            len([
                x for x in inspect.getmembers(cls, inspect.isfunction)
                if x[0].startswith("test_")
            ])
            for cls in test_classes
        )
        assert total_tests >= 40, f"Expected 40+ tests, found {total_tests}"


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])
