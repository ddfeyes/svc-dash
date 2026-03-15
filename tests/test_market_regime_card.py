"""
Unit tests for the Market Regime dashboard card rendering logic.

Mirrors the JS helper functions in app.js (renderMarketRegime) and validates
expected response shapes with a mocked HTTP layer.
"""
from unittest.mock import MagicMock, patch

import pytest


# ── Python mirrors of app.js helpers ─────────────────────────────────────────

REGIME_COLORS = {
    "trending": "var(--blue)",
    "ranging":  "var(--yellow)",
    "choppy":   "var(--red)",
}

BADGE_CLASSES = {
    "trending": "badge-blue",
    "ranging":  "badge-yellow",
    "choppy":   "badge-red",
}


def regime_color(regime):
    return REGIME_COLORS.get((regime or "").lower(), "var(--fg)")


def badge_class(regime):
    return BADGE_CLASSES.get((regime or "").lower(), "badge-blue")


def fmt_confidence(confidence):
    if confidence is None:
        return "—"
    return f"{float(confidence) * 100:.0f}%"


def fmt_metric(value, decimals=3):
    if value is None:
        return "—"
    return f"{float(value):.{decimals}f}"


# ── Sample API payloads ───────────────────────────────────────────────────────

TRENDING = {
    "status": "ok",
    "symbol": "BANANAS31USDT",
    "regime": "trending",
    "confidence": 0.82,
    "metrics": {
        "volatility": 0.0023,
        "trend_strength": 0.741,
        "range_ratio": 0.312,
    },
}

RANGING = {
    "status": "ok",
    "symbol": "BANANAS31USDT",
    "regime": "ranging",
    "confidence": 0.65,
    "metrics": {
        "volatility": 0.0011,
        "trend_strength": 0.210,
        "range_ratio": 0.780,
    },
}

CHOPPY = {
    "status": "ok",
    "symbol": "BANANAS31USDT",
    "regime": "choppy",
    "confidence": 0.51,
    "metrics": {
        "volatility": 0.0041,
        "trend_strength": 0.088,
        "range_ratio": 0.501,
    },
}

MISSING_METRICS = {
    "status": "ok",
    "symbol": "BANANAS31USDT",
    "regime": "trending",
    "confidence": 0.70,
    "metrics": {},
}


# ── Color / badge helpers ─────────────────────────────────────────────────────

class TestRegimeColor:
    def test_trending_is_blue(self):
        assert regime_color("trending") == "var(--blue)"

    def test_ranging_is_yellow(self):
        assert regime_color("ranging") == "var(--yellow)"

    def test_choppy_is_red(self):
        assert regime_color("choppy") == "var(--red)"

    def test_unknown_fallback(self):
        assert regime_color("unknown") == "var(--fg)"

    def test_none_fallback(self):
        assert regime_color(None) == "var(--fg)"

    def test_case_insensitive(self):
        assert regime_color("TRENDING") == "var(--blue)"
        assert regime_color("Ranging") == "var(--yellow)"


class TestBadgeClass:
    def test_trending_badge_blue(self):
        assert badge_class("trending") == "badge-blue"

    def test_ranging_badge_yellow(self):
        assert badge_class("ranging") == "badge-yellow"

    def test_choppy_badge_red(self):
        assert badge_class("choppy") == "badge-red"

    def test_unknown_badge_blue(self):
        assert badge_class("unknown") == "badge-blue"


class TestFmtConfidence:
    def test_82_percent(self):
        assert fmt_confidence(0.82) == "82%"

    def test_100_percent(self):
        assert fmt_confidence(1.0) == "100%"

    def test_0_percent(self):
        assert fmt_confidence(0.0) == "0%"

    def test_none_returns_dash(self):
        assert fmt_confidence(None) == "—"

    def test_rounds_down(self):
        # 0.651 → 65%
        assert fmt_confidence(0.651) == "65%"


class TestFmtMetric:
    def test_three_decimals_default(self):
        assert fmt_metric(0.741) == "0.741"

    def test_four_decimals(self):
        assert fmt_metric(0.0023, decimals=4) == "0.0023"

    def test_none_returns_dash(self):
        assert fmt_metric(None) == "—"


# ── Response shape validation ─────────────────────────────────────────────────

class TestMarketRegimeResponseShape:
    REQUIRED_KEYS = ("status", "symbol", "regime", "confidence", "metrics")
    METRIC_KEYS   = ("volatility", "trend_strength", "range_ratio")

    @pytest.mark.parametrize("payload", [TRENDING, RANGING, CHOPPY])
    def test_required_keys_present(self, payload):
        for key in self.REQUIRED_KEYS:
            assert key in payload, f"missing key: {key}"

    @pytest.mark.parametrize("payload", [TRENDING, RANGING, CHOPPY])
    def test_metric_keys_present(self, payload):
        for key in self.METRIC_KEYS:
            assert key in payload["metrics"], f"missing metric key: {key}"

    def test_status_ok(self):
        assert TRENDING["status"] == "ok"

    def test_regime_is_valid(self):
        valid = {"trending", "ranging", "choppy"}
        for p in [TRENDING, RANGING, CHOPPY]:
            assert p["regime"] in valid

    def test_confidence_between_0_and_1(self):
        for p in [TRENDING, RANGING, CHOPPY]:
            assert 0.0 <= p["confidence"] <= 1.0

    def test_volatility_positive(self):
        assert TRENDING["metrics"]["volatility"] > 0

    def test_trend_strength_between_0_and_1(self):
        for p in [TRENDING, RANGING, CHOPPY]:
            v = p["metrics"]["trend_strength"]
            assert 0.0 <= v <= 1.0

    def test_range_ratio_between_0_and_1(self):
        for p in [TRENDING, RANGING, CHOPPY]:
            v = p["metrics"]["range_ratio"]
            assert 0.0 <= v <= 1.0

    def test_missing_metric_keys_handled(self):
        """Cards must tolerate an empty metrics dict without crashing."""
        m = MISSING_METRICS["metrics"]
        assert fmt_metric(m.get("volatility")) == "—"
        assert fmt_metric(m.get("trend_strength")) == "—"
        assert fmt_metric(m.get("range_ratio")) == "—"


# ── Mocked fetch ──────────────────────────────────────────────────────────────

class TestMarketRegimeMockedFetch:
    @patch("requests.get")
    def test_mock_returns_trending(self, mock_get):
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = TRENDING
        mock_get.return_value = mock_resp

        import requests
        r = requests.get(
            "http://localhost:8765/api/market-regime",
            params={"symbol": "BANANAS31USDT"},
        )
        data = r.json()

        assert data["status"] == "ok"
        assert data["regime"] == "trending"
        assert data["confidence"] > 0.5

    @patch("requests.get")
    def test_mock_returns_ranging(self, mock_get):
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = RANGING
        mock_get.return_value = mock_resp

        import requests
        data = requests.get("http://localhost:8765/api/market-regime").json()

        assert data["regime"] == "ranging"
        assert data["metrics"]["range_ratio"] > data["metrics"]["trend_strength"]

    @patch("requests.get")
    def test_render_output_trending(self, mock_get):
        """Verify rendered HTML for trending regime contains correct color and values."""
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = TRENDING
        mock_get.return_value = mock_resp

        import requests
        data = requests.get("http://localhost:8765/api/market-regime").json()

        color  = regime_color(data["regime"])
        conf   = fmt_confidence(data["confidence"])
        vol    = fmt_metric(data["metrics"]["volatility"], decimals=4)
        trend  = fmt_metric(data["metrics"]["trend_strength"])
        rrange = fmt_metric(data["metrics"]["range_ratio"])
        bc     = badge_class(data["regime"])

        html = (
            f'<div class="phase-name" style="color:{color}">{data["regime"]}</div>'
            f'<div class="metric-value" style="color:{color}">{conf}</div>'
            f'<div class="metric-value" style="font-size:13px">{vol}</div>'
            f'<div class="metric-value" style="font-size:13px">{trend}</div>'
            f'<div class="metric-value" style="font-size:13px">{rrange}</div>'
        )

        assert "var(--blue)" in html
        assert "trending" in html
        assert "82%" in html
        assert "0.0023" in html
        assert "0.741" in html
        assert bc == "badge-blue"

    @pytest.mark.parametrize("regime,expected_color,expected_badge", [
        ("trending", "var(--blue)",   "badge-blue"),
        ("ranging",  "var(--yellow)", "badge-yellow"),
        ("choppy",   "var(--red)",    "badge-red"),
    ])
    def test_all_regimes_color_and_badge(self, regime, expected_color, expected_badge):
        assert regime_color(regime) == expected_color
        assert badge_class(regime) == expected_badge
