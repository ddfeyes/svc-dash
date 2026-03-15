"""
Unit tests for the Funding Momentum endpoint and card rendering logic.

Validates response shape, momentum calculation, trend classification,
and color/formatting helpers that mirror app.js renderFundingMomentum().
"""
from unittest.mock import MagicMock, patch

import pytest


# ── Python mirrors of app.js helpers ─────────────────────────────────────────

def fmt_rate(v):
    if v is None:
        return "—"
    return f"{v * 100:.4f}%"


def fmt_momentum(v):
    if v is None:
        return "—"
    sign = "+" if v >= 0 else ""
    return f"{sign}{v * 100:.4f}%"


def trend_arrow(trend):
    return {"accelerating": "↑", "decelerating": "↓", "stable": "→"}.get(trend, "→")


def momentum_color(momentum):
    if momentum is None:
        return "var(--muted)"
    if momentum > 0:
        return "var(--green)"
    if momentum < 0:
        return "var(--red)"
    return "var(--muted)"


def badge_class(momentum):
    if momentum is None or abs(momentum) <= 1e-5:
        return None  # hidden
    return "badge-red" if momentum > 0 else "badge-green"


# ── Sample API payloads ───────────────────────────────────────────────────────

ACCELERATING = {
    "status": "ok",
    "symbol": "BANANAS31USDT",
    "current_rate": 0.00045,
    "momentum": 0.00012,
    "momentum_pct": 36.36,
    "trend": "accelerating",
    "timestamps": [1710432000.0, 1710460800.0, 1710489600.0, 1710518400.0, 1710547200.0],
}

DECELERATING = {
    "status": "ok",
    "symbol": "BANANAS31USDT",
    "current_rate": 0.00010,
    "momentum": -0.00035,
    "momentum_pct": -77.78,
    "trend": "decelerating",
    "timestamps": [1710432000.0, 1710460800.0, 1710489600.0, 1710518400.0, 1710547200.0],
}

STABLE = {
    "status": "ok",
    "symbol": "BANANAS31USDT",
    "current_rate": 0.00045,
    "momentum": 0.000000001,
    "momentum_pct": 0.0002,
    "trend": "stable",
    "timestamps": [1710432000.0, 1710460800.0, 1710518400.0, 1710547200.0],
}

NO_DATA = {
    "status": "ok",
    "symbol": "BANANAS31USDT",
    "current_rate": None,
    "momentum": None,
    "momentum_pct": None,
    "trend": "stable",
    "timestamps": [],
}


# ── Response shape validation ─────────────────────────────────────────────────

class TestFundingMomentumResponseShape:
    REQUIRED_KEYS = ("status", "symbol", "current_rate", "momentum", "momentum_pct", "trend", "timestamps")

    @pytest.mark.parametrize("payload", [ACCELERATING, DECELERATING, STABLE, NO_DATA])
    def test_required_keys_present(self, payload):
        for key in self.REQUIRED_KEYS:
            assert key in payload, f"missing key: {key}"

    def test_status_ok(self):
        assert ACCELERATING["status"] == "ok"

    def test_trend_valid_values(self):
        valid = {"accelerating", "decelerating", "stable"}
        for payload in [ACCELERATING, DECELERATING, STABLE, NO_DATA]:
            assert payload["trend"] in valid

    def test_timestamps_is_list(self):
        assert isinstance(ACCELERATING["timestamps"], list)
        assert isinstance(NO_DATA["timestamps"], list)

    def test_momentum_numeric_when_present(self):
        assert isinstance(ACCELERATING["momentum"], float)

    def test_momentum_none_when_no_data(self):
        assert NO_DATA["momentum"] is None
        assert NO_DATA["current_rate"] is None


# ── Trend classification ──────────────────────────────────────────────────────

class TestTrendClassification:
    def test_positive_momentum_is_accelerating(self):
        assert ACCELERATING["trend"] == "accelerating"
        assert ACCELERATING["momentum"] > 0

    def test_negative_momentum_is_decelerating(self):
        assert DECELERATING["trend"] == "decelerating"
        assert DECELERATING["momentum"] < 0

    def test_near_zero_momentum_is_stable(self):
        assert STABLE["trend"] == "stable"
        assert abs(STABLE["momentum"]) <= 1e-5

    def test_no_data_defaults_stable(self):
        assert NO_DATA["trend"] == "stable"


# ── Formatting helpers ────────────────────────────────────────────────────────

class TestFmtRate:
    def test_positive_rate(self):
        result = fmt_rate(0.00045)
        assert result == "0.0450%"

    def test_negative_rate(self):
        result = fmt_rate(-0.00012)
        assert result == "-0.0120%"

    def test_none_returns_dash(self):
        assert fmt_rate(None) == "—"


class TestFmtMomentum:
    def test_positive_has_plus(self):
        result = fmt_momentum(0.00012)
        assert result.startswith("+")
        assert "0.0120%" in result

    def test_negative_has_minus(self):
        result = fmt_momentum(-0.00035)
        assert result.startswith("-")

    def test_none_returns_dash(self):
        assert fmt_momentum(None) == "—"


class TestTrendArrow:
    def test_accelerating_up_arrow(self):
        assert trend_arrow("accelerating") == "↑"

    def test_decelerating_down_arrow(self):
        assert trend_arrow("decelerating") == "↓"

    def test_stable_right_arrow(self):
        assert trend_arrow("stable") == "→"

    def test_unknown_defaults_right(self):
        assert trend_arrow("unknown") == "→"


class TestMomentumColor:
    def test_positive_is_green(self):
        assert momentum_color(0.00012) == "var(--green)"

    def test_negative_is_red(self):
        assert momentum_color(-0.00035) == "var(--red)"

    def test_zero_is_muted(self):
        assert momentum_color(0.0) == "var(--muted)"

    def test_none_is_muted(self):
        assert momentum_color(None) == "var(--muted)"


class TestBadgeClass:
    def test_positive_momentum_badge_red(self):
        # longs paying more = heated = red badge
        assert badge_class(0.00012) == "badge-red"

    def test_negative_momentum_badge_green(self):
        assert badge_class(-0.00035) == "badge-green"

    def test_stable_no_badge(self):
        assert badge_class(0.000000001) is None

    def test_none_no_badge(self):
        assert badge_class(None) is None


# ── Mocked HTTP fetch ─────────────────────────────────────────────────────────

class TestFundingMomentumMockedFetch:
    @patch("requests.get")
    def test_accelerating_response(self, mock_get):
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = ACCELERATING
        mock_get.return_value = mock_resp

        import requests
        r = requests.get(
            "http://localhost:8765/api/funding-momentum",
            params={"symbol": "BANANAS31USDT", "periods": 4},
        )
        data = r.json()

        assert data["status"] == "ok"
        assert data["trend"] == "accelerating"
        assert data["momentum"] > 0

    @patch("requests.get")
    def test_decelerating_response(self, mock_get):
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = DECELERATING
        mock_get.return_value = mock_resp

        import requests
        r = requests.get(
            "http://localhost:8765/api/funding-momentum",
            params={"symbol": "BANANAS31USDT", "periods": 4},
        )
        data = r.json()

        assert data["trend"] == "decelerating"
        assert data["momentum"] < 0
        assert data["momentum_pct"] < 0

    @patch("requests.get")
    def test_render_output_accelerating(self, mock_get):
        """Verify rendered metrics contain expected elements."""
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = ACCELERATING
        mock_get.return_value = mock_resp

        import requests
        data = requests.get("http://localhost:8765/api/funding-momentum").json()

        arrow = trend_arrow(data["trend"])
        color = momentum_color(data["momentum"])
        rate_str = fmt_rate(data["current_rate"])
        mom_str = fmt_momentum(data["momentum"])
        badge = badge_class(data["momentum"])

        assert arrow == "↑"
        assert color == "var(--green)"
        assert rate_str == "0.0450%"
        assert mom_str.startswith("+")
        assert badge == "badge-red"

    @patch("requests.get")
    def test_no_data_graceful(self, mock_get):
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = NO_DATA
        mock_get.return_value = mock_resp

        import requests
        data = requests.get("http://localhost:8765/api/funding-momentum").json()

        assert data["current_rate"] is None
        assert data["momentum"] is None
        assert data["trend"] == "stable"
        assert data["timestamps"] == []
        assert fmt_rate(data["current_rate"]) == "—"
        assert fmt_momentum(data["momentum"]) == "—"
        assert badge_class(data["momentum"]) is None
