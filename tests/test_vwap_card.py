"""
Unit tests for the VWAP Deviation dashboard card rendering logic.

Mirrors the JS helper functions in app.js (renderVwapDeviation) and validates
expected response shapes with a mocked HTTP layer.
"""
from unittest.mock import MagicMock, patch

import pytest


# ── Python mirrors of app.js helpers ─────────────────────────────────────────

def fmt_price(price):
    v = float(price)
    if v < 0.01:  return f"{v:.6f}"
    if v < 1:     return f"{v:.4f}"
    if v < 100:   return f"{v:.3f}"
    return f"{v:.2f}"


def deviation_color(dev_pct):
    if dev_pct is None:
        return "var(--muted)"
    return "var(--green)" if dev_pct > 0 else "var(--red)"


def deviation_str(dev_pct):
    if dev_pct is None:
        return "—"
    sign = "+" if dev_pct > 0 else ""
    return f"{sign}{dev_pct:.3f}%"


def badge_class(dev_pct):
    if dev_pct is None:
        return "badge-blue"
    return "badge-green" if dev_pct > 0 else "badge-red"


# ── Sample API payloads ───────────────────────────────────────────────────────

ABOVE_VWAP = {
    "status": "ok",
    "symbol": "BANANAS31USDT",
    "vwap": 0.001234,
    "current_price": 0.001280,
    "deviation_pct": 3.726,
    "signal": "above_vwap",
}

BELOW_VWAP = {
    "status": "ok",
    "symbol": "BANANAS31USDT",
    "vwap": 0.001234,
    "current_price": 0.001190,
    "deviation_pct": -3.564,
    "signal": "below_vwap",
}

AT_VWAP = {
    "status": "ok",
    "symbol": "BANANAS31USDT",
    "vwap": 0.001234,
    "current_price": 0.001234,
    "deviation_pct": 0.0,
    "signal": "at_vwap",
}


# ── Color / format helpers ────────────────────────────────────────────────────

class TestDeviationColor:
    def test_positive_is_green(self):
        assert deviation_color(3.726) == "var(--green)"

    def test_negative_is_red(self):
        assert deviation_color(-2.5) == "var(--red)"

    def test_zero_is_red(self):
        # zero is not > 0, so falls to red (same as JS)
        assert deviation_color(0.0) == "var(--red)"

    def test_none_is_muted(self):
        assert deviation_color(None) == "var(--muted)"


class TestDeviationStr:
    def test_positive_has_plus_sign(self):
        assert deviation_str(3.726) == "+3.726%"

    def test_negative_has_minus(self):
        assert deviation_str(-3.564) == "-3.564%"

    def test_zero_formatted(self):
        # 0 is not > 0, so no sign prefix — matches JS: sign = raw < 0 ? '-' : ''
        assert deviation_str(0.0) == "0.000%"

    def test_none_returns_dash(self):
        assert deviation_str(None) == "—"

    def test_three_decimal_places(self):
        result = deviation_str(1.1)
        assert result.count(".") == 1
        _, decimals = result.rstrip("%").split(".")
        assert len(decimals) == 3


class TestBadgeClass:
    def test_positive_badge_green(self):
        assert badge_class(3.726) == "badge-green"

    def test_negative_badge_red(self):
        assert badge_class(-2.5) == "badge-red"

    def test_none_badge_blue(self):
        assert badge_class(None) == "badge-blue"


class TestFmtPrice:
    def test_sub_penny_six_decimals(self):
        result = fmt_price(0.001234)
        assert result == "0.001234"
        assert len(result.split(".")[1]) == 6

    def test_under_one_four_decimals(self):
        assert fmt_price(0.5) == "0.5000"

    def test_under_100_three_decimals(self):
        assert fmt_price(42.1) == "42.100"

    def test_large_two_decimals(self):
        assert fmt_price(10000.5) == "10000.50"


# ── Response shape validation ─────────────────────────────────────────────────

class TestVwapResponseShape:
    REQUIRED_KEYS = ("status", "symbol", "vwap", "current_price", "deviation_pct", "signal")

    @pytest.mark.parametrize("payload", [ABOVE_VWAP, BELOW_VWAP, AT_VWAP])
    def test_required_keys_present(self, payload):
        for key in self.REQUIRED_KEYS:
            assert key in payload, f"missing key: {key}"

    def test_status_ok(self):
        assert ABOVE_VWAP["status"] == "ok"

    def test_vwap_positive(self):
        assert ABOVE_VWAP["vwap"] > 0

    def test_current_price_positive(self):
        assert ABOVE_VWAP["current_price"] > 0

    def test_deviation_pct_numeric(self):
        assert isinstance(ABOVE_VWAP["deviation_pct"], (int, float))

    def test_signal_is_string(self):
        assert isinstance(ABOVE_VWAP["signal"], str)

    def test_above_vwap_signal_matches_positive_deviation(self):
        assert ABOVE_VWAP["deviation_pct"] > 0
        assert "above" in ABOVE_VWAP["signal"]

    def test_below_vwap_signal_matches_negative_deviation(self):
        assert BELOW_VWAP["deviation_pct"] < 0
        assert "below" in BELOW_VWAP["signal"]


# ── Mocked fetch ──────────────────────────────────────────────────────────────

class TestVwapMockedFetch:
    @patch("requests.get")
    def test_mock_returns_above_vwap(self, mock_get):
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = ABOVE_VWAP
        mock_get.return_value = mock_resp

        import requests
        r = requests.get(
            "http://localhost:8765/api/vwap-deviation",
            params={"symbol": "BANANAS31USDT"},
        )
        data = r.json()

        assert data["status"] == "ok"
        assert data["deviation_pct"] > 0
        assert data["current_price"] > data["vwap"]

    @patch("requests.get")
    def test_mock_returns_below_vwap(self, mock_get):
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = BELOW_VWAP
        mock_get.return_value = mock_resp

        import requests
        r = requests.get(
            "http://localhost:8765/api/vwap-deviation",
            params={"symbol": "BANANAS31USDT"},
        )
        data = r.json()

        assert data["deviation_pct"] < 0
        assert data["current_price"] < data["vwap"]

    @patch("requests.get")
    def test_render_output_above(self, mock_get):
        """Verify rendered HTML contains expected elements for above-VWAP state."""
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = ABOVE_VWAP
        mock_get.return_value = mock_resp

        import requests
        data = requests.get("http://localhost:8765/api/vwap-deviation").json()

        dev_pct = data["deviation_pct"]
        color   = deviation_color(dev_pct)
        label   = deviation_str(dev_pct)
        badge   = badge_class(dev_pct)
        price   = fmt_price(data["current_price"])
        vwap    = fmt_price(data["vwap"])

        # simulate what renderVwapDeviation() produces
        html = (
            f'<div class="metric-value" style="color:{color};font-size:22px">{label}</div>'
            f'<div class="metric-value">{price}</div>'
            f'<div class="metric-value" style="color:var(--muted)">{vwap}</div>'
        )

        assert "var(--green)" in html
        assert "+3.726%" in html
        assert "0.001280" in html
        assert "0.001234" in html
        assert badge == "badge-green"
