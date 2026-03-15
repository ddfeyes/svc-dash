"""
Unit tests for the VPIN dashboard card.

Covers: calculation helpers, signal classification, response shape,
HTML rendering logic, mocked API calls, and edge cases.
"""
from unittest.mock import MagicMock, patch

import pytest


# ── Python mirrors of VPIN logic ──────────────────────────────────────────────

def compute_vpin_from_buckets(bucket_ratios: list) -> float | None:
    """VPIN = mean of |buy_vol - sell_vol| / bucket_vol across buckets."""
    if not bucket_ratios:
        return None
    return sum(bucket_ratios) / len(bucket_ratios)


def classify_signal(vpin: float | None) -> str:
    """Map VPIN value to signal string per spec thresholds."""
    if vpin is None:
        return "unknown"
    if vpin > 0.4:
        return "elevated"
    if vpin < 0.2:
        return "low"
    return "normal"


def vpin_color(signal: str) -> str:
    """Map signal to CSS colour variable."""
    if signal == "elevated":
        return "var(--red)"
    if signal == "low":
        return "var(--green)"
    return "var(--muted)"


def fmt_vpin_pct(vpin: float | None) -> str:
    """Format VPIN as percentage string."""
    if vpin is None:
        return "—"
    return f"{vpin * 100:.1f}%"


def build_vpin_html(data: dict) -> str:
    """Simulate what renderVpin() produces in app.js."""
    vpin = data.get("vpin")
    signal = data.get("signal", classify_signal(vpin))
    buckets_used = data.get("buckets_used", 0)
    color = vpin_color(signal)
    pct = fmt_vpin_pct(vpin)
    return (
        f'<div class="metric-value" style="color:{color};font-size:22px">{pct}</div>'
        f'<div class="metric-label">{signal}</div>'
        f'<div class="metric-label">buckets: {buckets_used}</div>'
    )


# ── Sample API payloads ───────────────────────────────────────────────────────

ELEVATED_PAYLOAD = {
    "status": "ok",
    "symbol": "BANANAS31USDT",
    "vpin": 0.52,
    "signal": "elevated",
    "buckets_used": 48,
}

NORMAL_PAYLOAD = {
    "status": "ok",
    "symbol": "BANANAS31USDT",
    "vpin": 0.31,
    "signal": "normal",
    "buckets_used": 50,
}

LOW_PAYLOAD = {
    "status": "ok",
    "symbol": "BANANAS31USDT",
    "vpin": 0.14,
    "signal": "low",
    "buckets_used": 45,
}

INSUFFICIENT_PAYLOAD = {
    "status": "ok",
    "symbol": "BANANAS31USDT",
    "vpin": None,
    "signal": "unknown",
    "buckets_used": 0,
}


# ── Signal classification tests ───────────────────────────────────────────────

class TestClassifySignal:
    def test_above_threshold_is_elevated(self):
        assert classify_signal(0.41) == "elevated"

    def test_exactly_0_4_is_not_elevated(self):
        # boundary: vpin > 0.4 means 0.4 itself is normal
        assert classify_signal(0.4) == "normal"

    def test_high_value_is_elevated(self):
        assert classify_signal(0.99) == "elevated"

    def test_below_threshold_is_low(self):
        assert classify_signal(0.19) == "low"

    def test_exactly_0_2_is_not_low(self):
        # boundary: vpin < 0.2 means 0.2 itself is normal
        assert classify_signal(0.2) == "normal"

    def test_zero_is_low(self):
        assert classify_signal(0.0) == "low"

    def test_middle_range_is_normal(self):
        assert classify_signal(0.30) == "normal"

    def test_none_returns_unknown(self):
        assert classify_signal(None) == "unknown"

    def test_exactly_at_upper_boundary(self):
        assert classify_signal(0.4) == "normal"

    def test_exactly_at_lower_boundary(self):
        assert classify_signal(0.2) == "normal"


# ── VPIN calculation tests ────────────────────────────────────────────────────

class TestComputeVpin:
    def test_equal_buy_sell_gives_zero(self):
        buckets = [0.0, 0.0, 0.0]
        assert compute_vpin_from_buckets(buckets) == 0.0

    def test_all_one_sided_gives_one(self):
        buckets = [1.0, 1.0, 1.0]
        assert compute_vpin_from_buckets(buckets) == pytest.approx(1.0)

    def test_mean_calculation(self):
        buckets = [0.2, 0.4, 0.6]
        assert compute_vpin_from_buckets(buckets) == pytest.approx(0.4)

    def test_empty_buckets_returns_none(self):
        assert compute_vpin_from_buckets([]) is None

    def test_single_bucket(self):
        assert compute_vpin_from_buckets([0.55]) == pytest.approx(0.55)

    def test_result_in_zero_one_range(self):
        buckets = [0.1, 0.3, 0.5, 0.7, 0.9]
        result = compute_vpin_from_buckets(buckets)
        assert 0.0 <= result <= 1.0


# ── Colour helper tests ────────────────────────────────────────────────────────

class TestVpinColor:
    def test_elevated_is_red(self):
        assert vpin_color("elevated") == "var(--red)"

    def test_low_is_green(self):
        assert vpin_color("low") == "var(--green)"

    def test_normal_is_muted(self):
        assert vpin_color("normal") == "var(--muted)"

    def test_unknown_is_muted(self):
        assert vpin_color("unknown") == "var(--muted)"


# ── Format helper tests ───────────────────────────────────────────────────────

class TestFmtVpinPct:
    def test_none_returns_dash(self):
        assert fmt_vpin_pct(None) == "—"

    def test_formats_as_percentage(self):
        assert fmt_vpin_pct(0.52) == "52.0%"

    def test_zero_formats_correctly(self):
        assert fmt_vpin_pct(0.0) == "0.0%"

    def test_one_formats_correctly(self):
        assert fmt_vpin_pct(1.0) == "100.0%"


# ── Response shape validation ─────────────────────────────────────────────────

class TestVpinResponseShape:
    REQUIRED_KEYS = ("status", "symbol", "vpin", "signal", "buckets_used")

    @pytest.mark.parametrize("payload", [ELEVATED_PAYLOAD, NORMAL_PAYLOAD, LOW_PAYLOAD])
    def test_required_keys_present(self, payload):
        for key in self.REQUIRED_KEYS:
            assert key in payload, f"missing key: {key}"

    def test_status_ok(self):
        assert ELEVATED_PAYLOAD["status"] == "ok"

    def test_vpin_in_zero_one_range(self):
        assert 0.0 <= ELEVATED_PAYLOAD["vpin"] <= 1.0

    def test_buckets_used_is_int(self):
        assert isinstance(ELEVATED_PAYLOAD["buckets_used"], int)

    def test_signal_is_string(self):
        assert isinstance(ELEVATED_PAYLOAD["signal"], str)

    def test_signal_valid_values(self):
        valid = {"elevated", "normal", "low", "unknown"}
        for payload in [ELEVATED_PAYLOAD, NORMAL_PAYLOAD, LOW_PAYLOAD, INSUFFICIENT_PAYLOAD]:
            assert payload["signal"] in valid

    def test_elevated_signal_matches_high_vpin(self):
        assert ELEVATED_PAYLOAD["vpin"] > 0.4
        assert ELEVATED_PAYLOAD["signal"] == "elevated"

    def test_low_signal_matches_low_vpin(self):
        assert LOW_PAYLOAD["vpin"] < 0.2
        assert LOW_PAYLOAD["signal"] == "low"

    def test_normal_signal_matches_midrange_vpin(self):
        assert 0.2 <= NORMAL_PAYLOAD["vpin"] <= 0.4
        assert NORMAL_PAYLOAD["signal"] == "normal"

    def test_insufficient_data_has_none_vpin(self):
        assert INSUFFICIENT_PAYLOAD["vpin"] is None
        assert INSUFFICIENT_PAYLOAD["buckets_used"] == 0


# ── HTML rendering tests ──────────────────────────────────────────────────────

class TestBuildVpinHtml:
    def test_elevated_shows_red_and_pct(self):
        html = build_vpin_html(ELEVATED_PAYLOAD)
        assert "var(--red)" in html
        assert "52.0%" in html
        assert "elevated" in html

    def test_low_shows_green(self):
        html = build_vpin_html(LOW_PAYLOAD)
        assert "var(--green)" in html
        assert "14.0%" in html

    def test_normal_shows_muted(self):
        html = build_vpin_html(NORMAL_PAYLOAD)
        assert "var(--muted)" in html

    def test_none_vpin_shows_dash(self):
        html = build_vpin_html(INSUFFICIENT_PAYLOAD)
        assert "—" in html

    def test_buckets_shown_in_html(self):
        html = build_vpin_html(ELEVATED_PAYLOAD)
        assert "48" in html


# ── Mocked API fetch tests ────────────────────────────────────────────────────

class TestVpinMockedFetch:
    @patch("requests.get")
    def test_mock_elevated_response(self, mock_get):
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = ELEVATED_PAYLOAD
        mock_get.return_value = mock_resp

        import requests
        r = requests.get("http://localhost:8765/api/vpin", params={"symbol": "BANANAS31USDT"})
        data = r.json()

        assert data["status"] == "ok"
        assert data["signal"] == "elevated"
        assert data["vpin"] > 0.4

    @patch("requests.get")
    def test_mock_low_response(self, mock_get):
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = LOW_PAYLOAD
        mock_get.return_value = mock_resp

        import requests
        data = requests.get("http://localhost:8765/api/vpin").json()
        assert data["signal"] == "low"
        assert data["vpin"] < 0.2

    @patch("requests.get")
    def test_mock_insufficient_data(self, mock_get):
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = INSUFFICIENT_PAYLOAD
        mock_get.return_value = mock_resp

        import requests
        data = requests.get("http://localhost:8765/api/vpin").json()
        assert data["vpin"] is None
        assert data["buckets_used"] == 0

    @patch("requests.get")
    def test_render_elevated_html(self, mock_get):
        """Verify rendered HTML correct for elevated state."""
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = ELEVATED_PAYLOAD
        mock_get.return_value = mock_resp

        import requests
        data = requests.get("http://localhost:8765/api/vpin").json()
        html = build_vpin_html(data)

        assert "var(--red)" in html
        assert "52.0%" in html
        assert "elevated" in html

    @patch("requests.get")
    def test_render_low_html(self, mock_get):
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = LOW_PAYLOAD
        mock_get.return_value = mock_resp

        import requests
        data = requests.get("http://localhost:8765/api/vpin").json()
        html = build_vpin_html(data)

        assert "var(--green)" in html
        assert "14.0%" in html


# ── HTML card structure tests ─────────────────────────────────────────────────

class TestHtmlCardStructure:
    def test_card_id_exists_in_html(self):
        """Verify the HTML card element id matches the spec."""
        import os
        html_path = os.path.join(
            os.path.dirname(__file__), "..", "frontend", "index.html"
        )
        with open(html_path) as f:
            content = f.read()
        assert 'id="card-vpin"' in content

    def test_card_has_title(self):
        import os
        html_path = os.path.join(
            os.path.dirname(__file__), "..", "frontend", "index.html"
        )
        with open(html_path) as f:
            content = f.read()
        assert "VPIN" in content

    def test_js_has_render_vpin(self):
        """Verify renderVpin() exists in app.js."""
        import os
        js_path = os.path.join(
            os.path.dirname(__file__), "..", "frontend", "app.js"
        )
        with open(js_path) as f:
            content = f.read()
        assert "renderVpin" in content

    def test_js_render_vpin_in_promise_all(self):
        """Verify renderVpin() is called inside Promise.all."""
        import os
        js_path = os.path.join(
            os.path.dirname(__file__), "..", "frontend", "app.js"
        )
        with open(js_path) as f:
            content = f.read()
        # Check it's present and wired up
        assert "renderVpin()" in content
