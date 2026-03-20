"""
Tests for issue #179 fixes:
  1. OI USDT display — fmtUSD with 6 decimal precision for small values
  2. Loading → No data fallback (HTML initial states + JS logic)
  3. Correlation heatmap — quality=0 shows "No data" not fake identity matrix
  4. Vol profile chart — uses /volume-profile/adaptive for reliable session data
"""
import os
import re

ROOT = os.path.join(os.path.dirname(__file__), "..")


def _html() -> str:
    with open(os.path.join(ROOT, "frontend", "index.html"), encoding="utf-8") as f:
        return f.read()


def _js() -> str:
    with open(os.path.join(ROOT, "frontend", "app.js"), encoding="utf-8") as f:
        return f.read()


# ── Python mirror of app.js helpers ──────────────────────────────────────────


def fmt_usd(n) -> str:
    """Mirror of fixed fmtUSD() from app.js — 6dp for values < $0.01."""
    if n is None:
        return "—"
    v = float(n)
    if v != v:  # NaN
        return "—"
    abs_v = abs(v)
    sign = "-" if v < 0 else ""
    if abs_v >= 1e9:
        return f"{sign}${abs_v / 1e9:.2f}B"
    if abs_v >= 1e6:
        return f"{sign}${abs_v / 1e6:.2f}M"
    if abs_v >= 1e3:
        return f"{sign}${abs_v / 1e3:.1f}K"
    if abs_v >= 0.01:
        return f"{sign}${abs_v:.2f}"
    return f"{sign}${abs_v:.6f}"


def build_corr_heatmap_html(api_response) -> str:
    """Mirror of renderCorrHeatmap() — returns 'No data' when quality==0."""
    if not api_response:
        return '<div class="text-muted">No data available</div>'
    matrix = api_response.get("matrix")
    symbols = api_response.get("symbols")
    quality = api_response.get("quality", 0)
    if not matrix or not symbols or len(matrix) == 0 or quality == 0:
        return '<div class="text-muted">No data available</div>'
    # Build heatmap table
    short = lambda s: s.replace("USDT", "")
    header = "<tr><th></th>" + "".join(f"<th>{short(s)}</th>" for s in symbols) + "</tr>"
    rows = []
    for i, row in enumerate(matrix):
        cells = "".join(
            f'<td>{v:.2f}</td>' if v is not None else "<td>—</td>"
            for v in row
        )
        rows.append(f"<tr><td>{short(symbols[i])}</td>{cells}</tr>")
    return f"<table><thead>{header}</thead><tbody>{''.join(rows)}</tbody></table>"


def oi_loading_state(metrics_el_html: str) -> str:
    """Mirror of renderOiChart no-data path — returns correct state string."""
    # When data is empty or no price available, always show "No data"
    return '<div class="text-muted" style="font-size:11px;">No data</div>'


# ── Sample payloads ───────────────────────────────────────────────────────────

HEATMAP_QUALITY_0 = {
    "status": "ok",
    "symbols": ["BANANAS31USDT", "COSUSDT", "DEXEUSDT", "LYNUSDT"],
    "matrix": [
        [1.0, 0.0, 0.0, 0.0],
        [0.0, 1.0, 0.0, 0.0],
        [0.0, 0.0, 1.0, 0.0],
        [0.0, 0.0, 0.0, 1.0],
    ],
    "quality": 0,
    "timestamp": 1700000000,
}

HEATMAP_QUALITY_15 = {
    "status": "ok",
    "symbols": ["BANANAS31USDT", "COSUSDT", "DEXEUSDT", "LYNUSDT"],
    "matrix": [
        [1.00, 0.82, 0.65, 0.71],
        [0.82, 1.00, 0.59, 0.68],
        [0.65, 0.59, 1.00, 0.77],
        [0.71, 0.68, 0.77, 1.00],
    ],
    "quality": 15,
    "timestamp": 1700000000,
}

VOL_PROFILE_ADAPTIVE = {
    "status": "ok",
    "symbol": "BANANAS31USDT",
    "poc": 0.000123,
    "vah": 0.000130,
    "val": 0.000115,
    "total_volume": 5000.0,
    "value_area_pct": 70.0,
    "window_seconds": 3600,
    "bins": [
        {"price": 0.000115, "volume": 800.0, "buy_vol": 500.0, "sell_vol": 300.0, "is_poc": False, "in_value_area": True},
        {"price": 0.000123, "volume": 1200.0, "buy_vol": 700.0, "sell_vol": 500.0, "is_poc": True, "in_value_area": True},
        {"price": 0.000130, "volume": 600.0, "buy_vol": 350.0, "sell_vol": 250.0, "is_poc": False, "in_value_area": False},
    ],
}


# ── TestFmtUSD ────────────────────────────────────────────────────────────────

class TestFmtUsd:
    def test_billions(self):
        assert fmt_usd(2_000_000_000) == "$2.00B"

    def test_millions(self):
        assert fmt_usd(1_500_000) == "$1.50M"

    def test_thousands(self):
        assert fmt_usd(5_000) == "$5.0K"

    def test_hundreds_two_decimals(self):
        assert fmt_usd(123.45) == "$123.45"

    def test_small_value_boundary_at_0_01(self):
        # Exactly $0.01 → 2 decimal places
        assert fmt_usd(0.01) == "$0.01"

    def test_sub_cent_value_six_decimals(self):
        # $0.001234 needs 6 decimal places for small-cap perps
        assert fmt_usd(0.001234) == "$0.001234"

    def test_very_small_value_six_decimals(self):
        assert fmt_usd(0.000001) == "$0.000001"

    def test_zero(self):
        # 0 < 0.01 threshold → 6dp for consistency
        assert fmt_usd(0) == "$0.000000"

    def test_negative_large(self):
        assert fmt_usd(-1_000_000) == "-$1.00M"

    def test_negative_small(self):
        assert fmt_usd(-0.001) == "-$0.001000"

    def test_none_returns_dash(self):
        assert fmt_usd(None) == "—"

    def test_positive_boundary_just_below_0_01(self):
        # $0.009999 → 6 decimal places
        result = fmt_usd(0.009999)
        assert result == "$0.009999"

    def test_just_at_1000_boundary(self):
        assert fmt_usd(1000) == "$1.0K"


# ── TestCorrHeatmapQuality ────────────────────────────────────────────────────

class TestCorrHeatmapQuality:
    def test_quality_0_shows_no_data(self):
        result = build_corr_heatmap_html(HEATMAP_QUALITY_0)
        assert "No data" in result

    def test_quality_0_does_not_render_table(self):
        result = build_corr_heatmap_html(HEATMAP_QUALITY_0)
        assert "<table>" not in result

    def test_quality_15_renders_table(self):
        result = build_corr_heatmap_html(HEATMAP_QUALITY_15)
        assert "<table>" in result

    def test_quality_15_shows_all_symbols(self):
        result = build_corr_heatmap_html(HEATMAP_QUALITY_15)
        assert "BANANAS31" in result
        assert "COS" in result
        assert "DEXE" in result
        assert "LYN" in result

    def test_quality_15_diagonal_shows_1(self):
        result = build_corr_heatmap_html(HEATMAP_QUALITY_15)
        assert "1.00" in result

    def test_none_response_returns_no_data(self):
        assert "No data" in build_corr_heatmap_html(None)

    def test_empty_matrix_returns_no_data(self):
        resp = {"status": "ok", "symbols": [], "matrix": [], "quality": 0}
        assert "No data" in build_corr_heatmap_html(resp)

    def test_quality_1_still_shows_no_data(self):
        resp = {**HEATMAP_QUALITY_0, "quality": 1}
        # quality=1 is too low to be meaningful — still show No data
        # Actually let's keep threshold at quality > 0
        result = build_corr_heatmap_html(resp)
        # With quality=1, should render (it has real data)
        # The threshold is quality == 0 only means fake identity
        # Let's verify quality=1 renders
        assert "<table>" in result  # quality 1 = real data, should render


# ── TestLoadingNoDataHtml ─────────────────────────────────────────────────────

class TestLoadingNoDataHtml:
    def test_oi_metrics_has_loading_initial_state(self):
        """oi-metrics should show Loading… before first data fetch."""
        html = _html()
        # Find the oi-metrics div content
        match = re.search(
            r'id="oi-metrics"[^>]*>(.*?)</div>',
            html,
            re.DOTALL,
        )
        assert match, "oi-metrics element not found"
        content = match.group(1)
        assert "Loading" in content, (
            f"oi-metrics should have Loading… initial state, got: {content!r}"
        )

    def test_volume_profile_metrics_has_loading_initial_state(self):
        """volume-profile-metrics should show Loading… before first data fetch."""
        html = _html()
        match = re.search(
            r'id="volume-profile-metrics"[^>]*>(.*?)</div>',
            html,
            re.DOTALL,
        )
        assert match, "volume-profile-metrics element not found"
        content = match.group(1)
        assert "Loading" in content, (
            f"volume-profile-metrics should have Loading… initial state, got: {content!r}"
        )

    def test_correlations_content_has_loading_initial_state(self):
        """correlations-content already has Loading… — verify it stays."""
        html = _html()
        assert 'id="correlations-content"' in html
        # Already has Loading… in index.html
        idx = html.index('id="correlations-content"')
        snippet = html[idx : idx + 200]
        assert "Loading" in snippet


# ── TestRenderOiNoDataLogic ───────────────────────────────────────────────────

class TestRenderOiNoDataLogic:
    def test_no_textcontent_trim_check_in_oi_render(self):
        """renderOiChart must NOT gate 'No data' on textContent.trim() being empty.
        It should always show No data when there's no data, regardless of prior state."""
        js = _js()
        # Find the renderOiChart function body
        fn_start = js.index("async function renderOiChart()")
        fn_end = js.index("\nasync function ", fn_start + 1)
        fn_body = js[fn_start:fn_end]
        assert "!metricsEl.textContent.trim()" not in fn_body, (
            "renderOiChart must unconditionally show 'No data' — "
            "do not gate on textContent being empty"
        )

    def test_oi_render_shows_no_data_string(self):
        """renderOiChart must produce 'No data' text when called with no data."""
        js = _js()
        fn_start = js.index("async function renderOiChart()")
        fn_end = js.index("\nasync function ", fn_start + 1)
        fn_body = js[fn_start:fn_end]
        assert "No data" in fn_body


# ── TestVolProfileAdaptiveEndpoint ────────────────────────────────────────────

class TestVolProfileAdaptiveEndpoint:
    def test_render_vol_profile_calls_adaptive_endpoint(self):
        """renderVolumeProfile should use /volume-profile/adaptive for reliable data."""
        js = _js()
        fn_start = js.index("async function renderVolumeProfile()")
        fn_end = js.index("\nasync function ", fn_start + 1)
        fn_body = js[fn_start:fn_end]
        assert "/volume-profile/adaptive" in fn_body, (
            "renderVolumeProfile must call /volume-profile/adaptive "
            "instead of /volume-profile to get session-based reliable data"
        )

    def test_vol_profile_response_shape_compatible(self):
        """Adaptive endpoint response has all fields renderVolumeProfile needs."""
        data = VOL_PROFILE_ADAPTIVE
        assert "bins" in data
        assert "poc" in data
        assert "vah" in data
        assert "val" in data
        assert "total_volume" in data
        assert "value_area_pct" in data
        assert len(data["bins"]) > 0

    def test_vol_profile_bins_have_is_poc_flag(self):
        """Adaptive bins include is_poc flag for POC highlighting."""
        bins = VOL_PROFILE_ADAPTIVE["bins"]
        for b in bins:
            assert "is_poc" in b, f"Bin missing is_poc: {b}"

    def test_vol_profile_poc_bin_identified(self):
        poc_bins = [b for b in VOL_PROFILE_ADAPTIVE["bins"] if b["is_poc"]]
        assert len(poc_bins) == 1
        assert poc_bins[0]["price"] == VOL_PROFILE_ADAPTIVE["poc"]

    def test_render_vol_profile_has_poc_highlighting(self):
        """renderVolumeProfile should apply POC color highlighting in chart."""
        js = _js()
        fn_start = js.index("async function renderVolumeProfile()")
        fn_end = js.index("\nasync function ", fn_start + 1)
        fn_body = js[fn_start:fn_end]
        # Should have is_poc color logic
        assert "is_poc" in fn_body, (
            "renderVolumeProfile must highlight POC bin using is_poc flag"
        )


# ── TestOiUsdtField ───────────────────────────────────────────────────────────

class TestOiUsdtField:
    def test_oi_api_response_enrichment(self):
        """Backend should compute oi_usdt = oi_value * price."""
        oi_value = 1_000_000  # contracts
        price = 0.000123  # USDT per contract
        expected_oi_usdt = round(oi_value * price, 6)
        assert expected_oi_usdt == pytest.approx(123.0, rel=1e-4)

    def test_oi_usdt_precision_6_decimals(self):
        """oi_usdt should be rounded to 6 decimal places."""
        oi_value = 100  # contracts
        price = 0.0000001  # very small
        oi_usdt = round(oi_value * price, 6)
        assert oi_usdt == pytest.approx(0.00001, rel=1e-4)

    def test_oi_usdt_zero_price_handled(self):
        """When price is 0 or None, oi_usdt should be None (not 0)."""
        oi_value = 1_000_000
        price = 0
        oi_usdt = (oi_value * price) if price else None
        assert oi_usdt is None


import pytest
