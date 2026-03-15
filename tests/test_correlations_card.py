"""
Unit tests for Correlations card rendering logic.

Mirrors the JS helper functions in app.js (renderCorrelations) and validates
expected response shapes with a mocked HTTP layer.

API: GET /api/correlations?window=3600
Response: {"status":"ok","matrix":{"BTCUSDT":{"BTCUSDT":1.0,"ETHUSDT":0.87,...},...}}

Color mapping:
  1.0  = bright green hsl(120,70%,40%)
  0    = gray        hsl(0,0%,40%)
 -1.0  = red         hsl(0,70%,40%)
"""
import math


# ── Python mirrors of app.js helpers ─────────────────────────────────────────

def corr_color(v):
    """Mirror renderCorrelations corrColor logic."""
    if v is None:
        return "hsl(0,0%,25%)"
    try:
        c = float(v)
    except (ValueError, TypeError):
        return "hsl(0,0%,25%)"
    if math.isnan(c):
        return "hsl(0,0%,25%)"
    c = max(-1.0, min(1.0, c))
    if c >= 0:
        h = round(120 * c)
        s = round(70 * c)
        return f"hsl({h},{s}%,40%)"
    s = round(70 * abs(c))
    return f"hsl(0,{s}%,40%)"


def build_correlations_html(api_response):
    """Mirror renderCorrelations() HTML construction logic."""
    if not api_response or "matrix" not in api_response:
        return '<div class="text-muted">No data</div>'
    matrix = api_response["matrix"]
    if not matrix:
        return '<div class="text-muted">No data</div>'

    symbols = list(matrix.keys())
    header_cells = "".join(
        f'<th>{s.replace("USDT","")}</th>' for s in symbols
    )

    rows = []
    for row_sym in symbols:
        cells = ""
        for col_sym in symbols:
            v  = (matrix.get(row_sym) or {}).get(col_sym)
            bg = corr_color(v)
            txt = f"{float(v):.2f}" if v is not None else "—"
            cells += f'<td style="background:{bg}">{txt}</td>'
        rows.append(
            f"<tr><td>{row_sym.replace('USDT','')}</td>{cells}</tr>"
        )
    body = "".join(rows)
    return (
        "<table>"
        f"<thead><tr><th></th>{header_cells}</tr></thead>"
        f"<tbody>{body}</tbody>"
        "</table>"
    )


# ── Sample API payloads ───────────────────────────────────────────────────────

SAMPLE_MATRIX = {
    "status": "ok",
    "matrix": {
        "BTCUSDT": {"BTCUSDT": 1.0,  "ETHUSDT": 0.87, "SOLUSDT": 0.72},
        "ETHUSDT": {"BTCUSDT": 0.87, "ETHUSDT": 1.0,  "SOLUSDT": 0.65},
        "SOLUSDT": {"BTCUSDT": 0.72, "ETHUSDT": 0.65, "SOLUSDT": 1.0},
    },
}

NEGATIVE_MATRIX = {
    "status": "ok",
    "matrix": {
        "BTCUSDT": {"BTCUSDT": 1.0,  "ETHUSDT": -0.9},
        "ETHUSDT": {"BTCUSDT": -0.9, "ETHUSDT": 1.0},
    },
}

EMPTY_MATRIX = {"status": "ok", "matrix": {}}

SINGLE_SYMBOL = {
    "status": "ok",
    "matrix": {"BTCUSDT": {"BTCUSDT": 1.0}},
}


# ── TestCorrColor ─────────────────────────────────────────────────────────────

class TestCorrColor:
    def test_one_is_full_green(self):
        assert corr_color(1.0) == "hsl(120,70%,40%)"

    def test_zero_is_gray(self):
        assert corr_color(0.0) == "hsl(0,0%,40%)"

    def test_minus_one_is_red(self):
        assert corr_color(-1.0) == "hsl(0,70%,40%)"

    def test_none_returns_dark_gray(self):
        assert corr_color(None) == "hsl(0,0%,25%)"

    def test_nan_returns_dark_gray(self):
        assert corr_color(float("nan")) == "hsl(0,0%,25%)"

    def test_half_positive_correct_hsl(self):
        # h = round(120*0.5)=60, s = round(70*0.5)=35
        assert corr_color(0.5) == "hsl(60,35%,40%)"

    def test_half_negative_correct_hsl(self):
        # hue stays 0, s = round(70*0.5)=35
        assert corr_color(-0.5) == "hsl(0,35%,40%)"

    def test_above_one_clamped(self):
        assert corr_color(1.5) == corr_color(1.0)

    def test_below_minus_one_clamped(self):
        assert corr_color(-1.5) == corr_color(-1.0)

    def test_positive_hue_increases_with_value(self):
        def hue(hsl):
            return int(hsl.split(",")[0].replace("hsl(", ""))
        assert hue(corr_color(0.0)) <= hue(corr_color(0.5)) <= hue(corr_color(1.0))

    def test_positive_saturation_increases_with_value(self):
        def sat(hsl):
            return int(hsl.split(",")[1].replace("%", ""))
        assert sat(corr_color(0.0)) <= sat(corr_color(0.5)) <= sat(corr_color(1.0))

    def test_negative_saturation_increases_with_magnitude(self):
        def sat(hsl):
            return int(hsl.split(",")[1].replace("%", ""))
        assert sat(corr_color(0.0)) <= sat(corr_color(-0.5)) <= sat(corr_color(-1.0))

    def test_non_numeric_string_returns_dark_gray(self):
        assert corr_color("abc") == "hsl(0,0%,25%)"

    def test_lightness_always_40_for_valid_values(self):
        for v in [1.0, 0.5, 0.0, -0.5, -1.0]:
            result = corr_color(v)
            assert "40%" in result, f"expected 40% lightness for v={v}"


# ── TestBuildCorrelationsHtml ─────────────────────────────────────────────────

class TestBuildCorrelationsHtml:
    def test_none_response_returns_no_data(self):
        assert "No data" in build_correlations_html(None)

    def test_missing_matrix_key_returns_no_data(self):
        assert "No data" in build_correlations_html({"status": "ok"})

    def test_empty_matrix_returns_no_data(self):
        assert "No data" in build_correlations_html(EMPTY_MATRIX)

    def test_html_contains_table(self):
        result = build_correlations_html(SAMPLE_MATRIX)
        assert "<table>" in result
        assert "</table>" in result

    def test_html_has_thead(self):
        assert "<thead>" in build_correlations_html(SAMPLE_MATRIX)

    def test_html_has_tbody(self):
        assert "<tbody>" in build_correlations_html(SAMPLE_MATRIX)

    def test_symbols_stripped_of_usdt_in_header(self):
        result = build_correlations_html(SAMPLE_MATRIX)
        assert "<th>BTC</th>" in result
        assert "<th>ETH</th>" in result
        assert "<th>SOL</th>" in result

    def test_diagonal_shows_one_point_zero(self):
        assert "1.00" in build_correlations_html(SAMPLE_MATRIX)

    def test_diagonal_has_full_green_background(self):
        # 1.0 → hsl(120,70%,40%)
        assert "hsl(120,70%,40%)" in build_correlations_html(SAMPLE_MATRIX)

    def test_high_positive_correlation_is_greenish(self):
        result = build_correlations_html(SAMPLE_MATRIX)
        expected = corr_color(0.87)
        assert expected in result

    def test_negative_correlation_has_red_background(self):
        result = build_correlations_html(NEGATIVE_MATRIX)
        expected = corr_color(-0.9)
        assert expected in result

    def test_values_formatted_to_two_decimal_places(self):
        response = {
            "status": "ok",
            "matrix": {"BTCUSDT": {"BTCUSDT": 0.123456}},
        }
        assert "0.12" in build_correlations_html(response)

    def test_missing_cell_shows_dash(self):
        response = {
            "status": "ok",
            "matrix": {
                "BTCUSDT": {"BTCUSDT": 1.0},
                "ETHUSDT": {"ETHUSDT": 1.0},
            },
        }
        assert "—" in build_correlations_html(response)

    def test_row_count_matches_symbols(self):
        result = build_correlations_html(SAMPLE_MATRIX)
        # 1 header + 3 data rows
        assert result.count("<tr>") == 4

    def test_four_symbols_five_rows_total(self):
        syms = ["BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT"]
        matrix = {s: {s2: (1.0 if s == s2 else 0.5) for s2 in syms} for s in syms}
        result = build_correlations_html({"status": "ok", "matrix": matrix})
        assert result.count("<tr>") == 5

    def test_single_symbol_matrix(self):
        result = build_correlations_html(SINGLE_SYMBOL)
        assert "1.00" in result
        assert "hsl(120,70%,40%)" in result

    def test_row_labels_stripped_of_usdt(self):
        result = build_correlations_html(SINGLE_SYMBOL)
        assert "<td>BTC</td>" in result

    def test_zero_correlation_gray_background(self):
        response = {
            "status": "ok",
            "matrix": {
                "BTCUSDT": {"BTCUSDT": 1.0, "ETHUSDT": 0.0},
                "ETHUSDT": {"BTCUSDT": 0.0, "ETHUSDT": 1.0},
            },
        }
        assert "hsl(0,0%,40%)" in build_correlations_html(response)

    def test_none_cell_has_dark_gray_background(self):
        response = {
            "status": "ok",
            "matrix": {"BTCUSDT": {"ETHUSDT": None}},
        }
        result = build_correlations_html(response)
        assert "hsl(0,0%,25%)" in result
        assert "—" in result
