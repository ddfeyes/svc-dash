"""
Unit tests for Momentum card rendering logic.

Mirrors the JS helper functions in app.js (renderMomentum) and validates
expected response shapes with a mocked HTTP layer.

API: GET /api/momentum
Response: {"status":"ok","symbols":{"BTCUSDT":{"1h":0.12,"4h":-0.5,"24h":2.1},...}}
"""
import math


# ── Python mirrors of app.js helpers ─────────────────────────────────────────

def pct_color(v):
    """Mirror fmtPct color logic: green >0, red <0, muted otherwise."""
    if v is None:
        return "var(--muted)"
    try:
        n = float(v)
    except (ValueError, TypeError):
        return "var(--muted)"
    if math.isnan(n):
        return "var(--muted)"
    if n > 0:
        return "var(--green)"
    if n < 0:
        return "var(--red)"
    return "var(--muted)"


def fmt_pct(v):
    """Mirror renderMomentum fmtPct: returns colored span HTML."""
    if v is None:
        return '<span style="color:var(--muted)">—</span>'
    try:
        n = float(v)
    except (ValueError, TypeError):
        return '<span style="color:var(--muted)">—</span>'
    if math.isnan(n):
        return '<span style="color:var(--muted)">—</span>'
    sign = "+" if n > 0 else ""
    col = "var(--green)" if n > 0 else ("var(--red)" if n < 0 else "var(--muted)")
    return f'<span style="color:{col}">{sign}{n:.2f}%</span>'


def build_momentum_html(api_response):
    """Mirror renderMomentum() HTML construction logic."""
    if not api_response or "symbols" not in api_response:
        return '<div class="text-muted">No data</div>'
    symbols_data = api_response["symbols"]
    if not symbols_data:
        return '<div class="text-muted">No data</div>'

    rows = []
    for sym, d in symbols_data.items():
        h1  = d.get("1h")
        h4  = d.get("4h")
        h24 = d.get("24h")
        rows.append(
            f"<tr>"
            f'<td class="sym">{sym.replace("USDT", "")}</td>'
            f"<td>{fmt_pct(h1)}</td>"
            f"<td>{fmt_pct(h4)}</td>"
            f"<td>{fmt_pct(h24)}</td>"
            f"</tr>"
        )
    body = "".join(rows)
    return (
        "<table>"
        "<thead><tr><th>Symbol</th><th>1h</th><th>4h</th><th>24h</th></tr></thead>"
        f"<tbody>{body}</tbody>"
        "</table>"
    )


# ── Sample API payloads ───────────────────────────────────────────────────────

SAMPLE_RESPONSE = {
    "status": "ok",
    "symbols": {
        "BTCUSDT":      {"1h": 0.12, "4h": -0.5,  "24h": 2.1},
        "ETHUSDT":      {"1h": 0.30, "4h":  0.8,  "24h": 1.5},
        "BANANAS31USDT":{"1h": -1.2, "4h": -2.1,  "24h": -3.4},
    },
}

EMPTY_SYMBOLS = {"status": "ok", "symbols": {}}

NULL_VALUES = {
    "status": "ok",
    "symbols": {"BTCUSDT": {"1h": None, "4h": None, "24h": None}},
}

MISSING_TIMEFRAMES = {
    "status": "ok",
    "symbols": {"BTCUSDT": {}},
}


# ── TestPctColor ──────────────────────────────────────────────────────────────

class TestPctColor:
    def test_positive_returns_green(self):
        assert pct_color(1.5) == "var(--green)"

    def test_negative_returns_red(self):
        assert pct_color(-0.5) == "var(--red)"

    def test_zero_returns_muted(self):
        assert pct_color(0) == "var(--muted)"

    def test_none_returns_muted(self):
        assert pct_color(None) == "var(--muted)"

    def test_nan_returns_muted(self):
        assert pct_color(float("nan")) == "var(--muted)"

    def test_large_positive_returns_green(self):
        assert pct_color(100.0) == "var(--green)"

    def test_large_negative_returns_red(self):
        assert pct_color(-99.9) == "var(--red)"

    def test_small_positive_returns_green(self):
        assert pct_color(0.001) == "var(--green)"

    def test_small_negative_returns_red(self):
        assert pct_color(-0.001) == "var(--red)"

    def test_non_numeric_string_returns_muted(self):
        assert pct_color("abc") == "var(--muted)"


# ── TestFmtPct ────────────────────────────────────────────────────────────────

class TestFmtPct:
    def test_positive_has_green_color(self):
        assert "var(--green)" in fmt_pct(2.1)

    def test_positive_has_plus_sign(self):
        assert "+2.10%" in fmt_pct(2.1)

    def test_negative_has_red_color(self):
        assert "var(--red)" in fmt_pct(-0.5)

    def test_negative_no_plus_sign(self):
        result = fmt_pct(-0.5)
        assert "-0.50%" in result
        assert "+" not in result

    def test_zero_has_muted_color(self):
        assert "var(--muted)" in fmt_pct(0)

    def test_none_returns_dash_span(self):
        result = fmt_pct(None)
        assert "—" in result
        assert "var(--muted)" in result

    def test_nan_returns_dash_span(self):
        result = fmt_pct(float("nan"))
        assert "—" in result

    def test_two_decimal_places(self):
        assert "1.23%" in fmt_pct(1.23456)

    def test_wraps_in_span_tag(self):
        result = fmt_pct(1.0)
        assert result.startswith("<span")
        assert result.endswith("</span>")

    def test_non_numeric_returns_dash(self):
        result = fmt_pct("not_a_number")
        assert "—" in result


# ── TestBuildMomentumHtml ─────────────────────────────────────────────────────

class TestBuildMomentumHtml:
    def test_none_response_returns_no_data(self):
        assert "No data" in build_momentum_html(None)

    def test_missing_symbols_key_returns_no_data(self):
        assert "No data" in build_momentum_html({"status": "ok"})

    def test_empty_symbols_returns_no_data(self):
        assert "No data" in build_momentum_html(EMPTY_SYMBOLS)

    def test_html_contains_table_tags(self):
        result = build_momentum_html(SAMPLE_RESPONSE)
        assert "<table>" in result
        assert "</table>" in result

    def test_html_has_thead(self):
        assert "<thead>" in build_momentum_html(SAMPLE_RESPONSE)

    def test_html_has_tbody(self):
        assert "<tbody>" in build_momentum_html(SAMPLE_RESPONSE)

    def test_symbol_stripped_of_usdt(self):
        result = build_momentum_html(SAMPLE_RESPONSE)
        assert "BTC" in result
        assert "ETH" in result

    def test_positive_24h_colored_green(self):
        # BTC 24h=2.1 → green
        assert "var(--green)" in build_momentum_html(SAMPLE_RESPONSE)

    def test_negative_4h_colored_red(self):
        # BTC 4h=-0.5 → red
        assert "var(--red)" in build_momentum_html(SAMPLE_RESPONSE)

    def test_null_values_show_dash(self):
        result = build_momentum_html(NULL_VALUES)
        assert "—" in result

    def test_missing_timeframes_show_dash(self):
        result = build_momentum_html(MISSING_TIMEFRAMES)
        assert "—" in result

    def test_header_has_four_columns(self):
        result = build_momentum_html(SAMPLE_RESPONSE)
        assert "<th>Symbol</th>" in result
        assert "<th>1h</th>" in result
        assert "<th>4h</th>" in result
        assert "<th>24h</th>" in result

    def test_row_count_matches_symbols(self):
        result = build_momentum_html(SAMPLE_RESPONSE)
        assert result.count("<tr>") == 4  # 1 header + 3 data rows

    def test_negative_only_response(self):
        response = {
            "status": "ok",
            "symbols": {"COSUSDT": {"1h": -5.0, "4h": -10.0, "24h": -15.0}},
        }
        result = build_momentum_html(response)
        assert result.count("var(--red)") == 3

    def test_all_positive_response(self):
        response = {
            "status": "ok",
            "symbols": {"DEXEUSDT": {"1h": 1.0, "4h": 2.0, "24h": 3.0}},
        }
        result = build_momentum_html(response)
        assert result.count("var(--green)") == 3

    def test_mixed_signs_in_single_row(self):
        response = {
            "status": "ok",
            "symbols": {"LYNUSDT": {"1h": 1.0, "4h": -1.0, "24h": 0.0}},
        }
        result = build_momentum_html(response)
        assert "var(--green)" in result
        assert "var(--red)" in result
        assert "var(--muted)" in result

    def test_empty_string_status_ok(self):
        """status field doesn't affect rendering — symbols key is what matters."""
        response = {"symbols": {"BTCUSDT": {"1h": 1.0, "4h": 1.0, "24h": 1.0}}}
        assert "<table>" in build_momentum_html(response)

    def test_plus_sign_present_for_positive(self):
        response = {
            "status": "ok",
            "symbols": {"BTCUSDT": {"1h": 5.5, "4h": None, "24h": None}},
        }
        assert "+5.50%" in build_momentum_html(response)
