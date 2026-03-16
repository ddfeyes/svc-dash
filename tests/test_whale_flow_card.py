"""
Frontend structural tests for WhaleFlowCard (Wave 23, Issue #117).

Validates HTML card presence, JS render function, API call wiring,
badge structure, sparkline logic, and signal color mapping.
Following project pattern: pytest tests that inspect static frontend files.
"""
import os
import sys
import re
import pytest

FRONTEND_DIR = os.path.join(os.path.dirname(__file__), "..", "frontend")
BACKEND_DIR  = os.path.join(os.path.dirname(__file__), "..", "backend")

sys.path.insert(0, BACKEND_DIR)


# ── Fixtures ──────────────────────────────────────────────────────────────────

@pytest.fixture(scope="module")
def html_content():
    path = os.path.join(FRONTEND_DIR, "index.html")
    with open(path, "r") as f:
        return f.read()


@pytest.fixture(scope="module")
def js_content():
    path = os.path.join(FRONTEND_DIR, "app.js")
    with open(path, "r") as f:
        return f.read()


@pytest.fixture(scope="module")
def whale_flow_module():
    from whale_flow import compute_whale_flow
    return compute_whale_flow


# ── HTML Card Tests ───────────────────────────────────────────────────────────

def test_html_card_whale_flow_exists(html_content):
    assert "card-whale-flow" in html_content


def test_html_card_has_title(html_content):
    assert "Whale Flow" in html_content


def test_html_card_has_content_div(html_content):
    assert "whale-flow-content" in html_content


def test_html_card_has_badge_span(html_content):
    assert "whale-flow-badge" in html_content


def test_html_card_has_meta_description(html_content):
    # Should have some description of what the card shows
    assert "inflow" in html_content or "accumulation" in html_content


# ── JS Render Function Tests ──────────────────────────────────────────────────

def test_js_render_function_exists(js_content):
    assert "renderWhaleFlow" in js_content


def test_js_api_call_to_whale_flow_endpoint(js_content):
    assert "/api/whale-flow" in js_content or "whale-flow" in js_content


def test_js_badge_update_logic(js_content):
    # Badge should be updated in the render function
    assert "whale-flow-badge" in js_content


def test_js_accumulation_signal_colors(js_content):
    # Green for accumulating, red for distributing
    assert "accumulating" in js_content
    assert "distributing" in js_content


def test_js_render_in_refresh_loop(js_content):
    assert "renderWhaleFlow" in js_content
    # Should be called via safe() wrapper
    assert "safe(renderWhaleFlow)" in js_content


def test_js_inflow_outflow_labels(js_content):
    assert "Inflow" in js_content
    assert "Outflow" in js_content


def test_js_net_flow_bps_display(js_content):
    assert "net_flow_bps" in js_content or "Net Flow" in js_content


def test_js_accumulation_score_display(js_content):
    assert "accumulation_score" in js_content


def test_js_flow_signal_display(js_content):
    assert "flow_signal" in js_content


def test_js_sparkline_implementation(js_content):
    assert "daily_buckets" in js_content


# ── Backend Integration (WhaleFlow model) ────────────────────────────────────

def test_whale_flow_btc_returns_data(whale_flow_module):
    result = whale_flow_module("BTCUSDT")
    assert result["symbol"] == "BTCUSDT"


def test_whale_flow_has_all_required_fields(whale_flow_module):
    result = whale_flow_module("BTCUSDT")
    required = ["symbol", "whale_inflow_7d", "whale_outflow_7d", "net_flow_bps",
                "accumulation_score", "flow_signal", "trend_7d", "daily_buckets"]
    for field in required:
        assert field in result, f"Missing field: {field}"


def test_whale_flow_response_fast():
    import time
    from whale_flow import compute_whale_flow
    t0 = time.time()
    compute_whale_flow("BTCUSDT")
    elapsed_ms = (time.time() - t0) * 1000
    assert elapsed_ms < 200, f"Response took {elapsed_ms:.0f}ms, expected <200ms"


def test_whale_flow_score_in_valid_range(whale_flow_module):
    result = whale_flow_module("ETHUSDT")
    assert 0 <= result["accumulation_score"] <= 100


def test_whale_flow_signal_is_valid(whale_flow_module):
    result = whale_flow_module("SOLUSDT")
    assert result["flow_signal"] in ("accumulating", "neutral", "distributing")
