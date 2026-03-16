"""50+ tests for the token unlock calendar feature.

Tests cover:
  - compute_token_unlock_calendar() return structure and semantics
  - _compute_unlock_risk_score() helper logic
  - risk label mapping
  - summary field correctness
  - API endpoint registration
  - HTML card and JS render function presence
"""
import asyncio
import os
import sys
import tempfile

import pytest

# ------------------------------------------------------------------
# Environment setup – must happen before any local imports
# ------------------------------------------------------------------
os.environ.setdefault("DB_PATH", os.path.join(tempfile.mkdtemp(), "test_unlock.db"))
os.environ.setdefault("SYMBOL_BINANCE", "BANANAS31USDT")
os.environ.setdefault("SYMBOL_BYBIT", "BANANAS31USDT")

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

# ------------------------------------------------------------------
# Helpers that mirror production logic (for unit testing in isolation)
# ------------------------------------------------------------------

VALID_RISK_LABELS = {"low", "medium", "high", "critical"}
VALID_CATEGORIES = {
    "team_investors", "early_backers", "ecosystem", "community", "foundation",
}

REQUIRED_EVENT_KEYS = {
    "token",
    "symbol",
    "unlock_date",
    "days_until",
    "unlock_usd",
    "unlock_usd_formatted",
    "pct_circulating_supply",
    "historical_price_impact_pct",
    "historical_unlock_count",
    "risk_score",
    "risk_label",
    "recipient_category",
}

REQUIRED_SUMMARY_KEYS = {
    "total_unlock_usd",
    "events_count",
    "window_days",
    "avg_risk_score",
    "highest_risk_token",
    "highest_risk_score",
    "critical_count",
    "high_risk_count",
}


def _risk_label_from_score(score: float) -> str:
    """Mirror of production risk-label logic."""
    if score >= 75:
        return "critical"
    if score >= 50:
        return "high"
    if score >= 25:
        return "medium"
    return "low"


def _compute_risk_score(pct_supply: float, hist_impact: float, days_until: int) -> float:
    """Mirror of production _compute_unlock_risk_score."""
    supply_score = min(50.0, pct_supply * 5.0)
    impact_score = min(30.0, abs(hist_impact) * 3.0) if hist_impact < 0 else 0.0
    urgency_score = max(0.0, 20.0 * (1.0 - days_until / 90.0))
    return round(min(100.0, max(0.0, supply_score + impact_score + urgency_score)), 1)


# ------------------------------------------------------------------
# Fixtures
# ------------------------------------------------------------------

@pytest.fixture(scope="module")
def event_loop_module():
    loop = asyncio.new_event_loop()
    yield loop
    loop.close()


@pytest.fixture(scope="module")
def unlock_data(event_loop_module):
    """Run compute_token_unlock_calendar once for all tests."""
    from metrics import compute_token_unlock_calendar
    return event_loop_module.run_until_complete(compute_token_unlock_calendar())


@pytest.fixture(scope="module")
def events(unlock_data):
    return unlock_data["events"]


@pytest.fixture(scope="module")
def summary(unlock_data):
    return unlock_data["summary"]


# ==================================================================
# 1. Top-level return structure
# ==================================================================

class TestReturnStructure:
    def test_returns_dict(self, unlock_data):
        assert isinstance(unlock_data, dict)

    def test_has_events_key(self, unlock_data):
        assert "events" in unlock_data

    def test_has_summary_key(self, unlock_data):
        assert "summary" in unlock_data

    def test_has_description_key(self, unlock_data):
        assert "description" in unlock_data

    def test_description_is_string(self, unlock_data):
        assert isinstance(unlock_data["description"], str)

    def test_description_not_empty(self, unlock_data):
        assert len(unlock_data["description"]) > 0

    def test_events_is_list(self, events):
        assert isinstance(events, list)

    def test_events_has_20_entries(self, events):
        assert len(events) == 20

    def test_summary_is_dict(self, summary):
        assert isinstance(summary, dict)


# ==================================================================
# 2. Individual event field presence
# ==================================================================

class TestEventFieldPresence:
    def test_each_event_has_token(self, events):
        for e in events:
            assert "token" in e, f"Missing 'token' in {e}"

    def test_each_event_has_symbol(self, events):
        for e in events:
            assert "symbol" in e

    def test_each_event_has_unlock_date(self, events):
        for e in events:
            assert "unlock_date" in e

    def test_each_event_has_days_until(self, events):
        for e in events:
            assert "days_until" in e

    def test_each_event_has_unlock_usd(self, events):
        for e in events:
            assert "unlock_usd" in e

    def test_each_event_has_unlock_usd_formatted(self, events):
        for e in events:
            assert "unlock_usd_formatted" in e

    def test_each_event_has_pct_circulating_supply(self, events):
        for e in events:
            assert "pct_circulating_supply" in e

    def test_each_event_has_historical_price_impact(self, events):
        for e in events:
            assert "historical_price_impact_pct" in e

    def test_each_event_has_historical_unlock_count(self, events):
        for e in events:
            assert "historical_unlock_count" in e

    def test_each_event_has_risk_score(self, events):
        for e in events:
            assert "risk_score" in e

    def test_each_event_has_risk_label(self, events):
        for e in events:
            assert "risk_label" in e

    def test_each_event_has_recipient_category(self, events):
        for e in events:
            assert "recipient_category" in e

    def test_all_required_keys_present(self, events):
        for e in events:
            missing = REQUIRED_EVENT_KEYS - set(e.keys())
            assert not missing, f"Missing keys {missing} in event {e.get('symbol')}"


# ==================================================================
# 3. Event field value ranges
# ==================================================================

class TestEventFieldRanges:
    def test_days_until_in_1_to_90(self, events):
        for e in events:
            assert 1 <= e["days_until"] <= 90, (
                f"{e['symbol']} days_until={e['days_until']} out of range"
            )

    def test_risk_score_in_0_to_100(self, events):
        for e in events:
            assert 0.0 <= e["risk_score"] <= 100.0, (
                f"{e['symbol']} risk_score={e['risk_score']} out of range"
            )

    def test_pct_circulating_supply_positive(self, events):
        for e in events:
            assert e["pct_circulating_supply"] > 0

    def test_unlock_usd_positive(self, events):
        for e in events:
            assert e["unlock_usd"] > 0

    def test_historical_unlock_count_positive(self, events):
        for e in events:
            assert e["historical_unlock_count"] > 0

    def test_risk_labels_valid(self, events):
        for e in events:
            assert e["risk_label"] in VALID_RISK_LABELS, (
                f"{e['symbol']} invalid risk_label={e['risk_label']}"
            )

    def test_unlock_date_is_iso_format(self, events):
        import re
        pattern = re.compile(r"^\d{4}-\d{2}-\d{2}$")
        for e in events:
            assert pattern.match(e["unlock_date"]), (
                f"{e['symbol']} unlock_date={e['unlock_date']} is not YYYY-MM-DD"
            )

    def test_unlock_usd_formatted_starts_with_dollar(self, events):
        for e in events:
            assert e["unlock_usd_formatted"].startswith("$"), (
                f"{e['symbol']} formatted={e['unlock_usd_formatted']}"
            )

    def test_sorted_by_unlock_usd_descending(self, events):
        usds = [e["unlock_usd"] for e in events]
        assert usds == sorted(usds, reverse=True), "Events not sorted by unlock_usd desc"


# ==================================================================
# 4. Risk score helper unit tests (pure function, no async)
# ==================================================================

class TestRiskScoreHelper:
    def test_zero_inputs_gives_zero(self):
        score = _compute_risk_score(0.0, 0.0, 90)
        assert score == 0.0

    def test_ten_pct_supply_gives_50_supply_points(self):
        # 10% supply × 5 = 50 pts; no impact, no urgency at day 90
        score = _compute_risk_score(10.0, 0.0, 90)
        assert score == 50.0

    def test_supply_capped_at_50_points(self):
        # 20% supply would be 100 pts but capped at 50
        score = _compute_risk_score(20.0, 0.0, 90)
        assert score == 50.0

    def test_negative_impact_increases_score(self):
        base = _compute_risk_score(5.0, 0.0, 45)
        with_impact = _compute_risk_score(5.0, -10.0, 45)
        assert with_impact > base

    def test_positive_impact_does_not_increase_score(self):
        base = _compute_risk_score(5.0, 0.0, 45)
        positive_impact = _compute_risk_score(5.0, 10.0, 45)
        assert positive_impact == base

    def test_impact_capped_at_30_points(self):
        # -100% impact × 3 = 300 pts, capped at 30
        score_big = _compute_risk_score(0.0, -100.0, 90)
        score_max_impact = _compute_risk_score(0.0, -10.0, 90)
        assert score_big == score_max_impact == 30.0

    def test_urgency_is_max_at_day_0(self):
        score = _compute_risk_score(0.0, 0.0, 0)
        assert score == 20.0

    def test_urgency_is_zero_at_day_90(self):
        score = _compute_risk_score(0.0, 0.0, 90)
        assert score == 0.0

    def test_score_capped_at_100(self):
        score = _compute_risk_score(20.0, -100.0, 0)
        assert score == 100.0

    def test_score_never_below_zero(self):
        score = _compute_risk_score(0.0, 10.0, 90)
        assert score >= 0.0


# ==================================================================
# 5. Risk label mapping unit tests
# ==================================================================

class TestRiskLabelMapping:
    def test_score_75_is_critical(self):
        assert _risk_label_from_score(75.0) == "critical"

    def test_score_100_is_critical(self):
        assert _risk_label_from_score(100.0) == "critical"

    def test_score_74_9_is_high(self):
        assert _risk_label_from_score(74.9) == "high"

    def test_score_50_is_high(self):
        assert _risk_label_from_score(50.0) == "high"

    def test_score_49_9_is_medium(self):
        assert _risk_label_from_score(49.9) == "medium"

    def test_score_25_is_medium(self):
        assert _risk_label_from_score(25.0) == "medium"

    def test_score_24_9_is_low(self):
        assert _risk_label_from_score(24.9) == "low"

    def test_score_0_is_low(self):
        assert _risk_label_from_score(0.0) == "low"

    def test_risk_label_consistent_with_production(self, events):
        """Every event's risk_label must match what the helper predicts."""
        for e in events:
            expected = _risk_label_from_score(e["risk_score"])
            assert e["risk_label"] == expected, (
                f"{e['symbol']}: score={e['risk_score']}, "
                f"label={e['risk_label']}, expected={expected}"
            )


# ==================================================================
# 6. Summary field tests
# ==================================================================

class TestSummaryFields:
    def test_all_required_summary_keys_present(self, summary):
        missing = REQUIRED_SUMMARY_KEYS - set(summary.keys())
        assert not missing, f"Missing summary keys: {missing}"

    def test_events_count_is_20(self, summary):
        assert summary["events_count"] == 20

    def test_window_days_is_90(self, summary):
        assert summary["window_days"] == 90

    def test_total_unlock_usd_positive(self, summary):
        assert summary["total_unlock_usd"] > 0

    def test_avg_risk_score_in_0_100(self, summary):
        assert 0.0 <= summary["avg_risk_score"] <= 100.0

    def test_highest_risk_score_in_0_100(self, summary):
        assert 0.0 <= summary["highest_risk_score"] <= 100.0

    def test_highest_risk_token_is_string(self, summary):
        assert isinstance(summary["highest_risk_token"], str)

    def test_highest_risk_token_not_empty(self, summary):
        assert len(summary["highest_risk_token"]) > 0

    def test_critical_count_non_negative(self, summary):
        assert summary["critical_count"] >= 0

    def test_high_risk_count_non_negative(self, summary):
        assert summary["high_risk_count"] >= 0

    def test_critical_count_matches_events(self, summary, events):
        expected = sum(1 for e in events if e["risk_label"] == "critical")
        assert summary["critical_count"] == expected

    def test_high_risk_count_matches_events(self, summary, events):
        expected = sum(1 for e in events if e["risk_label"] == "high")
        assert summary["high_risk_count"] == expected

    def test_total_usd_matches_sum_of_events(self, summary, events):
        expected = sum(e["unlock_usd"] for e in events)
        assert summary["total_unlock_usd"] == expected

    def test_highest_risk_token_matches_max_score_event(self, summary, events):
        best = max(events, key=lambda e: e["risk_score"])
        assert summary["highest_risk_token"] == best["token"]

    def test_highest_risk_score_matches_max(self, summary, events):
        best_score = max(e["risk_score"] for e in events)
        assert summary["highest_risk_score"] == best_score

    def test_avg_risk_score_is_mean(self, summary, events):
        expected = round(sum(e["risk_score"] for e in events) / len(events), 1)
        assert summary["avg_risk_score"] == expected


# ==================================================================
# 7. API endpoint registration
# ==================================================================

class TestApiEndpoint:
    @pytest.mark.asyncio
    async def test_endpoint_is_registered(self):
        from storage import init_db
        await init_db()
        from api import router
        paths = [r.path for r in router.routes]
        assert any("token-unlock-calendar" in p for p in paths)

    @pytest.mark.asyncio
    async def test_endpoint_returns_data(self):
        from storage import init_db
        await init_db()
        from api import router
        # Find and call the endpoint function directly
        for route in router.routes:
            if route.path == "/token-unlock-calendar":
                result = await route.endpoint()
                import json
                body = json.loads(result.body)
                assert "events" in body
                break


# ==================================================================
# 8. HTML card and JS render function presence
# ==================================================================

class TestFrontendIntegration:
    HTML_PATH = os.path.join(
        os.path.dirname(__file__), "..", "..", "frontend", "index.html"
    )
    JS_PATH = os.path.join(
        os.path.dirname(__file__), "..", "..", "frontend", "app.js"
    )

    def _read_html(self):
        with open(self.HTML_PATH, "r") as f:
            return f.read()

    def _read_js(self):
        with open(self.JS_PATH, "r") as f:
            return f.read()

    def test_html_has_card_token_unlock(self):
        assert 'id="card-token-unlock"' in self._read_html()

    def test_html_card_has_content_div(self):
        assert 'id="token-unlock-content"' in self._read_html()

    def test_html_card_has_badge_span(self):
        assert 'id="token-unlock-badge"' in self._read_html()

    def test_html_card_has_card_class(self):
        html = self._read_html()
        assert 'class="card"' in html and 'id="card-token-unlock"' in html

    def test_js_has_render_token_unlock_calendar(self):
        assert "renderTokenUnlockCalendar" in self._read_js()

    def test_js_render_is_async_function(self):
        assert "async function renderTokenUnlockCalendar" in self._read_js()

    def test_js_fetches_token_unlock_calendar_endpoint(self):
        assert "/token-unlock-calendar" in self._read_js()

    def test_js_render_called_in_refresh(self):
        js = self._read_js()
        # Both the definition and the call must be present
        assert js.count("renderTokenUnlockCalendar") >= 2

    def test_js_render_uses_token_unlock_content_id(self):
        assert "token-unlock-content" in self._read_js()

    def test_js_render_uses_token_unlock_badge_id(self):
        assert "token-unlock-badge" in self._read_js()
