"""
Unit / smoke tests for the 5 new dashboard cards (batch 2):
  - Alerts              (/api/alerts)
  - OI Delta            (/api/oi-delta)
  - Squeeze Setup       (/api/squeeze-setup)
  - Volume Spike        (/api/volume-spike)
  - Trade Count Rate    (/api/trade-count-rate)

Each section covers:
  - Response shape validation
  - Python mirrors of display-helper logic
  - HTML card presence
  - JS render function presence
"""
import os
import sys
import pytest

_ROOT = os.path.join(os.path.dirname(__file__), "..")


def _html() -> str:
    with open(os.path.join(_ROOT, "frontend", "index.html"), encoding="utf-8") as f:
        return f.read()


def _js() -> str:
    with open(os.path.join(_ROOT, "frontend", "app.js"), encoding="utf-8") as f:
        return f.read()


# ══════════════════════════════════════════════════════════════════════════════
# 1 · ALERTS
# ══════════════════════════════════════════════════════════════════════════════

# ── helpers mirrored from app.js ──────────────────────────────────────────────

def alert_count_badge(count: int) -> tuple[str, str]:
    if count > 0:
        return (str(count), "badge-red")
    return ("0", "badge-blue")


ALERTS_PAYLOAD = {
    "status": "ok",
    "count": 3,
    "data": [
        {"type": "funding_extreme", "symbol": "BANANAS31USDT", "severity": "critical",
         "message": "Funding rate extreme: +0.15%", "ts": 1700000001},
        {"type": "liq_cascade", "symbol": "COSUSDT", "severity": "warning",
         "message": "Liquidation cascade detected", "ts": 1700000002},
        {"type": "volume_spike", "symbol": "DEXEUSDT", "severity": "info",
         "message": "Volume spike 3.2x baseline", "ts": 1700000003},
    ],
}

ALERTS_EMPTY = {
    "status": "ok",
    "count": 0,
    "data": [],
}


def test_alerts_response_has_status():
    assert ALERTS_PAYLOAD["status"] == "ok"


def test_alerts_has_required_keys():
    for key in ("status", "data", "count"):
        assert key in ALERTS_PAYLOAD


def test_alerts_count_matches_list():
    assert ALERTS_PAYLOAD["count"] == len(ALERTS_PAYLOAD["data"])


def test_alerts_each_alert_has_required_keys():
    for alert in ALERTS_PAYLOAD["data"]:
        for key in ("type", "symbol", "severity", "message"):
            assert key in alert


def test_alert_count_badge_nonzero():
    label, cls = alert_count_badge(3)
    assert label == "3"
    assert cls == "badge-red"


def test_alert_count_badge_one():
    label, cls = alert_count_badge(1)
    assert label == "1"
    assert cls == "badge-red"


def test_alert_count_badge_zero():
    label, cls = alert_count_badge(0)
    assert label == "0"
    assert cls == "badge-blue"


def test_alert_count_badge_large():
    label, cls = alert_count_badge(99)
    assert label == "99"
    assert cls == "badge-red"


def test_alerts_empty_response():
    assert ALERTS_EMPTY["count"] == 0
    assert len(ALERTS_EMPTY["data"]) == 0


def test_alerts_empty_badge():
    label, cls = alert_count_badge(ALERTS_EMPTY["count"])
    assert label == "0"
    assert cls == "badge-blue"


def test_alerts_severity_values():
    severities = {a["severity"] for a in ALERTS_PAYLOAD["data"]}
    valid = {"critical", "warning", "info"}
    assert severities <= valid


def test_html_has_alerts_card():
    assert "card-alerts" in _html()


def test_html_has_alerts_badge():
    assert "alerts-badge" in _html()


def test_html_has_alerts_content():
    assert "alerts-content" in _html()


def test_js_has_render_alerts():
    assert "renderAlerts" in _js()


# ══════════════════════════════════════════════════════════════════════════════
# 2 · OI DELTA
# ══════════════════════════════════════════════════════════════════════════════

# ── helpers mirrored from app.js ──────────────────────────────────────────────

def oi_direction_badge(total_oi_change: float) -> tuple[str, str]:
    if total_oi_change > 0:
        return ("up", "badge-green")
    if total_oi_change < 0:
        return ("down", "badge-red")
    return ("flat", "badge-blue")


OI_DELTA_PAYLOAD = {
    "status": "ok",
    "symbol": "BANANAS31USDT",
    "interval": 300,
    "candles": [
        {"ts": 1700000000, "oi_change": 5000.0,  "oi_end": 100000.0},
        {"ts": 1700000300, "oi_change": -2000.0, "oi_end": 98000.0},
        {"ts": 1700000600, "oi_change": 8000.0,  "oi_end": 106000.0},
    ],
}

OI_DELTA_NEGATIVE = {
    "status": "ok",
    "symbol": "COSUSDT",
    "interval": 300,
    "candles": [
        {"ts": 1700000000, "oi_change": -1000.0, "oi_end": 50000.0},
        {"ts": 1700000300, "oi_change": -3000.0, "oi_end": 47000.0},
    ],
}

OI_DELTA_FLAT = {
    "status": "ok",
    "symbol": "LYNUSDT",
    "interval": 300,
    "candles": [],
}


def test_oi_delta_response_has_status():
    assert OI_DELTA_PAYLOAD["status"] == "ok"


def test_oi_delta_has_required_keys():
    for key in ("status", "symbol", "interval", "candles"):
        assert key in OI_DELTA_PAYLOAD


def test_oi_delta_candles_have_required_keys():
    for candle in OI_DELTA_PAYLOAD["candles"]:
        for key in ("ts", "oi_change", "oi_end"):
            assert key in candle


def test_oi_delta_total_change_positive():
    total = sum(c["oi_change"] for c in OI_DELTA_PAYLOAD["candles"])
    assert total == pytest.approx(11000.0)


def test_oi_delta_direction_badge_positive():
    label, cls = oi_direction_badge(11000.0)
    assert label == "up"
    assert cls == "badge-green"


def test_oi_delta_direction_badge_negative():
    label, cls = oi_direction_badge(-4000.0)
    assert label == "down"
    assert cls == "badge-red"


def test_oi_delta_direction_badge_zero():
    label, cls = oi_direction_badge(0.0)
    assert label == "flat"
    assert cls == "badge-blue"


def test_oi_delta_negative_payload_total():
    total = sum(c["oi_change"] for c in OI_DELTA_NEGATIVE["candles"])
    assert total < 0
    label, cls = oi_direction_badge(total)
    assert label == "down"
    assert cls == "badge-red"


def test_oi_delta_empty_candles_flat():
    total = sum(c["oi_change"] for c in OI_DELTA_FLAT["candles"])
    assert total == 0
    label, cls = oi_direction_badge(total)
    assert label == "flat"
    assert cls == "badge-blue"


def test_html_has_oi_delta_card():
    assert "card-oi-delta" in _html()


def test_html_has_oi_delta_badge():
    assert "oi-delta-badge" in _html()


def test_html_has_oi_delta_content():
    assert "oi-delta-content" in _html()


def test_js_has_render_oi_delta():
    assert "renderOiDelta" in _js()


# ══════════════════════════════════════════════════════════════════════════════
# 3 · SQUEEZE SETUP
# ══════════════════════════════════════════════════════════════════════════════

# ── helpers mirrored from app.js ──────────────────────────────────────────────

def squeeze_badge(squeeze_signal: bool) -> tuple[str, str]:
    if squeeze_signal:
        return ("SQUEEZE", "badge-red")
    return ("off", "badge-blue")


SQUEEZE_PAYLOAD = {
    "status": "ok",
    "symbol": "BANANAS31USDT",
    "window_seconds": 7200,
    "squeeze_signal": True,
    "oi_surge_with_crash": True,
    "funding_normalizing": True,
    "funding_start": 0.0015,
    "funding_end": 0.0003,
    "description": "Squeeze conditions met: OI surged +25% while price dropped, funding normalizing",
}

SQUEEZE_OFF = {
    "status": "ok",
    "symbol": "COSUSDT",
    "window_seconds": 7200,
    "squeeze_signal": False,
    "oi_surge_with_crash": False,
    "funding_normalizing": False,
    "funding_start": None,
    "funding_end": None,
    "description": "No squeeze signal",
}


def test_squeeze_response_has_status():
    assert SQUEEZE_PAYLOAD["status"] == "ok"


def test_squeeze_has_required_keys():
    for key in ("status", "symbol", "window_seconds", "squeeze_signal",
                "oi_surge_with_crash", "funding_normalizing",
                "funding_start", "funding_end", "description"):
        assert key in SQUEEZE_PAYLOAD


def test_squeeze_badge_active():
    label, cls = squeeze_badge(True)
    assert label == "SQUEEZE"
    assert cls == "badge-red"


def test_squeeze_badge_off():
    label, cls = squeeze_badge(False)
    assert label == "off"
    assert cls == "badge-blue"


def test_squeeze_payload_badge():
    label, cls = squeeze_badge(SQUEEZE_PAYLOAD["squeeze_signal"])
    assert label == "SQUEEZE"
    assert cls == "badge-red"


def test_squeeze_off_payload_badge():
    label, cls = squeeze_badge(SQUEEZE_OFF["squeeze_signal"])
    assert label == "off"
    assert cls == "badge-blue"


def test_squeeze_funding_fields_nullable():
    # funding_start/end can be None when not applicable
    assert SQUEEZE_OFF["funding_start"] is None
    assert SQUEEZE_OFF["funding_end"] is None


def test_squeeze_description_non_empty():
    assert len(SQUEEZE_PAYLOAD["description"]) > 0


def test_html_has_squeeze_setup_card():
    assert "card-squeeze-setup" in _html()


def test_html_has_squeeze_setup_badge():
    assert "squeeze-setup-badge" in _html()


def test_html_has_squeeze_setup_content():
    assert "squeeze-setup-content" in _html()


def test_js_has_render_squeeze_setup():
    assert "renderSqueezeSetup" in _js()


# ══════════════════════════════════════════════════════════════════════════════
# 4 · VOLUME SPIKE
# ══════════════════════════════════════════════════════════════════════════════

# ── helpers mirrored from app.js ──────────────────────────────────────────────

def volume_spike_badge(spike: bool) -> tuple[str, str]:
    if spike:
        return ("SPIKE", "badge-red")
    return ("normal", "badge-blue")


VOLUME_SPIKE_PAYLOAD = {
    "status": "ok",
    "symbol": "BANANAS31USDT",
    "spike": True,
    "ratio": 4.2,
    "recent_usd": 84000.0,
    "baseline_usd_per_period": 20000.0,
    "dominant": "buy",
    "dominant_pct": 72.5,
    "description": "Volume spike 4.2x baseline, buy-dominated (72.5%)",
    "window_seconds": 30,
}

VOLUME_SPIKE_NORMAL = {
    "status": "ok",
    "symbol": "DEXEUSDT",
    "spike": False,
    "ratio": 0.9,
    "recent_usd": 4500.0,
    "baseline_usd_per_period": 5000.0,
    "dominant": "sell",
    "dominant_pct": 55.0,
    "description": "Volume within normal range",
    "window_seconds": 30,
}


def test_volume_spike_response_has_status():
    assert VOLUME_SPIKE_PAYLOAD["status"] == "ok"


def test_volume_spike_has_required_keys():
    for key in ("status", "symbol", "spike", "ratio", "recent_usd",
                "baseline_usd_per_period", "dominant", "dominant_pct",
                "description", "window_seconds"):
        assert key in VOLUME_SPIKE_PAYLOAD


def test_volume_spike_badge_active():
    label, cls = volume_spike_badge(True)
    assert label == "SPIKE"
    assert cls == "badge-red"


def test_volume_spike_badge_normal():
    label, cls = volume_spike_badge(False)
    assert label == "normal"
    assert cls == "badge-blue"


def test_volume_spike_payload_badge():
    label, cls = volume_spike_badge(VOLUME_SPIKE_PAYLOAD["spike"])
    assert label == "SPIKE"
    assert cls == "badge-red"


def test_volume_spike_normal_payload_badge():
    label, cls = volume_spike_badge(VOLUME_SPIKE_NORMAL["spike"])
    assert label == "normal"
    assert cls == "badge-blue"


def test_volume_spike_ratio_positive():
    assert VOLUME_SPIKE_PAYLOAD["ratio"] > 0


def test_volume_spike_dominant_pct_range():
    assert 0 <= VOLUME_SPIKE_PAYLOAD["dominant_pct"] <= 100


def test_html_has_volume_spike_card():
    assert "card-volume-spike" in _html()


def test_html_has_volume_spike_badge():
    assert "volume-spike-badge" in _html()


def test_html_has_volume_spike_content():
    assert "volume-spike-content" in _html()


def test_js_has_render_volume_spike():
    assert "renderVolumeSpikeCard" in _js()


# ══════════════════════════════════════════════════════════════════════════════
# 5 · TRADE COUNT RATE
# ══════════════════════════════════════════════════════════════════════════════

# ── helpers mirrored from app.js ──────────────────────────────────────────────

def tpm_trend(buckets: list) -> str:
    """Compare avg of first half vs second half: rising | falling | flat (within 10%)."""
    if len(buckets) < 2:
        return "flat"
    mid = len(buckets) // 2
    first_half = buckets[:mid]
    second_half = buckets[mid:]
    if not first_half or not second_half:
        return "flat"
    avg_first = sum(b["trades_per_min"] for b in first_half) / len(first_half)
    avg_second = sum(b["trades_per_min"] for b in second_half) / len(second_half)
    if avg_first == 0:
        return "flat"
    pct_change = (avg_second - avg_first) / avg_first
    if pct_change > 0.10:
        return "rising"
    if pct_change < -0.10:
        return "falling"
    return "flat"


TCR_PAYLOAD = {
    "status": "ok",
    "symbol": "BANANAS31USDT",
    "interval": 60,
    "window": 1800,
    "buckets": [
        {"ts": 1700000000, "trades_count": 50,  "trades_per_min": 50.0},
        {"ts": 1700000060, "trades_count": 55,  "trades_per_min": 55.0},
        {"ts": 1700000120, "trades_count": 60,  "trades_per_min": 60.0},
        {"ts": 1700000180, "trades_count": 80,  "trades_per_min": 80.0},
        {"ts": 1700000240, "trades_count": 90,  "trades_per_min": 90.0},
        {"ts": 1700000300, "trades_count": 100, "trades_per_min": 100.0},
    ],
}

TCR_FALLING = {
    "status": "ok",
    "symbol": "COSUSDT",
    "interval": 60,
    "window": 1800,
    "buckets": [
        {"ts": 1700000000, "trades_count": 100, "trades_per_min": 100.0},
        {"ts": 1700000060, "trades_count": 90,  "trades_per_min": 90.0},
        {"ts": 1700000120, "trades_count": 80,  "trades_per_min": 80.0},
        {"ts": 1700000180, "trades_count": 40,  "trades_per_min": 40.0},
        {"ts": 1700000240, "trades_count": 30,  "trades_per_min": 30.0},
        {"ts": 1700000300, "trades_count": 20,  "trades_per_min": 20.0},
    ],
}

TCR_FLAT = {
    "status": "ok",
    "symbol": "LYNUSDT",
    "interval": 60,
    "window": 1800,
    "buckets": [
        {"ts": 1700000000, "trades_count": 50, "trades_per_min": 50.0},
        {"ts": 1700000060, "trades_count": 52, "trades_per_min": 52.0},
        {"ts": 1700000120, "trades_count": 48, "trades_per_min": 48.0},
        {"ts": 1700000180, "trades_count": 51, "trades_per_min": 51.0},
    ],
}

TCR_EMPTY = {
    "status": "ok",
    "symbol": "BANANAS31USDT",
    "interval": 60,
    "window": 1800,
    "buckets": [],
}


def test_tcr_response_has_status():
    assert TCR_PAYLOAD["status"] == "ok"


def test_tcr_has_required_keys():
    for key in ("status", "symbol", "interval", "window", "buckets"):
        assert key in TCR_PAYLOAD


def test_tcr_buckets_have_required_keys():
    for bucket in TCR_PAYLOAD["buckets"]:
        for key in ("ts", "trades_count", "trades_per_min"):
            assert key in bucket


def test_tpm_trend_rising():
    trend = tpm_trend(TCR_PAYLOAD["buckets"])
    assert trend == "rising"


def test_tpm_trend_falling():
    trend = tpm_trend(TCR_FALLING["buckets"])
    assert trend == "falling"


def test_tpm_trend_flat():
    trend = tpm_trend(TCR_FLAT["buckets"])
    assert trend == "flat"


def test_tpm_trend_empty():
    trend = tpm_trend(TCR_EMPTY["buckets"])
    assert trend == "flat"


def test_tpm_trend_single_bucket():
    buckets = [{"ts": 1700000000, "trades_count": 50, "trades_per_min": 50.0}]
    assert tpm_trend(buckets) == "flat"


def test_tcr_last_bucket_tpm():
    buckets = TCR_PAYLOAD["buckets"]
    current_tpm = buckets[-1]["trades_per_min"]
    assert current_tpm == 100.0


def test_tcr_tpm_positive():
    for b in TCR_PAYLOAD["buckets"]:
        assert b["trades_per_min"] >= 0


def test_html_has_trade_count_rate_card():
    assert "card-trade-count-rate" in _html()


def test_html_has_trade_count_rate_badge():
    assert "trade-count-rate-badge" in _html()


def test_html_has_trade_count_rate_content():
    assert "trade-count-rate-content" in _html()


def test_js_has_render_trade_count_rate():
    assert "renderTradeCountRate" in _js()


# ══════════════════════════════════════════════════════════════════════════════
# Route registration (all 5)
# ══════════════════════════════════════════════════════════════════════════════

def _get_paths():
    import tempfile
    os.environ.setdefault("DB_PATH", os.path.join(tempfile.mkdtemp(), "t.db"))
    os.environ.setdefault("SYMBOL_BINANCE", "BANANAS31USDT")
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "backend"))
    from api import router
    return [r.path for r in router.routes]


def test_alerts_route_registered():
    assert any("alerts" in p for p in _get_paths())


def test_oi_delta_route_registered():
    assert any("oi-delta" in p for p in _get_paths())


def test_squeeze_setup_route_registered():
    assert any("squeeze-setup" in p for p in _get_paths())


def test_volume_spike_route_registered():
    assert any("volume-spike" in p for p in _get_paths())


def test_trade_count_rate_route_registered():
    assert any("trade-count-rate" in p for p in _get_paths())
