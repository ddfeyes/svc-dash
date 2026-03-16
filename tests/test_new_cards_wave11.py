"""
Unit / smoke tests for the 5 new dashboard cards (wave 11):
  - CVD Divergence        (/api/cvd-divergence)
  - Squeeze Setup W11     (/api/squeeze-setup)
  - Flow Imbalance        (/api/flow-imbalance)
  - Volatility Regime     (/api/volatility-regime)
  - Price Velocity        (/api/price-velocity)

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
# 1 · CVD DIVERGENCE
# ══════════════════════════════════════════════════════════════════════════════

# ── helpers mirrored from app.js ──────────────────────────────────────────────

def cvd_signal_badge(signal: str) -> tuple[str, str]:
    if signal == "bearish_divergence":
        return ("bearish", "badge-red")
    if signal == "bullish_divergence":
        return ("bullish", "badge-green")
    return ("none", "badge-blue")


CVD_DIV_PAYLOAD = {
    "status": "ok",
    "BANANAS31USDT": {
        "signal": "bearish_divergence",
        "description": "Price rising but CVD falling — bearish divergence",
        "severity": 2,
        "price_pct": 1.5,
        "cvd_pct": -0.8,
    },
}

CVD_DIV_BULLISH = {
    "status": "ok",
    "COSUSDT": {
        "signal": "bullish_divergence",
        "description": "Price falling but CVD rising — bullish divergence",
        "severity": 1,
        "price_pct": -0.5,
        "cvd_pct": 0.3,
    },
}

CVD_DIV_NONE = {
    "status": "ok",
    "DEXEUSDT": {
        "signal": "none",
        "description": "No divergence detected",
        "severity": 0,
        "price_pct": 0.1,
        "cvd_pct": 0.1,
    },
}


def test_cvd_div_response_has_status():
    assert CVD_DIV_PAYLOAD["status"] == "ok"


def test_cvd_div_symbol_key_present():
    assert "BANANAS31USDT" in CVD_DIV_PAYLOAD


def test_cvd_div_has_required_keys():
    sym_data = CVD_DIV_PAYLOAD["BANANAS31USDT"]
    for key in ("signal", "description", "severity", "price_pct", "cvd_pct"):
        assert key in sym_data


def test_cvd_div_badge_bearish():
    label, cls = cvd_signal_badge("bearish_divergence")
    assert label == "bearish"
    assert cls == "badge-red"


def test_cvd_div_badge_bullish():
    label, cls = cvd_signal_badge("bullish_divergence")
    assert label == "bullish"
    assert cls == "badge-green"


def test_cvd_div_badge_none():
    label, cls = cvd_signal_badge("none")
    assert label == "none"
    assert cls == "badge-blue"


def test_cvd_div_badge_unknown_defaults_blue():
    label, cls = cvd_signal_badge("unknown_signal")
    assert cls == "badge-blue"


def test_cvd_div_bearish_payload():
    sym_data = CVD_DIV_PAYLOAD["BANANAS31USDT"]
    label, cls = cvd_signal_badge(sym_data["signal"])
    assert cls == "badge-red"
    assert sym_data["severity"] == 2


def test_cvd_div_bullish_payload():
    sym_data = CVD_DIV_BULLISH["COSUSDT"]
    label, cls = cvd_signal_badge(sym_data["signal"])
    assert cls == "badge-green"
    assert sym_data["severity"] == 1


def test_cvd_div_none_payload():
    sym_data = CVD_DIV_NONE["DEXEUSDT"]
    label, cls = cvd_signal_badge(sym_data["signal"])
    assert label == "none"
    assert sym_data["severity"] == 0


def test_html_has_cvd_divergence_card():
    assert "card-cvd-divergence" in _html()


def test_html_has_cvd_divergence_badge():
    assert "cvd-divergence-badge" in _html()


def test_html_has_cvd_divergence_content():
    assert "cvd-divergence-content" in _html()


def test_js_has_render_cvd_divergence():
    assert "renderCvdDivergence" in _js()


# ══════════════════════════════════════════════════════════════════════════════
# 2 · SQUEEZE SETUP W11
# ══════════════════════════════════════════════════════════════════════════════

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
    "funding_start": -0.012,
    "funding_end": -0.003,
    "description": "Squeeze setup detected: OI surged with price crash and funding is normalizing",
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
    "description": "No squeeze setup detected",
}


def test_squeeze_response_has_status():
    assert SQUEEZE_PAYLOAD["status"] == "ok"


def test_squeeze_has_required_keys():
    for key in ("symbol", "window_seconds", "squeeze_signal", "oi_surge_with_crash",
                "funding_normalizing", "funding_start", "funding_end", "description"):
        assert key in SQUEEZE_PAYLOAD


def test_squeeze_badge_active():
    label, cls = squeeze_badge(True)
    assert label == "SQUEEZE"
    assert cls == "badge-red"


def test_squeeze_badge_off():
    label, cls = squeeze_badge(False)
    assert label == "off"
    assert cls == "badge-blue"


def test_squeeze_active_payload():
    label, cls = squeeze_badge(SQUEEZE_PAYLOAD["squeeze_signal"])
    assert label == "SQUEEZE"
    assert cls == "badge-red"


def test_squeeze_off_payload():
    label, cls = squeeze_badge(SQUEEZE_OFF["squeeze_signal"])
    assert label == "off"
    assert cls == "badge-blue"


def test_squeeze_funding_start_none_allowed():
    assert SQUEEZE_OFF["funding_start"] is None


def test_squeeze_funding_end_none_allowed():
    assert SQUEEZE_OFF["funding_end"] is None


def test_squeeze_oi_surge_bool():
    assert isinstance(SQUEEZE_PAYLOAD["oi_surge_with_crash"], bool)


def test_html_has_squeeze_setup_w11_card():
    assert "card-squeeze-setup-w11" in _html()


def test_html_has_squeeze_setup_w11_badge():
    assert "squeeze-setup-w11-badge" in _html()


def test_html_has_squeeze_setup_w11_content():
    assert "squeeze-setup-w11-content" in _html()


def test_js_has_render_squeeze_setup_w11():
    assert "renderSqueezeSetupW11" in _js()


# ══════════════════════════════════════════════════════════════════════════════
# 3 · FLOW IMBALANCE
# ══════════════════════════════════════════════════════════════════════════════

def flow_bias_badge(bias: str) -> tuple[str, str]:
    if bias == "buy":
        return ("buy heavy", "badge-green")
    if bias == "sell":
        return ("sell heavy", "badge-red")
    return ("neutral", "badge-blue")


FLOW_IMBALANCE_PAYLOAD = {
    "status": "ok",
    "symbol": "BANANAS31USDT",
    "window_s": 3600,
    "bucket_size_s": 60,
    "ts": 1700000000.0,
    "summary": {
        "avg_ratio": 0.62,
        "total_buy_vol": 125000.0,
        "total_sell_vol": 76500.0,
        "bias": "buy",
        "bias_strength": 0.38,
        "buckets": 60,
    },
    "series": [
        {"ts": 1700000060.0, "ratio": 0.65, "buy_vol": 2100.0, "sell_vol": 1130.0},
        {"ts": 1700000120.0, "ratio": 0.58, "buy_vol": 1800.0, "sell_vol": 1300.0},
    ],
}

FLOW_IMBALANCE_NEUTRAL = {
    "status": "ok",
    "symbol": "DEXEUSDT",
    "window_s": 3600,
    "bucket_size_s": 60,
    "ts": 1700000000.0,
    "summary": {
        "avg_ratio": 0.50,
        "total_buy_vol": 5000.0,
        "total_sell_vol": 5000.0,
        "bias": "neutral",
        "bias_strength": 0.0,
        "buckets": 60,
    },
    "series": [],
}

FLOW_IMBALANCE_SELL = {
    "status": "ok",
    "symbol": "LYNUSDT",
    "window_s": 3600,
    "bucket_size_s": 60,
    "ts": 1700000000.0,
    "summary": {
        "avg_ratio": 0.32,
        "total_buy_vol": 3000.0,
        "total_sell_vol": 6375.0,
        "bias": "sell",
        "bias_strength": 0.68,
        "buckets": 60,
    },
    "series": [],
}


def test_flow_imbalance_response_has_status():
    assert FLOW_IMBALANCE_PAYLOAD["status"] == "ok"


def test_flow_imbalance_has_required_keys():
    for key in ("symbol", "window_s", "bucket_size_s", "ts", "summary", "series"):
        assert key in FLOW_IMBALANCE_PAYLOAD


def test_flow_imbalance_summary_has_required_keys():
    summary = FLOW_IMBALANCE_PAYLOAD["summary"]
    for key in ("avg_ratio", "total_buy_vol", "total_sell_vol", "bias", "bias_strength", "buckets"):
        assert key in summary


def test_flow_bias_badge_buy():
    label, cls = flow_bias_badge("buy")
    assert label == "buy heavy"
    assert cls == "badge-green"


def test_flow_bias_badge_sell():
    label, cls = flow_bias_badge("sell")
    assert label == "sell heavy"
    assert cls == "badge-red"


def test_flow_bias_badge_neutral():
    label, cls = flow_bias_badge("neutral")
    assert label == "neutral"
    assert cls == "badge-blue"


def test_flow_bias_badge_unknown_defaults_blue():
    label, cls = flow_bias_badge("unknown")
    assert cls == "badge-blue"


def test_flow_imbalance_buy_payload():
    summary = FLOW_IMBALANCE_PAYLOAD["summary"]
    label, cls = flow_bias_badge(summary["bias"])
    assert label == "buy heavy"
    assert cls == "badge-green"


def test_flow_imbalance_neutral_payload():
    summary = FLOW_IMBALANCE_NEUTRAL["summary"]
    label, cls = flow_bias_badge(summary["bias"])
    assert label == "neutral"
    assert cls == "badge-blue"


def test_flow_imbalance_sell_payload():
    summary = FLOW_IMBALANCE_SELL["summary"]
    label, cls = flow_bias_badge(summary["bias"])
    assert label == "sell heavy"
    assert cls == "badge-red"


def test_flow_avg_ratio_range():
    ratio = FLOW_IMBALANCE_PAYLOAD["summary"]["avg_ratio"]
    assert 0.0 <= ratio <= 1.0


def test_html_has_flow_imbalance_card():
    assert "card-flow-imbalance" in _html()


def test_html_has_flow_imbalance_badge():
    assert "flow-imbalance-badge" in _html()


def test_html_has_flow_imbalance_content():
    assert "flow-imbalance-content" in _html()


def test_js_has_render_flow_imbalance():
    assert "renderFlowImbalance" in _js()


# ══════════════════════════════════════════════════════════════════════════════
# 4 · VOLATILITY REGIME
# ══════════════════════════════════════════════════════════════════════════════

def vol_regime_badge(regime: str) -> tuple[str, str]:
    if regime == "high":
        return ("HIGH", "badge-red")
    if regime == "medium":
        return ("MEDIUM", "badge-yellow")
    if regime == "low":
        return ("low", "badge-green")
    return ("?", "badge-blue")


VOL_REGIME_HIGH = {
    "status": "ok",
    "symbol": "BANANAS31USDT",
    "regime": "high",
    "regime_label": "High Volatility",
    "percentile": 85.5,
    "current_atr_pct": 2.35,
    "atr_history": [1.8, 2.0, 2.1, 2.35],
}

VOL_REGIME_MEDIUM = {
    "status": "ok",
    "symbol": "COSUSDT",
    "regime": "medium",
    "regime_label": "Medium Volatility",
    "percentile": 52.0,
    "current_atr_pct": 1.12,
    "atr_history": [0.9, 1.0, 1.1, 1.12],
}

VOL_REGIME_LOW = {
    "status": "ok",
    "symbol": "DEXEUSDT",
    "regime": "low",
    "regime_label": "Low Volatility",
    "percentile": 15.0,
    "current_atr_pct": 0.32,
    "atr_history": [0.28, 0.30, 0.31, 0.32],
}

VOL_REGIME_UNKNOWN = {
    "status": "ok",
    "symbol": "LYNUSDT",
    "regime": "unknown",
    "regime_label": "Unknown",
    "percentile": None,
    "current_atr_pct": None,
    "atr_history": [],
    "note": "Insufficient data",
}


def test_vol_regime_response_has_status():
    assert VOL_REGIME_HIGH["status"] == "ok"


def test_vol_regime_has_required_keys():
    for key in ("symbol", "regime", "regime_label", "percentile", "current_atr_pct", "atr_history"):
        assert key in VOL_REGIME_HIGH


def test_vol_regime_badge_high():
    label, cls = vol_regime_badge("high")
    assert label == "HIGH"
    assert cls == "badge-red"


def test_vol_regime_badge_medium():
    label, cls = vol_regime_badge("medium")
    assert label == "MEDIUM"
    assert cls == "badge-yellow"


def test_vol_regime_badge_low():
    label, cls = vol_regime_badge("low")
    assert label == "low"
    assert cls == "badge-green"


def test_vol_regime_badge_unknown():
    label, cls = vol_regime_badge("unknown")
    assert label == "?"
    assert cls == "badge-blue"


def test_vol_regime_badge_unrecognized_defaults_blue():
    label, cls = vol_regime_badge("extreme")
    assert cls == "badge-blue"


def test_vol_regime_high_payload():
    label, cls = vol_regime_badge(VOL_REGIME_HIGH["regime"])
    assert cls == "badge-red"


def test_vol_regime_medium_payload():
    label, cls = vol_regime_badge(VOL_REGIME_MEDIUM["regime"])
    assert cls == "badge-yellow"


def test_vol_regime_low_payload():
    label, cls = vol_regime_badge(VOL_REGIME_LOW["regime"])
    assert cls == "badge-green"


def test_vol_regime_unknown_none_fields():
    assert VOL_REGIME_UNKNOWN["percentile"] is None
    assert VOL_REGIME_UNKNOWN["current_atr_pct"] is None


def test_vol_regime_percentile_range():
    assert 0.0 <= VOL_REGIME_HIGH["percentile"] <= 100.0


def test_html_has_volatility_regime_card():
    assert "card-volatility-regime" in _html()


def test_html_has_volatility_regime_badge():
    assert "volatility-regime-badge" in _html()


def test_html_has_volatility_regime_content():
    assert "volatility-regime-content" in _html()


def test_js_has_render_volatility_regime():
    assert "renderVolatilityRegime" in _js()


# ══════════════════════════════════════════════════════════════════════════════
# 5 · PRICE VELOCITY
# ══════════════════════════════════════════════════════════════════════════════

def velocity_badge(direction: str) -> tuple[str, str]:
    if direction == "up":
        return ("up", "badge-green")
    if direction == "down":
        return ("down", "badge-red")
    return ("flat", "badge-blue")


PRICE_VELOCITY_PAYLOAD = {
    "status": "ok",
    "BANANAS31USDT": {
        "instant_velocity": 0.000012,
        "trend_velocity": 0.000008,
        "instant_pct_per_sec": 0.0005,
        "trend_pct_per_sec": 0.00033,
        "score": 72,
        "direction": "up",
        "price_now": 0.002345,
    },
    "COSUSDT": {
        "instant_velocity": -0.000005,
        "trend_velocity": -0.000002,
        "instant_pct_per_sec": -0.0002,
        "trend_pct_per_sec": -0.00008,
        "score": -35,
        "direction": "down",
        "price_now": 0.01234,
    },
}

PRICE_VELOCITY_FLAT = {
    "status": "ok",
    "DEXEUSDT": {
        "instant_velocity": 0.0,
        "trend_velocity": 0.0,
        "instant_pct_per_sec": 0.0,
        "trend_pct_per_sec": 0.0,
        "score": 0,
        "direction": "flat",
        "price_now": None,
    },
}


def test_price_velocity_response_has_status():
    assert PRICE_VELOCITY_PAYLOAD["status"] == "ok"


def test_price_velocity_symbol_key_present():
    assert "BANANAS31USDT" in PRICE_VELOCITY_PAYLOAD


def test_price_velocity_has_required_keys():
    sym_data = PRICE_VELOCITY_PAYLOAD["BANANAS31USDT"]
    for key in ("instant_velocity", "trend_velocity", "instant_pct_per_sec",
                "trend_pct_per_sec", "score", "direction", "price_now"):
        assert key in sym_data


def test_velocity_badge_up():
    label, cls = velocity_badge("up")
    assert label == "up"
    assert cls == "badge-green"


def test_velocity_badge_down():
    label, cls = velocity_badge("down")
    assert label == "down"
    assert cls == "badge-red"


def test_velocity_badge_flat():
    label, cls = velocity_badge("flat")
    assert label == "flat"
    assert cls == "badge-blue"


def test_velocity_badge_unknown_defaults_blue():
    label, cls = velocity_badge("sideways")
    assert cls == "badge-blue"


def test_velocity_up_payload():
    sym_data = PRICE_VELOCITY_PAYLOAD["BANANAS31USDT"]
    label, cls = velocity_badge(sym_data["direction"])
    assert label == "up"
    assert cls == "badge-green"


def test_velocity_down_payload():
    sym_data = PRICE_VELOCITY_PAYLOAD["COSUSDT"]
    label, cls = velocity_badge(sym_data["direction"])
    assert label == "down"
    assert cls == "badge-red"


def test_velocity_flat_payload():
    sym_data = PRICE_VELOCITY_FLAT["DEXEUSDT"]
    label, cls = velocity_badge(sym_data["direction"])
    assert label == "flat"
    assert cls == "badge-blue"


def test_velocity_score_range_up():
    score = PRICE_VELOCITY_PAYLOAD["BANANAS31USDT"]["score"]
    assert -100 <= score <= 100


def test_velocity_score_range_down():
    score = PRICE_VELOCITY_PAYLOAD["COSUSDT"]["score"]
    assert -100 <= score <= 100


def test_velocity_flat_score_zero():
    assert PRICE_VELOCITY_FLAT["DEXEUSDT"]["score"] == 0


def test_velocity_price_now_none_allowed():
    assert PRICE_VELOCITY_FLAT["DEXEUSDT"]["price_now"] is None


def test_html_has_price_velocity_card():
    assert "card-price-velocity" in _html()


def test_html_has_price_velocity_badge():
    assert "price-velocity-badge" in _html()


def test_html_has_price_velocity_content():
    assert "price-velocity-content" in _html()


def test_js_has_render_price_velocity():
    assert "renderPriceVelocity" in _js()


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


def test_cvd_divergence_route_registered():
    assert any("cvd-divergence" in p for p in _get_paths())


def test_squeeze_setup_route_registered():
    assert any("squeeze-setup" in p for p in _get_paths())


def test_flow_imbalance_route_registered():
    assert any("flow-imbalance" in p for p in _get_paths())


def test_volatility_regime_route_registered():
    assert any("volatility-regime" in p for p in _get_paths())


def test_price_velocity_route_registered():
    assert any("price-velocity" in p for p in _get_paths())
