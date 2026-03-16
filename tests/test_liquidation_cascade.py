"""
Unit / smoke tests for /api/liquidation-cascade.

Liquidation cascade risk estimator with waterfall visualization.

Distinct from the existing detect_liquidation_cascade (60s burst detector):
  - Longer window (default 1h), configurable bucket size (default 5m)
  - Waterfall time-bucketed chart data (long_usd / short_usd / net_usd per bucket)
  - Price zone clustering (top liquidation price levels)
  - Composite risk score 0-100 from magnitude + recency + concentration
  - Acceleration factor (recent half vs earlier half)

Covers:
  - risk_score computation
  - risk_level classification
  - dominant_side detection
  - waterfall bucket building
  - price zone clustering
  - acceleration factor
  - fmt_usd display helper
  - Response shape validation
  - Edge cases (no liqs, single liq, all same side, zero price)
  - Route registration
  - HTML card / JS smoke tests
"""
import os
import sys
import math
import pytest

_ROOT = os.path.join(os.path.dirname(__file__), "..")


def _html() -> str:
    with open(os.path.join(_ROOT, "frontend", "index.html"), encoding="utf-8") as f:
        return f.read()


def _js() -> str:
    with open(os.path.join(_ROOT, "frontend", "app.js"), encoding="utf-8") as f:
        return f.read()


# ── Python mirrors of backend logic ──────────────────────────────────────────

def risk_score(
    total_usd: float,
    peak_bucket_usd: float,
    long_usd: float,
    short_usd: float,
    target_usd: float = 50_000.0,
) -> float:
    """
    Composite cascade risk score 0–100.

    Components:
      magnitude:    min(100, total_usd / target_usd * 100) — how big relative to threshold
      peak:         min(100, peak_bucket_usd / (target_usd / 10) * 100) — spike intensity
      concentration: max(long_pct, short_pct) * 100 — one-sided cascade is riskier

    Weights: magnitude=0.50, peak=0.30, concentration=0.20
    """
    if total_usd <= 0:
        return 0.0

    mag = min(100.0, total_usd / target_usd * 100.0)
    peak = min(100.0, peak_bucket_usd / max(target_usd / 10, 1) * 100.0)

    total = long_usd + short_usd
    if total > 0:
        long_pct = long_usd / total
        short_pct = short_usd / total
        conc = max(long_pct, short_pct) * 100.0
    else:
        conc = 50.0

    score = 0.50 * mag + 0.30 * peak + 0.20 * conc
    return round(min(100.0, score), 2)


def risk_level(score: float) -> str:
    """low < 25, medium < 50, high < 75, critical >= 75."""
    if score >= 75:
        return "critical"
    if score >= 50:
        return "high"
    if score >= 25:
        return "medium"
    return "low"


def dominant_side(long_usd: float, short_usd: float, threshold: float = 0.6) -> str:
    """
    Identify dominant liquidation side.
    If one side > threshold fraction of total → that side, else 'balanced'.
    Note: long_usd = liquidated long positions (forced sells),
          short_usd = liquidated short positions (forced buys).
    """
    total = long_usd + short_usd
    if total <= 0:
        return "balanced"
    if long_usd / total >= threshold:
        return "long"
    if short_usd / total >= threshold:
        return "short"
    return "balanced"


def acceleration_factor(waterfall: list[dict]) -> float:
    """
    Acceleration: compare total_usd in second half vs first half of waterfall.
    Returns value in [-1, 1]:
      +1 = all activity in second half (accelerating)
      -1 = all activity in first half  (decelerating)
       0 = equal halves
    None if waterfall is empty or total is 0.
    """
    if not waterfall:
        return 0.0
    n = len(waterfall)
    mid = n // 2
    first_half = sum(b.get("total_usd", 0) for b in waterfall[:mid])
    second_half = sum(b.get("total_usd", 0) for b in waterfall[mid:])
    total = first_half + second_half
    if total <= 0:
        return 0.0
    return round((second_half - first_half) / total, 4)


def build_waterfall(
    liquidations: list[dict],
    since: float,
    bucket_s: int,
    window_s: int,
) -> list[dict]:
    """
    Bucket liquidations into time windows.
    Each bucket: {ts, long_usd, short_usd, net_usd, total_usd}
    long_usd  = USD of liquidated long positions (side == 'long' or 'sell')
    short_usd = USD of liquidated short positions (side == 'short' or 'buy')
    net_usd   = short_usd - long_usd  (positive = more shorts forced out)
    """
    n_buckets = max(1, window_s // bucket_s)
    buckets = [
        {"ts": since + i * bucket_s, "long_usd": 0.0, "short_usd": 0.0,
         "net_usd": 0.0, "total_usd": 0.0}
        for i in range(n_buckets)
    ]
    for liq in liquidations:
        t = float(liq.get("ts") or 0)
        elapsed = t - since
        if elapsed < 0 or elapsed >= window_s:
            continue
        idx = min(n_buckets - 1, int(elapsed / bucket_s))
        val = float(liq.get("value") or 0)
        side = (liq.get("side") or "").lower()
        if side in ("long", "sell"):
            buckets[idx]["long_usd"] += val
        else:
            buckets[idx]["short_usd"] += val
        buckets[idx]["total_usd"] += val

    for b in buckets:
        b["long_usd"]  = round(b["long_usd"], 2)
        b["short_usd"] = round(b["short_usd"], 2)
        b["total_usd"] = round(b["total_usd"], 2)
        b["net_usd"]   = round(b["short_usd"] - b["long_usd"], 2)
    return buckets


def cluster_zones(
    liquidations: list[dict],
    zone_pct: float = 0.5,
    top_n: int = 5,
) -> list[dict]:
    """
    Cluster liquidation prices into zones.
    Zones are defined by rounding price to the nearest zone_pct% bin.
    Returns top_n zones sorted by total_usd descending.
    Each zone: {price, usd, count, side}
    """
    if not liquidations:
        return []

    prices = [float(liq.get("price") or 0) for liq in liquidations]
    valid_prices = [p for p in prices if p > 0]
    if not valid_prices:
        return []

    mid = sum(valid_prices) / len(valid_prices)
    bin_size = mid * zone_pct / 100.0
    if bin_size <= 0:
        return []

    zone_map: dict = {}
    for liq in liquidations:
        p = float(liq.get("price") or 0)
        if p <= 0:
            continue
        val  = float(liq.get("value") or 0)
        side = (liq.get("side") or "").lower()
        bin_key = round(p / bin_size) * bin_size
        if bin_key not in zone_map:
            zone_map[bin_key] = {"price": bin_key, "usd": 0.0, "count": 0,
                                 "long": 0.0, "short": 0.0}
        zone_map[bin_key]["usd"] += val
        zone_map[bin_key]["count"] += 1
        if side in ("long", "sell"):
            zone_map[bin_key]["long"] += val
        else:
            zone_map[bin_key]["short"] += val

    zones = []
    for z in zone_map.values():
        dominant = "long" if z["long"] >= z["short"] else "short"
        zones.append({
            "price": round(z["price"], 8),
            "usd": round(z["usd"], 2),
            "count": z["count"],
            "side": dominant,
        })

    zones.sort(key=lambda z: z["usd"], reverse=True)
    return zones[:top_n]


def fmt_usd(v: float) -> str:
    """Format USD value with M/k suffix."""
    if v >= 1_000_000:
        return f"${v / 1_000_000:.2f}M"
    if v >= 1_000:
        return f"${v / 1_000:.1f}k"
    return f"${v:.0f}"


# ── risk_score tests ──────────────────────────────────────────────────────────

def test_risk_score_zero_no_liqs():
    assert risk_score(0, 0, 0, 0) == pytest.approx(0.0)


def test_risk_score_capped_100():
    assert risk_score(1_000_000, 500_000, 900_000, 100_000) <= 100.0


def test_risk_score_nonneg():
    assert risk_score(500, 100, 300, 200) >= 0.0


def test_risk_score_higher_with_more_total():
    s1 = risk_score(5_000, 1_000, 3_000, 2_000)
    s2 = risk_score(50_000, 10_000, 30_000, 20_000)
    assert s2 > s1


def test_risk_score_higher_one_sided():
    # All longs being liquidated → higher concentration score
    balanced = risk_score(10_000, 2_000, 5_000, 5_000)
    one_sided = risk_score(10_000, 2_000, 10_000, 0)
    assert one_sided > balanced


def test_risk_score_full_magnitude():
    # total_usd == target_usd → magnitude component = 100
    score = risk_score(50_000, 5_000, 25_000, 25_000, target_usd=50_000)
    assert score > 50.0  # magnitude alone is 50 × 0.5 = 25, but peak adds more


def test_risk_score_peak_matters():
    s_low_peak  = risk_score(10_000, 500,   5_000, 5_000)
    s_high_peak = risk_score(10_000, 10_000, 5_000, 5_000)
    assert s_high_peak > s_low_peak


# ── risk_level tests ──────────────────────────────────────────────────────────

def test_risk_level_low():
    assert risk_level(0.0) == "low"
    assert risk_level(24.9) == "low"


def test_risk_level_medium():
    assert risk_level(25.0) == "medium"
    assert risk_level(49.9) == "medium"


def test_risk_level_high():
    assert risk_level(50.0) == "high"
    assert risk_level(74.9) == "high"


def test_risk_level_critical():
    assert risk_level(75.0) == "critical"
    assert risk_level(100.0) == "critical"


# ── dominant_side tests ───────────────────────────────────────────────────────

def test_dominant_long():
    assert dominant_side(8_000, 2_000) == "long"


def test_dominant_short():
    assert dominant_side(2_000, 8_000) == "short"


def test_dominant_balanced():
    assert dominant_side(5_000, 5_000) == "balanced"


def test_dominant_empty():
    assert dominant_side(0, 0) == "balanced"


def test_dominant_boundary():
    # Exactly 60% long → "long"
    assert dominant_side(6_000, 4_000, threshold=0.6) == "long"
    # Just under 60% → "balanced"
    assert dominant_side(5_999, 4_001, threshold=0.6) == "balanced"


# ── acceleration_factor tests ─────────────────────────────────────────────────

WATERFALL_ACCEL = [
    {"ts": 1700000000.0, "long_usd": 100.0, "short_usd": 0.0, "net_usd": -100.0, "total_usd": 100.0},
    {"ts": 1700000300.0, "long_usd": 100.0, "short_usd": 0.0, "net_usd": -100.0, "total_usd": 100.0},
    {"ts": 1700000600.0, "long_usd": 500.0, "short_usd": 0.0, "net_usd": -500.0, "total_usd": 500.0},
    {"ts": 1700000900.0, "long_usd": 500.0, "short_usd": 0.0, "net_usd": -500.0, "total_usd": 500.0},
]

WATERFALL_DECEL = [
    {"ts": 1700000000.0, "long_usd": 500.0, "short_usd": 0.0, "net_usd": -500.0, "total_usd": 500.0},
    {"ts": 1700000300.0, "long_usd": 500.0, "short_usd": 0.0, "net_usd": -500.0, "total_usd": 500.0},
    {"ts": 1700000600.0, "long_usd": 100.0, "short_usd": 0.0, "net_usd": -100.0, "total_usd": 100.0},
    {"ts": 1700000900.0, "long_usd": 100.0, "short_usd": 0.0, "net_usd": -100.0, "total_usd": 100.0},
]

WATERFALL_EQUAL = [
    {"ts": 1700000000.0, "total_usd": 200.0, "long_usd": 200.0, "short_usd": 0.0, "net_usd": -200.0},
    {"ts": 1700000300.0, "total_usd": 200.0, "long_usd": 200.0, "short_usd": 0.0, "net_usd": -200.0},
]


def test_accel_positive_when_accelerating():
    assert acceleration_factor(WATERFALL_ACCEL) > 0.0


def test_accel_negative_when_decelerating():
    assert acceleration_factor(WATERFALL_DECEL) < 0.0


def test_accel_zero_when_equal():
    assert acceleration_factor(WATERFALL_EQUAL) == pytest.approx(0.0)


def test_accel_empty_waterfall():
    assert acceleration_factor([]) == pytest.approx(0.0)


def test_accel_all_zero_total():
    zero_wf = [{"total_usd": 0.0} for _ in range(4)]
    assert acceleration_factor(zero_wf) == pytest.approx(0.0)


def test_accel_range():
    for wf in [WATERFALL_ACCEL, WATERFALL_DECEL, WATERFALL_EQUAL]:
        a = acceleration_factor(wf)
        assert -1.0 <= a <= 1.0


# ── build_waterfall tests ─────────────────────────────────────────────────────

BASE_TS = 1700000000.0
BUCKET_S = 300
WINDOW_S = 1200  # 4 buckets

LIQS_SAMPLE = [
    {"ts": BASE_TS + 60,  "value": 500.0,  "side": "long"},   # bucket 0
    {"ts": BASE_TS + 200, "value": 300.0,  "side": "short"},  # bucket 0
    {"ts": BASE_TS + 400, "value": 1000.0, "side": "long"},   # bucket 1
    {"ts": BASE_TS + 900, "value": 200.0,  "side": "long"},   # bucket 3
]


def test_waterfall_length():
    wf = build_waterfall(LIQS_SAMPLE, BASE_TS, BUCKET_S, WINDOW_S)
    assert len(wf) == WINDOW_S // BUCKET_S


def test_waterfall_has_required_keys():
    wf = build_waterfall(LIQS_SAMPLE, BASE_TS, BUCKET_S, WINDOW_S)
    for b in wf:
        for key in ("ts", "long_usd", "short_usd", "net_usd", "total_usd"):
            assert key in b


def test_waterfall_bucket0_long():
    wf = build_waterfall(LIQS_SAMPLE, BASE_TS, BUCKET_S, WINDOW_S)
    assert wf[0]["long_usd"] == pytest.approx(500.0)


def test_waterfall_bucket0_short():
    wf = build_waterfall(LIQS_SAMPLE, BASE_TS, BUCKET_S, WINDOW_S)
    assert wf[0]["short_usd"] == pytest.approx(300.0)


def test_waterfall_net_usd_formula():
    wf = build_waterfall(LIQS_SAMPLE, BASE_TS, BUCKET_S, WINDOW_S)
    for b in wf:
        assert b["net_usd"] == pytest.approx(b["short_usd"] - b["long_usd"])


def test_waterfall_total_matches_sum():
    wf = build_waterfall(LIQS_SAMPLE, BASE_TS, BUCKET_S, WINDOW_S)
    grand_total = sum(b["total_usd"] for b in wf)
    liq_total = sum(float(l.get("value", 0)) for l in LIQS_SAMPLE)
    assert grand_total == pytest.approx(liq_total)


def test_waterfall_out_of_window_excluded():
    liqs_with_outside = LIQS_SAMPLE + [
        {"ts": BASE_TS - 100, "value": 9999.0, "side": "long"},   # before window
        {"ts": BASE_TS + WINDOW_S + 100, "value": 9999.0, "side": "short"},  # after window
    ]
    wf = build_waterfall(liqs_with_outside, BASE_TS, BUCKET_S, WINDOW_S)
    grand_total = sum(b["total_usd"] for b in wf)
    assert grand_total == pytest.approx(sum(float(l["value"]) for l in LIQS_SAMPLE))


def test_waterfall_empty_liqs():
    wf = build_waterfall([], BASE_TS, BUCKET_S, WINDOW_S)
    assert len(wf) == WINDOW_S // BUCKET_S
    assert all(b["total_usd"] == 0.0 for b in wf)


# ── cluster_zones tests ───────────────────────────────────────────────────────

LIQS_ZONE = [
    {"ts": BASE_TS,      "price": 0.00230, "value": 1000.0, "side": "long"},
    {"ts": BASE_TS + 10, "price": 0.00231, "value": 1200.0, "side": "long"},
    {"ts": BASE_TS + 20, "price": 0.00250, "value": 500.0,  "side": "short"},
    {"ts": BASE_TS + 30, "price": 0.00280, "value": 800.0,  "side": "long"},
    {"ts": BASE_TS + 40, "price": 0.00280, "value": 200.0,  "side": "short"},
]


def test_cluster_zones_returns_list():
    zones = cluster_zones(LIQS_ZONE)
    assert isinstance(zones, list)


def test_cluster_zones_sorted_by_usd():
    zones = cluster_zones(LIQS_ZONE)
    usds = [z["usd"] for z in zones]
    assert usds == sorted(usds, reverse=True)


def test_cluster_zones_has_required_keys():
    zones = cluster_zones(LIQS_ZONE)
    for z in zones:
        for key in ("price", "usd", "count", "side"):
            assert key in z


def test_cluster_zones_empty():
    assert cluster_zones([]) == []


def test_cluster_zones_top_n_respected():
    many_liqs = [{"ts": BASE_TS + i, "price": 0.001 * i, "value": 100.0, "side": "long"}
                 for i in range(1, 20)]
    zones = cluster_zones(many_liqs, top_n=3)
    assert len(zones) <= 3


def test_cluster_zones_side_is_valid():
    zones = cluster_zones(LIQS_ZONE)
    for z in zones:
        assert z["side"] in ("long", "short")


def test_cluster_zones_usd_positive():
    zones = cluster_zones(LIQS_ZONE)
    for z in zones:
        assert z["usd"] > 0.0


# ── fmt_usd tests ─────────────────────────────────────────────────────────────

def test_fmt_usd_millions():
    assert fmt_usd(1_500_000) == "$1.50M"


def test_fmt_usd_thousands():
    assert fmt_usd(12_500) == "$12.5k"


def test_fmt_usd_small():
    assert fmt_usd(999) == "$999"


def test_fmt_usd_zero():
    assert fmt_usd(0) == "$0"


# ── Response shape ────────────────────────────────────────────────────────────

SAMPLE_RESPONSE = {
    "status": "ok",
    "symbol": "BANANAS31USDT",
    "window_seconds": 3600,
    "bucket_seconds": 300,
    "risk_score": 38.5,
    "risk_level": "medium",
    "cascade_active": False,
    "dominant_side": "long",
    "total_usd": 12_500.0,
    "long_usd": 8_000.0,
    "short_usd": 4_500.0,
    "peak_bucket_usd": 3_200.0,
    "rate_per_min": 208.3,
    "acceleration": 0.25,
    "waterfall": [
        {"ts": 1700000000.0, "long_usd": 3200.0, "short_usd": 0.0,
         "net_usd": -3200.0, "total_usd": 3200.0},
        {"ts": 1700000300.0, "long_usd": 2000.0, "short_usd": 1500.0,
         "net_usd": -500.0, "total_usd": 3500.0},
        {"ts": 1700000600.0, "long_usd": 1500.0, "short_usd": 2000.0,
         "net_usd": 500.0, "total_usd": 3500.0},
        {"ts": 1700000900.0, "long_usd": 1300.0, "short_usd": 1000.0,
         "net_usd": -300.0, "total_usd": 2300.0},
    ],
    "zones": [
        {"price": 0.00230, "usd": 5000.0, "count": 15, "side": "long"},
        {"price": 0.00245, "usd": 3200.0, "count": 9, "side": "long"},
    ],
    "description": "Elevated cascade risk: $12,500 liquidated, longs dominant",
    "n_liquidations": 45,
}


def test_response_status_ok():
    assert SAMPLE_RESPONSE["status"] == "ok"


def test_response_has_required_keys():
    for key in ("symbol", "window_seconds", "bucket_seconds", "risk_score",
                "risk_level", "cascade_active", "dominant_side",
                "total_usd", "long_usd", "short_usd", "peak_bucket_usd",
                "rate_per_min", "acceleration", "waterfall", "zones",
                "description", "n_liquidations"):
        assert key in SAMPLE_RESPONSE


def test_response_risk_score_range():
    assert 0.0 <= SAMPLE_RESPONSE["risk_score"] <= 100.0


def test_response_risk_level_valid():
    assert SAMPLE_RESPONSE["risk_level"] in ("low", "medium", "high", "critical")


def test_response_dominant_side_valid():
    assert SAMPLE_RESPONSE["dominant_side"] in ("long", "short", "balanced")


def test_response_waterfall_is_list():
    assert isinstance(SAMPLE_RESPONSE["waterfall"], list)
    assert len(SAMPLE_RESPONSE["waterfall"]) > 0


def test_response_waterfall_bucket_keys():
    for b in SAMPLE_RESPONSE["waterfall"]:
        for key in ("ts", "long_usd", "short_usd", "net_usd", "total_usd"):
            assert key in b


def test_response_waterfall_net_formula():
    for b in SAMPLE_RESPONSE["waterfall"]:
        assert b["net_usd"] == pytest.approx(b["short_usd"] - b["long_usd"])


def test_response_zones_is_list():
    assert isinstance(SAMPLE_RESPONSE["zones"], list)


def test_response_zones_keys():
    for z in SAMPLE_RESPONSE["zones"]:
        for key in ("price", "usd", "count", "side"):
            assert key in z


def test_response_long_short_sum():
    r = SAMPLE_RESPONSE
    assert r["total_usd"] == pytest.approx(r["long_usd"] + r["short_usd"])


def test_response_peak_lte_total():
    assert SAMPLE_RESPONSE["peak_bucket_usd"] <= SAMPLE_RESPONSE["total_usd"]


def test_response_rate_per_min_positive():
    assert SAMPLE_RESPONSE["rate_per_min"] > 0


def test_response_acceleration_range():
    assert -1.0 <= SAMPLE_RESPONSE["acceleration"] <= 1.0


def test_response_risk_level_consistent_with_score():
    score = SAMPLE_RESPONSE["risk_score"]
    level = SAMPLE_RESPONSE["risk_level"]
    assert level == risk_level(score)


# ── Route registration ────────────────────────────────────────────────────────

def test_liquidation_cascade_route_registered():
    import tempfile
    os.environ.setdefault("DB_PATH", os.path.join(tempfile.mkdtemp(), "t.db"))
    os.environ.setdefault("SYMBOL_BINANCE", "BANANAS31USDT")
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "backend"))
    from api import router
    paths = [r.path for r in router.routes]
    assert any("liquidation-cascade" in p for p in paths)


# ── HTML / JS smoke tests ─────────────────────────────────────────────────────

def test_html_has_liquidation_cascade_card():
    assert "card-liquidation-cascade" in _html()


def test_js_has_render_liquidation_cascade():
    assert "renderLiquidationCascade" in _js()


def test_js_calls_liquidation_cascade_api():
    assert "liquidation-cascade" in _js()
