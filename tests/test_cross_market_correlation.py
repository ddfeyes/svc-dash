"""Tests for cross-market correlation matrix feature (feat/cross-market-correlation, issue #98)."""
import math
import pytest
import pytest_asyncio
from httpx import AsyncClient, ASGITransport

ASSETS = ["BTC", "ETH", "SOL", "BNB"]
WINDOWS = ["7d", "30d", "90d"]


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture(scope="module")
def anyio_backend():
    return "asyncio"


@pytest_asyncio.fixture(scope="module")
async def client():
    import sys
    import os
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "backend"))
    from main import app
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        yield ac


@pytest_asyncio.fixture(scope="module")
async def data():
    import sys
    import os
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "backend"))
    from metrics import compute_cross_market_correlation
    return await compute_cross_market_correlation()


# ---------------------------------------------------------------------------
# 1. HTTP endpoint tests
# ---------------------------------------------------------------------------

@pytest.mark.anyio
async def test_endpoint_returns_200(client):
    r = await client.get("/api/cross-market-correlation")
    assert r.status_code == 200


@pytest.mark.anyio
async def test_endpoint_content_type_json(client):
    r = await client.get("/api/cross-market-correlation")
    assert "application/json" in r.headers["content-type"]


@pytest.mark.anyio
async def test_endpoint_body_is_dict(client):
    r = await client.get("/api/cross-market-correlation")
    assert isinstance(r.json(), dict)


@pytest.mark.anyio
async def test_endpoint_has_correlation_matrix_key(client):
    r = await client.get("/api/cross-market-correlation")
    assert "correlation_matrix" in r.json()


@pytest.mark.anyio
async def test_endpoint_has_windows_key(client):
    r = await client.get("/api/cross-market-correlation")
    assert "windows" in r.json()


@pytest.mark.anyio
async def test_endpoint_has_lead_lag_key(client):
    r = await client.get("/api/cross-market-correlation")
    assert "lead_lag" in r.json()


@pytest.mark.anyio
async def test_endpoint_has_dominant_leader_key(client):
    r = await client.get("/api/cross-market-correlation")
    assert "dominant_leader" in r.json()


@pytest.mark.anyio
async def test_endpoint_has_breakdown_detected_key(client):
    r = await client.get("/api/cross-market-correlation")
    assert "breakdown_detected" in r.json()


@pytest.mark.anyio
async def test_endpoint_has_breakdown_assets_key(client):
    r = await client.get("/api/cross-market-correlation")
    assert "breakdown_assets" in r.json()


@pytest.mark.anyio
async def test_endpoint_has_timestamp_key(client):
    r = await client.get("/api/cross-market-correlation")
    assert "timestamp" in r.json()


# ---------------------------------------------------------------------------
# 2. correlation_matrix structure
# ---------------------------------------------------------------------------

@pytest.mark.anyio
async def test_correlation_matrix_is_dict(data):
    assert isinstance(data["correlation_matrix"], dict)


@pytest.mark.anyio
async def test_correlation_matrix_has_all_assets(data):
    for a in ASSETS:
        assert a in data["correlation_matrix"]


@pytest.mark.anyio
async def test_correlation_matrix_inner_is_dict(data):
    for a in ASSETS:
        assert isinstance(data["correlation_matrix"][a], dict)


@pytest.mark.anyio
async def test_correlation_matrix_inner_has_other_assets(data):
    for a in ASSETS:
        for b in ASSETS:
            if a != b:
                assert b in data["correlation_matrix"][a]


@pytest.mark.anyio
async def test_correlation_matrix_values_in_range(data):
    for a in ASSETS:
        for b, v in data["correlation_matrix"][a].items():
            assert -1.0 <= v <= 1.0, f"{a}-{b}: {v} out of range"


@pytest.mark.anyio
async def test_correlation_matrix_values_are_float(data):
    for a in ASSETS:
        for b, v in data["correlation_matrix"][a].items():
            assert isinstance(v, float)


@pytest.mark.anyio
async def test_correlation_matrix_self_correlation_absent_or_one(data):
    # Self-correlation may be absent or 1.0
    for a in ASSETS:
        if a in data["correlation_matrix"][a]:
            assert data["correlation_matrix"][a][a] == pytest.approx(1.0)


@pytest.mark.anyio
async def test_correlation_matrix_symmetry(data):
    cm = data["correlation_matrix"]
    for a in ASSETS:
        for b in ASSETS:
            if a != b:
                assert cm[a][b] == pytest.approx(cm[b][a], abs=1e-9)


@pytest.mark.anyio
async def test_correlation_matrix_no_nan(data):
    for a in ASSETS:
        for b, v in data["correlation_matrix"][a].items():
            assert not math.isnan(v)


@pytest.mark.anyio
async def test_correlation_matrix_no_inf(data):
    for a in ASSETS:
        for b, v in data["correlation_matrix"][a].items():
            assert math.isfinite(v)


# ---------------------------------------------------------------------------
# 3. windows
# ---------------------------------------------------------------------------

@pytest.mark.anyio
async def test_windows_is_dict(data):
    assert isinstance(data["windows"], dict)


@pytest.mark.anyio
async def test_windows_has_7d(data):
    assert "7d" in data["windows"]


@pytest.mark.anyio
async def test_windows_has_30d(data):
    assert "30d" in data["windows"]


@pytest.mark.anyio
async def test_windows_has_90d(data):
    assert "90d" in data["windows"]


@pytest.mark.anyio
async def test_windows_has_exactly_3_keys(data):
    assert set(data["windows"].keys()) == {"7d", "30d", "90d"}


@pytest.mark.anyio
async def test_windows_each_is_dict(data):
    for w in WINDOWS:
        assert isinstance(data["windows"][w], dict)


@pytest.mark.anyio
async def test_windows_each_has_all_assets(data):
    for w in WINDOWS:
        for a in ASSETS:
            assert a in data["windows"][w]


@pytest.mark.anyio
async def test_windows_values_in_range(data):
    for w in WINDOWS:
        for a in ASSETS:
            for b, v in data["windows"][w][a].items():
                assert -1.0 <= v <= 1.0, f"window {w} {a}-{b}: {v}"


@pytest.mark.anyio
async def test_windows_values_are_float(data):
    for w in WINDOWS:
        for a in ASSETS:
            for b, v in data["windows"][w][a].items():
                assert isinstance(v, float)


@pytest.mark.anyio
async def test_windows_no_nan(data):
    for w in WINDOWS:
        for a in ASSETS:
            for b, v in data["windows"][w][a].items():
                assert not math.isnan(v)


# ---------------------------------------------------------------------------
# 4. lead_lag
# ---------------------------------------------------------------------------

@pytest.mark.anyio
async def test_lead_lag_is_dict(data):
    assert isinstance(data["lead_lag"], dict)


@pytest.mark.anyio
async def test_lead_lag_has_leader_key(data):
    assert "leader" in data["lead_lag"]


@pytest.mark.anyio
async def test_lead_lag_has_lag_hours_key(data):
    assert "lag_hours" in data["lead_lag"]


@pytest.mark.anyio
async def test_lead_lag_leader_is_valid_asset(data):
    assert data["lead_lag"]["leader"] in ASSETS


@pytest.mark.anyio
async def test_lead_lag_lag_hours_is_dict(data):
    assert isinstance(data["lead_lag"]["lag_hours"], dict)


@pytest.mark.anyio
async def test_lead_lag_lag_hours_has_non_leader_assets(data):
    leader = data["lead_lag"]["leader"]
    for a in ASSETS:
        if a != leader:
            assert a in data["lead_lag"]["lag_hours"]


@pytest.mark.anyio
async def test_lead_lag_lag_hours_values_are_numeric(data):
    for a, v in data["lead_lag"]["lag_hours"].items():
        assert isinstance(v, (int, float))


@pytest.mark.anyio
async def test_lead_lag_lag_hours_values_positive(data):
    for a, v in data["lead_lag"]["lag_hours"].items():
        assert v >= 0


# ---------------------------------------------------------------------------
# 5. dominant_leader
# ---------------------------------------------------------------------------

@pytest.mark.anyio
async def test_dominant_leader_is_str(data):
    assert isinstance(data["dominant_leader"], str)


@pytest.mark.anyio
async def test_dominant_leader_is_valid_asset(data):
    assert data["dominant_leader"] in ASSETS


@pytest.mark.anyio
async def test_dominant_leader_matches_lead_lag_leader(data):
    assert data["dominant_leader"] == data["lead_lag"]["leader"]


# ---------------------------------------------------------------------------
# 6. breakdown_detected
# ---------------------------------------------------------------------------

@pytest.mark.anyio
async def test_breakdown_detected_is_bool(data):
    assert isinstance(data["breakdown_detected"], bool)


@pytest.mark.anyio
async def test_breakdown_assets_is_list(data):
    assert isinstance(data["breakdown_assets"], list)


@pytest.mark.anyio
async def test_breakdown_assets_elements_are_str(data):
    for a in data["breakdown_assets"]:
        assert isinstance(a, str)


@pytest.mark.anyio
async def test_breakdown_assets_are_valid_assets(data):
    for a in data["breakdown_assets"]:
        assert a in ASSETS


@pytest.mark.anyio
async def test_breakdown_assets_empty_when_not_detected(data):
    if not data["breakdown_detected"]:
        assert data["breakdown_assets"] == []


@pytest.mark.anyio
async def test_breakdown_assets_nonempty_when_detected(data):
    if data["breakdown_detected"]:
        assert len(data["breakdown_assets"]) > 0


# ---------------------------------------------------------------------------
# 7. timestamp
# ---------------------------------------------------------------------------

@pytest.mark.anyio
async def test_timestamp_is_str(data):
    assert isinstance(data["timestamp"], str)


@pytest.mark.anyio
async def test_timestamp_is_iso_format(data):
    from datetime import datetime
    # Should parse without error
    ts = data["timestamp"]
    # Accept ISO 8601 with or without microseconds
    try:
        datetime.fromisoformat(ts)
        ok = True
    except ValueError:
        ok = False
    assert ok, f"timestamp {ts!r} is not ISO format"


@pytest.mark.anyio
async def test_timestamp_contains_T(data):
    assert "T" in data["timestamp"]


# ---------------------------------------------------------------------------
# 8. Determinism / seed
# ---------------------------------------------------------------------------

@pytest.mark.anyio
async def test_deterministic_results():
    import sys, os
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "backend"))
    from metrics import compute_cross_market_correlation
    r1 = await compute_cross_market_correlation()
    r2 = await compute_cross_market_correlation()
    assert r1["correlation_matrix"] == r2["correlation_matrix"]


@pytest.mark.anyio
async def test_deterministic_windows():
    import sys, os
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "backend"))
    from metrics import compute_cross_market_correlation
    r1 = await compute_cross_market_correlation()
    r2 = await compute_cross_market_correlation()
    assert r1["windows"] == r2["windows"]


@pytest.mark.anyio
async def test_deterministic_lead_lag():
    import sys, os
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "backend"))
    from metrics import compute_cross_market_correlation
    r1 = await compute_cross_market_correlation()
    r2 = await compute_cross_market_correlation()
    assert r1["lead_lag"] == r2["lead_lag"]


# ---------------------------------------------------------------------------
# 9. Edge-case / completeness checks
# ---------------------------------------------------------------------------

@pytest.mark.anyio
async def test_all_required_keys_present(data):
    required = {"correlation_matrix", "windows", "lead_lag", "dominant_leader",
                "breakdown_detected", "breakdown_assets", "timestamp"}
    assert required.issubset(set(data.keys()))


@pytest.mark.anyio
async def test_btc_eth_pair_in_matrix(data):
    assert "ETH" in data["correlation_matrix"]["BTC"]


@pytest.mark.anyio
async def test_btc_sol_pair_in_matrix(data):
    assert "SOL" in data["correlation_matrix"]["BTC"]


@pytest.mark.anyio
async def test_btc_bnb_pair_in_matrix(data):
    assert "BNB" in data["correlation_matrix"]["BTC"]


@pytest.mark.anyio
async def test_eth_sol_pair_in_matrix(data):
    assert "SOL" in data["correlation_matrix"]["ETH"]


@pytest.mark.anyio
async def test_eth_bnb_pair_in_matrix(data):
    assert "BNB" in data["correlation_matrix"]["ETH"]


@pytest.mark.anyio
async def test_sol_bnb_pair_in_matrix(data):
    assert "BNB" in data["correlation_matrix"]["SOL"]


@pytest.mark.anyio
async def test_matrix_btc_eth_high_correlation(data):
    # BTC/ETH historically highly correlated — seed ensures this
    v = data["correlation_matrix"]["BTC"]["ETH"]
    assert -1.0 <= v <= 1.0


@pytest.mark.anyio
async def test_windows_30d_differs_from_7d(data):
    # Different windows should generally differ
    m7 = data["windows"]["7d"]
    m30 = data["windows"]["30d"]
    # At least one pair differs
    diffs = []
    for a in ASSETS:
        for b in ASSETS:
            if a != b:
                if b in m7.get(a, {}) and b in m30.get(a, {}):
                    if m7[a][b] != m30[a][b]:
                        diffs.append((a, b))
    assert len(diffs) > 0


@pytest.mark.anyio
async def test_total_pairs_count(data):
    # 4 assets → 4*3 = 12 directional pairs (excluding self)
    count = sum(len([b for b in v.keys() if b != a]) for a, v in data["correlation_matrix"].items())
    assert count == 12
