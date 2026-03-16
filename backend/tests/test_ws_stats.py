"""Tests for GET /api/ws-stats endpoint and WS message counter."""
import asyncio
import os
import sys
import tempfile
import time

import pytest

os.environ.setdefault("DB_PATH", os.path.join(tempfile.mkdtemp(), "test_ws_stats.db"))
os.environ.setdefault("SYMBOL_BINANCE", "BANANAS31USDT")
os.environ.setdefault("SYMBOL_BYBIT", "BANANAS31USDT")

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from storage import init_db


# ── Helper: fresh FastAPI test client ─────────────────────────────────────────


def _make_client():
    from fastapi import FastAPI
    from fastapi.testclient import TestClient
    import api as api_module

    app = FastAPI()
    app.include_router(api_module.router)
    return TestClient(app), api_module


# ── Tests ─────────────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_ws_stats_endpoint_exists():
    """GET /api/ws-stats should return 200."""
    await init_db()
    client, _ = _make_client()
    r = client.get("/api/ws-stats")
    assert r.status_code == 200


@pytest.mark.asyncio
async def test_ws_stats_response_shape():
    """Response must contain required fields."""
    await init_db()
    client, _ = _make_client()
    r = client.get("/api/ws-stats")
    data = r.json()
    assert "connections" in data
    assert "messages_per_sec" in data
    assert "uptime_sec" in data
    assert "total_messages" in data
    assert data["status"] == "ok"


@pytest.mark.asyncio
async def test_ws_stats_connections_is_int():
    """connections field must be a non-negative integer."""
    await init_db()
    client, _ = _make_client()
    data = client.get("/api/ws-stats").json()
    assert isinstance(data["connections"], int)
    assert data["connections"] >= 0


@pytest.mark.asyncio
async def test_ws_stats_messages_per_sec_is_float():
    """messages_per_sec must be a non-negative float."""
    await init_db()
    client, _ = _make_client()
    data = client.get("/api/ws-stats").json()
    assert isinstance(data["messages_per_sec"], (int, float))
    assert data["messages_per_sec"] >= 0.0


@pytest.mark.asyncio
async def test_ws_stats_uptime_is_positive():
    """uptime_sec must be positive (module has been loaded for >0 seconds)."""
    await init_db()
    client, _ = _make_client()
    data = client.get("/api/ws-stats").json()
    assert data["uptime_sec"] > 0.0


@pytest.mark.asyncio
async def test_ws_stats_total_messages_is_int():
    """total_messages must be a non-negative integer."""
    await init_db()
    client, _ = _make_client()
    data = client.get("/api/ws-stats").json()
    assert isinstance(data["total_messages"], int)
    assert data["total_messages"] >= 0


@pytest.mark.asyncio
async def test_ws_stats_no_connections_at_start():
    """Without WS connections the counter should be 0."""
    await init_db()
    client, _ = _make_client()
    data = client.get("/api/ws-stats").json()
    assert data["connections"] == 0


@pytest.mark.asyncio
async def test_ws_stats_rate_consistent_with_total_and_uptime():
    """messages_per_sec ≈ total_messages / uptime_sec (within 1%)."""
    await init_db()
    client, _ = _make_client()
    data = client.get("/api/ws-stats").json()
    total = data["total_messages"]
    uptime = data["uptime_sec"]
    rate = data["messages_per_sec"]
    if uptime > 0 and total > 0:
        expected = total / uptime
        assert abs(rate - expected) < expected * 0.01 + 0.01
    else:
        assert rate >= 0.0


@pytest.mark.asyncio
async def test_ws_inc_increments_global_counter():
    """_ws_inc() should increment the module-level _ws_msg_count."""
    await init_db()
    import api as api_module

    before = api_module._ws_msg_count
    api_module._ws_inc(5)
    assert api_module._ws_msg_count == before + 5


@pytest.mark.asyncio
async def test_ws_inc_default_increments_by_one():
    """_ws_inc() with no args should increment by 1."""
    await init_db()
    import api as api_module

    before = api_module._ws_msg_count
    api_module._ws_inc()
    assert api_module._ws_msg_count == before + 1


@pytest.mark.asyncio
async def test_ws_msg_count_reflected_in_stats():
    """After calling _ws_inc, total_messages in /ws-stats increases."""
    await init_db()
    import api as api_module

    client, _ = _make_client()

    before_data = client.get("/api/ws-stats").json()
    before_total = before_data["total_messages"]

    api_module._ws_inc(10)

    after_data = client.get("/api/ws-stats").json()
    assert after_data["total_messages"] == before_total + 10


@pytest.mark.asyncio
async def test_ws_stats_start_time_is_set():
    """_ws_start_time must be a float set at module load (before now)."""
    await init_db()
    import api as api_module

    assert isinstance(api_module._ws_start_time, float)
    assert api_module._ws_start_time <= time.time()


@pytest.mark.asyncio
async def test_ws_stats_uptime_grows():
    """Two sequential calls to /ws-stats should show increasing uptime."""
    await init_db()
    client, _ = _make_client()
    d1 = client.get("/api/ws-stats").json()
    await asyncio.sleep(0.05)
    d2 = client.get("/api/ws-stats").json()
    assert d2["uptime_sec"] >= d1["uptime_sec"]


@pytest.mark.asyncio
async def test_connection_manager_tracks_connections():
    """ConnectionManager._connections dict counts sockets per symbol."""
    await init_db()
    import api as api_module

    mgr = api_module.ConnectionManager()
    assert sum(len(v) for v in mgr._connections.values()) == 0


@pytest.mark.asyncio
async def test_alert_manager_tracks_clients():
    """AlertManager._clients starts empty."""
    await init_db()
    import api as api_module

    amgr = api_module.AlertManager()
    assert len(amgr._clients) == 0


@pytest.mark.asyncio
async def test_ws_stats_json_content_type():
    """Response Content-Type should be application/json."""
    await init_db()
    client, _ = _make_client()
    r = client.get("/api/ws-stats")
    assert "application/json" in r.headers.get("content-type", "")


@pytest.mark.asyncio
async def test_ws_stats_status_ok():
    """status field should equal 'ok'."""
    await init_db()
    client, _ = _make_client()
    data = client.get("/api/ws-stats").json()
    assert data["status"] == "ok"
