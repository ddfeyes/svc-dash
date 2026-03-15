"""Tests for API endpoints."""
import asyncio
import os
import tempfile
import pytest

os.environ["DB_PATH"] = os.path.join(tempfile.mkdtemp(), "test_api.db")
os.environ["SYMBOL_BINANCE"] = "BANANAS31USDT"
os.environ["SYMBOL_BYBIT"] = "BANANAS31USDT"

import sys
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from storage import init_db


@pytest.mark.asyncio
async def test_api_router_imports():
    """API module should import without errors."""
    await init_db()
    from api import router
    assert router is not None


@pytest.mark.asyncio
async def test_api_has_routes():
    """API router should have registered routes."""
    await init_db()
    from api import router
    routes = [r.path for r in router.routes]
    assert len(routes) > 0
    # Check some expected endpoints exist
    assert "/symbols" in routes or any("/symbols" in r for r in routes)
