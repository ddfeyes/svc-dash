"""Shared fixtures for svc-dash tests."""
import asyncio
import os
import tempfile
import pytest

# Use temp DB for all tests
os.environ["DB_PATH"] = os.path.join(tempfile.mkdtemp(), "test.db")
os.environ["SYMBOL_BINANCE"] = "BANANAS31USDT"
os.environ["SYMBOL_BYBIT"] = "BANANAS31USDT"


@pytest.fixture(scope="session")
def event_loop():
    loop = asyncio.new_event_loop()
    yield loop
    loop.close()


@pytest.fixture
async def db():
    """Initialize and return a test database."""
    from storage import init_db, get_db
    await init_db()
    conn = await get_db()
    yield conn
    await conn.close()
