"""Shared pytest fixtures for integration tests."""
import pytest
import requests


def pytest_configure(config):
    config.addinivalue_line("markers", "integration: mark test as integration (requires live server)")


@pytest.fixture(scope="session")
def base_url():
    return "http://localhost:8765/api"


@pytest.fixture(scope="session")
def symbol():
    return "BANANAS31USDT"


@pytest.fixture(scope="session")
def server_available(base_url):
    """Skip all integration tests if server is not reachable."""
    try:
        r = requests.get(f"{base_url}/health", timeout=3)
        return r.status_code == 200
    except Exception:
        return False
