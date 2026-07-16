"""
conftest.py -- Pytest configuration and shared fixtures
"""
import pytest
import os

def pytest_configure(config):
    config.addinivalue_line("markers","unit: Unit tests (no Spark required)")
    config.addinivalue_line("markers","integration: Integration tests (requires PySpark)")
    config.addinivalue_line("markers","e2e: End-to-end pipeline tests")

@pytest.fixture(scope="session")
def test_data_path(tmp_path_factory):
    """Shared temp directory for test data."""
    return str(tmp_path_factory.mktemp("streamflow_test_data"))
