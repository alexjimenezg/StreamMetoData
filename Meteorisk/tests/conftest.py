"""
tests/conftest.py
---------
Pytest configuration and shared fixtures for testing.
"""

import pytest
import sys
import os
from pathlib import Path

# Add parent directory to path so imports work
sys.path.insert(0, str(Path(__file__).parent.parent))


@pytest.fixture
def project_root():
    """Return the project root directory."""
    return Path(__file__).parent.parent


@pytest.fixture
def data_dir(tmp_path):
    """Create a temporary data directory for tests."""
    data = tmp_path / "data"
    data.mkdir()
    (data / "processed").mkdir()
    (data / "aggregates").mkdir()
    (data / "predictions").mkdir()
    (data / "metrics").mkdir()
    (data / "checkpoints").mkdir()
    return data


@pytest.fixture
def sample_weather_event():
    """Return a sample weather event."""
    return {
        "city": "Mexico City",
        "timestamp": "2024-01-15T12:00",
        "temperature": 22.5,
        "humidity": 65.0,
        "precipitation": 0.0,
        "wind_speed": 8.5,
        "wind_gusts": 15.2,
        "surface_pressure": 1013.5,
        "apparent_temperature": 21.0,
        "weather_code": 0,
    }
