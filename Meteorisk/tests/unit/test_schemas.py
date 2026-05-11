"""
tests/unit/test_schema_registry.py
---------
Unit tests for schema registry utilities.
"""

import pytest
from utils.schema_registry import get_weather_schema, get_feature_columns, get_required_columns


def test_weather_schema_fields():
    """Test that weather schema has all required fields."""
    schema = get_weather_schema()
    field_names = [f.name for f in schema.fields]
    
    expected_fields = [
        "city", "timestamp", "temperature", "humidity",
        "precipitation", "wind_speed", "wind_gusts",
        "surface_pressure", "apparent_temperature", "weather_code"
    ]
    
    for field in expected_fields:
        assert field in field_names, f"Missing field: {field}"


def test_feature_columns():
    """Test that feature columns are defined."""
    features = get_feature_columns()
    
    assert isinstance(features, list)
    assert len(features) == 8
    assert "temperature" in features
    assert "wind_speed" in features


def test_feature_columns_order():
    """Test that feature columns maintain order (critical for ML)."""
    features = get_feature_columns()
    
    # Order matters for ML model compatibility
    expected_order = [
        "temperature", "humidity", "precipitation", "wind_speed",
        "wind_gusts", "surface_pressure", "apparent_temperature", "weather_code"
    ]
    
    assert features == expected_order, "Feature column order changed!"


def test_required_columns():
    """Test that required columns are defined."""
    required = get_required_columns()
    
    assert isinstance(required, list)
    assert len(required) > 0
    assert "city" in required
    assert "timestamp" in required
    assert "temperature" in required
