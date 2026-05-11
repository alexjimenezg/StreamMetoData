"""
tests/unit/test_config.py
---------
Unit tests for configuration module.
"""

import pytest
from pathlib import Path
import config


def test_config_kafka_bootstrap_servers():
    """Test that Kafka bootstrap servers are configured."""
    assert config.KAFKA_BOOTSTRAP_SERVERS == "localhost:9092"


def test_config_kafka_topic():
    """Test that Kafka topic is configured."""
    assert config.KAFKA_TOPIC == "weather_stream"


def test_config_city():
    """Test that city is configured."""
    assert config.CITY_NAME == "Mexico City"
    assert config.LATITUDE == 19.4326
    assert config.LONGITUDE == -99.1332


def test_config_paths_exist():
    """Test that configured paths exist or are creatable."""
    paths = [
        config.DATA_RAW_PATH,
        config.DATA_PROCESSED_PATH,
        config.DATA_AGGREGATES_PATH,
        config.DATA_PREDICTIONS_PATH,
        config.DATA_METRICS_PATH,
        config.CHECKPOINTS_PATH,
    ]
    
    for path_str in paths:
        # Paths should exist or be valid directory paths
        p = Path(path_str)
        assert p.parent.exists() or p.exists(), f"Path invalid: {path_str}"


def test_config_model_path_valid():
    """Test that model path is valid."""
    model_path = Path(config.MODEL_PATH)
    assert model_path.parent.exists() or model_path.exists()
