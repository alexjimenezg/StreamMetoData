"""
tests/unit/test_producer.py
---------
Unit tests for producer helper functions.
"""

import producer


def test_build_event_maps_weather_fields(sample_weather_event):
    """build_event should map Open-Meteo data to the internal schema."""
    observation = {
        "time": sample_weather_event["timestamp"],
        "temperature_2m": sample_weather_event["temperature"],
        "relative_humidity_2m": sample_weather_event["humidity"],
        "precipitation": sample_weather_event["precipitation"],
        "wind_speed_10m": sample_weather_event["wind_speed"],
        "wind_gusts_10m": sample_weather_event["wind_gusts"],
        "surface_pressure": sample_weather_event["surface_pressure"],
        "apparent_temperature": sample_weather_event["apparent_temperature"],
        "weather_code": sample_weather_event["weather_code"],
    }

    event = producer.build_event(observation)

    assert event["city"] == "Mexico City"
    assert event["timestamp"] == observation["time"]
    assert event["temperature"] == observation["temperature_2m"]
    assert event["humidity"] == observation["relative_humidity_2m"]
    assert event["wind_speed"] == observation["wind_speed_10m"]
    assert event["weather_code"] == observation["weather_code"]


def test_apply_demo_anomalies_injects_temperature_spike(sample_weather_event):
    """DEMO mode should inject anomalies periodically."""
    event = dict(sample_weather_event)
    modified = producer.apply_demo_anomalies(event, index=15)

    assert modified["temperature"] == 36.5


def test_apply_demo_anomalies_injects_precipitation_spike(sample_weather_event):
    """DEMO mode should inject heavy rain spikes."""
    event = dict(sample_weather_event)
    modified = producer.apply_demo_anomalies(event, index=25)

    assert modified["precipitation"] == 55.0


def test_apply_demo_anomalies_injects_wind_spike(sample_weather_event):
    """DEMO mode should inject wind spikes."""
    event = dict(sample_weather_event)
    modified = producer.apply_demo_anomalies(event, index=40)

    assert modified["wind_speed"] == 70.0
