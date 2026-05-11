"""
utils/schema_registry.py
---------
Centralized schema definitions for weather events and models.

Eliminates schema duplication across producer, streaming, and predict modules.
"""

from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)


def get_weather_schema() -> StructType:
    """
    Weather event schema - used by all modules.
    
    This is the schema produced by producer.py and consumed by
    streaming.py and predict_stream.py.
    """
    return StructType([
        StructField("city", StringType(), True),
        StructField("timestamp", StringType(), True),
        StructField("temperature", DoubleType(), True),
        StructField("humidity", DoubleType(), True),
        StructField("precipitation", DoubleType(), True),
        StructField("wind_speed", DoubleType(), True),
        StructField("wind_gusts", DoubleType(), True),
        StructField("surface_pressure", DoubleType(), True),
        StructField("apparent_temperature", DoubleType(), True),
        StructField("weather_code", IntegerType(), True),
    ])


def get_feature_columns() -> list:
    """
    Feature columns used for ML model training and prediction.
    
    IMPORTANT: This order is critical - train_model.py and 
    predict_stream.py must use the same order!
    """
    return [
        "temperature",
        "humidity",
        "precipitation",
        "wind_speed",
        "wind_gusts",
        "surface_pressure",
        "apparent_temperature",
        "weather_code",
    ]


def get_required_columns() -> list:
    """Columns that must not be null for processing."""
    return [
        "city",
        "timestamp",
        "temperature",
        "humidity",
        "precipitation",
        "wind_speed",
    ]
