"""
utils/spark_utils.py
---------
Spark utilities and factory methods.

Centralized SparkSession creation and common operations.
"""

from pyspark.sql import SparkSession
from utils.logging_config import get_logger

logger = get_logger(__name__)


def create_spark_session(app_name: str, log_level: str = "WARN") -> SparkSession:
    """
    Create and return a SparkSession with standard configuration.
    
    Args:
        app_name: Name of the Spark application
        log_level: Spark logging level (DEBUG, INFO, WARN, ERROR)
        
    Returns:
        Configured SparkSession
    """
    try:
        spark = (
            SparkSession.builder
            .appName(app_name)
            .getOrCreate()
        )
        spark.sparkContext.setLogLevel(log_level)
        logger.info(f"SparkSession '{app_name}' created successfully")
        return spark
    except Exception as exc:
        logger.error(f"Failed to create SparkSession '{app_name}': {exc}")
        raise


def stop_spark_session(spark: SparkSession) -> None:
    """
    Stop a SparkSession gracefully.
    
    Args:
        spark: SparkSession to stop
    """
    try:
        if spark and spark._sc is not None:
            spark.stop()
            logger.info("SparkSession stopped successfully")
    except Exception as exc:
        logger.warning(f"Error stopping SparkSession: {exc}")
