"""
utils/kafka_utils.py
---------
Kafka utilities and factory methods.

Centralized Kafka producer/consumer creation.
"""

from kafka import KafkaProducer, KafkaConsumer
from kafka.errors import KafkaError, NoBrokersAvailable
import json
from utils.logging_config import get_logger

logger = get_logger(__name__)


def create_kafka_producer(bootstrap_servers: str) -> KafkaProducer:
    """
    Create a Kafka producer for JSON events.
    
    Args:
        bootstrap_servers: Comma-separated list of Kafka brokers
        
    Returns:
        Configured KafkaProducer
        
    Raises:
        NoBrokersAvailable: If cannot connect to Kafka
        KafkaError: For other Kafka errors
    """
    try:
        producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            linger_ms=10,
            retries=3,
        )
        logger.info(f"KafkaProducer connected to {bootstrap_servers}")
        return producer
    except NoBrokersAvailable as exc:
        logger.error(f"Cannot connect to Kafka brokers at {bootstrap_servers}: {exc}")
        logger.error("Is Kafka running? Try: docker compose up -d")
        raise
    except KafkaError as exc:
        logger.error(f"Kafka error creating producer: {exc}")
        raise


def close_kafka_producer(producer: KafkaProducer, timeout: int = 5) -> None:
    """
    Close a Kafka producer gracefully.
    
    Args:
        producer: KafkaProducer to close
        timeout: Timeout in seconds for final flush
    """
    try:
        if producer:
            producer.flush(timeout=timeout)
            producer.close()
            logger.info("KafkaProducer closed successfully")
    except Exception as exc:
        logger.warning(f"Error closing KafkaProducer: {exc}")
