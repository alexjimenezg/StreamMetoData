"""
tests/unit/test_logging.py
---------
Unit tests for logging utilities.
"""

from utils.logging_config import setup_logging


def test_setup_logging_returns_logger():
    """setup_logging should return a usable logger instance."""
    logger = setup_logging(module_name="test_logger")

    assert logger.name == "test_logger"
    assert logger.handlers
