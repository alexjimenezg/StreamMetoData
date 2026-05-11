"""
utils/logging_config.py
---------
Centralized logging configuration for all Meteorisk modules.

Provides structured logging with both console and file outputs,
replacing all print() statements across the pipeline.
"""

import logging
import os
import sys
from datetime import datetime
from pathlib import Path


def setup_logging(
    module_name: str = "meteorisk",
    log_level: str = "INFO",
    log_file: str = None,
) -> logging.Logger:
    """
    Configure logging for a module with console and optional file output.

    Args:
        module_name: Name for the logger (typically __name__)
        log_level: Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        log_file: Optional file path to write logs to

    Returns:
        Configured logger instance
    """
    logger = logging.getLogger(module_name)
    
    # Avoid duplicate handlers if called multiple times
    if logger.handlers:
        return logger

    logger.setLevel(getattr(logging, log_level.upper(), logging.INFO))

    # Format: [TIMESTAMP] [LEVEL] [MODULE] Message
    formatter = logging.Formatter(
        fmt="[%(asctime)s] [%(levelname)8s] [%(name)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )

    # Console handler (always)
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(getattr(logging, log_level.upper(), logging.INFO))
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    # File handler (if requested)
    if log_file:
        log_dir = Path(log_file).parent
        log_dir.mkdir(parents=True, exist_ok=True)
        
        file_handler = logging.FileHandler(log_file, encoding="utf-8")
        file_handler.setLevel(getattr(logging, log_level.upper(), logging.INFO))
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

    return logger


def get_logger(module_name: str = "meteorisk") -> logging.Logger:
    """
    Get or create a logger for the given module name.
    
    Args:
        module_name: The module name (__name__ in calling module)
    
    Returns:
        Logger instance
    """
    return logging.getLogger(module_name)


# Global log directory
LOGS_DIR = Path("logs")
LOGS_DIR.mkdir(exist_ok=True)

# Create main logger for application startup
app_logger = setup_logging(
    module_name="meteorisk",
    log_level=os.getenv("METEORISK_LOG_LEVEL", "INFO"),
    log_file=str(LOGS_DIR / f"meteorisk_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log")
)
