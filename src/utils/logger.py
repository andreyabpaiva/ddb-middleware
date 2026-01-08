"""
Logger utility for the distributed database middleware.
Provides logging to console and file with proper formatting.
"""
import logging
import os
from datetime import datetime
from typing import Optional


def setup_logger(node_id: Optional[int] = None, log_level: int = logging.INFO) -> logging.Logger:
    """
    Setup logger with console and file handlers.

    Args:
        node_id: Node identifier for log file naming
        log_level: Logging level (default: INFO)

    Returns:
        Configured logger instance
    """
    # Create logs directory if it doesn't exist
    log_dir = "logs"
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)

    # Create logger
    logger_name = f"ddb_node_{node_id}" if node_id else "ddb_middleware"
    logger = logging.getLogger(logger_name)
    logger.setLevel(log_level)

    # Avoid adding handlers multiple times
    if logger.handlers:
        return logger

    # Create formatters
    detailed_formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - [%(filename)s:%(lineno)d] - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )

    console_formatter = logging.Formatter(
        '%(asctime)s - %(levelname)s - %(message)s',
        datefmt='%H:%M:%S'
    )

    # Console handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(log_level)
    console_handler.setFormatter(console_formatter)
    logger.addHandler(console_handler)

    # File handler
    if node_id:
        log_file = os.path.join(log_dir, f"node_{node_id}.log")
    else:
        log_file = os.path.join(log_dir, "middleware.log")

    file_handler = logging.FileHandler(log_file, mode='a', encoding='utf-8')
    file_handler.setLevel(log_level)
    file_handler.setFormatter(detailed_formatter)
    logger.addHandler(file_handler)

    return logger


def get_logger(name: str) -> logging.Logger:
    """
    Get a logger instance by name.

    Args:
        name: Logger name

    Returns:
        Logger instance
    """
    return logging.getLogger(name)
