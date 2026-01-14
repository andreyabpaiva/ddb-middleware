import logging
import os
from datetime import datetime
from typing import Optional


def setup_logger(node_id: Optional[int] = None, log_level: int = logging.INFO) -> logging.Logger:

    log_dir = "logs"
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)

    logger_name = f"ddb_node_{node_id}" if node_id else "ddb_middleware"
    logger = logging.getLogger(logger_name)
    logger.setLevel(log_level)

    if logger.handlers:
        return logger

    detailed_formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - [%(filename)s:%(lineno)d] - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )

    console_formatter = logging.Formatter(
        '%(asctime)s - %(levelname)s - %(message)s',
        datefmt='%H:%M:%S'
    )

    console_handler = logging.StreamHandler()
    console_handler.setLevel(log_level)
    console_handler.setFormatter(console_formatter)
    logger.addHandler(console_handler)

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

    return logging.getLogger(name)
