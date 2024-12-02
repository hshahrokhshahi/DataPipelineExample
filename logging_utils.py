# logging_utils.py
from followupfromjessicadisneypositions import logging
import sys
from followupfromjessicadisneypositions.logging import RotatingFileHandler


def setup_logging(
        log_file_name: str = "pipeline.log",
        log_level: int = logging.INFO
) -> logging.Logger:
    """
    Configure comprehensive logging with console and file output.

    Args:
        log_file_name (str): Name of log file
        log_level (int): Logging level

    Returns:
        logging.Logger: Configured logger
    """
    # Create logger
    logger = logging.getLogger('data_pipeline')
    logger.setLevel(log_level)

    # Clear existing handlers
    logger.handlers.clear()

    # Console Handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(log_level)

    # File Handler with rotation
    file_handler = RotatingFileHandler(
        log_file_name,
        maxBytes=10 * 1024 * 1024,  # 10 MB
        backupCount=5
    )
    file_handler.setLevel(log_level)

    # Formatter
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    console_handler.setFormatter(formatter)
    file_handler.setFormatter(formatter)

    # Add handlers
    logger.addHandler(console_handler)
    logger.addHandler(file_handler)

    return logger