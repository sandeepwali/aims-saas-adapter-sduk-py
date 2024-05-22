import os
import urllib3
import logging

from logging.handlers import RotatingFileHandler
from env import LOG_LEVEL


urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


def set_logger(
    logger_name="",
    max_log_size=10 * 1024 * 1024,
    backup_count=3,
    log_file="./logs/main.log",
    log_format="%(asctime)s - %(levelname)s - %(module)s - %(funcName)s - %(message)s",
):
    """
    Set up a logger with options for log rotation and formatting.

    Args:
        max_log_size (int): Maximum size (in bytes) for each log file before rotation. (Default: 10 MB)
        backup_count (int): Number of backup log files to retain. (Default: 3)
        log_file (str): Path to the main log file. (Default: ./logs/main.log)
        log_format (str): Log message format using logging formatter placeholders. (Default: %(asctime)s - %(levelname)s - %(module)s - %(message)s)

    Returns:
        logging.Logger: Configured logger instance.
    """

    # Create a dictionary to map log level names to logging level constants
    log_levels = {
        "CRITICAL": logging.CRITICAL,
        "ERROR": logging.ERROR,
        "WARNING": logging.WARNING,
        "INFO": logging.INFO,
        "DEBUG": logging.DEBUG,
    }

    # Convert the LOG_LEVEL string to the corresponding logging level
    log_level = log_levels.get(LOG_LEVEL, logging.INFO)

    logger = logging.getLogger(logger_name)
    logger.setLevel(log_level)
    formatter = logging.Formatter(log_format)

    # Add a StreamHandler to print log messages to the console
    stream_handler = logging.StreamHandler()
    stream_handler.setLevel(log_level)
    stream_handler.setFormatter(formatter)

    # Check if logs directory exists or not
    log_directory = os.path.dirname(log_file)
    if not os.path.exists(log_directory):
        os.makedirs(log_directory)

    # Use RotatingFileHandler for log rotation
    file_handler = RotatingFileHandler(
        log_file, maxBytes=max_log_size, backupCount=backup_count
    )
    file_handler.setLevel(log_level)
    file_handler.setFormatter(formatter)

    logger.addHandler(stream_handler)
    logger.addHandler(file_handler)

    return logger


def main(logger):

    return


if __name__ == "__main__":
    # Set logger
    logger = set_logger("main")
    main(logger)
