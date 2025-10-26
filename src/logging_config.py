"""
Centralized logging configuration for clif_mimic package.

This module provides a unified logging setup that creates:
- Main log file (all events INFO+)
- Error log file (warnings/errors only)
- Console output (user-facing messages)

All logs are stored in output/logs/ subdirectory with emoji formatting
for better readability.
"""

import logging
import sys
import os
from pathlib import Path
from typing import Optional


# Emoji mapping for different log levels
EMOJI_MAP = {
    'DEBUG': '🐛',
    'INFO': '📢',
    'WARNING': '⚠️',
    'ERROR': '❌',
    'CRITICAL': '🆘'
}


class EmojiFormatter(logging.Formatter):
    """Custom formatter that adds emoji indicators to log messages."""

    def format(self, record):
        """Add emoji to the record before formatting."""
        emoji = EMOJI_MAP.get(record.levelname, '•')
        record.emoji = emoji
        return super().format(record)


def setup_logging(
    output_directory: Optional[str] = None,
    level: int = logging.INFO,
    console_output: bool = True,
    separate_error_log: bool = True
) -> logging.Logger:
    """
    Configure centralized logging for the clif_mimic package.

    Creates log files in output_directory/logs/ subdirectory and optionally
    outputs to console. All loggers in the clif_mimic.* namespace will use this
    configuration.

    This function is idempotent - calling it multiple times will reconfigure
    the logging system with the new parameters. This is safe and allows
    updating the log directory when needed.

    Parameters
    ----------
    output_directory : str, optional
        Base output directory. Logs will be created in {output_directory}/logs/
        If None, uses current working directory + 'output'
    level : int, default=logging.INFO
        Minimum log level to capture (DEBUG, INFO, WARNING, ERROR, CRITICAL)
    console_output : bool, default=True
        Whether to show log messages in console (maintains print() experience)
    separate_error_log : bool, default=True
        Whether to create separate log file for warnings/errors only

    Returns
    -------
    logging.Logger
        Configured root logger for clif_mimic package

    Notes
    -----
    Log file structure:
    - output/logs/clif_mimic_all.log: All log messages (INFO and above)
    - output/logs/clif_mimic_errors.log: Only warnings and errors (if separate_error_log=True)

    Console output matches what users would see with print() statements,
    but messages are also saved to log files with full context.

    Examples
    --------
    >>> from src.logging_config import setup_logging
    >>> logger = setup_logging(output_directory='./output')
    >>> logger.info("Loading data...")  # Appears in console + log files
    """
    # Determine log directory
    if output_directory is None:
        output_directory = os.path.join(os.getcwd(), 'output')

    log_dir = os.path.join(output_directory, 'logs')
    os.makedirs(log_dir, exist_ok=True)

    # Get or create root logger for clif_mimic package
    root_logger = logging.getLogger('clif_mimic')
    root_logger.setLevel(logging.DEBUG)  # Capture everything, handlers will filter

    # Remove any existing handlers to avoid duplicates (makes this idempotent)
    root_logger.handlers = []

    # Format strings with timestamp precision to seconds
    file_format = '%(asctime)s | %(emoji)s %(levelname)-8s | %(name)s | [%(funcName)s:%(lineno)d] | %(message)s'
    console_format = '%(asctime)s | %(emoji)s %(message)s'

    # Date format: YYYY-MM-DD HH:MM:SS (seconds precision)
    date_format = '%Y-%m-%d %H:%M:%S'

    # Handler 1: Main log file (all messages INFO and above)
    all_handler = logging.FileHandler(
        os.path.join(log_dir, 'clif_mimic_all.log'),
        mode='w',
        encoding='utf-8'
    )
    all_handler.setLevel(level)
    all_handler.setFormatter(EmojiFormatter(file_format, datefmt=date_format))
    root_logger.addHandler(all_handler)

    # Handler 2: Error log file (warnings and errors only)
    if separate_error_log:
        error_handler = logging.FileHandler(
            os.path.join(log_dir, 'clif_mimic_errors.log'),
            mode='w',
            encoding='utf-8'
        )
        error_handler.setLevel(logging.WARNING)
        error_handler.setFormatter(EmojiFormatter(file_format, datefmt=date_format))
        root_logger.addHandler(error_handler)

    # Handler 3: Console output (user-facing, like print())
    if console_output:
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(level)
        console_handler.setFormatter(EmojiFormatter(console_format, datefmt=date_format))
        root_logger.addHandler(console_handler)

    # Prevent propagation to avoid duplicate messages
    root_logger.propagate = False

    # Log initialization message
    root_logger.debug(f"Logging initialized - logs directory: {log_dir}")

    return root_logger


def get_logger(name: str) -> logging.Logger:
    """
    Get a logger for a specific module within clif_mimic.

    This is a convenience function that ensures the logger name
    follows the clif_mimic.* namespace convention.

    Parameters
    ----------
    name : str
        Module name (will be prefixed with 'clif_mimic.' if not already)

    Returns
    -------
    logging.Logger
        Configured logger for the module

    Examples
    --------
    >>> from src.logging_config import get_logger
    >>> logger = get_logger('tables.patient')  # Creates 'clif_mimic.tables.patient' logger
    >>> logger.info("Patient table loaded")
    """
    if not name.startswith('clif_mimic.'):
        name = f'clif_mimic.{name}'
    return logging.getLogger(name)
