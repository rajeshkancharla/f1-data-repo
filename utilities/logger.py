"""
Logging setup for the F1 Analytics Pipeline.

Using colorlog for better readability during development and debugging.
Supports both console and file logging.
"""
import logging
import sys
from pathlib import Path
from colorlog import ColoredFormatter

import config


def setup_logger(name, log_file=None):
    """
    Create a logger with colored console output and optional file logging.
    
    Args:
        name: Logger name (typically __name__ from calling module)
        log_file: Optional path to log file. If None, uses config settings.
        
    Returns:
        logging.Logger: Configured logger instance
    """
    logger = logging.getLogger(name)
    
    # Avoid duplicate handlers if logger already exists
    if logger.handlers:
        return logger
    
    logger.setLevel(getattr(logging, config.LOG_LEVEL))
    logger.propagate = False  # Prevent duplicate logs
    
    # Console handler with colors (if enabled)
    if config.LOG_TO_CONSOLE:
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(getattr(logging, config.LOG_LEVEL))
        
        # Color scheme - easier to spot errors and warnings
        console_formatter = ColoredFormatter(
            "%(log_color)s%(asctime)s - %(name)s - %(levelname)s%(reset)s - %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
            log_colors={
                "DEBUG": "cyan",
                "INFO": "green",
                "WARNING": "yellow",
                "ERROR": "red",
                "CRITICAL": "red,bg_white",
            },
        )
        
        console_handler.setFormatter(console_formatter)
        logger.addHandler(console_handler)
    
    # File handler (if enabled)
    if config.LOG_TO_FILE:
        if log_file is None:
            log_file = config.get_log_file_path()
        
        # Ensure log directory exists
        Path(log_file).parent.mkdir(parents=True, exist_ok=True)
        
        file_handler = logging.FileHandler(log_file, encoding='utf-8')
        file_handler.setLevel(getattr(logging, config.LOG_LEVEL))
        
        # File formatter (no colors for file)
        file_formatter = logging.Formatter(
            "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S"
        )
        
        file_handler.setFormatter(file_formatter)
        logger.addHandler(file_handler)
        
        # Log the log file location
        logger.info(f"Logging to file: {log_file}")
    
    return logger


def get_logger(name, log_file=None):
    """
    Convenience function to get or create a logger.
    
    Args:
        name: Logger name
        log_file: Optional log file path
        
    Returns:
        logging.Logger: Configured logger instance
    """
    return setup_logger(name, log_file)


# Convenience functions for quick logging without setup
def log_info(message, logger_name="f1_pipeline"):
    """Quick info log without explicit logger setup."""
    logger = setup_logger(logger_name)
    logger.info(message)


def log_error(message, logger_name="f1_pipeline"):
    """Quick error log without explicit logger setup."""
    logger = setup_logger(logger_name)
    logger.error(message)


def log_warning(message, logger_name="f1_pipeline"):
    """Quick warning log without explicit logger setup."""
    logger = setup_logger(logger_name)
    logger.warning(message)


def log_debug(message, logger_name="f1_pipeline"):
    """Quick debug log without explicit logger setup."""
    logger = setup_logger(logger_name)
    logger.debug(message)