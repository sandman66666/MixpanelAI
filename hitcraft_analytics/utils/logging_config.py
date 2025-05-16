"""
Logging Configuration Module

This module provides utilities for setting up and configuring logging
throughout the HitCraft Analytics Engine.
"""

import os
import logging
import sys
from datetime import datetime
from logging.handlers import RotatingFileHandler

# Default log directory
LOG_DIR = os.getenv('HITCRAFT_LOG_DIR', 'logs')

# Ensure log directory exists
if not os.path.exists(LOG_DIR):
    try:
        os.makedirs(LOG_DIR)
    except Exception as e:
        print(f"Warning: Could not create log directory {LOG_DIR}: {str(e)}")

# Default log levels
DEFAULT_CONSOLE_LEVEL = logging.INFO
DEFAULT_FILE_LEVEL = logging.DEBUG

# Color codes for console output
COLORS = {
    'DEBUG': '\033[94m',    # Blue
    'INFO': '\033[92m',     # Green
    'WARNING': '\033[93m',  # Yellow
    'ERROR': '\033[91m',    # Red
    'CRITICAL': '\033[91m\033[1m',  # Bold Red
    'RESET': '\033[0m'      # Reset
}

class ColoredFormatter(logging.Formatter):
    """Custom formatter that adds colors to log levels for console output."""
    
    def format(self, record):
        levelname = record.levelname
        if levelname in COLORS:
            record.levelname = f"{COLORS[levelname]}{levelname}{COLORS['RESET']}"
        return super().format(record)

def setup_logger(name, console_level=None, file_level=None, log_file=None):
    """
    Set up a logger with console and file handlers.
    
    Args:
        name: Logger name
        console_level: Logging level for console output
        file_level: Logging level for file output
        log_file: Custom log file name
        
    Returns:
        logging.Logger: Configured logger
    """
    logger = logging.getLogger(name)
    
    # Only configure logger once
    if logger.handlers:
        return logger
    
    # Set logger level to lowest of console/file to capture all relevant logs
    logger.setLevel(min(
        console_level or DEFAULT_CONSOLE_LEVEL,
        file_level or DEFAULT_FILE_LEVEL
    ))
    
    # Set up console handler with colored output
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(console_level or DEFAULT_CONSOLE_LEVEL)
    
    console_format = '%(asctime)s [%(levelname)s] %(name)s: %(message)s'
    console_formatter = ColoredFormatter(console_format, datefmt='%Y-%m-%d %H:%M:%S')
    console_handler.setFormatter(console_formatter)
    
    logger.addHandler(console_handler)
    
    # Set up file handler if logging to file is enabled
    if log_file is None:
        # Default log file name based on logger name
        safe_name = name.replace('.', '_')
        log_file = f"{safe_name}.log"
    
    try:
        file_path = os.path.join(LOG_DIR, log_file)
        file_handler = RotatingFileHandler(
            file_path,
            maxBytes=10 * 1024 * 1024,  # 10 MB
            backupCount=5
        )
        file_handler.setLevel(file_level or DEFAULT_FILE_LEVEL)
        
        file_format = '%(asctime)s [%(levelname)s] %(name)s: %(message)s'
        file_formatter = logging.Formatter(file_format, datefmt='%Y-%m-%d %H:%M:%S')
        file_handler.setFormatter(file_formatter)
        
        logger.addHandler(file_handler)
    except Exception as e:
        # Fall back to console only logging if file logging fails
        logger.warning(f"Could not set up file logging to {log_file}: {str(e)}")
    
    return logger

def get_logger(name):
    """
    Get an existing logger or create a new one with default settings.
    
    Args:
        name: Logger name
        
    Returns:
        logging.Logger: Logger instance
    """
    logger = logging.getLogger(name)
    
    # If logger exists and is configured, return it
    if logger.handlers:
        return logger
    
    # Otherwise set up a new logger
    return setup_logger(name)
