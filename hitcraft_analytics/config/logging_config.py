"""
HitCraft AI Analytics Engine - Logging Configuration

This module contains configuration settings for application logging.
Environmental variables can override these settings by using the same name prefixed with 'HITCRAFT_LOG_'.
"""

import os
import logging.config
from pathlib import Path

from hitcraft_analytics.config.app_config import LOG_LEVEL, LOG_FILE, ENVIRONMENT

# Ensure log directory exists
log_dir = Path(LOG_FILE).parent
log_dir.mkdir(parents=True, exist_ok=True)

# Define logging configuration
LOGGING_CONFIG = {
    'version': 1,
    'disable_existing_loggers': False,
    'formatters': {
        'standard': {
            'format': '%(asctime)s [%(levelname)s] %(name)s: %(message)s',
            'datefmt': '%Y-%m-%d %H:%M:%S',
        },
        'detailed': {
            'format': '%(asctime)s [%(levelname)s] %(name)s (%(filename)s:%(lineno)d): %(message)s',
            'datefmt': '%Y-%m-%d %H:%M:%S',
        },
        'json': {
            '()': 'hitcraft_analytics.utils.logging.log_formatter.JsonFormatter',
        },
    },
    'handlers': {
        'console': {
            'level': LOG_LEVEL,
            'class': 'logging.StreamHandler',
            'formatter': 'standard',
        },
        'file': {
            'level': LOG_LEVEL,
            'class': 'logging.handlers.RotatingFileHandler',
            'filename': LOG_FILE,
            'maxBytes': 10485760,  # 10 MB
            'backupCount': 10,
            'formatter': 'detailed',
            'encoding': 'utf8',
        },
    },
    'loggers': {
        '': {  # Root logger
            'handlers': ['console', 'file'],
            'level': LOG_LEVEL,
            'propagate': True,
        },
        'hitcraft': {  # Application logger
            'handlers': ['console', 'file'],
            'level': LOG_LEVEL,
            'propagate': False,
        },
        'sqlalchemy.engine': {  # Database query logger
            'handlers': ['file'],
            'level': 'WARNING',
            'propagate': False,
        },
        'urllib3': {  # HTTP client logger
            'handlers': ['file'],
            'level': 'WARNING',
            'propagate': False,
        },
    },
}

# Adjust logging configuration based on environment
if ENVIRONMENT == 'production':
    # In production, use JSON formatter for structured logging
    LOGGING_CONFIG['handlers']['console']['formatter'] = 'json'
    LOGGING_CONFIG['handlers']['file']['formatter'] = 'json'
    
    # Increase log level for some components to reduce noise
    LOGGING_CONFIG['loggers']['sqlalchemy.engine']['level'] = 'ERROR'
    LOGGING_CONFIG['loggers']['urllib3']['level'] = 'ERROR'
elif ENVIRONMENT == 'development':
    # In development, enable debug logging for the application
    LOGGING_CONFIG['loggers']['hitcraft']['level'] = 'DEBUG'
    
    # Add more verbose SQL logging in development
    LOGGING_CONFIG['loggers']['sqlalchemy.engine']['level'] = 'INFO'
    LOGGING_CONFIG['loggers']['sqlalchemy.engine']['handlers'] = ['console', 'file']

# Apply logging configuration
logging.config.dictConfig(LOGGING_CONFIG)

# Get logger
def get_logger(name):
    """
    Get a logger with the given name.
    
    Args:
        name (str): Logger name, usually the module name.
        
    Returns:
        logging.Logger: Configured logger instance.
    """
    return logging.getLogger(f"hitcraft.{name}")
