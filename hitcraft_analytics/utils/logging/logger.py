"""
HitCraft AI Analytics Engine - Logger Utility

This module provides a consistent logging interface for the entire application.
"""

import logging
import sys
import os
from datetime import datetime

# Configure basic logging
LOG_LEVEL = os.environ.get("HITCRAFT_LOG_LEVEL", "INFO")
log_level_num = getattr(logging, LOG_LEVEL.upper(), logging.INFO)

# Create log directory if it doesn't exist
log_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), "logs")
os.makedirs(log_dir, exist_ok=True)

# Configure logging
logging.basicConfig(
    level=log_level_num,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(os.path.join(log_dir, f"hitcraft-{datetime.now().strftime('%Y-%m-%d')}.log"))
    ]
)

def get_logger(name):
    """
    Get a logger with the given name.
    
    Args:
        name (str): Logger name, usually the module name.
        
    Returns:
        logging.Logger: Configured logger instance.
    """
    logger = logging.getLogger(name)
    logger.setLevel(log_level_num)
    return logger
