"""
HitCraft AI Analytics Engine - Log Formatter

This module provides log formatting utilities, including a JSON formatter
for structured logging in production environments.
"""

import json
import logging
import traceback
from datetime import datetime

class JsonFormatter(logging.Formatter):
    """
    Format logs as JSON strings for easier parsing and analysis.
    """
    
    def format(self, record):
        """
        Format the log record as a JSON string.
        
        Args:
            record (LogRecord): The log record to format.
            
        Returns:
            str: JSON-formatted log entry.
        """
        # Create a basic log object
        log_object = {
            "timestamp": datetime.fromtimestamp(record.created).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            "module": record.module,
            "function": record.funcName,
            "line": record.lineno
        }
        
        # Add exception info if available
        if record.exc_info:
            log_object["exception"] = {
                "type": record.exc_info[0].__name__,
                "message": str(record.exc_info[1]),
                "traceback": traceback.format_exception(*record.exc_info)
            }
        
        # Add extra fields if available
        if hasattr(record, "extra"):
            log_object["extra"] = record.extra
        
        return json.dumps(log_object)
