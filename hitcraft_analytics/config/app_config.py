"""
HitCraft AI Analytics Engine - Application Configuration

This module contains the main application configuration settings.
Environmental variables can override these settings by using the same name prefixed with 'HITCRAFT_'.
"""

import os
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables from .env file if it exists
load_dotenv()

# Base directory
BASE_DIR = Path(__file__).parent.parent.absolute()

# Environment: development, testing, production
ENVIRONMENT = os.getenv("HITCRAFT_ENVIRONMENT", "development")

# API Settings
API_HOST = os.getenv("HITCRAFT_API_HOST", "0.0.0.0")
API_PORT = int(os.getenv("HITCRAFT_API_PORT", "8000"))
API_DEBUG = os.getenv("HITCRAFT_API_DEBUG", "True").lower() in ("true", "1", "t")
API_CORS_ORIGINS = os.getenv("HITCRAFT_API_CORS_ORIGINS", "*").split(",")

# Application Settings
APP_NAME = "HitCraft AI Analytics Engine"
APP_VERSION = "0.1.0"
APP_DESCRIPTION = "AI-powered analytics platform for HitCraft's Mixpanel data"

# Security Settings
SECRET_KEY = os.getenv("HITCRAFT_SECRET_KEY", "development_secret_key")
JWT_ALGORITHM = "HS256"
JWT_EXPIRATION_DELTA = 24 * 60 * 60  # 24 hours in seconds

# Data Storage Settings
DATA_DIR = os.path.join(BASE_DIR, "data", "storage")
CACHE_DIR = os.path.join(BASE_DIR, "data", "cache")

# Default user for development
DEFAULT_ADMIN_EMAIL = os.getenv("HITCRAFT_DEFAULT_ADMIN_EMAIL", "admin@example.com")
DEFAULT_ADMIN_PASSWORD = os.getenv("HITCRAFT_DEFAULT_ADMIN_PASSWORD", "admin")

# Ensure required directories exist
os.makedirs(DATA_DIR, exist_ok=True)
os.makedirs(CACHE_DIR, exist_ok=True)

# Logging configuration
LOG_LEVEL = os.getenv("HITCRAFT_LOG_LEVEL", "INFO")
LOG_FILE = os.path.join(BASE_DIR, "logs", "analytics.log")
os.makedirs(os.path.dirname(LOG_FILE), exist_ok=True)

# Schedule settings
# Time to run daily data pull (24-hour format, local time)
DAILY_DATA_PULL_TIME = os.getenv("HITCRAFT_DAILY_DATA_PULL_TIME", "04:00")

# Feature flags
ENABLE_AI_INSIGHTS = os.getenv("HITCRAFT_ENABLE_AI_INSIGHTS", "True").lower() in ("true", "1", "t")
ENABLE_SCHEDULED_REPORTS = os.getenv("HITCRAFT_ENABLE_SCHEDULED_REPORTS", "True").lower() in ("true", "1", "t")
ENABLE_NOTIFICATIONS = os.getenv("HITCRAFT_ENABLE_NOTIFICATIONS", "True").lower() in ("true", "1", "t")
