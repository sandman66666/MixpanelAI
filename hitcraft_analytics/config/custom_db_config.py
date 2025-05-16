"""
HitCraft AI Analytics Engine - Custom Database Configuration

This module contains custom overrides for database connections.
"""

import os
from dotenv import load_dotenv
from hitcraft_analytics.config.db_config import *

# Load environment variables from .env file if it exists
load_dotenv()

# Override TimescaleDB settings
USE_TIMESCALEDB = False
