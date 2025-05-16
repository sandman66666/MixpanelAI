"""
HitCraft AI Analytics Engine - Database Configuration

This module contains configuration settings for database connections.
Environmental variables can override these settings by using the same name prefixed with 'HITCRAFT_DB_'.
"""

import os
from dotenv import load_dotenv

# Load environment variables from .env file if it exists
load_dotenv()

# Database Configuration
DB_TYPE = os.getenv("HITCRAFT_DB_TYPE", "postgresql")
DB_HOST = os.getenv("HITCRAFT_DB_HOST", "localhost")
DB_PORT = int(os.getenv("HITCRAFT_DB_PORT", "5432"))
DB_NAME = os.getenv("HITCRAFT_DB_NAME", "hitcraft_analytics")
DB_USER = os.getenv("HITCRAFT_DB_USER", "postgres")
DB_PASSWORD = os.getenv("HITCRAFT_DB_PASSWORD", "postgres")
DB_SCHEMA = os.getenv("HITCRAFT_DB_SCHEMA", "public")

# Connection string for SQLAlchemy
DB_CONNECTION_STRING = f"{DB_TYPE}://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

# Connection pool settings
DB_POOL_SIZE = int(os.getenv("HITCRAFT_DB_POOL_SIZE", "5"))
DB_MAX_OVERFLOW = int(os.getenv("HITCRAFT_DB_MAX_OVERFLOW", "10"))
DB_POOL_TIMEOUT = int(os.getenv("HITCRAFT_DB_POOL_TIMEOUT", "30"))  # seconds
DB_POOL_RECYCLE = int(os.getenv("HITCRAFT_DB_POOL_RECYCLE", "1800"))  # seconds (30 minutes)

# TimescaleDB specific settings
# TimescaleDB is an extension for PostgreSQL optimized for time-series data
USE_TIMESCALEDB = os.getenv("HITCRAFT_DB_USE_TIMESCALEDB", "True").lower() in ("true", "1", "t")
TIMESCALE_CHUNK_INTERVAL = os.getenv("HITCRAFT_DB_TIMESCALE_CHUNK_INTERVAL", "1 day")

# Query settings
DB_STATEMENT_TIMEOUT = int(os.getenv("HITCRAFT_DB_STATEMENT_TIMEOUT", "60000"))  # milliseconds (60 seconds)
DB_QUERY_LIMIT = int(os.getenv("HITCRAFT_DB_QUERY_LIMIT", "10000"))  # Default max results for queries

# Migration settings
ALEMBIC_CONFIG = os.getenv("HITCRAFT_DB_ALEMBIC_CONFIG", "alembic.ini")
AUTO_MIGRATE = os.getenv("HITCRAFT_DB_AUTO_MIGRATE", "True").lower() in ("true", "1", "t")
