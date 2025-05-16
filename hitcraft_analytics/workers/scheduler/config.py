"""
Task Scheduler Configuration

This module contains configuration settings for the HitCraft Analytics task scheduler.
"""

import os
from datetime import time

# Task execution times (24-hour format)
DAILY_DATA_PULL_TIME = time(hour=4, minute=0)  # 4:00 AM
DAILY_ANALYSIS_TIME = time(hour=4, minute=30)  # 4:30 AM
INSIGHTS_GENERATION_TIME = time(hour=5, minute=0)  # 5:00 AM
DAILY_REPORT_TIME = time(hour=7, minute=0)  # 7:00 AM
WEEKLY_REPORT_TIME = time(hour=8, minute=0)  # 8:00 AM Monday

# Time periods for data collection
DAILY_DATA_DAYS = 7  # Collect last 7 days of data each day
FULL_BACKFILL_DAYS = 90  # Maximum days for full data backfill

# Retry settings
MAX_RETRIES = 3  # Maximum number of retry attempts
RETRY_DELAY_SECONDS = 300  # 5 minutes between retries

# Scheduler settings
SCHEDULER_CHECK_INTERVAL = 60  # Check for tasks every minute
TASK_TIMEOUT_SECONDS = 1800  # 30 minute timeout for tasks

# Logging settings
LOG_LEVEL = os.getenv('HITCRAFT_SCHEDULER_LOG_LEVEL', 'INFO')
LOG_FILE = os.getenv('HITCRAFT_SCHEDULER_LOG_FILE', 'scheduler.log')

# Email notification settings for task failures
ADMIN_EMAIL = os.getenv('HITCRAFT_ADMIN_EMAIL', 'admin@hitcraft.io')
ALERTS_ENABLED = os.getenv('HITCRAFT_ALERTS_ENABLED', 'true').lower() == 'true'
