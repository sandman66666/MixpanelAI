"""
HitCraft AI Analytics Engine - Mixpanel Configuration

This module contains configuration settings for connecting to the Mixpanel API.
Environmental variables can override these settings by using the same name prefixed with 'HITCRAFT_MIXPANEL_'.
"""

import os
from dotenv import load_dotenv

# Load environment variables from .env file if it exists
load_dotenv()

# Mixpanel API Credentials
# These should be set in environment variables for production deployments
MIXPANEL_PROJECT_ID = os.getenv("HITCRAFT_MIXPANEL_PROJECT_ID", "")
MIXPANEL_API_SECRET = os.getenv("HITCRAFT_MIXPANEL_API_SECRET", "")
MIXPANEL_SERVICE_ACCOUNT = os.getenv("HITCRAFT_MIXPANEL_SERVICE_ACCOUNT", "")

# Mixpanel API Endpoints
MIXPANEL_API_URL = "https://mixpanel.com/api"
MIXPANEL_EU_API_URL = "https://eu.mixpanel.com/api"  # For EU data residency

# Use EU endpoint if specified
USE_EU_ENDPOINT = os.getenv("HITCRAFT_MIXPANEL_USE_EU_ENDPOINT", "False").lower() in ("true", "1", "t")
MIXPANEL_BASE_URL = MIXPANEL_EU_API_URL if USE_EU_ENDPOINT else MIXPANEL_API_URL

# API Version
MIXPANEL_API_VERSION = "2.0"

# API Rate Limiting
# Maximum number of requests per second to avoid rate limiting
MIXPANEL_RATE_LIMIT = int(os.getenv("HITCRAFT_MIXPANEL_RATE_LIMIT", "10"))

# Timeouts and retries
MIXPANEL_REQUEST_TIMEOUT = int(os.getenv("HITCRAFT_MIXPANEL_REQUEST_TIMEOUT", "30"))  # seconds
MIXPANEL_MAX_RETRIES = int(os.getenv("HITCRAFT_MIXPANEL_MAX_RETRIES", "3"))
MIXPANEL_RETRY_BACKOFF = int(os.getenv("HITCRAFT_MIXPANEL_RETRY_BACKOFF", "2"))  # seconds

# Data Fetch Settings
# Default time range for data pulls (in days)
MIXPANEL_DEFAULT_DAYS = int(os.getenv("HITCRAFT_MIXPANEL_DEFAULT_DAYS", "30"))

# Event properties to always include in data pulls
MIXPANEL_DEFAULT_PROPERTIES = [
    "distinct_id",
    "time",
    "$browser",
    "$browser_version",
    "$city",
    "$country_code",
    "$device",
    "$os",
    "$referrer",
    "$referring_domain",
    "$screen_height",
    "$screen_width",
    "mp_country_code",
]

# Events to always pull data for
MIXPANEL_DEFAULT_EVENTS = [
    # User account events
    "Sign Up",
    "Login",
    "Logout",
    
    # Content production events
    "Production Started",
    "Production Completed",
    "Sketch Uploaded",
    "Prompt Selected",
    
    # Engagement events
    "Feature Used",
    "Feedback Submitted",
    "Message Liked",
    "Message Disliked",
]

# Define core funnels to analyze
MIXPANEL_CORE_FUNNELS = [
    {
        "name": "Music Production Funnel",
        "steps": [
            "Production Started",
            "Sketch Uploaded",
            "Production Completed"
        ]
    },
    {
        "name": "User Onboarding Funnel",
        "steps": [
            "Sign Up",
            "Profile Completed",
            "First Production Started",
            "First Production Completed"
        ]
    },
    {
        "name": "Lyrics Composition Funnel",
        "steps": [
            "Lyrics Composition Started",
            "Prompt Selected",
            "Lyrics Completed"
        ]
    }
]
