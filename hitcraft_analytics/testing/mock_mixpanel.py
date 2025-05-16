"""
HitCraft AI Analytics Engine - Mock Mixpanel Connector

This module provides a mock implementation of the MixpanelConnector for testing purposes.
It returns sample event data that matches the expected format from the real Mixpanel API.
"""

import json
import uuid
from typing import List, Dict, Optional, Any
from datetime import datetime, timedelta
import random

from hitcraft_analytics.data.connectors.mixpanel_connector import MixpanelConnector
from hitcraft_analytics.utils.logging.logger import get_logger

logger = get_logger(__name__)

class MockMixpanelConnector(MixpanelConnector):
    """
    Mock implementation of MixpanelConnector for testing.
    
    Provides sample data matching the format from the real Mixpanel API,
    allowing tests to run without actual API access.
    """
    
    def __init__(self, *args, **kwargs):
        """Initialize the mock connector."""
        super().__init__(*args, **kwargs)
        logger.info("Mock Mixpanel connector initialized - will return sample data")
        self.project_id = "sample_project_12345"
        
    def get_events(self, 
                  event_names: Optional[List[str]] = None, 
                  from_date: Optional[str] = None,
                  to_date: Optional[str] = None,
                  properties: Optional[List[str]] = None,
                  where: Optional[str] = None) -> List[Dict]:
        """
        Get mock event data matching the Mixpanel API format.
        
        Args:
            event_names: List of event names to filter by
            from_date: Start date in format 'YYYY-MM-DD'
            to_date: End date in format 'YYYY-MM-DD'
            properties: List of properties to include
            where: Filter expression
            
        Returns:
            List of mock event data dictionaries
        """
        logger.info(f"Generating mock events from {from_date} to {to_date}")
        
        # Parse dates
        if from_date:
            start_date = datetime.fromisoformat(from_date)
        else:
            start_date = datetime.now() - timedelta(days=30)
            
        if to_date:
            end_date = datetime.fromisoformat(to_date)
        else:
            end_date = datetime.now()
            
        # Default event names if not provided
        if not event_names:
            event_names = [
                "app_open", 
                "view_content", 
                "add_to_cart", 
                "purchase", 
                "sign_up",
                "create_new_project",
                "save_project",
                "share_project"
            ]
            
        # Generate random events
        events = []
        days = (end_date - start_date).days + 1
        
        # Generate 5-10 users for variety
        user_ids = [f"mock_user_{i}" for i in range(1, random.randint(5, 10))]
        
        # For each day in the range
        for day in range(days):
            current_date = start_date + timedelta(days=day)
            
            # Each user does 2-5 events per day
            for user_id in user_ids:
                # Each user always opens the app
                events.append(self._generate_event(
                    "app_open", 
                    user_id, 
                    current_date + timedelta(hours=random.randint(8, 10))
                ))
                
                # Randomly choose 1-4 more events
                additional_events = random.sample(
                    [e for e in event_names if e != "app_open"], 
                    k=random.randint(1, min(4, len(event_names)-1))
                )
                
                # Add those events later in the day
                for i, event_name in enumerate(additional_events):
                    event_time = current_date + timedelta(
                        hours=random.randint(10, 18),
                        minutes=random.randint(0, 59)
                    )
                    events.append(self._generate_event(event_name, user_id, event_time))
        
        # If specific event names were requested, filter the results
        if event_names:
            events = [e for e in events if e.get("event") in event_names]
            
        logger.info(f"Generated {len(events)} mock events")
        return events
        
    def _generate_event(self, event_name: str, user_id: str, timestamp: datetime) -> Dict[str, Any]:
        """
        Generate a single mock event.
        
        Args:
            event_name: Name of the event
            user_id: User identifier
            timestamp: Event timestamp
            
        Returns:
            Dict containing the event data
        """
        # Base event data
        event = {
            "event": event_name,
            "properties": {
                "time": int(timestamp.timestamp()),
                "distinct_id": user_id,
                "$browser": "Chrome",
                "$browser_version": "88.0",
                "$city": "San Francisco",
                "$country_code": "US",
                "$device": "Desktop",
                "$os": "macOS",
                "$referrer": "https://www.google.com/",
                "$referring_domain": "www.google.com",
                "$screen_height": 1080,
                "$screen_width": 1920,
                "mp_country_code": "US"
            }
        }
        
        # ISO format time for easier processing
        event["time"] = timestamp.isoformat()
        
        # Add event-specific properties
        if event_name == "app_open":
            event["properties"]["session_id"] = str(uuid.uuid4())
            event["properties"]["version"] = "1.0.0"
        elif event_name == "view_content":
            event["properties"]["content_id"] = str(uuid.uuid4())
            event["properties"]["category"] = random.choice(["music", "lyrics", "chord", "beat"])
            event["properties"]["duration"] = random.randint(10, 300)
        elif event_name == "add_to_cart":
            event["properties"]["item_id"] = str(uuid.uuid4())
            event["properties"]["item_name"] = f"Product {random.randint(1, 100)}"
            event["properties"]["price"] = round(random.uniform(4.99, 99.99), 2)
        elif event_name == "purchase":
            event["properties"]["order_id"] = str(uuid.uuid4())
            event["properties"]["total"] = round(random.uniform(9.99, 199.99), 2)
            event["properties"]["items"] = random.randint(1, 5)
        elif "project" in event_name:
            event["properties"]["project_id"] = str(uuid.uuid4())
            event["properties"]["project_type"] = random.choice(["song", "lyrics", "chord", "beat"])
            event["properties"]["duration"] = random.randint(60, 240)
            
        return event
        
    def get_event_names(self, from_date: Optional[str] = None, to_date: Optional[str] = None) -> List[str]:
        """Get mock event names."""
        return [
            "app_open", 
            "view_content", 
            "add_to_cart", 
            "purchase", 
            "sign_up",
            "create_new_project",
            "save_project",
            "share_project"
        ]
