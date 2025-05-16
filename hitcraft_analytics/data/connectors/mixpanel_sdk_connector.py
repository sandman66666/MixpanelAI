"""
HitCraft AI Analytics Engine - Mixpanel SDK Connector

This module provides a connector to the Mixpanel API using the official Mixpanel Python SDK.
It handles data extraction and analysis from Mixpanel.
"""

import json
import time
import requests
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Union, Any, Tuple

from mixpanel import Mixpanel

from hitcraft_analytics.config.mixpanel_config import (
    MIXPANEL_API_SECRET,
    MIXPANEL_PROJECT_ID,
    MIXPANEL_SERVICE_ACCOUNT,
    MIXPANEL_BASE_URL,
    MIXPANEL_API_VERSION,
    MIXPANEL_REQUEST_TIMEOUT,
    MIXPANEL_MAX_RETRIES,
    MIXPANEL_RETRY_BACKOFF,
    MIXPANEL_RATE_LIMIT,
    MIXPANEL_DEFAULT_EVENTS,
    MIXPANEL_DEFAULT_PROPERTIES,
    MIXPANEL_DEFAULT_DAYS
)
from hitcraft_analytics.utils.logging.logger import get_logger

logger = get_logger(__name__)

class MixpanelSDKConnector:
    """
    Connector to the Mixpanel API using the official Mixpanel Python SDK.
    Handles data extraction and interaction with Mixpanel.
    """
    
    def __init__(self):
        """
        Initialize the Mixpanel SDK connector with API credentials.
        """
        # Use hardcoded credentials to ensure proper connection
        self.api_secret = "9a685163559ec32b97c7d89a4adebafc"
        self.project_id = "3bbc24764765962cb8af4c45ac04ae4d"
        self.api_token = self.project_id  # Mixpanel SDK uses token which is the project ID
        
        # Log the credentials being used
        logger.info(f"Using Mixpanel project token: {self.api_token}")
        logger.info(f"Using Mixpanel API secret: {self.api_secret[:4]}...{self.api_secret[-4:]}")
        
        # Track request times to handle rate limiting
        self.last_request_time = 0
        self.rate_limit = MIXPANEL_RATE_LIMIT
        
        # Initialize Mixpanel SDK with EU endpoints since the project is in the EU region
        self.mixpanel = Mixpanel(self.api_token)
        
        # Set EU API endpoints
        self.api_host = "api-eu.mixpanel.com"
        self.api_url = f"https://{self.api_host}"
        self.data_host = "data-eu.mixpanel.com"
        self.data_url = f"https://{self.data_host}"
        
        logger.info(f"Using Mixpanel EU data centers: {self.api_host} and {self.data_host}")
        
        # Log successful initialization
        logger.info(f"Mixpanel SDK connector initialized for project ID: {self.project_id}")
    
    def _rate_limit_request(self):
        """
        Implement rate limiting to avoid API request limits.
        """
        current_time = time.time()
        elapsed = current_time - self.last_request_time
        
        # If we've made a request too recently, sleep to respect rate limit
        if elapsed < (1.0 / self.rate_limit):
            sleep_time = (1.0 / self.rate_limit) - elapsed
            logger.debug(f"Rate limiting - sleeping for {sleep_time:.2f} seconds")
            time.sleep(sleep_time)
            
        self.last_request_time = time.time()
    
    def get_events(self, 
                  event_names: Optional[List[str]] = None, 
                  from_date: Optional[str] = None,
                  to_date: Optional[str] = None,
                  properties: Optional[List[str]] = None,
                  where: Optional[str] = None,
                  limit_events: Optional[int] = None) -> List[Dict]:
        """
        Get event data from Mixpanel for the specified events and date range.
        
        Args:
            event_names: List of event names to retrieve
            from_date: Start date in format 'YYYY-MM-DD'
            to_date: End date in format 'YYYY-MM-DD'
            properties: Event properties to include
            where: Filter expression for events
            
        Returns:
            List of event data records
        """
        # Use default values if not provided
        if from_date is None:
            from_date = (datetime.now() - timedelta(days=MIXPANEL_DEFAULT_DAYS)).strftime("%Y-%m-%d")
            
        if to_date is None:
            to_date = datetime.now().strftime("%Y-%m-%d")
        
        # Set defaults for event names
        if event_names is None:
            event_names = MIXPANEL_DEFAULT_EVENTS
        
        logger.info(f"Retrieving events from {from_date} to {to_date} using Mixpanel SDK")
        
        try:
            # Need to use the Mixpanel /export endpoint via the API directly as the SDK
            # doesn't have a direct method for bulk data export
            
            # Implement the export API call using requests directly
            import requests
            
            # For authentication with the API
            auth = (self.api_secret, "")
            
            all_events = []
            
            # For all events, don't filter by event_name for efficiency
            self._rate_limit_request()
            
            # Parameters for the export API - use EU endpoint
            url = f"{self.data_url}/api/2.0/export"
            params = {
                "from_date": from_date,
                "to_date": to_date,
            }
            
            # Add event filter if specified
            if event_names and len(event_names) == 1:
                params["event"] = event_names[0]
                logger.info(f"Fetching specific event: {event_names[0]} from {from_date} to {to_date}")
            else:
                logger.info(f"Fetching all events from {from_date} to {to_date}")
            
            # Make the request
            logger.info(f"Making request to: {url} with params: {params}")
            response = requests.get(
                url,
                auth=auth,
                params=params,
                timeout=MIXPANEL_REQUEST_TIMEOUT
            )
            
            # Check for errors
            if response.status_code != 200:
                logger.error(f"Mixpanel API error: {response.status_code} - {response.text}")
                response.raise_for_status()
            
            # Debug response
            logger.info(f"Response status: {response.status_code}")
            if len(response.text) < 100:
                logger.info(f"Full response: {response.text}")
            else:
                logger.info(f"Response preview: {response.text[:100]}...")
            
            # Parse the newline-delimited JSON response
            event_count = 0
            for line in response.text.strip().split("\n"):
                if line:  # Skip empty lines
                    try:
                        event = json.loads(line)
                        
                        # Filter by event_name if needed
                        if event_names and len(event_names) > 1:
                            if event.get('event') in event_names:
                                all_events.append(event)
                                event_count += 1
                        else:
                            all_events.append(event)
                            event_count += 1
                        
                        # Log progress for large datasets
                        if event_count % 1000 == 0:
                            logger.info(f"Processed {event_count} events so far...")
                            
                        # Stop if we've reached the limit
                        if limit_events and event_count >= limit_events:
                            logger.info(f"Reached event limit of {limit_events} - stopping")
                            break
                            
                    except json.JSONDecodeError as e:
                        logger.warning(f"Error parsing JSON response: {str(e)}")
            
            logger.info(f"Retrieved {len(all_events)} events from Mixpanel")
            return all_events
            
        except Exception as e:
            logger.error(f"Failed to retrieve events from Mixpanel: {str(e)}")
            raise
    
    def get_event_names(self, from_date: Optional[str] = None, to_date: Optional[str] = None) -> List[str]:
        """
        Get a list of event names that have been recorded in the project.
        
        Args:
            from_date: Start date in format 'YYYY-MM-DD'
            to_date: End date in format 'YYYY-MM-DD'
                
        Returns:
            List of event names
        """
        if from_date is None:
            from_date = (datetime.now() - timedelta(days=MIXPANEL_DEFAULT_DAYS)).strftime("%Y-%m-%d")
            
        if to_date is None:
            to_date = datetime.now().strftime("%Y-%m-%d")
            
        logger.info(f"Getting event names from {from_date} to {to_date}")
        
        try:
            # The SDK doesn't have a direct method for this, so we use requests
            import requests
            
            # For authentication with the API
            auth = (self.api_secret, "")
            
            # Parameters for the events/names API - use EU endpoint
            url = f"{self.api_url}/api/2.0/events/names"
            params = {
                "from_date": from_date,
                "to_date": to_date,
            }
            
            self._rate_limit_request()
            
            # Make the request
            response = requests.get(
                url,
                auth=auth,
                params=params,
                timeout=MIXPANEL_REQUEST_TIMEOUT
            )
            
            # Check for errors
            response.raise_for_status()
            
            # Parse the JSON response
            event_names = response.json()
            logger.info(f"Retrieved {len(event_names)} event names")
            return event_names
            
        except Exception as e:
            logger.error(f"Failed to get event names: {str(e)}")
            raise
    
    def track_event(self, distinct_id: str, event_name: str, properties: Dict = None, meta: Dict = None):
        """
        Track an event using the Mixpanel SDK.
        
        Args:
            distinct_id: The user's distinct ID
            event_name: Name of the event to track
            properties: Dict of event properties
            meta: Dict of meta properties like IP
        """
        if properties is None:
            properties = {}
            
        if meta is None:
            meta = {}
            
        try:
            # Track the event using the SDK
            self._rate_limit_request()
            self.mixpanel.track(distinct_id, event_name, properties, meta)
            logger.info(f"Tracked event '{event_name}' for user '{distinct_id}'")
            return True
            
        except Exception as e:
            logger.error(f"Failed to track event: {str(e)}")
            raise
    
    def import_event(self, distinct_id: str, event_name: str, timestamp: int, properties: Dict = None):
        """
        Import a historical event using the Mixpanel SDK.
        
        Args:
            distinct_id: The user's distinct ID
            event_name: Name of the event to import
            timestamp: Unix timestamp for the event
            properties: Dict of event properties
        """
        if properties is None:
            properties = {}
            
        try:
            # Import the historical event
            self._rate_limit_request()
            self.mixpanel.import_data(
                api_key='',  # Deprecated but required parameter
                distinct_id=distinct_id,
                event_name=event_name,
                timestamp=timestamp,
                properties=properties,
                api_secret=self.api_secret
            )
            logger.info(f"Imported historical event '{event_name}' for user '{distinct_id}'")
            return True
            
        except Exception as e:
            logger.error(f"Failed to import historical event: {str(e)}")
            raise
    
    def get_funnels(self, 
                 funnel_id: int,
                 from_date: Optional[str] = None,
                 to_date: Optional[str] = None,
                 interval: str = "day") -> Dict:
        """
        Get funnel conversion data from Mixpanel.
        
        Args:
            funnel_id: ID of the funnel to retrieve
            from_date: Start date in format 'YYYY-MM-DD'
            to_date: End date in format 'YYYY-MM-DD'
            interval: Time interval for data (day, week, month)
            
        Returns:
            Dict of funnel conversion data
        """
        if from_date is None:
            from_date = (datetime.now() - timedelta(days=MIXPANEL_DEFAULT_DAYS)).strftime("%Y-%m-%d")
            
        if to_date is None:
            to_date = datetime.now().strftime("%Y-%m-%d")
            
        logger.info(f"Getting funnel data for funnel ID {funnel_id} from {from_date} to {to_date}")
        
        try:
            # The SDK doesn't have a direct method for this, so we use requests
            import requests
            
            # For authentication with the API
            auth = (self.api_secret, "")
            
            # Parameters for the funnels API
            url = "https://mixpanel.com/api/2.0/funnels"
            params = {
                "funnel_id": funnel_id,
                "from_date": from_date,
                "to_date": to_date,
                "interval": interval
            }
            
            self._rate_limit_request()
            
            # Make the request
            response = requests.get(
                url,
                auth=auth,
                params=params,
                timeout=MIXPANEL_REQUEST_TIMEOUT
            )
            
            # Check for errors
            response.raise_for_status()
            
            # Parse the JSON response
            funnel_data = response.json()
            logger.info(f"Retrieved funnel data for funnel ID {funnel_id}")
            return funnel_data
            
        except Exception as e:
            logger.error(f"Failed to get funnel data: {str(e)}")
            raise
