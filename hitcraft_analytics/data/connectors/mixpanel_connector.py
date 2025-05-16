"""
HitCraft AI Analytics Engine - Mixpanel Connector

This module handles communication with the Mixpanel API, including authentication,
data extraction, and error handling for all Mixpanel-related operations.
"""

import base64
import json
import time
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Union, Any, Tuple

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

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

class MixpanelConnector:
    """
    Handles interaction with the Mixpanel API for data extraction.
    """
    
    def __init__(self):
        """
        Initialize the Mixpanel connector with API credentials and session configuration.
        """
        self.api_secret = MIXPANEL_API_SECRET
        self.project_id = MIXPANEL_PROJECT_ID
        self.service_account = MIXPANEL_SERVICE_ACCOUNT
        self.base_url = MIXPANEL_BASE_URL
        self.api_version = MIXPANEL_API_VERSION
        
        # Initialize session with retry logic
        self.session = self._create_session()
        
        # Track request times to handle rate limiting
        self.last_request_time = 0
        self.rate_limit = MIXPANEL_RATE_LIMIT
        
        # Validate required credentials
        self._validate_credentials()
        
        logger.info("Mixpanel connector initialized for project ID: %s", self.project_id)

    def _validate_credentials(self) -> None:
        """
        Validate that required Mixpanel credentials are provided.
        
        Raises:
            ValueError: If required credentials are missing.
        """
        if not self.api_secret:
            raise ValueError("Mixpanel API Secret is required")
    
    def _create_session(self) -> requests.Session:
        """
        Create a requests session with retry logic for handling transient failures.
        
        Returns:
            requests.Session: Configured session object.
        """
        session = requests.Session()
        
        # Configure retry strategy
        retry_strategy = Retry(
            total=MIXPANEL_MAX_RETRIES,
            backoff_factor=MIXPANEL_RETRY_BACKOFF,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET", "POST"]
        )
        
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        
        return session
    
    def _get_auth_headers(self) -> Dict[str, str]:
        """
        Generate authentication headers for Mixpanel API requests.
        
        Returns:
            Dict[str, str]: Dictionary of headers to include with requests.
        """
        headers = {
            "Accept": "application/json"
        }
        
        return headers
        
    def _get_auth(self) -> Tuple[str, str]:
        """
        Get authentication tuple for requests that use basic auth.
        
        Returns:
            Tuple[str, str]: (username, password) tuple for basic auth.
        """
        return (self.api_secret, "")
    
    def _get_api_url(self, endpoint: str) -> str:
        """
        Build the full API URL for a given endpoint.
        
        Args:
            endpoint (str): API endpoint path.
            
        Returns:
            str: Full API URL.
        """
        return f"{self.base_url}/{self.api_version}/{endpoint}"
    
    def _rate_limit_request(self) -> None:
        """
        Implement rate limiting to avoid API request limits.
        """
        current_time = time.time()
        elapsed = current_time - self.last_request_time
        
        # If less than 1/rate_limit seconds have passed, wait
        if elapsed < (1.0 / self.rate_limit):
            sleep_time = (1.0 / self.rate_limit) - elapsed
            time.sleep(sleep_time)
            
        self.last_request_time = time.time()
    
    def _make_request(self, method: str, endpoint: str, params: Optional[Dict] = None, 
                    data: Optional[Dict] = None, use_data_api: bool = False) -> Dict:
        """
        Make an API request to Mixpanel with proper error handling.
        
        Args:
            method (str): HTTP method (e.g., "GET", "POST").
            endpoint (str): API endpoint path.
            params (Optional[Dict]): URL parameters for the request.
            data (Optional[Dict]): Request body for POST requests.
            
        Returns:
            Dict: Response data from Mixpanel.
            
        Raises:
            requests.RequestException: If the request fails.
        """
        url = self._get_api_url(endpoint)
        headers = self._get_auth_headers()
        
        # Apply rate limiting
        self._rate_limit_request()
        
        try:
            logger.debug("Making %s request to Mixpanel endpoint: %s", method, endpoint)
            
            # For data export API, use basic auth
            if use_data_api:
                auth = self._get_auth()
                
                if method.upper() == "GET":
                    response = self.session.get(
                        url, 
                        auth=auth,
                        params=params,
                        timeout=MIXPANEL_REQUEST_TIMEOUT
                    )
                else:  # POST
                    headers["Content-Type"] = "application/json"
                    response = self.session.post(
                        url, 
                        auth=auth,
                        params=params,
                        json=data,
                        timeout=MIXPANEL_REQUEST_TIMEOUT
                    )
            else:
                # For regular API, use headers
                if method.upper() == "GET":
                    response = self.session.get(
                        url, 
                        headers=headers, 
                        params=params,
                        timeout=MIXPANEL_REQUEST_TIMEOUT
                    )
                else:  # POST
                    headers["Content-Type"] = "application/json"
                    response = self.session.post(
                        url, 
                        headers=headers, 
                        params=params,
                        json=data,
                        timeout=MIXPANEL_REQUEST_TIMEOUT
                    )
            
            response.raise_for_status()
            return response.json()
            
        except requests.RequestException as e:
            logger.error("Mixpanel API request failed: %s", str(e))
            if hasattr(e.response, 'text'):
                logger.error("Response: %s", e.response.text)
            raise
    
    def get_events(self, 
                event_names: Optional[List[str]] = None, 
                from_date: Optional[str] = None,
                to_date: Optional[str] = None,
                properties: Optional[List[str]] = None,
                where: Optional[str] = None) -> List[Dict]:
        """
        Get event data from Mixpanel for the specified events and date range.
        
        Args:
            event_names (Optional[List[str]]): List of event names to retrieve.
                Defaults to MIXPANEL_DEFAULT_EVENTS if not provided.
            from_date (Optional[str]): Start date in format 'YYYY-MM-DD'.
                Defaults to MIXPANEL_DEFAULT_DAYS ago if not provided.
            to_date (Optional[str]): End date in format 'YYYY-MM-DD'.
                Defaults to today if not provided.
            properties (Optional[List[str]]): Event properties to include.
                Defaults to MIXPANEL_DEFAULT_PROPERTIES if not provided.
            where (Optional[str]): Filter expression for events.
                
        Returns:
            List[Dict]: List of event data records.
        """
        # Use default values if not provided
        if from_date is None:
            from_date = (datetime.now() - timedelta(days=MIXPANEL_DEFAULT_DAYS)).strftime("%Y-%m-%d")
            
        if to_date is None:
            to_date = datetime.now().strftime("%Y-%m-%d")
        
        # Prepare parameters
        params = {
            "from_date": from_date,
            "to_date": to_date,
        }
        
        # Add event names as comma-separated string if provided
        if event_names:
            params["event"] = ",".join(event_names)
            
        logger.info("Retrieving events from %s to %s", from_date, to_date)
        
        try:
            # Use the data API with basic auth
            url = "https://data.mixpanel.com/api/2.0/export/"
            
            # Create a direct request rather than using _make_request
            # because the response format is different (newline-delimited JSON)
            auth = self._get_auth()
            response = self.session.get(
                url,
                auth=auth,
                params=params,
                timeout=MIXPANEL_REQUEST_TIMEOUT
            )
            response.raise_for_status()
            
            # Parse the newline-delimited JSON response
            events = []
            for line in response.text.strip().split("\n"):
                if line:  # Skip empty lines
                    try:
                        events.append(json.loads(line))
                    except json.JSONDecodeError as e:
                        logger.warning("Error parsing JSON response: %s", str(e))
                        continue
            
            logger.info("Retrieved %d events from Mixpanel", len(events))
            return events
        except Exception as e:
            logger.error("Failed to retrieve events: %s", str(e))
            raise

    def get_event_names(self, from_date: Optional[str] = None, to_date: Optional[str] = None) -> List[str]:
        """
        Get a list of event names that have been recorded in the project.
        
        Args:
            from_date (Optional[str]): Start date in format 'YYYY-MM-DD'.
                Defaults to 30 days ago if not provided.
            to_date (Optional[str]): End date in format 'YYYY-MM-DD'.
                Defaults to today if not provided.
                
        Returns:
            List[str]: List of event names.
        """
        if from_date is None:
            from_date = (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d")
            
        if to_date is None:
            to_date = datetime.now().strftime("%Y-%m-%d")
            
        params = {
            "from_date": from_date,
            "to_date": to_date
        }
        
        try:
            # Use the data API with basic auth
            url = "https://mixpanel.com/api/2.0/events/names"
            auth = self._get_auth()
            
            response = self.session.get(
                url,
                auth=auth,
                params=params,
                timeout=MIXPANEL_REQUEST_TIMEOUT
            )
            response.raise_for_status()
            
            return response.json()
        except Exception as e:
            logger.error("Failed to get event names: %s", str(e))
            return []

    def get_engagement(self, 
                    from_date: Optional[str] = None,
                    to_date: Optional[str] = None,
                    interval: str = "day") -> List[Dict]:
        """
        Get engagement metrics from Mixpanel.
        
        Args:
            from_date (Optional[str]): Start date in format 'YYYY-MM-DD'.
                Defaults to MIXPANEL_DEFAULT_DAYS ago if not provided.
            to_date (Optional[str]): End date in format 'YYYY-MM-DD'.
                Defaults to today if not provided.
            interval (str): Time interval for data bucketing (day, week, month).
                
        Returns:
            List[Dict]: Engagement metrics by time interval.
        """
        if from_date is None:
            from_date = (datetime.now() - timedelta(days=MIXPANEL_DEFAULT_DAYS)).strftime("%Y-%m-%d")
            
        if to_date is None:
            to_date = datetime.now().strftime("%Y-%m-%d")
        
        # Prepare parameters
        params = {
            "from_date": from_date,
            "to_date": to_date,
            "interval": interval,
        }
        
        logger.info("Retrieving engagement metrics from %s to %s with interval %s",
                   from_date, to_date, interval)
        
        try:
            url = "https://mixpanel.com/api/2.0/insights/engagement"
            auth = self._get_auth()
            
            response = self.session.get(
                url,
                auth=auth,
                params=params,
                timeout=MIXPANEL_REQUEST_TIMEOUT
            )
            response.raise_for_status()
            
            return response.json()
        except Exception as e:
            logger.error("Failed to retrieve engagement metrics: %s", str(e))
            raise

    def get_funnels(self, 
                 funnel_id: int,
                 from_date: Optional[str] = None,
                 to_date: Optional[str] = None,
                 interval: str = "day") -> Dict:
        """
        Get funnel conversion data from Mixpanel.
        
        Args:
            funnel_id (int): ID of the funnel to retrieve.
            from_date (Optional[str]): Start date in format 'YYYY-MM-DD'.
                Defaults to MIXPANEL_DEFAULT_DAYS ago if not provided.
            to_date (Optional[str]): End date in format 'YYYY-MM-DD'.
                Defaults to today if not provided.
            interval (str): Time interval for data bucketing (day, week, month).
                
        Returns:
            Dict: Funnel conversion data.
        """
        if from_date is None:
            from_date = (datetime.now() - timedelta(days=MIXPANEL_DEFAULT_DAYS)).strftime("%Y-%m-%d")
            
        if to_date is None:
            to_date = datetime.now().strftime("%Y-%m-%d")
        
        # Prepare parameters
        params = {
            "funnel_id": funnel_id,
            "from_date": from_date,
            "to_date": to_date,
            "interval": interval,
        }
        
        logger.info("Retrieving funnel ID %s data from %s to %s with interval %s",
                   funnel_id, from_date, to_date, interval)
        
        try:
            url = "https://mixpanel.com/api/2.0/insights/funnels"
            auth = self._get_auth()
            
            response = self.session.get(
                url,
                auth=auth,
                params=params,
                timeout=MIXPANEL_REQUEST_TIMEOUT
            )
            response.raise_for_status()
            
            return response.json()
        except Exception as e:
            logger.error("Failed to retrieve funnel data: %s", str(e))
            raise
    
    def get_retention(self,
                    from_date: Optional[str] = None,
                    to_date: Optional[str] = None,
                    retention_type: str = "retention",
                    born_event: Optional[str] = None,
                    born_where: Optional[str] = None,
                    return_event: Optional[str] = None,
                    return_where: Optional[str] = None,
                    interval: str = "day") -> Dict:
        """
        Get user retention data from Mixpanel.
        
        Args:
            from_date (Optional[str]): Start date in format 'YYYY-MM-DD'.
                Defaults to MIXPANEL_DEFAULT_DAYS ago if not provided.
            to_date (Optional[str]): End date in format 'YYYY-MM-DD'.
                Defaults to today if not provided.
            retention_type (str): Type of retention analysis (retention, addiction).
            born_event (Optional[str]): Initial event for cohort definition.
            born_where (Optional[str]): Filter for initial event.
            return_event (Optional[str]): Return event to measure.
            return_where (Optional[str]): Filter for return event.
            interval (str): Time interval for data bucketing (day, week, month).
                
        Returns:
            Dict: Retention analysis data.
        """
        if from_date is None:
            from_date = (datetime.now() - timedelta(days=MIXPANEL_DEFAULT_DAYS)).strftime("%Y-%m-%d")
            
        if to_date is None:
            to_date = datetime.now().strftime("%Y-%m-%d")
        
        # Prepare parameters
        params = {
            "from_date": from_date,
            "to_date": to_date,
            "retention_type": retention_type,
            "interval": interval,
        }
        
        # Add optional parameters if provided
        if born_event:
            params["born_event"] = born_event
            
        if born_where:
            params["born_where"] = born_where
            
        if return_event:
            params["return_event"] = return_event
            
        if return_where:
            params["return_where"] = return_where
        
        logger.info("Retrieving retention data from %s to %s with interval %s",
                   from_date, to_date, interval)
        
        try:
            url = "https://mixpanel.com/api/2.0/insights/retention"
            auth = self._get_auth()
            
            response = self.session.get(
                url,
                auth=auth,
                params=params,
                timeout=MIXPANEL_REQUEST_TIMEOUT
            )
            response.raise_for_status()
            
            return response.json()
        except Exception as e:
            logger.error("Failed to retrieve retention data: %s", str(e))
            raise
    
    def run_jql_query(self, script: str) -> Any:
        """
        Run a JQL (JavaScript Query Language) query against Mixpanel data.
        
        Args:
            script (str): JQL script to execute.
                
        Returns:
            Any: Result of the JQL query.
        """
        # Prepare the query data
        data = {
            "script": script
        }
        
        logger.info("Running JQL query")
        logger.debug("JQL script: %s", script)
        
        try:
            url = "https://mixpanel.com/api/2.0/jql"
            auth = self._get_auth()
            
            # Configure headers for POST request
            headers = {
                "Accept": "application/json",
                "Content-Type": "application/json"
            }
            
            response = self.session.post(
                url,
                auth=auth,
                json=data,
                headers=headers,
                timeout=MIXPANEL_REQUEST_TIMEOUT
            )
            response.raise_for_status()
            
            return response.json()
        except Exception as e:
            logger.error("Failed to execute JQL query: %s", str(e))
            raise
