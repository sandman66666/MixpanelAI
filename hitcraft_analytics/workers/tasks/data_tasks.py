"""
Data Collection Tasks

This module implements concrete tasks for data collection from Mixpanel.
"""

import logging
import os
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional

from hitcraft_analytics.data.connectors.mixpanel_connector import MixpanelConnector
from hitcraft_analytics.data.repositories.events_repository import EventsRepository
from hitcraft_analytics.workers.scheduler.tasks import DataPullTask
from hitcraft_analytics.workers.scheduler.config import DAILY_DATA_DAYS, FULL_BACKFILL_DAYS
from hitcraft_analytics.utils.logging_config import setup_logger

# Set up logger
logger = setup_logger("workers.tasks.data")

class MixpanelDataPullTask(DataPullTask):
    """
    Task for pulling event data from Mixpanel.
    
    This task is responsible for retrieving events from Mixpanel within a specified
    date range and storing them in the local database.
    """
    
    def __init__(self, event_types: Optional[List[str]] = None):
        """
        Initialize the Mixpanel data pull task.
        
        Args:
            event_types: Specific event types to pull, or None for all events
        """
        task_id = "mixpanel_data_pull"
        description = "Pull event data from Mixpanel API"
        super().__init__(task_id, description)
        
        self.event_types = event_types
        self.connector = MixpanelConnector()
        self.repository = EventsRepository()
    
    def run(self, 
            from_date: Optional[str] = None, 
            to_date: Optional[str] = None, 
            days_back: int = DAILY_DATA_DAYS
           ) -> Dict[str, Any]:
        """
        Execute the Mixpanel data pull task.
        
        Args:
            from_date: Start date in YYYY-MM-DD format
            to_date: End date in YYYY-MM-DD format
            days_back: Number of days to look back if from_date not specified
            
        Returns:
            Dict[str, Any]: Results of the data pull operation
        """
        # Determine date range
        if not from_date or not to_date:
            calculated_from, calculated_to = self.extract_date_range(days_back)
            from_date = from_date or calculated_from
            to_date = to_date or calculated_to
        
        logger.info(f"Pulling Mixpanel data from {from_date} to {to_date}")
        
        # Pull events from Mixpanel
        events = self.connector.get_events(
            from_date=from_date,
            to_date=to_date,
            event_names=self.event_types
        )
        
        if not events:
            logger.warning(f"No events retrieved from Mixpanel for period {from_date} to {to_date}")
            return {
                "events_count": 0,
                "from_date": from_date,
                "to_date": to_date,
                "status": "completed_no_data"
            }
        
        # Store events in the repository
        stored_count = self.repository.store_events(events)
        
        logger.info(f"Successfully pulled and stored {stored_count} events from Mixpanel")
        
        return {
            "events_count": len(events),
            "stored_count": stored_count,
            "from_date": from_date,
            "to_date": to_date,
            "status": "completed_success"
        }


class UserProfileDataPullTask(DataPullTask):
    """
    Task for pulling user profile data from Mixpanel.
    
    This task retrieves user profile data from Mixpanel's Engage API
    and stores it in the local database.
    """
    
    def __init__(self):
        task_id = "mixpanel_user_profiles_pull"
        description = "Pull user profile data from Mixpanel Engage API"
        super().__init__(task_id, description)
        
        self.connector = MixpanelConnector()
        self.repository = EventsRepository()
    
    def run(self) -> Dict[str, Any]:
        """
        Execute the user profile data pull task.
        
        Returns:
            Dict[str, Any]: Results of the profile data pull operation
        """
        logger.info("Pulling user profile data from Mixpanel")
        
        # Pull user profiles from Mixpanel
        try:
            profiles = self.connector.get_user_profiles()
            
            if not profiles:
                logger.warning("No user profiles retrieved from Mixpanel")
                return {
                    "profiles_count": 0,
                    "status": "completed_no_data"
                }
            
            # Store profiles in the repository
            stored_count = self.repository.store_user_profiles(profiles)
            
            logger.info(f"Successfully pulled and stored {stored_count} user profiles from Mixpanel")
            
            return {
                "profiles_count": len(profiles),
                "stored_count": stored_count,
                "status": "completed_success"
            }
            
        except Exception as e:
            logger.error(f"Error pulling user profiles: {str(e)}", exc_info=True)
            raise


class FullDataBackfillTask(DataPullTask):
    """
    Task for performing a full historical data backfill from Mixpanel.
    
    This task retrieves all available historical data within the configured
    time window and stores it in the local database.
    """
    
    def __init__(self):
        task_id = "mixpanel_full_backfill"
        description = "Full historical data backfill from Mixpanel"
        super().__init__(task_id, description)
        
        self.connector = MixpanelConnector()
        self.repository = EventsRepository()
        
        # This task can take longer
        self.retry_delay = 1800  # 30 minutes between retries
    
    def run(self, days_back: int = FULL_BACKFILL_DAYS) -> Dict[str, Any]:
        """
        Execute the full data backfill task.
        
        Args:
            days_back: Maximum number of days to look back
            
        Returns:
            Dict[str, Any]: Results of the backfill operation
        """
        to_date = datetime.now().date().isoformat()
        from_date = (datetime.now() - timedelta(days=days_back)).date().isoformat()
        
        logger.info(f"Starting full data backfill from {from_date} to {to_date}")
        
        # Track progress for the backfill
        total_events = 0
        total_days = days_back
        days_processed = 0
        
        current_date = datetime.strptime(from_date, "%Y-%m-%d")
        end_date = datetime.strptime(to_date, "%Y-%m-%d")
        
        # Process data in smaller chunks (7 days at a time)
        chunk_size = 7  # days
        
        while current_date < end_date:
            chunk_end = min(current_date + timedelta(days=chunk_size), end_date)
            
            chunk_from = current_date.date().isoformat()
            chunk_to = chunk_end.date().isoformat()
            
            logger.info(f"Processing backfill chunk from {chunk_from} to {chunk_to}")
            
            try:
                # Pull events for this chunk
                events = self.connector.get_events(
                    from_date=chunk_from,
                    to_date=chunk_to
                )
                
                if events:
                    # Store events
                    stored_count = self.repository.store_events(events)
                    total_events += stored_count
                    
                    logger.info(f"Stored {stored_count} events for period {chunk_from} to {chunk_to}")
                
                # Update progress
                days_in_chunk = (chunk_end - current_date).days
                days_processed += days_in_chunk
                
                # Move to next chunk
                current_date = chunk_end
                
            except Exception as e:
                logger.error(f"Error during backfill for period {chunk_from} to {chunk_to}: {str(e)}", exc_info=True)
                # Continue with next chunk despite errors
                current_date = chunk_end
        
        logger.info(f"Completed full data backfill, processed {days_processed} days, stored {total_events} events")
        
        return {
            "total_events": total_events,
            "days_processed": days_processed,
            "from_date": from_date,
            "to_date": to_date,
            "status": "completed_success"
        }
