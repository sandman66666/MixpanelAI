"""
HitCraft AI Analytics Engine - Events Repository

This module provides a repository for managing event data from Mixpanel,
including fetching, transforming, and storing events in the database.
"""

from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Union, Tuple

import pandas as pd

from hitcraft_analytics.data.connectors.mixpanel_connector import MixpanelConnector
from hitcraft_analytics.data.connectors.database_connector import DatabaseConnector
from hitcraft_analytics.data.processing.data_transformation import DataTransformer
from hitcraft_analytics.data.schemas.event_schemas import Event, UserProfile, UserSession, EventSequence
from hitcraft_analytics.core.analysis.funnel_analysis import FunnelAnalyzer
from hitcraft_analytics.utils.logging.logger import get_logger

logger = get_logger(__name__)

class EventsRepository:
    """
    Repository for managing event data from Mixpanel.
    """
    
    def __init__(self, 
                mixpanel_connector: Optional[MixpanelConnector] = None,
                db_connector: Optional[DatabaseConnector] = None,
                data_transformer: Optional[DataTransformer] = None,
                funnel_analyzer: Optional[FunnelAnalyzer] = None):
        """
        Initialize the events repository.
        
        Args:
            mixpanel_connector (Optional[MixpanelConnector]): Mixpanel connector instance.
                If None, a new instance will be created.
            db_connector (Optional[DatabaseConnector]): Database connector instance.
                If None, a new instance will be created.
            data_transformer (Optional[DataTransformer]): Data transformer instance.
                If None, a new instance will be created.
            funnel_analyzer (Optional[FunnelAnalyzer]): Funnel analyzer instance.
                If None, a new instance will be created.
        """
        self.mixpanel = mixpanel_connector or MixpanelConnector()
        self.db = db_connector or DatabaseConnector()
        self.transformer = data_transformer or DataTransformer()
        self.funnel_analyzer = funnel_analyzer or FunnelAnalyzer()
        
        logger.info("Events repository initialized")
    
    def fetch_and_store_events(self, 
                             event_names: Optional[List[str]] = None,
                             from_date: Optional[str] = None,
                             to_date: Optional[str] = None,
                             properties: Optional[List[str]] = None,
                             where: Optional[str] = None) -> Tuple[int, int]:
        """
        Fetch events from Mixpanel and store them in the database.
        
        Args:
            event_names (Optional[List[str]]): List of event names to retrieve.
            from_date (Optional[str]): Start date in format 'YYYY-MM-DD'.
            to_date (Optional[str]): End date in format 'YYYY-MM-DD'.
            properties (Optional[List[str]]): Event properties to include.
            where (Optional[str]): Filter expression for events.
            
        Returns:
            Tuple[int, int]: (number of events fetched, number of events stored)
        """
        logger.info("Fetching and storing events from Mixpanel")
        
        try:
            # Fetch events from Mixpanel
            raw_events = self.mixpanel.get_events(
                event_names=event_names,
                from_date=from_date,
                to_date=to_date,
                properties=properties,
                where=where
            )
            
            logger.info("Fetched %d events from Mixpanel", len(raw_events))
            
            if not raw_events:
                logger.warning("No events fetched from Mixpanel")
                return 0, 0
            
            # Transform events to DataFrame
            events_df = self.transformer.transform_events(raw_events)
            
            if events_df.empty:
                logger.warning("No events after transformation")
                return len(raw_events), 0
            
            # Prepare events for storage
            event_records = self.transformer.prepare_data_for_storage(events_df, "events")
            
            # Store events in database
            stored_count = self._store_events(event_records)
            
            logger.info("Stored %d events in the database", stored_count)
            
            # Update user profiles with event data
            self._update_user_profiles_from_events(events_df)
            
            # Process sessions
            self._process_sessions_from_events(events_df)
            
            return len(raw_events), stored_count
            
        except Exception as e:
            logger.error("Failed to fetch and store events: %s", str(e))
            raise
    
    def _store_events(self, event_records: List[Dict[str, Any]]) -> int:
        """
        Store event records in the database.
        
        Args:
            event_records (List[Dict[str, Any]]): Event records to store.
            
        Returns:
            int: Number of events stored.
        """
        if not event_records:
            return 0
            
        try:
            # Insert events into the database
            return self.db.bulk_insert("events", event_records)
            
        except Exception as e:
            logger.error("Failed to store events: %s", str(e))
            raise
    
    def _update_user_profiles_from_events(self, events_df: pd.DataFrame) -> int:
        """
        Update user profiles based on event data.
        
        Args:
            events_df (pd.DataFrame): DataFrame with event data.
            
        Returns:
            int: Number of user profiles updated.
        """
        if events_df.empty:
            return 0
            
        logger.info("Updating user profiles from events")
        
        try:
            # Group events by user
            user_stats = self.transformer.extract_features(events_df)
            
            if user_stats.empty:
                return 0
                
            # Prepare user profile updates
            updates = []
            
            for _, row in user_stats.iterrows():
                distinct_id = row["distinct_id"]
                
                # Prepare user profile data
                profile_data = {
                    "distinct_id": distinct_id,
                    "last_seen": row.get("time_max"),
                    "event_count": row.get("event_count", 0),
                    "days_active": row.get("days_active", 0),
                    "updated_at": datetime.utcnow()
                }
                
                # If this is a new user, add first_seen
                if "time_min" in row:
                    profile_data["first_seen"] = row["time_min"]
                
                # Check if user already exists
                existing_user = self.get_user_profile(distinct_id)
                
                if existing_user:
                    # Update existing user
                    for key, value in profile_data.items():
                        if key in ["event_count", "days_active"]:
                            # Increment existing values
                            if key in existing_user and existing_user[key]:
                                profile_data[key] = existing_user[key] + value
                    
                    # Upsert user profile
                    self.db.upsert(
                        table_name="user_profiles",
                        data=profile_data,
                        conflict_columns=["distinct_id"],
                    )
                else:
                    # Insert new user
                    self.db.bulk_insert("user_profiles", [profile_data])
                
                updates.append(distinct_id)
            
            logger.info("Updated %d user profiles", len(updates))
            return len(updates)
            
        except Exception as e:
            logger.error("Failed to update user profiles: %s", str(e))
            raise
    
    def _process_sessions_from_events(self, events_df: pd.DataFrame, 
                                    session_timeout_minutes: int = 30) -> int:
        """
        Process user sessions from event data.
        
        Args:
            events_df (pd.DataFrame): DataFrame with event data.
            session_timeout_minutes (int): Session timeout in minutes.
            
        Returns:
            int: Number of sessions processed.
        """
        if events_df.empty:
            return 0
            
        logger.info("Processing sessions from events")
        
        try:
            # Sort events by user and time
            if "distinct_id" not in events_df.columns or "time" not in events_df.columns:
                logger.warning("Events DataFrame missing required columns for session processing")
                return 0
                
            # Make a copy to avoid modifying the original
            df = events_df.copy()
            
            # Ensure time column is datetime
            if df["time"].dtype != 'datetime64[ns]':
                df["time"] = pd.to_datetime(df["time"])
                
            # Sort by user and time
            df = df.sort_values(["distinct_id", "time"])
            
            # Initialize session tracking
            session_data = []
            current_sessions = {}
            
            # Process each event
            for _, event in df.iterrows():
                user_id = event["distinct_id"]
                event_time = event["time"]
                event_name = event.get("event", "unknown")
                
                # Check if user has an active session
                if user_id in current_sessions:
                    last_time = current_sessions[user_id]["last_time"]
                    
                    # Check if session has timed out
                    if (event_time - last_time).total_seconds() / 60 > session_timeout_minutes:
                        # Close current session
                        session = current_sessions[user_id]
                        session["end_time"] = last_time
                        session["duration_seconds"] = (last_time - session["start_time"]).total_seconds()
                        session_data.append(session)
                        
                        # Start a new session
                        current_sessions[user_id] = {
                            "session_id": f"{user_id}_{event_time.timestamp()}",
                            "distinct_id": user_id,
                            "start_time": event_time,
                            "last_time": event_time,
                            "event_count": 1,
                            "browser": event.get("$browser"),
                            "os": event.get("$os"),
                            "device_type": event.get("$device"),
                            "referrer": event.get("$referrer"),
                            "properties": {}
                        }
                    else:
                        # Update current session
                        current_sessions[user_id]["last_time"] = event_time
                        current_sessions[user_id]["event_count"] += 1
                        
                        # Track specific events
                        if event_name == "Page View":
                            current_sessions[user_id]["page_view_count"] = \
                                current_sessions[user_id].get("page_view_count", 0) + 1
                        elif event_name == "Feature Used":
                            current_sessions[user_id]["feature_used_count"] = \
                                current_sessions[user_id].get("feature_used_count", 0) + 1
                        elif event_name == "Production Completed":
                            current_sessions[user_id]["production_count"] = \
                                current_sessions[user_id].get("production_count", 0) + 1
                else:
                    # Start a new session
                    current_sessions[user_id] = {
                        "session_id": f"{user_id}_{event_time.timestamp()}",
                        "distinct_id": user_id,
                        "start_time": event_time,
                        "last_time": event_time,
                        "event_count": 1,
                        "browser": event.get("$browser"),
                        "os": event.get("$os"),
                        "device_type": event.get("$device"),
                        "referrer": event.get("$referrer"),
                        "properties": {}
                    }
            
            # Close all open sessions
            for user_id, session in current_sessions.items():
                session["end_time"] = session["last_time"]
                session["duration_seconds"] = (session["last_time"] - session["start_time"]).total_seconds()
                session_data.append(session)
            
            # Store sessions in database
            if session_data:
                stored_count = self.db.bulk_insert("user_sessions", session_data)
                logger.info("Stored %d sessions in the database", stored_count)
                return stored_count
            
            return 0
            
        except Exception as e:
            logger.error("Failed to process sessions: %s", str(e))
            raise
    
    def get_user_profile(self, distinct_id: str) -> Optional[Dict[str, Any]]:
        """
        Get a user profile by ID.
        
        Args:
            distinct_id (str): User's distinct ID.
            
        Returns:
            Optional[Dict[str, Any]]: User profile data, or None if not found.
        """
        try:
            query = "SELECT * FROM user_profiles WHERE distinct_id = :distinct_id LIMIT 1"
            results = self.db.execute_query(query, {"distinct_id": distinct_id})
            
            if results and len(results) > 0:
                return results[0]
                
            return None
            
        except Exception as e:
            logger.error("Failed to get user profile: %s", str(e))
            raise
    
    def get_sessions_for_user(self, distinct_id: str, limit: int = 10) -> List[Dict[str, Any]]:
        """
        Get recent sessions for a user.
        
        Args:
            distinct_id (str): User's distinct ID.
            limit (int): Maximum number of sessions to return.
            
        Returns:
            List[Dict[str, Any]]: List of user sessions.
        """
        try:
            query = """
                SELECT * FROM user_sessions 
                WHERE distinct_id = :distinct_id 
                ORDER BY start_time DESC 
                LIMIT :limit
            """
            
            return self.db.execute_query(query, {
                "distinct_id": distinct_id,
                "limit": limit
            })
            
        except Exception as e:
            logger.error("Failed to get user sessions: %s", str(e))
            raise
    
    def get_events_for_user(self, distinct_id: str, 
                           from_date: Optional[str] = None,
                           to_date: Optional[str] = None,
                           event_names: Optional[List[str]] = None,
                           limit: int = 100) -> List[Dict[str, Any]]:
        """
        Get events for a specific user.
        
        Args:
            distinct_id (str): User's distinct ID.
            from_date (Optional[str]): Start date in format 'YYYY-MM-DD'.
            to_date (Optional[str]): End date in format 'YYYY-MM-DD'.
            event_names (Optional[List[str]]): Filter to specific event names.
            limit (int): Maximum number of events to return.
            
        Returns:
            List[Dict[str, Any]]: List of events for the user.
        """
        try:
            params = {"distinct_id": distinct_id, "limit": limit}
            
            query = """
                SELECT * FROM events 
                WHERE distinct_id = :distinct_id
            """
            
            # Add date filters if provided
            if from_date:
                query += " AND time >= :from_date"
                params["from_date"] = from_date
                
            if to_date:
                query += " AND time <= :to_date"
                params["to_date"] = to_date
                
            # Add event name filter if provided
            if event_names and len(event_names) > 0:
                placeholders = [f":event_name_{i}" for i in range(len(event_names))]
                query += f" AND event_name IN ({', '.join(placeholders)})"
                
                for i, name in enumerate(event_names):
                    params[f"event_name_{i}"] = name
            
            # Add order and limit
            query += " ORDER BY time DESC LIMIT :limit"
            
            return self.db.execute_query(query, params)
            
        except Exception as e:
            logger.error("Failed to get user events: %s", str(e))
            raise
    
    def analyze_user_funnel(self, funnel_name: str, 
                          distinct_id: Optional[str] = None,
                          from_date: Optional[str] = None,
                          to_date: Optional[str] = None) -> Dict[str, Any]:
        """
        Analyze funnel completion for a user or all users.
        
        Args:
            funnel_name (str): Name of the funnel to analyze.
            distinct_id (Optional[str]): User's distinct ID to filter for specific user.
            from_date (Optional[str]): Start date in format 'YYYY-MM-DD'.
            to_date (Optional[str]): End date in format 'YYYY-MM-DD'.
            
        Returns:
            Dict[str, Any]: Funnel analysis results.
        """
        try:
            params = {"funnel_name": funnel_name}
            
            query = """
                SELECT 
                    funnel_name,
                    step_index,
                    step_name,
                    COUNT(DISTINCT distinct_id) AS user_count,
                    AVG(time_to_next_step_seconds) AS avg_time_to_next_step
                FROM event_sequences
                WHERE funnel_name = :funnel_name
            """
            
            # Add user filter if provided
            if distinct_id:
                query += " AND distinct_id = :distinct_id"
                params["distinct_id"] = distinct_id
                
            # Add date filters if provided
            if from_date:
                query += " AND event_time >= :from_date"
                params["from_date"] = from_date
                
            if to_date:
                query += " AND event_time <= :to_date"
                params["to_date"] = to_date
                
            # Group by funnel and step
            query += """
                GROUP BY funnel_name, step_index, step_name
                ORDER BY step_index
            """
            
            steps = self.db.execute_query(query, params)
            
            # Calculate conversion rates
            results = {
                "funnel_name": funnel_name,
                "steps": steps,
                "overall_conversion_rate": 0.0,
                "drop_off_points": []
            }
            
            if steps and len(steps) > 0:
                first_step_count = steps[0]["user_count"] if steps[0]["user_count"] else 0
                last_step_count = steps[-1]["user_count"] if steps[-1]["user_count"] else 0
                
                # Calculate overall conversion
                if first_step_count > 0:
                    results["overall_conversion_rate"] = last_step_count / first_step_count
                
                # Identify drop-off points
                prev_count = None
                for step in steps:
                    if prev_count is not None and prev_count > 0 and step["user_count"] < prev_count:
                        drop_rate = 1 - (step["user_count"] / prev_count)
                        if drop_rate > 0.1:  # More than 10% drop-off
                            results["drop_off_points"].append({
                                "step_name": step["step_name"],
                                "drop_rate": drop_rate,
                                "users_lost": prev_count - step["user_count"]
                            })
                    
                    prev_count = step["user_count"]
            
            return results
            
        except Exception as e:
            logger.error("Failed to analyze user funnel: %s", str(e))
            raise
    
    def analyze_funnel_advanced(self, 
                            funnel_steps: List[str],
                            from_date: Optional[str] = None,
                            to_date: Optional[str] = None,
                            segment_column: Optional[str] = None,
                            time_period: Optional[str] = None,
                            compare_to_previous: bool = False,
                            max_days_to_convert: int = 30) -> Dict[str, Any]:
        """
        Perform advanced funnel analysis using the FunnelAnalyzer.
        
        Args:
            funnel_steps (List[str]): Ordered list of event names forming the funnel
            from_date (Optional[str]): Start date in format 'YYYY-MM-DD'
            to_date (Optional[str]): End date in format 'YYYY-MM-DD'
            segment_column (Optional[str]): Column name to use for segmentation
            time_period (Optional[str]): Time period for grouping ('day', 'week', 'month')
            compare_to_previous (bool): Whether to compare with previous period
            max_days_to_convert (int): Maximum number of days allowed to complete the funnel
            
        Returns:
            Dict[str, Any]: Detailed funnel analysis results
        """
        try:
            logger.info(f"Performing advanced funnel analysis for steps: {', '.join(funnel_steps)}")
            
            # Fetch events for the specified period
            raw_events = self.mixpanel.get_events(
                event_names=funnel_steps,
                from_date=from_date,
                to_date=to_date
            )
            
            if not raw_events:
                logger.warning("No events fetched for funnel analysis")
                return {"error": "No events found for the specified period"}
            
            # Transform events to DataFrame
            events_df = self.transformer.transform_events(raw_events)
            
            if events_df.empty:
                logger.warning("No events after transformation")
                return {"error": "No events available after transformation"}
            
            # Basic funnel analysis
            funnel_results = self.funnel_analyzer.analyze_funnel(
                events=events_df,
                funnel_steps=funnel_steps,
                max_days_to_convert=max_days_to_convert
            )
            
            results = {"funnel_analysis": funnel_results}
            
            # Perform segmented analysis if requested
            if segment_column and segment_column in events_df.columns:
                segment_results = self.funnel_analyzer.segment_funnel_analysis(
                    events=events_df,
                    funnel_steps=funnel_steps,
                    segment_column=segment_column,
                    max_days_to_convert=max_days_to_convert
                )
                results["segment_analysis"] = segment_results
            
            # Perform time-based analysis if requested
            if time_period in ["day", "week", "month"]:
                time_results = self.funnel_analyzer.time_based_funnel_analysis(
                    events=events_df,
                    funnel_steps=funnel_steps,
                    time_period=time_period,
                    max_days_to_convert=max_days_to_convert
                )
                results["time_analysis"] = time_results
            
            # Compare with previous period if requested
            if compare_to_previous and from_date and to_date:
                try:
                    # Calculate the previous period of equal length
                    from_date_dt = datetime.strptime(from_date, "%Y-%m-%d")
                    to_date_dt = datetime.strptime(to_date, "%Y-%m-%d")
                    period_length = (to_date_dt - from_date_dt).days
                    
                    prev_to_date = from_date_dt - timedelta(days=1)
                    prev_from_date = prev_to_date - timedelta(days=period_length)
                    
                    # Fetch events for the previous period
                    prev_raw_events = self.mixpanel.get_events(
                        event_names=funnel_steps,
                        from_date=prev_from_date.strftime("%Y-%m-%d"),
                        to_date=prev_to_date.strftime("%Y-%m-%d")
                    )
                    
                    if prev_raw_events:
                        prev_events_df = self.transformer.transform_events(prev_raw_events)
                        
                        if not prev_events_df.empty:
                            prev_funnel_results = self.funnel_analyzer.analyze_funnel(
                                events=prev_events_df,
                                funnel_steps=funnel_steps,
                                max_days_to_convert=max_days_to_convert
                            )
                            
                            # Compare current period with previous period
                            comparison_results = self.funnel_analyzer.compare_funnels(
                                funnel_results_a=prev_funnel_results,
                                funnel_results_b=funnel_results,
                                name_a=f"Previous ({prev_from_date.strftime('%Y-%m-%d')} to {prev_to_date.strftime('%Y-%m-%d')})",
                                name_b=f"Current ({from_date} to {to_date})"
                            )
                            
                            results["period_comparison"] = comparison_results
                except Exception as e:
                    logger.error(f"Error comparing with previous period: {str(e)}")
                    results["period_comparison_error"] = str(e)
            
            return results
            
        except Exception as e:
            logger.error(f"Failed to perform advanced funnel analysis: {str(e)}")
            raise
