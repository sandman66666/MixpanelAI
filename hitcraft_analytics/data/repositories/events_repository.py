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
    
    Provides methods for fetching, transforming, storing, and analyzing event data.
    Key methods include:
        - fetch_and_store_events: Retrieves events from Mixpanel and stores them in the database
        - get_key_metrics: Retrieves key performance metrics based on stored event data
        - get_funnel_data: Analyzes conversion funnels across user journeys
        - get_trend_data: Analyzes time-based trends in metrics and events
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
    
    def get_key_metrics(self, from_date: Optional[str] = None, to_date: Optional[str] = None) -> Dict[str, Any]:
        """
        Get key performance metrics based on stored event data.
        
        Args:
            from_date (Optional[str]): Start date in format 'YYYY-MM-DD'
            to_date (Optional[str]): End date in format 'YYYY-MM-DD'
            
        Returns:
            Dict[str, Any]: Dictionary containing key metrics with current, previous, and change values
        """
        logger.info(f"Getting key metrics from {from_date} to {to_date}")
        
        # Set default date range if not provided
        if not to_date:
            to_date = datetime.now().strftime("%Y-%m-%d")
        if not from_date:
            from_date_dt = datetime.strptime(to_date, "%Y-%m-%d") - timedelta(days=30)
            from_date = from_date_dt.strftime("%Y-%m-%d")
            
        # Calculate previous period for comparison
        current_from_dt = datetime.strptime(from_date, "%Y-%m-%d")
        current_to_dt = datetime.strptime(to_date, "%Y-%m-%d")
        period_days = (current_to_dt - current_from_dt).days
        
        prev_to_dt = current_from_dt - timedelta(days=1)
        prev_from_dt = prev_to_dt - timedelta(days=period_days)
        
        prev_from = prev_from_dt.strftime("%Y-%m-%d")
        prev_to = prev_to_dt.strftime("%Y-%m-%d")
        
        try:
            # Query for current period metrics
            metrics = {}
            
            # Daily Active Users (DAU)
            current_dau_query = f"""
                SELECT COUNT(DISTINCT distinct_id) as value
                FROM events
                WHERE time BETWEEN '{from_date}' AND '{to_date}'
                GROUP BY DATE(time)
                ORDER BY value DESC
                LIMIT 1
            """
            
            # Previous period DAU
            prev_dau_query = f"""
                SELECT COUNT(DISTINCT distinct_id) as value
                FROM events
                WHERE time BETWEEN '{prev_from}' AND '{prev_to}'
                GROUP BY DATE(time)
                ORDER BY value DESC
                LIMIT 1
            """
            
            # Weekly Active Users (WAU)
            current_wau_query = f"""
                SELECT COUNT(DISTINCT distinct_id) as value
                FROM events
                WHERE time BETWEEN '{from_date}' AND '{to_date}'
            """
            
            # Previous period WAU
            prev_wau_query = f"""
                SELECT COUNT(DISTINCT distinct_id) as value
                FROM events
                WHERE time BETWEEN '{prev_from}' AND '{prev_to}'
            """
            
            # Retention rate (users who returned after their first visit)
            current_retention_query = f"""
                SELECT 
                    (COUNT(DISTINCT u2.distinct_id) * 100.0 / NULLIF(COUNT(DISTINCT u1.distinct_id), 0)) as retention_rate
                FROM 
                    (SELECT DISTINCT distinct_id FROM events 
                     WHERE time BETWEEN '{from_date}' AND '{to_date}') u1
                LEFT JOIN 
                    (SELECT DISTINCT e2.distinct_id 
                     FROM events e1
                     JOIN events e2 ON e1.distinct_id = e2.distinct_id AND e2.time > e1.time + interval '1 day'
                     WHERE e1.time BETWEEN '{from_date}' AND '{to_date}'
                     AND e2.time BETWEEN '{from_date}' AND '{to_date}') u2
                ON u1.distinct_id = u2.distinct_id
            """
            
            # Previous period retention
            prev_retention_query = f"""
                SELECT 
                    (COUNT(DISTINCT u2.distinct_id) * 100.0 / NULLIF(COUNT(DISTINCT u1.distinct_id), 0)) as retention_rate
                FROM 
                    (SELECT DISTINCT distinct_id FROM events 
                     WHERE time BETWEEN '{prev_from}' AND '{prev_to}') u1
                LEFT JOIN 
                    (SELECT DISTINCT e2.distinct_id 
                     FROM events e1
                     JOIN events e2 ON e1.distinct_id = e2.distinct_id AND e2.time > e1.time + interval '1 day'
                     WHERE e1.time BETWEEN '{prev_from}' AND '{prev_to}'
                     AND e2.time BETWEEN '{prev_from}' AND '{prev_to}') u2
                ON u1.distinct_id = u2.distinct_id
            """
            
            # Get conversion rate (signup to purchase or similar key conversion)
            # This is an example - adjust event names based on your actual funnel
            current_conversion_query = f"""
                WITH funnel AS (
                    SELECT
                        COUNT(DISTINCT CASE WHEN event_name = 'app_open' THEN distinct_id END) as step1,
                        COUNT(DISTINCT CASE WHEN event_name = 'purchase' THEN distinct_id END) as step2
                    FROM events
                    WHERE time BETWEEN '{from_date}' AND '{to_date}'
                )
                SELECT 
                    (step2 * 100.0 / NULLIF(step1, 0)) as conversion_rate
                FROM funnel
            """
            
            # Previous period conversion
            prev_conversion_query = f"""
                WITH funnel AS (
                    SELECT
                        COUNT(DISTINCT CASE WHEN event_name = 'app_open' THEN distinct_id END) as step1,
                        COUNT(DISTINCT CASE WHEN event_name = 'purchase' THEN distinct_id END) as step2
                    FROM events
                    WHERE time BETWEEN '{prev_from}' AND '{prev_to}'
                )
                SELECT 
                    (step2 * 100.0 / NULLIF(step1, 0)) as conversion_rate
                FROM funnel
            """
            
            # Execute queries and store results
            try:
                current_dau_result = self.db.execute_query(current_dau_query)
                current_dau = current_dau_result[0]['value'] if current_dau_result else 0
            except Exception as e:
                logger.warning(f"Error calculating current DAU: {str(e)}")
                current_dau = 0
                
            try:
                prev_dau_result = self.db.execute_query(prev_dau_query)
                prev_dau = prev_dau_result[0]['value'] if prev_dau_result else 0
            except Exception as e:
                logger.warning(f"Error calculating previous DAU: {str(e)}")
                prev_dau = 0
                
            try:
                current_wau_result = self.db.execute_query(current_wau_query)
                current_wau = current_wau_result[0]['value'] if current_wau_result else 0
            except Exception as e:
                logger.warning(f"Error calculating current WAU: {str(e)}")
                current_wau = 0
                
            try:
                prev_wau_result = self.db.execute_query(prev_wau_query)
                prev_wau = prev_wau_result[0]['value'] if prev_wau_result else 0
            except Exception as e:
                logger.warning(f"Error calculating previous WAU: {str(e)}")
                prev_wau = 0
            
            try:
                current_retention_result = self.db.execute_query(current_retention_query)
                current_retention = current_retention_result[0]['retention_rate'] if current_retention_result else 0
            except Exception as e:
                logger.warning(f"Error calculating current retention: {str(e)}")
                current_retention = 0
                
            try:
                prev_retention_result = self.db.execute_query(prev_retention_query)
                prev_retention = prev_retention_result[0]['retention_rate'] if prev_retention_result else 0
            except Exception as e:
                logger.warning(f"Error calculating previous retention: {str(e)}")
                prev_retention = 0
                
            try:
                current_conversion_result = self.db.execute_query(current_conversion_query)
                current_conversion = current_conversion_result[0]['conversion_rate'] if current_conversion_result else 0
            except Exception as e:
                logger.warning(f"Error calculating current conversion: {str(e)}")
                current_conversion = 0
                
            try:
                prev_conversion_result = self.db.execute_query(prev_conversion_query)
                prev_conversion = prev_conversion_result[0]['conversion_rate'] if prev_conversion_result else 0
            except Exception as e:
                logger.warning(f"Error calculating previous conversion: {str(e)}")
                prev_conversion = 0
            
            # Calculate percentage changes
            try:
                dau_change = ((current_dau - prev_dau) / prev_dau * 100) if prev_dau > 0 else 0
            except ZeroDivisionError:
                dau_change = 0
                
            try:
                wau_change = ((current_wau - prev_wau) / prev_wau * 100) if prev_wau > 0 else 0
            except ZeroDivisionError:
                wau_change = 0
                
            try:
                retention_change = current_retention - prev_retention
            except Exception:
                retention_change = 0
                
            try:
                conversion_change = current_conversion - prev_conversion
            except Exception:
                conversion_change = 0
            
            # Build metrics object
            metrics = {
                "dau": {"current": current_dau, "previous": prev_dau, "change": dau_change},
                "wau": {"current": current_wau, "previous": prev_wau, "change": wau_change},
                "retention": {"current": current_retention, "previous": prev_retention, "change": retention_change},
                "conversion": {"current": current_conversion, "previous": prev_conversion, "change": conversion_change}
            }
            
            # Add period information
            metrics["period"] = {
                "current": {"from": from_date, "to": to_date},
                "previous": {"from": prev_from, "to": prev_to},
                "days": period_days
            }
            
            return metrics
        
        except Exception as e:
            logger.error(f"Failed to get key metrics: {str(e)}", exc_info=True)
            # Instead of falling back to sample data, raise the exception
            raise
    
    def get_funnel_data(self, from_date: Optional[str] = None, to_date: Optional[str] = None) -> Dict[str, Any]:
        """
        Get funnel data for user journey analysis.
        
        Args:
            from_date (Optional[str]): Start date in format 'YYYY-MM-DD'
            to_date (Optional[str]): End date in format 'YYYY-MM-DD'
            
        Returns:
            Dict[str, Any]: Dictionary containing funnel data for different user journeys
        """
        logger.info(f"Getting funnel data from {from_date} to {to_date}")
        
        # Set default date range if not provided
        if not to_date:
            to_date = datetime.now().strftime("%Y-%m-%d")
        if not from_date:
            from_date_dt = datetime.strptime(to_date, "%Y-%m-%d") - timedelta(days=30)
            from_date = from_date_dt.strftime("%Y-%m-%d")
        
        try:
            # Define common user journeys/funnels to analyze
            funnels = {
                "signup": ["visit_landing_page", "view_signup_form", "start_registration", "complete_registration", "verify_email", "complete_profile"],
                "purchase": ["app_open", "view_content", "add_to_cart", "purchase"],  # Removed checkout_start as it's not in our sample data
                "content_creation": ["app_open", "create_new_project", "save_project", "share_project"]
            }
            
            funnel_results = {}
            
            # Analyze each funnel using direct DB queries instead of the API
            for funnel_name, funnel_steps in funnels.items():
                # Check if we have these event types in our database
                event_check_query = f"""
                    SELECT event_name, COUNT(*) as count
                    FROM events
                    WHERE time BETWEEN '{from_date}' AND '{to_date}'
                    AND event_name IN ({', '.join(["'" + step + "'" for step in funnel_steps])})
                    GROUP BY event_name
                """
                
                try:
                    event_counts = self.db.execute_query(event_check_query)
                except Exception as e:
                    logger.warning(f"Error checking events for funnel {funnel_name}: {str(e)}")
                    continue
                
                # Skip this funnel if we don't have any of the required events
                if not event_counts:
                    logger.warning(f"No events found for funnel: {funnel_name}")
                    continue
                
                # Create a simple funnel analysis using direct SQL queries
                try:
                    # Get counts for each step directly from the database
                    steps = []
                    first_step_count = 0
                    
                    for i, step in enumerate(funnel_steps):
                        # Query for this specific step
                        step_query = f"""
                            SELECT COUNT(DISTINCT distinct_id) as count
                            FROM events
                            WHERE time BETWEEN '{from_date}' AND '{to_date}'
                            AND event_name = '{step}'
                        """
                        
                        step_result = self.db.execute_query(step_query)
                        step_count = step_result[0]['count'] if step_result else 0
                        
                        # Store first step count for conversion rate calculation
                        if i == 0:
                            first_step_count = step_count
                        
                        # Calculate conversion rate
                        conversion_rate = 100.0 if i == 0 or first_step_count == 0 else (step_count / first_step_count) * 100
                        
                        # Add step to results
                        steps.append({
                            "name": step,
                            "count": step_count,
                            "conversion_rate": conversion_rate
                        })
                    
                    # Only add funnel if we have at least some data
                    if first_step_count > 0:
                        funnel_results[funnel_name] = {
                            "name": funnel_name.replace("_", " ").title() + " Funnel",
                            "steps": steps,
                            "period": {"from": from_date, "to": to_date}
                        }
                    
                except Exception as e:
                    logger.warning(f"Error analyzing funnel {funnel_name}: {str(e)}")
            
            # If we couldn't analyze any funnels, raise an exception
            if not funnel_results:
                # Create a basic fallback funnel with our data
                # Find which events we do have in our database
                all_events_query = """
                    SELECT event_name, COUNT(DISTINCT distinct_id) as count
                    FROM events
                    WHERE time BETWEEN '{from_date}' AND '{to_date}'
                    GROUP BY event_name
                    ORDER BY count DESC
                """.format(from_date=from_date, to_date=to_date)
                
                all_events = self.db.execute_query(all_events_query)
                
                if all_events:
                    # Create a custom funnel from the events we have
                    custom_steps = []
                    for event in all_events:
                        event_name = event['event_name']
                        event_count = event['count']
                        
                        # Calculate conversion rate based on the first event
                        first_event_count = all_events[0]['count']
                        conversion_rate = 100.0 if event_name == all_events[0]['event_name'] else (event_count / first_event_count) * 100
                        
                        custom_steps.append({
                            "name": event_name,
                            "count": event_count,
                            "conversion_rate": conversion_rate
                        })
                    
                    funnel_results["custom"] = {
                        "name": "Custom Event Funnel",
                        "steps": custom_steps,
                        "period": {"from": from_date, "to": to_date}
                    }
                else:
                    raise ValueError("No funnel data available for the specified period")
                
            return funnel_results
            
        except Exception as e:
            logger.error(f"Failed to get funnel data: {str(e)}", exc_info=True)
            # Instead of falling back to sample data, raise the exception
            raise
    
    def get_trend_data(self, from_date: Optional[str] = None, to_date: Optional[str] = None, 
                     metrics: Optional[List[str]] = None) -> Dict[str, List[Dict[str, Any]]]:
        """
        Get trend data for time-series analysis of key metrics.
        
        Args:
            from_date (Optional[str]): Start date in format 'YYYY-MM-DD'
            to_date (Optional[str]): End date in format 'YYYY-MM-DD'
            metrics (Optional[List[str]]): List of metric names to retrieve trends for
            
        Returns:
            Dict[str, List[Dict[str, Any]]]: Dictionary of metric trends with time-series data
        """
        logger.info(f"Getting trend data from {from_date} to {to_date} for metrics: {metrics}")
        
        # Set default date range if not provided
        if not to_date:
            to_date = datetime.now().strftime("%Y-%m-%d")
        if not from_date:
            from_date_dt = datetime.strptime(to_date, "%Y-%m-%d") - timedelta(days=30)
            from_date = from_date_dt.strftime("%Y-%m-%d")
            
        # Set default metrics if not provided
        if not metrics:
            metrics = ["dau", "wau", "event_count", "session_count", "retention_rate"]
            
        try:
            trends = {}
            
            # Generate date range for the period
            from_date_dt = datetime.strptime(from_date, "%Y-%m-%d")
            to_date_dt = datetime.strptime(to_date, "%Y-%m-%d")
            date_range = [(from_date_dt + timedelta(days=i)).strftime("%Y-%m-%d") 
                          for i in range((to_date_dt - from_date_dt).days + 1)]
            
            # Query for each metric over time
            for metric in metrics:
                if metric == "dau":
                    # Daily Active Users trend
                    dau_query = f"""
                        SELECT 
                            DATE(time) as date,
                            COUNT(DISTINCT distinct_id) as value
                        FROM events
                        WHERE time BETWEEN '{from_date}' AND '{to_date}'
                        GROUP BY DATE(time)
                        ORDER BY DATE(time)
                    """
                    
                    try:
                        dau_results = self.db.execute_query(dau_query)
                    except Exception as e:
                        logger.warning(f"Error calculating DAU trend: {str(e)}")
                        dau_results = []
                    
                    # Convert to time-series format
                    if dau_results:
                        # Create dict with date as key for easy lookup
                        dau_lookup = {row['date'].strftime("%Y-%m-%d"): row['value'] for row in dau_results}
                        
                        # Fill in any missing dates with 0 or the previous value
                        trend_data = []
                        prev_value = 0
                        for date in date_range:
                            if date in dau_lookup:
                                value = dau_lookup[date]
                                prev_value = value
                            else:
                                value = 0  # No users on this day
                                
                            trend_data.append({"date": date, "value": value})
                            
                        trends["dau"] = trend_data
                
                elif metric == "wau":
                    # Weekly Active Users trend (rolling 7-day window)
                    wau_data = []
                    
                    for i, date in enumerate(date_range):
                        current_date = datetime.strptime(date, "%Y-%m-%d")
                        start_date = (current_date - timedelta(days=6)).strftime("%Y-%m-%d")
                        end_date = current_date.strftime("%Y-%m-%d")
                        
                        # Only calculate WAU if we have a full 7-day window
                        if current_date - timedelta(days=6) >= from_date_dt:
                            wau_query = f"""
                                SELECT 
                                    COUNT(DISTINCT distinct_id) as value
                                FROM events
                                WHERE time BETWEEN '{start_date}' AND '{end_date}'
                            """
                            
                            try:
                                wau_result = self.db.execute_query(wau_query)
                                value = wau_result[0]['value'] if wau_result else 0
                            except Exception as e:
                                logger.warning(f"Error calculating WAU for {date}: {str(e)}")
                                value = 0
                        else:
                            # Not enough data for a full week
                            value = None
                            
                        wau_data.append({"date": date, "value": value})
                    
                    # Only add WAU if we have at least some data points
                    if any(item["value"] is not None for item in wau_data):
                        trends["wau"] = wau_data
                
                elif metric == "event_count":
                    # Daily event count trend
                    event_query = f"""
                        SELECT 
                            DATE(time) as date,
                            COUNT(*) as value
                        FROM events
                        WHERE time BETWEEN '{from_date}' AND '{to_date}'
                        GROUP BY DATE(time)
                        ORDER BY DATE(time)
                    """
                    
                    try:
                        event_results = self.db.execute_query(event_query)
                    except Exception as e:
                        logger.warning(f"Error calculating event count trend: {str(e)}")
                        event_results = []
                    
                    if event_results:
                        # Create dict with date as key for easy lookup
                        event_lookup = {row['date'].strftime("%Y-%m-%d"): row['value'] for row in event_results}
                        
                        # Fill in any missing dates with 0
                        trend_data = []
                        for date in date_range:
                            value = event_lookup.get(date, 0)
                            trend_data.append({"date": date, "value": value})
                            
                        trends["event_count"] = trend_data
                
                elif metric == "session_count":
                    # Daily session count trend (approx. by counting sessions that start on each day)
                    session_query = f"""
                        SELECT 
                            DATE(start_time) as date,
                            COUNT(*) as value
                        FROM user_sessions
                        WHERE start_time BETWEEN '{from_date}' AND '{to_date}'
                        GROUP BY DATE(start_time)
                        ORDER BY DATE(start_time)
                    """
                    
                    try:
                        session_results = self.db.execute_query(session_query)
                    except Exception as e:
                        logger.warning(f"Error calculating session count trend: {str(e)}")
                        session_results = []
                    
                    if session_results:
                        # Create dict with date as key for easy lookup
                        session_lookup = {row['date'].strftime("%Y-%m-%d"): row['value'] for row in session_results}
                        
                        # Fill in any missing dates with 0
                        trend_data = []
                        for date in date_range:
                            value = session_lookup.get(date, 0)
                            trend_data.append({"date": date, "value": value})
                            
                        trends["session_count"] = trend_data
                            
                elif metric == "retention_rate":
                    # Rolling 7-day retention rate
                    retention_data = []
                    
                    for i, date in enumerate(date_range):
                        if i >= 7:  # Need at least 7 days of data
                            current_date = datetime.strptime(date, "%Y-%m-%d")
                            cohort_date = (current_date - timedelta(days=7)).strftime("%Y-%m-%d")
                            
                            retention_query = f"""
                                SELECT 
                                    (COUNT(DISTINCT returning.distinct_id) * 100.0 / 
                                     NULLIF(COUNT(DISTINCT cohort.distinct_id), 0)) as value
                                FROM 
                                    (SELECT DISTINCT distinct_id FROM events 
                                     WHERE DATE(time) = '{cohort_date}') cohort
                                LEFT JOIN 
                                    (SELECT DISTINCT distinct_id FROM events 
                                     WHERE DATE(time) = '{date}') returning
                                ON cohort.distinct_id = returning.distinct_id
                            """
                            
                            try:
                                retention_result = self.db.execute_query(retention_query)
                                value = retention_result[0]['value'] if retention_result else 0
                            except Exception as e:
                                logger.warning(f"Error calculating retention for {date}: {str(e)}")
                                value = 0
                        else:
                            # Not enough data yet
                            value = None
                            
                        retention_data.append({"date": date, "value": value})
                    
                    # Only add retention data if we have at least some points
                    if any(item["value"] is not None for item in retention_data):
                        trends["retention_rate"] = retention_data
            
            # If we couldn't get any trend data, raise an exception
            if not trends:
                raise ValueError("No trend data available for the specified period and metrics")
                
            return trends
            
        except Exception as e:
            logger.error(f"Failed to get trend data: {str(e)}", exc_info=True)
            # Instead of falling back to sample data, raise the exception
            raise
