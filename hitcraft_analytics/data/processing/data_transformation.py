"""
HitCraft AI Analytics Engine - Data Transformation

This module handles the transformation of raw data from Mixpanel into structured formats
suitable for storage and analysis in the HitCraft system.
"""

import json
from datetime import datetime
from typing import Dict, List, Any, Optional, Union

import pandas as pd
import numpy as np

from hitcraft_analytics.utils.logging.logger import get_logger

logger = get_logger(__name__)

class DataTransformer:
    """
    Handles transformation of raw data from Mixpanel into structured formats.
    """
    
    def __init__(self):
        """
        Initialize the data transformer.
        """
        logger.info("Data transformer initialized")
    
    def transform_events(self, events: List[Dict[str, Any]]) -> pd.DataFrame:
        """
        Transform raw Mixpanel event data into a structured DataFrame.
        
        Args:
            events (List[Dict[str, Any]]): Raw event data from Mixpanel.
            
        Returns:
            pd.DataFrame: Structured DataFrame with events.
        """
        logger.info("Transforming %d events", len(events))
        
        if not events:
            logger.warning("No events to transform")
            return pd.DataFrame()
        
        try:
            # Convert to DataFrame
            df = pd.DataFrame(events)
            
            # Extract nested properties into separate columns
            if "properties" in df.columns:
                # Normalize properties column into separate columns
                props_df = pd.json_normalize(df["properties"])
                
                # Drop original properties column
                df = df.drop(columns=["properties"])
                
                # Combine with properties dataframe
                df = pd.concat([df, props_df], axis=1)
            
            # Convert timestamp to datetime
            if "time" in df.columns:
                df["time"] = pd.to_datetime(df["time"], unit="s")
            
            # Handle missing values
            df = df.fillna({
                "distinct_id": "unknown",
                "event": "unknown",
                "$browser": "unknown",
                "$os": "unknown",
                "$city": "unknown",
                "$country_code": "unknown"
            })
            
            logger.info("Successfully transformed events to DataFrame with shape %s", df.shape)
            return df
            
        except Exception as e:
            logger.error("Failed to transform events: %s", str(e))
            raise
    
    def transform_engagement(self, engagement_data: Dict[str, Any]) -> pd.DataFrame:
        """
        Transform Mixpanel engagement data into a structured DataFrame.
        
        Args:
            engagement_data (Dict[str, Any]): Raw engagement data from Mixpanel.
            
        Returns:
            pd.DataFrame: Structured DataFrame with engagement metrics.
        """
        logger.info("Transforming engagement data")
        
        try:
            # Extract series data
            if "series" not in engagement_data:
                logger.warning("No series data in engagement_data")
                return pd.DataFrame()
                
            series = engagement_data["series"]
            
            # Create list for DataFrame
            data_rows = []
            
            # Process each series (typically by date)
            for date_str, values in series.items():
                # Convert date string to datetime
                date = datetime.strptime(date_str, "%Y-%m-%d")
                
                # Create row with date and metrics
                row = {"date": date}
                row.update(values)
                
                data_rows.append(row)
            
            # Convert to DataFrame
            df = pd.DataFrame(data_rows)
            
            logger.info("Successfully transformed engagement data to DataFrame with shape %s", df.shape)
            return df
            
        except Exception as e:
            logger.error("Failed to transform engagement data: %s", str(e))
            raise
    
    def transform_funnel(self, funnel_data: Dict[str, Any]) -> Dict[str, pd.DataFrame]:
        """
        Transform Mixpanel funnel data into structured DataFrames.
        
        Args:
            funnel_data (Dict[str, Any]): Raw funnel data from Mixpanel.
            
        Returns:
            Dict[str, pd.DataFrame]: Dictionary with "overall" and "steps" DataFrames.
        """
        logger.info("Transforming funnel data")
        
        try:
            results = {}
            
            # Extract funnel metadata
            funnel_id = funnel_data.get("funnel_id")
            funnel_name = funnel_data.get("name", f"Funnel_{funnel_id}")
            
            # Transform overall funnel metrics
            overall_data = []
            
            if "data" in funnel_data and "values" in funnel_data["data"]:
                for date_str, values in funnel_data["data"]["values"].items():
                    date = datetime.strptime(date_str, "%Y-%m-%d")
                    
                    row = {
                        "date": date,
                        "funnel_id": funnel_id,
                        "funnel_name": funnel_name,
                        "analysis_type": "overall"
                    }
                    
                    # Add conversion rates
                    if isinstance(values, dict):
                        row.update(values)
                    
                    overall_data.append(row)
            
            # Create overall DataFrame
            results["overall"] = pd.DataFrame(overall_data)
            
            # Transform step data
            steps_data = []
            
            if "steps" in funnel_data:
                for i, step in enumerate(funnel_data["steps"]):
                    step_name = step.get("name", f"Step {i+1}")
                    step_goal = step.get("goal", "")
                    step_count = step.get("count", 0)
                    
                    row = {
                        "funnel_id": funnel_id,
                        "funnel_name": funnel_name,
                        "step_index": i,
                        "step_name": step_name,
                        "step_goal": step_goal,
                        "count": step_count,
                        "analysis_type": "step"
                    }
                    
                    # Calculate conversion rate if possible
                    if i > 0 and "steps" in funnel_data and i-1 < len(funnel_data["steps"]):
                        prev_step_count = funnel_data["steps"][i-1].get("count", 0)
                        if prev_step_count > 0:
                            row["conversion_rate"] = step_count / prev_step_count
                        else:
                            row["conversion_rate"] = 0
                    elif i == 0:
                        row["conversion_rate"] = 1.0  # First step
                    
                    steps_data.append(row)
            
            # Create steps DataFrame
            results["steps"] = pd.DataFrame(steps_data)
            
            logger.info("Successfully transformed funnel data to DataFrames")
            return results
            
        except Exception as e:
            logger.error("Failed to transform funnel data: %s", str(e))
            raise
    
    def transform_retention(self, retention_data: Dict[str, Any]) -> pd.DataFrame:
        """
        Transform Mixpanel retention data into a structured DataFrame.
        
        Args:
            retention_data (Dict[str, Any]): Raw retention data from Mixpanel.
            
        Returns:
            pd.DataFrame: Structured DataFrame with retention metrics.
        """
        logger.info("Transforming retention data")
        
        try:
            data_rows = []
            
            if "cohorts" not in retention_data:
                logger.warning("No cohorts found in retention data")
                return pd.DataFrame()
            
            cohorts = retention_data["cohorts"]
            
            for cohort in cohorts:
                cohort_id = cohort.get("cohort_id", "")
                cohort_date = cohort.get("date")
                
                if not cohort_date:
                    continue
                
                # Convert date string to datetime
                try:
                    cohort_date = datetime.strptime(cohort_date, "%Y-%m-%d")
                except ValueError:
                    logger.warning("Invalid date format in retention data: %s", cohort_date)
                    continue
                
                # Get retention data
                if "retention" not in cohort:
                    continue
                    
                retention_values = cohort["retention"]
                
                # Create a row for each day in the retention series
                for day, value in enumerate(retention_values):
                    row = {
                        "cohort_id": cohort_id,
                        "cohort_date": cohort_date,
                        "day": day,
                        "retention_rate": value,
                        "total_users": cohort.get("total_users", 0)
                    }
                    
                    data_rows.append(row)
            
            # Convert to DataFrame
            df = pd.DataFrame(data_rows)
            
            logger.info("Successfully transformed retention data to DataFrame with shape %s", df.shape)
            return df
            
        except Exception as e:
            logger.error("Failed to transform retention data: %s", str(e))
            raise
    
    def prepare_data_for_storage(self, df: pd.DataFrame, table_name: str) -> List[Dict[str, Any]]:
        """
        Prepare DataFrame for storage in the database.
        
        Args:
            df (pd.DataFrame): DataFrame to prepare.
            table_name (str): Target table name, used to determine specific transformations.
            
        Returns:
            List[Dict[str, Any]]: List of dictionaries ready for database insertion.
        """
        logger.info("Preparing data for storage in table: %s", table_name)
        
        if df.empty:
            logger.warning("Empty DataFrame, nothing to prepare for storage")
            return []
        
        try:
            # Make a copy to avoid modifying the original
            df_copy = df.copy()
            
            # Handle NaN values
            df_copy = df_copy.replace({np.nan: None})
            
            # Specific transformations for different tables
            if table_name == "events":
                # Ensure time column is properly formatted
                if "time" in df_copy.columns and df_copy["time"].dtype != 'datetime64[ns]':
                    df_copy["time"] = pd.to_datetime(df_copy["time"])
            
            elif table_name == "metrics":
                # Handle date columns
                date_cols = [col for col in df_copy.columns if "date" in col.lower()]
                for col in date_cols:
                    if df_copy[col].dtype != 'datetime64[ns]':
                        df_copy[col] = pd.to_datetime(df_copy[col])
            
            # Convert to list of dictionaries
            records = df_copy.to_dict(orient="records")
            
            logger.info("Successfully prepared %d records for storage", len(records))
            return records
            
        except Exception as e:
            logger.error("Failed to prepare data for storage: %s", str(e))
            raise
    
    def extract_features(self, events_df: pd.DataFrame) -> pd.DataFrame:
        """
        Extract features from event data for machine learning models.
        
        Args:
            events_df (pd.DataFrame): DataFrame with event data.
            
        Returns:
            pd.DataFrame: DataFrame with extracted features.
        """
        logger.info("Extracting features from event data")
        
        if events_df.empty:
            logger.warning("Empty events DataFrame, cannot extract features")
            return pd.DataFrame()
        
        try:
            # Make a copy to avoid modifying the original
            df = events_df.copy()
            
            # Ensure time column is datetime
            if "time" in df.columns and df["time"].dtype != 'datetime64[ns]':
                df["time"] = pd.to_datetime(df["time"])
            
            # Group by user
            user_features = df.groupby("distinct_id").agg({
                "time": ["min", "max", "count"],
                "event": ["nunique", "count"]
            })
            
            # Flatten column names
            user_features.columns = ["_".join(col).strip() for col in user_features.columns.values]
            
            # Calculate time-based features
            if "time_min" in user_features.columns and "time_max" in user_features.columns:
                user_features["days_active"] = (
                    user_features["time_max"] - user_features["time_min"]
                ).dt.total_seconds() / (24 * 3600)
                
                # Average events per day
                user_features["events_per_day"] = user_features["event_count"] / user_features["days_active"].replace(0, 1)
            
            # Reset index to make distinct_id a regular column
            user_features = user_features.reset_index()
            
            logger.info("Successfully extracted features with shape %s", user_features.shape)
            return user_features
            
        except Exception as e:
            logger.error("Failed to extract features: %s", str(e))
            raise
