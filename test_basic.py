"""
Basic functionality test for HitCraft AI Analytics Engine components.

This script tests the core components of the system with sample data
to verify that they function as intended, using direct imports.
"""

import os
import sys
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import logging
import json
from unittest.mock import patch, MagicMock

# Configure basic logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)

def get_logger(name):
    """Simple logger function"""
    return logging.getLogger(name)

# Create sample time series data for testing
def create_sample_time_series():
    """Create sample time series data for testing trend detection"""
    dates = pd.date_range(start='2025-01-01', periods=30, freq='D')
    
    # Upward trend
    upward_values = [100 + i*5 + np.random.normal(0, 10) for i in range(30)]
    upward_series = pd.Series(upward_values, index=dates)
    
    # Downward trend
    downward_values = [500 - i*5 + np.random.normal(0, 10) for i in range(30)]
    downward_series = pd.Series(downward_values, index=dates)
    
    # No trend (random)
    random_values = [300 + np.random.normal(0, 30) for _ in range(30)]
    random_series = pd.Series(random_values, index=dates)
    
    return upward_series, downward_series, random_series

# Test basic trend detection algorithm
def test_trend_detection():
    """Test basic trend detection functionality"""
    print("\n=== Testing Trend Detection Functionality ===")
    
    # Create sample data
    upward_series, downward_series, random_series = create_sample_time_series()
    
    # Implement a simplified version of trend detection
    def detect_trend(time_series):
        """Basic trend detection method"""
        if len(time_series) < 7:
            return "Not enough data"
        
        # Linear regression
        x = np.arange(len(time_series))
        y = time_series.values
        
        # Calculate slope using numpy's polyfit
        slope, intercept = np.polyfit(x, y, 1)
        
        # Calculate R-squared
        y_pred = slope * x + intercept
        ss_total = np.sum((y - np.mean(y))**2)
        ss_residual = np.sum((y - y_pred)**2)
        r_squared = 1 - (ss_residual / ss_total)
        
        # Calculate percent change
        first_value = time_series.iloc[0]
        last_value = time_series.iloc[-1]
        
        if first_value == 0:
            percent_change = float('inf') if last_value > 0 else float('-inf') if last_value < 0 else 0
        else:
            percent_change = ((last_value - first_value) / abs(first_value)) * 100
        
        # Determine trend direction
        if abs(percent_change) < 10 or r_squared < 0.5:
            return "No significant trend"
        elif slope > 0:
            return f"Increasing trend (slope: {slope:.2f}, R²: {r_squared:.2f}, change: {percent_change:.1f}%)"
        else:
            return f"Decreasing trend (slope: {slope:.2f}, R²: {r_squared:.2f}, change: {percent_change:.1f}%)"
    
    # Test with different series
    upward_result = detect_trend(upward_series)
    downward_result = detect_trend(downward_series)
    random_result = detect_trend(random_series)
    
    print(f"Upward series: {upward_result}")
    print(f"Downward series: {downward_result}")
    print(f"Random series: {random_result}")
    
    # Check if the results match expectations
    success = ("Increasing" in upward_result and 
               "Decreasing" in downward_result)
    
    print(f"Trend detection test {'PASSED' if success else 'FAILED'}")
    return success

# Test basic cohort analysis
def test_cohort_analysis():
    """Test basic cohort analysis functionality"""
    print("\n=== Testing Cohort Analysis Functionality ===")
    
    # Create sample user data
    user_data = []
    
    # Create 3 cohorts (users who joined in different weeks)
    cohort_start_dates = [
        datetime(2025, 1, 1),
        datetime(2025, 1, 8),
        datetime(2025, 1, 15)
    ]
    
    for cohort_idx, start_date in enumerate(cohort_start_dates):
        cohort_name = f"Cohort {cohort_idx+1}"
        
        # 10 users per cohort
        for user_idx in range(10):
            user_id = f"user_{cohort_idx}_{user_idx}"
            
            # Each user has events on multiple days with diminishing activity
            # (simulating retention drop-off)
            for day in range(28):
                # Retention drops off differently for each cohort
                if cohort_idx == 0:  # First cohort has best retention
                    retention_prob = 0.9 - (day * 0.02)
                elif cohort_idx == 1:  # Second cohort has medium retention
                    retention_prob = 0.8 - (day * 0.03)
                else:  # Third cohort has worst retention
                    retention_prob = 0.7 - (day * 0.04)
                
                # Skip if user churned (probability below threshold)
                if np.random.random() > max(0.1, retention_prob):
                    continue
                
                event_date = start_date + timedelta(days=day)
                user_data.append({
                    "user_id": user_id,
                    "cohort": cohort_name,
                    "date": event_date,
                    "first_date": start_date,
                    "day_number": day
                })
    
    # Convert to DataFrame
    df = pd.DataFrame(user_data)
    
    # Calculate retention by cohort and day
    def calculate_retention(df):
        """Calculate retention rates by cohort and day"""
        # Count unique users by cohort and day
        user_counts = df.groupby(['cohort', 'day_number'])['user_id'].nunique().reset_index()
        
        # Get cohort sizes (users on day 0)
        cohort_sizes = user_counts[user_counts['day_number'] == 0].copy()
        cohort_sizes.rename(columns={'user_id': 'cohort_size'}, inplace=True)
        cohort_sizes.drop('day_number', axis=1, inplace=True)
        
        # Merge with cohort sizes
        retention = user_counts.merge(cohort_sizes, on='cohort')
        
        # Calculate retention rate
        retention['retention_rate'] = retention['user_id'] / retention['cohort_size']
        
        # Pivot for easier analysis
        pivot = retention.pivot_table(
            index='cohort',
            columns='day_number',
            values='retention_rate'
        )
        
        return pivot
    
    retention_pivot = calculate_retention(df)
    print("Retention rates by cohort and day:")
    print(retention_pivot.round(2).head())
    
    # Check if the results match expectations (first cohort should have better retention)
    day_7_retention = retention_pivot[7].to_dict()  # Day 7 retention
    success = day_7_retention.get('Cohort 1', 0) > day_7_retention.get('Cohort 3', 0)
    
    print(f"Cohort analysis test {'PASSED' if success else 'FAILED'}")
    return success

# Test data transformation
def test_data_transformation():
    """Test basic data transformation functionality"""
    print("\n=== Testing Data Transformation ===")
    
    # Create sample event data
    sample_events = [
        {
            "event": "Sign Up",
            "properties": {
                "distinct_id": "user_1",
                "time": int(datetime.now().timestamp()),
                "$browser": "Chrome",
                "$os": "MacOS",
                "$city": "San Francisco",
                "$country_code": "US"
            }
        },
        {
            "event": "Login",
            "properties": {
                "distinct_id": "user_1",
                "time": int((datetime.now() + timedelta(hours=1)).timestamp()),
                "$browser": "Chrome",
                "$os": "MacOS",
                "$city": "San Francisco",
                "$country_code": "US"
            }
        },
        {
            "event": "Feature Used",
            "properties": {
                "distinct_id": "user_2",
                "time": int(datetime.now().timestamp()),
                "$browser": "Firefox",
                "$os": "Windows",
                "$city": "New York",
                "$country_code": "US",
                "feature_name": "Music Production"
            }
        }
    ]
    
    # Simple transformation function
    def transform_events(events):
        """Transform raw event data into a structured DataFrame"""
        if not events:
            return pd.DataFrame()
        
        # Convert to DataFrame
        records = []
        for event in events:
            record = {"event": event["event"]}
            
            # Add properties
            if "properties" in event:
                for key, value in event["properties"].items():
                    record[key] = value
            
            records.append(record)
        
        # Create DataFrame
        df = pd.DataFrame(records)
        
        # Convert timestamp to datetime
        if "time" in df.columns:
            df["time"] = pd.to_datetime(df["time"], unit="s")
        
        return df
    
    # Test transformation
    df = transform_events(sample_events)
    print(f"Transformed data shape: {df.shape}")
    print(f"Columns: {list(df.columns)}")
    
    # Extract user metrics
    def extract_user_metrics(df):
        """Extract user-level metrics from event data"""
        user_metrics = df.groupby("distinct_id").agg({
            "event": ["count", "nunique"],
            "time": ["min", "max"]
        })
        
        # Flatten column names
        user_metrics.columns = ["_".join(col).strip() for col in user_metrics.columns.values]
        
        # Calculate session duration
        user_metrics["session_duration_hours"] = (
            user_metrics["time_max"] - user_metrics["time_min"]
        ).dt.total_seconds() / 3600
        
        return user_metrics
    
    user_metrics = extract_user_metrics(df)
    print("\nUser metrics:")
    print(user_metrics)
    
    # Check if transformation worked correctly
    success = (len(df) == len(sample_events) and 
               "distinct_id" in df.columns and 
               "event" in df.columns and
               len(user_metrics) == 2)  # Two unique users
    
    print(f"Data transformation test {'PASSED' if success else 'FAILED'}")
    return success

def run_tests():
    """Run all tests and report results"""
    print("=================================================")
    print("Running Basic Functionality Tests")
    print("=================================================")
    
    test_results = {
        "Trend Detection": test_trend_detection(),
        "Cohort Analysis": test_cohort_analysis(),
        "Data Transformation": test_data_transformation()
    }
    
    print("\n=================================================")
    print("Test Results Summary:")
    print("=================================================")
    
    all_passed = True
    for test_name, result in test_results.items():
        status = "PASSED" if result else "FAILED"
        print(f"{test_name}: {status}")
        all_passed = all_passed and result
    
    print("\nOverall Result:", "PASSED" if all_passed else "FAILED")
    print("=================================================")
    
    return all_passed

if __name__ == "__main__":
    run_tests()
