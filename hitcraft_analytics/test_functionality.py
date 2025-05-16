"""
Basic functionality test for HitCraft AI Analytics Engine components.

This script tests the core components of the system with sample data
to verify that they function as intended.
"""

import os
import sys
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from unittest.mock import patch, MagicMock

# Ensure we can import our modules
parent_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.append(parent_dir)
sys.path.append(os.path.abspath(os.path.dirname(__file__)))

# Create a function to test the Mixpanel connector
def test_mixpanel_connector():
    print("\n=== Testing Mixpanel Connector ===")
    
    # Import the connector
    try:
        from hitcraft_analytics.data.connectors.mixpanel_connector import MixpanelConnector
        print("✓ Successfully imported MixpanelConnector")
    except ImportError as e:
        print(f"✗ Failed to import MixpanelConnector: {str(e)}")
        return False
    
    # Mock the requests module for testing without actual API calls
    with patch('requests.Session') as mock_session:
        # Configure the mock
        mock_response = MagicMock()
        mock_response.json.return_value = {"test": "success"}
        mock_response.raise_for_status.return_value = None
        
        session_instance = MagicMock()
        session_instance.get.return_value = mock_response
        session_instance.post.return_value = mock_response
        
        mock_session.return_value = session_instance
        
        # Create the connector with test values
        try:
            connector = MixpanelConnector()
            connector.api_secret = "test_secret"
            connector.project_id = "test_project"
            
            # Test authentication headers
            headers = connector._get_auth_headers()
            print(f"✓ Generated auth headers: {headers is not None}")
            
            # Test URL construction
            url = connector._get_api_url("events/export")
            print(f"✓ Generated API URL: {url}")
            
            # Test _make_request method
            result = connector._make_request("GET", "events/export", params={"project_id": "test_project"})
            print(f"✓ Made test request: {result == {'test': 'success'}}")
            
            print("✓ MixpanelConnector tests passed")
            return True
        except Exception as e:
            print(f"✗ MixpanelConnector tests failed: {str(e)}")
            return False

# Create a function to test the data transformation
def test_data_transformer():
    print("\n=== Testing Data Transformer ===")
    
    # Import the transformer
    try:
        from hitcraft_analytics.data.processing.data_transformation import DataTransformer
        print("✓ Successfully imported DataTransformer")
    except ImportError as e:
        print(f"✗ Failed to import DataTransformer: {str(e)}")
        return False
    
    # Create sample event data
    try:
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
            }
        ]
        
        # Create the transformer
        transformer = DataTransformer()
        
        # Test events transformation
        events_df = transformer.transform_events(sample_events)
        print(f"✓ Transformed events to DataFrame with shape {events_df.shape}")
        
        # Verify all expected columns are present
        expected_cols = ['event', 'distinct_id', 'time']
        missing_cols = [col for col in expected_cols if col not in events_df.columns]
        if missing_cols:
            print(f"✗ Missing columns in transformed events: {missing_cols}")
            return False
        else:
            print("✓ All expected columns are present in transformed events")
        
        # Test preparation for storage
        records = transformer.prepare_data_for_storage(events_df, "events")
        print(f"✓ Prepared {len(records)} records for storage")
        
        # Test feature extraction
        user_features = transformer.extract_features(events_df)
        print(f"✓ Extracted user features with shape {user_features.shape}")
        
        print("✓ DataTransformer tests passed")
        return True
    except Exception as e:
        print(f"✗ DataTransformer tests failed: {str(e)}")
        return False

# Create a function to test the trend detection
def test_trend_detector():
    print("\n=== Testing Trend Detector ===")
    
    # Import the trend detector
    try:
        from hitcraft_analytics.core.analysis.trend_detection import TrendDetector
        print("✓ Successfully imported TrendDetector")
    except ImportError as e:
        print(f"✗ Failed to import TrendDetector: {str(e)}")
        return False
    
    # Create sample time series data
    try:
        # Create a time series with a clear trend
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
        
        # Create detector
        detector = TrendDetector()
        
        # Test upward trend detection
        upward_result = detector.detect_linear_trend(upward_series)
        print(f"✓ Detected upward trend: {upward_result['trend_detected']} with direction: {upward_result['direction']}")
        
        # Test downward trend detection
        downward_result = detector.detect_linear_trend(downward_series)
        print(f"✓ Detected downward trend: {downward_result['trend_detected']} with direction: {downward_result['direction']}")
        
        # Test no trend detection
        random_result = detector.detect_linear_trend(random_series)
        print(f"✓ Analyzed random data: trend detected: {random_result['trend_detected']}")
        
        # Test anomaly detection
        # Create a series with anomalies
        anomaly_values = upward_values.copy()
        anomaly_values[10] = anomaly_values[10] * 3  # Create spike
        anomaly_series = pd.Series(anomaly_values, index=dates)
        
        anomalies = detector.detect_anomalies(anomaly_series)
        print(f"✓ Detected {len(anomalies)} anomalies in test data")
        
        # Test comprehensive analysis
        analysis = detector.analyze_time_series(upward_series, "Test Metric")
        print(f"✓ Performed comprehensive analysis with {len(analysis)} result sections")
        
        print("✓ TrendDetector tests passed")
        return True
    except Exception as e:
        print(f"✗ TrendDetector tests failed: {str(e)}")
        return False

# Create a function to test the cohort analyzer
def test_cohort_analyzer():
    print("\n=== Testing Cohort Analyzer ===")
    
    # Import the cohort analyzer
    try:
        from hitcraft_analytics.core.analysis.cohort_analysis import CohortAnalyzer
        print("✓ Successfully imported CohortAnalyzer")
    except ImportError as e:
        print(f"✗ Failed to import CohortAnalyzer: {str(e)}")
        return False
    
    # Create sample user event data
    try:
        # Generate event data for multiple users across different cohorts
        user_events = []
        
        # Create 3 cohorts of users (starting on different days)
        cohort_start_dates = [
            datetime(2025, 1, 1),
            datetime(2025, 1, 8),
            datetime(2025, 1, 15)
        ]
        
        for cohort_idx, start_date in enumerate(cohort_start_dates):
            # 10 users per cohort
            for user_idx in range(10):
                user_id = f"user_{cohort_idx}_{user_idx}"
                
                # Each user has events on multiple days
                for day in range(30):
                    # Not all users are active every day
                    if np.random.random() < 0.7:
                        event_date = start_date + timedelta(days=day)
                        
                        # Multiple events per day
                        for _ in range(np.random.randint(1, 5)):
                            event = {
                                "distinct_id": user_id,
                                "event": np.random.choice(["login", "view_content", "create_content"]),
                                "time": event_date,
                                "first_event_date": start_date,  # Pre-compute for testing
                                "cohort_group": start_date.strftime("%Y-%m")
                            }
                            user_events.append(event)
        
        # Convert to DataFrame
        df = pd.DataFrame(user_events)
        
        # Create analyzer
        analyzer = CohortAnalyzer()
        
        # Test retention cohort creation
        cohort_retention = analyzer.create_retention_cohorts(
            df, 
            time_column='time',
            user_column='distinct_id',
            cohort_period='W'
        )
        print(f"✓ Created retention cohorts with shape {cohort_retention.shape}")
        
        # Test cohort metrics calculation
        df['value'] = np.random.rand(len(df)) * 10  # Add a test metric
        cohort_metrics = analyzer.calculate_cohort_metrics(
            df,
            time_column='time',
            user_column='distinct_id',
            metric_columns=['value']
        )
        print(f"✓ Calculated cohort metrics with {len(cohort_metrics)} metrics")
        
        # Test cohort comparison
        if cohort_metrics and 'value' in cohort_metrics:
            comparison = analyzer.compare_cohorts(cohort_metrics['value'], "value")
            print(f"✓ Compared cohorts with result keys: {list(comparison.keys())}")
        
        print("✓ CohortAnalyzer tests passed")
        return True
    except Exception as e:
        print(f"✗ CohortAnalyzer tests failed: {str(e)}")
        return False

def run_all_tests():
    print("===================================")
    print("Running HitCraft Analytics Tests...")
    print("===================================")
    
    # Track test results
    results = []
    
    # Test each component
    results.append(("Mixpanel Connector", test_mixpanel_connector()))
    results.append(("Data Transformer", test_data_transformer()))
    results.append(("Trend Detector", test_trend_detector()))
    results.append(("Cohort Analyzer", test_cohort_analyzer()))
    
    # Print summary
    print("\n===================================")
    print("Test Results Summary:")
    print("===================================")
    
    all_passed = True
    for component, passed in results:
        status = "PASSED" if passed else "FAILED"
        print(f"{component}: {status}")
        all_passed = all_passed and passed
    
    print("\nOverall Result:", "PASSED" if all_passed else "FAILED")
    print("===================================")
    
    return all_passed

if __name__ == "__main__":
    run_all_tests()
