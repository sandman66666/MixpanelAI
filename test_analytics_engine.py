"""
HitCraft AI Analytics Engine - Integration Test

This script tests the integrated functionality of the HitCraft AI Analytics Engine components,
including Mixpanel data fetching, funnel analysis, and insights generation.
"""

import os
import sys
import json
from datetime import datetime, timedelta
from typing import Dict, List, Any

# Add the project path to system path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

# Import required components
from hitcraft_analytics.data.connectors.mixpanel_connector import MixpanelConnector
from hitcraft_analytics.data.connectors.database_connector import DatabaseConnector
from hitcraft_analytics.data.processing.data_transformation import DataTransformer
from hitcraft_analytics.data.repositories.events_repository import EventsRepository
from hitcraft_analytics.core.analysis.trend_detection import TrendDetector
from hitcraft_analytics.core.analysis.cohort_analysis import CohortAnalyzer
from hitcraft_analytics.core.analysis.funnel_analysis import FunnelAnalyzer
from hitcraft_analytics.insights.insights_engine import InsightsEngine

# Set up colorful output
GREEN = "\033[92m"
RED = "\033[91m"
YELLOW = "\033[93m"
BLUE = "\033[94m"
RESET = "\033[0m"
BOLD = "\033[1m"

def print_header(title):
    """Print a section header."""
    print(f"\n{BOLD}{BLUE}{'=' * 80}{RESET}")
    print(f"{BOLD}{BLUE}== {title}{RESET}")
    print(f"{BOLD}{BLUE}{'=' * 80}{RESET}\n")

def print_result(name, success, details=None):
    """Print a test result."""
    status = f"{GREEN}PASSED{RESET}" if success else f"{RED}FAILED{RESET}"
    print(f"{BOLD}{name}: {status}{RESET}")
    
    if details:
        if isinstance(details, dict) or isinstance(details, list):
            # Pretty print JSON for dictionaries and lists
            print(json.dumps(details, indent=2, default=str))
        else:
            # Print string details
            print(f"  {details}")
    
    print()  # Add empty line

def test_mixpanel_connection():
    """Test connection to Mixpanel API."""
    print_header("Testing Mixpanel Connection")
    
    try:
        # Initialize Mixpanel connector
        connector = MixpanelConnector()
        
        # Get events from the last 30 days
        from_date = (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d")
        to_date = datetime.now().strftime("%Y-%m-%d")
        
        events = connector.get_events(
            from_date=from_date,
            to_date=to_date
        )
        
        success = True
        details = {
            "events_fetched": len(events),
            "period": f"{from_date} to {to_date}"
        }
        
        # If we have events, show a sample
        if events:
            sample_event = events[0]
            # Mask potential sensitive data
            if "properties" in sample_event:
                for key in ["distinct_id", "$email", "email", "name", "user_id"]:
                    if key in sample_event["properties"]:
                        sample_event["properties"][key] = f"[MASKED {key}]"
            
            details["sample_event"] = sample_event
        
        print_result("Mixpanel Connection", success, details)
        return success, events
        
    except Exception as e:
        print_result("Mixpanel Connection", False, f"Error: {str(e)}")
        return False, []

def test_data_transformation(events):
    """Test data transformation component."""
    print_header("Testing Data Transformation")
    
    try:
        # Skip if no events
        if not events:
            print_result("Data Transformation", False, "No events to transform")
            return False, None
        
        # Initialize transformer
        transformer = DataTransformer()
        
        # Transform events
        events_df = transformer.transform_events(events)
        
        success = not events_df.empty
        
        details = {
            "dataframe_shape": events_df.shape,
            "columns": list(events_df.columns),
            "row_count": len(events_df)
        }
        
        print_result("Data Transformation", success, details)
        return success, events_df
        
    except Exception as e:
        print_result("Data Transformation", False, f"Error: {str(e)}")
        return False, None

def test_funnel_analysis(events_df):
    """Test funnel analysis functionality."""
    print_header("Testing Funnel Analysis")
    
    try:
        # Skip if no events dataframe
        if events_df is None or events_df.empty:
            print_result("Funnel Analysis", False, "No events data for analysis")
            return False, None
        
        # Initialize funnel analyzer
        funnel_analyzer = FunnelAnalyzer()
        
        # Define a test funnel - using generic event names that might exist
        # Note: Adjust these event names based on what's actually in your Mixpanel data
        funnel_steps = [
            "page_view",  # Common event
            "button_click",
            "form_submit"
        ]
        
        # Get actual event names from the data
        actual_event_names = events_df['event'].unique().tolist() if 'event' in events_df.columns else []
        
        # If we have actual events, use the first few as our funnel steps
        if len(actual_event_names) >= 2:
            funnel_steps = actual_event_names[:min(3, len(actual_event_names))]
        
        print(f"Using funnel steps: {funnel_steps}")
        
        # Analyze funnel
        try:
            funnel_results = funnel_analyzer.analyze_funnel(
                events=events_df,
                funnel_steps=funnel_steps,
                max_days_to_convert=30
            )
            
            success = True
            details = {
                "funnel_steps": funnel_steps,
                "results": funnel_results
            }
            
        except ValueError as ve:
            # This might happen if we don't have enough matching events
            success = False
            details = f"Could not analyze funnel: {str(ve)}"
        
        print_result("Funnel Analysis", success, details)
        return success, funnel_results if success else None
        
    except Exception as e:
        print_result("Funnel Analysis", False, f"Error: {str(e)}")
        return False, None

def test_events_repository():
    """Test the EventsRepository functionality."""
    print_header("Testing Events Repository")
    
    try:
        # Initialize repository
        repo = EventsRepository()
        
        # Fetch events from the last 7 days
        from_date = (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d")
        to_date = datetime.now().strftime("%Y-%m-%d")
        
        # Use the enhanced analyze_funnel_advanced method
        funnel_steps = ["page_view", "button_click", "form_submit"]
        
        try:
            results = repo.analyze_funnel_advanced(
                funnel_steps=funnel_steps,
                from_date=from_date,
                to_date=to_date,
                segment_column="user_type",
                time_period="day",
                compare_to_previous=True
            )
            
            success = "error" not in results
            details = results
            
        except Exception as e:
            success = False
            details = f"Could not analyze funnel: {str(e)}"
        
        print_result("Events Repository", success, details)
        return success, results if success else None
        
    except Exception as e:
        print_result("Events Repository", False, f"Error: {str(e)}")
        return False, None

def test_insights_engine():
    """Test the InsightsEngine functionality."""
    print_header("Testing Insights Engine")
    
    try:
        # Initialize insights engine
        engine = InsightsEngine()
        
        # Generate insights for the last 30 days
        from_date = (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d")
        to_date = datetime.now().strftime("%Y-%m-%d")
        
        try:
            insights = engine.generate_insights(
                from_date=from_date,
                to_date=to_date,
                insight_types=["funnel", "trend"],
                max_insights=5
            )
            
            success = True
            details = {
                "insights_count": len(insights),
                "period": f"{from_date} to {to_date}",
                "insights": insights
            }
            
        except Exception as e:
            success = False
            details = f"Could not generate insights: {str(e)}"
        
        print_result("Insights Engine", success, details)
        return success, insights if success else None
        
    except Exception as e:
        print_result("Insights Engine", False, f"Error: {str(e)}")
        return False, None

def main():
    """Run all tests and summarize results."""
    print(f"\n{BOLD}{YELLOW}HitCraft AI Analytics Engine - Integration Test{RESET}\n")
    print(f"Starting tests at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
    
    # Store test results
    results = {}
    
    # Test Mixpanel connection
    mixpanel_success, events = test_mixpanel_connection()
    results["Mixpanel Connection"] = mixpanel_success
    
    # Test data transformation
    transform_success, events_df = test_data_transformation(events)
    results["Data Transformation"] = transform_success
    
    # Test funnel analysis
    funnel_success, _ = test_funnel_analysis(events_df)
    results["Funnel Analysis"] = funnel_success
    
    # Test events repository
    repo_success, _ = test_events_repository()
    results["Events Repository"] = repo_success
    
    # Test insights engine
    insights_success, _ = test_insights_engine()
    results["Insights Engine"] = insights_success
    
    # Print summary
    print_header("Test Summary")
    
    all_success = True
    for test_name, success in results.items():
        status = f"{GREEN}PASSED{RESET}" if success else f"{RED}FAILED{RESET}"
        print(f"{test_name}: {status}")
        all_success = all_success and success
    
    print(f"\n{BOLD}Overall Test Result: {GREEN}PASSED{RESET}" if all_success else f"\n{BOLD}Overall Test Result: {RED}FAILED{RESET}")
    print(f"\nTests completed at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

if __name__ == "__main__":
    main()
