#!/usr/bin/env python3
"""
Test Analytics Methods

This script tests the key analytics methods in the EventsRepository using real data.
"""

from datetime import datetime, timedelta
from pprint import pprint

from hitcraft_analytics.data.repositories.events_repository import EventsRepository
from hitcraft_analytics.utils.logging.logger import get_logger

# Set up logger
logger = get_logger("test_analytics_methods")

def test_analytics_methods():
    """Test the analytics methods in the EventsRepository."""
    # Create events repository
    events_repo = EventsRepository()
    
    # Set date range for testing (last 30 days)
    end_date = datetime.now()
    start_date = end_date - timedelta(days=30)
    from_date = start_date.strftime("%Y-%m-%d")
    to_date = end_date.strftime("%Y-%m-%d")
    
    logger.info(f"Testing analytics methods with date range: {from_date} to {to_date}")
    
    # Test get_key_metrics method
    try:
        logger.info("Testing get_key_metrics()...")
        key_metrics = events_repo.get_key_metrics(from_date=from_date, to_date=to_date)
        logger.info("Key metrics retrieved successfully")
        logger.info("Key metrics summary:")
        for metric_name, metric_data in key_metrics.items():
            current_value = metric_data.get('current', 'N/A')
            change = metric_data.get('change', 'N/A')
            logger.info(f"  {metric_name}: {current_value} (change: {change}%)")
    except Exception as e:
        logger.error(f"Error testing get_key_metrics: {str(e)}")
    
    # Test get_funnel_data method
    try:
        logger.info("Testing get_funnel_data()...")
        funnel_data = events_repo.get_funnel_data(from_date=from_date, to_date=to_date)
        logger.info("Funnel data retrieved successfully")
        logger.info("Funnel data summary:")
        for funnel_name, funnel_info in funnel_data.items():
            logger.info(f"  Funnel: {funnel_info.get('name')}")
            for step in funnel_info.get('steps', []):
                logger.info(f"    {step.get('name')}: {step.get('count')} users ({step.get('conversion_rate'):.1f}%)")
    except Exception as e:
        logger.error(f"Error testing get_funnel_data: {str(e)}")
    
    # Test get_trend_data method
    try:
        logger.info("Testing get_trend_data()...")
        trend_data = events_repo.get_trend_data(
            from_date=from_date, 
            to_date=to_date,
            metrics=["dau", "event_count"]  # Test with just a couple of metrics
        )
        logger.info("Trend data retrieved successfully")
        logger.info("Trend data summary:")
        for metric_name, trend_points in trend_data.items():
            latest_point = trend_points[-1] if trend_points else {"date": "N/A", "value": "N/A"}
            logger.info(f"  {metric_name}: Latest value on {latest_point.get('date')}: {latest_point.get('value')}")
            logger.info(f"  {metric_name}: Total data points: {len(trend_points)}")
    except Exception as e:
        logger.error(f"Error testing get_trend_data: {str(e)}")

if __name__ == "__main__":
    test_analytics_methods()
