#!/usr/bin/env python3
"""
Simple Test Data Loader for HitCraft Analytics

This script loads a small set of test events directly using SQL to ensure
we have real data for testing the AI insights functionality.
"""

import uuid
import json
from datetime import datetime, timedelta

from hitcraft_analytics.data.connectors.database_connector import DatabaseConnector
from hitcraft_analytics.utils.logging.logger import get_logger

# Set up logger
logger = get_logger("load_test_data")

def create_test_data():
    """Create a small set of test data for the events table."""
    # Get database connector
    db = DatabaseConnector()
    
    # Create some event data directly with SQL
    logger.info("Creating test events...")
    
    # Base timestamp (1 month ago)
    base_time = datetime.now() - timedelta(days=30)
    
    # User IDs
    user_ids = ["test_user_1", "test_user_2", "test_user_3", "test_user_4", "test_user_5"]
    
    # Event types
    event_types = ["app_open", "view_content", "add_to_cart", "purchase"]
    
    # Direct SQL query to insert events
    for day in range(30):  # For each day in the last month
        current_date = base_time + timedelta(days=day)
        
        # Each user does some events each day
        for user_id in user_ids:
            # Create app_open event
            event_id = str(uuid.uuid4())
            event_time = current_date + timedelta(hours=9, minutes=day % 60)  # Morning
            properties = json.dumps({"session_id": str(uuid.uuid4()), "version": "1.0.0"})
            
            insert_query = f"""
            INSERT INTO events (
                event_id, event_name, distinct_id, time, insert_time, 
                browser, browser_version, os, device, properties
            ) VALUES (
                '{event_id}', 'app_open', '{user_id}', '{event_time.isoformat()}', '{datetime.now().isoformat()}',
                'Chrome', '88.0', 'macOS', 'Desktop', '{properties}'
            );
            """
            
            try:
                db.execute_query(insert_query)
            except Exception as e:
                logger.error(f"Error inserting app_open event: {str(e)}")
            
            # 50% chance of completing a purchase
            if day % 2 == 0:
                # View content
                event_id = str(uuid.uuid4())
                event_time = current_date + timedelta(hours=10, minutes=day % 60)
                properties = json.dumps({"content_id": str(uuid.uuid4()), "category": "music"})
                
                insert_query = f"""
                INSERT INTO events (
                    event_id, event_name, distinct_id, time, insert_time, 
                    browser, browser_version, os, device, properties
                ) VALUES (
                    '{event_id}', 'view_content', '{user_id}', '{event_time.isoformat()}', '{datetime.now().isoformat()}',
                    'Chrome', '88.0', 'macOS', 'Desktop', '{properties}'
                );
                """
                
                try:
                    db.execute_query(insert_query)
                except Exception as e:
                    logger.error(f"Error inserting view_content event: {str(e)}")
                
                # Add to cart
                event_id = str(uuid.uuid4())
                event_time = current_date + timedelta(hours=11, minutes=day % 60)
                properties = json.dumps({"item_id": str(uuid.uuid4()), "price": 19.99})
                
                insert_query = f"""
                INSERT INTO events (
                    event_id, event_name, distinct_id, time, insert_time, 
                    browser, browser_version, os, device, properties
                ) VALUES (
                    '{event_id}', 'add_to_cart', '{user_id}', '{event_time.isoformat()}', '{datetime.now().isoformat()}',
                    'Chrome', '88.0', 'macOS', 'Desktop', '{properties}'
                );
                """
                
                try:
                    db.execute_query(insert_query)
                except Exception as e:
                    logger.error(f"Error inserting add_to_cart event: {str(e)}")
                
                # Purchase
                event_id = str(uuid.uuid4())
                event_time = current_date + timedelta(hours=12, minutes=day % 60)
                properties = json.dumps({"order_id": str(uuid.uuid4()), "total": 19.99})
                
                insert_query = f"""
                INSERT INTO events (
                    event_id, event_name, distinct_id, time, insert_time, 
                    browser, browser_version, os, device, properties
                ) VALUES (
                    '{event_id}', 'purchase', '{user_id}', '{event_time.isoformat()}', '{datetime.now().isoformat()}',
                    'Chrome', '88.0', 'macOS', 'Desktop', '{properties}'
                );
                """
                
                try:
                    db.execute_query(insert_query)
                except Exception as e:
                    logger.error(f"Error inserting purchase event: {str(e)}")
    
    logger.info("Test data creation complete!")

if __name__ == "__main__":
    create_test_data()
