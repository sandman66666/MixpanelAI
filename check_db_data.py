#!/usr/bin/env python3
"""
Check Database Data

This script checks the data in the database tables to verify the setup.
"""

import psycopg2
from hitcraft_analytics.config.db_config import (
    DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD
)
from hitcraft_analytics.utils.logging.logger import get_logger

# Set up logger
logger = get_logger("check_db_data")

def check_database():
    """Check data in database tables."""
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD
        )
        cursor = conn.cursor()
        logger.info("Connected to database")
        
        # Check event counts
        cursor.execute("SELECT COUNT(*) FROM events")
        event_count = cursor.fetchone()[0]
        logger.info(f"Events table contains {event_count} records")
        
        # Sample event data
        if event_count > 0:
            cursor.execute("""
                SELECT event_name, distinct_id, time 
                FROM events 
                ORDER BY time DESC 
                LIMIT 5
            """)
            logger.info("Sample events:")
            for row in cursor.fetchall():
                logger.info(f"  Event: {row[0]}, User: {row[1]}, Time: {row[2]}")
        
        # Check user profile counts
        cursor.execute("SELECT COUNT(*) FROM user_profiles")
        profile_count = cursor.fetchone()[0]
        logger.info(f"User profiles table contains {profile_count} records")
        
        # Sample user profile data
        if profile_count > 0:
            cursor.execute("""
                SELECT distinct_id, event_count, session_count 
                FROM user_profiles 
                LIMIT 5
            """)
            logger.info("Sample user profiles:")
            for row in cursor.fetchall():
                logger.info(f"  User: {row[0]}, Events: {row[1]}, Sessions: {row[2]}")
        
        # Check user session counts
        try:
            cursor.execute("SELECT COUNT(*) FROM user_sessions")
            session_count = cursor.fetchone()[0]
            logger.info(f"User sessions table contains {session_count} records")
            
            # Sample session data
            if session_count > 0:
                cursor.execute("""
                    SELECT session_id, distinct_id, start_time, end_time 
                    FROM user_sessions 
                    LIMIT 5
                """)
                logger.info("Sample sessions:")
                for row in cursor.fetchall():
                    logger.info(f"  Session: {row[0]}, User: {row[1]}, Start: {row[2]}, End: {row[3]}")
        except Exception as e:
            logger.error(f"Error checking sessions: {str(e)}")
        
        # Check event sequence counts
        try:
            cursor.execute("SELECT COUNT(*) FROM event_sequences")
            sequence_count = cursor.fetchone()[0]
            logger.info(f"Event sequences table contains {sequence_count} records")
            
            # Sample sequence data
            if sequence_count > 0:
                cursor.execute("""
                    SELECT sequence_id, distinct_id, funnel_name, step_name 
                    FROM event_sequences 
                    LIMIT 5
                """)
                logger.info("Sample sequences:")
                for row in cursor.fetchall():
                    logger.info(f"  Sequence: {row[0]}, User: {row[1]}, Funnel: {row[2]}, Step: {row[3]}")
        except Exception as e:
            logger.error(f"Error checking sequences: {str(e)}")
        
        conn.close()
        
    except Exception as e:
        logger.error(f"Error checking database: {str(e)}")

if __name__ == "__main__":
    check_database()
