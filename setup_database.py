#!/usr/bin/env python3
"""
Setup Database for HitCraft Analytics

This script sets up the database tables and loads sample data for testing.
"""

import os
import random
import uuid
import json
from datetime import datetime, timedelta
import psycopg2
from psycopg2.extras import Json

from hitcraft_analytics.config.db_config import (
    DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD
)
from hitcraft_analytics.utils.logging.logger import get_logger

# Set up logger
logger = get_logger("setup_database")

def setup_database():
    """Set up the database tables and load sample data."""
    logger.info("Setting up database...")
    
    # Connect to PostgreSQL
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD
        )
        conn.autocommit = True
        cursor = conn.cursor()
        logger.info("Connected to database")
    except Exception as e:
        logger.error(f"Failed to connect to database: {str(e)}")
        return
    
    # Create tables
    try:
        # Events table
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS events (
            id SERIAL PRIMARY KEY,
            event_id VARCHAR(64) NOT NULL,
            event_name VARCHAR(256) NOT NULL,
            distinct_id VARCHAR(256) NOT NULL,
            time TIMESTAMP NOT NULL,
            insert_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            browser VARCHAR(64),
            browser_version VARCHAR(64),
            os VARCHAR(64),
            device VARCHAR(64),
            city VARCHAR(64),
            country_code VARCHAR(8),
            referrer VARCHAR(512),
            referring_domain VARCHAR(256),
            screen_height INTEGER,
            screen_width INTEGER,
            properties JSONB
        )
        """)
        
        # Create indexes for events table
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_events_event_id ON events(event_id)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_events_event_name ON events(event_name)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_events_distinct_id ON events(distinct_id)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_events_time ON events(time)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_events_time_event ON events(time, event_name)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_events_user_time ON events(distinct_id, time)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_events_event_user ON events(event_name, distinct_id)")
        
        # User profiles table
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS user_profiles (
            id SERIAL PRIMARY KEY,
            distinct_id VARCHAR(256) NOT NULL UNIQUE,
            email VARCHAR(256),
            first_name VARCHAR(128),
            last_name VARCHAR(128),
            role VARCHAR(64),
            experience_level VARCHAR(64),
            user_type VARCHAR(64),
            user_segment VARCHAR(64),
            first_seen TIMESTAMP,
            last_seen TIMESTAMP,
            session_count INTEGER NOT NULL DEFAULT 0,
            event_count INTEGER NOT NULL DEFAULT 0,
            feature_count INTEGER NOT NULL DEFAULT 0,
            production_count INTEGER NOT NULL DEFAULT 0,
            sketch_count INTEGER NOT NULL DEFAULT 0,
            days_active INTEGER NOT NULL DEFAULT 0,
            retention_score FLOAT NOT NULL DEFAULT 0,
            satisfaction_score FLOAT NOT NULL DEFAULT 0,
            churn_risk FLOAT NOT NULL DEFAULT 0,
            value_score FLOAT NOT NULL DEFAULT 0,
            properties JSONB,
            created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
        )
        """)
        
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_user_profiles_distinct_id ON user_profiles(distinct_id)")
        
        # User sessions table
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS user_sessions (
            id SERIAL PRIMARY KEY,
            session_id VARCHAR(64) NOT NULL UNIQUE,
            distinct_id VARCHAR(256) NOT NULL,
            start_time TIMESTAMP NOT NULL,
            end_time TIMESTAMP,
            duration_seconds INTEGER,
            event_count INTEGER NOT NULL DEFAULT 0,
            page_view_count INTEGER NOT NULL DEFAULT 0,
            feature_used_count INTEGER NOT NULL DEFAULT 0,
            production_count INTEGER NOT NULL DEFAULT 0,
            referrer VARCHAR(512),
            utm_source VARCHAR(128),
            utm_medium VARCHAR(128),
            utm_campaign VARCHAR(128),
            browser VARCHAR(64),
            os VARCHAR(64),
            device_type VARCHAR(64),
            properties JSONB
        )
        """)
        
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_user_sessions_session_id ON user_sessions(session_id)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_user_sessions_distinct_id ON user_sessions(distinct_id)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_session_user_time ON user_sessions(distinct_id, start_time)")
        
        # Event sequences table
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS event_sequences (
            id SERIAL PRIMARY KEY,
            sequence_id VARCHAR(64) NOT NULL,
            distinct_id VARCHAR(256) NOT NULL,
            funnel_name VARCHAR(256) NOT NULL,
            step_index INTEGER NOT NULL,
            step_name VARCHAR(256) NOT NULL,
            event_name VARCHAR(256) NOT NULL,
            event_time TIMESTAMP NOT NULL,
            previous_step_time TIMESTAMP,
            next_step_time TIMESTAMP,
            is_completed BOOLEAN NOT NULL DEFAULT FALSE,
            is_converted BOOLEAN NOT NULL DEFAULT FALSE,
            time_to_next_step_seconds INTEGER,
            time_from_start_seconds INTEGER,
            properties JSONB
        )
        """)
        
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_event_sequences_sequence_id ON event_sequences(sequence_id)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_event_sequences_distinct_id ON event_sequences(distinct_id)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_sequence_funnel ON event_sequences(sequence_id, funnel_name)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_sequence_user_funnel ON event_sequences(distinct_id, funnel_name)")
        
        logger.info("Tables created successfully")
    except Exception as e:
        logger.error(f"Error creating tables: {str(e)}")
        conn.close()
        return
    
    # Generate sample data
    try:
        # Sample user IDs
        user_ids = [f"user_{i}" for i in range(1, 11)]
        
        # Sample event types
        event_types = [
            "app_open", 
            "view_content", 
            "add_to_cart", 
            "purchase",
            "create_new_project",
            "save_project",
            "share_project"
        ]
        
        # Generate events for the past 30 days
        logger.info("Generating sample events...")
        end_date = datetime.now()
        start_date = end_date - timedelta(days=30)
        current_date = start_date
        
        while current_date <= end_date:
            # For each day
            for user_id in user_ids:
                # Each user does 3-7 events per day
                num_events = random.randint(3, 7)
                
                # Always start with app_open
                event_id = str(uuid.uuid4())
                event_time = datetime.combine(
                    current_date.date(),
                    datetime.min.time()
                ) + timedelta(hours=random.randint(8, 20), minutes=random.randint(0, 59))
                
                properties = {
                    "session_id": str(uuid.uuid4()),
                    "version": "1.0.0",
                    "platform": random.choice(["web", "ios", "android"])
                }
                
                cursor.execute(
                    """
                    INSERT INTO events (
                        event_id, event_name, distinct_id, time, insert_time,
                        browser, browser_version, os, device, properties
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    (
                        event_id, "app_open", user_id, event_time, datetime.now(),
                        "Chrome", "88.0", "macOS", "Desktop", Json(properties)
                    )
                )
                
                # Additional events
                last_time = event_time
                for _ in range(1, num_events):
                    event_id = str(uuid.uuid4())
                    last_time = last_time + timedelta(minutes=random.randint(5, 30))
                    event_name = random.choice(event_types[1:])  # Skip app_open
                    
                    # Create appropriate properties based on event type
                    if event_name == "view_content":
                        properties = {
                            "content_id": str(uuid.uuid4()),
                            "category": random.choice(["music", "lyrics", "chord", "beat"]),
                            "duration": random.randint(10, 300)
                        }
                    elif event_name == "add_to_cart":
                        properties = {
                            "item_id": str(uuid.uuid4()),
                            "item_name": f"Product {random.randint(1, 100)}",
                            "price": round(random.uniform(4.99, 99.99), 2)
                        }
                    elif event_name == "purchase":
                        properties = {
                            "order_id": str(uuid.uuid4()),
                            "total": round(random.uniform(9.99, 199.99), 2),
                            "items": random.randint(1, 5)
                        }
                    elif "project" in event_name:
                        properties = {
                            "project_id": str(uuid.uuid4()),
                            "project_type": random.choice(["song", "lyrics", "chord", "beat"]),
                            "duration": random.randint(60, 240)
                        }
                    else:
                        properties = {"generic": "property"}
                    
                    cursor.execute(
                        """
                        INSERT INTO events (
                            event_id, event_name, distinct_id, time, insert_time,
                            browser, browser_version, os, device, properties
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        """,
                        (
                            event_id, event_name, user_id, last_time, datetime.now(),
                            "Chrome", "88.0", "macOS", "Desktop", Json(properties)
                        )
                    )
            
            # Move to next day
            current_date += timedelta(days=1)
        
        # Create user profiles based on events
        logger.info("Creating user profiles...")
        for user_id in user_ids:
            # Get user's first and last seen times
            cursor.execute(
                """
                SELECT MIN(time), MAX(time), COUNT(*) 
                FROM events 
                WHERE distinct_id = %s
                """, 
                (user_id,)
            )
            first_seen, last_seen, event_count = cursor.fetchone()
            
            # Calculate feature counts
            cursor.execute(
                """
                SELECT COUNT(*) FROM events 
                WHERE distinct_id = %s AND event_name LIKE %s
                """, 
                (user_id, '%project%')
            )
            feature_count = cursor.fetchone()[0]
            
            # Calculate session count (app opens)
            cursor.execute(
                """
                SELECT COUNT(*) FROM events 
                WHERE distinct_id = %s AND event_name = %s
                """, 
                (user_id, 'app_open')
            )
            session_count = cursor.fetchone()[0]
            
            # Random metrics for demonstration
            days_active = random.randint(1, 30)
            retention_score = random.uniform(10.0, 100.0)
            satisfaction_score = random.uniform(50.0, 100.0)
            churn_risk = 100.0 - retention_score
            
            # Profile properties
            properties = {
                "preferences": {
                    "theme": random.choice(["light", "dark", "auto"]),
                    "notifications": random.choice([True, False]),
                    "language": random.choice(["en", "fr", "es", "de"])
                },
                "interests": random.sample(["music", "production", "songwriting", "beats", "vocals"], 
                                          random.randint(1, 3))
            }
            
            # Insert user profile
            cursor.execute(
                """
                INSERT INTO user_profiles (
                    distinct_id, first_seen, last_seen, session_count, event_count,
                    feature_count, days_active, retention_score, satisfaction_score,
                    churn_risk, user_type, experience_level, properties
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    user_id, first_seen, last_seen, session_count, event_count,
                    feature_count, days_active, retention_score, satisfaction_score,
                    churn_risk, 
                    random.choice(["songwriter", "producer", "artist", "musician"]),
                    random.choice(["beginner", "intermediate", "professional"]),
                    Json(properties)
                )
            )
        
        # Create sessions based on app_open events
        logger.info("Creating user sessions...")
        cursor.execute(
            """
            SELECT distinct_id, time, properties 
            FROM events 
            WHERE event_name = 'app_open' 
            ORDER BY distinct_id, time
            """
        )
        app_opens = cursor.fetchall()
        
        for i, (user_id, start_time, properties) in enumerate(app_opens):
            # Session ID from properties or generate a new one
            session_id = properties.get("session_id", str(uuid.uuid4())) if properties else str(uuid.uuid4())
            
            # Find next app_open for this user to determine session end
            if i < len(app_opens) - 1 and app_opens[i+1][0] == user_id:
                end_time = app_opens[i+1][1]
            else:
                # Last session or last for this user, estimate end time
                end_time = start_time + timedelta(minutes=random.randint(10, 60))
            
            # Calculate duration
            duration = int((end_time - start_time).total_seconds())
            
            # Count events in this session
            cursor.execute(
                """
                SELECT COUNT(*), 
                       SUM(CASE WHEN event_name = 'view_content' THEN 1 ELSE 0 END),
                       SUM(CASE WHEN event_name LIKE '%project%' THEN 1 ELSE 0 END),
                       SUM(CASE WHEN event_name = 'create_new_project' THEN 1 ELSE 0 END)
                FROM events 
                WHERE distinct_id = %s AND time BETWEEN %s AND %s
                """,
                (user_id, start_time, end_time)
            )
            event_count, page_view_count, feature_used_count, production_count = cursor.fetchone()
            
            # Set nulls to 0
            page_view_count = page_view_count or 0
            feature_used_count = feature_used_count or 0
            production_count = production_count or 0
            
            # Session properties
            session_properties = {
                "device_info": {
                    "screen_size": f"{random.choice([1080, 1440, 1920])}x{random.choice([720, 900, 1080])}",
                    "connection": random.choice(["wifi", "cellular", "ethernet"])
                },
                "exit_page": random.choice(["home", "profile", "content", "checkout"])
            }
            
            # Insert session
            cursor.execute(
                """
                INSERT INTO user_sessions (
                    session_id, distinct_id, start_time, end_time, duration_seconds,
                    event_count, page_view_count, feature_used_count, production_count,
                    browser, os, device_type, properties
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    session_id, user_id, start_time, end_time, duration,
                    event_count, page_view_count, feature_used_count, production_count,
                    "Chrome", "macOS", "Desktop", Json(session_properties)
                )
            )
        
        # Create sample funnel sequences
        logger.info("Creating funnel sequences...")
        purchase_funnel = ["app_open", "view_content", "add_to_cart", "purchase"]
        project_funnel = ["app_open", "create_new_project", "save_project", "share_project"]
        
        # Process purchase funnel
        cursor.execute(
            """
            SELECT DISTINCT distinct_id FROM events 
            WHERE event_name = 'app_open'
            """
        )
        users = [row[0] for row in cursor.fetchall()]
        
        for user_id in users:
            # Create a purchase funnel sequence
            sequence_id = str(uuid.uuid4())
            funnel_name = "purchase_funnel"
            
            # Get user's relevant events
            cursor.execute(
                """
                SELECT event_name, time 
                FROM events 
                WHERE distinct_id = %s 
                AND event_name IN %s
                ORDER BY time
                """,
                (user_id, tuple(purchase_funnel))
            )
            user_events = cursor.fetchall()
            
            # Skip if no events
            if not user_events:
                continue
            
            # Find sequences in user events
            event_dict = {}
            for event_name, event_time in user_events:
                if event_name not in event_dict:
                    event_dict[event_name] = []
                event_dict[event_name].append(event_time)
            
            # Check if user completed at least some steps
            steps_completed = [step for step in purchase_funnel if step in event_dict]
            if len(steps_completed) <= 1:
                continue
            
            # Create sequence with the steps the user actually completed
            prev_time = None
            is_converted = "purchase" in event_dict
            
            for i, step_name in enumerate(purchase_funnel):
                if step_name not in event_dict:
                    # User didn't complete this step
                    continue
                
                # Use the first occurrence of this event
                event_time = event_dict[step_name][0]
                
                # Calculate time metrics
                time_from_start = None
                if i > 0 and prev_time:
                    time_from_start = int((event_time - prev_time).total_seconds())
                
                # Insert sequence step
                cursor.execute(
                    """
                    INSERT INTO event_sequences (
                        sequence_id, distinct_id, funnel_name, step_index, step_name,
                        event_name, event_time, previous_step_time, is_completed,
                        is_converted, time_from_start_seconds
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    (
                        sequence_id, user_id, funnel_name, i, step_name,
                        step_name, event_time, prev_time, True,
                        is_converted, time_from_start
                    )
                )
                
                prev_time = event_time
            
            # Process project funnel similarly
            sequence_id = str(uuid.uuid4())
            funnel_name = "project_funnel"
            
            # Get user's relevant events
            cursor.execute(
                """
                SELECT event_name, time 
                FROM events 
                WHERE distinct_id = %s 
                AND event_name IN %s
                ORDER BY time
                """,
                (user_id, tuple(project_funnel))
            )
            user_events = cursor.fetchall()
            
            # Skip if no events
            if not user_events:
                continue
            
            # Find sequences in user events
            event_dict = {}
            for event_name, event_time in user_events:
                if event_name not in event_dict:
                    event_dict[event_name] = []
                event_dict[event_name].append(event_time)
            
            # Check if user completed at least some steps
            steps_completed = [step for step in project_funnel if step in event_dict]
            if len(steps_completed) <= 1:
                continue
            
            # Create sequence with the steps the user actually completed
            prev_time = None
            is_converted = "share_project" in event_dict
            
            for i, step_name in enumerate(project_funnel):
                if step_name not in event_dict:
                    # User didn't complete this step
                    continue
                
                # Use the first occurrence of this event
                event_time = event_dict[step_name][0]
                
                # Calculate time metrics
                time_from_start = None
                if i > 0 and prev_time:
                    time_from_start = int((event_time - prev_time).total_seconds())
                
                # Insert sequence step
                cursor.execute(
                    """
                    INSERT INTO event_sequences (
                        sequence_id, distinct_id, funnel_name, step_index, step_name,
                        event_name, event_time, previous_step_time, is_completed,
                        is_converted, time_from_start_seconds
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    (
                        sequence_id, user_id, funnel_name, i, step_name,
                        step_name, event_time, prev_time, True,
                        is_converted, time_from_start
                    )
                )
                
                prev_time = event_time
        
        # Get counts of data loaded
        cursor.execute("SELECT COUNT(*) FROM events")
        event_count = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM user_profiles")
        profile_count = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM user_sessions")
        session_count = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM event_sequences")
        sequence_count = cursor.fetchone()[0]
        
        logger.info(f"Sample data loaded: {event_count} events, {profile_count} profiles, {session_count} sessions, {sequence_count} funnel steps")
        
    except Exception as e:
        logger.error(f"Error loading sample data: {str(e)}")
    
    # Close the connection
    conn.close()
    logger.info("Database setup complete!")

if __name__ == "__main__":
    setup_database()
