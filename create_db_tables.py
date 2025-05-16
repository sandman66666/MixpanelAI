#!/usr/bin/env python3
"""
Create Database Tables Script

This script creates the necessary tables in the PostgreSQL database using direct SQL queries
to ensure they are properly created.
"""

from hitcraft_analytics.data.connectors.database_connector import DatabaseConnector
from hitcraft_analytics.utils.logging.logger import get_logger

# Set up logger
logger = get_logger("create_db_tables")

def create_tables():
    """Create necessary database tables."""
    # Get database connector
    db = DatabaseConnector()
    
    # Create events table
    events_table_sql = """
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
    );
    
    CREATE INDEX IF NOT EXISTS idx_events_event_id ON events(event_id);
    CREATE INDEX IF NOT EXISTS idx_events_event_name ON events(event_name);
    CREATE INDEX IF NOT EXISTS idx_events_distinct_id ON events(distinct_id);
    CREATE INDEX IF NOT EXISTS idx_events_time ON events(time);
    CREATE INDEX IF NOT EXISTS idx_events_time_event ON events(time, event_name);
    CREATE INDEX IF NOT EXISTS idx_events_user_time ON events(distinct_id, time);
    CREATE INDEX IF NOT EXISTS idx_events_event_user ON events(event_name, distinct_id);
    """
    
    # Create user_profiles table
    user_profiles_sql = """
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
    );
    
    CREATE INDEX IF NOT EXISTS idx_user_profiles_distinct_id ON user_profiles(distinct_id);
    """
    
    # Create user_sessions table
    user_sessions_sql = """
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
    );
    
    CREATE INDEX IF NOT EXISTS idx_user_sessions_session_id ON user_sessions(session_id);
    CREATE INDEX IF NOT EXISTS idx_user_sessions_distinct_id ON user_sessions(distinct_id);
    CREATE INDEX IF NOT EXISTS idx_session_user_time ON user_sessions(distinct_id, start_time);
    """
    
    # Create event_sequences table
    event_sequences_sql = """
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
    );
    
    CREATE INDEX IF NOT EXISTS idx_event_sequences_sequence_id ON event_sequences(sequence_id);
    CREATE INDEX IF NOT EXISTS idx_event_sequences_distinct_id ON event_sequences(distinct_id);
    CREATE INDEX IF NOT EXISTS idx_sequence_funnel ON event_sequences(sequence_id, funnel_name);
    CREATE INDEX IF NOT EXISTS idx_sequence_user_funnel ON event_sequences(distinct_id, funnel_name);
    """
    
    # Execute SQL to create tables
    try:
        logger.info("Creating events table...")
        db.execute_query(events_table_sql)
        logger.info("Events table created successfully")
        
        logger.info("Creating user_profiles table...")
        db.execute_query(user_profiles_sql)
        logger.info("User profiles table created successfully")
        
        logger.info("Creating user_sessions table...")
        db.execute_query(user_sessions_sql)
        logger.info("User sessions table created successfully")
        
        logger.info("Creating event_sequences table...")
        db.execute_query(event_sequences_sql)
        logger.info("Event sequences table created successfully")
        
        logger.info("All tables created successfully!")
        
    except Exception as e:
        logger.error(f"Error creating tables: {str(e)}")
        raise

if __name__ == "__main__":
    create_tables()
