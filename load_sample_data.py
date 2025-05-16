#!/usr/bin/env python3
"""
Load Sample Data Script for HitCraft Analytics

This script loads sample event data into the database to test the analytics engine with real data
instead of using fallback sample data in the AnthropicClient.
"""

import uuid
import random
from datetime import datetime, timedelta
import logging

from hitcraft_analytics.data.connectors.database_connector import DatabaseConnector
from hitcraft_analytics.utils.logging.logger import get_logger

# Set up logger
logger = get_logger("load_sample_data")

# Sample event types
EVENT_TYPES = [
    "app_open",
    "view_content",
    "create_new_project",
    "save_project",
    "share_project",
    "view_signup_form",
    "start_registration",
    "complete_registration",
    "verify_email",
    "complete_profile",
    "add_to_cart",
    "checkout_start",
    "purchase",
    "visit_landing_page"
]

# Sample distinct IDs (user IDs)
DISTINCT_IDS = [
    f"user_{i}" for i in range(1, 101)  # 100 sample users
]

# Sample browsers
BROWSERS = ["Chrome", "Firefox", "Safari", "Edge"]
BROWSER_VERSIONS = ["76.0", "88.0", "15.0", "18.0"]

# Sample operating systems
OPERATING_SYSTEMS = ["Windows", "macOS", "iOS", "Android"]

# Sample devices
DEVICES = ["Desktop", "Laptop", "iPhone", "Android Phone", "Tablet"]

# Sample cities
CITIES = ["San Francisco", "New York", "London", "Tokyo", "Berlin", "Paris"]

# Sample countries
COUNTRIES = ["US", "UK", "JP", "DE", "FR", "CA"]


def create_sample_event(event_name, distinct_id, timestamp):
    """Create a sample event record with realistic properties."""
    # Generate basic event data
    event = {
        "event_id": str(uuid.uuid4()),
        "event_name": event_name,
        "distinct_id": distinct_id,
        "time": timestamp,
        "insert_time": datetime.utcnow(),
        "browser": random.choice(BROWSERS),
        "browser_version": random.choice(BROWSER_VERSIONS),
        "os": random.choice(OPERATING_SYSTEMS),
        "device": random.choice(DEVICES),
        "city": random.choice(CITIES),
        "country_code": random.choice(COUNTRIES),
        "screen_height": random.choice([768, 900, 1080, 1440]),
        "screen_width": random.choice([1366, 1440, 1920, 2560])
    }
    
    # Add some event-specific properties as JSON
    properties = {}
    
    if event_name == "app_open":
        properties["session_id"] = str(uuid.uuid4())
        properties["app_version"] = f"{random.randint(1, 3)}.{random.randint(0, 9)}.{random.randint(0, 9)}"
        properties["login_method"] = random.choice(["email", "google", "apple", "guest"])
        
    elif event_name == "view_content":
        properties["content_id"] = str(uuid.uuid4())
        properties["content_type"] = random.choice(["song", "lyrics", "chord", "beat"])
        properties["content_category"] = random.choice(["pop", "rock", "hip-hop", "electronic"])
        properties["duration"] = random.randint(10, 300)
        
    elif "project" in event_name:
        properties["project_id"] = str(uuid.uuid4())
        properties["project_type"] = random.choice(["song", "lyrics", "chord", "beat"])
        properties["track_count"] = random.randint(1, 8)
        properties["duration"] = random.randint(60, 240)
        
    elif "registration" in event_name or "signup" in event_name or "profile" in event_name:
        properties["method"] = random.choice(["email", "google", "apple"])
        properties["user_type"] = random.choice(["songwriter", "producer", "artist", "musician"])
        properties["experience_level"] = random.choice(["beginner", "intermediate", "professional"])
        
    elif "cart" in event_name or "checkout" in event_name or "purchase" in event_name:
        properties["item_count"] = random.randint(1, 5)
        properties["total_amount"] = round(random.uniform(4.99, 99.99), 2)
        properties["currency"] = "USD"
        properties["payment_method"] = random.choice(["credit_card", "paypal", "apple_pay", "google_pay"])
    
    # Add properties to the event
    event["properties"] = properties
    
    return event


def generate_user_session(distinct_id, start_time, session_duration=30):
    """Generate a set of events representing a coherent user session."""
    session_events = []
    current_time = start_time
    
    # Start with app_open
    session_events.append(create_sample_event("app_open", distinct_id, current_time))
    
    # Generate 3-10 events per session
    num_events = random.randint(3, 10)
    for _ in range(num_events):
        current_time += timedelta(minutes=random.randint(1, 5))
        event_name = random.choice(EVENT_TYPES[1:])  # Exclude app_open
        session_events.append(create_sample_event(event_name, distinct_id, current_time))
    
    return session_events


def generate_sample_data(num_days=30, events_per_day=100):
    """Generate sample event data for the specified number of days."""
    all_events = []
    end_date = datetime.now()
    start_date = end_date - timedelta(days=num_days)
    
    current_date = start_date
    
    while current_date <= end_date:
        # Generate events for this day
        day_events_count = random.randint(max(1, events_per_day - 20), events_per_day + 20)
        
        for _ in range(day_events_count):
            # Select a random user
            distinct_id = random.choice(DISTINCT_IDS)
            
            # Generate a random time during the day
            hour = random.randint(0, 23)
            minute = random.randint(0, 59)
            second = random.randint(0, 59)
            event_time = datetime(current_date.year, current_date.month, current_date.day, 
                                hour, minute, second)
            
            # Generate a session of events
            session_events = generate_user_session(distinct_id, event_time)
            all_events.extend(session_events)
        
        current_date += timedelta(days=1)
    
    return all_events


def load_data_to_database(events):
    """Load the generated events into the database."""
    db = DatabaseConnector()
    
    # Split events into batches to avoid memory issues
    batch_size = 100
    total_loaded = 0
    
    for i in range(0, len(events), batch_size):
        batch = events[i:i+batch_size]
        try:
            rows_inserted = db.bulk_insert("events", batch)
            total_loaded += rows_inserted
            logger.info(f"Loaded batch {i//batch_size + 1}: {rows_inserted} events")
        except Exception as e:
            logger.error(f"Error loading batch {i//batch_size + 1}: {str(e)}")
    
    return total_loaded


def create_user_sessions(events):
    """Create user session records from the event data."""
    # Group events by user ID
    sessions_by_user = {}
    
    for event in events:
        distinct_id = event["distinct_id"]
        event_time = event["time"]
        
        if distinct_id not in sessions_by_user:
            sessions_by_user[distinct_id] = []
        
        # Check if this is an app_open event (session start)
        if event["event_name"] == "app_open":
            # Create a new session
            session = {
                "session_id": event.get("properties", {}).get("session_id", str(uuid.uuid4())),
                "distinct_id": distinct_id,
                "start_time": event_time,
                "events": [event]
            }
            sessions_by_user[distinct_id].append(session)
        else:
            # Add to the last session if it exists
            if sessions_by_user[distinct_id]:
                sessions_by_user[distinct_id][-1]["events"].append(event)
    
    # Process sessions to create session records
    session_records = []
    
    for user_sessions in sessions_by_user.values():
        for session in user_sessions:
            if not session["events"]:
                continue
                
            # Sort events by time
            sorted_events = sorted(session["events"], key=lambda e: e["time"])
            
            # Get session end time (time of last event)
            end_time = sorted_events[-1]["time"]
            
            # Create session record
            session_record = {
                "session_id": session["session_id"],
                "distinct_id": session["distinct_id"],
                "start_time": session["start_time"],
                "end_time": end_time,
                "duration_seconds": int((end_time - session["start_time"]).total_seconds()),
                "event_count": len(session["events"]),
                "page_view_count": sum(1 for e in session["events"] if e["event_name"] in ["view_content", "visit_landing_page"]),
                "feature_used_count": sum(1 for e in session["events"] if "project" in e["event_name"]),
                "production_count": sum(1 for e in session["events"] if e["event_name"] == "create_new_project"),
                "browser": sorted_events[0]["browser"],
                "os": sorted_events[0]["os"],
                "device_type": sorted_events[0]["device"],
                "properties": {
                    "first_event": sorted_events[0]["event_name"],
                    "last_event": sorted_events[-1]["event_name"]
                }
            }
            
            session_records.append(session_record)
    
    return session_records


def load_sessions_to_database(sessions):
    """Load session records into the database."""
    db = DatabaseConnector()
    
    # Split sessions into batches
    batch_size = 50
    total_loaded = 0
    
    for i in range(0, len(sessions), batch_size):
        batch = sessions[i:i+batch_size]
        try:
            rows_inserted = db.bulk_insert("user_sessions", batch)
            total_loaded += rows_inserted
            logger.info(f"Loaded session batch {i//batch_size + 1}: {rows_inserted} sessions")
        except Exception as e:
            logger.error(f"Error loading session batch {i//batch_size + 1}: {str(e)}")
    
    return total_loaded


def create_user_profiles(events):
    """Create user profile records from the event data."""
    profiles = {}
    
    for event in events:
        distinct_id = event["distinct_id"]
        event_time = event["time"]
        
        if distinct_id not in profiles:
            # Initialize new profile
            profiles[distinct_id] = {
                "distinct_id": distinct_id,
                "first_seen": event_time,
                "last_seen": event_time,
                "session_count": 0,
                "event_count": 1,
                "feature_count": 0,
                "production_count": 0,
                "sketch_count": 0,
                "days_active": 1
            }
        else:
            # Update profile
            profile = profiles[distinct_id]
            profile["event_count"] += 1
            
            # Update first/last seen
            profile["first_seen"] = min(profile["first_seen"], event_time)
            profile["last_seen"] = max(profile["last_seen"], event_time)
        
        # Update feature counts
        if "project" in event["event_name"]:
            profiles[distinct_id]["feature_count"] += 1
            
        if event["event_name"] == "create_new_project":
            profiles[distinct_id]["production_count"] += 1
            
        if event["event_name"] == "save_project":
            profiles[distinct_id]["sketch_count"] += 1
            
        # Extract user properties from registration events
        if "registration" in event["event_name"] or "profile" in event["event_name"]:
            props = event.get("properties", {})
            
            if "user_type" in props:
                profiles[distinct_id]["user_type"] = props["user_type"]
                
            if "experience_level" in props:
                profiles[distinct_id]["experience_level"] = props["experience_level"]
    
    # Calculate active days for each user
    for distinct_id, profile in profiles.items():
        # Get days between first and last seen
        days_between = (profile["last_seen"] - profile["first_seen"]).days + 1
        
        # Simulate activity on random days
        active_days = min(days_between, random.randint(1, days_between))
        profile["days_active"] = active_days
        
        # Calculate retention score (days active / days between)
        if days_between > 0:
            profile["retention_score"] = (active_days / days_between) * 100
        else:
            profile["retention_score"] = 100
            
        # Add random satisfaction score
        profile["satisfaction_score"] = random.uniform(50, 100)
        
        # Add churn risk (inverse of retention)
        profile["churn_risk"] = max(0, 100 - profile["retention_score"])
        
        # Add value score
        profile["value_score"] = (
            profile["days_active"] * 0.4 + 
            profile["event_count"] * 0.1 + 
            profile["feature_count"] * 0.3 + 
            profile["production_count"] * 0.2
        )
        
        # Count sessions (app opens)
        profile["session_count"] = sum(1 for e in events if e["distinct_id"] == distinct_id and e["event_name"] == "app_open")
    
    # Convert to list of records
    profile_records = list(profiles.values())
    
    return profile_records


def load_profiles_to_database(profiles):
    """Load profile records into the database."""
    db = DatabaseConnector()
    
    # Split profiles into batches
    batch_size = 50
    total_loaded = 0
    
    for i in range(0, len(profiles), batch_size):
        batch = profiles[i:i+batch_size]
        try:
            rows_inserted = db.bulk_insert("user_profiles", batch)
            total_loaded += rows_inserted
            logger.info(f"Loaded profile batch {i//batch_size + 1}: {rows_inserted} profiles")
        except Exception as e:
            logger.error(f"Error loading profile batch {i//batch_size + 1}: {str(e)}")
    
    return total_loaded


def main():
    """Main function to generate and load sample data."""
    logger.info("Starting sample data generation...")
    
    # Generate sample events for the last 30 days
    events = generate_sample_data(num_days=30, events_per_day=100)
    logger.info(f"Generated {len(events)} sample events")
    
    # Load events into the database
    events_loaded = load_data_to_database(events)
    logger.info(f"Loaded {events_loaded} events into the database")
    
    # Create and load user sessions
    sessions = create_user_sessions(events)
    sessions_loaded = load_sessions_to_database(sessions)
    logger.info(f"Loaded {sessions_loaded} user sessions into the database")
    
    # Create and load user profiles
    profiles = create_user_profiles(events)
    profiles_loaded = load_profiles_to_database(profiles)
    logger.info(f"Loaded {profiles_loaded} user profiles into the database")
    
    logger.info("Sample data generation complete!")


if __name__ == "__main__":
    main()
