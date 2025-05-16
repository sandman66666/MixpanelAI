"""
HitCraft AI Analytics Engine - Event Data Schemas

This module defines the database schemas for storing event data from Mixpanel.
"""

from datetime import datetime
from typing import Dict, List, Optional, Any

from sqlalchemy import Column, Integer, String, Float, DateTime, JSON, Boolean, ForeignKey, Index
from sqlalchemy.ext.declarative import declarative_base

from hitcraft_analytics.data.connectors.database_connector import Base


class Event(Base):
    """
    Schema for storing Mixpanel events.
    """
    __tablename__ = "events"
    
    # Primary key
    id = Column(Integer, primary_key=True, autoincrement=True)
    
    # Event identification
    event_id = Column(String(64), nullable=False, index=True)
    event_name = Column(String(256), nullable=False, index=True)
    
    # User identification
    distinct_id = Column(String(256), nullable=False, index=True)
    
    # Time information
    time = Column(DateTime, nullable=False, index=True)
    insert_time = Column(DateTime, default=datetime.utcnow, nullable=False)
    
    # Device and location
    browser = Column(String(64), nullable=True)
    browser_version = Column(String(64), nullable=True)
    os = Column(String(64), nullable=True)
    device = Column(String(64), nullable=True)
    city = Column(String(64), nullable=True)
    country_code = Column(String(8), nullable=True)
    
    # Referrer information
    referrer = Column(String(512), nullable=True)
    referring_domain = Column(String(256), nullable=True)
    
    # Screen information
    screen_height = Column(Integer, nullable=True)
    screen_width = Column(Integer, nullable=True)
    
    # Additional properties
    properties = Column(JSON, nullable=True)
    
    # Create indexes for common queries
    __table_args__ = (
        Index("idx_events_time_event", time, event_name),
        Index("idx_events_user_time", distinct_id, time),
        Index("idx_events_event_user", event_name, distinct_id),
    )
    
    def __repr__(self):
        return f"<Event(event_name='{self.event_name}', distinct_id='{self.distinct_id}', time='{self.time}')>"


class UserProfile(Base):
    """
    Schema for storing user profile data.
    """
    __tablename__ = "user_profiles"
    
    # Primary key
    id = Column(Integer, primary_key=True, autoincrement=True)
    
    # User identification
    distinct_id = Column(String(256), nullable=False, unique=True, index=True)
    
    # User attributes
    email = Column(String(256), nullable=True)
    first_name = Column(String(128), nullable=True)
    last_name = Column(String(128), nullable=True)
    role = Column(String(64), nullable=True)
    experience_level = Column(String(64), nullable=True)
    
    # User categorization
    user_type = Column(String(64), nullable=True)
    user_segment = Column(String(64), nullable=True)
    
    # Activity metrics
    first_seen = Column(DateTime, nullable=True)
    last_seen = Column(DateTime, nullable=True)
    session_count = Column(Integer, default=0, nullable=False)
    event_count = Column(Integer, default=0, nullable=False)
    
    # Product usage
    feature_count = Column(Integer, default=0, nullable=False)
    production_count = Column(Integer, default=0, nullable=False)
    sketch_count = Column(Integer, default=0, nullable=False)
    
    # Engagement metrics
    days_active = Column(Integer, default=0, nullable=False)
    retention_score = Column(Float, default=0, nullable=False)
    satisfaction_score = Column(Float, default=0, nullable=False)
    
    # Risk assessment
    churn_risk = Column(Float, default=0, nullable=False)
    value_score = Column(Float, default=0, nullable=False)
    
    # Additional data
    properties = Column(JSON, nullable=True)
    
    # Timestamps
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)
    
    def __repr__(self):
        return f"<UserProfile(distinct_id='{self.distinct_id}', user_type='{self.user_type}')>"


class UserSession(Base):
    """
    Schema for storing user session data.
    """
    __tablename__ = "user_sessions"
    
    # Primary key
    id = Column(Integer, primary_key=True, autoincrement=True)
    
    # Session identification
    session_id = Column(String(64), nullable=False, unique=True, index=True)
    
    # User identification
    distinct_id = Column(String(256), nullable=False, index=True)
    
    # Session timing
    start_time = Column(DateTime, nullable=False, index=True)
    end_time = Column(DateTime, nullable=True)
    duration_seconds = Column(Integer, nullable=True)
    
    # Session activity
    event_count = Column(Integer, default=0, nullable=False)
    page_view_count = Column(Integer, default=0, nullable=False)
    feature_used_count = Column(Integer, default=0, nullable=False)
    production_count = Column(Integer, default=0, nullable=False)
    
    # Session origin
    referrer = Column(String(512), nullable=True)
    utm_source = Column(String(128), nullable=True)
    utm_medium = Column(String(128), nullable=True)
    utm_campaign = Column(String(128), nullable=True)
    
    # Device information
    browser = Column(String(64), nullable=True)
    os = Column(String(64), nullable=True)
    device_type = Column(String(64), nullable=True)
    
    # Additional data
    properties = Column(JSON, nullable=True)
    
    # Create indexes for common queries
    __table_args__ = (
        Index("idx_session_user_time", distinct_id, start_time),
    )
    
    def __repr__(self):
        return f"<UserSession(session_id='{self.session_id}', distinct_id='{self.distinct_id}', duration_seconds={self.duration_seconds})>"


class EventSequence(Base):
    """
    Schema for storing sequences of events for funnel analysis.
    """
    __tablename__ = "event_sequences"
    
    # Primary key
    id = Column(Integer, primary_key=True, autoincrement=True)
    
    # Sequence identification
    sequence_id = Column(String(64), nullable=False, index=True)
    
    # User identification
    distinct_id = Column(String(256), nullable=False, index=True)
    
    # Sequence details
    funnel_name = Column(String(256), nullable=False, index=True)
    step_index = Column(Integer, nullable=False)
    step_name = Column(String(256), nullable=False)
    event_name = Column(String(256), nullable=False)
    
    # Timing information
    event_time = Column(DateTime, nullable=False)
    previous_step_time = Column(DateTime, nullable=True)
    next_step_time = Column(DateTime, nullable=True)
    
    # Completion status
    is_completed = Column(Boolean, default=False, nullable=False)
    is_converted = Column(Boolean, default=False, nullable=False)
    
    # Time metrics
    time_to_next_step_seconds = Column(Integer, nullable=True)
    time_from_start_seconds = Column(Integer, nullable=True)
    
    # Additional data
    properties = Column(JSON, nullable=True)
    
    # Create indexes for common queries
    __table_args__ = (
        Index("idx_sequence_funnel", sequence_id, funnel_name),
        Index("idx_sequence_user_funnel", distinct_id, funnel_name),
    )
    
    def __repr__(self):
        return f"<EventSequence(funnel_name='{self.funnel_name}', step_name='{self.step_name}', is_converted={self.is_converted})>"
