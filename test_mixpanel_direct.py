#!/usr/bin/env python3

"""
Direct Mixpanel API Test

This script tests direct connection to Mixpanel's API using the provided
project key and API secret to retrieve event counts.
"""

import os
import json
import requests
from datetime import datetime, timedelta

# Mixpanel credentials from user
PROJECT_KEY = "3bbc24764765962cb8af4c45ac04ae4d"  # This is the project token/ID
API_SECRET = "9a685163559ec32b97c7d89a4adebafc"

# Set date range - past 6 months 
end_date = datetime.now()
start_date = end_date - timedelta(days=180)

from_date = start_date.strftime("%Y-%m-%d")
to_date = end_date.strftime("%Y-%m-%d") 

print(f"Testing Mixpanel API connection")
print(f"Project key: {PROJECT_KEY}")
print(f"API secret: {API_SECRET[:4]}...{API_SECRET[-4:]}")
print(f"Date range: {from_date} to {to_date}")

# Try to get event counts
try:
    # For authentication with the API
    auth = (API_SECRET, "")
    
    # Parameters for the events/count API
    url = "https://data.mixpanel.com/api/2.0/export"
    params = {
        "from_date": from_date,
        "to_date": to_date,
    }
    
    # Make the request
    print("Fetching event counts from Mixpanel API...")
    response = requests.get(
        url,
        auth=auth,
        params=params,
    )
    
    # Check for errors
    response.raise_for_status()
    
    # Count the number of events by parsing the newline-delimited JSON response
    events = []
    for line in response.text.strip().split("\n"):
        if line:  # Skip empty lines
            try:
                events.append(json.loads(line))
            except json.JSONDecodeError as e:
                print(f"Error parsing JSON response: {str(e)}")
    
    print(f"Total events found: {len(events)}")
    
    # Sample a few events to show
    if events:
        print("\nSample event types:")
        event_types = {}
        for event in events[:1000]:  # Check first 1000 events
            event_type = event.get("event", "unknown")
            if event_type in event_types:
                event_types[event_type] += 1
            else:
                event_types[event_type] = 1
        
        # Show top 10 event types
        print("Top event types:")
        for event_type, count in sorted(event_types.items(), key=lambda x: x[1], reverse=True)[:10]:
            print(f"  - {event_type}: {count} events")
    
except Exception as e:
    print(f"Error connecting to Mixpanel API: {str(e)}")
