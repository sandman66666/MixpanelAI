"""
Test Mixpanel integration via the tracking API.
"""

import os
import sys
import json
import base64
import requests
import time
from datetime import datetime
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Get Mixpanel credentials from environment variables
PROJECT_TOKEN = os.getenv("HITCRAFT_MIXPANEL_PROJECT_ID")

print(f"Using Project Token: {PROJECT_TOKEN}")

def test_tracking_api():
    """Test Mixpanel Tracking API"""
    print("\n=== Testing Mixpanel Tracking API ===")
    
    # Create a test event
    event_data = {
        "event": "Test Event",
        "properties": {
            "token": PROJECT_TOKEN,
            "distinct_id": "test_user_id",
            "time": int(time.time()),
            "test_property": "test_value",
            "$insert_id": f"test_{int(time.time())}"  # Prevent duplicates
        }
    }
    
    # Encode the data
    data = json.dumps(event_data)
    encoded_data = base64.b64encode(data.encode()).decode()
    
    # Make request to track endpoint
    url = "https://api.mixpanel.com/track"
    params = {
        "data": encoded_data,
        "verbose": 1  # Get detailed response
    }
    
    print(f"Making request to: {url}")
    print(f"With event: {event_data['event']}")
    
    try:
        response = requests.get(url, params=params)
        response.raise_for_status()
        
        result = response.json()
        print(f"Response: {result}")
        
        # Check if the event was tracked successfully
        if result.get("status") == 1:
            print("Event tracked successfully!")
            return True
        else:
            print(f"Event tracking failed: {result.get('error')}")
            return False
    except requests.exceptions.HTTPError as e:
        print(f"HTTP error: {e}")
        print(f"Response: {e.response.text}")
        return False
    except Exception as e:
        print(f"Error: {str(e)}")
        return False

if __name__ == "__main__":
    if not PROJECT_TOKEN:
        print("Error: Mixpanel project token not found in .env file")
        print("Please ensure HITCRAFT_MIXPANEL_PROJECT_ID is set")
        sys.exit(1)
    
    success = test_tracking_api()
    print(f"\nTest result: {'PASSED' if success else 'FAILED'}")
