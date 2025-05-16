"""
Test script to verify Mixpanel API integration using the provided credentials.
"""

import os
import sys
import json
import base64
import requests
from datetime import datetime, timedelta
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Get Mixpanel credentials from environment variables
PROJECT_ID = os.getenv("HITCRAFT_MIXPANEL_PROJECT_ID")
API_SECRET = os.getenv("HITCRAFT_MIXPANEL_API_SECRET")

print(f"Using Project ID: {PROJECT_ID}")
print(f"API Secret: {'*' * len(API_SECRET) if API_SECRET else 'Not found'}")

def test_mixpanel_auth():
    """Test authentication with Mixpanel API"""
    print("\n=== Testing Mixpanel Authentication ===")
    
    # Base64 encode API secret for basic auth
    auth_string = f"{API_SECRET}:"
    encoded_auth = base64.b64encode(auth_string.encode()).decode()
    
    headers = {
        "Accept": "application/json",
        "Authorization": f"Basic {encoded_auth}"
    }
    
    # Try to access a simple endpoint to test authentication
    url = "https://mixpanel.com/api/2.0/events"
    
    params = {
        "project_id": PROJECT_ID,
        "from_date": (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d"),
        "to_date": datetime.now().strftime("%Y-%m-%d"),
    }
    
    print(f"Making request to: {url}")
    print(f"With params: {params}")
    
    try:
        response = requests.get(url, headers=headers, params=params)
        response.raise_for_status()  # Raise exception for 4XX/5XX responses
        
        print(f"Authentication successful! Status code: {response.status_code}")
        return True
    except requests.exceptions.HTTPError as e:
        print(f"Authentication failed: {e}")
        print(f"Response: {e.response.text}")
        return False
    except Exception as e:
        print(f"Error: {str(e)}")
        return False

def get_event_names():
    """Get list of event names from Mixpanel"""
    print("\n=== Getting Event Names ===")
    
    # Base64 encode API secret for basic auth
    auth_string = f"{API_SECRET}:"
    encoded_auth = base64.b64encode(auth_string.encode()).decode()
    
    headers = {
        "Accept": "application/json",
        "Authorization": f"Basic {encoded_auth}"
    }
    
    # Make request to events/names endpoint
    url = "https://mixpanel.com/api/2.0/events/names"
    
    params = {
        "project_id": PROJECT_ID,
        "from_date": (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d"),
        "to_date": datetime.now().strftime("%Y-%m-%d"),
    }
    
    try:
        response = requests.get(url, headers=headers, params=params)
        response.raise_for_status()
        
        event_names = response.json()
        print(f"Found {len(event_names)} event types:")
        for event in event_names:
            print(f" - {event}")
        
        return event_names
    except requests.exceptions.HTTPError as e:
        print(f"Error getting event names: {e}")
        print(f"Response: {e.response.text}")
        return []
    except Exception as e:
        print(f"Error: {str(e)}")
        return []

def get_event_data():
    """Get sample event data from Mixpanel"""
    print("\n=== Getting Sample Event Data ===")
    
    # Base64 encode API secret for basic auth
    auth_string = f"{API_SECRET}:"
    encoded_auth = base64.b64encode(auth_string.encode()).decode()
    
    headers = {
        "Accept": "application/json",
        "Authorization": f"Basic {encoded_auth}"
    }
    
    # Make request to events endpoint
    url = "https://mixpanel.com/api/2.0/export"
    
    params = {
        "project_id": PROJECT_ID,
        "from_date": (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d"),
        "to_date": datetime.now().strftime("%Y-%m-%d"),
        "limit": 10  # Only get a few events
    }
    
    try:
        response = requests.get(url, headers=headers, params=params)
        response.raise_for_status()
        
        # Export endpoint returns new-line delimited JSON
        events = []
        for line in response.text.strip().split("\n"):
            if line:  # Skip empty lines
                events.append(json.loads(line))
        
        print(f"Retrieved {len(events)} events")
        
        # Print sample event (with sensitive data masked)
        if events:
            print("\nSample event structure:")
            sample = events[0]
            
            # Mask potential PII/sensitive data
            if "properties" in sample:
                for key in ["distinct_id", "$email", "email", "name", "user_id"]:
                    if key in sample["properties"]:
                        sample["properties"][key] = f"[MASKED {key}]"
            
            print(json.dumps(sample, indent=2))
        
        return events
    except requests.exceptions.HTTPError as e:
        print(f"Error getting event data: {e}")
        print(f"Response: {e.response.text}")
        return []
    except Exception as e:
        print(f"Error: {str(e)}")
        return []

def run_mixpanel_tests():
    """Run all Mixpanel integration tests"""
    print("=================================================")
    print("Testing Mixpanel API Integration")
    print("=================================================")
    
    # Test authentication
    auth_success = test_mixpanel_auth()
    
    if auth_success:
        # Get event names
        event_names = get_event_names()
        
        # Get sample event data
        events = get_event_data()
        
        success = auth_success and len(event_names) > 0 and len(events) > 0
    else:
        success = False
    
    print("\n=================================================")
    print(f"Mixpanel Integration Test Result: {'PASSED' if success else 'FAILED'}")
    print("=================================================")
    
    return success

if __name__ == "__main__":
    if not PROJECT_ID or not API_SECRET:
        print("Error: Mixpanel credentials not found in .env file")
        print("Please ensure HITCRAFT_MIXPANEL_PROJECT_ID and HITCRAFT_MIXPANEL_API_SECRET are set")
        sys.exit(1)
    
    run_mixpanel_tests()
