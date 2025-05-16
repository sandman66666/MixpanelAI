"""
Test Mixpanel data export API using the confirmed working approach.
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
API_SECRET = os.getenv("HITCRAFT_MIXPANEL_API_SECRET")

print(f"Using API Secret: {'*' * len(API_SECRET) if API_SECRET else 'Not found'}")

def test_mixpanel_export():
    """Test Mixpanel Data Export API"""
    print("\n=== Testing Mixpanel Data Export API ===")
    
    # Create authentication
    auth = (API_SECRET, '')  # API secret as username, empty password
    
    # Try with a very broad date range to catch any data
    from_date = "2020-01-01"
    to_date = "2023-12-31"
    
    # Make request to export endpoint
    url = f"https://data.mixpanel.com/api/2.0/export/?from_date={from_date}&to_date={to_date}"
    
    print(f"Making request to: {url}")
    print(f"Date range: {from_date} to {to_date}")
    
    try:
        response = requests.get(url, auth=auth)
        response.raise_for_status()
        
        # Check if we got any data
        content = response.text.strip()
        if not content:
            print("Request was successful, but no data was returned for the specified period.")
            return True
        
        # Try to parse the response
        events = []
        for line in content.split("\n"):
            if line:  # Skip empty lines
                try:
                    events.append(json.loads(line))
                except json.JSONDecodeError as e:
                    print(f"Error parsing JSON: {e}")
                    continue
        
        print(f"Successfully retrieved {len(events)} events")
        
        # Print sample event (with sensitive data masked)
        if events:
            print("\nSample event structure:")
            sample = events[0]
            
            # Mask potential PII/sensitive data
            if "properties" in sample:
                for key in ["distinct_id", "$email", "email", "name", "user_id"]:
                    if key in sample["properties"]:
                        sample["properties"][key] = f"[MASKED {key}]"
            
            print(json.dumps(sample, indent=2, default=str))
        
        return True
    except requests.exceptions.HTTPError as e:
        print(f"HTTP error: {e}")
        print(f"Response: {e.response.text}")
        return False
    except Exception as e:
        print(f"Error: {str(e)}")
        return False

def test_mixpanel_engage():
    """Test Mixpanel Engage API to fetch user profiles"""
    print("\n=== Testing Mixpanel Engage API ===")
    
    # Create authentication
    auth = (API_SECRET, '')  # API secret as username, empty password
    
    # Make request to engage endpoint
    url = "https://mixpanel.com/api/2.0/engage/"
    
    print(f"Making request to: {url}")
    
    try:
        response = requests.get(url, auth=auth)
        response.raise_for_status()
        
        data = response.json()
        
        # Calculate total results
        total = data.get("total", 0)
        results = data.get("results", [])
        
        print(f"Successfully retrieved {len(results)} user profiles out of {total} total")
        
        # Print sample profile (with sensitive data masked)
        if results:
            print("\nSample user profile structure:")
            sample = results[0]
            
            # Mask the distinct_id
            if "$distinct_id" in sample:
                sample["$distinct_id"] = "[MASKED]"
            
            # Mask sensitive properties
            if "$properties" in sample:
                for key in ["$email", "$name", "email", "name", "user_id"]:
                    if key in sample["$properties"]:
                        sample["$properties"][key] = f"[MASKED {key}]"
            
            print(json.dumps(sample, indent=2, default=str))
        
        return True
    except requests.exceptions.HTTPError as e:
        print(f"HTTP error: {e}")
        print(f"Response: {e.response.text}")
        return False
    except Exception as e:
        print(f"Error: {str(e)}")
        return False

if __name__ == "__main__":
    if not API_SECRET:
        print("Error: Mixpanel API secret not found in .env file")
        print("Please ensure HITCRAFT_MIXPANEL_API_SECRET is set")
        sys.exit(1)
    
    export_success = test_mixpanel_export()
    engage_success = test_mixpanel_engage()
    
    print("\n=================================================")
    print(f"Mixpanel API Test Results:")
    print(f"Data Export API: {'PASSED' if export_success else 'FAILED'}")
    print(f"Engage API: {'PASSED' if engage_success else 'FAILED'}")
    print(f"Overall: {'PASSED' if export_success or engage_success else 'FAILED'}")
    print("=================================================")
