"""
Test script to verify Mixpanel API integration using the provided token approach.
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
PROJECT_TOKEN = os.getenv("HITCRAFT_MIXPANEL_PROJECT_ID")  # Using this as the token
API_SECRET = os.getenv("HITCRAFT_MIXPANEL_API_SECRET")

print(f"Using Project Token: {PROJECT_TOKEN}")
print(f"API Secret: {'*' * len(API_SECRET) if API_SECRET else 'Not found'}")

def test_mixpanel_service_account():
    """Test authentication with Mixpanel API using service account approach"""
    print("\n=== Testing Mixpanel Service Account Authentication ===")
    
    headers = {
        "Accept": "application/json",
        "Authorization": f"Bearer {API_SECRET}"  # Using API Secret as service account token
    }
    
    # Try to access a simple endpoint to test authentication
    url = "https://mixpanel.com/api/2.0/export"
    
    params = {
        "project_id": PROJECT_TOKEN,
        "from_date": (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d"),
        "to_date": datetime.now().strftime("%Y-%m-%d"),
    }
    
    print(f"Making request to: {url}")
    print(f"With params: {params}")
    
    try:
        response = requests.get(url, headers=headers, params=params)
        response.raise_for_status()  # Raise exception for 4XX/5XX responses
        
        print(f"Authentication successful! Status code: {response.status_code}")
        
        # Print part of the response to verify data
        print(f"Response preview: {response.text[:200]}...")
        return True
    except requests.exceptions.HTTPError as e:
        print(f"Authentication failed: {e}")
        print(f"Response: {e.response.text}")
        return False
    except Exception as e:
        print(f"Error: {str(e)}")
        return False

def test_jql_query():
    """Test JQL query to Mixpanel"""
    print("\n=== Testing Mixpanel JQL Query ===")
    
    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json",
        "Authorization": f"Bearer {API_SECRET}"  # Using API Secret as service account token
    }
    
    # Test with a simple JQL query
    data = {
        "script": """
        function main() {
          return Events({
            from_date: "2025-05-01",
            to_date: "2025-05-16"
          })
          .limit(5)
          .map(function(event) {
            return {
              event: event.name,
              time: event.time,
              distinct_id: event.distinct_id
            };
          });
        }
        """,
        "project_id": PROJECT_TOKEN
    }
    
    url = "https://mixpanel.com/api/2.0/jql"
    print(f"Making JQL request to: {url}")
    
    try:
        response = requests.post(url, headers=headers, json=data)
        response.raise_for_status()
        
        results = response.json()
        print(f"JQL query successful! Got {len(results)} results")
        
        # Print results (with sensitive data masked)
        if results:
            print("\nSample results:")
            sample = results[:2]  # Just show first 2 results
            
            # Mask potential PII/sensitive data
            for item in sample:
                if "distinct_id" in item:
                    item["distinct_id"] = "[MASKED]"
            
            print(json.dumps(sample, indent=2))
        
        return True
    except requests.exceptions.HTTPError as e:
        print(f"JQL query failed: {e}")
        print(f"Response: {e.response.text}")
        return False
    except Exception as e:
        print(f"Error: {str(e)}")
        return False

def test_data_export_api():
    """Test Mixpanel Data Export API using token authentication"""
    print("\n=== Testing Mixpanel Data Export API ===")
    
    # Base64 encode "api_secret:' for Basic Auth
    auth_string = f"{API_SECRET}:"
    encoded_auth = base64.b64encode(auth_string.encode()).decode()
    
    headers = {
        "Accept": "application/json",
        "Authorization": f"Basic {encoded_auth}"
    }
    
    # Format dates for the API
    from_date = (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d")
    to_date = datetime.now().strftime("%Y-%m-%d")
    
    # Make request to events endpoint
    url = f"https://data.mixpanel.com/api/2.0/export?from_date={from_date}&to_date={to_date}"
    
    print(f"Making request to: {url}")
    
    try:
        response = requests.get(url, headers=headers)
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
            
            print(json.dumps(sample, indent=2, default=str))
        
        return len(events) > 0
    except requests.exceptions.HTTPError as e:
        print(f"Error getting event data: {e}")
        print(f"Response: {e.response.text}")
        return False
    except Exception as e:
        print(f"Error: {str(e)}")
        return False

def run_mixpanel_tests():
    """Run all Mixpanel integration tests"""
    print("=================================================")
    print("Testing Mixpanel API Integration (Alternative Methods)")
    print("=================================================")
    
    # Test different approaches
    service_account_success = test_mixpanel_service_account()
    jql_success = test_jql_query()
    export_success = test_data_export_api()
    
    success = service_account_success or jql_success or export_success
    
    print("\n=================================================")
    print(f"Mixpanel Integration Test Results:")
    print(f"Service Account Auth: {'PASSED' if service_account_success else 'FAILED'}")
    print(f"JQL Query: {'PASSED' if jql_success else 'FAILED'}")
    print(f"Data Export API: {'PASSED' if export_success else 'FAILED'}")
    print(f"Overall: {'PASSED' if success else 'FAILED'}")
    print("=================================================")
    
    return success

if __name__ == "__main__":
    if not PROJECT_TOKEN or not API_SECRET:
        print("Error: Mixpanel credentials not found in .env file")
        print("Please ensure HITCRAFT_MIXPANEL_PROJECT_ID and HITCRAFT_MIXPANEL_API_SECRET are set")
        sys.exit(1)
    
    run_mixpanel_tests()
