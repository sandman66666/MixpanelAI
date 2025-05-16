#!/usr/bin/env python3

"""
Count All Mixpanel Events

This script connects to Mixpanel using the provided credentials and counts
all events available in the account, with detailed output for debugging.
"""

import base64
import json
import requests
import time
from datetime import datetime, timedelta
import sys

# Mixpanel credentials directly from user
PROJECT_TOKEN = "3bbc24764765962cb8af4c45ac04ae4d"
API_SECRET = "9a685163559ec32b97c7d89a4adebafc"

# Date range - use a wide range to capture all events
# Going back 2 years to ensure we get historical data
end_date = datetime.now()
start_date = end_date - timedelta(days=730)  # 2 years of data

from_date = start_date.strftime("%Y-%m-%d")
to_date = end_date.strftime("%Y-%m-%d")

print(f"Counting all Mixpanel events from {from_date} to {to_date}")
print(f"Using Project Token: {PROJECT_TOKEN}")
print(f"Using API Secret: {API_SECRET[:4]}...{API_SECRET[-4:]}")


def count_all_events():
    """Count all events using the JQL API"""
    
    # First try using the Events API to get event count by type
    try:
        print("\n1. TRYING EVENTS API (Event Count By Type)")
        
        url = "https://mixpanel.com/api/2.0/events"
        params = {
            "event": [""],  # Empty list means all events
            "type": "general",
            "unit": "day",
            "interval": 30,  # Get last 30 days
            "format": "json",
            "from_date": from_date,
            "to_date": to_date
        }
        
        auth = (API_SECRET, '')
        
        print(f"Making request to {url} with params: {params}")
        response = requests.get(url, auth=auth, params=params)
        
        if response.status_code == 200:
            data = response.json()
            print(f"Response: {response.status_code} - Success!")
            print(f"Response data: {json.dumps(data, indent=2)[:200]}...")
            
            # Try to count events from the response
            if "data" in data:
                events = data["data"]["values"]
                total_events = sum(sum(day_counts.values()) for day_counts in events.values())
                print(f"Total events: {total_events}")
                
                # Get event types
                event_types = set()
                for event_data in events.values():
                    event_types.update(event_data.keys())
                
                print(f"Event types found: {len(event_types)}")
                print(f"Event types: {', '.join(list(event_types)[:10])}...")
            else:
                print("No 'data' in response")
        else:
            print(f"Response: {response.status_code} - Failed")
            print(f"Error message: {response.text}")
    
    except Exception as e:
        print(f"Events API Error: {str(e)}")
    
    # Try the Export API directly
    try:
        print("\n2. TRYING EXPORT API (Raw Event Export)")
        
        url = "https://data.mixpanel.com/api/2.0/export"
        params = {
            "from_date": from_date,
            "to_date": to_date,
        }
        
        auth = (API_SECRET, '')
        
        print(f"Making request to {url} with params: {params}")
        response = requests.get(url, auth=auth, params=params, stream=True)
        
        if response.status_code == 200:
            print(f"Response: {response.status_code} - Success!")
            
            # Sample the first few lines to see the structure
            lines = []
            event_count = 0
            print("Reading events (this might take a while for large datasets)...")
            
            for line in response.iter_lines():
                if line:
                    event_count += 1
                    if len(lines) < 5:  # Just sample the first 5 events
                        lines.append(line.decode('utf-8'))
                    
                    # Print progress every 10,000 events
                    if event_count % 10000 == 0:
                        print(f"Processed {event_count} events so far...")
            
            print(f"Total events found: {event_count}")
            
            if lines:
                print("\nSample events:")
                for i, line in enumerate(lines):
                    print(f"Event {i+1}: {line[:200]}...")
            else:
                print("No events found in response")
        else:
            print(f"Response: {response.status_code} - Failed")
            print(f"Error message: {response.text}")
    
    except Exception as e:
        print(f"Export API Error: {str(e)}")
    
    # Try using JQL (JSON Query Language) API
    try:
        print("\n3. TRYING JQL API (Advanced Query)")
        
        url = "https://mixpanel.com/api/2.0/jql"
        
        # Simple script to count events by type
        script = f"""
        function main() {{
          return Events({{
            from_date: "{from_date}",
            to_date: "{to_date}"
          }})
          .groupBy(["name"], mixpanel.reducer.count())
          .map(function(r) {{
            return {{ event: r.key[0], count: r.value }};
          }});
        }}
        """
        
        headers = {
            "Content-Type": "application/json",
            "Accept": "application/json"
        }
        
        print(f"Making request to {url} with JQL script")
        response = requests.post(
            url,
            auth=auth,
            headers=headers,
            json={"script": script}
        )
        
        if response.status_code == 200:
            data = response.json()
            print(f"Response: {response.status_code} - Success!")
            
            # Calculate total events
            total_events = sum(item["count"] for item in data)
            print(f"Total events: {total_events}")
            
            # Show top event types by count
            print("\nTop event types:")
            for item in sorted(data, key=lambda x: x["count"], reverse=True)[:10]:
                print(f"  {item['event']}: {item['count']} events")
        else:
            print(f"Response: {response.status_code} - Failed")
            print(f"Error message: {response.text}")
    
    except Exception as e:
        print(f"JQL API Error: {str(e)}")
        
    # Try the engage API to get user counts
    try:
        print("\n4. TRYING ENGAGE API (User Profiles)")
        
        url = "https://mixpanel.com/api/2.0/engage"
        params = {
            "page_size": 1000,  # Maximum allowed
        }
        
        print(f"Making request to {url}")
        response = requests.get(url, auth=auth, params=params)
        
        if response.status_code == 200:
            data = response.json()
            print(f"Response: {response.status_code} - Success!")
            
            if "results" in data:
                user_count = len(data["results"])
                total_user_count = data.get("total", user_count)
                print(f"Total users: {total_user_count}")
                print(f"Page size: {len(data['results'])}")
                
                if data["results"]:
                    print("\nSample user profile:")
                    sample_user = data["results"][0]
                    print(json.dumps(sample_user, indent=2)[:300] + "...")
            else:
                print("No 'results' in response")
        else:
            print(f"Response: {response.status_code} - Failed")
            print(f"Error message: {response.text}")
    
    except Exception as e:
        print(f"Engage API Error: {str(e)}")


if __name__ == "__main__":
    count_all_events()
