#!/usr/bin/env python3

"""
Count Mixpanel Events from EU Data Centers

This script connects to Mixpanel's EU data centers using the provided credentials
and counts all events available in the account.
"""

import json
import requests
import time
from datetime import datetime, timedelta

# Mixpanel credentials
PROJECT_TOKEN = "3bbc24764765962cb8af4c45ac04ae4d"
API_SECRET = "9a685163559ec32b97c7d89a4adebafc"

# EU endpoints
API_HOST = "api-eu.mixpanel.com"
DATA_HOST = "data-eu.mixpanel.com"

# Date range - use a wide range to capture all events
end_date = datetime.now()
start_date = end_date - timedelta(days=730)  # 2 years of data

from_date = start_date.strftime("%Y-%m-%d")
to_date = end_date.strftime("%Y-%m-%d")

print(f"Counting all Mixpanel events from {from_date} to {to_date}")
print(f"Using Project Token: {PROJECT_TOKEN}")
print(f"Using API Secret: {API_SECRET[:4]}...{API_SECRET[-4:]}")
print(f"Using EU endpoints: {API_HOST} and {DATA_HOST}")


def count_all_events():
    """Count all events from EU data centers"""
    
    # Try the EU Events API to get event count by type
    try:
        print("\n1. TRYING EU EVENTS API (Event Count By Type)")
        
        url = f"https://{API_HOST}/api/2.0/events"
        params = {
            "event": ["$overall"],  # Use $overall for all events
            "type": "general",
            "unit": "day",
            "interval": 30,
            "format": "json",
            "from_date": from_date,
            "to_date": to_date
        }
        
        auth = (API_SECRET, '')
        
        print(f"Making request to {url} with params: {params}")
        response = requests.get(url, auth=auth, params=params)
        
        print(f"Response: {response.status_code}")
        if response.status_code == 200:
            data = response.json()
            print(f"Response data: {json.dumps(data, indent=2)[:500]}...")
            
            # Try to count events from the response
            if "data" in data:
                events = data.get("data", {}).get("values", {}).get("$overall", {})
                total_events = sum(sum(day_counts.values()) for event, day_counts in events.items())
                print(f"Total events: {total_events}")
                
                # Get event types
                event_types = set()
                for event_data in events.values():
                    event_types.update(event_data.keys())
                
                print(f"Event types found: {len(event_types)}")
                if event_types:
                    print(f"Event types: {', '.join(list(event_types)[:10])}...")
            else:
                print("No 'data' in response")
        else:
            print(f"Error message: {response.text}")
    
    except Exception as e:
        print(f"Events API Error: {str(e)}")
    
    # Try the EU Export API directly
    try:
        print("\n2. TRYING EU EXPORT API (Raw Event Export)")
        
        url = f"https://{DATA_HOST}/api/2.0/export"
        params = {
            "from_date": from_date,
            "to_date": to_date,
        }
        
        auth = (API_SECRET, '')
        
        print(f"Making request to {url} with params: {params}")
        response = requests.get(url, auth=auth, params=params, stream=True)
        
        print(f"Response: {response.status_code}")
        if response.status_code == 200:
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
                        
                    # Stop after 50,000 events to avoid overwhelming memory/output 
                    if event_count >= 50000:
                        print("Reached 50,000 events - stopping count for performance")
                        print("There are more than 50,000 events in your account")
                        break
            
            print(f"Total events found: {event_count}")
            
            if lines:
                print("\nSample events:")
                for i, line in enumerate(lines):
                    try:
                        event = json.loads(line)
                        # Print a formatted version of the event
                        print(f"Event {i+1} type: {event.get('event')}")
                        print(f"  Time: {datetime.fromtimestamp(event.get('properties', {}).get('time')).strftime('%Y-%m-%d %H:%M:%S')}")
                        print(f"  User: {event.get('properties', {}).get('distinct_id')}")
                        print(f"  Properties: {json.dumps({k: v for k, v in event.get('properties', {}).items() if k not in ['time', 'distinct_id']})[:200]}...")
                    except:
                        print(f"Event {i+1}: {line[:200]}...")
            else:
                print("No events found in response")
        else:
            print(f"Error message: {response.text}")
    
    except Exception as e:
        print(f"Export API Error: {str(e)}")
    
    # Try using EU JQL (JSON Query Language) API
    try:
        print("\n3. TRYING EU JQL API (Advanced Query)")
        
        url = f"https://{API_HOST}/api/2.0/jql"
        
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
            auth=(API_SECRET, ''),
            headers=headers,
            json={"script": script}
        )
        
        print(f"Response: {response.status_code}")
        if response.status_code == 200:
            data = response.json()
            
            # Calculate total events
            total_events = sum(item["count"] for item in data)
            print(f"Total events: {total_events}")
            
            # Show top event types by count
            print("\nTop event types:")
            for item in sorted(data, key=lambda x: x["count"], reverse=True)[:10]:
                print(f"  {item['event']}: {item['count']} events")
        else:
            print(f"Error message: {response.text}")
    
    except Exception as e:
        print(f"JQL API Error: {str(e)}")


if __name__ == "__main__":
    count_all_events()
