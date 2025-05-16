#!/usr/bin/env python3

"""
Mixpanel SDK Test

This script tests the connection to Mixpanel using the official Mixpanel SDK
with the provided credentials.
"""

import json
import time
from datetime import datetime, timedelta
from mixpanel import Mixpanel

# Mixpanel credentials from user
PROJECT_TOKEN = "3bbc24764765962cb8af4c45ac04ae4d"
API_SECRET = "9a685163559ec32b97c7d89a4adebafc"

print(f"Testing Mixpanel SDK connection")
print(f"Project token: {PROJECT_TOKEN}")
print(f"API secret: {API_SECRET[:4]}...{API_SECRET[-4:]}")

# Initialize the Mixpanel client
mp = Mixpanel(PROJECT_TOKEN)

# Set date range - past 6 months
end_date = datetime.now()
start_date = end_date - timedelta(days=180)
from_date = start_date.strftime("%Y-%m-%d")
to_date = end_date.strftime("%Y-%m-%d")
print(f"Date range: {from_date} to {to_date}")

# First test - track a simple event
try:
    print("\nTesting event tracking...")
    mp.track('test_user', 'test_event', {
        'source': 'sdk_test',
        'test_time': datetime.now().isoformat()
    })
    print("✅ Event tracking successful!")
except Exception as e:
    print(f"❌ Event tracking failed: {str(e)}")

# Second test - JQL query to count events
try:
    print("\nFetching event counts using JQL API...")
    
    # Use JQL API with API secret
    import requests
    
    jql_url = "https://mixpanel.com/api/2.0/jql"
    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json"
    }
    
    # Query to count events by type
    script = """
    function main() {
      return Events({
        from_date: "STARTDATE",
        to_date: "ENDDATE"
      })
      .groupBy(["name"], mixpanel.reducer.count())
      .map(function(r) {
        return { event: r.key[0], count: r.value };
      });
    }
    """.replace("STARTDATE", from_date).replace("ENDDATE", to_date)
    
    # Make the API request
    response = requests.post(
        jql_url,
        auth=(API_SECRET, ''),
        headers=headers,
        data=script
    )
    
    # Check response
    if response.status_code == 200:
        results = response.json()
        total_events = sum(event['count'] for event in results)
        print(f"✅ JQL query successful!")
        print(f"Total events found: {total_events}")
        
        # Show top event types
        print("\nTop event types:")
        for event in sorted(results, key=lambda x: x['count'], reverse=True)[:10]:
            print(f"  - {event['event']}: {event['count']} events")
    else:
        print(f"❌ JQL query failed: {response.status_code} {response.text}")
        
except Exception as e:
    print(f"❌ JQL query failed: {str(e)}")

# Third test - export API with API secret
try:
    print("\nTesting export API...")
    
    export_url = "https://data.mixpanel.com/api/2.0/export"
    params = {
        "from_date": from_date,
        "to_date": to_date,
    }
    
    # Make the request
    response = requests.get(
        export_url,
        auth=(API_SECRET, ""),
        params=params,
    )
    
    # Check for errors
    if response.status_code == 200:
        # Count the number of events by parsing the newline-delimited JSON response
        events = []
        event_count = 0
        for line in response.text.strip().split("\n"):
            if line:  # Skip empty lines
                try:
                    events.append(json.loads(line))
                    event_count += 1
                    # Display progress for every 1000 events
                    if event_count % 1000 == 0:
                        print(f"Processed {event_count} events...")
                except json.JSONDecodeError as e:
                    print(f"Error parsing JSON response: {str(e)}")
                    if not events:
                        print(f"First 100 characters of response: {response.text[:100]}")
        
        print(f"✅ Export API successful!")
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
            
            # Show top event types
            print("Top event types:")
            for event_type, count in sorted(event_types.items(), key=lambda x: x[1], reverse=True)[:10]:
                print(f"  - {event_type}: {count} events")
    else:
        print(f"❌ Export API failed: {response.status_code} {response.text}")
        
except Exception as e:
    print(f"❌ Export API failed: {str(e)}")
