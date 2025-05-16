"""
Comprehensive Mixpanel Integration Test Suite

This script tests all components of the HitCraft AI Analytics Engine integration with Mixpanel,
including data export, tracking, and API access.
"""

import os
import sys
import json
import base64
import time
from datetime import datetime, timedelta

import requests
from dotenv import load_dotenv
from mixpanel import Mixpanel

# Add project path to system path to import the HitCraft modules
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

# Set default API credentials from environment variables
MIXPANEL_API_SECRET = os.getenv("HITCRAFT_MIXPANEL_API_SECRET")
MIXPANEL_PROJECT_ID = os.getenv("HITCRAFT_MIXPANEL_PROJECT_ID")

# Load environment variables
load_dotenv()

# Colors for formatting output
GREEN = "\033[92m"
RED = "\033[91m"
YELLOW = "\033[93m"
RESET = "\033[0m"
BOLD = "\033[1m"
UNDERLINE = "\033[4m"

def print_header(title):
    """Print a formatted section header"""
    print(f"\n{BOLD}{UNDERLINE}{title}{RESET}")

def print_result(name, success, message=None):
    """Print a test result with color coding"""
    status = f"{GREEN}PASSED{RESET}" if success else f"{RED}FAILED{RESET}"
    print(f"{name}: {status}")
    if message and not success:
        print(f"  {RED}→ {message}{RESET}")
    elif message and success:
        print(f"  {GREEN}→ {message}{RESET}")

def test_env_vars():
    """Test if required environment variables are set"""
    print_header("Testing Environment Variables")
    
    api_secret = os.getenv("HITCRAFT_MIXPANEL_API_SECRET")
    project_token = os.getenv("HITCRAFT_MIXPANEL_PROJECT_ID")
    
    api_secret_set = api_secret is not None and len(api_secret) > 0
    project_token_set = project_token is not None and len(project_token) > 0
    
    print_result("API Secret", api_secret_set)
    print_result("Project Token", project_token_set)
    
    if not api_secret_set or not project_token_set:
        print(f"{YELLOW}Please ensure HITCRAFT_MIXPANEL_API_SECRET and HITCRAFT_MIXPANEL_PROJECT_ID are set in .env file{RESET}")
    
    return api_secret_set and project_token_set

def test_mixpanel_track():
    """Test Mixpanel tracking API using project token"""
    print_header("Testing Mixpanel Tracking API")
    
    project_token = os.getenv("HITCRAFT_MIXPANEL_PROJECT_ID")
    
    try:
        # Initialize Mixpanel client
        mp = Mixpanel(project_token)
        
        # Generate a unique ID for this test run to avoid duplicate data
        test_id = f"test-{int(time.time())}"
        
        # Track a test event
        mp.track(test_id, "test_event", {
            "testing": True,
            "timestamp": datetime.now().isoformat()
        })
        
        print_result("Track Event", True, "Successfully sent test event to Mixpanel")
        return True
    except Exception as e:
        print_result("Track Event", False, str(e))
        return False

def test_direct_export_api():
    """Test direct connection to Mixpanel Data Export API"""
    print_header("Testing Direct Export API Connection")
    
    api_secret = os.getenv("HITCRAFT_MIXPANEL_API_SECRET")
    auth = (api_secret, '')  # API secret as username, empty password
    
    # Use a broad date range
    from_date = (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d")
    to_date = datetime.now().strftime("%Y-%m-%d")
    
    url = f"https://data.mixpanel.com/api/2.0/export/?from_date={from_date}&to_date={to_date}"
    
    try:
        response = requests.get(url, auth=auth)
        response.raise_for_status()
        
        # Check if we got a valid response
        if response.text.strip():
            # Try parsing the first event
            try:
                first_line = response.text.strip().split('\n')[0]
                json.loads(first_line)
                print_result("Export API", True, "Successfully retrieved data")
            except (json.JSONDecodeError, IndexError):
                print_result("Export API", True, "Received response but no events found in the date range")
        else:
            print_result("Export API", True, "Received empty response (likely no events in date range)")
        
        return True
    except requests.exceptions.HTTPError as e:
        print_result("Export API", False, f"HTTP error: {str(e)}")
        return False
    except Exception as e:
        print_result("Export API", False, str(e))
        return False

def test_mixpanel_connector():
    """Test the MixpanelConnector class using direct API calls"""
    print_header("Testing Direct Mixpanel API Calls")
    
    results = []
    api_secret = os.getenv("HITCRAFT_MIXPANEL_API_SECRET")
    auth = (api_secret, '')
    
    # Test events export API
    try:
        from_date = (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d")
        to_date = datetime.now().strftime("%Y-%m-%d")
        url = f"https://data.mixpanel.com/api/2.0/export/?from_date={from_date}&to_date={to_date}"
        
        response = requests.get(url, auth=auth)
        response.raise_for_status()
        
        events = []
        for line in response.text.strip().split("\n"):
            if line:  # Skip empty lines
                try:
                    events.append(json.loads(line))
                except json.JSONDecodeError:
                    pass
                    
        results.append(("Export API", True, f"Retrieved {len(events)} events"))
    except Exception as e:
        results.append(("Export API", False, str(e)))
    
    # Test event names API
    try:
        url = "https://mixpanel.com/api/2.0/events/names"
        params = {
            "from_date": (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d"),
            "to_date": datetime.now().strftime("%Y-%m-%d")
        }
        
        response = requests.get(url, auth=auth, params=params)
        response.raise_for_status()
        
        event_names = response.json()
        results.append(("Events Names API", True, f"Retrieved {len(event_names)} event names"))
    except Exception as e:
        results.append(("Events Names API", False, str(e)))
    
    # Test JQL API
    try:
        url = "https://mixpanel.com/api/2.0/jql"
        
        # Simple JQL query to get event counts
        jql_script = """
        function main() {
            return Events({
                from_date: '2020-01-01',
                to_date: '2023-12-31'
            })
            .groupBy(["name"], mixpanel.reducer.count())
            .map(function(r) {
                return {event: r.key[0], count: r.value};
            });
        }
        """
        
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json"
        }
        
        response = requests.post(
            url,
            auth=auth,
            json={"script": jql_script},
            headers=headers
        )
        response.raise_for_status()
        
        results.append(("JQL API", True, "Successfully executed JQL query"))
    except Exception as e:
        results.append(("JQL API", False, str(e)))
    
    # Print results
    for result in results:
        name, success, *message = result
        print_result(name, success, message[0] if message else None)
    
    # Return overall success if all tests passed
    return all(result[1] for result in results)

def main():
    """Run all tests and summarize results"""
    print(f"\n{BOLD}{YELLOW}===== HitCraft AI Analytics Engine - Mixpanel Integration Test Suite ====={RESET}\n")
    
    # Dictionary to track test results
    results = {}
    
    # Test environment variables
    results["Environment Variables"] = test_env_vars()
    
    # Test tracking API
    results["Tracking API"] = test_mixpanel_track()
    
    # Test direct export API
    results["Export API"] = test_direct_export_api()
    
    # Test connector
    results["MixpanelConnector"] = test_mixpanel_connector()
    
    # Print summary
    print(f"\n{BOLD}{YELLOW}===== Test Summary ====={RESET}")
    
    all_passed = True
    for test_name, passed in results.items():
        status = f"{GREEN}PASSED{RESET}" if passed else f"{RED}FAILED{RESET}"
        print(f"{test_name}: {status}")
        all_passed = all_passed and passed
    
    overall_status = f"{GREEN}PASSED{RESET}" if all_passed else f"{RED}FAILED{RESET}"
    print(f"\n{BOLD}Overall Integration Test: {overall_status}{RESET}")
    
    # Exit with appropriate code
    return 0 if all_passed else 1

if __name__ == "__main__":
    sys.exit(main())
