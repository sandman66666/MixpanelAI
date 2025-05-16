#!/usr/bin/env python3
"""
Dashboard Launcher Script

This script launches the HitCraft Analytics Dashboard.
"""

import os
import sys
import streamlit.web.bootstrap
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Add the project root to PYTHONPATH
project_root = Path(__file__).parent.absolute()
sys.path.append(str(project_root))

def main():
    """Launch the Streamlit dashboard."""
    # Path to the dashboard script
    dashboard_path = os.path.join(
        project_root, 
        "hitcraft_analytics", 
        "ui", 
        "dashboard", 
        "main_dashboard.py"
    )
    
    # Check if Anthropic API key is set
    if not os.getenv("ANTHROPIC_API_KEY"):
        print("Warning: ANTHROPIC_API_KEY environment variable is not set.")
        print("The AI Insights feature will be disabled.")
        print("Please add your API key to the .env file or set it as an environment variable.")
        print("Example: ANTHROPIC_API_KEY=sk-ant-api03-...")
    else:
        print("Anthropic API key found. AI Insights feature will be enabled.")
    
    # Launch Streamlit with our dashboard
    sys.argv = [
        "streamlit", "run", 
        dashboard_path,
        "--server.port=8501",
        "--server.address=localhost"
    ]
    
    # Use the current Streamlit API correctly
    streamlit.web.bootstrap.run(
        main_script_path=dashboard_path,
        args=[],
        flag_options={},
        is_hello=False
    )

if __name__ == "__main__":
    main()
