#!/usr/bin/env python3
"""
HitCraft Analytics Dashboard Helper

This script sets up the Python path properly and runs the Streamlit dashboard
with the correct environment.
"""

import os
import sys
from pathlib import Path

# Add the project root to sys.path to ensure all modules can be found
project_root = Path(__file__).parent.absolute()
sys.path.insert(0, str(project_root))

# Print environment information for debugging
print(f"Python version: {sys.version}")
print(f"Python executable: {sys.executable}")
print(f"Project root: {project_root}")

try:
    import streamlit
    print(f"Streamlit version: {streamlit.__version__}")
except ImportError:
    print("Streamlit not found in the current Python environment")
    sys.exit(1)

try:
    import plotly
    print(f"Plotly version: {plotly.__version__}")
except ImportError:
    print("Plotly not found in the current Python environment")
    sys.exit(1)

try:
    import pandas
    print(f"Pandas version: {pandas.__version__}")
except ImportError:
    print("Pandas not found in the current Python environment")
    sys.exit(1)

# Set the PYTHONPATH environment variable
os.environ["PYTHONPATH"] = str(project_root)

# Set the dashboard file path
dashboard_path = os.path.join(project_root, "hitcraft_analytics", "ui", "dashboard", "main_dashboard.py")
print(f"Dashboard path: {dashboard_path}")

# Import the subprocess module for more reliable execution
import subprocess

# Run the Streamlit CLI directly, handling paths with spaces properly
print("Starting Streamlit dashboard...")
subprocess.run([
    sys.executable, 
    "-m", 
    "streamlit", 
    "run", 
    dashboard_path,
    "--server.port=8501",
    "--server.address=localhost"
])
