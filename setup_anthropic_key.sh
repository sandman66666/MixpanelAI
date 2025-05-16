#!/bin/bash
# Script to set up Anthropic API key

# Set the API key as an environment variable
export ANTHROPIC_API_KEY="YOUR_API_KEY_HERE"

# Instructions for the user
echo "Anthropic API key has been set for this terminal session."
echo "To make this permanent, add this line to your shell profile (~/.bash_profile, ~/.zshrc, etc.):"
echo "export ANTHROPIC_API_KEY=\"YOUR_API_KEY_HERE\""
echo ""
echo "Alternatively, you can save it in your .env file:"
echo "ANTHROPIC_API_KEY=YOUR_API_KEY_HERE"
echo ""
echo "To update run_dashboard.py to load from .env file, modify it to use python-dotenv:"
echo "1. Install python-dotenv: pip install python-dotenv"
echo "2. Add to run_dashboard.py:"
echo "   from dotenv import load_dotenv"
echo "   load_dotenv()"
