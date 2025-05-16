"""
AI Insights Panel Component for the HitCraft Dashboard

Provides a natural language interface to interact with the Anthropic AI
for analytics queries and insights.
"""

import streamlit as st
import pandas as pd
from typing import Dict, List, Any, Optional
import os
import json
from datetime import datetime, timedelta

from hitcraft_analytics.insights.ai.anthropic_client import AnthropicClient, AnalyticsQueryProcessor

class AIInsightsPanel:
    """
    Component for displaying and interacting with AI-powered insights.
    """
    
    def __init__(self):
        """Initialize the AI insights panel component."""
        self.anthropic_client = AnthropicClient()
        self.query_processor = AnalyticsQueryProcessor()
        
        # Initialize session state for chat history if it doesn't exist
        if "chat_history" not in st.session_state:
            st.session_state.chat_history = []
    
    def render(self, analytics_data: Optional[Dict[str, Any]] = None):
        """
        Render the AI insights panel.
        
        Args:
            analytics_data: Optional dictionary containing analytics data to provide context
        """
        st.subheader("AI Insights Assistant")
        
        # Display information about the AI assistant
        with st.expander("About AI Insights Assistant"):
            st.markdown("""
            The AI Insights Assistant uses Anthropic Claude to help you analyze your analytics data.
            
            You can:
            - Ask natural language questions about your data
            - Request specific insights on metrics, funnels, or user segments
            - Get recommendations based on the latest analytics
            
            Example questions:
            - "What are the key metrics for the past 30 days?"
            - "Analyze the signup funnel and identify drop-off points."
            - "What user segments have the highest retention?"
            - "What recommendations do you have to improve conversion rates?"
            """)
        
        # Display suggested prompts
        col1, col2 = st.columns(2)
        
        with col1:
            if st.button("📊 Show Key Metrics Summary"):
                self._add_user_message("Summarize the key metrics for HitCraft over the past 30 days.")
                self._process_query("Summarize the key metrics for HitCraft over the past 30 days.", analytics_data)
        
        with col2:
            if st.button("🔍 Analyze Funnel Performance"):
                self._add_user_message("Analyze the signup funnel performance and identify drop-off points.")
                self._process_query("Analyze the signup funnel performance and identify drop-off points.", analytics_data)
        
        col3, col4 = st.columns(2)
        
        with col3:
            if st.button("👥 User Segment Analysis"):
                self._add_user_message("Which user segments have the highest engagement?")
                self._process_query("Which user segments have the highest engagement?", analytics_data)
        
        with col4:
            if st.button("💡 Growth Recommendations"):
                self._add_user_message("What are the top growth opportunities based on recent data?")
                self._process_query("What are the top growth opportunities based on recent data?", analytics_data)
        
        # Display chat history
        self._display_chat_history()
        
        # User input
        user_query = st.text_input("Ask a question about your analytics data:", key="user_query")
        
        # Process query when submitted
        if user_query:
            self._add_user_message(user_query)
            self._process_query(user_query, analytics_data)
            # Clear input after submission
            st.session_state.user_query = ""
    
    def _display_chat_history(self):
        """Display the chat history between the user and AI assistant."""
        if not st.session_state.chat_history:
            st.info("Start a conversation with the AI Insights Assistant using the suggestions above or by typing your own question.")
            return
        
        # Display chat messages
        for message in st.session_state.chat_history:
            role = message["role"]
            content = message["content"]
            
            if role == "user":
                st.markdown(f'<div style="background-color: #f0f2f6; padding: 10px; border-radius: 5px; margin-bottom: 10px;"><strong>You:</strong> {content}</div>', unsafe_allow_html=True)
            else:
                st.markdown(f'<div style="background-color: #deeaff; padding: 10px; border-radius: 5px; margin-bottom: 10px;"><strong>AI Assistant:</strong> {content}</div>', unsafe_allow_html=True)
    
    def _add_user_message(self, message: str):
        """
        Add a user message to the chat history.
        
        Args:
            message: The user's message
        """
        st.session_state.chat_history.append({
            "role": "user",
            "content": message,
            "timestamp": datetime.now().isoformat()
        })
    
    def _add_assistant_message(self, message: str):
        """
        Add an assistant message to the chat history.
        
        Args:
            message: The assistant's message
        """
        st.session_state.chat_history.append({
            "role": "assistant",
            "content": message,
            "timestamp": datetime.now().isoformat()
        })
    
    def _process_query(self, query: str, analytics_data: Optional[Dict[str, Any]] = None):
        """
        Process a user query and display the response.
        
        Args:
            query: The user's query
            analytics_data: Optional analytics data to provide context
        """
        # Get spinner while processing
        with st.spinner("Analyzing data and generating insights..."):
            try:
                # Check if API key is configured
                if not os.getenv("ANTHROPIC_API_KEY"):
                    response = "⚠️ Anthropic API key not configured. Please set the ANTHROPIC_API_KEY environment variable to enable AI insights."
                else:
                    # Process the query using the Anthropic client
                    response = self.anthropic_client.query(query, analytics_data)
            except Exception as e:
                response = f"Sorry, I encountered an error: {str(e)}"
        
        # Add response to chat history
        self._add_assistant_message(response)
        
        # Force refresh to show the new message
        st.rerun()
    
    def get_preset_insights(self) -> Dict[str, str]:
        """
        Get a set of preset insights for quick display.
        
        Returns:
            Dict[str, str]: Dictionary of preset insights
        """
        try:
            # Get key metrics summary
            key_metrics = self.query_processor.get_key_metrics_summary()
            
            # Get funnel analysis
            funnel_analysis = self.query_processor.analyze_funnel("signup")
            
            # Get growth opportunities
            growth_opportunities = self.query_processor.identify_growth_opportunities()
            
            # Get retention insights
            retention_insights = self.query_processor.get_retention_insights()
            
            return {
                "key_metrics": key_metrics,
                "funnel_analysis": funnel_analysis,
                "growth_opportunities": growth_opportunities,
                "retention_insights": retention_insights
            }
        except Exception as e:
            return {
                "error": f"Error generating preset insights: {str(e)}"
            }
