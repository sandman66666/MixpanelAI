"""
HitCraft Analytics Dashboard

Main dashboard application built with Streamlit that provides:
1. Real-time visualization of metrics
2. Funnel analysis
3. User segmentation analysis
4. Anthropic AI-powered insights
"""

import streamlit as st
import pandas as pd
import os
import json
from datetime import datetime, timedelta

# Import dashboard components
from hitcraft_analytics.ui.dashboard.components.metrics_panel import MetricsPanel
from hitcraft_analytics.ui.dashboard.components.funnel_panel import FunnelPanel
from hitcraft_analytics.ui.dashboard.components.ai_insights_panel import AIInsightsPanel

# Import data and insights modules
from hitcraft_analytics.data.repositories.events_repository import EventsRepository
from hitcraft_analytics.insights.insights_engine import InsightsEngine
from hitcraft_analytics.utils.logging_config import setup_logger

# Setup logger
logger = setup_logger("dashboard")

class Dashboard:
    """
    Main dashboard class that orchestrates all components and data flow.
    """
    
    def __init__(self):
        """Initialize the dashboard."""
        # Initialize components
        self.metrics_panel = MetricsPanel()
        self.funnel_panel = FunnelPanel()
        self.ai_insights_panel = AIInsightsPanel()
        
        # Initialize data repositories
        self.repository = EventsRepository()
        self.insights_engine = InsightsEngine()
        
        # Set default date range (last 30 days)
        end_date = datetime.now().date()
        start_date = end_date - timedelta(days=30)
        
        # Store in session state if not already there
        if "start_date" not in st.session_state:
            st.session_state.start_date = start_date
        
        if "end_date" not in st.session_state:
            st.session_state.end_date = end_date
    
    def load_data(self):
        """
        Load data for all dashboard components.
        
        Returns:
            Dict: Dictionary containing all the data needed for dashboard components
        """
        # Get date range from session state
        start_date = st.session_state.start_date
        end_date = st.session_state.end_date
        
        try:
            # Get metrics data
            metrics_data = self.repository.get_key_metrics(
                from_date=start_date.isoformat(),
                to_date=end_date.isoformat()
            )
            
            # Get trend data
            trend_data = self.repository.get_trend_data(
                from_date=start_date.isoformat(),
                to_date=end_date.isoformat(),
                metrics=["dau", "wau", "mau", "session_duration", "retention_rate"]
            )
            
            # Add trend data to metrics
            metrics_data["trends"] = trend_data
            
            # Get funnel data
            funnel_data = self.repository.get_funnel_data(
                from_date=start_date.isoformat(),
                to_date=end_date.isoformat()
            )
            
            # Get segment data
            segment_data = self.repository.get_segment_data(
                from_date=start_date.isoformat(),
                to_date=end_date.isoformat(),
                segment_by=["user_type", "platform", "country"]
            )
            
            # Get insights
            insights = self.insights_engine.generate_insights(
                from_date=start_date.isoformat(),
                to_date=end_date.isoformat(),
                insight_types=["trend", "funnel", "cohort"],
                max_insights=5
            )
            
            return {
                "metrics": metrics_data,
                "funnels": funnel_data,
                "segments": segment_data,
                "insights": insights,
                "timeframe": {
                    "start_date": start_date.isoformat(),
                    "end_date": end_date.isoformat(),
                    "days": (end_date - start_date).days
                }
            }
        
        except Exception as e:
            logger.error(f"Error loading data: {str(e)}", exc_info=True)
            return self._load_sample_data()
    
    def _load_sample_data(self):
        """
        Load sample data for demonstration purposes when real data is unavailable.
        
        Returns:
            Dict: Dictionary containing sample data
        """
        # Get date range from session state
        start_date = st.session_state.start_date
        end_date = st.session_state.end_date
        days = (end_date - start_date).days
        
        # Generate dates for time series
        dates = [(end_date - timedelta(days=i)).isoformat() for i in range(days)]
        dates.reverse()
        
        # Sample metrics data
        metrics_data = {
            "dau": {
                "current": 1250,
                "previous": 1100,
                "change": 13.6
            },
            "wau": {
                "current": 5400,
                "previous": 4800,
                "change": 12.5
            },
            "mau": {
                "current": 12000,
                "previous": 11000,
                "change": 9.1
            },
            "retention": {
                "current": 42.5,
                "previous": 39.8,
                "change": 2.7
            },
            "conversion": {
                "current": 8.3,
                "previous": 7.5,
                "change": 0.8
            },
            "session_duration": {
                "current": 15.4,  # minutes
                "previous": 14.2,
                "change": 8.5
            },
            "trends": {
                "dau": [
                    {"date": date, "value": 1000 + i * 10 + (i % 7) * 50} 
                    for i, date in enumerate(dates)
                ],
                "wau": [
                    {"date": date, "value": 4500 + i * 30 + (i % 7) * 100} 
                    for i, date in enumerate(dates)
                ],
                "mau": [
                    {"date": date, "value": 10000 + i * 100} 
                    for i, date in enumerate(dates)
                ],
                "session_duration": [
                    {"date": date, "value": 14 + (i % 10) / 5} 
                    for i, date in enumerate(dates)
                ],
                "retention_rate": [
                    {"date": date, "value": 38 + (i % 10) / 2} 
                    for i, date in enumerate(dates)
                ]
            }
        }
        
        # Sample funnel data
        funnel_data = {
            "signup": {
                "name": "Signup Funnel",
                "steps": [
                    {"name": "Visit Landing Page", "count": 10000, "conversion_rate": 100},
                    {"name": "View Signup Form", "count": 5000, "conversion_rate": 50},
                    {"name": "Start Registration", "count": 3000, "conversion_rate": 30},
                    {"name": "Complete Registration", "count": 2000, "conversion_rate": 20},
                    {"name": "Verify Email", "count": 1500, "conversion_rate": 15},
                    {"name": "Complete Profile", "count": 1000, "conversion_rate": 10}
                ],
                "time_series": [
                    {
                        "date": date,
                        "steps": [
                            {"name": "Visit Landing Page", "count": 10000 - (i % 500), "conversion_rate": 100},
                            {"name": "View Signup Form", "count": 5000 - (i % 300), "conversion_rate": 50 - (i % 5)},
                            {"name": "Start Registration", "count": 3000 - (i % 200), "conversion_rate": 30 - (i % 3)},
                            {"name": "Complete Registration", "count": 2000 - (i % 150), "conversion_rate": 20 - (i % 2)},
                            {"name": "Verify Email", "count": 1500 - (i % 100), "conversion_rate": 15 - (i % 2)},
                            {"name": "Complete Profile", "count": 1000 - (i % 50), "conversion_rate": 10 - (i % 1)}
                        ]
                    }
                    for i, date in enumerate(dates[::7])  # Weekly data points
                ]
            },
            "content_creation": {
                "name": "Content Creation Funnel",
                "steps": [
                    {"name": "Open Creator Dashboard", "count": 8000, "conversion_rate": 100},
                    {"name": "Start New Project", "count": 6000, "conversion_rate": 75},
                    {"name": "Use AI Features", "count": 4000, "conversion_rate": 50},
                    {"name": "Save Draft", "count": 3500, "conversion_rate": 43.8},
                    {"name": "Publish Content", "count": 2800, "conversion_rate": 35}
                ],
                "time_series": [
                    {
                        "date": date,
                        "steps": [
                            {"name": "Open Creator Dashboard", "count": 8000 - (i % 400), "conversion_rate": 100},
                            {"name": "Start New Project", "count": 6000 - (i % 300), "conversion_rate": 75 - (i % 5)},
                            {"name": "Use AI Features", "count": 4000 - (i % 200), "conversion_rate": 50 - (i % 3)},
                            {"name": "Save Draft", "count": 3500 - (i % 175), "conversion_rate": 43.8 - (i % 2)},
                            {"name": "Publish Content", "count": 2800 - (i % 140), "conversion_rate": 35 - (i % 2)}
                        ]
                    }
                    for i, date in enumerate(dates[::7])  # Weekly data points
                ]
            }
        }
        
        # Sample segment data
        segment_data = {
            "user_type": [
                {"name": "Creators", "count": 5000, "percent": 42},
                {"name": "Consumers", "count": 4500, "percent": 38},
                {"name": "Collaborators", "count": 2000, "percent": 17},
                {"name": "Administrators", "count": 300, "percent": 3}
            ],
            "platform": [
                {"name": "Web", "count": 6800, "percent": 57},
                {"name": "iOS", "count": 3500, "percent": 29},
                {"name": "Android", "count": 1500, "percent": 13},
                {"name": "Other", "count": 200, "percent": 1}
            ],
            "country": [
                {"name": "United States", "count": 4500, "percent": 38},
                {"name": "United Kingdom", "count": 1800, "percent": 15},
                {"name": "Canada", "count": 1500, "percent": 13},
                {"name": "Germany", "count": 1200, "percent": 10},
                {"name": "France", "count": 900, "percent": 8},
                {"name": "Australia", "count": 800, "percent": 7},
                {"name": "Other", "count": 1300, "percent": 11}
            ]
        }
        
        # Sample insights
        insights = [
            {
                "type": "trend",
                "title": "DAU Growth Trend",
                "description": "Daily active users have shown consistent growth over the past 30 days, with a 13.6% increase compared to the previous period.",
                "metrics": ["dau"],
                "importance": 90,
                "recommendations": [
                    "Continue the current user acquisition strategy",
                    "Focus on enhancing onboarding to maintain growth momentum"
                ]
            },
            {
                "type": "funnel",
                "title": "Signup Funnel Drop-off",
                "description": "There's a significant drop-off (40%) between 'View Signup Form' and 'Start Registration' steps in the signup funnel.",
                "metrics": ["conversion_rate"],
                "importance": 85,
                "recommendations": [
                    "Simplify the registration form to reduce friction",
                    "Add clearer calls-to-action on the signup page"
                ]
            },
            {
                "type": "cohort",
                "title": "Retention by User Type",
                "description": "Creator users show 63% higher retention compared to Consumer users.",
                "metrics": ["retention_rate"],
                "importance": 80,
                "recommendations": [
                    "Develop more features for Creators to leverage this strength",
                    "Identify what drives Creator retention and apply to Consumer experience"
                ]
            },
            {
                "type": "trend",
                "title": "Mobile Usage Increase",
                "description": "Mobile platform usage has increased by 23% over the past month, with iOS leading the growth.",
                "metrics": ["platform_distribution"],
                "importance": 75,
                "recommendations": [
                    "Prioritize mobile feature development, especially for iOS",
                    "Consider a mobile-first approach for new features"
                ]
            },
            {
                "type": "anomaly",
                "title": "Content Creation Spike",
                "description": "An unusual spike in content creation occurred on [recent date], coinciding with a product update.",
                "metrics": ["content_creation_rate"],
                "importance": 70,
                "recommendations": [
                    "Analyze which new features drove the increase in content creation",
                    "Highlight these features in marketing and onboarding"
                ]
            }
        ]
        
        return {
            "metrics": metrics_data,
            "funnels": funnel_data,
            "segments": segment_data,
            "insights": insights,
            "timeframe": {
                "start_date": start_date.isoformat(),
                "end_date": end_date.isoformat(),
                "days": days
            }
        }
    
    def render(self):
        """Render the complete dashboard."""
        # Set page config
        st.set_page_config(
            page_title="HitCraft Analytics Dashboard",
            page_icon="📊",
            layout="wide",
            initial_sidebar_state="expanded"
        )
        
        # Dashboard title
        st.title("HitCraft Analytics Dashboard")
        
        # Sidebar for controls
        with st.sidebar:
            st.header("Dashboard Controls")
            
            # Date range selector
            st.subheader("Date Range")
            
            # Date inputs
            col1, col2 = st.columns(2)
            with col1:
                start_date = st.date_input(
                    "Start Date",
                    value=st.session_state.start_date,
                    key="start_date_input"
                )
            
            with col2:
                end_date = st.date_input(
                    "End Date",
                    value=st.session_state.end_date,
                    key="end_date_input"
                )
            
            # Update session state on change
            if start_date != st.session_state.start_date:
                st.session_state.start_date = start_date
            
            if end_date != st.session_state.end_date:
                st.session_state.end_date = end_date
            
            # Quick date range buttons
            col1, col2 = st.columns(2)
            with col1:
                if st.button("Last 7 Days"):
                    st.session_state.end_date = datetime.now().date()
                    st.session_state.start_date = st.session_state.end_date - timedelta(days=7)
                    st.rerun()
            
            with col2:
                if st.button("Last 30 Days"):
                    st.session_state.end_date = datetime.now().date()
                    st.session_state.start_date = st.session_state.end_date - timedelta(days=30)
                    st.rerun()
            
            col1, col2 = st.columns(2)
            with col1:
                if st.button("Last 90 Days"):
                    st.session_state.end_date = datetime.now().date()
                    st.session_state.start_date = st.session_state.end_date - timedelta(days=90)
                    st.rerun()
            
            with col2:
                if st.button("Year to Date"):
                    st.session_state.end_date = datetime.now().date()
                    st.session_state.start_date = datetime(st.session_state.end_date.year, 1, 1).date()
                    st.rerun()
            
            # Data source information
            st.subheader("Data Source")
            st.info("Data is sourced from Mixpanel API and processed through the HitCraft Analytics Engine.")
            
            # About section
            st.subheader("About")
            st.markdown("""
            **HitCraft Analytics Dashboard**
            
            This dashboard provides real-time analytics and insights for the HitCraft platform, 
            powered by the HitCraft Analytics Engine and enhanced with Anthropic Claude AI.
            
            Version: 1.0.0
            """)
        
        # Load data for all components
        dashboard_data = self.load_data()
        
        # Create tabs for different sections
        tab1, tab2, tab3, tab4 = st.tabs([
            "Overview", 
            "Funnel Analysis", 
            "User Segments", 
            "AI Insights"
        ])
        
        # Overview tab
        with tab1:
            # Render key metrics panel
            self.metrics_panel.render(dashboard_data["metrics"])
            
            # Display top insights
            st.subheader("Top Insights")
            
            # Display insights in columns
            insights = dashboard_data["insights"]
            if insights:
                # Get top 3 insights
                top_insights = sorted(insights, key=lambda x: x.get("importance", 0), reverse=True)[:3]
                
                # Create columns for insights
                cols = st.columns(len(top_insights))
                
                # Display each insight in a column
                for col, insight in zip(cols, top_insights):
                    with col:
                        st.markdown(f"**{insight['title']}**")
                        st.markdown(insight["description"])
                        
                        # Show recommendations if available
                        if "recommendations" in insight and insight["recommendations"]:
                            with st.expander("Recommendations"):
                                for i, rec in enumerate(insight["recommendations"], 1):
                                    st.markdown(f"{i}. {rec}")
            else:
                st.info("No insights available for the selected time period.")
        
        # Funnel Analysis tab
        with tab2:
            self.funnel_panel.render(dashboard_data["funnels"])
        
        # User Segments tab
        with tab3:
            st.subheader("User Segmentation")
            
            # Get segment data
            segment_data = dashboard_data["segments"]
            
            # Select segment type
            segment_type = st.selectbox(
                "Select Segment Type",
                list(segment_data.keys()),
                key="segment_type_selector"
            )
            
            # Display selected segment data
            if segment_type in segment_data:
                segments = segment_data[segment_type]
                
                # Create pie chart for segments
                import plotly.express as px
                
                fig = px.pie(
                    segments,
                    values="count",
                    names="name",
                    title=f"User Distribution by {segment_type.replace('_', ' ').title()}",
                    hole=0.4
                )
                
                # Update layout
                fig.update_layout(
                    legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
                    margin=dict(l=40, r=40, t=60, b=40),
                    height=400
                )
                
                st.plotly_chart(fig, use_container_width=True)
                
                # Display segment metrics in table
                st.subheader(f"{segment_type.replace('_', ' ').title()} Metrics")
                
                # Convert to DataFrame for display
                df = pd.DataFrame(segments)
                
                # Format percentages
                df["percent"] = df["percent"].apply(lambda x: f"{x}%")
                
                # Format counts with commas
                df["count"] = df["count"].apply(lambda x: f"{x:,}")
                
                # Rename columns
                df.columns = [col.replace("_", " ").title() for col in df.columns]
                
                # Display table
                st.dataframe(df, use_container_width=True)
            else:
                st.info(f"No data available for segment type: {segment_type}")
        
        # AI Insights tab
        with tab4:
            self.ai_insights_panel.render(dashboard_data)


def main():
    """Main function to run the dashboard."""
    dashboard = Dashboard()
    dashboard.render()


if __name__ == "__main__":
    main()
