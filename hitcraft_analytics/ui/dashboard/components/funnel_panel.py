"""
Funnel Analysis Panel Component for the HitCraft Dashboard

Displays funnel visualizations and conversion metrics at each step.
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from typing import Dict, List, Any, Optional

class FunnelPanel:
    """
    Component for displaying funnel analysis visualizations.
    """
    
    def __init__(self):
        """Initialize the funnel panel component."""
        pass
    
    def render(self, funnel_data: Dict[str, Any]):
        """
        Render the funnel panel with the provided data.
        
        Args:
            funnel_data: Dictionary containing funnel data
        """
        st.subheader("Funnel Analysis")
        
        # Get available funnels
        available_funnels = list(funnel_data.keys())
        
        if not available_funnels:
            st.info("No funnel data available.")
            return
        
        # Select funnel to display
        selected_funnel = st.selectbox(
            "Select Funnel",
            available_funnels,
            key="funnel_selector"
        )
        
        # Get data for the selected funnel
        funnel = funnel_data.get(selected_funnel, {})
        
        if not funnel:
            st.info(f"No data available for the {selected_funnel} funnel.")
            return
        
        # Display funnel visualization
        self._render_funnel_chart(funnel)
        
        # Display conversion rates between steps
        self._render_conversion_metrics(funnel)
        
        # Display funnel over time
        self._render_funnel_over_time(funnel)
    
    def _render_funnel_chart(self, funnel: Dict[str, Any]):
        """
        Render the funnel visualization.
        
        Args:
            funnel: Dictionary containing funnel data
        """
        steps = funnel.get("steps", [])
        
        if not steps:
            st.info("No step data available for this funnel.")
            return
        
        # Create funnel chart using Plotly
        fig = go.Figure()
        
        step_names = [step.get("name", f"Step {i+1}") for i, step in enumerate(steps)]
        values = [step.get("count", 0) for step in steps]
        
        # Add trace for funnel chart
        fig.add_trace(go.Funnel(
            name="Conversion Funnel",
            y=step_names,
            x=values,
            textposition="inside",
            textinfo="value+percent initial",
            opacity=0.8,
            marker=dict(
                color=[
                    "#1f77b4",
                    "#2ca02c",
                    "#d62728",
                    "#9467bd",
                    "#8c564b",
                    "#e377c2"
                ][:len(steps)],
                line=dict(width=2, color="#FFFFFF")
            ),
            connector=dict(line=dict(color="rgba(0, 0, 0, 0.3)", width=1))
        ))
        
        # Update layout
        fig.update_layout(
            title=f"{funnel.get('name', 'Conversion Funnel')}",
            margin=dict(l=40, r=40, t=60, b=40),
            height=400
        )
        
        st.plotly_chart(fig, use_container_width=True)
    
    def _render_conversion_metrics(self, funnel: Dict[str, Any]):
        """
        Render metrics for conversion rates between steps.
        
        Args:
            funnel: Dictionary containing funnel data
        """
        steps = funnel.get("steps", [])
        
        if len(steps) < 2:
            return
        
        st.subheader("Step Conversion Rates")
        
        # Calculate conversion rates between steps
        conversions = []
        
        for i in range(len(steps) - 1):
            current_step = steps[i]
            next_step = steps[i + 1]
            
            current_count = current_step.get("count", 0)
            next_count = next_step.get("count", 0)
            
            if current_count > 0:
                conversion_rate = (next_count / current_count) * 100
            else:
                conversion_rate = 0
            
            conversions.append({
                "from_step": current_step.get("name", f"Step {i+1}"),
                "to_step": next_step.get("name", f"Step {i+2}"),
                "conversion_rate": conversion_rate,
                "from_count": current_count,
                "to_count": next_count,
                "drop_off": current_count - next_count
            })
        
        # Convert to DataFrame for display
        df = pd.DataFrame(conversions)
        
        # Create metrics display in columns
        cols = st.columns(len(conversions))
        
        for i, (col, conversion) in enumerate(zip(cols, conversions)):
            with col:
                st.metric(
                    label=f"{conversion['from_step']} → {conversion['to_step']}",
                    value=f"{conversion['conversion_rate']:.1f}%",
                    delta=f"-{conversion['drop_off']:,} users"
                )
    
    def _render_funnel_over_time(self, funnel: Dict[str, Any]):
        """
        Render the funnel performance over time.
        
        Args:
            funnel: Dictionary containing funnel data
        """
        time_series = funnel.get("time_series", [])
        
        if not time_series:
            return
        
        st.subheader("Funnel Performance Over Time")
        
        # Convert to DataFrame for plotting
        data = []
        
        for ts_entry in time_series:
            date = ts_entry.get("date", "")
            steps = ts_entry.get("steps", [])
            
            for step in steps:
                data.append({
                    "date": date,
                    "step": step.get("name", ""),
                    "count": step.get("count", 0),
                    "conversion_rate": step.get("conversion_rate", 0)
                })
        
        df = pd.DataFrame(data)
        
        # Allow switching between count and conversion rate
        view_option = st.radio(
            "View",
            ["User Count", "Conversion Rate"],
            horizontal=True
        )
        
        # Create line chart
        fig = go.Figure()
        
        for step in df["step"].unique():
            step_data = df[df["step"] == step]
            
            if view_option == "User Count":
                y_values = step_data["count"]
                y_title = "User Count"
            else:
                y_values = step_data["conversion_rate"]
                y_title = "Conversion Rate (%)"
            
            fig.add_trace(
                go.Scatter(
                    x=step_data["date"],
                    y=y_values,
                    mode="lines+markers",
                    name=step,
                    line=dict(width=2)
                )
            )
        
        # Update layout
        fig.update_layout(
            title=f"Funnel Performance Over Time ({view_option})",
            xaxis_title="Date",
            yaxis_title=y_title,
            legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
            margin=dict(l=40, r=40, t=60, b=40),
            height=400
        )
        
        st.plotly_chart(fig, use_container_width=True)
