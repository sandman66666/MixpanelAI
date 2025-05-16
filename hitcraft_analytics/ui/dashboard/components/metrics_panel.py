"""
Metrics Panel Component for the HitCraft Dashboard

Displays key performance indicators and metrics in a user-friendly panel.
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional

class MetricsPanel:
    """
    Component for displaying key metrics and KPIs.
    """
    
    def __init__(self):
        """Initialize the metrics panel component."""
        pass
    
    def render(self, metrics_data: Dict[str, Any]):
        """
        Render the metrics panel with the provided data.
        
        Args:
            metrics_data: Dictionary containing metrics data
        """
        st.subheader("Key Metrics")
        
        # Create columns for metrics display
        col1, col2, col3, col4 = st.columns(4)
        
        # Display DAU (Daily Active Users)
        with col1:
            dau = metrics_data.get("dau", {})
            current_dau = dau.get("current", 0)
            prev_dau = dau.get("previous", 0)
            
            if prev_dau > 0:
                percent_change = ((current_dau - prev_dau) / prev_dau) * 100
                delta_color = "normal" if percent_change >= 0 else "inverse"
                st.metric(
                    label="DAU",
                    value=f"{current_dau:,}",
                    delta=f"{percent_change:.1f}%",
                    delta_color=delta_color
                )
            else:
                st.metric(label="DAU", value=f"{current_dau:,}")
        
        # Display WAU (Weekly Active Users)
        with col2:
            wau = metrics_data.get("wau", {})
            current_wau = wau.get("current", 0)
            prev_wau = wau.get("previous", 0)
            
            if prev_wau > 0:
                percent_change = ((current_wau - prev_wau) / prev_wau) * 100
                delta_color = "normal" if percent_change >= 0 else "inverse"
                st.metric(
                    label="WAU",
                    value=f"{current_wau:,}",
                    delta=f"{percent_change:.1f}%",
                    delta_color=delta_color
                )
            else:
                st.metric(label="WAU", value=f"{current_wau:,}")
        
        # Display Retention Rate
        with col3:
            retention = metrics_data.get("retention", {})
            current_retention = retention.get("current", 0)
            prev_retention = retention.get("previous", 0)
            
            if prev_retention > 0:
                percent_change = current_retention - prev_retention
                delta_color = "normal" if percent_change >= 0 else "inverse"
                st.metric(
                    label="Retention (7d)",
                    value=f"{current_retention:.1f}%",
                    delta=f"{percent_change:.1f}%",
                    delta_color=delta_color
                )
            else:
                st.metric(label="Retention (7d)", value=f"{current_retention:.1f}%")
        
        # Display Conversion Rate
        with col4:
            conversion = metrics_data.get("conversion", {})
            current_conversion = conversion.get("current", 0)
            prev_conversion = conversion.get("previous", 0)
            
            if prev_conversion > 0:
                percent_change = current_conversion - prev_conversion
                delta_color = "normal" if percent_change >= 0 else "inverse"
                st.metric(
                    label="Conversion",
                    value=f"{current_conversion:.1f}%",
                    delta=f"{percent_change:.1f}%",
                    delta_color=delta_color
                )
            else:
                st.metric(label="Conversion", value=f"{current_conversion:.1f}%")
        
        # Display trend chart for selected metric
        self._render_trend_chart(metrics_data)
    
    def _render_trend_chart(self, metrics_data: Dict[str, Any]):
        """
        Render a trend chart for the selected metric.
        
        Args:
            metrics_data: Dictionary containing metrics data
        """
        available_metrics = ["DAU", "WAU", "MAU", "Session Duration", "Retention Rate"]
        selected_metric = st.selectbox("Select Metric to Display", available_metrics)
        
        # Get trend data for the selected metric
        trend_data = metrics_data.get("trends", {}).get(selected_metric.lower().replace(" ", "_"), [])
        
        if not trend_data:
            st.info(f"No trend data available for {selected_metric}")
            return
        
        # Convert to DataFrame for plotting
        df = pd.DataFrame(trend_data)
        
        # Create the trend chart
        fig = go.Figure()
        
        fig.add_trace(
            go.Scatter(
                x=df.get("date", []),
                y=df.get("value", []),
                mode="lines+markers",
                name=selected_metric,
                line=dict(color="#3366ff", width=2),
                marker=dict(color="#3366ff", size=5)
            )
        )
        
        # Add a trend line
        if len(df) > 1:
            try:
                import numpy as np
                x = np.arange(len(df))
                y = df.get("value", [])
                
                z = np.polyfit(x, y, 1)
                p = np.poly1d(z)
                
                fig.add_trace(
                    go.Scatter(
                        x=df.get("date", []),
                        y=p(x),
                        mode="lines",
                        name="Trend",
                        line=dict(color="rgba(255, 0, 0, 0.5)", width=1, dash="dash")
                    )
                )
            except Exception as e:
                st.error(f"Error calculating trend line: {str(e)}")
        
        # Update layout
        fig.update_layout(
            title=f"{selected_metric} Trend",
            xaxis_title="Date",
            yaxis_title=selected_metric,
            legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
            margin=dict(l=40, r=40, t=40, b=40),
            height=400
        )
        
        st.plotly_chart(fig, use_container_width=True)
