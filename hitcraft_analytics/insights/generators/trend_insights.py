"""
HitCraft AI Analytics Engine - Trend Insight Generator

This module generates insights from trend analysis results, including:
- Significant growth or decline in key metrics
- Anomalies in time series data
- Seasonal patterns
- Correlation between metrics
"""

import logging
from typing import Dict, List, Optional, Any, Union
from datetime import datetime

import pandas as pd
import numpy as np

from hitcraft_analytics.core.analysis.trend_detection import TrendDetector
from hitcraft_analytics.utils.logging.logger import get_logger

logger = get_logger(__name__)

class TrendInsightGenerator:
    """
    Generates actionable insights from trend analysis results.
    """
    
    def __init__(self, 
                trend_detector: Optional[TrendDetector] = None,
                significance_threshold: float = 0.1):  # 10% change is significant
        """
        Initialize the trend insight generator.
        
        Args:
            trend_detector (Optional[TrendDetector]): Trend detector for time-series analysis.
                If None, a new instance will be created.
            significance_threshold (float): Threshold for considering a change significant.
        """
        self.trend_detector = trend_detector or TrendDetector()
        self.significance_threshold = significance_threshold
        
        logger.info("Trend Insight Generator initialized")
    
    def generate_insights(self, 
                        time_series: List[Dict[str, Any]],
                        metric_name: str,
                        from_date: str,
                        to_date: str) -> List[Dict[str, Any]]:
        """
        Generate insights from time series data.
        
        Args:
            time_series (List[Dict[str, Any]]): Time series data with date and value.
            metric_name (str): Name of the metric being analyzed.
            from_date (str): Start date of the analysis period.
            to_date (str): End date of the analysis period.
            
        Returns:
            List[Dict[str, Any]]: List of generated insights.
        """
        insights = []
        
        # Convert time series to pandas Series
        if not time_series:
            logger.warning(f"No time series data available for {metric_name}")
            return []
        
        try:
            # Create DataFrame from time series data
            df = pd.DataFrame(time_series)
            
            # Ensure we have date and value columns
            if 'date' not in df.columns or 'value' not in df.columns:
                logger.error(f"Invalid time series format for {metric_name}")
                return []
            
            # Convert date strings to datetime
            df['date'] = pd.to_datetime(df['date'])
            
            # Set date as index
            df = df.set_index('date')
            
            # Create pandas Series
            series = df['value']
            
            # Detect linear trend
            trend_results = self.trend_detector.detect_linear_trend(series)
            
            if trend_results:
                trend_insights = self._generate_trend_insights(trend_results, metric_name, from_date, to_date)
                insights.extend(trend_insights)
            
            # Detect change points
            change_point_results = self.trend_detector.detect_change_points(series)
            
            if change_point_results:
                change_point_insights = self._generate_change_point_insights(change_point_results, metric_name, from_date, to_date)
                insights.extend(change_point_insights)
            
            # Detect anomalies
            anomaly_results = self.trend_detector.detect_anomalies(series)
            
            if anomaly_results:
                anomaly_insights = self._generate_anomaly_insights(anomaly_results, metric_name, from_date, to_date)
                insights.extend(anomaly_insights)
            
        except Exception as e:
            logger.error(f"Error generating trend insights for {metric_name}: {str(e)}")
        
        return insights
    
    def _generate_trend_insights(self,
                               trend_results: Dict[str, Any],
                               metric_name: str,
                               from_date: str,
                               to_date: str) -> List[Dict[str, Any]]:
        """
        Generate insights from linear trend detection.
        
        Args:
            trend_results (Dict[str, Any]): Results from trend detection.
            metric_name (str): Name of the metric being analyzed.
            from_date (str): Start date of the analysis period.
            to_date (str): End date of the analysis period.
            
        Returns:
            List[Dict[str, Any]]: List of trend insights.
        """
        insights = []
        
        # Check if we have a significant trend
        if "slope" in trend_results and "r_squared" in trend_results:
            slope = trend_results["slope"]
            r_squared = trend_results["r_squared"]
            
            # Calculate relative slope (% change over the entire period)
            baseline = trend_results.get("baseline", 0)
            if baseline > 0:
                period_length = trend_results.get("n_points", 30)
                relative_slope = (slope * period_length) / baseline
            else:
                relative_slope = 0
            
            # Only create insight if trend is significant
            if abs(relative_slope) >= self.significance_threshold and r_squared >= 0.5:
                trend_type = "increasing" if slope > 0 else "decreasing"
                
                # Format the change as percentage
                change_pct = relative_slope * 100
                
                insight = {
                    "type": "metric_trend",
                    "subtype": f"{trend_type}_trend",
                    "title": f"{metric_name} is {trend_type.title()}",
                    "description": f"{metric_name} shows a significant {trend_type} trend of {change_pct:.1f}% over the period.",
                    "impact_score": min(0.9, abs(relative_slope) * 2 * r_squared),
                    "metric_value": slope,
                    "metric_name": metric_name,
                    "date_range": f"{from_date} to {to_date}",
                    "data": {
                        "metric_name": metric_name,
                        "slope": slope,
                        "r_squared": r_squared,
                        "relative_slope": relative_slope,
                        "baseline": baseline,
                        "trend_type": trend_type
                    }
                }
                
                # Add recommendations based on trend type
                if trend_type == "increasing":
                    insight["recommendations"] = [
                        f"Analyze factors contributing to the increase in {metric_name}",
                        "Consider allocating more resources to support the growth",
                        "Look for correlations with marketing campaigns or product changes"
                    ]
                else:
                    insight["recommendations"] = [
                        f"Investigate potential causes of {metric_name} decline",
                        "Review recent changes that might have negatively impacted this metric",
                        "Consider targeted interventions to reverse the trend"
                    ]
                
                insights.append(insight)
        
        return insights
    
    def _generate_change_point_insights(self,
                                      change_point_results: Dict[str, Any],
                                      metric_name: str,
                                      from_date: str,
                                      to_date: str) -> List[Dict[str, Any]]:
        """
        Generate insights from change point detection.
        
        Args:
            change_point_results (Dict[str, Any]): Results from change point detection.
            metric_name (str): Name of the metric being analyzed.
            from_date (str): Start date of the analysis period.
            to_date (str): End date of the analysis period.
            
        Returns:
            List[Dict[str, Any]]: List of change point insights.
        """
        insights = []
        
        # Check if we have change points
        if "change_points" in change_point_results and change_point_results["change_points"]:
            change_points = change_point_results["change_points"]
            
            for i, cp in enumerate(change_points):
                # Get change point details
                cp_date = cp.get("date", "")
                before_mean = cp.get("before_mean", 0)
                after_mean = cp.get("after_mean", 0)
                
                # Calculate relative change
                if before_mean > 0:
                    relative_change = (after_mean - before_mean) / before_mean
                else:
                    relative_change = 0 if after_mean == 0 else 1
                
                # Only create insight if change is significant
                if abs(relative_change) >= self.significance_threshold:
                    change_type = "increase" if relative_change > 0 else "decrease"
                    
                    # Format the change as percentage
                    change_pct = relative_change * 100
                    
                    insight = {
                        "type": "metric_change_point",
                        "subtype": f"significant_{change_type}",
                        "title": f"Significant {change_type.title()} in {metric_name}",
                        "description": f"{metric_name} experienced a significant {change_type} of {abs(change_pct):.1f}% around {cp_date}.",
                        "impact_score": min(0.95, abs(relative_change) * 2),
                        "metric_value": relative_change,
                        "metric_name": metric_name,
                        "date_range": f"{from_date} to {to_date}",
                        "data": {
                            "metric_name": metric_name,
                            "change_point_date": cp_date,
                            "before_mean": before_mean,
                            "after_mean": after_mean,
                            "relative_change": relative_change,
                            "change_type": change_type
                        },
                        "recommendations": [
                            f"Review events and changes around {cp_date} that might have caused this shift",
                            "Analyze whether this change point correlates with other metrics",
                            f"Consider whether this {change_type} is beneficial or requires intervention"
                        ]
                    }
                    
                    insights.append(insight)
        
        return insights
    
    def _generate_anomaly_insights(self,
                                 anomaly_results: Dict[str, Any],
                                 metric_name: str,
                                 from_date: str,
                                 to_date: str) -> List[Dict[str, Any]]:
        """
        Generate insights from anomaly detection.
        
        Args:
            anomaly_results (Dict[str, Any]): Results from anomaly detection.
            metric_name (str): Name of the metric being analyzed.
            from_date (str): Start date of the analysis period.
            to_date (str): End date of the analysis period.
            
        Returns:
            List[Dict[str, Any]]: List of anomaly insights.
        """
        insights = []
        
        # Check if we have anomalies
        if "anomalies" in anomaly_results and anomaly_results["anomalies"]:
            anomalies = anomaly_results["anomalies"]
            
            # Group anomalies by type (positive or negative)
            positive_anomalies = [a for a in anomalies if a.get("deviation", 0) > 0]
            negative_anomalies = [a for a in anomalies if a.get("deviation", 0) < 0]
            
            # Create insight for positive anomalies if there are any
            if positive_anomalies:
                # Sort by severity
                positive_anomalies.sort(key=lambda x: x.get("deviation", 0), reverse=True)
                
                # Take the top anomaly
                top_anomaly = positive_anomalies[0]
                anomaly_date = top_anomaly.get("date", "")
                expected_value = top_anomaly.get("expected", 0)
                actual_value = top_anomaly.get("actual", 0)
                
                # Calculate relative deviation
                if expected_value > 0:
                    relative_deviation = (actual_value - expected_value) / expected_value
                else:
                    relative_deviation = 1 if actual_value > 0 else 0
                
                # Format the deviation as percentage
                deviation_pct = relative_deviation * 100
                
                insight = {
                    "type": "metric_anomaly",
                    "subtype": "positive_spike",
                    "title": f"Positive Spike in {metric_name}",
                    "description": f"{metric_name} had an unusually high value on {anomaly_date}, {deviation_pct:.1f}% above expected.",
                    "impact_score": min(0.9, relative_deviation),
                    "metric_value": relative_deviation,
                    "metric_name": metric_name,
                    "date_range": f"{from_date} to {to_date}",
                    "data": {
                        "metric_name": metric_name,
                        "anomaly_date": anomaly_date,
                        "expected_value": expected_value,
                        "actual_value": actual_value,
                        "relative_deviation": relative_deviation,
                        "total_anomalies": len(positive_anomalies)
                    },
                    "recommendations": [
                        f"Investigate what caused the spike in {metric_name} on {anomaly_date}",
                        "Look for correlations with marketing campaigns or external events",
                        "Consider whether this represents an opportunity to replicate success"
                    ]
                }
                
                insights.append(insight)
            
            # Create insight for negative anomalies if there are any
            if negative_anomalies:
                # Sort by severity (most negative first)
                negative_anomalies.sort(key=lambda x: x.get("deviation", 0))
                
                # Take the top anomaly
                top_anomaly = negative_anomalies[0]
                anomaly_date = top_anomaly.get("date", "")
                expected_value = top_anomaly.get("expected", 0)
                actual_value = top_anomaly.get("actual", 0)
                
                # Calculate relative deviation
                if expected_value > 0:
                    relative_deviation = (actual_value - expected_value) / expected_value
                else:
                    relative_deviation = -1 if actual_value < 0 else 0
                
                # Format the deviation as percentage
                deviation_pct = abs(relative_deviation) * 100
                
                insight = {
                    "type": "metric_anomaly",
                    "subtype": "negative_drop",
                    "title": f"Negative Drop in {metric_name}",
                    "description": f"{metric_name} had an unusually low value on {anomaly_date}, {deviation_pct:.1f}% below expected.",
                    "impact_score": min(0.9, abs(relative_deviation)),
                    "metric_value": relative_deviation,
                    "metric_name": metric_name,
                    "date_range": f"{from_date} to {to_date}",
                    "data": {
                        "metric_name": metric_name,
                        "anomaly_date": anomaly_date,
                        "expected_value": expected_value,
                        "actual_value": actual_value,
                        "relative_deviation": relative_deviation,
                        "total_anomalies": len(negative_anomalies)
                    },
                    "recommendations": [
                        f"Investigate what caused the drop in {metric_name} on {anomaly_date}",
                        "Check for system issues, data collection problems, or external factors",
                        "Assess whether this represents a one-time event or the beginning of a trend"
                    ]
                }
                
                insights.append(insight)
        
        return insights
