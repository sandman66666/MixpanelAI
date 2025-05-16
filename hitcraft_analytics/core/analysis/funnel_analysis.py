"""
HitCraft AI Analytics Engine - Funnel Analysis

This module provides functionality for analyzing user conversion funnels,
tracking user progression through defined funnels, calculating conversion rates,
and identifying drop-off points within the funnel.
"""

import logging
from typing import Dict, List, Optional, Tuple, Any, Union
import pandas as pd
import numpy as np
from datetime import datetime, timedelta

from hitcraft_analytics.utils.logging.logger import get_logger

logger = get_logger(__name__)

class FunnelAnalyzer:
    """
    Analyzes user conversion funnels to identify conversion rates and drop-off points.
    """
    
    def __init__(self):
        """Initialize the FunnelAnalyzer."""
        logger.info("Initializing FunnelAnalyzer")
    
    def analyze_funnel(self, 
                     events: pd.DataFrame, 
                     funnel_steps: List[str],
                     user_id_col: str = "distinct_id",
                     event_name_col: str = "event",
                     timestamp_col: str = "time",
                     max_days_to_convert: int = 30,
                     step_properties: Optional[Dict[str, Dict[str, Any]]] = None) -> Dict[str, Any]:
        """
        Analyze a conversion funnel based on a sequence of events.
        
        Args:
            events (pd.DataFrame): DataFrame containing event data from Mixpanel
            funnel_steps (List[str]): Ordered list of event names forming the funnel
            user_id_col (str): Column name containing user identifiers
            event_name_col (str): Column name containing event names
            timestamp_col (str): Column name containing event timestamps
            max_days_to_convert (int): Maximum number of days allowed to complete the funnel
            step_properties (Optional[Dict[str, Dict[str, Any]]]): Additional property filters for each step
                Example: {"step_name": {"property_name": "property_value"}}
        
        Returns:
            Dict[str, Any]: Analysis results including conversion rates and drop-off points
        """
        if len(funnel_steps) < 2:
            logger.error("Funnel analysis requires at least 2 steps")
            raise ValueError("At least 2 steps are required for funnel analysis")
        
        logger.info(f"Analyzing funnel with {len(funnel_steps)} steps: {', '.join(funnel_steps)}")
        
        # Ensure the dataframe has the required columns
        required_cols = [user_id_col, event_name_col, timestamp_col]
        for col in required_cols:
            if col not in events.columns:
                logger.error(f"Required column '{col}' not found in event data")
                raise ValueError(f"Required column '{col}' not found in event data")
        
        # Ensure timestamp is datetime
        if not pd.api.types.is_datetime64_any_dtype(events[timestamp_col]):
            logger.info("Converting timestamp column to datetime")
            events[timestamp_col] = pd.to_datetime(events[timestamp_col])
        
        # Sort events by user and timestamp
        events = events.sort_values([user_id_col, timestamp_col])
        
        # Filter events to only include funnel steps
        funnel_events = events[events[event_name_col].isin(funnel_steps)].copy()
        
        # Apply additional property filters if specified
        if step_properties:
            for step, properties in step_properties.items():
                step_mask = funnel_events[event_name_col] == step
                for prop_name, prop_value in properties.items():
                    if prop_name in funnel_events.columns:
                        prop_mask = funnel_events[prop_name] == prop_value
                        step_mask = step_mask & prop_mask
                funnel_events = funnel_events[step_mask | (funnel_events[event_name_col] != step)]
        
        # Initialize tracking of users at each step
        conversions = {}
        for i, step in enumerate(funnel_steps):
            conversions[step] = set()
        
        # Track average time between steps
        step_times = {}
        for i in range(len(funnel_steps) - 1):
            step_times[(funnel_steps[i], funnel_steps[i+1])] = []
        
        # Process each user's journey
        for user_id, user_events in funnel_events.groupby(user_id_col):
            current_step_idx = 0
            current_step_time = None
            
            # We need to check if they have the first event
            if not user_events[user_events[event_name_col] == funnel_steps[0]].empty:
                # Mark user as converted for the first step
                conversions[funnel_steps[0]].add(user_id)
                current_step_time = user_events[user_events[event_name_col] == funnel_steps[0]].iloc[0][timestamp_col]
                current_step_idx = 1
            else:
                # Skip this user if they don't have the first event
                continue
            
            # Process subsequent steps
            for step_idx in range(1, len(funnel_steps)):
                step = funnel_steps[step_idx]
                
                # Check if user performed this step after the previous step
                step_events = user_events[
                    (user_events[event_name_col] == step) & 
                    (user_events[timestamp_col] > current_step_time) &
                    (user_events[timestamp_col] <= current_step_time + timedelta(days=max_days_to_convert))
                ]
                
                if not step_events.empty:
                    # User converted at this step
                    step_time = step_events.iloc[0][timestamp_col]
                    conversions[step].add(user_id)
                    
                    # Track time to convert from previous step
                    prev_step = funnel_steps[step_idx-1]
                    time_diff = (step_time - current_step_time).total_seconds() / 86400  # in days
                    step_times[(prev_step, step)].append(time_diff)
                    
                    # Update current step
                    current_step_time = step_time
                    current_step_idx = step_idx + 1
                else:
                    # User dropped off at this step
                    break
        
        # Calculate conversion rates and absolute numbers
        step_counts = [len(conversions[step]) for step in funnel_steps]
        conversion_rates = []
        for i in range(len(step_counts)):
            if i == 0:
                # First step conversion rate is 100% by definition
                conversion_rates.append(1.0)
            else:
                # Calculate relative conversion rate from previous step
                prev_count = step_counts[i-1]
                curr_count = step_counts[i]
                rate = curr_count / prev_count if prev_count > 0 else 0
                conversion_rates.append(rate)
        
        # Calculate overall funnel conversion
        overall_conversion = step_counts[-1] / step_counts[0] if step_counts[0] > 0 else 0
        
        # Calculate average conversion time between steps
        avg_conversion_times = {}
        for step_pair, times in step_times.items():
            if times:
                avg_conversion_times[f"{step_pair[0]} → {step_pair[1]}"] = sum(times) / len(times)
            else:
                avg_conversion_times[f"{step_pair[0]} → {step_pair[1]}"] = None
        
        # Identify drop-off points
        drop_offs = []
        for i in range(len(funnel_steps) - 1):
            drop_off_rate = 1 - conversion_rates[i+1]
            drop_offs.append({
                "step": funnel_steps[i],
                "next_step": funnel_steps[i+1],
                "drop_off_rate": drop_off_rate,
                "drop_off_count": step_counts[i] - step_counts[i+1]
            })
        
        # Sort drop-offs by severity
        drop_offs.sort(key=lambda x: x["drop_off_rate"], reverse=True)
        
        # Prepare results
        results = {
            "funnel_steps": funnel_steps,
            "step_counts": {funnel_steps[i]: step_counts[i] for i in range(len(funnel_steps))},
            "conversion_rates": {funnel_steps[i]: conversion_rates[i] for i in range(len(funnel_steps))},
            "overall_conversion_rate": overall_conversion,
            "total_users_entered": step_counts[0],
            "total_users_converted": step_counts[-1],
            "avg_conversion_times": avg_conversion_times,
            "drop_offs": drop_offs
        }
        
        logger.info(f"Funnel analysis complete. Overall conversion rate: {overall_conversion:.2%}")
        return results
    
    def compare_funnels(self, 
                      funnel_results_a: Dict[str, Any], 
                      funnel_results_b: Dict[str, Any],
                      name_a: str = "Funnel A", 
                      name_b: str = "Funnel B") -> Dict[str, Any]:
        """
        Compare two funnel analyses to identify differences in conversion rates.
        
        Args:
            funnel_results_a (Dict[str, Any]): Results from the first funnel analysis
            funnel_results_b (Dict[str, Any]): Results from the second funnel analysis
            name_a (str): Name of the first funnel for reporting
            name_b (str): Name of the second funnel for reporting
            
        Returns:
            Dict[str, Any]: Comparison results highlighting differences
        """
        # Validate that the funnels have the same steps
        if funnel_results_a["funnel_steps"] != funnel_results_b["funnel_steps"]:
            logger.error("Cannot compare funnels with different steps")
            raise ValueError("Funnels must have the same steps to be compared")
        
        funnel_steps = funnel_results_a["funnel_steps"]
        
        # Calculate conversion rate differences
        conversion_diffs = {}
        for step in funnel_steps:
            rate_a = funnel_results_a["conversion_rates"][step]
            rate_b = funnel_results_b["conversion_rates"][step]
            abs_diff = rate_b - rate_a
            rel_diff = abs_diff / rate_a if rate_a > 0 else float('inf')
            conversion_diffs[step] = {
                "absolute_diff": abs_diff,
                "relative_diff": rel_diff,
                f"{name_a}_rate": rate_a,
                f"{name_b}_rate": rate_b
            }
        
        # Calculate overall conversion rate difference
        overall_a = funnel_results_a["overall_conversion_rate"]
        overall_b = funnel_results_b["overall_conversion_rate"]
        overall_abs_diff = overall_b - overall_a
        overall_rel_diff = overall_abs_diff / overall_a if overall_a > 0 else float('inf')
        
        # Compare drop-offs
        drop_off_comparison = []
        for i, step in enumerate(funnel_steps[:-1]):
            next_step = funnel_steps[i+1]
            
            # Find corresponding drop-offs in both funnels
            drop_off_a = next((d for d in funnel_results_a["drop_offs"] if d["step"] == step), None)
            drop_off_b = next((d for d in funnel_results_b["drop_offs"] if d["step"] == step), None)
            
            if drop_off_a and drop_off_b:
                rate_diff = drop_off_b["drop_off_rate"] - drop_off_a["drop_off_rate"]
                drop_off_comparison.append({
                    "step": step,
                    "next_step": next_step,
                    f"{name_a}_drop_off_rate": drop_off_a["drop_off_rate"],
                    f"{name_b}_drop_off_rate": drop_off_b["drop_off_rate"],
                    "rate_diff": rate_diff,
                    "improved": rate_diff < 0  # Negative difference means lower drop-off rate, which is good
                })
        
        # Sort by absolute improvement
        drop_off_comparison.sort(key=lambda x: abs(x["rate_diff"]), reverse=True)
        
        # Prepare comparison results
        comparison = {
            "funnel_steps": funnel_steps,
            "conversion_rate_diffs": conversion_diffs,
            "overall_conversion_comparison": {
                f"{name_a}_rate": overall_a,
                f"{name_b}_rate": overall_b,
                "absolute_diff": overall_abs_diff,
                "relative_diff": overall_rel_diff,
                "improved": overall_abs_diff > 0  # Positive difference means higher conversion, which is good
            },
            "drop_off_comparison": drop_off_comparison,
            "total_users_comparison": {
                f"{name_a}_entered": funnel_results_a["total_users_entered"],
                f"{name_b}_entered": funnel_results_b["total_users_entered"],
                f"{name_a}_converted": funnel_results_a["total_users_converted"],
                f"{name_b}_converted": funnel_results_b["total_users_converted"]
            }
        }
        
        logger.info(f"Funnel comparison complete between {name_a} and {name_b}")
        return comparison
    
    def segment_funnel_analysis(self,
                              events: pd.DataFrame,
                              funnel_steps: List[str],
                              segment_column: str,
                              user_id_col: str = "distinct_id",
                              event_name_col: str = "event",
                              timestamp_col: str = "time",
                              max_days_to_convert: int = 30) -> Dict[str, Dict[str, Any]]:
        """
        Analyze a funnel across different segments of users.
        
        Args:
            events (pd.DataFrame): DataFrame containing event data from Mixpanel
            funnel_steps (List[str]): Ordered list of event names forming the funnel
            segment_column (str): Column name to use for segmentation
            user_id_col (str): Column name containing user identifiers
            event_name_col (str): Column name containing event names
            timestamp_col (str): Column name containing event timestamps
            max_days_to_convert (int): Maximum number of days allowed to complete the funnel
            
        Returns:
            Dict[str, Dict[str, Any]]: Analysis results for each segment
        """
        if segment_column not in events.columns:
            logger.error(f"Segment column '{segment_column}' not found in event data")
            raise ValueError(f"Segment column '{segment_column}' not found in event data")
        
        # Get unique segment values
        segments = events[segment_column].dropna().unique()
        
        logger.info(f"Performing segmented funnel analysis using {segment_column} with {len(segments)} segments")
        
        # Analyze funnel for each segment
        segment_results = {}
        for segment in segments:
            # Filter events for this segment
            segment_events = events[events[segment_column] == segment]
            
            if len(segment_events) == 0:
                logger.warning(f"No events found for segment '{segment}'")
                continue
            
            try:
                # Analyze funnel for this segment
                result = self.analyze_funnel(
                    events=segment_events,
                    funnel_steps=funnel_steps,
                    user_id_col=user_id_col,
                    event_name_col=event_name_col,
                    timestamp_col=timestamp_col,
                    max_days_to_convert=max_days_to_convert
                )
                
                # Store results for this segment
                segment_results[str(segment)] = result
                
            except Exception as e:
                logger.error(f"Error analyzing funnel for segment '{segment}': {str(e)}")
        
        # Compare segments
        if len(segment_results) > 1:
            # Find segment with highest overall conversion
            best_segment = max(segment_results.items(), key=lambda x: x[1]["overall_conversion_rate"])
            best_segment_name = best_segment[0]
            best_segment_rate = best_segment[1]["overall_conversion_rate"]
            
            # Find segment with lowest overall conversion
            worst_segment = min(segment_results.items(), key=lambda x: x[1]["overall_conversion_rate"])
            worst_segment_name = worst_segment[0]
            worst_segment_rate = worst_segment[1]["overall_conversion_rate"]
            
            # Calculate conversion rate range
            conversion_range = best_segment_rate - worst_segment_rate
            
            # Add comparison summary
            segment_results["_summary"] = {
                "segment_column": segment_column,
                "segment_count": len(segment_results),
                "best_segment": {
                    "name": best_segment_name,
                    "conversion_rate": best_segment_rate
                },
                "worst_segment": {
                    "name": worst_segment_name,
                    "conversion_rate": worst_segment_rate
                },
                "conversion_range": conversion_range,
                "segments_analyzed": list(segment_results.keys())
            }
            
            logger.info(f"Completed segmented funnel analysis. Best segment: {best_segment_name} ({best_segment_rate:.2%}), " \
                       f"Worst segment: {worst_segment_name} ({worst_segment_rate:.2%})")
        else:
            logger.warning("Not enough segments available for comparison")
        
        return segment_results
    
    def time_based_funnel_analysis(self,
                                 events: pd.DataFrame,
                                 funnel_steps: List[str],
                                 time_period: str = "week",
                                 user_id_col: str = "distinct_id",
                                 event_name_col: str = "event",
                                 timestamp_col: str = "time",
                                 max_days_to_convert: int = 30) -> Dict[str, Dict[str, Any]]:
        """
        Analyze funnel performance over different time periods.
        
        Args:
            events (pd.DataFrame): DataFrame containing event data from Mixpanel
            funnel_steps (List[str]): Ordered list of event names forming the funnel
            time_period (str): Time period for grouping ('day', 'week', 'month')
            user_id_col (str): Column name containing user identifiers
            event_name_col (str): Column name containing event names
            timestamp_col (str): Column name containing event timestamps
            max_days_to_convert (int): Maximum number of days allowed to complete the funnel
            
        Returns:
            Dict[str, Dict[str, Any]]: Analysis results for each time period
        """
        # Ensure the timestamp column is datetime
        if not pd.api.types.is_datetime64_any_dtype(events[timestamp_col]):
            events[timestamp_col] = pd.to_datetime(events[timestamp_col])
        
        # Create time period column
        if time_period == "day":
            events["_time_period"] = events[timestamp_col].dt.date
        elif time_period == "week":
            events["_time_period"] = events[timestamp_col].dt.to_period("W").apply(lambda x: str(x))
        elif time_period == "month":
            events["_time_period"] = events[timestamp_col].dt.to_period("M").apply(lambda x: str(x))
        else:
            logger.error(f"Invalid time period: {time_period}")
            raise ValueError(f"Invalid time period: {time_period}. Must be 'day', 'week', or 'month'")
        
        # Get unique time periods, sorted chronologically
        time_periods = sorted(events["_time_period"].unique())
        
        logger.info(f"Performing time-based funnel analysis by {time_period} with {len(time_periods)} periods")
        
        # Analyze funnel for each time period
        period_results = {}
        for period in time_periods:
            # Filter events for this period
            period_events = events[events["_time_period"] == period].copy()
            
            if len(period_events) == 0:
                logger.warning(f"No events found for period '{period}'")
                continue
            
            try:
                # Analyze funnel for this period
                result = self.analyze_funnel(
                    events=period_events,
                    funnel_steps=funnel_steps,
                    user_id_col=user_id_col,
                    event_name_col=event_name_col,
                    timestamp_col=timestamp_col,
                    max_days_to_convert=max_days_to_convert
                )
                
                # Store results for this period
                period_results[str(period)] = result
                
            except Exception as e:
                logger.error(f"Error analyzing funnel for period '{period}': {str(e)}")
        
        # Trend analysis
        if len(period_results) > 1:
            # Extract overall conversion rates by period
            conversion_trend = {
                period: results["overall_conversion_rate"] 
                for period, results in period_results.items()
            }
            
            # Calculate trend metrics
            periods_list = sorted(conversion_trend.keys())
            rates_list = [conversion_trend[p] for p in periods_list]
            
            # Calculate growth rate if we have at least 2 periods
            if len(rates_list) >= 2:
                first_rate = rates_list[0]
                last_rate = rates_list[-1]
                abs_change = last_rate - first_rate
                rel_change = abs_change / first_rate if first_rate > 0 else float('inf')
                
                # Check if trend is consistently improving or declining
                is_improving = all(rates_list[i] <= rates_list[i+1] for i in range(len(rates_list)-1))
                is_declining = all(rates_list[i] >= rates_list[i+1] for i in range(len(rates_list)-1))
                
                trend_direction = "improving" if is_improving else "declining" if is_declining else "mixed"
                
                # Add trend summary
                period_results["_trend_analysis"] = {
                    "time_period": time_period,
                    "period_count": len(period_results),
                    "first_period": periods_list[0],
                    "last_period": periods_list[-1],
                    "first_period_rate": first_rate,
                    "last_period_rate": last_rate,
                    "absolute_change": abs_change,
                    "relative_change": rel_change,
                    "trend_direction": trend_direction,
                    "all_periods": conversion_trend
                }
                
                logger.info(f"Completed time-based funnel analysis. Trend is {trend_direction} with " \
                           f"{abs_change:.2%} absolute change over {len(periods_list)} periods")
            else:
                logger.warning("Not enough time periods for trend analysis")
        else:
            logger.warning("Not enough time periods available for trend analysis")
        
        return period_results
