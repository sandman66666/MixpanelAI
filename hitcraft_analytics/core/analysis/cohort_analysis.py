"""
HitCraft AI Analytics Engine - Cohort Analysis

This module provides functionality for analyzing user cohorts, allowing
comparison of behavior patterns across different user segments over time.
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Tuple, Union

from hitcraft_analytics.utils.logging.logger import get_logger

logger = get_logger(__name__)

class CohortAnalyzer:
    """
    Analyzes user cohorts to identify patterns and differences in behavior.
    """
    
    def __init__(self):
        """
        Initialize the cohort analyzer.
        """
        logger.info("Cohort analyzer initialized")
    
    def create_retention_cohorts(self, 
                               user_events: pd.DataFrame,
                               time_column: str = 'time',
                               user_column: str = 'distinct_id',
                               event_column: str = 'event_name',
                               cohort_period: str = 'W',
                               max_periods: int = 12) -> pd.DataFrame:
        """
        Create retention cohorts based on user's first appearance.
        
        Args:
            user_events (pd.DataFrame): DataFrame with user event data.
            time_column (str): Column name for the event timestamp.
            user_column (str): Column name for the user identifier.
            event_column (str): Column name for the event name.
            cohort_period (str): Period for cohort grouping ('D' for daily, 'W' for weekly, 'M' for monthly).
            max_periods (int): Maximum number of periods to analyze.
            
        Returns:
            pd.DataFrame: Cohort retention matrix.
        """
        logger.info("Creating retention cohorts with period '%s'", cohort_period)
        
        if user_events.empty:
            logger.warning("Empty user events DataFrame")
            return pd.DataFrame()
            
        try:
            # Ensure timestamp column is datetime
            if user_events[time_column].dtype != 'datetime64[ns]':
                user_events[time_column] = pd.to_datetime(user_events[time_column])
            
            # Create cohort groups
            user_events = user_events.copy()
            
            # Get the first event date for each user
            first_event = user_events.groupby(user_column)[time_column].min().reset_index()
            first_event.columns = [user_column, 'first_event_date']
            
            # Merge first event date back to the main DataFrame
            user_events = user_events.merge(first_event, on=user_column, how='left')
            
            # Create cohort group based on the first event date
            if cohort_period == 'D':
                user_events['cohort_group'] = user_events['first_event_date'].dt.strftime('%Y-%m-%d')
            elif cohort_period == 'W':
                user_events['cohort_group'] = user_events['first_event_date'].dt.to_period('W').dt.strftime('%Y-%W')
            elif cohort_period == 'M':
                user_events['cohort_group'] = user_events['first_event_date'].dt.strftime('%Y-%m')
            else:
                logger.warning("Invalid cohort_period '%s', defaulting to 'M'", cohort_period)
                user_events['cohort_group'] = user_events['first_event_date'].dt.strftime('%Y-%m')
            
            # Calculate period index (0 for first period, 1 for second, etc.)
            if cohort_period == 'D':
                user_events['period_index'] = (user_events[time_column].dt.date - 
                                           user_events['first_event_date'].dt.date).dt.days
            elif cohort_period == 'W':
                user_events['period_index'] = ((user_events[time_column].dt.to_period('W').astype(int) - 
                                             user_events['first_event_date'].dt.to_period('W').astype(int)))
            elif cohort_period == 'M':
                user_events['period_index'] = ((user_events[time_column].dt.year - user_events['first_event_date'].dt.year) * 12 + 
                                             (user_events[time_column].dt.month - user_events['first_event_date'].dt.month))
                
            # Limit to max_periods
            user_events = user_events[user_events['period_index'] < max_periods]
            
            # Group by cohort and period_index, count unique users
            cohort_data = user_events.groupby(['cohort_group', 'period_index'])[user_column].nunique().reset_index()
            cohort_data.columns = ['cohort_group', 'period_index', 'user_count']
            
            # Get the size of each cohort
            cohort_sizes = cohort_data[cohort_data['period_index'] == 0].copy()
            cohort_sizes.columns = ['cohort_group', 'period_index', 'cohort_size']
            
            # Merge with cohort sizes
            cohort_data = cohort_data.merge(cohort_sizes[['cohort_group', 'cohort_size']], on='cohort_group', how='left')
            
            # Calculate retention rate
            cohort_data['retention_rate'] = cohort_data['user_count'] / cohort_data['cohort_size']
            
            # Create pivot table for visualization
            cohort_pivot = cohort_data.pivot_table(
                index='cohort_group',
                columns='period_index',
                values='retention_rate'
            )
            
            # Sort by cohort group
            cohort_pivot = cohort_pivot.sort_index()
            
            # Fill NaN with 0
            cohort_pivot = cohort_pivot.fillna(0)
            
            # Add cohort size as a column
            cohort_pivot['cohort_size'] = cohort_sizes.set_index('cohort_group')['cohort_size']
            
            logger.info("Created retention cohorts with %d cohort groups", len(cohort_pivot))
            return cohort_pivot
            
        except Exception as e:
            logger.error("Error creating retention cohorts: %s", str(e))
            raise
    
    def calculate_cohort_metrics(self, 
                               user_events: pd.DataFrame,
                               time_column: str = 'time',
                               user_column: str = 'distinct_id',
                               event_column: str = 'event_name',
                               metric_columns: Optional[List[str]] = None,
                               cohort_column: str = 'cohort_group',
                               period_column: str = 'period_index') -> Dict[str, pd.DataFrame]:
        """
        Calculate various metrics for each cohort and period.
        
        Args:
            user_events (pd.DataFrame): DataFrame with user event data.
            time_column (str): Column name for the event timestamp.
            user_column (str): Column name for the user identifier.
            event_column (str): Column name for the event name.
            metric_columns (Optional[List[str]]): Columns to aggregate for each cohort.
            cohort_column (str): Column name for the cohort group.
            period_column (str): Column name for the period index.
            
        Returns:
            Dict[str, pd.DataFrame]: Dictionary of metrics for each cohort.
        """
        logger.info("Calculating cohort metrics")
        
        if user_events.empty:
            logger.warning("Empty user events DataFrame")
            return {}
            
        try:
            # Default metrics if not provided
            if metric_columns is None:
                # Use available numeric columns or default to event count
                numeric_cols = user_events.select_dtypes(include=[np.number]).columns.tolist()
                metric_columns = [col for col in numeric_cols if col not in [user_column, period_column, 'cohort_size']]
                
                if not metric_columns:
                    metric_columns = ['event_count']
                    user_events['event_count'] = 1
            
            results = {}
            
            # Calculate metrics for each cohort and period
            for metric in metric_columns:
                # Check if metric exists
                if metric not in user_events.columns and metric != 'event_count':
                    logger.warning("Metric '%s' not found in DataFrame", metric)
                    continue
                
                if metric == 'event_count' and 'event_count' not in user_events.columns:
                    user_events['event_count'] = 1
                
                # Calculate mean value per user for each cohort and period
                agg_df = user_events.groupby([cohort_column, period_column, user_column])[metric].sum().reset_index()
                metric_df = agg_df.groupby([cohort_column, period_column])[metric].mean().reset_index()
                
                # Pivot for visualization
                pivot_df = metric_df.pivot_table(
                    index=cohort_column,
                    columns=period_column,
                    values=metric
                )
                
                # Sort by cohort group
                pivot_df = pivot_df.sort_index()
                
                # Fill NaN with 0
                pivot_df = pivot_df.fillna(0)
                
                results[metric] = pivot_df
            
            logger.info("Calculated %d metrics for cohorts", len(results))
            return results
            
        except Exception as e:
            logger.error("Error calculating cohort metrics: %s", str(e))
            raise
    
    def compare_cohorts(self, 
                      cohort_data: pd.DataFrame,
                      metric: str,
                      cohort_groups: Optional[List[str]] = None,
                      periods: Optional[List[int]] = None) -> Dict[str, Any]:
        """
        Compare metrics between different cohorts.
        
        Args:
            cohort_data (pd.DataFrame): Cohort data in pivot table format.
            metric (str): Name of the metric being compared.
            cohort_groups (Optional[List[str]]): List of cohort groups to compare.
                If None, all cohorts will be compared.
            periods (Optional[List[int]]): List of periods to compare.
                If None, all periods will be compared.
            
        Returns:
            Dict[str, Any]: Comparison results.
        """
        logger.info("Comparing cohorts for metric '%s'", metric)
        
        if cohort_data.empty:
            logger.warning("Empty cohort data DataFrame")
            return {"error": "No cohort data available"}
            
        try:
            # Filter cohort groups if specified
            if cohort_groups:
                cohort_data = cohort_data.loc[cohort_data.index.isin(cohort_groups)]
                
            # Filter periods if specified
            if periods:
                available_periods = [col for col in cohort_data.columns if isinstance(col, int)]
                periods = [p for p in periods if p in available_periods]
                cohort_data = cohort_data[periods]
            
            if cohort_data.empty:
                logger.warning("No data after filtering cohorts and periods")
                return {"error": "No data available for the specified cohorts and periods"}
            
            # Calculate statistics
            results = {
                "metric": metric,
                "cohorts": len(cohort_data),
                "periods": len([col for col in cohort_data.columns if isinstance(col, int)]),
                "best_cohort": None,
                "worst_cohort": None,
                "trend": None,
                "period_comparison": [],
                "cohort_comparison": []
            }
            
            # Identify best and worst cohorts (based on the latest period)
            latest_period = max([col for col in cohort_data.columns if isinstance(col, int)])
            if not np.isnan(cohort_data[latest_period]).all():
                best_cohort = cohort_data[latest_period].idxmax()
                worst_cohort = cohort_data[latest_period].idxmin()
                
                results["best_cohort"] = {
                    "name": best_cohort,
                    "value": cohort_data.loc[best_cohort, latest_period],
                    "period": latest_period
                }
                
                results["worst_cohort"] = {
                    "name": worst_cohort,
                    "value": cohort_data.loc[worst_cohort, latest_period],
                    "period": latest_period
                }
            
            # Compare cohorts' performance over time
            for cohort in cohort_data.index:
                cohort_values = cohort_data.loc[cohort]
                cohort_periods = [col for col in cohort_values.index if isinstance(col, int)]
                
                if len(cohort_periods) >= 2:
                    first_period = min(cohort_periods)
                    last_period = max(cohort_periods)
                    
                    first_value = cohort_values[first_period]
                    last_value = cohort_values[last_period]
                    
                    if first_value > 0:
                        change_pct = ((last_value - first_value) / first_value) * 100
                    else:
                        change_pct = float('inf') if last_value > 0 else 0
                    
                    results["cohort_comparison"].append({
                        "cohort": cohort,
                        "first_period": first_period,
                        "last_period": last_period,
                        "first_value": first_value,
                        "last_value": last_value,
                        "absolute_change": last_value - first_value,
                        "percent_change": change_pct
                    })
            
            # Sort cohort comparison by percent change
            if results["cohort_comparison"]:
                results["cohort_comparison"] = sorted(
                    results["cohort_comparison"],
                    key=lambda x: x["percent_change"],
                    reverse=True
                )
                
                # Identify overall trend
                avg_change = sum(c["percent_change"] for c in results["cohort_comparison"] 
                               if c["percent_change"] != float('inf')) / len(results["cohort_comparison"])
                
                results["trend"] = {
                    "direction": "improving" if avg_change > 0 else "declining" if avg_change < 0 else "stable",
                    "average_change_percent": avg_change
                }
            
            # Compare performance across periods
            period_cols = [col for col in cohort_data.columns if isinstance(col, int)]
            for period in period_cols:
                period_data = cohort_data[period]
                
                results["period_comparison"].append({
                    "period": period,
                    "average": period_data.mean(),
                    "median": period_data.median(),
                    "min": period_data.min(),
                    "max": period_data.max(),
                    "std_dev": period_data.std()
                })
            
            # Sort period comparison by period
            results["period_comparison"] = sorted(
                results["period_comparison"],
                key=lambda x: x["period"]
            )
            
            logger.info("Completed cohort comparison for metric '%s'", metric)
            return results
            
        except Exception as e:
            logger.error("Error comparing cohorts: %s", str(e))
            return {"error": str(e)}
    
    def segment_users(self, 
                    user_data: pd.DataFrame,
                    segment_by: List[str],
                    metrics: List[str]) -> Dict[str, Any]:
        """
        Segment users based on specified attributes and compare metrics across segments.
        
        Args:
            user_data (pd.DataFrame): DataFrame with user data.
            segment_by (List[str]): List of columns to use for segmentation.
            metrics (List[str]): List of metrics to compare across segments.
            
        Returns:
            Dict[str, Any]: Segmentation analysis results.
        """
        logger.info("Segmenting users by %s", segment_by)
        
        if user_data.empty:
            logger.warning("Empty user data DataFrame")
            return {"error": "No user data available"}
            
        try:
            # Validate that segment columns exist
            missing_cols = [col for col in segment_by if col not in user_data.columns]
            if missing_cols:
                logger.warning("Segment columns not found: %s", missing_cols)
                segment_by = [col for col in segment_by if col in user_data.columns]
                
                if not segment_by:
                    return {"error": "No valid segment columns provided"}
            
            # Validate that metric columns exist
            missing_metrics = [m for m in metrics if m not in user_data.columns]
            if missing_metrics:
                logger.warning("Metric columns not found: %s", missing_metrics)
                metrics = [m for m in metrics if m in user_data.columns]
                
                if not metrics:
                    return {"error": "No valid metric columns provided"}
            
            results = {
                "segments": {},
                "metrics": metrics,
                "segment_columns": segment_by,
                "total_users": len(user_data)
            }
            
            # Analyze each segmentation dimension
            for segment_col in segment_by:
                # Get unique segment values
                segments = user_data[segment_col].unique()
                
                segment_results = {
                    "values": [],
                    "comparison": []
                }
                
                # Calculate overall metrics for reference
                overall_metrics = {
                    metric: {
                        "mean": user_data[metric].mean(),
                        "median": user_data[metric].median()
                    } for metric in metrics if pd.api.types.is_numeric_dtype(user_data[metric])
                }
                
                # Analyze each segment value
                for segment_value in segments:
                    # Skip None or NaN values
                    if pd.isna(segment_value):
                        continue
                        
                    segment_users = user_data[user_data[segment_col] == segment_value]
                    user_count = len(segment_users)
                    
                    # Skip segments with too few users
                    if user_count < 10:
                        continue
                    
                    segment_data = {
                        "value": segment_value,
                        "user_count": user_count,
                        "percentage": (user_count / len(user_data)) * 100,
                        "metrics": {}
                    }
                    
                    # Calculate metrics for this segment
                    for metric in metrics:
                        if not pd.api.types.is_numeric_dtype(user_data[metric]):
                            continue
                            
                        metric_values = segment_users[metric].dropna()
                        
                        if len(metric_values) == 0:
                            continue
                            
                        mean_value = metric_values.mean()
                        median_value = metric_values.median()
                        
                        # Calculate difference from overall average
                        if metric in overall_metrics:
                            mean_diff_pct = ((mean_value - overall_metrics[metric]["mean"]) / 
                                          overall_metrics[metric]["mean"]) * 100 if overall_metrics[metric]["mean"] != 0 else 0
                        else:
                            mean_diff_pct = 0
                        
                        segment_data["metrics"][metric] = {
                            "mean": mean_value,
                            "median": median_value,
                            "min": metric_values.min(),
                            "max": metric_values.max(),
                            "std": metric_values.std(),
                            "diff_from_overall_pct": mean_diff_pct
                        }
                    
                    segment_results["values"].append(segment_data)
                
                # Sort segments by user count
                segment_results["values"] = sorted(
                    segment_results["values"],
                    key=lambda x: x["user_count"],
                    reverse=True
                )
                
                # Find significant differences between segments for each metric
                for metric in metrics:
                    if not pd.api.types.is_numeric_dtype(user_data[metric]):
                        continue
                        
                    # Compare each segment to overall average
                    comparisons = []
                    
                    for segment_data in segment_results["values"]:
                        if metric in segment_data["metrics"]:
                            diff_pct = segment_data["metrics"][metric]["diff_from_overall_pct"]
                            
                            # Only include significant differences (> 10%)
                            if abs(diff_pct) >= 10:
                                comparisons.append({
                                    "segment_value": segment_data["value"],
                                    "mean": segment_data["metrics"][metric]["mean"],
                                    "diff_percent": diff_pct,
                                    "user_count": segment_data["user_count"],
                                    "user_percent": segment_data["percentage"]
                                })
                    
                    # Sort by absolute difference
                    comparisons = sorted(
                        comparisons,
                        key=lambda x: abs(x["diff_percent"]),
                        reverse=True
                    )
                    
                    segment_results["comparison"].append({
                        "metric": metric,
                        "overall_mean": overall_metrics.get(metric, {}).get("mean", 0),
                        "segments": comparisons
                    })
                
                results["segments"][segment_col] = segment_results
            
            logger.info("Completed user segmentation analysis")
            return results
            
        except Exception as e:
            logger.error("Error segmenting users: %s", str(e))
            return {"error": str(e)}
