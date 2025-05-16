"""
HitCraft AI Analytics Engine - Trend Detection

This module detects significant trends in time-series metrics to identify
growth opportunities, risks, and patterns worthy of attention.
"""

import numpy as np
import pandas as pd
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Tuple, Union
from scipy import stats
import statsmodels.api as sm

from hitcraft_analytics.utils.logging.logger import get_logger

logger = get_logger(__name__)

class TrendDetector:
    """
    Detects significant trends in time-series data.
    """
    
    def __init__(self, 
                min_data_points: int = 7,
                significance_level: float = 0.05,
                change_threshold_percent: float = 10.0):
        """
        Initialize the trend detector.
        
        Args:
            min_data_points (int): Minimum number of data points required for trend analysis.
            significance_level (float): P-value threshold for statistical significance.
            change_threshold_percent (float): Minimum percentage change to consider a trend significant.
        """
        self.min_data_points = min_data_points
        self.significance_level = significance_level
        self.change_threshold_percent = change_threshold_percent
        
        logger.info("Trend detector initialized")
    
    def detect_linear_trend(self, time_series: pd.Series) -> Dict[str, Any]:
        """
        Detect if there is a statistically significant linear trend in the time series.
        
        Args:
            time_series (pd.Series): Time series data with datetime index.
            
        Returns:
            Dict[str, Any]: Trend analysis results containing:
                - trend_detected: Boolean indicating if a significant trend was found
                - direction: 'increasing', 'decreasing', or 'stable'
                - slope: Rate of change per day
                - p_value: Statistical significance of the trend
                - confidence: Confidence in the trend detection (1-p_value)
                - percent_change: Percentage change over the period
                - absolute_change: Absolute change over the period
                - r_squared: R-squared value of the linear model
        """
        logger.info("Detecting linear trend in time series with %d points", len(time_series))
        
        # Check if we have enough data points
        if len(time_series) < self.min_data_points:
            logger.warning("Not enough data points for trend detection (%d < %d)", 
                         len(time_series), self.min_data_points)
            return self._create_no_trend_result()
        
        try:
            # Convert index to numeric (days since start)
            if isinstance(time_series.index, pd.DatetimeIndex):
                days_since_start = (time_series.index - time_series.index.min()).total_seconds() / (24 * 3600)
            else:
                # If not datetime index, just use numeric indices
                days_since_start = np.arange(len(time_series))
            
            # Clean data (remove NaN values)
            valid_mask = ~np.isnan(time_series.values)
            if sum(valid_mask) < self.min_data_points:
                logger.warning("Not enough valid data points after removing NaN values")
                return self._create_no_trend_result()
                
            x = days_since_start[valid_mask]
            y = time_series.values[valid_mask]
            
            # Add constant for statsmodels
            X = sm.add_constant(x)
            
            # Fit linear model
            model = sm.OLS(y, X)
            results = model.fit()
            
            # Extract slope and p-value
            slope = results.params[1]
            p_value = results.pvalues[1]
            r_squared = results.rsquared
            
            # Calculate percentage and absolute change
            first_value = time_series.iloc[0]
            last_value = time_series.iloc[-1]
            
            if first_value == 0:
                # Avoid division by zero
                percent_change = np.inf if last_value > 0 else -np.inf if last_value < 0 else 0
            else:
                percent_change = ((last_value - first_value) / abs(first_value)) * 100
                
            absolute_change = last_value - first_value
            
            # Determine if trend is significant
            trend_detected = (p_value < self.significance_level and 
                             abs(percent_change) >= self.change_threshold_percent)
            
            # Determine direction
            if trend_detected:
                direction = "increasing" if slope > 0 else "decreasing"
            else:
                direction = "stable"
            
            logger.info("Trend detection results: detected=%s, direction=%s, p_value=%.4f, percent_change=%.2f%%",
                      trend_detected, direction, p_value, percent_change)
            
            return {
                "trend_detected": trend_detected,
                "direction": direction,
                "slope": slope,
                "p_value": p_value,
                "confidence": 1 - p_value,
                "percent_change": percent_change,
                "absolute_change": absolute_change,
                "r_squared": r_squared
            }
            
        except Exception as e:
            logger.error("Error in linear trend detection: %s", str(e))
            return self._create_no_trend_result()
    
    def detect_change_points(self, time_series: pd.Series, 
                           window_size: Optional[int] = None) -> List[Dict[str, Any]]:
        """
        Detect significant change points in the time series.
        
        Args:
            time_series (pd.Series): Time series data with datetime index.
            window_size (Optional[int]): Size of the sliding window for change detection.
                If None, will use 1/4 of the series length or 7, whichever is larger.
            
        Returns:
            List[Dict[str, Any]]: List of detected change points with:
                - index: Position in the time series
                - timestamp: Datetime of the change point
                - value_before: Average value before change
                - value_after: Average value after change
                - percent_change: Percentage change
                - absolute_change: Absolute change
                - significance: Statistical significance score
        """
        logger.info("Detecting change points in time series with %d points", len(time_series))
        
        # Check if we have enough data points
        if len(time_series) < self.min_data_points:
            logger.warning("Not enough data points for change point detection (%d < %d)", 
                         len(time_series), self.min_data_points)
            return []
        
        try:
            # Determine window size if not provided
            if window_size is None:
                window_size = max(7, len(time_series) // 4)
                
            # Ensure window size is not too large
            window_size = min(window_size, len(time_series) // 2)
            
            change_points = []
            
            # Exclude first and last windows from change point detection
            for i in range(window_size, len(time_series) - window_size):
                # Get values before and after the potential change point
                values_before = time_series.iloc[i-window_size:i].values
                values_after = time_series.iloc[i:i+window_size].values
                
                # Clean data (remove NaN values)
                values_before = values_before[~np.isnan(values_before)]
                values_after = values_after[~np.isnan(values_after)]
                
                if len(values_before) < 3 or len(values_after) < 3:
                    continue
                
                # Calculate means
                mean_before = np.mean(values_before)
                mean_after = np.mean(values_after)
                
                # Calculate percentage change
                if mean_before == 0:
                    percent_change = np.inf if mean_after > 0 else -np.inf if mean_after < 0 else 0
                else:
                    percent_change = ((mean_after - mean_before) / abs(mean_before)) * 100
                
                absolute_change = mean_after - mean_before
                
                # Perform t-test to determine if change is significant
                t_stat, p_value = stats.ttest_ind(values_before, values_after, equal_var=False)
                
                # Check if change is significant
                if (p_value < self.significance_level and 
                    abs(percent_change) >= self.change_threshold_percent):
                    
                    change_point = {
                        "index": i,
                        "timestamp": time_series.index[i],
                        "value_before": mean_before,
                        "value_after": mean_after,
                        "percent_change": percent_change,
                        "absolute_change": absolute_change,
                        "significance": 1 - p_value
                    }
                    
                    change_points.append(change_point)
                    
                    logger.info("Detected change point at %s: %.2f%% change, p-value=%.4f",
                              time_series.index[i], percent_change, p_value)
            
            # Filter out overlapping change points (keep the most significant)
            if change_points:
                filtered_change_points = self._filter_overlapping_change_points(
                    change_points, window_size)
                
                logger.info("Detected %d change points after filtering", len(filtered_change_points))
                return filtered_change_points
                
            return []
            
        except Exception as e:
            logger.error("Error in change point detection: %s", str(e))
            return []
    
    def detect_seasonality(self, time_series: pd.Series, 
                         frequencies: Optional[List[int]] = None) -> Dict[str, Any]:
        """
        Detect seasonality patterns in the time series.
        
        Args:
            time_series (pd.Series): Time series data with datetime index.
            frequencies (Optional[List[int]]): List of frequencies to check for seasonality.
                If None, will check for daily (1), weekly (7), and monthly (30) seasonality.
            
        Returns:
            Dict[str, Any]: Seasonality analysis results containing:
                - seasonality_detected: Boolean indicating if seasonality was found
                - strongest_period: Period with the strongest seasonal pattern
                - strength: Strength of the seasonality pattern (0-1)
                - periods: Details of all tested periods
        """
        logger.info("Detecting seasonality in time series with %d points", len(time_series))
        
        # Check if we have enough data points
        if len(time_series) < self.min_data_points * 2:
            logger.warning("Not enough data points for seasonality detection (%d < %d)", 
                         len(time_series), self.min_data_points * 2)
            return {"seasonality_detected": False, "strongest_period": None, "strength": 0, "periods": []}
        
        try:
            # Default frequencies to check
            if frequencies is None:
                if isinstance(time_series.index, pd.DatetimeIndex):
                    # Determine frequency based on data
                    freq = pd.infer_freq(time_series.index)
                    if freq and 'D' in freq:  # Daily data
                        frequencies = [1, 7, 30]  # Daily, weekly, monthly
                    elif freq and 'H' in freq:  # Hourly data
                        frequencies = [24, 24*7]  # Daily, weekly
                    else:  # Default
                        frequencies = [1, 7, 30]
                else:
                    frequencies = [1, 7, 30]
            
            results = []
            
            # Check for each frequency
            for freq in frequencies:
                if len(time_series) < freq * 2:
                    logger.info("Skipping frequency %d, not enough data points", freq)
                    continue
                
                # Calculate autocorrelation
                autocorr = sm.tsa.acf(time_series.dropna(), nlags=freq*2)
                
                # Check for seasonal peak at the frequency
                peak_value = autocorr[freq]
                significance = abs(peak_value) > 1.96 / np.sqrt(len(time_series))
                
                result = {
                    "period": freq,
                    "autocorrelation": peak_value,
                    "significant": significance,
                    "p_value": 1 - significance  # Simplified p-value
                }
                
                results.append(result)
            
            # Find strongest seasonal pattern
            significant_results = [r for r in results if r["significant"]]
            
            if significant_results:
                strongest = max(significant_results, key=lambda x: abs(x["autocorrelation"]))
                seasonality_detected = True
                strength = abs(strongest["autocorrelation"])
                strongest_period = strongest["period"]
                
                logger.info("Seasonality detected with period %d, strength %.2f", 
                          strongest_period, strength)
            else:
                seasonality_detected = False
                strength = 0
                strongest_period = None
                
                logger.info("No significant seasonality detected")
            
            return {
                "seasonality_detected": seasonality_detected,
                "strongest_period": strongest_period,
                "strength": strength,
                "periods": results
            }
            
        except Exception as e:
            logger.error("Error in seasonality detection: %s", str(e))
            return {"seasonality_detected": False, "strongest_period": None, "strength": 0, "periods": []}
    
    def detect_anomalies(self, time_series: pd.Series, 
                       window_size: Optional[int] = None,
                       std_threshold: float = 3.0) -> List[Dict[str, Any]]:
        """
        Detect anomalies in the time series using rolling statistics.
        
        Args:
            time_series (pd.Series): Time series data with datetime index.
            window_size (Optional[int]): Size of the rolling window.
                If None, will use 1/4 of the series length or 7, whichever is larger.
            std_threshold (float): Number of standard deviations for anomaly threshold.
            
        Returns:
            List[Dict[str, Any]]: List of detected anomalies with:
                - index: Position in the time series
                - timestamp: Datetime of the anomaly
                - value: Anomalous value
                - expected_value: Expected value (rolling mean)
                - deviation: Number of standard deviations from the mean
                - percent_deviation: Percentage deviation from the mean
        """
        logger.info("Detecting anomalies in time series with %d points", len(time_series))
        
        # Check if we have enough data points
        if len(time_series) < self.min_data_points:
            logger.warning("Not enough data points for anomaly detection (%d < %d)", 
                         len(time_series), self.min_data_points)
            return []
        
        try:
            # Determine window size if not provided
            if window_size is None:
                window_size = max(7, len(time_series) // 4)
            
            # Calculate rolling statistics
            rolling_mean = time_series.rolling(window=window_size, center=True).mean()
            rolling_std = time_series.rolling(window=window_size, center=True).std()
            
            # Calculate z-scores
            z_scores = (time_series - rolling_mean) / rolling_std
            
            # Identify anomalies
            anomalies = []
            
            for i in range(len(time_series)):
                # Skip points with insufficient history (beginning and end)
                if np.isnan(z_scores.iloc[i]):
                    continue
                
                # Check if point exceeds threshold
                if abs(z_scores.iloc[i]) > std_threshold:
                    value = time_series.iloc[i]
                    expected = rolling_mean.iloc[i]
                    
                    # Calculate percentage deviation
                    if expected == 0:
                        pct_deviation = np.inf if value > 0 else -np.inf if value < 0 else 0
                    else:
                        pct_deviation = ((value - expected) / abs(expected)) * 100
                    
                    anomaly = {
                        "index": i,
                        "timestamp": time_series.index[i],
                        "value": value,
                        "expected_value": expected,
                        "deviation": z_scores.iloc[i],
                        "percent_deviation": pct_deviation
                    }
                    
                    anomalies.append(anomaly)
                    
                    logger.info("Detected anomaly at %s: value=%.2f, expected=%.2f, deviation=%.2f σ",
                              time_series.index[i], value, expected, z_scores.iloc[i])
            
            logger.info("Detected %d anomalies in time series", len(anomalies))
            return anomalies
            
        except Exception as e:
            logger.error("Error in anomaly detection: %s", str(e))
            return []
    
    def analyze_time_series(self, time_series: pd.Series, 
                          name: str,
                          detect_anomalies: bool = True,
                          detect_change_points: bool = True,
                          detect_seasonality: bool = True) -> Dict[str, Any]:
        """
        Perform comprehensive analysis of a time series, including trend, 
        anomalies, change points, and seasonality.
        
        Args:
            time_series (pd.Series): Time series data with datetime index.
            name (str): Name of the metric being analyzed.
            detect_anomalies (bool): Whether to detect anomalies.
            detect_change_points (bool): Whether to detect change points.
            detect_seasonality (bool): Whether to detect seasonality.
            
        Returns:
            Dict[str, Any]: Comprehensive analysis results.
        """
        logger.info("Performing comprehensive analysis of '%s' time series with %d points",
                  name, len(time_series))
        
        results = {
            "metric_name": name,
            "data_points": len(time_series),
            "time_range": {
                "start": time_series.index.min(),
                "end": time_series.index.max()
            },
            "basic_stats": {
                "mean": time_series.mean(),
                "median": time_series.median(),
                "min": time_series.min(),
                "max": time_series.max(),
                "std": time_series.std()
            }
        }
        
        # Detect trend
        trend_result = self.detect_linear_trend(time_series)
        results["trend"] = trend_result
        
        # Detect anomalies if requested
        if detect_anomalies:
            anomalies = self.detect_anomalies(time_series)
            results["anomalies"] = anomalies
            results["anomaly_count"] = len(anomalies)
        
        # Detect change points if requested
        if detect_change_points:
            change_points = self.detect_change_points(time_series)
            results["change_points"] = change_points
            results["change_point_count"] = len(change_points)
        
        # Detect seasonality if requested
        if detect_seasonality:
            seasonality = self.detect_seasonality(time_series)
            results["seasonality"] = seasonality
        
        logger.info("Completed analysis of '%s'", name)
        return results
    
    def _create_no_trend_result(self) -> Dict[str, Any]:
        """
        Create a default result for cases where trend detection can't be performed.
        
        Returns:
            Dict[str, Any]: Default trend analysis result.
        """
        return {
            "trend_detected": False,
            "direction": "stable",
            "slope": 0,
            "p_value": 1.0,
            "confidence": 0.0,
            "percent_change": 0.0,
            "absolute_change": 0.0,
            "r_squared": 0.0
        }
    
    def _filter_overlapping_change_points(self, 
                                        change_points: List[Dict[str, Any]], 
                                        window_size: int) -> List[Dict[str, Any]]:
        """
        Filter out overlapping change points, keeping the most significant ones.
        
        Args:
            change_points (List[Dict[str, Any]]): List of detected change points.
            window_size (int): Window size used for detection.
            
        Returns:
            List[Dict[str, Any]]: Filtered list of change points.
        """
        if not change_points:
            return []
            
        # Sort by significance
        sorted_points = sorted(change_points, key=lambda x: x["significance"], reverse=True)
        
        filtered_points = []
        used_indices = set()
        
        for point in sorted_points:
            index = point["index"]
            
            # Check if this point overlaps with any previously selected points
            overlap = False
            for i in range(index - window_size, index + window_size + 1):
                if i in used_indices:
                    overlap = True
                    break
            
            if not overlap:
                # Add point to filtered list
                filtered_points.append(point)
                
                # Mark indices as used
                for i in range(index - window_size // 2, index + window_size // 2 + 1):
                    used_indices.add(i)
        
        return filtered_points
