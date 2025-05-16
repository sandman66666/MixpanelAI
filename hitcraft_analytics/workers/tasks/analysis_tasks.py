"""
Analysis Tasks

This module implements scheduled tasks for data analysis operations.
These tasks run after data has been successfully collected from Mixpanel.
"""

import logging
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional

from hitcraft_analytics.data.repositories.events_repository import EventsRepository
from hitcraft_analytics.core.analysis.trend_detection import TrendDetector
from hitcraft_analytics.core.analysis.cohort_analysis import CohortAnalyzer
from hitcraft_analytics.core.analysis.funnel_analysis import FunnelAnalyzer
from hitcraft_analytics.workers.scheduler.tasks import AnalysisTask
from hitcraft_analytics.utils.logging_config import setup_logger

# Set up logger
logger = setup_logger("workers.tasks.analysis")

class TrendAnalysisTask(AnalysisTask):
    """
    Task for analyzing trends in event data.
    
    This task identifies significant trends, change points, and anomalies
    in key metrics over time.
    """
    
    def __init__(self):
        task_id = "trend_analysis"
        description = "Analyze trends, change points, and anomalies in event data"
        super().__init__(task_id, description)
        
        self.repository = EventsRepository()
        self.trend_detector = TrendDetector()
    
    def run(self, 
            from_date: Optional[str] = None, 
            to_date: Optional[str] = None,
            event_metrics: Optional[List[str]] = None,
            min_data_points: int = 14
           ) -> Dict[str, Any]:
        """
        Execute trend analysis on event data.
        
        Args:
            from_date: Start date in YYYY-MM-DD format
            to_date: End date in YYYY-MM-DD format
            event_metrics: List of metrics to analyze
            min_data_points: Minimum number of data points required
            
        Returns:
            Dict[str, Any]: Results of trend analysis
        """
        # Default date range: last 30 days
        if not from_date or not to_date:
            to_date_obj = datetime.now().date()
            from_date_obj = to_date_obj - timedelta(days=30)
            to_date = to_date or to_date_obj.isoformat()
            from_date = from_date or from_date_obj.isoformat()
        
        logger.info(f"Running trend analysis for period {from_date} to {to_date}")
        
        # Default metrics if not specified
        if not event_metrics:
            event_metrics = ["total_events", "unique_users", "sessions"]
        
        # Get event volume data from repository
        try:
            metrics_data = self.repository.get_event_metrics(
                from_date=from_date,
                to_date=to_date,
                group_by="day",
                metrics=event_metrics
            )
            
            if not metrics_data or all(len(metrics_data.get(metric, [])) < min_data_points for metric in event_metrics):
                logger.warning(f"Insufficient data for trend analysis: {from_date} to {to_date}")
                return {
                    "status": "insufficient_data",
                    "from_date": from_date,
                    "to_date": to_date
                }
            
            # Analyze trends for each metric
            trends_results = {}
            
            for metric in event_metrics:
                if metric in metrics_data and len(metrics_data[metric]) >= min_data_points:
                    # Get time series data for this metric
                    time_series = metrics_data[metric]
                    
                    # Detect trends
                    linear_trend = self.trend_detector.detect_linear_trend(time_series)
                    change_points = self.trend_detector.detect_change_points(time_series)
                    anomalies = self.trend_detector.detect_anomalies(time_series)
                    
                    trends_results[metric] = {
                        "linear_trend": linear_trend,
                        "change_points": change_points,
                        "anomalies": anomalies,
                        "data_points": len(time_series)
                    }
            
            logger.info(f"Completed trend analysis for {len(trends_results)} metrics")
            
            # Store analysis results for later use
            self.repository.store_analysis_results(
                analysis_type="trend",
                from_date=from_date,
                to_date=to_date,
                results=trends_results
            )
            
            return {
                "status": "completed",
                "metrics_analyzed": list(trends_results.keys()),
                "from_date": from_date,
                "to_date": to_date,
                "trends_detected": sum(1 for m in trends_results.values() 
                                      if m.get("linear_trend", {}).get("detected", False)),
                "change_points_detected": sum(len(m.get("change_points", [])) for m in trends_results.values()),
                "anomalies_detected": sum(len(m.get("anomalies", [])) for m in trends_results.values())
            }
            
        except Exception as e:
            logger.error(f"Error during trend analysis: {str(e)}", exc_info=True)
            raise


class FunnelAnalysisTask(AnalysisTask):
    """
    Task for analyzing conversion funnels.
    
    This task analyzes user progression through predefined funnels,
    calculates conversion rates, and identifies drop-off points.
    """
    
    def __init__(self, funnel_definitions: Optional[Dict[str, List[str]]] = None):
        """
        Initialize the funnel analysis task.
        
        Args:
            funnel_definitions: Dictionary mapping funnel names to lists of event steps
        """
        task_id = "funnel_analysis"
        description = "Analyze user conversion funnels and identify drop-off points"
        super().__init__(task_id, description)
        
        # Set default funnels if none provided
        self.funnel_definitions = funnel_definitions or {
            "signup_funnel": [
                "visit_signup_page", 
                "begin_signup", 
                "complete_signup", 
                "account_activation"
            ],
            "purchase_funnel": [
                "view_product",
                "add_to_cart",
                "begin_checkout",
                "complete_purchase"
            ]
        }
        
        self.repository = EventsRepository()
        self.funnel_analyzer = FunnelAnalyzer()
    
    def run(self,
            from_date: Optional[str] = None,
            to_date: Optional[str] = None,
            funnels_to_analyze: Optional[List[str]] = None,
            segment_by: Optional[List[str]] = None
           ) -> Dict[str, Any]:
        """
        Execute funnel analysis on event data.
        
        Args:
            from_date: Start date in YYYY-MM-DD format
            to_date: End date in YYYY-MM-DD format
            funnels_to_analyze: List of funnel names to analyze, or None for all
            segment_by: Properties to segment the analysis by
            
        Returns:
            Dict[str, Any]: Results of funnel analysis
        """
        # Default date range: last 30 days
        if not from_date or not to_date:
            to_date_obj = datetime.now().date()
            from_date_obj = to_date_obj - timedelta(days=30)
            to_date = to_date or to_date_obj.isoformat()
            from_date = from_date or from_date_obj.isoformat()
        
        logger.info(f"Running funnel analysis for period {from_date} to {to_date}")
        
        # Determine which funnels to analyze
        funnels = {}
        if funnels_to_analyze:
            funnels = {name: steps for name, steps in self.funnel_definitions.items() 
                      if name in funnels_to_analyze}
        else:
            funnels = self.funnel_definitions
        
        if not funnels:
            logger.warning("No valid funnels to analyze")
            return {
                "status": "no_funnels",
                "from_date": from_date,
                "to_date": to_date
            }
        
        # Default segmentation properties
        segment_properties = segment_by or ["user_type", "platform"]
        
        # Run analysis for each funnel
        all_results = {}
        analyzed_count = 0
        
        for funnel_name, funnel_steps in funnels.items():
            try:
                logger.info(f"Analyzing funnel: {funnel_name} with steps: {funnel_steps}")
                
                # Use the repository's advanced funnel analysis
                funnel_results = self.repository.analyze_funnel_advanced(
                    funnel_steps=funnel_steps,
                    from_date=from_date,
                    to_date=to_date,
                    segment_column=segment_properties,
                    time_period="day",
                    compare_to_previous=True
                )
                
                if not funnel_results or "error" in funnel_results:
                    logger.warning(f"Failed to analyze funnel {funnel_name}: {funnel_results.get('error', 'No data')}")
                    continue
                
                # Store results
                all_results[funnel_name] = funnel_results
                analyzed_count += 1
                
                # Store analysis results for later use
                self.repository.store_analysis_results(
                    analysis_type="funnel",
                    from_date=from_date,
                    to_date=to_date,
                    results={funnel_name: funnel_results},
                    metadata={"funnel_steps": funnel_steps}
                )
                
                logger.info(f"Successfully analyzed funnel: {funnel_name}")
                
            except Exception as e:
                logger.error(f"Error analyzing funnel {funnel_name}: {str(e)}", exc_info=True)
        
        logger.info(f"Completed funnel analysis for {analyzed_count} funnels")
        
        return {
            "status": "completed" if analyzed_count > 0 else "no_results",
            "funnels_analyzed": list(all_results.keys()),
            "from_date": from_date,
            "to_date": to_date,
            "segment_properties": segment_properties
        }


class CohortAnalysisTask(AnalysisTask):
    """
    Task for analyzing user cohorts.
    
    This task creates and analyzes user cohorts to understand retention,
    engagement, and conversion patterns over time.
    """
    
    def __init__(self):
        task_id = "cohort_analysis"
        description = "Analyze user cohorts for retention and engagement"
        super().__init__(task_id, description)
        
        self.repository = EventsRepository()
        self.cohort_analyzer = CohortAnalyzer()
    
    def run(self,
            from_date: Optional[str] = None,
            to_date: Optional[str] = None,
            cohort_period: str = "week",
            max_periods: int = 12
           ) -> Dict[str, Any]:
        """
        Execute cohort analysis on user data.
        
        Args:
            from_date: Start date in YYYY-MM-DD format
            to_date: End date in YYYY-MM-DD format
            cohort_period: Time unit for cohort grouping ('day', 'week', or 'month')
            max_periods: Maximum number of periods to analyze
            
        Returns:
            Dict[str, Any]: Results of cohort analysis
        """
        # Default date range
        if not from_date or not to_date:
            periods = {"day": 30, "week": 12, "month": 6}
            period_days = periods.get(cohort_period, 90)
            
            to_date_obj = datetime.now().date()
            from_date_obj = to_date_obj - timedelta(days=period_days)
            to_date = to_date or to_date_obj.isoformat()
            from_date = from_date or from_date_obj.isoformat()
        
        logger.info(f"Running cohort analysis for period {from_date} to {to_date}")
        
        try:
            # Get user events data
            user_events = self.repository.get_user_events(
                from_date=from_date,
                to_date=to_date
            )
            
            if user_events.empty:
                logger.warning(f"No user event data available for cohort analysis: {from_date} to {to_date}")
                return {
                    "status": "no_data",
                    "from_date": from_date,
                    "to_date": to_date
                }
            
            # Create retention cohorts
            retention_cohorts = self.cohort_analyzer.create_retention_cohorts(
                user_events=user_events,
                cohort_period=cohort_period,
                max_periods=max_periods
            )
            
            # Calculate cohort metrics
            cohort_metrics = self.cohort_analyzer.calculate_cohort_metrics(
                cohorts=retention_cohorts,
                metrics=["retention", "engagement", "conversion"]
            )
            
            # Store results
            self.repository.store_analysis_results(
                analysis_type="cohort",
                from_date=from_date,
                to_date=to_date,
                results={
                    "retention_cohorts": retention_cohorts.to_dict() if not retention_cohorts.empty else {},
                    "cohort_metrics": cohort_metrics
                },
                metadata={
                    "cohort_period": cohort_period,
                    "max_periods": max_periods
                }
            )
            
            logger.info(f"Completed cohort analysis with {len(cohort_metrics)} metrics")
            
            return {
                "status": "completed",
                "from_date": from_date,
                "to_date": to_date,
                "cohort_period": cohort_period,
                "cohorts_analyzed": len(retention_cohorts) if not retention_cohorts.empty else 0,
                "metrics_calculated": list(cohort_metrics.keys())
            }
            
        except Exception as e:
            logger.error(f"Error during cohort analysis: {str(e)}", exc_info=True)
            raise
