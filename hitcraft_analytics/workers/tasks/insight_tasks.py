"""
Insight Generation Tasks

This module implements scheduled tasks for generating insights from analyzed data.
These tasks run after analysis tasks have completed and transform analytical
results into actionable business insights.
"""

import logging
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional

from hitcraft_analytics.data.repositories.events_repository import EventsRepository
from hitcraft_analytics.insights.insights_engine import InsightsEngine
from hitcraft_analytics.insights.processors.insight_prioritizer import InsightPrioritizer
from hitcraft_analytics.insights.processors.insight_enricher import InsightEnricher
from hitcraft_analytics.workers.scheduler.tasks import InsightGenerationTask
from hitcraft_analytics.utils.logging_config import setup_logger

# Set up logger
logger = setup_logger("workers.tasks.insights")

class DailyInsightsGenerationTask(InsightGenerationTask):
    """
    Task for generating daily insights from analyzed data.
    
    This task processes the results of various analyses and generates
    actionable insights with business context.
    """
    
    def __init__(self):
        task_id = "daily_insights_generation"
        description = "Generate daily insights from analyzed data"
        super().__init__(task_id, description)
        
        self.repository = EventsRepository()
        self.insights_engine = InsightsEngine()
        self.insight_prioritizer = InsightPrioritizer()
        self.insight_enricher = InsightEnricher()
    
    def run(self,
            from_date: Optional[str] = None,
            to_date: Optional[str] = None,
            insight_types: Optional[List[str]] = None,
            max_insights: int = 10
           ) -> Dict[str, Any]:
        """
        Generate daily insights from analyzed data.
        
        Args:
            from_date: Start date in YYYY-MM-DD format
            to_date: End date in YYYY-MM-DD format
            insight_types: Types of insights to generate
            max_insights: Maximum number of insights to generate
            
        Returns:
            Dict[str, Any]: Generated insights
        """
        # Default date range: last 7 days
        if not from_date or not to_date:
            to_date_obj = datetime.now().date()
            from_date_obj = to_date_obj - timedelta(days=7)
            to_date = to_date or to_date_obj.isoformat()
            from_date = from_date or from_date_obj.isoformat()
        
        # Default insight types
        if not insight_types:
            insight_types = ["trend", "funnel", "cohort"]
        
        logger.info(f"Generating insights for period {from_date} to {to_date}")
        
        try:
            # Generate raw insights using the insights engine
            raw_insights = self.insights_engine.generate_insights(
                from_date=from_date,
                to_date=to_date,
                insight_types=insight_types,
                max_insights=max_insights * 2  # Generate more than needed for prioritization
            )
            
            if not raw_insights:
                logger.warning(f"No insights generated for period {from_date} to {to_date}")
                return {
                    "status": "no_insights",
                    "from_date": from_date,
                    "to_date": to_date,
                    "insight_types": insight_types
                }
            
            # Prioritize insights
            prioritized_insights = self.insight_prioritizer.prioritize_insights(
                insights=raw_insights,
                max_insights=max_insights
            )
            
            # Enrich insights with business context and recommendations
            enriched_insights = self.insight_enricher.enrich_insights(
                insights=prioritized_insights
            )
            
            # Store generated insights
            self.repository.store_insights(
                insights=enriched_insights,
                from_date=from_date,
                to_date=to_date,
                metadata={
                    "insight_types": insight_types,
                    "generation_date": datetime.now().isoformat()
                }
            )
            
            # Group insights by type for the result summary
            insights_by_type = {}
            for insight in enriched_insights:
                insight_type = insight.get("type", "unknown")
                if insight_type not in insights_by_type:
                    insights_by_type[insight_type] = 0
                insights_by_type[insight_type] += 1
            
            logger.info(f"Generated and stored {len(enriched_insights)} insights")
            
            return {
                "status": "completed",
                "insights_count": len(enriched_insights),
                "from_date": from_date,
                "to_date": to_date,
                "insights_by_type": insights_by_type
            }
            
        except Exception as e:
            logger.error(f"Error generating insights: {str(e)}", exc_info=True)
            raise


class WeeklyInsightsSummaryTask(InsightGenerationTask):
    """
    Task for generating a weekly summary of the most important insights.
    
    This task aggregates daily insights for the week and creates a
    comprehensive summary with the most significant findings.
    """
    
    def __init__(self):
        task_id = "weekly_insights_summary"
        description = "Generate weekly summary of key insights"
        super().__init__(task_id, description)
        
        self.repository = EventsRepository()
        self.insights_engine = InsightsEngine()
        self.insight_prioritizer = InsightPrioritizer()
        self.insight_enricher = InsightEnricher()
    
    def run(self, max_insights: int = 15) -> Dict[str, Any]:
        """
        Generate weekly insights summary.
        
        Args:
            max_insights: Maximum number of insights to include in summary
            
        Returns:
            Dict[str, Any]: Weekly insights summary
        """
        # Calculate date range for the past week
        to_date = datetime.now().date()
        from_date = to_date - timedelta(days=7)
        
        logger.info(f"Generating weekly insights summary for {from_date.isoformat()} to {to_date.isoformat()}")
        
        try:
            # Retrieve all insights generated in the past week
            weekly_insights = self.repository.get_insights(
                from_date=from_date.isoformat(),
                to_date=to_date.isoformat()
            )
            
            if not weekly_insights:
                logger.warning(f"No insights found for the past week")
                return {
                    "status": "no_insights",
                    "from_date": from_date.isoformat(),
                    "to_date": to_date.isoformat()
                }
            
            # Identify the top insights for the week using the prioritizer
            prioritized_weekly_insights = self.insight_prioritizer.prioritize_insights(
                insights=weekly_insights,
                max_insights=max_insights,
                prioritization_criteria={
                    "importance": 0.5,  # Weight for importance score
                    "recency": 0.3,     # Weight for recency
                    "impact": 0.2       # Weight for business impact
                }
            )
            
            # Group insights by category
            categorized_insights = {}
            for insight in prioritized_weekly_insights:
                category = insight.get("category", "Other")
                if category not in categorized_insights:
                    categorized_insights[category] = []
                categorized_insights[category].append(insight)
            
            # Create the weekly summary
            summary = {
                "period": {
                    "from_date": from_date.isoformat(),
                    "to_date": to_date.isoformat()
                },
                "total_insights": len(weekly_insights),
                "top_insights_count": len(prioritized_weekly_insights),
                "insights_by_category": {
                    category: len(insights) for category, insights in categorized_insights.items()
                },
                "categorized_insights": categorized_insights,
                "generation_date": datetime.now().isoformat()
            }
            
            # Store the weekly summary
            self.repository.store_insight_summary(
                summary_type="weekly",
                summary=summary,
                from_date=from_date.isoformat(),
                to_date=to_date.isoformat()
            )
            
            logger.info(f"Generated weekly insights summary with {len(prioritized_weekly_insights)} top insights")
            
            return {
                "status": "completed",
                "from_date": from_date.isoformat(),
                "to_date": to_date.isoformat(),
                "total_insights": len(weekly_insights),
                "top_insights_count": len(prioritized_weekly_insights),
                "categories": list(categorized_insights.keys())
            }
            
        except Exception as e:
            logger.error(f"Error generating weekly insights summary: {str(e)}", exc_info=True)
            raise


class MonthlyTrendsAnalysisTask(InsightGenerationTask):
    """
    Task for performing a comprehensive monthly trends analysis.
    
    This task analyzes long-term trends across multiple metrics and
    generates strategic insights for business planning.
    """
    
    def __init__(self):
        task_id = "monthly_trends_analysis"
        description = "Generate monthly comprehensive trends analysis"
        super().__init__(task_id, description)
        
        self.repository = EventsRepository()
        self.insights_engine = InsightsEngine()
    
    def run(self) -> Dict[str, Any]:
        """
        Generate monthly trends analysis.
        
        Returns:
            Dict[str, Any]: Monthly trends analysis
        """
        # Calculate date range for the past month
        to_date = datetime.now().date()
        from_date = to_date.replace(day=1) - timedelta(days=1)  # Last day of previous month
        from_date = from_date.replace(day=1)  # First day of previous month
        
        # For comparison, include the previous month as well
        comparison_from_date = from_date.replace(day=1) - timedelta(days=1)  # Last day of month before last
        comparison_from_date = comparison_from_date.replace(day=1)  # First day of month before last
        
        logger.info(f"Generating monthly trends analysis for {from_date.isoformat()} to {to_date.isoformat()}")
        
        try:
            # Generate monthly trend insights with comparison to previous month
            monthly_insights = self.insights_engine.generate_trend_insights(
                from_date=from_date.isoformat(),
                to_date=to_date.isoformat(),
                comparison_period={
                    "from_date": comparison_from_date.isoformat(),
                    "to_date": from_date.isoformat()
                },
                metrics=["dau", "mau", "retention", "conversion_rate", "revenue"],
                timeframe="month"
            )
            
            if not monthly_insights:
                logger.warning(f"No trend insights generated for the month")
                return {
                    "status": "no_insights",
                    "from_date": from_date.isoformat(),
                    "to_date": to_date.isoformat()
                }
            
            # Create monthly summary with key metrics
            monthly_summary = {
                "period": {
                    "month": from_date.strftime("%B %Y"),
                    "from_date": from_date.isoformat(),
                    "to_date": to_date.isoformat()
                },
                "insights_count": len(monthly_insights),
                "insights": monthly_insights,
                "key_metrics": self._extract_key_metrics(monthly_insights),
                "generation_date": datetime.now().isoformat()
            }
            
            # Store monthly summary
            self.repository.store_insight_summary(
                summary_type="monthly",
                summary=monthly_summary,
                from_date=from_date.isoformat(),
                to_date=to_date.isoformat()
            )
            
            logger.info(f"Generated monthly trends analysis with {len(monthly_insights)} insights")
            
            return {
                "status": "completed",
                "month": from_date.strftime("%B %Y"),
                "from_date": from_date.isoformat(),
                "to_date": to_date.isoformat(),
                "insights_count": len(monthly_insights),
                "metrics_analyzed": list(monthly_summary.get("key_metrics", {}).keys())
            }
            
        except Exception as e:
            logger.error(f"Error generating monthly trends analysis: {str(e)}", exc_info=True)
            raise
    
    def _extract_key_metrics(self, insights: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Extract key metrics from insights for the monthly summary.
        
        Args:
            insights: List of insight dictionaries
            
        Returns:
            Dict[str, Any]: Key metrics with their values and trends
        """
        key_metrics = {}
        
        for insight in insights:
            metric_name = insight.get("metric")
            if not metric_name:
                continue
                
            # Extract metric values and changes
            current_value = insight.get("current_value")
            previous_value = insight.get("previous_value")
            percent_change = insight.get("percent_change")
            trend_direction = insight.get("trend_direction")
            
            if current_value is not None:
                key_metrics[metric_name] = {
                    "value": current_value,
                    "previous_value": previous_value,
                    "percent_change": percent_change,
                    "trend": trend_direction,
                    "significant": insight.get("is_significant", False)
                }
        
        return key_metrics
