"""
HitCraft AI Analytics Engine - Insight Prioritizer

This module prioritizes insights based on their potential business impact,
relevance, actionability, and other factors to ensure most important
insights are highlighted first.
"""

import logging
from typing import Dict, List, Optional, Any, Union
from datetime import datetime

from hitcraft_analytics.utils.logging.logger import get_logger

logger = get_logger(__name__)

class InsightPrioritizer:
    """
    Prioritizes insights based on business impact and relevance.
    """
    
    def __init__(self,
                impact_weight: float = 0.7,
                recency_weight: float = 0.2,
                trend_weight: float = 0.1):
        """
        Initialize the insight prioritizer.
        
        Args:
            impact_weight (float): Weight given to impact score in prioritization.
            recency_weight (float): Weight given to insight recency in prioritization.
            trend_weight (float): Weight given to trend direction and strength.
        """
        self.impact_weight = impact_weight
        self.recency_weight = recency_weight
        self.trend_weight = trend_weight
        
        # Ensure weights sum to 1
        total_weight = impact_weight + recency_weight + trend_weight
        if total_weight != 1.0:
            self.impact_weight /= total_weight
            self.recency_weight /= total_weight
            self.trend_weight /= total_weight
        
        logger.info("Insight Prioritizer initialized")
    
    def prioritize_insights(self, insights: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Prioritize insights based on multiple factors.
        
        Args:
            insights (List[Dict[str, Any]]): List of insights to prioritize.
            
        Returns:
            List[Dict[str, Any]]: Prioritized list of insights.
        """
        if not insights:
            return []
        
        logger.info(f"Prioritizing {len(insights)} insights")
        
        # Calculate priority score for each insight
        for insight in insights:
            priority_score = self._calculate_priority_score(insight)
            insight["priority_score"] = priority_score
        
        # Sort insights by priority score (descending)
        prioritized_insights = sorted(insights, key=lambda x: x.get("priority_score", 0), reverse=True)
        
        # Log top insights
        if prioritized_insights:
            top_insight = prioritized_insights[0]
            logger.info(f"Top insight: {top_insight.get('title', 'Untitled')} with score {top_insight.get('priority_score', 0):.2f}")
        
        return prioritized_insights
    
    def _calculate_priority_score(self, insight: Dict[str, Any]) -> float:
        """
        Calculate a priority score for an insight.
        
        Args:
            insight (Dict[str, Any]): Insight to calculate priority score for.
            
        Returns:
            float: Priority score between 0 and 1.
        """
        # Impact component (from insight's impact score)
        impact_score = insight.get("impact_score", 0.5)
        
        # Recency component
        recency_score = self._calculate_recency_score(insight)
        
        # Trend component
        trend_score = self._calculate_trend_score(insight)
        
        # Calculate weighted score
        priority_score = (
            (impact_score * self.impact_weight) +
            (recency_score * self.recency_weight) +
            (trend_score * self.trend_weight)
        )
        
        return priority_score
    
    def _calculate_recency_score(self, insight: Dict[str, Any]) -> float:
        """
        Calculate a recency score for an insight.
        
        Args:
            insight (Dict[str, Any]): Insight to calculate recency score for.
            
        Returns:
            float: Recency score between 0 and 1.
        """
        # Default to current time if generated_at is not present
        generated_at = insight.get("generated_at", datetime.now().isoformat())
        
        try:
            # Parse the generated_at timestamp
            if isinstance(generated_at, str):
                generated_time = datetime.fromisoformat(generated_at)
            else:
                generated_time = generated_at
            
            # Calculate time since generation
            time_diff = (datetime.now() - generated_time).total_seconds()
            
            # Convert to hours
            hours_diff = time_diff / 3600
            
            # Score decreases with age (1.0 for new, 0.5 for 24 hours old, asymptotic to 0)
            recency_score = 1.0 / (1.0 + (hours_diff / 24))
            
            return recency_score
            
        except Exception as e:
            logger.error(f"Error calculating recency score: {str(e)}")
            return 0.5  # Default to middle value on error
    
    def _calculate_trend_score(self, insight: Dict[str, Any]) -> float:
        """
        Calculate a trend score for an insight.
        
        Args:
            insight (Dict[str, Any]): Insight to calculate trend score for.
            
        Returns:
            float: Trend score between 0 and 1.
        """
        # Default to 0.5 (neutral)
        trend_score = 0.5
        
        # Check insight type
        insight_type = insight.get("type", "")
        insight_subtype = insight.get("subtype", "")
        
        # Positive trend types get higher scores
        positive_types = [
            "improving_trend", "positive_spike", "segment_outperforms",
            "overall_improvement", "step_improvement"
        ]
        
        # Negative trend types get higher scores too (they need attention)
        negative_types = [
            "declining_trend", "negative_drop", "segment_underperforms",
            "overall_decline", "step_decline", "significant_drop_off"
        ]
        
        # Check for trend-related subtypes
        if any(pt in insight_subtype for pt in positive_types):
            trend_score = 0.8
        elif any(nt in insight_subtype for nt in negative_types):
            trend_score = 0.9  # Negative trends get slightly higher priority
        
        # For funnel insights specifically
        if insight_type == "funnel_drop_off":
            # Higher drop-off rates get higher priority
            metric_value = insight.get("metric_value", 0)
            if metric_value > 0.5:  # More than 50% drop-off
                trend_score = 0.95
            elif metric_value > 0.3:  # More than 30% drop-off
                trend_score = 0.85
            elif metric_value > 0.1:  # More than 10% drop-off
                trend_score = 0.75
        
        return trend_score
