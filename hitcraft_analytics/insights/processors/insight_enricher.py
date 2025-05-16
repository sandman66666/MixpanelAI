"""
HitCraft AI Analytics Engine - Insight Enricher

This module enriches insights with additional context, supporting data,
and actionable recommendations to make them more valuable to users.
"""

import logging
from typing import Dict, List, Optional, Any, Union
from datetime import datetime

from hitcraft_analytics.utils.logging.logger import get_logger

logger = get_logger(__name__)

class InsightEnricher:
    """
    Enriches insights with additional context and recommendations.
    """
    
    def __init__(self):
        """
        Initialize the insight enricher.
        """
        logger.info("Insight Enricher initialized")
    
    def enrich_insights(self, insights: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Enrich insights with additional context and recommendations.
        
        Args:
            insights (List[Dict[str, Any]]): List of insights to enrich.
            
        Returns:
            List[Dict[str, Any]]: Enriched insights.
        """
        if not insights:
            return []
        
        logger.info(f"Enriching {len(insights)} insights")
        
        enriched_insights = []
        
        for insight in insights:
            try:
                # Enrich insight based on its type
                enriched_insight = self._enrich_insight(insight)
                enriched_insights.append(enriched_insight)
            except Exception as e:
                logger.error(f"Error enriching insight: {str(e)}")
                # Include the original insight if enrichment fails
                enriched_insights.append(insight)
        
        return enriched_insights
    
    def _enrich_insight(self, insight: Dict[str, Any]) -> Dict[str, Any]:
        """
        Enrich a single insight based on its type.
        
        Args:
            insight (Dict[str, Any]): Insight to enrich.
            
        Returns:
            Dict[str, Any]: Enriched insight.
        """
        # Create a copy of the insight to avoid modifying the original
        enriched = insight.copy()
        
        # Add enrichment timestamp
        enriched["enriched_at"] = datetime.now().isoformat()
        
        # Ensure we have basic fields
        if "recommendations" not in enriched:
            enriched["recommendations"] = []
        
        if "supporting_data" not in enriched:
            enriched["supporting_data"] = {}
        
        # Enrich based on insight type
        insight_type = enriched.get("type", "")
        
        if insight_type.startswith("funnel_"):
            self._enrich_funnel_insight(enriched)
        elif insight_type.startswith("metric_"):
            self._enrich_metric_insight(enriched)
        elif insight_type.startswith("segment_"):
            self._enrich_segment_insight(enriched)
        else:
            # Generic enrichment for other types
            self._enrich_generic_insight(enriched)
        
        return enriched
    
    def _enrich_funnel_insight(self, insight: Dict[str, Any]) -> None:
        """
        Enrich a funnel-related insight.
        
        Args:
            insight (Dict[str, Any]): Funnel insight to enrich.
        """
        subtype = insight.get("subtype", "")
        funnel_name = insight.get("funnel_name", "")
        
        # Add context based on subtype
        if subtype == "significant_drop_off":
            # Add specific recommendation for drop-off
            from_step = insight.get("data", {}).get("from_step", "")
            to_step = insight.get("data", {}).get("to_step", "")
            
            if from_step and to_step and not insight.get("recommendations"):
                insight["recommendations"] = [
                    f"A/B test different user experiences for the transition from '{from_step}' to '{to_step}'",
                    f"Conduct user interviews to understand barriers in the '{from_step}' step",
                    f"Consider adding guidance or reducing friction between these steps"
                ]
            
            # Add supporting data about common user paths
            insight["supporting_data"]["user_paths"] = {
                "description": "Common paths users take after completing the first step",
                "data_available": True,  # Indicates that this data could be fetched
                "query_params": {
                    "funnel_name": funnel_name,
                    "step": from_step
                }
            }
        
        elif subtype == "overall_conversion":
            # Add conversion benchmarks if not present
            if "benchmarks" not in insight["supporting_data"]:
                insight["supporting_data"]["benchmarks"] = {
                    "description": "Industry benchmarks for similar funnels",
                    "data_available": True,
                    "query_params": {
                        "funnel_type": funnel_name.lower().replace(" ", "_")
                    }
                }
            
            # Add general recommendations for improving funnel conversion
            if not insight.get("recommendations"):
                insight["recommendations"] = [
                    f"Review each step in the {funnel_name} funnel for opportunities to simplify",
                    "Consider implementing progress indicators to give users a sense of advancement",
                    "Test incentives at critical decision points to boost conversion"
                ]
        
        elif subtype == "step_change":
            # Add recommendations for addressing step performance changes
            improved = insight.get("data", {}).get("improved", False)
            
            if improved and not insight.get("recommendations"):
                insight["recommendations"] = [
                    "Analyze what changes contributed to this improvement",
                    "Consider applying similar improvements to other funnels",
                    "Document and share the successful approach with the team"
                ]
            elif not improved and not insight.get("recommendations"):
                insight["recommendations"] = [
                    "Investigate recent changes that might have affected this step",
                    "Consider rolling back changes if appropriate",
                    "A/B test alternative approaches to improve this transition"
                ]
    
    def _enrich_metric_insight(self, insight: Dict[str, Any]) -> None:
        """
        Enrich a metric-related insight.
        
        Args:
            insight (Dict[str, Any]): Metric insight to enrich.
        """
        subtype = insight.get("subtype", "")
        metric_name = insight.get("metric_name", "")
        
        # Add context based on subtype
        if subtype in ("increasing_trend", "decreasing_trend"):
            # Add related metrics that might explain the trend
            insight["supporting_data"]["related_metrics"] = {
                "description": "Related metrics that might explain this trend",
                "data_available": True,
                "query_params": {
                    "primary_metric": metric_name,
                    "correlation_threshold": 0.6
                }
            }
            
            # Add recommendations based on trend direction
            if subtype == "increasing_trend" and not insight.get("recommendations"):
                insight["recommendations"] = [
                    f"Analyze which user segments are driving the increase in {metric_name}",
                    "Identify successful strategies that might be contributing to growth",
                    "Allocate resources to support and amplify this positive trend"
                ]
            elif subtype == "decreasing_trend" and not insight.get("recommendations"):
                insight["recommendations"] = [
                    f"Investigate root causes for the decline in {metric_name}",
                    "Compare with industry trends to determine if this is market-wide",
                    "Develop an action plan to address the decline"
                ]
        
        elif subtype in ("positive_spike", "negative_drop"):
            # Add historical context for anomalies
            anomaly_date = insight.get("data", {}).get("anomaly_date", "")
            
            insight["supporting_data"]["historical_context"] = {
                "description": "Historical performance around this date",
                "data_available": True,
                "query_params": {
                    "metric": metric_name,
                    "date": anomaly_date,
                    "window_days": 30
                }
            }
            
            # Add specific recommendations for anomalies
            if subtype == "positive_spike" and not insight.get("recommendations"):
                insight["recommendations"] = [
                    f"Identify what caused the spike in {metric_name} on {anomaly_date}",
                    "Determine if this represents an opportunity that can be replicated",
                    "Check for correlations with marketing campaigns or product changes"
                ]
            elif subtype == "negative_drop" and not insight.get("recommendations"):
                insight["recommendations"] = [
                    f"Investigate what caused the drop in {metric_name} on {anomaly_date}",
                    "Assess whether this is a one-time event or the beginning of a trend",
                    "Check for technical issues or external factors that might explain the drop"
                ]
    
    def _enrich_segment_insight(self, insight: Dict[str, Any]) -> None:
        """
        Enrich a segment-related insight.
        
        Args:
            insight (Dict[str, Any]): Segment insight to enrich.
        """
        subtype = insight.get("subtype", "")
        segment_name = insight.get("data", {}).get("segment_name", "")
        
        # Add context based on subtype
        if subtype == "performance_gap":
            # Add segment characteristics comparison
            best_segment = insight.get("data", {}).get("best_segment", "")
            worst_segment = insight.get("data", {}).get("worst_segment", "")
            
            if best_segment and worst_segment:
                insight["supporting_data"]["segment_comparison"] = {
                    "description": "Detailed comparison between segments",
                    "data_available": True,
                    "query_params": {
                        "segment_1": best_segment,
                        "segment_2": worst_segment
                    }
                }
                
                # Add recommendations for addressing segment performance gaps
                if not insight.get("recommendations"):
                    insight["recommendations"] = [
                        f"Analyze the user experience differences between '{best_segment}' and '{worst_segment}' segments",
                        f"Identify what makes the '{best_segment}' segment more successful",
                        f"Develop targeted improvements for the '{worst_segment}' segment"
                    ]
        
        elif subtype == "deviation_from_average":
            # Add segment behavior analysis
            if segment_name:
                insight["supporting_data"]["segment_behavior"] = {
                    "description": "Detailed behavior analysis for this segment",
                    "data_available": True,
                    "query_params": {
                        "segment": segment_name
                    }
                }
                
                # Check if this is a positive or negative deviation
                outperforms = insight.get("data", {}).get("deviation", 0) > 0
                
                # Add recommendations based on deviation direction
                if outperforms and not insight.get("recommendations"):
                    insight["recommendations"] = [
                        f"Study what makes the '{segment_name}' segment particularly successful",
                        "Apply learnings from this segment to other underperforming segments",
                        f"Consider allocating more resources to support the '{segment_name}' segment"
                    ]
                elif not outperforms and not insight.get("recommendations"):
                    insight["recommendations"] = [
                        f"Investigate why the '{segment_name}' segment is underperforming",
                        "Conduct user research to identify pain points specific to this segment",
                        "Develop a targeted action plan to improve performance for this segment"
                    ]
    
    def _enrich_generic_insight(self, insight: Dict[str, Any]) -> None:
        """
        Enrich a generic insight with general recommendations.
        
        Args:
            insight (Dict[str, Any]): Generic insight to enrich.
        """
        # Add basic recommendations if none exist
        if not insight.get("recommendations"):
            metric_name = insight.get("metric_name", "this metric")
            
            insight["recommendations"] = [
                f"Further analyze the factors influencing {metric_name}",
                "Consider conducting user research to better understand this finding",
                "Develop a hypothesis and test it with controlled experiments"
            ]
        
        # Add historical trends as supporting data
        insight["supporting_data"]["historical_trends"] = {
            "description": "Historical trends for related metrics",
            "data_available": True,
            "query_params": {
                "related_to": insight.get("metric_name", ""),
                "period": "6_months"
            }
        }
