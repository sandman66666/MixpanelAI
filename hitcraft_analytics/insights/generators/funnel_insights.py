"""
HitCraft AI Analytics Engine - Funnel Insight Generator

This module generates insights from funnel analysis results, including:
- Drop-off points that need attention
- Segment comparison insights
- Period-over-period changes
- Optimization opportunities
"""

import logging
from typing import Dict, List, Optional, Any, Union
from datetime import datetime

from hitcraft_analytics.utils.logging.logger import get_logger

logger = get_logger(__name__)

class FunnelInsightGenerator:
    """
    Generates actionable insights from funnel analysis results.
    """
    
    def __init__(self, 
                significance_threshold: float = 0.1,  # 10% change is significant
                drop_off_threshold: float = 0.2):     # 20% drop-off rate is significant
        """
        Initialize the funnel insight generator.
        
        Args:
            significance_threshold (float): Threshold for considering a change significant.
            drop_off_threshold (float): Threshold for considering a drop-off significant.
        """
        self.significance_threshold = significance_threshold
        self.drop_off_threshold = drop_off_threshold
        
        logger.info("Funnel Insight Generator initialized")
    
    def generate_insights(self, 
                        funnel_analysis: Dict[str, Any],
                        funnel_name: str,
                        from_date: str,
                        to_date: str) -> List[Dict[str, Any]]:
        """
        Generate insights from funnel analysis results.
        
        Args:
            funnel_analysis (Dict[str, Any]): Results from funnel analysis.
            funnel_name (str): Name of the analyzed funnel.
            from_date (str): Start date of the analysis period.
            to_date (str): End date of the analysis period.
            
        Returns:
            List[Dict[str, Any]]: List of generated insights.
        """
        insights = []
        
        # Process basic funnel analysis
        if "funnel_analysis" in funnel_analysis:
            basic_insights = self._generate_basic_funnel_insights(
                funnel_analysis["funnel_analysis"], 
                funnel_name,
                from_date,
                to_date
            )
            insights.extend(basic_insights)
        
        # Process segment analysis if available
        if "segment_analysis" in funnel_analysis:
            segment_insights = self._generate_segment_insights(
                funnel_analysis["segment_analysis"],
                funnel_name,
                from_date,
                to_date
            )
            insights.extend(segment_insights)
        
        # Process time analysis if available
        if "time_analysis" in funnel_analysis:
            time_insights = self._generate_time_insights(
                funnel_analysis["time_analysis"],
                funnel_name,
                from_date,
                to_date
            )
            insights.extend(time_insights)
        
        # Process period comparison if available
        if "period_comparison" in funnel_analysis:
            comparison_insights = self._generate_comparison_insights(
                funnel_analysis["period_comparison"],
                funnel_name,
                from_date,
                to_date
            )
            insights.extend(comparison_insights)
        
        return insights
    
    def _generate_basic_funnel_insights(self,
                                      funnel_results: Dict[str, Any],
                                      funnel_name: str,
                                      from_date: str,
                                      to_date: str) -> List[Dict[str, Any]]:
        """
        Generate insights from basic funnel analysis.
        
        Args:
            funnel_results (Dict[str, Any]): Basic funnel analysis results.
            funnel_name (str): Name of the analyzed funnel.
            from_date (str): Start date of the analysis period.
            to_date (str): End date of the analysis period.
            
        Returns:
            List[Dict[str, Any]]: List of basic funnel insights.
        """
        insights = []
        
        # Overall conversion rate insight
        overall_rate = funnel_results.get("overall_conversion_rate", 0)
        total_users = funnel_results.get("total_users_entered", 0)
        
        if total_users > 0:
            # Create insight about overall funnel performance
            insights.append({
                "type": "funnel_overview",
                "subtype": "overall_conversion",
                "title": f"{funnel_name} Funnel Performance",
                "description": f"The overall conversion rate for {funnel_name} is {overall_rate:.1%} with {total_users} users entering the funnel.",
                "impact_score": self._calculate_impact_score(overall_rate, total_users),
                "metric_value": overall_rate,
                "metric_name": "Conversion Rate",
                "funnel_name": funnel_name,
                "date_range": f"{from_date} to {to_date}",
                "data": {
                    "funnel_name": funnel_name,
                    "overall_rate": overall_rate,
                    "total_users": total_users,
                    "converted_users": funnel_results.get("total_users_converted", 0)
                }
            })
        
        # Drop-off points insights
        if "drop_offs" in funnel_results and funnel_results["drop_offs"]:
            for drop_off in funnel_results["drop_offs"]:
                drop_rate = drop_off.get("drop_off_rate", 0)
                
                # Only create insight if drop-off is significant
                if drop_rate >= self.drop_off_threshold:
                    from_step = drop_off.get("step", "")
                    to_step = drop_off.get("next_step", "")
                    drop_count = drop_off.get("drop_off_count", 0)
                    
                    # Calculate impact score based on drop-off rate and number of users affected
                    impact_score = self._calculate_impact_score(drop_rate, drop_count)
                    
                    insights.append({
                        "type": "funnel_drop_off",
                        "subtype": "significant_drop_off",
                        "title": f"Significant Drop-off in {funnel_name}",
                        "description": f"{drop_rate:.1%} of users ({drop_count} total) drop off when going from '{from_step}' to '{to_step}'.",
                        "impact_score": impact_score,
                        "metric_value": drop_rate,
                        "metric_name": "Drop-off Rate",
                        "funnel_name": funnel_name,
                        "date_range": f"{from_date} to {to_date}",
                        "data": {
                            "funnel_name": funnel_name,
                            "from_step": from_step,
                            "to_step": to_step,
                            "drop_rate": drop_rate,
                            "drop_count": drop_count
                        },
                        "recommendations": [
                            f"Analyze user behavior during the '{from_step}' to '{to_step}' transition",
                            f"Review the UX flow between these two steps for potential barriers",
                            f"Consider implementing guidance or simplifying the transition"
                        ]
                    })
        
        # Time to convert insights
        if "avg_conversion_times" in funnel_results:
            # Find the longest step transition
            longest_transition = None
            longest_time = 0
            
            for transition, time in funnel_results["avg_conversion_times"].items():
                if time and time > longest_time:
                    longest_time = time
                    longest_transition = transition
            
            if longest_transition and longest_time > 1:  # More than 1 day is significant
                insights.append({
                    "type": "funnel_timing",
                    "subtype": "slow_transition",
                    "title": f"Slow Transition in {funnel_name}",
                    "description": f"Users take an average of {longest_time:.1f} days to move through '{longest_transition}'.",
                    "impact_score": min(0.8, longest_time / 10),  # Scale based on days, max at 0.8
                    "metric_value": longest_time,
                    "metric_name": "Average Days to Convert",
                    "funnel_name": funnel_name,
                    "date_range": f"{from_date} to {to_date}",
                    "data": {
                        "funnel_name": funnel_name,
                        "transition": longest_transition,
                        "days": longest_time
                    },
                    "recommendations": [
                        "Consider implementing reminders or re-engagement campaigns",
                        "Analyze what's causing users to delay at this step",
                        "Look for ways to streamline this particular transition"
                    ]
                })
        
        return insights
    
    def _generate_segment_insights(self,
                                 segment_analysis: Dict[str, Any],
                                 funnel_name: str,
                                 from_date: str,
                                 to_date: str) -> List[Dict[str, Any]]:
        """
        Generate insights from segment analysis.
        
        Args:
            segment_analysis (Dict[str, Any]): Segment analysis results.
            funnel_name (str): Name of the analyzed funnel.
            from_date (str): Start date of the analysis period.
            to_date (str): End date of the analysis period.
            
        Returns:
            List[Dict[str, Any]]: List of segment-related insights.
        """
        insights = []
        
        # Check if we have summary data
        if "_summary" in segment_analysis:
            summary = segment_analysis["_summary"]
            segment_column = summary.get("segment_column", "")
            
            # Check if we have best and worst segments
            if "best_segment" in summary and "worst_segment" in summary:
                best = summary["best_segment"]
                worst = summary["worst_segment"]
                
                conversion_range = summary.get("conversion_range", 0)
                
                # Only create insight if the difference is significant
                if conversion_range >= self.significance_threshold:
                    insights.append({
                        "type": "segment_comparison",
                        "subtype": "performance_gap",
                        "title": f"Segment Performance Gap in {funnel_name}",
                        "description": f"The '{best['name']}' segment ({best['conversion_rate']:.1%} conversion) outperforms the '{worst['name']}' segment ({worst['conversion_rate']:.1%} conversion) by {conversion_range:.1%}.",
                        "impact_score": min(0.9, conversion_range * 2),  # Scale based on range, max at 0.9
                        "metric_value": conversion_range,
                        "metric_name": "Conversion Gap",
                        "funnel_name": funnel_name,
                        "date_range": f"{from_date} to {to_date}",
                        "data": {
                            "funnel_name": funnel_name,
                            "segment_column": segment_column,
                            "best_segment": best["name"],
                            "best_rate": best["conversion_rate"],
                            "worst_segment": worst["name"],
                            "worst_rate": worst["conversion_rate"],
                            "gap": conversion_range
                        },
                        "recommendations": [
                            f"Analyze what makes the '{best['name']}' segment more successful",
                            f"Focus optimization efforts on improving conversion for the '{worst['name']}' segment",
                            "Consider tailoring the experience for different segments"
                        ]
                    })
            
            # Compare each segment to the average
            avg_conversion = 0
            segment_count = 0
            total_users = 0
            
            # Calculate average conversion rate across segments
            for segment_name, segment_data in segment_analysis.items():
                if segment_name != "_summary" and isinstance(segment_data, dict):
                    conversion_rate = segment_data.get("overall_conversion_rate", 0)
                    user_count = segment_data.get("total_users_entered", 0)
                    
                    if user_count > 0:
                        avg_conversion += conversion_rate * user_count
                        total_users += user_count
                        segment_count += 1
            
            if total_users > 0:
                avg_conversion = avg_conversion / total_users
                
                # Look for segments with significant deviation from average
                for segment_name, segment_data in segment_analysis.items():
                    if segment_name != "_summary" and isinstance(segment_data, dict):
                        conversion_rate = segment_data.get("overall_conversion_rate", 0)
                        user_count = segment_data.get("total_users_entered", 0)
                        
                        if user_count > 0:
                            deviation = conversion_rate - avg_conversion
                            abs_deviation = abs(deviation)
                            
                            # Only create insight if deviation is significant
                            if abs_deviation >= self.significance_threshold and user_count >= 10:
                                direction = "outperforms" if deviation > 0 else "underperforms"
                                
                                insights.append({
                                    "type": "segment_performance",
                                    "subtype": "deviation_from_average",
                                    "title": f"Segment {direction.capitalize()} in {funnel_name}",
                                    "description": f"The '{segment_name}' segment ({conversion_rate:.1%} conversion) {direction} the average ({avg_conversion:.1%}) by {abs_deviation:.1%}.",
                                    "impact_score": min(0.85, abs_deviation * 2 * (user_count / total_users)),
                                    "metric_value": deviation,
                                    "metric_name": "Conversion Deviation",
                                    "funnel_name": funnel_name,
                                    "date_range": f"{from_date} to {to_date}",
                                    "data": {
                                        "funnel_name": funnel_name,
                                        "segment_column": segment_column,
                                        "segment_name": segment_name,
                                        "segment_rate": conversion_rate,
                                        "average_rate": avg_conversion,
                                        "deviation": deviation,
                                        "segment_users": user_count,
                                        "total_users": total_users
                                    }
                                })
        
        return insights
    
    def _generate_time_insights(self,
                              time_analysis: Dict[str, Any],
                              funnel_name: str,
                              from_date: str,
                              to_date: str) -> List[Dict[str, Any]]:
        """
        Generate insights from time-based analysis.
        
        Args:
            time_analysis (Dict[str, Any]): Time analysis results.
            funnel_name (str): Name of the analyzed funnel.
            from_date (str): Start date of the analysis period.
            to_date (str): End date of the analysis period.
            
        Returns:
            List[Dict[str, Any]]: List of time-based insights.
        """
        insights = []
        
        # Check if we have trend analysis
        if "_trend_analysis" in time_analysis:
            trend = time_analysis["_trend_analysis"]
            
            # Check if we have sufficient data for trend analysis
            if "trend_direction" in trend and "relative_change" in trend:
                direction = trend["trend_direction"]
                rel_change = trend["relative_change"]
                abs_change = trend.get("absolute_change", 0)
                time_period = trend.get("time_period", "period")
                first_period = trend.get("first_period", "")
                last_period = trend.get("last_period", "")
                
                # Only create insight if change is significant
                if abs(rel_change) >= self.significance_threshold:
                    if direction == "improving":
                        title = f"Improving {funnel_name} Funnel Performance"
                        description = f"Conversion rate has consistently improved from {trend['first_period_rate']:.1%} to {trend['last_period_rate']:.1%} ({abs_change:.1%} increase) over the analyzed period."
                    elif direction == "declining":
                        title = f"Declining {funnel_name} Funnel Performance"
                        description = f"Conversion rate has consistently declined from {trend['first_period_rate']:.1%} to {trend['last_period_rate']:.1%} ({abs(abs_change):.1%} decrease) over the analyzed period."
                    else:  # mixed
                        title = f"Fluctuating {funnel_name} Funnel Performance"
                        direction_text = "increased" if abs_change > 0 else "decreased"
                        description = f"Conversion rate shows mixed trend, but has overall {direction_text} from {trend['first_period_rate']:.1%} to {trend['last_period_rate']:.1%} ({abs(abs_change):.1%} {direction_text}) over the analyzed period."
                    
                    impact_score = min(0.9, abs(rel_change) * 2)
                    
                    insights.append({
                        "type": "funnel_trend",
                        "subtype": f"{direction}_trend",
                        "title": title,
                        "description": description,
                        "impact_score": impact_score,
                        "metric_value": abs_change,
                        "metric_name": "Conversion Rate Change",
                        "funnel_name": funnel_name,
                        "date_range": f"{from_date} to {to_date}",
                        "data": {
                            "funnel_name": funnel_name,
                            "time_period": time_period,
                            "direction": direction,
                            "first_period": first_period,
                            "last_period": last_period,
                            "first_rate": trend["first_period_rate"],
                            "last_rate": trend["last_period_rate"],
                            "abs_change": abs_change,
                            "rel_change": rel_change
                        }
                    })
        
        return insights
    
    def _generate_comparison_insights(self,
                                    comparison: Dict[str, Any],
                                    funnel_name: str,
                                    from_date: str,
                                    to_date: str) -> List[Dict[str, Any]]:
        """
        Generate insights from period comparison.
        
        Args:
            comparison (Dict[str, Any]): Period comparison results.
            funnel_name (str): Name of the analyzed funnel.
            from_date (str): Start date of the analysis period.
            to_date (str): End date of the analysis period.
            
        Returns:
            List[Dict[str, Any]]: List of comparison insights.
        """
        insights = []
        
        # Check for overall conversion comparison
        if "overall_conversion_comparison" in comparison:
            overall = comparison["overall_conversion_comparison"]
            
            # Check if we have sufficient data
            if "absolute_diff" in overall:
                abs_diff = overall["absolute_diff"]
                rel_diff = overall.get("relative_diff", 0)
                improved = overall.get("improved", False)
                
                prev_name = next(k for k in overall.keys() if k.endswith("_rate") and not k.startswith("Current"))
                curr_name = next(k for k in overall.keys() if k.startswith("Current") and k.endswith("_rate"))
                
                prev_rate = overall[prev_name]
                curr_rate = overall[curr_name]
                
                # Only create insight if change is significant
                if abs(abs_diff) >= self.significance_threshold:
                    if improved:
                        title = f"Improved {funnel_name} Funnel Performance"
                        description = f"Conversion rate has improved from {prev_rate:.1%} to {curr_rate:.1%} ({abs_diff:.1%} increase) compared to the previous period."
                    else:
                        title = f"Declined {funnel_name} Funnel Performance"
                        description = f"Conversion rate has declined from {prev_rate:.1%} to {curr_rate:.1%} ({abs(abs_diff):.1%} decrease) compared to the previous period."
                    
                    impact_score = min(0.9, abs(rel_diff) * 2)
                    
                    insights.append({
                        "type": "funnel_period_comparison",
                        "subtype": "overall_change",
                        "title": title,
                        "description": description,
                        "impact_score": impact_score,
                        "metric_value": abs_diff,
                        "metric_name": "Period-over-Period Change",
                        "funnel_name": funnel_name,
                        "date_range": f"{from_date} to {to_date}",
                        "data": {
                            "funnel_name": funnel_name,
                            "previous_rate": prev_rate,
                            "current_rate": curr_rate,
                            "abs_diff": abs_diff,
                            "rel_diff": rel_diff,
                            "improved": improved
                        }
                    })
        
        # Check for drop-off comparison
        if "drop_off_comparison" in comparison:
            drop_offs = comparison["drop_off_comparison"]
            
            for drop_off in drop_offs:
                # Look for steps with significant improvement or decline
                if "rate_diff" in drop_off:
                    rate_diff = drop_off["rate_diff"]
                    improved = drop_off.get("improved", False)
                    
                    # Only create insight if change is significant
                    if abs(rate_diff) >= self.significance_threshold:
                        step = drop_off.get("step", "")
                        next_step = drop_off.get("next_step", "")
                        
                        # Get previous and current rates
                        prev_rate_key = next(k for k in drop_off.keys() if k.endswith("_drop_off_rate") and not k.startswith("Current"))
                        curr_rate_key = next(k for k in drop_off.keys() if k.startswith("Current") and k.endswith("_drop_off_rate"))
                        
                        prev_rate = drop_off[prev_rate_key]
                        curr_rate = drop_off[curr_rate_key]
                        
                        if improved:
                            title = f"Improved Step Transition in {funnel_name}"
                            description = f"Drop-off rate between '{step}' and '{next_step}' has decreased from {prev_rate:.1%} to {curr_rate:.1%} ({abs(rate_diff):.1%} improvement)."
                        else:
                            title = f"Worsened Step Transition in {funnel_name}"
                            description = f"Drop-off rate between '{step}' and '{next_step}' has increased from {prev_rate:.1%} to {curr_rate:.1%} ({abs(rate_diff):.1%} decline)."
                        
                        impact_score = min(0.85, abs(rate_diff) * 2)
                        
                        insights.append({
                            "type": "funnel_step_comparison",
                            "subtype": "step_change",
                            "title": title,
                            "description": description,
                            "impact_score": impact_score,
                            "metric_value": rate_diff,
                            "metric_name": "Drop-off Rate Change",
                            "funnel_name": funnel_name,
                            "date_range": f"{from_date} to {to_date}",
                            "data": {
                                "funnel_name": funnel_name,
                                "step": step,
                                "next_step": next_step,
                                "previous_rate": prev_rate,
                                "current_rate": curr_rate,
                                "rate_diff": rate_diff,
                                "improved": improved
                            }
                        })
        
        return insights
    
    def _calculate_impact_score(self, primary_metric: float, scale_factor: int) -> float:
        """
        Calculate impact score based on metrics and scale.
        
        Args:
            primary_metric (float): Primary metric value (conversion or drop-off rate).
            scale_factor (int): Scale factor (typically number of users affected).
            
        Returns:
            float: Impact score between 0 and 1.
        """
        # For conversion rates, higher is better; for drop-offs, lower is better
        # We use a logarithmic scale for user counts to ensure small segments can still get attention
        
        # Scale the user count logarithmically (1-10 users: small impact, 100+ users: high impact)
        scale_component = min(0.7, max(0.1, (1 if scale_factor <= 0 else min(1, 0.1 + (0.6 * (min(scale_factor, 1000) / 1000))))))
        
        # For conversion rates, higher is better (0.5 is neutral point)
        metric_component = min(0.9, max(0.1, primary_metric * 0.9))
        
        # Combine both components, favoring the metric component
        return (metric_component * 0.7) + (scale_component * 0.3)
