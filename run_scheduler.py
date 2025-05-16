"""
HitCraft Analytics Engine - Task Scheduler Runner

This script sets up and runs the automated task scheduler for the HitCraft Analytics Engine.
It configures all tasks with appropriate schedules and dependencies.

Usage:
    python run_scheduler.py

The scheduler will run continuously, executing tasks according to their defined schedules.
"""

import os
import sys
import logging
import time
from datetime import datetime, timedelta, time as dt_time

# Add the project path to system path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from hitcraft_analytics.workers.scheduler.scheduler import TaskScheduler
from hitcraft_analytics.workers.scheduler.config import (
    DAILY_DATA_PULL_TIME,
    DAILY_ANALYSIS_TIME,
    INSIGHTS_GENERATION_TIME,
    DAILY_REPORT_TIME,
    WEEKLY_REPORT_TIME
)
from hitcraft_analytics.workers.tasks.data_tasks import (
    MixpanelDataPullTask,
    UserProfileDataPullTask,
    FullDataBackfillTask
)
from hitcraft_analytics.workers.tasks.analysis_tasks import (
    TrendAnalysisTask,
    FunnelAnalysisTask,
    CohortAnalysisTask
)
from hitcraft_analytics.workers.tasks.insight_tasks import (
    DailyInsightsGenerationTask,
    WeeklyInsightsSummaryTask,
    MonthlyTrendsAnalysisTask
)
from hitcraft_analytics.utils.logging_config import setup_logger

# Set up logger
logger = setup_logger("scheduler.runner")

def next_execution_time(hour: int, minute: int, day_of_week: int = None) -> datetime:
    """
    Calculate the next execution time for a scheduled task.
    
    Args:
        hour: Hour of day (0-23)
        minute: Minute (0-59)
        day_of_week: Day of week (0=Monday, 6=Sunday) or None for daily
        
    Returns:
        datetime: Next execution time
    """
    now = datetime.now()
    target_time = now.replace(hour=hour, minute=minute, second=0, microsecond=0)
    
    # If target time is in the past, move to next day
    if target_time <= now:
        target_time += timedelta(days=1)
    
    # If day of week is specified, adjust to next occurrence of that day
    if day_of_week is not None:
        days_ahead = day_of_week - target_time.weekday()
        if days_ahead <= 0:  # Target day already happened this week
            days_ahead += 7
        target_time += timedelta(days=days_ahead)
    
    return target_time

def setup_scheduler() -> TaskScheduler:
    """
    Set up the task scheduler with all required tasks.
    
    Returns:
        TaskScheduler: Configured task scheduler
    """
    scheduler = TaskScheduler()
    
    # Register data collection tasks
    mixpanel_data_pull = MixpanelDataPullTask()
    user_profile_pull = UserProfileDataPullTask()
    full_backfill = FullDataBackfillTask()
    
    scheduler.register_task(mixpanel_data_pull)
    scheduler.register_task(user_profile_pull)
    scheduler.register_task(full_backfill)
    
    # Register analysis tasks with dependencies on data collection
    trend_analysis = TrendAnalysisTask()
    funnel_analysis = FunnelAnalysisTask()
    cohort_analysis = CohortAnalysisTask()
    
    scheduler.register_task(trend_analysis, dependencies=["mixpanel_data_pull"])
    scheduler.register_task(funnel_analysis, dependencies=["mixpanel_data_pull"])
    scheduler.register_task(cohort_analysis, dependencies=["mixpanel_data_pull", "mixpanel_user_profiles_pull"])
    
    # Register insight generation tasks with dependencies on analysis
    daily_insights = DailyInsightsGenerationTask()
    weekly_insights = WeeklyInsightsSummaryTask()
    monthly_trends = MonthlyTrendsAnalysisTask()
    
    scheduler.register_task(daily_insights, dependencies=["trend_analysis", "funnel_analysis", "cohort_analysis"])
    scheduler.register_task(weekly_insights, dependencies=["daily_insights_generation"])
    scheduler.register_task(monthly_trends, dependencies=["trend_analysis"])
    
    # Schedule initial execution times
    now = datetime.now()
    
    # Schedule data collection tasks
    data_pull_time = DAILY_DATA_PULL_TIME
    scheduler.schedule_task(
        "mixpanel_data_pull", 
        next_execution_time(data_pull_time.hour, data_pull_time.minute)
    )
    
    # Schedule user profile pull weekly
    scheduler.schedule_task(
        "mixpanel_user_profiles_pull", 
        next_execution_time(data_pull_time.hour, data_pull_time.minute + 15)
    )
    
    # Schedule analysis tasks
    analysis_time = DAILY_ANALYSIS_TIME
    scheduler.schedule_task(
        "trend_analysis", 
        next_execution_time(analysis_time.hour, analysis_time.minute)
    )
    scheduler.schedule_task(
        "funnel_analysis", 
        next_execution_time(analysis_time.hour, analysis_time.minute + 15)
    )
    scheduler.schedule_task(
        "cohort_analysis", 
        next_execution_time(analysis_time.hour, analysis_time.minute + 30)
    )
    
    # Schedule insight generation tasks
    insights_time = INSIGHTS_GENERATION_TIME
    scheduler.schedule_task(
        "daily_insights_generation", 
        next_execution_time(insights_time.hour, insights_time.minute)
    )
    
    # Schedule weekly insights for Monday mornings
    weekly_time = WEEKLY_REPORT_TIME
    scheduler.schedule_task(
        "weekly_insights_summary", 
        next_execution_time(weekly_time.hour, weekly_time.minute, day_of_week=0)  # Monday
    )
    
    # Schedule monthly trends for the 1st of each month
    first_of_next_month = now.replace(day=28) + timedelta(days=4)  # Move to next month
    first_of_next_month = first_of_next_month.replace(day=1, hour=9, minute=0, second=0, microsecond=0)  # 1st day at 9 AM
    scheduler.schedule_task("monthly_trends_analysis", first_of_next_month)
    
    logger.info("Scheduler configured with all tasks")
    return scheduler

def main():
    """Run the scheduler."""
    print(f"Starting HitCraft Analytics Engine Scheduler at {datetime.now().isoformat()}")
    print("Press Ctrl+C to stop")
    
    # Set up and start the scheduler
    scheduler = setup_scheduler()
    
    try:
        # Run the scheduler
        scheduler.start()
    except KeyboardInterrupt:
        print("\nStopping scheduler...")
        scheduler.stop()
        print("Scheduler stopped")
    except Exception as e:
        logger.error(f"Error running scheduler: {str(e)}", exc_info=True)
        scheduler.stop()
        print(f"Scheduler stopped due to error: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main()
