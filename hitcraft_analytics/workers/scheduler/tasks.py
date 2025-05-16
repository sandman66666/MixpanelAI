"""
Task Base Classes

This module defines the base classes for all scheduled tasks in the HitCraft Analytics Engine.
All tasks derive from the BaseTask class which provides common functionality.
"""

import logging
import time
from abc import ABC, abstractmethod
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple, Union

from hitcraft_analytics.utils.logging_config import setup_logger

# Set up task-specific logger
logger = setup_logger("scheduler.tasks")

class TaskResult:
    """Represents the result of a scheduled task execution."""
    
    def __init__(
        self, 
        success: bool, 
        task_id: str,
        start_time: datetime,
        end_time: datetime,
        message: str = "",
        data: Optional[Dict[str, Any]] = None,
        error: Optional[Exception] = None
    ):
        self.success = success
        self.task_id = task_id
        self.start_time = start_time
        self.end_time = end_time
        self.duration = (end_time - start_time).total_seconds()
        self.message = message
        self.data = data or {}
        self.error = error
    
    def __str__(self) -> str:
        status = "SUCCESS" if self.success else "FAILURE"
        return (f"Task {self.task_id} - {status}: {self.message} "
                f"(Duration: {self.duration:.2f}s)")


class BaseTask(ABC):
    """
    Base class for all scheduled tasks.
    
    Provides common functionality for task execution, logging, and error handling.
    """
    
    def __init__(self, task_id: str, description: str = ""):
        self.task_id = task_id
        self.description = description
        self.last_run: Optional[datetime] = None
        self.next_run: Optional[datetime] = None
        self.retries_attempted = 0
        self.max_retries = 3
        self.retry_delay = 300  # seconds
        self.logger = logging.getLogger(f"scheduler.tasks.{task_id}")
    
    @abstractmethod
    def run(self, *args, **kwargs) -> Dict[str, Any]:
        """
        Execute the task logic.
        
        This method must be implemented by all task subclasses.
        It should contain the core functionality of the task.
        
        Returns:
            Dict[str, Any]: Result data from the task execution
        """
        pass
    
    def execute(self, *args, **kwargs) -> TaskResult:
        """
        Execute the task with timing, logging, and error handling.
        
        This method wraps the task's run method with common functionality.
        
        Returns:
            TaskResult: Object containing the task execution results
        """
        start_time = datetime.now()
        self.last_run = start_time
        
        self.logger.info(f"Starting task {self.task_id}")
        
        try:
            # Execute the task's custom logic
            result = self.run(*args, **kwargs)
            
            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()
            
            # Reset retry counter on success
            self.retries_attempted = 0
            
            self.logger.info(f"Task {self.task_id} completed successfully in {duration:.2f}s")
            
            return TaskResult(
                success=True,
                task_id=self.task_id,
                start_time=start_time,
                end_time=end_time,
                message="Task completed successfully",
                data=result
            )
            
        except Exception as e:
            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()
            
            self.retries_attempted += 1
            self.logger.error(f"Task {self.task_id} failed after {duration:.2f}s: {str(e)}", exc_info=True)
            
            return TaskResult(
                success=False,
                task_id=self.task_id,
                start_time=start_time,
                end_time=end_time,
                message=f"Task failed: {str(e)}",
                error=e
            )
    
    def should_retry(self) -> bool:
        """
        Determine if the task should be retried after a failure.
        
        Returns:
            bool: True if the task should be retried, False otherwise
        """
        return self.retries_attempted < self.max_retries
    
    def schedule_next_run(self, next_run: datetime) -> None:
        """
        Schedule the next execution time for this task.
        
        Args:
            next_run (datetime): When the task should next run
        """
        self.next_run = next_run
        self.logger.info(f"Task {self.task_id} scheduled for {next_run.isoformat()}")


class DataPullTask(BaseTask):
    """Base class for tasks that pull data from external sources like Mixpanel."""
    
    def __init__(self, task_id: str, description: str = ""):
        super().__init__(task_id, description)
        # Set a longer retry delay for API tasks
        self.retry_delay = 600  # 10 minutes
    
    def extract_date_range(self, days_back: int = 30) -> Tuple[str, str]:
        """
        Helper to calculate a date range for data pulling.
        
        Args:
            days_back (int): Number of days to look back
            
        Returns:
            Tuple[str, str]: from_date and to_date formatted as YYYY-MM-DD
        """
        to_date = datetime.now().date()
        from_date = to_date - timedelta(days=days_back)
        
        return from_date.isoformat(), to_date.isoformat()


class AnalysisTask(BaseTask):
    """Base class for tasks that perform analysis on data."""
    
    def __init__(self, task_id: str, description: str = ""):
        super().__init__(task_id, description)
    
    def validate_input_data(self, data: Any) -> bool:
        """
        Validate that input data meets the minimum requirements for analysis.
        
        Args:
            data: The data to validate
            
        Returns:
            bool: True if the data is valid for analysis
        """
        # Base implementation just checks if data exists
        return data is not None and (
            isinstance(data, Dict) or 
            (hasattr(data, '__len__') and len(data) > 0)
        )


class InsightGenerationTask(BaseTask):
    """Base class for tasks that generate insights from analyzed data."""
    
    def __init__(self, task_id: str, description: str = ""):
        super().__init__(task_id, description)
    
    def filter_insights_by_importance(
        self, 
        insights: List[Dict[str, Any]], 
        min_importance: float = 0.5
    ) -> List[Dict[str, Any]]:
        """
        Filter insights based on importance score.
        
        Args:
            insights: List of insight dictionaries
            min_importance: Minimum importance threshold (0-1)
            
        Returns:
            List[Dict[str, Any]]: Filtered list of insights
        """
        return [i for i in insights if i.get('importance', 0) >= min_importance]
