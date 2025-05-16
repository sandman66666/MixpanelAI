"""
Task Scheduler

This module implements the task scheduler for the HitCraft Analytics Engine.
The scheduler is responsible for running tasks at specified times and managing
task dependencies, retries, and execution.
"""

import logging
import signal
import threading
import time
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Set

from hitcraft_analytics.utils.logging_config import setup_logger
from hitcraft_analytics.workers.scheduler.config import (
    SCHEDULER_CHECK_INTERVAL,
    TASK_TIMEOUT_SECONDS,
    MAX_RETRIES,
    RETRY_DELAY_SECONDS
)
from hitcraft_analytics.workers.scheduler.tasks import BaseTask, TaskResult

# Set up scheduler-specific logger
logger = setup_logger("scheduler.manager")

class TaskScheduler:
    """
    Task scheduler for managing and executing recurring tasks.
    
    The scheduler maintains a registry of tasks and executes them according 
    to their schedules. It handles dependencies between tasks and manages
    retries for failed tasks.
    """
    
    def __init__(self):
        self.tasks: Dict[str, BaseTask] = {}
        self.task_dependencies: Dict[str, Set[str]] = {}
        self.task_results: Dict[str, TaskResult] = {}
        self.running_tasks: Set[str] = set()
        self.stop_event = threading.Event()
        self.lock = threading.RLock()
        self.task_threads: Dict[str, threading.Thread] = {}
        
        # Configure task execution settings
        self.check_interval = SCHEDULER_CHECK_INTERVAL
        self.task_timeout = TASK_TIMEOUT_SECONDS
        
        logger.info("TaskScheduler initialized")
    
    def register_task(self, task: BaseTask, dependencies: List[str] = None) -> None:
        """
        Register a task with the scheduler.
        
        Args:
            task: The task to register
            dependencies: List of task IDs that must complete before this task
        """
        with self.lock:
            task_id = task.task_id
            
            if task_id in self.tasks:
                logger.warning(f"Task {task_id} already registered, replacing")
            
            self.tasks[task_id] = task
            
            # Set default values for task retry settings
            task.max_retries = task.max_retries or MAX_RETRIES
            task.retry_delay = task.retry_delay or RETRY_DELAY_SECONDS
            
            # Register dependencies
            if dependencies:
                self.task_dependencies[task_id] = set(dependencies)
                
                # Verify all dependencies exist
                for dep_id in dependencies:
                    if dep_id not in self.tasks:
                        logger.warning(f"Dependency {dep_id} for task {task_id} is not registered")
            else:
                self.task_dependencies[task_id] = set()
            
            logger.info(f"Task {task_id} registered with dependencies: {dependencies or []}")
    
    def unregister_task(self, task_id: str) -> None:
        """
        Remove a task from the scheduler.
        
        Args:
            task_id: ID of the task to remove
        """
        with self.lock:
            if task_id in self.running_tasks:
                logger.warning(f"Cannot unregister task {task_id} while it is running")
                return
            
            if task_id in self.tasks:
                self.tasks.pop(task_id)
                self.task_dependencies.pop(task_id, None)
                
                # Remove this task from other tasks' dependencies
                for deps in self.task_dependencies.values():
                    deps.discard(task_id)
                
                logger.info(f"Task {task_id} unregistered")
            else:
                logger.warning(f"Task {task_id} not found in registry")
    
    def schedule_task(self, task_id: str, next_run: datetime) -> bool:
        """
        Schedule a task to run at a specific time.
        
        Args:
            task_id: ID of the task to schedule
            next_run: When the task should run
            
        Returns:
            bool: True if task was scheduled, False otherwise
        """
        with self.lock:
            if task_id not in self.tasks:
                logger.warning(f"Cannot schedule unknown task {task_id}")
                return False
            
            task = self.tasks[task_id]
            task.schedule_next_run(next_run)
            return True
    
    def _can_run_task(self, task_id: str) -> bool:
        """
        Check if a task can run based on its dependencies.
        
        Args:
            task_id: ID of the task to check
            
        Returns:
            bool: True if all dependencies have completed successfully
        """
        # If task has no dependencies, it can always run
        if task_id not in self.task_dependencies or not self.task_dependencies[task_id]:
            return True
        
        # Check each dependency
        for dep_id in self.task_dependencies[task_id]:
            # Check if dependency exists
            if dep_id not in self.tasks:
                logger.warning(f"Task {task_id} has unknown dependency {dep_id}")
                return False
                
            # Check if dependency is currently running
            if dep_id in self.running_tasks:
                logger.debug(f"Task {task_id} waiting for dependency {dep_id} to complete")
                return False
                
            # Check if dependency has results
            if dep_id not in self.task_results:
                logger.debug(f"Task {task_id} waiting for dependency {dep_id} to run")
                return False
            
            # Check if dependency completed successfully
            if not self.task_results[dep_id].success:
                logger.warning(f"Task {task_id} blocked: dependency {dep_id} failed")
                return False
        
        logger.debug(f"All dependencies for task {task_id} are satisfied")
        return True
    
    def _execute_task(self, task_id: str) -> None:
        """
        Execute a task in a separate thread.
        
        Args:
            task_id: ID of the task to execute
        """
        task = self.tasks[task_id]
        
        try:
            # Mark task as running
            with self.lock:
                self.running_tasks.add(task_id)
            
            logger.info(f"Executing task {task_id}")
            
            # Execute the task and store the result
            result = task.execute()
            
            with self.lock:
                self.task_results[task_id] = result
            
            logger.info(f"Task {task_id} completed with status: {result.success}")
            
            # Handle task failure with retry if needed
            if not result.success and task.should_retry():
                retry_time = datetime.now() + timedelta(seconds=task.retry_delay)
                logger.info(f"Scheduling retry for task {task_id} at {retry_time.isoformat()}")
                self.schedule_task(task_id, retry_time)
                
        except Exception as e:
            logger.error(f"Error executing task {task_id}: {str(e)}", exc_info=True)
        finally:
            # Mark task as no longer running
            with self.lock:
                self.running_tasks.discard(task_id)
                self.task_threads.pop(task_id, None)
    
    def _execute_task_sync(self, task_id: str) -> TaskResult:
        """
        Execute a task synchronously (for testing).
        
        Args:
            task_id: ID of the task to execute
            
        Returns:
            TaskResult: Result of the task execution
        """
        task = self.tasks[task_id]
        
        try:
            # Mark task as running
            with self.lock:
                self.running_tasks.add(task_id)
            
            logger.info(f"Executing task {task_id} synchronously")
            
            # Execute the task
            result = task.execute()
            
            # Store the result
            with self.lock:
                self.task_results[task_id] = result
            
            logger.info(f"Task {task_id} completed with status: {result.success}")
            
            return result
                
        except Exception as e:
            logger.error(f"Error executing task {task_id}: {str(e)}", exc_info=True)
            raise
        finally:
            # Mark task as no longer running
            with self.lock:
                self.running_tasks.discard(task_id)
    
    def _check_schedule(self) -> None:
        """Check for tasks that are due to run and execute them."""
        now = datetime.now()
        
        with self.lock:
            for task_id, task in self.tasks.items():
                # Skip tasks that are already running
                if task_id in self.running_tasks:
                    continue
                
                # Skip tasks that don't have a next run time
                if not task.next_run:
                    continue
                
                # Check if task is due to run
                if task.next_run <= now:
                    # Check if dependencies are satisfied
                    if self._can_run_task(task_id):
                        logger.info(f"Task {task_id} is due to run")
                        
                        # Clear the next run time to prevent repeated execution
                        # It will be rescheduled if this is a recurring task
                        task.next_run = None
                        
                        # Start task in a new thread
                        thread = threading.Thread(
                            target=self._execute_task,
                            args=(task_id,),
                            name=f"task-{task_id}"
                        )
                        thread.daemon = True
                        thread.start()
                        
                        self.task_threads[task_id] = thread
                    else:
                        logger.info(f"Task {task_id} is due but dependencies not met")
    
    def start(self) -> None:
        """Start the scheduler."""
        logger.info("Starting scheduler")
        
        # Set up signal handlers, but only in the main thread
        if threading.current_thread() is threading.main_thread():
            def handle_signal(signum, frame):
                logger.info(f"Received signal {signum}, stopping scheduler")
                self.stop()
            
            signal.signal(signal.SIGINT, handle_signal)
            signal.signal(signal.SIGTERM, handle_signal)
        
        # Reset stop event
        self.stop_event.clear()
        
        try:
            while not self.stop_event.is_set():
                try:
                    self._check_schedule()
                except Exception as e:
                    logger.error(f"Error in scheduler loop: {str(e)}", exc_info=True)
                
                # Wait for next check interval
                self.stop_event.wait(self.check_interval)
        finally:
            logger.info("Scheduler stopped")
    
    def stop(self) -> None:
        """Stop the scheduler."""
        logger.info("Stopping scheduler")
        self.stop_event.set()
        
        # Wait for all tasks to complete with timeout
        logger.info("Waiting for running tasks to complete")
        deadline = time.time() + 30  # 30 second timeout
        
        while time.time() < deadline:
            with self.lock:
                if not self.running_tasks:
                    break
            time.sleep(0.1)
        
        # Force stop any remaining tasks
        with self.lock:
            if self.running_tasks:
                logger.warning(f"Forcing stop with {len(self.running_tasks)} tasks still running")
    
    def get_task_status(self, task_id: str) -> Dict:
        """
        Get the current status of a task.
        
        Args:
            task_id: ID of the task
            
        Returns:
            Dict: Status information for the task
        """
        with self.lock:
            if task_id not in self.tasks:
                return {"error": f"Task {task_id} not found"}
            
            task = self.tasks[task_id]
            
            status = {
                "task_id": task_id,
                "description": task.description,
                "is_running": task_id in self.running_tasks,
                "last_run": task.last_run.isoformat() if task.last_run else None,
                "next_run": task.next_run.isoformat() if task.next_run else None,
                "dependencies": list(self.task_dependencies.get(task_id, [])),
                "retries_attempted": task.retries_attempted,
            }
            
            # Include last result if available
            if task_id in self.task_results:
                result = self.task_results[task_id]
                status.update({
                    "last_result": {
                        "success": result.success,
                        "message": result.message,
                        "start_time": result.start_time.isoformat(),
                        "end_time": result.end_time.isoformat(),
                        "duration": result.duration
                    }
                })
            
            return status
    
    def get_all_task_statuses(self) -> Dict:
        """
        Get status information for all registered tasks.
        
        Returns:
            Dict: Status information keyed by task ID
        """
        with self.lock:
            return {
                task_id: self.get_task_status(task_id)
                for task_id in self.tasks
            }
