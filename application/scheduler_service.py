#!/usr/bin/env python3
"""
Scheduler Service - Cron-based pipeline execution scheduling
"""

import sys
from pathlib import Path
from typing import Dict, Any, List, Optional
from domain.pipeline import PipelineConfig
from infrastructure.database.unified_db import UnifiedDatabase
from infrastructure.database.config_service import ConfigService
from application.pipeline_manager import PipelineManager
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.events import EVENT_JOB_EXECUTED, EVENT_JOB_ERROR, EVENT_JOB_MISSED
from infrastructure.database.logging_service import LoggingService, LogLevel
import logging
from datetime import datetime
import json

class SchedulerService:
    """
    Cron-based pipeline execution scheduler
    """
    
    def __init__(self, db: UnifiedDatabase):
        self.db = db
        self.config_service = ConfigService(db)
        self.pipeline_manager = PipelineManager(db)
        
        # Initialize background scheduler
        self.scheduler = BackgroundScheduler()
        self.scheduler.start()
        
        # Job tracking
        self.job_registry: Dict[str, str] = {}  # pipeline_id -> job_id
        
        # Set up logging for scheduler events
        self._setup_event_logging()
    
    def _setup_event_logging(self):
        """
        Set up event listeners for scheduler monitoring
        """
        def job_executed(event):
            self._log_job_event("EXECUTED", event.job_id, event.exception)
        
        def job_error(event):
            self._log_job_event("ERROR", event.job_id, event.exception)
        
        def job_missed(event):
            self._log_job_event("MISSED", event.job_id, "Job missed execution time")
        
        self.scheduler.add_listener(job_executed, EVENT_JOB_EXECUTED)
        self.scheduler.add_listener(job_error, EVENT_JOB_ERROR)
        self.scheduler.add_listener(job_missed, EVENT_JOB_MISSED)
    
    def _log_job_event(self, event_type: str, job_id: str, exception: Optional[Exception]):
        """
        Log scheduler events
        """
        log_data = {
            "event_type": event_type,
            "job_id": job_id,
            "timestamp": datetime.now().isoformat(),
            "exception": str(exception) if exception else None
        }
        
        # Log to database
        logging_service = LoggingService(self.db)
        logging_service.log_message(
            level=LogLevel.INFO if event_type == "EXECUTED" else LogLevel.ERROR,
            message=f"Scheduler event: {event_type}",
            extra_data=log_data
        )
    
    def schedule_pipeline(self, pipeline_id: str, cron_expression: str, 
                         document_paths: List[str], run_metadata: Optional[Dict[str, Any]] = None) -> str:
        """
        Schedule pipeline execution
        Args:
            pipeline_id: Pipeline identifier
            cron_expression: Cron schedule expression (e.g., "0 2 * * *" for daily at 2 AM)
            document_paths: List of document paths to process
            run_meta Additional metadata for scheduled runs
        Returns:
            str: Job ID
        Raises:
            ValueError: If cron expression is invalid or pipeline doesn't exist
        """
        # Validate pipeline exists
        config = self.pipeline_manager.get_pipeline_config(pipeline_id)
        if not config:
            raise ValueError(f"Pipeline not found: {pipeline_id}")
        
        # Validate cron expression
        try:
            CronTrigger.from_crontab(cron_expression)
        except ValueError as e:
            raise ValueError(f"Invalid cron expression: {cron_expression}. Error: {e}")
        
        # Create job function
        job_func = self._create_pipeline_job(pipeline_id, document_paths, run_metadata)
        
        # Schedule job
        job = self.scheduler.add_job(
            func=job_func,
            trigger=CronTrigger.from_crontab(cron_expression),
            id=f"pipeline_{pipeline_id}_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
            name=f"Pipeline {pipeline_id} - {config.name}",
            replace_existing=True
        )
        
        # Register job in registry
        self.job_registry[pipeline_id] = job.id
        
        # Log scheduling
        logging_service = LoggingService(self.db)
        logging_service.log_message(
            level=LogLevel.INFO,
            message=f"Pipeline scheduled: {pipeline_id} with cron {cron_expression}",
            pipeline_id=pipeline_id,
            extra_data={
                "cron_expression": cron_expression,
                "document_count": len(document_paths),
                "job_id": job.id
            }
        )
        
        return job.id
    
    def _create_pipeline_job(self, pipeline_id: str, document_paths: List[str], 
                           run_metadata :Optional[Dict[str, Any]]) -> callable:
        """
        Create job function for pipeline execution
        """
        def job_function():
            try:
                # Execute pipeline
                run_id = self.pipeline_manager.execute_pipeline(
                    pipeline_id, 
                    document_paths, 
                    run_metadata
                )
                
                # Log successful execution
                logging_service = LoggingService(self.db)
                logging_service.log_message(
                    level=LogLevel.INFO,
                    message=f"Scheduled pipeline executed successfully: {pipeline_id}",
                    pipeline_id=pipeline_id,
                    extra_data={
                        "run_id": run_id,
                        "document_count": len(document_paths)
                    }
                )
                
            except Exception as e:
                # Log execution error
                logging_service = LoggingService(self.db)
                logging_service.log_message(
                    level=LogLevel.ERROR,
                    message=f"Scheduled pipeline execution failed: {pipeline_id}",
                    pipeline_id=pipeline_id,
                    extra_data={
                        "error_type": type(e).__name__,
                        "error_message": str(e),
                        "document_count": len(document_paths)
                    }
                )
        
        return job_function
    
    def cancel_scheduled_pipeline(self, pipeline_id: str) -> bool:
        """
        Cancel scheduled pipeline execution
        Args:
            pipeline_id: Pipeline identifier
        Returns:
            bool: True if cancelled successfully
        """
        if pipeline_id not in self.job_registry:
            return False
        
        job_id = self.job_registry[pipeline_id]
        
        try:
            self.scheduler.remove_job(job_id)
            del self.job_registry[pipeline_id]
            
            # Log cancellation
            logging_service = LoggingService(self.db)
            logging_service.log_message(
                level=LogLevel.INFO,
                message=f"Pipeline schedule cancelled: {pipeline_id}",
                pipeline_id=pipeline_id,
                extra_data={"job_id": job_id}
            )
            
            return True
        except Exception:
            return False
    
    def reschedule_pipeline(self, pipeline_id: str, new_cron_expression: str) -> bool:
        """
        Reschedule existing pipeline with new cron expression
        Args:
            pipeline_id: Pipeline identifier
            new_cron_expression: New cron schedule expression
        Returns:
            bool: True if rescheduled successfully
        """
        if pipeline_id not in self.job_registry:
            return False
        
        # Get current job info
        job_id = self.job_registry[pipeline_id]
        job = self.scheduler.get_job(job_id)
        
        if not job:
            return False
        
        # Validate new cron expression
        try:
            CronTrigger.from_crontab(new_cron_expression)
        except ValueError as e:
            raise ValueError(f"Invalid cron expression: {new_cron_expression}. Error: {e}")
        
        # Reschedule job
        self.scheduler.reschedule_job(job_id, trigger=CronTrigger.from_crontab(new_cron_expression))
        
        # Log rescheduling
        logging_service = LoggingService(self.db)
        logging_service.log_message(
            level=LogLevel.INFO,
            message=f"Pipeline rescheduled: {pipeline_id}",
            pipeline_id=pipeline_id,
            extra_data={
                "old_cron": job.trigger.cron,
                "new_cron": new_cron_expression
            }
        )
        
        return True
    
    def get_scheduled_pipelines(self) -> List[Dict[str, Any]]:
        """
        Get list of all scheduled pipelines
        Returns:
            List of scheduled pipeline information
        """
        scheduled = []
        
        for job in self.scheduler.get_jobs():
            # Extract pipeline ID from job ID (assuming format: pipeline_{id}_timestamp)
            if job.id.startswith("pipeline_"):
                parts = job.id.split('_')
                if len(parts) >= 3:
                    pipeline_id = "_".join(parts[1:-2])  # Extract pipeline ID
                    
                    # Get pipeline config to get name
                    config = self.pipeline_manager.get_pipeline_config(pipeline_id)
                    pipeline_name = config.name if config else "Unknown"
                    
                    scheduled.append({
                        "pipeline_id": pipeline_id,
                        "pipeline_name": pipeline_name,
                        "job_id": job.id,
                        "next_run_time": job.next_run_time.isoformat() if job.next_run_time else None,
                        "cron_expression": str(job.trigger),
                        "misfire_grace_time": job.misfire_grace_time
                    })
        
        return scheduled
    
    def get_next_run_time(self, pipeline_id: str) -> Optional[datetime]:
        """
        Get next scheduled run time for pipeline
        Args:
            pipeline_id: Pipeline identifier
        Returns:
            datetime: Next run time or None if not scheduled
        """
        if pipeline_id not in self.job_registry:
            return None
        
        job_id = self.job_registry[pipeline_id]
        job = self.scheduler.get_job(job_id)
        
        return job.next_run_time if job else None
    
    def pause_scheduler(self):
        """
        Pause all scheduled jobs
        """
        self.scheduler.pause()
        
        # Log pause
        logging_service = LoggingService(self.db)
        logging_service.log_message(
            level=LogLevel.WARNING,
            message="Scheduler paused - all jobs suspended"
        )
    
    def resume_scheduler(self):
        """
        Resume all scheduled jobs
        """
        self.scheduler.resume()
        
        # Log resume
        logging_service = LoggingService(self.db)
        logging_service.log_message(
            level=LogLevel.INFO,
            message="Scheduler resumed - all jobs active"
        )
    
    def shutdown(self):
        """
        Shutdown scheduler service
        """
        self.scheduler.shutdown(wait=True)
        
        # Log shutdown
        logging_service = LoggingService(self.db)
        logging_service.log_message(
            level=LogLevel.INFO,
            message="Scheduler service shut down"
        )
    
    def validate_cron_expression(self, cron_expr: str) -> bool:
        """
        Validate cron expression format
        Args:
            cron_expr: Cron expression to validate
        Returns:
            bool: True if valid
        """
        try:
            CronTrigger.from_crontab(cron_expr)
            return True
        except ValueError:
            return False