#!/usr/bin/env python3
"""
Pipeline Manager - Central orchestrator for pipeline operations
Manages pipeline lifecycle: creation, validation, execution, monitoring
"""

import sys
from pathlib import Path
from typing import Dict, Any, List, Optional, Union
from domain.pipeline import PipelineConfig, PipelineStepConfig, PipelineRun, PipelineStatus, StepType
from domain.document import Document
from infrastructure.database.unified_db import UnifiedDatabase
from infrastructure.database.config_service import ConfigService
from infrastructure.database.script_manager import ScriptManager
from infrastructure.database.logging_service import LoggingService, LogLevel
from infrastructure.loaders.document_factory import DocumentFactory
from infrastructure.security.script_sandbox import SecurityError
from application.task_dispatcher import TaskDispatcher
from application.resource_monitor import ResourceMonitor
from datetime import datetime
import json
import secrets
import threading
import time
import os

class PipelineManager:
    """
    Manages pipeline lifecycle operations
    """
    
    def __init__(self, db: UnifiedDatabase):
        self.db = db
        self.config_service = ConfigService(db)
        self.script_manager = ScriptManager(db)
        self.logging_service = LoggingService(db)
        self.task_dispatcher = TaskDispatcher(db)
        self.resource_monitor = ResourceMonitor()
        
        # Active pipeline runs
        self.active_runs: Dict[str, PipelineRun] = {}
        self.run_lock = threading.Lock()
    
    def create_pipeline(self, config: PipelineConfig) -> str:
        """
        Create new pipeline configuration
        Args:
            config: Pipeline configuration
        Returns:
            str: Pipeline ID
        Raises:
            ValueError: If configuration is invalid
            SecurityError: If scripts fail security validation
        """
        # Validate configuration
        validation_errors = self.validate_pipeline_config(config)
        if validation_errors:
            raise ValueError(f"Pipeline configuration validation failed: {validation_errors}")
        
        # Validate all scripts in pipeline steps
        for step in config.steps:
            if step.type == StepType.USER_SCRIPT:
                script_id = step.params.get("script_id")
                if script_id:
                    script_data = self.script_manager.load_script(script_id)
                    if not script_data:
                        raise ValueError(f"Script not found: {script_id}")
                    
                    # Validate script security
                    from infrastructure.security.script_sandbox import ScriptSecurityValidator
                    security_errors = ScriptSecurityValidator.validate_script_security(script_data["code"])
                    if security_errors:
                        raise SecurityError(f"Script security validation failed: {security_errors}")
        
        # Save to database
        pipeline_id = self.config_service.save_pipeline_config(config)
        
        # Log creation
        self.logging_service.log_message(
            level=LogLevel.INFO,
            message=f"Pipeline created: {config.name} ({pipeline_id})",
            pipeline_id=pipeline_id
        )
        
        return pipeline_id
    
    def update_pipeline(self, pipeline_id: str, config: PipelineConfig) -> bool:
        """
        Update existing pipeline configuration
        Args:
            pipeline_id: Pipeline identifier
            config: Updated configuration
        Returns:
            bool: True if updated successfully
        """
        # Validate new configuration
        validation_errors = self.validate_pipeline_config(config)
        if validation_errors:
            raise ValueError(f"Pipeline configuration validation failed: {validation_errors}")
        
        # Check if pipeline is currently running
        with self.run_lock:
            if pipeline_id in self.active_runs:
                raise RuntimeError(f"Cannot update pipeline {pipeline_id} - currently running")
        
        # Update in database
        success = self.config_service.update_pipeline_config(pipeline_id, config)
        
        if success:
            self.logging_service.log_message(
                level=LogLevel.INFO,
                message=f"Pipeline updated: {config.name} ({pipeline_id})",
                pipeline_id=pipeline_id
            )
        
        return success
    
    def delete_pipeline(self, pipeline_id: str) -> bool:
        """
        Delete pipeline configuration (soft delete)
        Args:
            pipeline_id: Pipeline identifier
        Returns:
            bool: True if deleted successfully
        """
        # Check if pipeline is currently running
        with self.run_lock:
            if pipeline_id in self.active_runs:
                raise RuntimeError(f"Cannot delete pipeline {pipeline_id} - currently running")
        
        # Soft delete in database
        success = self.config_service.delete_pipeline_config(pipeline_id)
        
        if success:
            self.logging_service.log_message(
                level=LogLevel.INFO,
                message=f"Pipeline deleted: {pipeline_id}",
                pipeline_id=pipeline_id
            )
        
        return success
    
    def get_pipeline_config(self, pipeline_id: str) -> Optional[PipelineConfig]:
        """
        Get pipeline configuration
        Args:
            pipeline_id: Pipeline identifier
        Returns:
            PipelineConfig: Configuration or None if not found
        """
        return self.config_service.load_pipeline_config(pipeline_id)
    
    def list_pipelines(self, active_only: bool = True) -> List[Dict[str, Any]]:
        """
        List all pipeline configurations
        Args:
            active_only: Only return active pipelines
        Returns:
            List of pipeline metadata
        """
        return self.config_service.list_pipeline_configs(active_only=active_only)
    
    def validate_pipeline_config(self, config: PipelineConfig) -> List[str]:
        """
        Validate pipeline configuration
        Args:
            config: Pipeline configuration to validate
        Returns:
            List of validation errors (empty if valid)
        """
        errors = []
        
        # Basic validation
        if not config.name.strip():
            errors.append("Pipeline name cannot be empty")
        
        if not config.steps:
            errors.append("Pipeline must have at least one step")
        
        # Validate step connections
        step_ids = {step.id for step in config.steps}
        
        for i, step in enumerate(config.steps):
            if not step.id:
                errors.append(f"Step {i+1} has no ID")
            
            # Check input step references
            if step.input_step_id and step.input_step_id not in step_ids:
                errors.append(f"Step {step.id} references non-existent input step: {step.input_step_id}")
            
            # Check dependency references
            for dep_id in step.depends_on:
                if dep_id not in step_ids:
                    errors.append(f"Step {step.id} has dependency on non-existent step: {dep_id}")
        
        # Validate step-specific parameters
        for step in config.steps:
            step_errors = self._validate_step_config(step)
            errors.extend(step_errors)
        
        return errors
    
    def _validate_step_config(self, step: PipelineStepConfig) -> List[str]:
        """
        Validate individual step configuration
        """
        errors = []
        
        if step.type == StepType.USER_SCRIPT:
            script_id = step.params.get("script_id")
            if not script_id:
                errors.append(f"Script step {step.id} requires 'script_id' parameter")
            else:
                # Check if script exists and is valid
                script_data = self.script_manager.load_script(script_id)
                if not script_data:
                    errors.append(f"Script not found: {script_id}")
        
        elif step.type == StepType.DOCUMENT_LOADER:
            source_path = step.params.get("source_path")
            if not source_path:
                errors.append(f"Document loader step {step.id} requires 'source_path' parameter")
            elif not os.path.exists(source_path):
                errors.append(f"Source path does not exist: {source_path}")
        
        elif step.type == StepType.DB_EXPORTER:
            table_name = step.params.get("table_name")
            if not table_name:
                errors.append(f"DB exporter step {step.id} requires 'table_name' parameter")
        
        return errors
    
    def execute_pipeline(self, pipeline_id: str, document_paths: List[str], 
                        run_metadata: Optional[Dict[str, Any]] = None) -> str:
        """
        Execute pipeline for list of documents
        Args:
            pipeline_id: Pipeline identifier
            document_paths: List of document file paths to process
            run_meta Additional metadata for the run
        Returns:
            str: Pipeline run ID
        Raises:
            ValueError: If pipeline or documents are invalid
            RuntimeError: If pipeline is already running
        """
        # Load pipeline configuration
        config = self.get_pipeline_config(pipeline_id)
        if not config:
            raise ValueError(f"Pipeline not found: {pipeline_id}")
        
        # Check if pipeline is already running
        with self.run_lock:
            if pipeline_id in self.active_runs:
                raise RuntimeError(f"Pipeline {pipeline_id} is already running")
        
        # Validate document paths
        errors = []
        valid_paths = []
        for path in document_paths:
            if not os.path.exists(path):
                errors.append(f"Document path does not exist: {path}")
            elif not os.path.isfile(path):
                errors.append(f"Not a file: {path}")
            else:
                valid_paths.append(path)
        
        if errors:
            raise ValueError(f"Invalid document paths: {errors}")
        
        if not valid_paths:
            raise ValueError("No valid document paths provided")
        
        # Create pipeline run
        run_id = f"run_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{secrets.token_hex(4)}"
        run = PipelineRun(
            id=run_id,
            pipeline_id=pipeline_id,
            start_time=datetime.now(),
            status=PipelineStatus.RUNNING,
            document_paths=valid_paths,
            metadata=run_metadata or {}
        )
        
        # Track active run
        with self.run_lock:
            self.active_runs[pipeline_id] = run
        
        try:
            # Use task dispatcher to process documents in parallel
            results = self.task_dispatcher.process_documents_parallel(config, valid_paths)
            
            # Update run status to completed
            run.end_time = datetime.now()
            run.status = PipelineStatus.COMPLETED
            run.processed_count = results["processed_count"]
            run.success_count = results["success_count"]
            run.error_count = results["error_count"]
            run.errors = results["errors"]
            
            # Log completion
            self.logging_service.log_pipeline_run(run)
            
            return run_id
            
        except Exception as e:
            # Handle execution error
            run.end_time = datetime.now()
            run.status = PipelineStatus.FAILED
            run.error_count = 1
            run.errors = [{
                "timestamp": datetime.now().isoformat(),
                "error_type": type(e).__name__,
                "error_message": str(e),
                "stage": "execution"
            }]
            
            # Log failure
            self.logging_service.log_pipeline_run(run)
            
            # Attempt recovery using error recovery service (import inside function)
            from application.error_recovery import ErrorRecoveryService
            error_recovery = ErrorRecoveryService(self.db)
            error_recovery.handle_pipeline_failure(run, str(e))
            
            raise
        
        finally:
            # Remove from active runs
            with self.run_lock:
                if pipeline_id in self.active_runs:
                    del self.active_runs[pipeline_id]
    
    def get_pipeline_status(self, pipeline_id: str) -> Dict[str, Any]:
        """
        Get current status of pipeline (including active runs)
        """
        # Check if pipeline is currently running
        with self.run_lock:
            if pipeline_id in self.active_runs:
                run = self.active_runs[pipeline_id]
                return {
                    "status": "RUNNING",
                    "current_run": run.to_dict(),
                    "progress": self._calculate_progress(run)
                }
        
        # Get historical status from database
        return self.config_service.get_pipeline_statistics(pipeline_id)
    
    def _calculate_progress(self, run: PipelineRun) -> Dict[str, Any]:
        """
        Calculate execution progress
        """
        return {
            "processed": run.processed_count,
            "successful": run.success_count,
            "failed": run.error_count,
            "total": len(run.document_paths),
            "percentage": (run.processed_count / len(run.document_paths) * 100) if run.document_paths else 0,
            "elapsed_time": (datetime.now() - run.start_time).total_seconds()
        }
    
    def cancel_running_pipeline(self, pipeline_id: str) -> bool:
        """
        Cancel currently running pipeline
        Args:
            pipeline_id: Pipeline identifier
        Returns:
            bool: True if cancelled successfully
        """
        with self.run_lock:
            if pipeline_id not in self.active_runs:
                return False
            
            run = self.active_runs[pipeline_id]
            run.end_time = datetime.now()
            run.status = PipelineStatus.CANCELLED
            
            # Log cancellation
            self.logging_service.log_pipeline_run(run)
            
            # Remove from active runs
            del self.active_runs[pipeline_id]
            
            # Log cancellation event
            self.logging_service.log_message(
                level=LogLevel.WARNING,
                message=f"Pipeline cancelled: {pipeline_id}",
                pipeline_id=pipeline_id,
                pipeline_run_id=run.id
            )
            
            return True
    
    def get_pipeline_history(self, pipeline_id: str, limit: int = 50) -> List[Dict[str, Any]]:
        """
        Get execution history for pipeline
        Args:
            pipeline_id: Pipeline identifier
            limit: Maximum number of runs to return
        Returns:
            List of pipeline run records
        """
        return self.logging_service.get_run_history(pipeline_id, limit)
    
    def get_all_active_runs(self) -> Dict[str, Dict[str, Any]]:
        """
        Get all currently active pipeline runs
        Returns:
            Dict mapping pipeline_id to run information
        """
        with self.run_lock:
            return {
                pid: {
                    "run_id": run.id,
                    "pipeline_name": self.config_service.get_pipeline_name(pid),
                    "start_time": run.start_time.isoformat(),
                    "processed_count": run.processed_count,
                    "success_count": run.success_count,
                    "error_count": run.error_count,
                    "progress": self._calculate_progress(run)
                }
                for pid, run in self.active_runs.items()
            }