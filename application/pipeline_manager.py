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
from application.task_dispatcher import TaskDispatcher
from application.error_recovery import ErrorRecoveryService
from application.resource_monitor import ResourceMonitor
from infrastructure.security.script_sandbox import ScriptSandbox, ScriptSecurityValidator, SecurityError, ScriptExecutionError, ScriptExecutionTimeout
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
        self.error_recovery = ErrorRecoveryService(db)
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
            run_metadata Additional metadata for the run
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
            # Execute pipeline steps
            self._execute_pipeline_steps(run, config, valid_paths)
            
            # Update run status to completed
            run.end_time = datetime.now()
            run.status = PipelineStatus.COMPLETED
            
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
            
            # Attempt recovery
            self.error_recovery.handle_pipeline_failure(run, str(e))
            
            raise
        
        finally:
            # Remove from active runs
            with self.run_lock:
                if pipeline_id in self.active_runs:
                    del self.active_runs[pipeline_id]
    
    def _execute_pipeline_steps(self, run: PipelineRun, config: PipelineConfig, document_paths: List[str]):
        """
        Execute pipeline steps for given documents
        """
        # Initialize step results storage
        step_results = {}
        
        # Execute each step in sequence
        for step_config in config.steps:
            step_start_time = datetime.now()
            
            try:
                # Get input for this step
                input_data = self._get_step_input(step_config, step_results)
                
                # Execute step
                output_data = self._execute_step(step_config, input_data, run)
                
                # Store results
                step_results[step_config.id] = {
                    "output": output_data,
                    "execution_time": (datetime.now() - step_start_time).total_seconds()
                }
                
                # Log step completion
                self.logging_service.log_message(
                    level=LogLevel.INFO,
                    message=f"Step completed: {step_config.name}",
                    pipeline_id=run.pipeline_id,
                    pipeline_run_id=run.id,
                    extra_data={
                        "step_id": step_config.id,
                        "execution_time": step_results[step_config.id]["execution_time"],
                        "output_count": len(output_data) if isinstance(output_data, list) else 1
                    }
                )
                
            except Exception as e:
                # Log step failure
                error_msg = f"Step failed: {step_config.name} ({step_config.id}) - {str(e)}"
                self.logging_service.log_message(
                    level=LogLevel.ERROR,
                    message=error_msg,
                    pipeline_id=run.pipeline_id,
                    pipeline_run_id=run.id,
                    extra_data={
                        "step_id": step_config.id,
                        "error_type": type(e).__name__,
                        "execution_time": (datetime.now() - step_start_time).total_seconds()
                    }
                )
                
                # Add to run errors
                run.errors.append({
                    "timestamp": datetime.now().isoformat(),
                    "step_id": step_config.id,
                    "error_type": type(e).__name__,
                    "error_message": str(e),
                    "stage": f"step_{step_config.name}"
                })
                
                # If step is critical (not optional), stop pipeline
                if not step_config.params.get("optional", False):
                    raise
    
    def _get_step_input(self, step_config: PipelineStepConfig, step_results: Dict[str, Any]):
        """
        Get input data for step based on configuration
        """
        if step_config.input_step_id:
            # Use output from previous step
            prev_result = step_results.get(step_config.input_step_id)
            if prev_result:
                return prev_result["output"]
        
        # For initial steps, input comes from document paths
        if step_config.type == StepType.DOCUMENT_LOADER:
            return step_config.params.get("document_paths", [])
        
        # For other steps without explicit input, return empty
        return []
    
    def _execute_step(self, step_config: PipelineStepConfig, input_data, run: PipelineRun):
        """
        Execute individual pipeline step
        """
        if step_config.type == StepType.DOCUMENT_LOADER:
            return self._execute_document_loader_step(step_config, input_data, run)
        
        elif step_config.type == StepType.USER_SCRIPT:
            return self._execute_script_step(step_config, input_data, run)
        
        elif step_config.type == StepType.LINE_SPLITTER:
            return self._execute_line_splitter_step(step_config, input_data, run)
        
        elif step_config.type == StepType.DELIMITER_SPLITTER:
            return self._execute_delimiter_splitter_step(step_config, input_data, run)
        
        elif step_config.type == StepType.DB_EXPORTER:
            return self._execute_db_exporter_step(step_config, input_data, run)
        
        else:
            # Use generic processor for other step types
            processor = self._get_step_processor(step_config.type)
            return processor.process(input_data, step_config.params)
    
    def _execute_document_loader_step(self, step_config: PipelineStepConfig, document_paths: List[str], run: PipelineRun):
        """
        Execute document loader step
        """
        loaded_documents = []
    
        for path in document_paths:
            try:
                # Use the DocumentFactory to create loader (fixed import)
                from infrastructure.loaders.document_factory import DocumentFactory
                loader = DocumentFactory.create_loader(path)  # ← This method now exists!
            
                # Load document
                doc = loader.load({
                    "path": path,
                    "style_config_path": step_config.params.get("style_config_path")
                })
                loaded_documents.append(doc)
            
                # Update run progress
                run.processed_count += 1
                run.success_count += 1
            
            except Exception as e:
                run.processed_count += 1
                run.error_count += 1
                run.errors.append({
                    "timestamp": datetime.now().isoformat(),
                    "document_path": path,
                    "error_type": type(e).__name__,
                    "error_message": str(e),
                    "stage": "document_loading"
                })
    
        return loaded_documents
    
    def _execute_script_step(self, step_config: PipelineStepConfig, input_data, run: PipelineRun):
        """
        Execute user script step
        """
        script_id = step_config.params.get("script_id")
        if not script_id:
            raise ValueError("Script step requires 'script_id' parameter")
        
        # Load and execute script for each input item
        results = []
        
        for item in input_data:
            try:
                context = {
                    "input": item,
                    "pipeline_run": run,
                    "step_config": step_config
                }
                
                result = self.script_manager.validate_and_execute_script(script_id, context)
                results.append(result)
                
            except Exception as e:
                run.error_count += 1
                run.errors.append({
                    "timestamp": datetime.now().isoformat(),
                    "error_type": type(e).__name__,
                    "error_message": str(e),
                    "stage": f"script_execution_{script_id}",
                    "input_item": str(item)[:100]  # First 100 chars of input
                })
        
        return results
    
    def _execute_line_splitter_step(self, step_config: PipelineStepConfig, input_data, run: PipelineRun):
        """
        Execute line splitter step
        """
        from infrastructure.processors.line_splitter import LineSplitter
        splitter = LineSplitter()
        
        results = []
        for item in input_data:
            try:
                split_results = splitter.process(item, step_config.params)
                results.extend(split_results)
            except Exception as e:
                run.error_count += 1
                run.errors.append({
                    "timestamp": datetime.now().isoformat(),
                    "error_type": type(e).__name__,
                    "error_message": str(e),
                    "stage": "line_splitting"
                })
        
        return results
    
    def _execute_delimiter_splitter_step(self, step_config: PipelineStepConfig, input_data, run: PipelineRun):
        """
        Execute delimiter splitter step
        """
        from infrastructure.processors.delimiter_splitter import DelimiterSplitter
        splitter = DelimiterSplitter()
        
        results = []
        for item in input_data:
            try:
                split_results = splitter.process(item, step_config.params)
                results.extend(split_results)
            except Exception as e:
                run.error_count += 1
                run.errors.append({
                    "timestamp": datetime.now().isoformat(),
                    "error_type": type(e).__name__,
                    "error_message": str(e),
                    "stage": "delimiter_splitting"
                })
        
        return results
    
    def _execute_db_exporter_step(self, step_config: PipelineStepConfig, input_data, run: PipelineRun):
        """
        Execute database exporter step
        """
        from infrastructure.exporters.target_db_exporter import TargetDbExporter
        exporter = TargetDbExporter()
        
        try:
            # Connect to database
            db_config = step_config.params.get("db_config", {})
            exporter.connect(db_config)
            
            # Export data
            table_name = step_config.params.get("table_name", "chunks")
            if isinstance(input_data, list):
                exporter.batch_insert(input_data, table_name)
            else:
                exporter.batch_insert([input_data], table_name)
            
            exporter.close()
            
        except Exception as e:
            run.error_count += 1
            run.errors.append({
                "timestamp": datetime.now().isoformat(),
                "error_type": type(e).__name__,
                "error_message": str(e),
                "stage": "database_export"
            })
            raise
        
        return input_data  # Return original data (exporter doesn't transform)
    
    def _get_step_processor(self, step_type: StepType):
        """
        Get appropriate processor for step type
        """
        from domain.interfaces import IChunkProcessor
        
        processor_map = {
            StepType.LINE_SPLITTER: "infrastructure.processors.LineSplitter",
            StepType.DELIMITER_SPLITTER: "infrastructure.processors.DelimiterSplitter", 
            StepType.PARAGRAPH_SPLITTER: "infrastructure.processors.ParagraphSplitter",
            StepType.SENTENCE_SPLITTER: "infrastructure.processors.SentenceSplitter",
            StepType.REGEX_EXTRACTOR: "infrastructure.processors.RegexExtractor",
            StepType.METADATA_PROPAGATOR: "infrastructure.processors.MetadataPropagator"
        }
        
        if step_type in processor_map:
            module_path, class_name = processor_map[step_type].rsplit('.', 1)
            module = __import__(module_path, fromlist=[class_name])
            return getattr(module, class_name)()
        
        raise ValueError(f"No processor found for step type: {step_type}")
    
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
    
    def get_default_pipeline_config(self) -> PipelineConfig:
        """
        Get default pipeline configuration template
        """
        from domain.pipeline import PipelineConfig, PipelineStepConfig, StepType
        
        return PipelineConfig(
            name="Default Pipeline",
            description="Basic document processing pipeline",
            steps=[
                PipelineStepConfig(
                    type=StepType.DOCUMENT_LOADER,
                    name="Load Documents",
                    params={
                        "source_path": "",  # Will be filled during execution
                        "style_config_path": "",
                        "batch_size": 100
                    }
                ),
                PipelineStepConfig(
                    type=StepType.LINE_SPLITTER,
                    name="Split to Lines",
                    params={"preserve_empty": True},
                    input_step_id=""  # Will be set during pipeline execution
                ),
                PipelineStepConfig(
                    type=StepType.DB_EXPORTER,
                    name="Export to Database",
                    params={
                        "table_name": "chunks",
                        "batch_size": 1000
                    },
                    input_step_id=""
                )
            ]
        )
    
    def load_pipeline_from_file(self, file_path: str) -> PipelineConfig:
        """
        Load pipeline configuration from JSON file
        Args:
            file_path: Path to configuration file
        Returns:
            PipelineConfig: Loaded configuration
        """
        with open(file_path, 'r', encoding='utf-8') as f:
            config_data = json.load(f)
        
        return PipelineConfig.from_dict(config_data)