#!/usr/bin/env python3
"""
Document Executor - Individual document processing engine
Executes pipeline steps for single documents with context preservation
"""

import sys
from pathlib import Path
from typing import Dict, Any, List, Optional, Union
from domain.document import Document, Page, Section
from domain.pipeline import PipelineConfig, PipelineStepConfig, PipelineRun, PipelineStatus, StepType
from domain.chunk import Chunk, Metadata, ChunkType
from infrastructure.loaders.document_factory import DocumentFactory
from infrastructure.processors.metadata_propagator import MetadataPropagator
from infrastructure.security.script_sandbox import ScriptSandbox
from application.resource_monitor import ResourceMonitor
from datetime import datetime
import time
import os

class DocumentExecutor:
    """
    Executes pipeline steps for individual documents
    Maintains context and metadata propagation throughout processing
    """
    
    def __init__(self, db):
        self.db = db
        self.resource_monitor = ResourceMonitor()
        self.script_sandbox = ScriptSandbox(timeout=60, memory_limit_mb=200)
        self.metadata_propagator = MetadataPropagator()
    
    def execute_document(self, pipeline_config: PipelineConfig, document_path: str) -> bool:
        """
        Execute pipeline for single document
        Args:
            pipeline_config: Pipeline configuration
            document_path: Path to document to process
        Returns:
            bool: True if processing completed successfully
        """
        if not os.path.exists(document_path):
            raise ValueError(f"Document path does not exist: {document_path}")
        
        # Create pipeline run record
        run = PipelineRun(
            id=f"run_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{os.path.basename(document_path)}",
            pipeline_id=pipeline_config.id,
            start_time=datetime.now(),
            status=PipelineStatus.RUNNING,
            document_paths=[document_path],
            metadata={"source_document": document_path}
        )
        
        try:
            # Load document
            loader = DocumentFactory.create_loader(document_path)
            document = loader.load({
                "path": document_path,
                "style_config_path": pipeline_config.source_config.get("style_config_path")
            })
            
            # Execute pipeline steps
            processed_chunks = self._execute_pipeline_steps(pipeline_config, document, run)
            
            # Update run status to completed
            run.end_time = datetime.now()
            run.status = PipelineStatus.COMPLETED
            run.success_count = len(processed_chunks)
            run.processed_count = len(processed_chunks)
            
            # Log successful completion
            from infrastructure.database.logging_service import LoggingService
            logging_service = LoggingService(self.db)
            logging_service.log_pipeline_run(run)
            
            return True
            
        except Exception as e:
            # Handle execution error
            run.end_time = datetime.now()
            run.status = PipelineStatus.FAILED
            run.error_count = 1
            run.errors = [{
                "timestamp": datetime.now().isoformat(),
                "error_type": type(e).__name__,
                "error_message": str(e),
                "document_path": document_path
            }]
            
            # Log failure
            from infrastructure.database.logging_service import LoggingService
            logging_service = LoggingService(self.db)
            logging_service.log_pipeline_run(run)
            
            # Attempt recovery through error recovery service (import inside function)
            from application.error_recovery import ErrorRecoveryService
            error_recovery = ErrorRecoveryService(self.db)
            error_recovery.handle_document_processing_failure(run, document_path, str(e))
            
            return False
    
    def _execute_pipeline_steps(self, config: PipelineConfig, document: Document, run: PipelineRun) -> List[Chunk]:
        """
        Execute all steps in pipeline for document
        Args:
            config: Pipeline configuration
            document: Document to process
            run: Pipeline run object for logging
        Returns:
            List[Chunk]: Processed chunks
        """
        step_results = {}
        all_chunks = []
        
        for step_config in config.steps:
            step_start_time = time.time()
            
            try:
                # Get input for this step
                input_data = self._get_step_input(step_config, step_results, document)
                
                # Execute step
                output_data = self._execute_step(step_config, input_data, document, run)
                
                # Store results
                step_results[step_config.id] = {
                    "output": output_data,
                    "execution_time": time.time() - step_start_time
                }
                
                # Log step completion
                from infrastructure.database.logging_service import LoggingService, LogLevel
                logging_service = LoggingService(self.db)
                logging_service.log_message(
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
                
                # If this is an output step (like DB exporter), collect results
                if step_config.type in [StepType.DB_EXPORTER, StepType.FILE_EXPORTER]:
                    if isinstance(output_data, list):
                        all_chunks.extend(output_data)
                    elif output_data is not None:
                        all_chunks.append(output_data)
                
            except Exception as e:
                # Log step failure
                error_msg = f"Step failed: {step_config.name} - {str(e)}"
                from infrastructure.database.logging_service import LoggingService, LogLevel
                logging_service = LoggingService(self.db)
                logging_service.log_message(
                    level=LogLevel.ERROR,
                    message=error_msg,
                    pipeline_id=run.pipeline_id,
                    pipeline_run_id=run.id,
                    extra_data={
                        "step_id": step_config.id,
                        "error_type": type(e).__name__,
                        "execution_time": time.time() - step_start_time
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
                
                # If step is critical (not optional), stop processing
                if not step_config.params.get("optional", False):
                    raise
        
        return all_chunks
    
    def _get_step_input(self, step_config: PipelineStepConfig, step_results: Dict[str, Any], 
                       document: Document):
        """
        Get input data for step based on configuration
        """
        if step_config.input_step_id:
            # Use output from previous step
            prev_result = step_results.get(step_config.input_step_id)
            if prev_result:
                return prev_result["output"]
        
        # For initial steps, input comes from document
        if step_config.type == StepType.DOCUMENT_LOADER:
            return document
        
        # For other steps without explicit input, return document or empty
        return document
    
    def _execute_step(self, step_config: PipelineStepConfig, input_data, document: Document, run: PipelineRun):
        """
        Execute individual pipeline step
        """
        if step_config.type == StepType.DOCUMENT_LOADER:
            return self._execute_document_loader_step(step_config, input_data, document, run)
        
        elif step_config.type == StepType.USER_SCRIPT:
            return self._execute_script_step(step_config, input_data, document, run)
        
        elif step_config.type == StepType.LINE_SPLITTER:
            return self._execute_line_splitter_step(step_config, input_data, document, run)
        
        elif step_config.type == StepType.DELIMITER_SPLITTER:
            return self._execute_delimiter_splitter_step(step_config, input_data, document, run)
        
        elif step_config.type == StepType.DB_EXPORTER:
            return self._execute_db_exporter_step(step_config, input_data, document, run)
        
        else:
            # Use generic processor for other step types
            from domain.interfaces import IChunkProcessor
            processor = self._get_step_processor(step_config.type)
            return processor.process(input_data, step_config.params)
    
    def _execute_document_loader_step(self, step_config: PipelineStepConfig, input_data: Document, 
                                    document: Document, run: PipelineRun) -> Document:
        """
        Execute document loader step (usually the first step)
        """
        # Input is already the loaded document
        return document
    
    def _execute_script_step(self, step_config: PipelineStepConfig, input_data, 
                           document: Document, run: PipelineRun):
        """
        Execute user script step with security sandbox
        """
        script_id = step_config.params.get("script_id")
        if not script_id:
            raise ValueError("Script step requires 'script_id' parameter")
        
        from infrastructure.database.script_manager import ScriptManager
        script_manager = ScriptManager(self.db)
        
        # Load and validate script
        script_data = script_manager.load_script(script_id)
        if not script_data:
            raise ValueError(f"Script not found: {script_id}")
        
        # Create execution context
        context = {
            "input": input_data,
            "document": document,
            "pipeline_run": run,
            "step_config": step_config,
            "metadata": {
                "document_id": document.id,
                "pipeline_id": run.pipeline_id,
                "run_id": run.id
            }
        }
        
        # Execute script in secure sandbox
        result = script_manager.validate_and_execute_script(script_id, context)
        return result
    
    def _execute_line_splitter_step(self, step_config: PipelineStepConfig, input_data, 
                                  document: Document, run: PipelineRun):
        """
        Execute line splitter step
        """
        from infrastructure.processors.line_splitter import LineSplitter
        splitter = LineSplitter()
        
        # Ensure input is in correct format
        if isinstance(input_data, Document):
            # Process document pages
            all_chunks = []
            for page in input_data.pages:
                chunks = splitter.process(page.raw_text, step_config.params)
                # Propagate metadata
                for chunk in chunks:
                    chunk.meta.document_id = document.id
                    chunk.meta.page_num = page.number
                all_chunks.extend(chunks)
            return all_chunks
        elif isinstance(input_data, list):
            # Process list of chunks
            all_chunks = []
            for item in input_data:
                if hasattr(item, 'text'):  # It's a chunk
                    chunks = splitter.process(item, step_config.params)
                    all_chunks.extend(chunks)
                else:  # It's raw text
                    chunks = splitter.process(item, step_config.params)
                    all_chunks.extend(chunks)
            return all_chunks
        else:
            # Process single item
            return splitter.process(input_data, step_config.params)
    
    def _execute_delimiter_splitter_step(self, step_config: PipelineStepConfig, input_data, 
                                       document: Document, run: PipelineRun):
        """
        Execute delimiter splitter step
        """
        from infrastructure.processors.delimiter_splitter import DelimiterSplitter
        splitter = DelimiterSplitter()
        
        # Similar logic to line splitter
        if isinstance(input_data, Document):
            all_chunks = []
            for page in input_data.pages:
                chunks = splitter.process(page.raw_text, step_config.params)
                for chunk in chunks:
                    chunk.meta.document_id = document.id
                    chunk.meta.page_num = page.number
                all_chunks.extend(chunks)
            return all_chunks
        elif isinstance(input_data, list):
            all_chunks = []
            for item in input_data:
                if hasattr(item, 'text'):
                    chunks = splitter.process(item, step_config.params)
                    all_chunks.extend(chunks)
                else:
                    chunks = splitter.process(item, step_config.params)
                    all_chunks.extend(chunks)
            return all_chunks
        else:
            return splitter.process(input_data, step_config.params)
    
    def _execute_db_exporter_step(self, step_config: PipelineStepConfig, input_data, 
                                document: Document, run: PipelineRun):
        """
        Execute database exporter step
        """
        from infrastructure.exporters.target_db_exporter import TargetDbExporter
        exporter = TargetDbExporter()
        
        try:
            # Get database configuration
            db_config = step_config.params.get("db_config", {})
            exporter.connect(db_config)
            
            # Export data
            table_name = step_config.params.get("table_name", "chunks")
            if isinstance(input_data, list):
                exporter.batch_insert(input_data, table_name)
            else:
                exporter.batch_insert([input_data], table_name)
            
            # Close connection
            exporter.close()
            
        except Exception as e:
            raise RuntimeError(f"Database export failed: {str(e)}")
        
        # Return original data (exporter doesn't transform)
        return input_data
    
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
    
    def validate_document_compatibility(self, pipeline_config: PipelineConfig, document_path: str) -> List[str]:
        """
        Validate that document is compatible with pipeline configuration
        Args:
            pipeline_config: Pipeline configuration
            document_path: Document path to validate
        Returns:
            List of compatibility errors (empty if compatible)
        """
        errors = []
        
        # Check document format compatibility
        doc_ext = Path(document_path).suffix.lower()
        supported_formats = set()
        
        for step in pipeline_config.steps:
            if step.type == StepType.DOCUMENT_LOADER:
                # Check if loader supports format
                loader = DocumentFactory.create_loader(document_path)
                if not loader.supports_format(document_path):
                    errors.append(f"Loader doesn't support format: {doc_ext}")
        
        # Check for required parameters in steps
        for step in pipeline_config.steps:
            if step.type == StepType.USER_SCRIPT:
                script_id = step.params.get("script_id")
                if not script_id:
                    errors.append(f"Script step {step.id} requires 'script_id' parameter")
        
        return errors
    
    def execute_with_recovery(self, pipeline_config: PipelineConfig, document_path: str) -> bool:
        """
        Execute document processing with automatic error recovery
        """
        try:
            return self.execute_document(pipeline_config, document_path)
        except Exception as e:
            # Attempt recovery using error recovery service (import inside function)
            from application.error_recovery import ErrorRecoveryService
            error_recovery = ErrorRecoveryService(self.db)
            recovery_result = error_recovery.attempt_document_recovery(
                pipeline_config, document_path, str(e)
            )
            
            if recovery_result["success"]:
                # Retry with recovered state
                return self.execute_document(pipeline_config, document_path)
            else:
                raise
    
    def get_processing_statistics(self, pipeline_run_id: str) -> Dict[str, Any]:
        """
        Get detailed statistics for pipeline run
        """
        from infrastructure.database.logging_service import LoggingService
        logging_service = LoggingService(self.db)
        
        # Get run details
        run_details = logging_service.get_run_details(pipeline_run_id)
        
        # Get step-level statistics
        step_stats = logging_service.get_step_statistics(pipeline_run_id)
        
        return {
            "run_details": run_details,
            "step_statistics": step_stats,
            "resource_usage": self.resource_monitor.get_current_usage(),
            "processing_efficiency": self._calculate_efficiency(run_details)
        }
    
    def _calculate_efficiency(self, run_details: Dict[str, Any]) -> float:
        """
        Calculate processing efficiency metric
        """
        if not run_details.get("start_time") or not run_details.get("end_time"):
            return 0.0
        
        start_time = datetime.fromisoformat(run_details["start_time"])
        end_time = datetime.fromisoformat(run_details["end_time"])
        duration = (end_time - start_time).total_seconds()
        
        processed_count = run_details.get("success_count", 0)
        
        if duration > 0:
            return processed_count / duration  # Items per second
        else:
            return 0.0