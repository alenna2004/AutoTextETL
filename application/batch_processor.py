#!/usr/bin/env python3
"""
Batch Processor - Orchestrates large-scale document processing
Handles file discovery, scheduling, and result aggregation
"""

import sys
from pathlib import Path
from typing import Dict, Any, List, Optional, Generator
from domain.pipeline import PipelineConfig
from application.task_dispatcher import TaskDispatcher
from application.scheduler_service import SchedulerService
from application.error_recovery import ErrorRecoveryService
from application.resource_monitor import ResourceMonitor
from infrastructure.database.logging_service import LoggingService, LogLevel
from application.document_executor import DocumentExecutor
from datetime import datetime, timedelta
import os
import glob
import fnmatch
import shutil
from pathlib import Path
import json

class BatchProcessor:
    """
    Batch document processor for large-scale operations
    """
    
    def __init__(self, db):
        self.db = db
        self.task_dispatcher = TaskDispatcher(db)
        self.scheduler_service = SchedulerService(db)
        self.error_recovery = ErrorRecoveryService(db)
        self.resource_monitor = ResourceMonitor()
    
    def discover_documents(self, source_path: str, patterns: Optional[List[str]] = None) -> List[str]:
        """
        Discover documents in source path based on patterns
        Args:
            source_path: Source directory or file path
            patterns: List of file patterns (e.g., ['*.pdf', '*.docx'])
        Returns:
            List of document file paths
        """
        if patterns is None:
            patterns = ['*.pdf', '*.docx', '*.txt', '*.xlsx', '*.csv']
        
        source_path = Path(source_path)
        
        if source_path.is_file():
            # Single file
            if self._matches_pattern(source_path.name, patterns):
                return [str(source_path)]
            else:
                return []
        
        elif source_path.is_dir():
            # Directory - search recursively
            documents = []
            for pattern in patterns:
                # Use glob to find files matching pattern
                pattern_path = source_path / "**" / pattern
                matches = glob.glob(str(pattern_path), recursive=True)
                documents.extend(matches)
            
            # Filter out non-files and sort
            documents = [f for f in documents if os.path.isfile(f)]
            documents.sort()
            return documents
        
        else:
            raise ValueError(f"Source path does not exist: {source_path}")
    
    def _matches_pattern(self, filename: str, patterns: List[str]) -> bool:
        """
        Check if filename matches any of the patterns
        """
        for pattern in patterns:
            if fnmatch.fnmatch(filename.lower(), pattern.lower()):
                return True
        return False
    
    def process_batch(self, pipeline_config: PipelineConfig, 
                     source_path: str,
                     patterns: Optional[List[str]] = None,
                     max_workers: Optional[int] = None,
                     output_dir: Optional[str] = None) -> Dict[str, Any]:
        """
        Process batch of documents
        Args:
            pipeline_config: Pipeline configuration
            source_path: Source directory or file path
            patterns: File patterns to match (defaults to common document formats)
            max_workers: Maximum parallel workers (defaults to CPU count)
            output_dir: Output directory for results
        Returns:
            Dict with processing results and statistics
        """
        # Discover documents
        document_paths = self.discover_documents(source_path, patterns)
        
        if not document_paths:
            return {
                "processed_count": 0,
                "success_count": 0,
                "error_count": 0,
                "errors": ["No matching documents found"],
                "document_paths": []
            }
        
        # Log batch start
        
        logging_service = LoggingService(self.db)
        logging_service.log_message(
            level=LogLevel.INFO,
            message=f"Starting batch processing: {len(document_paths)} documents",
            pipeline_id=pipeline_config.id,
            extra_data={
                "source_path": source_path,
                "document_count": len(document_paths),
                "patterns": patterns or ['*.pdf', '*.docx', '*.txt']
            }
        )
        
        # Process documents in parallel
        results = self.task_dispatcher.process_documents_parallel(
            pipeline_config, 
            document_paths, 
            max_workers=max_workers
        )
        
        # Save results if output directory specified
        if output_dir:
            self._save_batch_results(results, output_dir, pipeline_config.id)
        
        # Log batch completion
        logging_service.log_message(
            level=LogLevel.INFO,
            message=f"Batch processing completed: {results['success_count']}/{results['processed_count']} successful",
            pipeline_id=pipeline_config.id,
            extra_data={
                "success_count": results["success_count"],
                "error_count": results["error_count"],
                "total_processed": results["processed_count"]
            }
        )
        
        return results
    
    def process_batch_sequentially(self, pipeline_config: PipelineConfig,
                                 source_path: str,
                                 patterns: Optional[List[str]] = None) -> Dict[str, Any]:
        """
        Process batch sequentially (for large documents or memory-constrained environments)
        """
        document_paths = self.discover_documents(source_path, patterns)
        
        if not document_paths:
            return {
                "processed_count": 0,
                "success_count": 0,
                "error_count": 0,
                "errors": ["No matching documents found"],
                "document_paths": []
            }
        
        results = {
            "processed_count": 0,
            "success_count": 0,
            "error_count": 0,
            "errors": [],
            "processing_times": {}
        }
        
        executor = DocumentExecutor(self.db)
        
        for doc_path in document_paths:
            try:
                success = executor.execute_document(pipeline_config, doc_path)
                
                if success:
                    results["success_count"] += 1
                else:
                    results["error_count"] += 1
                    results["errors"].append({
                        "document_path": doc_path,
                        "error": "Processing failed",
                        "timestamp": datetime.now().isoformat()
                    })
                
                results["processed_count"] += 1
                
            except Exception as e:
                results["error_count"] += 1
                results["errors"].append({
                    "document_path": doc_path,
                    "error": str(e),
                    "error_type": type(e).__name__,
                    "timestamp": datetime.now().isoformat()
                })
                results["processed_count"] += 1
        
        return results
    
    def process_batch_with_scheduling(self, pipeline_config: PipelineConfig,
                                    source_path: str,
                                    cron_expression: str,
                                    patterns: Optional[List[str]] = None) -> str:
        """
        Schedule batch processing with cron expression
        Args:
            pipeline_config: Pipeline configuration
            source_path: Source directory or file path
            cron_expression: Cron schedule expression
            patterns: File patterns to match
        Returns:
            str: Job ID
        """
        document_paths = self.discover_documents(source_path, patterns)
        
        return self.scheduler_service.schedule_pipeline(
            pipeline_config.id,
            cron_expression,
            document_paths
        )
    
    def process_with_adaptive_scaling(self, pipeline_config: PipelineConfig,
                                    source_path: str,
                                    patterns: Optional[List[str]] = None) -> Dict[str, Any]:
        """
        Process batch with adaptive worker scaling based on system resources
        """
        document_paths = self.discover_documents(source_path, patterns)
        
        if not document_paths:
            return {
                "processed_count": 0,
                "success_count": 0,
                "error_count": 0,
                "errors": ["No matching documents found"],
                "document_paths": []
            }
        
        # Use adaptive processing from task dispatcher
        return self.task_dispatcher.adaptive_processing(
            pipeline_config, 
            document_paths
        )
    
    def process_with_priority_handling(self, pipeline_config: PipelineConfig,
                                     source_path: str,
                                     priority_patterns: Optional[List[str]] = None,
                                     normal_patterns: Optional[List[str]] = None) -> Dict[str, Any]:
        """
        Process batch with priority handling
        Args:
            pipeline_config: Pipeline configuration
            source_path: Source directory
            priority_patterns: Patterns for high-priority documents
            normal_patterns: Patterns for normal-priority documents
        Returns:
            Dict with combined results
        """
        if priority_patterns is None:
            priority_patterns = ['*.urgent.*', '*important*', '*critical*']
        if normal_patterns is None:
            normal_patterns = ['*.pdf', '*.docx', '*.txt']
        
        priority_paths = self.discover_documents(source_path, priority_patterns)
        normal_paths = self.discover_documents(source_path, normal_patterns)
        
        # Remove priority paths from normal paths to avoid duplicates
        normal_paths = [p for p in normal_paths if p not in priority_paths]
        
        return self.task_dispatcher.process_with_priority(
            pipeline_config,
            priority_paths,
            normal_paths
        )
    
    def _save_batch_results(self, results: Dict[str, Any], output_dir: str, pipeline_id: str):
        """
        Save batch processing results to output directory
        """
        output_path = Path(output_dir) / f"batch_results_{pipeline_id}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(results, f, indent=2, ensure_ascii=False, default=str)
    
    def get_batch_statistics(self, pipeline_id: str, start_date: Optional[datetime] = None, 
                           end_date: Optional[datetime] = None) -> Dict[str, Any]:
        """
        Get batch processing statistics
        Args:
            pipeline_id: Pipeline identifier
            start_date: Start date for statistics (defaults to last 7 days)
            end_date: End date for statistics (defaults to now)
        Returns:
            Dict with batch statistics
        """
        if start_date is None:
            start_date = datetime.now() - timedelta(days=7)
        if end_date is None:
            end_date = datetime.now()
        
        logging_service = LoggingService(self.db)
        
        # Get run history
        runs = logging_service.get_run_history(pipeline_id, limit=1000)
        
        # Filter by date range
        filtered_runs = [
            run for run in runs
            if start_date <= datetime.fromisoformat(run['start_time']) <= end_date
        ]
        
        # Calculate statistics
        total_runs = len(filtered_runs)
        successful_runs = sum(1 for run in filtered_runs if run['status'] == 'COMPLETED')
        failed_runs = sum(1 for run in filtered_runs if run['status'] == 'FAILED')
        
        total_processed = sum(run.get('processed_count', 0) for run in filtered_runs)
        total_success = sum(run.get('success_count', 0) for run in filtered_runs)
        total_errors = sum(run.get('error_count', 0) for run in filtered_runs)
        
        # Calculate average processing time
        processing_times = [
            (datetime.fromisoformat(run['end_time']) - datetime.fromisoformat(run['start_time'])).total_seconds()
            for run in filtered_runs if run.get('end_time')
        ]
        avg_processing_time = sum(processing_times) / len(processing_times) if processing_times else 0
        
        return {
            "period": {
                "start_date": start_date.isoformat(),
                "end_date": end_date.isoformat()
            },
            "total_runs": total_runs,
            "successful_runs": successful_runs,
            "failed_runs": failed_runs,
            "success_rate": successful_runs / total_runs * 100 if total_runs > 0 else 0,
            "total_processed": total_processed,
            "total_success": total_success,
            "total_errors": total_errors,
            "average_processing_time": avg_processing_time,
            "documents_per_second": total_processed / sum(processing_times) if processing_times else 0,
            "resource_usage": self.resource_monitor.get_average_usage(filtered_runs)
        }
    
    def archive_processed_documents(self, source_dir: str, archive_dir: str, 
                                  days_old: int = 7):
        """
        Archive processed documents older than specified days
        Args:
            source_dir: Source directory to archive from
            archive_dir: Archive destination directory
            days_old: Archive documents older than this many days
        """
        cutoff_date = datetime.now() - timedelta(days=days_old)
        
        source_path = Path(source_dir)
        archive_path = Path(archive_dir)
        archive_path.mkdir(parents=True, exist_ok=True)
        
        # Find all document files
        patterns = ['*.pdf', '*.docx', '*.txt', '*.xlsx', '*.csv']
        for pattern in patterns:
            for file_path in source_path.rglob(pattern):
                if file_path.is_file():
                    # Check modification time
                    mod_time = datetime.fromtimestamp(file_path.stat().st_mtime)
                    if mod_time < cutoff_date:
                        # Move to archive
                        relative_path = file_path.relative_to(source_path)
                        archive_file_path = archive_path / relative_path
                        archive_file_path.parent.mkdir(parents=True, exist_ok=True)
                        
                        shutil.move(str(file_path), str(archive_file_path))
    
    def cleanup_temporary_files(self, temp_dir: str, days_old: int = 1):
        """
        Clean up temporary files older than specified days
        Args:
            temp_dir: Temporary directory path
            days_old: Delete files older than this many days
        """
        cutoff_date = datetime.now() - timedelta(days=days_old)
        
        temp_path = Path(temp_dir)
        if not temp_path.exists():
            return
        
        for temp_file in temp_path.iterdir():
            if temp_file.is_file():
                mod_time = datetime.fromtimestamp(temp_file.stat().st_mtime)
                if mod_time < cutoff_date:
                    temp_file.unlink()
    
    def validate_batch_configuration(self, pipeline_config: PipelineConfig, 
                                   source_path: str,
                                   patterns: Optional[List[str]] = None) -> List[str]:
        """
        Validate batch processing configuration
        Args:
            pipeline_config: Pipeline configuration
            source_path: Source directory
            patterns: File patterns to match
        Returns:
            List of validation errors
        """
        errors = []
        
        # Check source path
        if not os.path.exists(source_path):
            errors.append(f"Source path does not exist: {source_path}")
        elif not os.path.isdir(source_path):
            errors.append(f"Source path is not a directory: {source_path}")
        
        # Check pipeline configuration
        pipeline_errors = self.pipeline_manager.validate_pipeline_config(pipeline_config)
        errors.extend(pipeline_errors)
        
        # Check if any documents match patterns
        if patterns:
            document_paths = self.discover_documents(source_path, patterns)
            if not document_paths:
                errors.append(f"No documents found matching patterns: {patterns}")
        
        return errors