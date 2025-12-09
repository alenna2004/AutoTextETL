#!/usr/bin/env python3
"""
Task Dispatcher - Parallel document processing coordination
"""

import sys
from pathlib import Path
from typing import Dict, Any, List, Optional, Callable
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor, as_completed, Future
from multiprocessing import cpu_count
import threading
from domain.document import Document
from domain.pipeline import PipelineConfig, PipelineRun
from infrastructure.loaders.document_factory import DocumentFactory
from application.resource_monitor import ResourceMonitor
from application.error_recovery import ErrorRecoveryService
from datetime import datetime
import queue
import time

class TaskDispatcher:
    """
    Dispatches document processing tasks with parallel execution
    """
    
    def __init__(self, db):
        self.db = db
        self.resource_monitor = ResourceMonitor()
        self.error_recovery = ErrorRecoveryService(db)
        
        # Configuration
        self.max_workers = min(cpu_count(), 8)  # Cap at 8 workers
        self.max_memory_percentage = 80  # Use max 80% of available memory
        self.timeout_seconds = 300  # 5 minutes per document
        
        # Task queues and tracking
        self.active_tasks: Dict[str, Future] = {}
        self.task_queue = queue.Queue()
        self.dispatcher_lock = threading.Lock()
    
    def process_documents_parallel(self, pipeline_config: PipelineConfig, 
                                 document_paths: List[str], 
                                 max_workers: Optional[int] = None) -> Dict[str, Any]:
        """
        Process documents in parallel using thread pool
        Args:
            pipeline_config: Pipeline configuration
            document_paths: List of document paths to process
            max_workers: Maximum number of parallel workers (defaults to CPU count)
        Returns:
            Dict with processing results and statistics
        """
        max_workers = max_workers or min(self.max_workers, len(document_paths))
        
        # Monitor resource usage
        initial_resources = self.resource_monitor.get_current_usage()
        
        results = {
            "processed_count": 0,
            "success_count": 0,
            "error_count": 0,
            "errors": [],
            "processing_times": {},
            "memory_usage_peak": 0,
            "cpu_usage_avg": 0
        }
        
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Submit tasks
            future_to_path = {}
            for doc_path in document_paths:
                # Import DocumentExecutor inside the loop to avoid circular import
                from application.document_executor import DocumentExecutor
                executor_instance = DocumentExecutor(self.db)
                
                future = executor.submit(
                    self._process_single_document,
                    executor_instance,
                    pipeline_config,
                    doc_path
                )
                future_to_path[future] = doc_path
            
            # Collect results
            for future in as_completed(future_to_path, timeout=self.timeout_seconds):
                doc_path = future_to_path[future]
                
                try:
                    result = future.result(timeout=60)  # 1 minute timeout per result
                    if result["success"]:
                        results["success_count"] += 1
                        results["processing_times"][doc_path] = result["processing_time"]
                    else:
                        results["error_count"] += 1
                        results["errors"].append({
                            "document_path": doc_path,
                            "error": result["error"],
                            "timestamp": datetime.now().isoformat()
                        })
                
                except Exception as e:
                    results["error_count"] += 1
                    results["errors"].append({
                        "document_path": doc_path,
                        "error": str(e),
                        "error_type": type(e).__name__,
                        "timestamp": datetime.now().isoformat()
                    })
                
                results["processed_count"] += 1
        
        # Calculate final resource usage
        final_resources = self.resource_monitor.get_current_usage()
        results["memory_usage_peak"] = max(
            initial_resources["memory_percent"], 
            final_resources["memory_percent"]
        )
        results["cpu_usage_avg"] = (initial_resources["cpu_percent"] + final_resources["cpu_percent"]) / 2
        
        return results
    
    def process_documents_sequentially(self, pipeline_config: PipelineConfig, 
                                     document_paths: List[str]) -> Dict[str, Any]:
        """
        Process documents sequentially (single-threaded)
        Args:
            pipeline_config: Pipeline configuration
            document_paths: List of document paths to process
        Returns:
            Dict with processing results
        """
        results = {
            "processed_count": 0,
            "success_count": 0,
            "error_count": 0,
            "errors": [],
            "processing_times": {}
        }
        
        for doc_path in document_paths:
            try:
                start_time = time.time()
                
                # Import DocumentExecutor inside the loop to avoid circular import
                from application.document_executor import DocumentExecutor
                executor = DocumentExecutor(self.db)
                
                success = executor.execute_document(pipeline_config, doc_path)
                
                processing_time = time.time() - start_time
                
                if success:
                    results["success_count"] += 1
                    results["processing_times"][doc_path] = processing_time
                else:
                    results["error_count"] += 1
                    results["errors"].append({
                        "document_path": doc_path,
                        "error": "Document processing failed",
                        "timestamp": datetime.now().isoformat()
                    })
            
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
    
    def _process_single_document(self, executor, pipeline_config: PipelineConfig, 
                               document_path: str) -> Dict[str, Any]:
        """
        Process single document with error handling and timing
        """
        try:
            start_time = time.time()
            
            # Check resource availability before processing
            if not self._check_resource_availability():
                raise RuntimeError("Insufficient system resources for document processing")
            
            # Execute document processing using the passed executor
            success = executor.execute_document(pipeline_config, document_path)
            
            processing_time = time.time() - start_time
            
            return {
                "success": success,
                "processing_time": processing_time,
                "document_path": document_path
            }
        
        except Exception as e:
            return {
                "success": False,
                "error": str(e),
                "error_type": type(e).__name__,
                "processing_time": time.time() - start_time if 'start_time' in locals() else 0,
                "document_path": document_path
            }
    
    def _check_resource_availability(self) -> bool:
        """
        Check if system has sufficient resources for processing
        """
        resources = self.resource_monitor.get_current_usage()
        
        # Check memory usage
        if resources["memory_percent"] > self.max_memory_percentage:
            return False
        
        # Check CPU load (optional threshold)
        if resources["cpu_percent"] > 90:  # If CPU is very high
            return False  # Wait for CPU to cool down
        
        return True
    
    def adaptive_processing(self, pipeline_config: PipelineConfig, 
                          document_paths: List[str]) -> Dict[str, Any]:
        """
        Adaptive processing that adjusts parallelism based on system resources
        """
        # Start with conservative parallelism
        initial_workers = min(2, len(document_paths))
        
        # Monitor system during processing and adjust
        results = {
            "processed_count": 0,
            "success_count": 0,
            "error_count": 0,
            "errors": [],
            "processing_times": {},
            "workers_used": initial_workers,
            "adaptive_adjustments": []
        }
        
        # Process in batches, adjusting workers based on resource usage
        batch_size = max(1, len(document_paths) // 4)  # Process in 4 batches
        current_workers = initial_workers
        
        for i in range(0, len(document_paths), batch_size):
            batch_paths = document_paths[i:i + batch_size]
            
            # Adjust workers based on current resource usage
            resources = self.resource_monitor.get_current_usage()
            if resources["memory_percent"] > 70 or resources["cpu_percent"] > 80:
                current_workers = max(1, current_workers - 1)  # Reduce workers
                results["adaptive_adjustments"].append({
                    "timestamp": datetime.now().isoformat(),
                    "action": "reduce_workers",
                    "from": current_workers + 1,
                    "to": current_workers
                })
            elif resources["memory_percent"] < 50 and resources["cpu_percent"] < 60:
                current_workers = min(self.max_workers, current_workers + 1)  # Increase workers
                results["adaptive_adjustments"].append({
                    "timestamp": datetime.now().isoformat(),
                    "action": "increase_workers",
                    "from": current_workers - 1,
                    "to": current_workers
                })
            
            # Process batch with current worker count
            batch_results = self.process_documents_parallel(
                pipeline_config, batch_paths, max_workers=current_workers
            )
            
            # Aggregate results
            results["processed_count"] += batch_results["processed_count"]
            results["success_count"] += batch_results["success_count"]
            results["error_count"] += batch_results["error_count"]
            results["errors"].extend(batch_results["errors"])
            results["processing_times"].update(batch_results["processing_times"])
        
        return results
    
    def process_with_priority(self, pipeline_config: PipelineConfig,
                            priority_documents: List[str],
                            normal_documents: List[str]) -> Dict[str, Any]:
        """
        Process documents with priority - process priority docs first
        Args:
            pipeline_config: Pipeline configuration
            priority_documents: High-priority documents to process first
            normal_documents: Normal-priority documents to process after
        Returns:
            Dict with combined processing results
        """
        results = {
            "priority_results": {},
            "normal_results": {},
            "total_processed": 0,
            "total_success": 0,
            "total_errors": 0,
            "all_errors": []
        }
        
        # Process priority documents first
        if priority_documents:
            results["priority_results"] = self.process_documents_parallel(
                pipeline_config, priority_documents, max_workers=min(4, self.max_workers)
            )
        
        # Process normal documents second
        if normal_documents:
            results["normal_results"] = self.process_documents_parallel(
                pipeline_config, normal_documents, max_workers=self.max_workers
            )
        
        # Aggregate results
        for category in ["priority_results", "normal_results"]:
            if results[category]:
                cat_results = results[category]
                results["total_processed"] += cat_results["processed_count"]
                results["total_success"] += cat_results["success_count"]
                results["total_errors"] += cat_results["error_count"]
                results["all_errors"].extend(cat_results["errors"])
        
        return results
    
    def get_active_tasks_status(self) -> Dict[str, Any]:
        """
        Get status of currently active tasks
        """
        with self.dispatcher_lock:
            active_tasks = {}
            for task_id, future in self.active_tasks.items():
                active_tasks[task_id] = {
                    "done": future.done(),
                    "cancelled": future.cancelled(),
                    "running": not future.done() and not future.cancelled()
                }
        
        return {
            "active_task_count": len(active_tasks),
            "tasks": active_tasks,
            "max_workers": self.max_workers,
            "resource_usage": self.resource_monitor.get_current_usage()
        }
    
    def cancel_all_tasks(self):
        """
        Cancel all currently active tasks
        """
        with self.dispatcher_lock:
            cancelled_count = 0
            for task_id, future in list(self.active_tasks.items()):
                if not future.done():
                    future.cancel()
                    del self.active_tasks[task_id]
                    cancelled_count += 1
        
        return cancelled_count
    
    def set_max_workers(self, max_workers: int):
        """
        Set maximum number of parallel workers
        """
        if max_workers < 1:
            raise ValueError("Max workers must be at least 1")
        
        self.max_workers = max_workers
    
    def set_memory_limit_percentage(self, percentage: int):
        """
        Set maximum memory usage percentage
        """
        if not 1 <= percentage <= 100:
            raise ValueError("Memory limit percentage must be between 1 and 100")
        
        self.max_memory_percentage = percentage
    
    def set_timeout_seconds(self, seconds: int):
        """
        Set timeout for document processing
        """
        if seconds < 1:
            raise ValueError("Timeout must be at least 1 second")
        
        self.timeout_seconds = seconds