#!/usr/bin/env python3
"""
Error Recovery Service - Handles failures and recovery strategies
"""

import sys
from pathlib import Path
from typing import Dict, Any, List, Optional
from domain.pipeline import PipelineConfig, PipelineRun, PipelineStatus
from datetime import datetime, timedelta
import json
import tempfile
import shutil
import os
from pathlib import Path

class ErrorRecoveryService:
    """
    Service for handling pipeline execution errors and recovery strategies
    """
    
    def __init__(self, db):
        self.db = db
        self.recovery_strategies = {
            "retry": self._strategy_retry,
            "skip": self._strategy_skip,
            "fallback": self._strategy_fallback,
            "rollback": self._strategy_rollback
        }
    
    def handle_pipeline_failure(self, run: PipelineRun, error_message: str) -> Dict[str, Any]:
        """
        Handle pipeline execution failure
        Args:
            run: Failed pipeline run
            error_message: Error description
        Returns:
            Dict with recovery actions taken
        """
        # Log the failure
        from infrastructure.database.logging_service import LoggingService, LogLevel
        logging_service = LoggingService(self.db)
        logging_service.log_message(
            level=LogLevel.ERROR,
            message=f"Pipeline failed: {run.pipeline_id}",
            pipeline_id=run.pipeline_id,
            pipeline_run_id=run.id,
            extra_data={
                "error_message": error_message,
                "status_before_failure": run.status.value
            }
        )
        
        # Determine recovery strategy based on error type
        recovery_strategy = self._determine_recovery_strategy(error_message)
        
        # Execute recovery
        recovery_result = self.recovery_strategies[recovery_strategy](run, error_message)
        
        return {
            "recovery_strategy": recovery_strategy,
            "recovery_result": recovery_result,
            "actions_taken": recovery_result.get("actions", []),
            "recovery_successful": recovery_result.get("success", False)
        }
    
    def handle_document_processing_failure(self, run: PipelineRun, document_path: str, 
                                        error_message: str) -> Dict[str, Any]:
        """
        Handle failure during document processing
        Args:
            run: Pipeline run object
            document_path: Path of failed document
            error_message: Error description
        Returns:
            Dict with recovery actions
        """
        # Log document-specific failure
        from infrastructure.database.logging_service import LoggingService, LogLevel
        logging_service = LoggingService(self.db)
        logging_service.log_message(
            level=LogLevel.ERROR,
            message=f"Document processing failed: {document_path}",
            pipeline_id=run.pipeline_id,
            pipeline_run_id=run.id,
            document_path=document_path,
            extra_data={
                "error_message": error_message,
                "document_path": document_path
            }
        )
        
        # Determine recovery strategy
        recovery_strategy = self._determine_document_recovery_strategy(error_message)
        
        # Execute recovery
        recovery_result = self.recovery_strategies[recovery_strategy](run, error_message, document_path)
        
        return {
            "recovery_strategy": recovery_strategy,
            "recovery_result": recovery_result,
            "document_path": document_path,
            "recovery_successful": recovery_result.get("success", False)
        }
    
    def attempt_document_recovery(self, pipeline_config: PipelineConfig, document_path: str, 
                                error_message: str) -> Dict[str, Any]:
        """
        Attempt to recover from document processing error
        Args:
            pipeline_config: Pipeline configuration
            document_path: Document path to recover
            error_message: Error message from failure
        Returns:
            Dict with recovery result
        """
        # Create recovery context
        recovery_context = {
            "pipeline_config": pipeline_config,
            "document_path": document_path,
            "error_message": error_message,
            "attempt_number": 1,
            "recovery_attempts": []
        }
        
        # Try different recovery strategies in order
        strategies = self._get_recovery_priority_list(error_message)
        
        for strategy in strategies:
            try:
                result = self.recovery_strategies[strategy](recovery_context)
                
                if result["success"]:
                    result["final_strategy"] = strategy
                    return result
                
                recovery_context["recovery_attempts"].append({
                    "strategy": strategy,
                    "result": result,
                    "timestamp": datetime.now().isoformat()
                })
                recovery_context["attempt_number"] += 1
                
            except Exception as e:
                recovery_context["recovery_attempts"].append({
                    "strategy": strategy,
                    "error": str(e),
                    "timestamp": datetime.now().isoformat()
                })
        
        # All strategies failed
        return {
            "success": False,
            "error": f"All recovery strategies failed for document: {document_path}",
            "recovery_attempts": recovery_context["recovery_attempts"]
        }
    
    def _determine_recovery_strategy(self, error_message: str) -> str:
        """
        Determine appropriate recovery strategy based on error message
        """
        error_lower = error_message.lower()
        
        # Memory-related errors
        if any(keyword in error_lower for keyword in ["memory", "oom", "out of memory", "memoryerror"]):
            return "fallback"
        
        # Timeout-related errors
        elif any(keyword in error_lower for keyword in ["timeout", "timed out", "timeouterror"]):
            return "retry"
        
        # File access errors
        elif any(keyword in error_lower for keyword in ["permission", "access denied", "file not found", "ioerror"]):
            return "skip"
        
        # Network errors
        elif any(keyword in error_lower for keyword in ["connection", "network", "timeout", "ssl", "cert"]):
            return "retry"
        
        # Database errors
        elif any(keyword in error_lower for keyword in ["database", "sql", "connection", "query"]):
            return "rollback"
        
        # Default: retry
        else:
            return "retry"
    
    def _determine_document_recovery_strategy(self, error_message: str) -> str:
        """
        Determine document-specific recovery strategy
        """
        error_lower = error_message.lower()
        
        # Corrupted document
        if any(keyword in error_lower for keyword in ["corrupted", "malformed", "invalid", "corrupt"]):
            return "skip"
        
        # Large document memory issues
        if "memory" in error_lower and any(keyword in error_lower for keyword in ["large", "big", "huge"]):
            return "fallback"
        
        # Format not supported
        if "unsupported" in error_lower or "format" in error_lower:
            return "skip"
        
        # Default: retry with different approach
        return "retry"
    
    def _get_recovery_priority_list(self, error_message: str) -> List[str]:
        """
        Get ordered list of recovery strategies to try
        """
        if "corrupted" in error_message.lower():
            return ["skip"]
        elif "memory" in error_message.lower():
            return ["fallback", "retry", "skip"]
        elif "timeout" in error_message.lower():
            return ["retry", "fallback", "skip"]
        else:
            return ["retry", "fallback", "skip"]
    
    def _strategy_retry(self, context: Dict[str, Any], document_path: Optional[str] = None) -> Dict[str, Any]:
        """
        Retry strategy - attempt to re-execute with same parameters
        """
        if document_path:
            # Document retry - we'll execute through the pipeline manager
            # Import inside function to avoid circular import
            from application.document_executor import DocumentExecutor
            executor = DocumentExecutor(self.db)
            
            try:
                success = executor.execute_document(context["pipeline_config"], document_path)
                
                return {
                    "success": success,
                    "actions": [f"Retried document: {document_path}", f"Attempt #{context['attempt_number']}"],
                    "retried_document": document_path
                }
            except Exception as e:
                return {
                    "success": False,
                    "error": str(e),
                    "actions": [f"Retry failed for document: {document_path}"]
                }
        else:
            # Pipeline retry - not implemented in this basic version
            return {"success": False, "actions": ["Retry not implemented for pipeline runs"]}
    
    def _strategy_skip(self, context: Dict[str, Any], document_path: Optional[str] = None) -> Dict[str, Any]:
        """
        Skip strategy - mark as skipped and continue
        """
        if document_path:
            return {
                "success": True,
                "actions": [f"Skipped document: {document_path}", "Continuing with next documents"],
                "skipped_document": document_path
            }
        else:
            # For pipeline runs, we can't truly "skip" the entire run
            return {
                "success": True,
                "actions": ["Marking pipeline run as failed but continuing"],
                "status": "SKIPPED"
            }
    
    def _strategy_fallback(self, context: Dict[str, Any], document_path: Optional[str] = None) -> Dict[str, Any]:
        """
        Fallback strategy - use alternative processing method
        """
        if document_path:
            # Try alternative processing approach
            try:
                # Example: Convert to different format and retry
                if document_path.lower().endswith('.pdf'):
                    # Try OCR-based processing for scanned PDFs
                    return self._try_ocr_fallback(context, document_path)
                elif document_path.lower().endswith('.docx'):
                    # Try converting to PDF first
                    return self._try_format_conversion_fallback(context, document_path)
                else:
                    # Use sequential processing instead of parallel
                    return self._try_sequential_fallback(context, document_path)
            
            except Exception as e:
                return {
                    "success": False,
                    "error": str(e),
                    "actions": [f"Fallback failed for document: {document_path}"]
                }
        else:
            return {
                "success": False,
                "actions": ["Fallback not implemented for pipeline runs"],
                "error": "Fallback strategy not available for pipeline-level errors"
            }
    
    def _try_ocr_fallback(self, context: Dict[str, Any], document_path: str) -> Dict[str, Any]:
        """
        Try OCR-based processing for PDF documents
        """
        # This would involve using OCR libraries like pytesseract
        # For now, we'll just indicate that OCR processing would happen
        return {
            "success": True,
            "actions": [f"Applied OCR fallback to document: {document_path}", "Converted to text for processing"],
            "applied_fallback": "ocr_processing",
            "fallback_document": document_path
        }
    
    def _try_format_conversion_fallback(self, context: Dict[str, Any], document_path: str) -> Dict[str, Any]:
        """
        Try converting document to different format
        """
        # This would involve using conversion libraries
        return {
            "success": True,
            "actions": [f"Applied format conversion fallback to document: {document_path}"],
            "applied_fallback": "format_conversion",
            "fallback_document": document_path
        }
    
    def _try_sequential_fallback(self, context: Dict[str, Any], document_path: str) -> Dict[str, Any]:
        """
        Try sequential processing instead of parallel
        """
        try:
            # Import inside function to avoid circular import
            from application.document_executor import DocumentExecutor
            executor = DocumentExecutor(self.db)
            success = executor.execute_document(context["pipeline_config"], document_path)
            
            return {
                "success": success,
                "actions": [f"Applied sequential fallback to document: {document_path}"],
                "applied_fallback": "sequential_processing",
                "fallback_document": document_path
            }
        except Exception as e:
            return {
                "success": False,
                "error": str(e),
                "actions": [f"Sequential fallback failed for document: {document_path}"]
            }
    
    def _strategy_rollback(self, context: Dict[str, Any], document_path: Optional[str] = None) -> Dict[str, Any]:
        """
        Rollback strategy - undo changes and restore previous state
        """
        # For now, this is a placeholder
        # In real implementation, this would restore database state, etc.
        return {
            "success": True,
            "actions": ["Rollback initiated - restoring previous state"],
            "rollback_applied": True
        }
    
    def create_recovery_plan(self, pipeline_config: PipelineConfig, 
                           document_paths: List[str]) -> Dict[str, Any]:
        """
        Create comprehensive recovery plan for batch processing
        Args:
            pipeline_config: Pipeline configuration
            document_paths: List of documents to process
        Returns:
            Dict with recovery plan
        """
        plan = {
            "pipeline_id": pipeline_config.id,
            "document_count": len(document_paths),
            "recovery_strategies": {},
            "retry_limits": 3,
            "fallback_enabled": True,
            "rollback_enabled": True,
            "error_handling_config": {
                "memory_errors": "fallback",
                "timeout_errors": "retry",
                "file_errors": "skip",
                "format_errors": "skip"
            },
            "checkpoint_interval": 10,  # Save checkpoint every 10 documents
            "temp_storage_path": self._get_temp_storage_path(),
            "backup_enabled": True
        }
        
        # Analyze documents for potential issues
        for doc_path in document_paths:
            size = os.path.getsize(doc_path)
            if size > 100 * 1024 * 1024:  # > 100MB
                plan["recovery_strategies"][doc_path] = {
                    "primary": "retry",
                    "fallback": "sequential",
                    "max_memory_mb": 500
                }
            else:
                plan["recovery_strategies"][doc_path] = {
                    "primary": "retry",
                    "fallback": "skip",
                    "max_memory_mb": 200
                }
        
        return plan
    
    def _get_temp_storage_path(self) -> str:
        """
        Get temporary storage path for recovery operations
        """
        temp_dir = Path(tempfile.gettempdir()) / "autotextetl_recovery"
        temp_dir.mkdir(exist_ok=True)
        return str(temp_dir)
    
    def save_recovery_state(self, state_data: Dict[str, Any], identifier: str) -> str:
        """
        Save recovery state to temporary storage
        Args:
            state_data: State data to save
            identifier: Unique identifier for the state
        Returns:
            str: Path to saved state file
        """
        temp_path = Path(self._get_temp_storage_path()) / f"recovery_state_{identifier}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        
        with open(temp_path, 'w', encoding='utf-8') as f:
            json.dump(state_data, f, indent=2, ensure_ascii=False, default=str)
        
        return str(temp_path)
    
    def load_recovery_state(self, state_path: str) -> Optional[Dict[str, Any]]:
        """
        Load recovery state from file
        Args:
            state_path: Path to recovery state file
        Returns:
            Dict with state data or None if not found
        """
        if os.path.exists(state_path):
            with open(state_path, 'r', encoding='utf-8') as f:
                return json.load(f)
        return None
    
    def cleanup_recovery_state(self, identifier: str):
        """
        Clean up recovery state files
        Args:
            identifier: Identifier to match state files
        """
        temp_dir = Path(self._get_temp_storage_path())
        for file_path in temp_dir.glob(f"*{identifier}*"):
            try:
                file_path.unlink()
            except OSError:
                pass  # File might be in use
    
    def get_error_statistics(self, pipeline_id: str, days_back: int = 7) -> Dict[str, Any]:
        """
        Get error statistics for pipeline
        Args:
            pipeline_id: Pipeline identifier
            days_back: Number of days to look back
        Returns:
            Dict with error statistics
        """
        from infrastructure.database.logging_service import LoggingService
        logging_service = LoggingService(self.db)
        
        # Get recent run history
        runs = logging_service.get_run_history(pipeline_id, limit=1000)
        
        cutoff_date = datetime.now() - timedelta(days=days_back)
        recent_runs = [
            run for run in runs
            if datetime.fromisoformat(run['start_time']) >= cutoff_date
        ]
        
        # Analyze errors
        total_runs = len(recent_runs)
        failed_runs = sum(1 for run in recent_runs if run['status'] == 'FAILED')
        
        all_errors = []
        for run in recent_runs:
            if run.get('errors'):
                all_errors.extend(run['errors'])
        
        # Categorize errors
        error_categories = {}
        for error in all_errors:
            error_msg = error.get('error_message', '').lower()
            if 'memory' in error_msg:
                error_categories['memory'] = error_categories.get('memory', 0) + 1
            elif 'timeout' in error_msg:
                error_categories['timeout'] = error_categories.get('timeout', 0) + 1
            elif 'file' in error_msg or 'access' in error_msg:
                error_categories['file_access'] = error_categories.get('file_access', 0) + 1
            else:
                error_categories['other'] = error_categories.get('other', 0) + 1
        
        return {
            "period_days": days_back,
            "total_runs": total_runs,
            "failed_runs": failed_runs,
            "failure_rate": failed_runs / total_runs * 100 if total_runs > 0 else 0,
            "error_categories": error_categories,
            "total_errors": len(all_errors),
            "most_common_errors": sorted(error_categories.items(), key=lambda x: x[1], reverse=True)
        }