#!/usr/bin/env python3
"""
Logging Service - Track execution history and monitoring
"""

import sys
from pathlib import Path
from typing import Dict, Any, List, Optional
import json
from datetime import datetime, timedelta
import threading
import sqlite3

# Add project root to Python path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from domain.pipeline import PipelineRun, PipelineStatus
from domain.enums import LogLevel
from domain.chunk import Chunk
from .unified_db import UnifiedDatabase

class LoggingService:
    """
    Service for logging pipeline execution and monitoring
    """
    
    def __init__(self, db: UnifiedDatabase):
        self.db = db
        self._initialized = True  # Prevent recursion
        self._is_logging = threading.Lock()  # Prevent concurrent logging issues
    
    def log_pipeline_run(self, run: PipelineRun):
        """
        Log pipeline execution run
        Args:
            run: Pipeline run object
        """
        query = """
            INSERT OR REPLACE INTO pipeline_runs 
            (id, pipeline_id, start_time, end_time, status, processed_count, success_count, error_count, errors_json, metadata_json)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        
        params = (
            run.id,
            run.pipeline_id,
            run.start_time.isoformat() if run.start_time else None,
            run.end_time.isoformat() if run.end_time else None,
            run.status.value if hasattr(run.status, 'value') else str(run.status),
            run.processed_count,
            run.success_count,
            run.error_count,
            json.dumps(run.errors, ensure_ascii=False),
            json.dumps(run.metadata, ensure_ascii=False)
        )
        
        # Execute without triggering additional logging to prevent recursion
        self.db.execute_update(query, params)
    
    def log_message(self, level: LogLevel, message: str, pipeline_id: Optional[str] = None, 
                   pipeline_run_id: Optional[str] = None, document_path: Optional[str] = None,
                   extra_data: Optional[Dict[str, Any]] = None):
        """
        Log general message
        Args:
            level: Log level (LogLevel enum)
            message: Log message
            pipeline_id: Associated pipeline ID
            pipeline_run_id: Associated run ID
            document_path: Associated document path
            extra_data Additional context data
        """
        query = """
            INSERT INTO logs (level, message, pipeline_id, pipeline_run_id, document_path, extra_data_json, logged_at)
            VALUES (?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
        """
        
        params = (
            level.value,  # Use enum value
            message,
            pipeline_id,
            pipeline_run_id,
            document_path,
            json.dumps(extra_data or {}, ensure_ascii=False)
        )
        
        # Execute without triggering additional logging to prevent recursion
        self.db.execute_update(query, params)
    
    def get_run_history(self, pipeline_id: Optional[str] = None, limit: int = 50) -> List[Dict[str, Any]]:
        """
        Get execution history for pipeline
        Args:
            pipeline_id: Pipeline identifier (None for all pipelines)
            limit: Maximum number of runs to return
        Returns:
            List of run records
        """
        if pipeline_id:
            query = """
                SELECT * FROM pipeline_runs 
                WHERE pipeline_id = ? 
                ORDER BY start_time DESC 
                LIMIT ?
            """
            params = (pipeline_id, limit)
        else:
            query = """
                SELECT * FROM pipeline_runs 
                ORDER BY start_time DESC 
                LIMIT ?
            """
            params = (limit,)
        
        # Execute query directly without additional logging to prevent recursion
        runs = self.db.execute_query(query, params)
        
        # Convert datetime strings back to datetime objects for some fields
        for run in runs:
            if run.get("start_time"):
                try:
                    run["start_time"] = datetime.fromisoformat(run["start_time"])
                except ValueError:
                    pass  # Keep as string if conversion fails
            
            if run.get("end_time"):
                try:
                    run["end_time"] = datetime.fromisoformat(run["end_time"])
                except ValueError:
                    pass  # Keep as string if conversion fails
            
            if run.get("errors_json"):
                try:
                    run["errors"] = json.loads(run["errors_json"])
                except (json.JSONDecodeError, TypeError):
                    run["errors"] = []
            
            if run.get("metadata_json"):
                try:
                    run["metadata"] = json.loads(run["metadata_json"])
                except (json.JSONDecodeError, TypeError):
                    run["metadata"] = {}
        
        return runs
    
    def get_run_details(self, run_id: str) -> Optional[Dict[str, Any]]:
        """
        Get details for specific run
        Args:
            run_id: Run identifier
        Returns:
            Dict with run details or None if not found
        """
        query = "SELECT * FROM pipeline_runs WHERE id = ?"
        results = self.db.execute_query(query, (run_id,))
        
        if results:
            run = results[0]
            
            if run.get("start_time"):
                try:
                    run["start_time"] = datetime.fromisoformat(run["start_time"])
                except ValueError:
                    pass
            
            if run.get("end_time"):
                try:
                    run["end_time"] = datetime.fromisoformat(run["end_time"])
                except ValueError:
                    pass
            
            if run.get("errors_json"):
                try:
                    run["errors"] = json.loads(run["errors_json"])
                except (json.JSONDecodeError, TypeError):
                    run["errors"] = []
            
            if run.get("metadata_json"):
                try:
                    run["metadata"] = json.loads(run["metadata_json"])
                except (json.JSONDecodeError, TypeError):
                    run["metadata"] = {}
            
            return run
        
        return None
    
    def get_error_statistics(self, pipeline_id: Optional[str] = None, 
                           days_back: int = 7) -> Dict[str, Any]:
        """
        Get error statistics for pipeline
        Args:
            pipeline_id: Pipeline identifier (None for all pipelines)
            days_back: Number of days to look back
        Returns:
            Dict with error statistics
        """
        cutoff_date = (datetime.now() - timedelta(days=days_back)).isoformat()
        
        if pipeline_id:
            query = """
                SELECT status, COUNT(*) as count 
                FROM pipeline_runs 
                WHERE pipeline_id = ? AND start_time >= ? 
                GROUP BY status
            """
            params = (pipeline_id, cutoff_date)
        else:
            query = """
                SELECT status, COUNT(*) as count 
                FROM pipeline_runs 
                WHERE start_time >= ? 
                GROUP BY status
            """
            params = (cutoff_date,)
        
        results = self.db.execute_query(query, params)
        
        stats = {"completed": 0, "failed": 0, "running": 0, "pending": 0}
        for row in results:
            stats[row["status"].lower()] = row["count"]
        
        return stats
    
    def get_log_messages(self, pipeline_id: Optional[str] = None, 
                        level: Optional[LogLevel] = None, 
                        limit: int = 100) -> List[Dict[str, Any]]:
        """
        Get log messages with optional filtering
        Args:
            pipeline_id: Filter by pipeline ID
            level: Filter by log level
            limit: Maximum number of messages
        Returns:
            List of log messages
        """
        conditions = []
        params = []
        
        if pipeline_id:
            conditions.append("pipeline_id = ?")
            params.append(pipeline_id)
        
        if level:
            conditions.append("level = ?")
            params.append(level.value)
        
        where_clause = f"WHERE {' AND '.join(conditions)}" if conditions else ""
        query = f"""
            SELECT * FROM logs 
            {where_clause}
            ORDER BY logged_at DESC 
            LIMIT ?
        """
        params.append(limit)
        
        return self.db.execute_query(query, params)
    
    def cleanup_old_logs(self, days_to_keep: int = 30) -> int:
        """
        Clean up old log entries
        Args:
            days_to_keep: Keep logs for this many days
        Returns:
            int: Number of deleted records
        """
        cutoff_date = (datetime.now() - timedelta(days=days_to_keep)).isoformat()
        
        query = "DELETE FROM logs WHERE logged_at < ?"
        return self.db.execute_update(query, (cutoff_date,))
    
    def export_logs_to_file(self, pipeline_id: str, output_path: str, 
                           days_back: int = 7) -> bool:
        """
        Export logs to external file
        Args:
            pipeline_id: Pipeline identifier
            output_path: Output file path
            days_back: Days to export
        Returns:
            bool: True if export successful
        """
        cutoff_date = (datetime.now() - timedelta(days=days_back)).isoformat()
        
        query = """
            SELECT * FROM pipeline_runs 
            WHERE pipeline_id = ? AND start_time >= ?
            ORDER BY start_time DESC
        """
        
        runs = self.db.execute_query(query, (pipeline_id, cutoff_date))
        
        try:
            with open(output_path, 'w', encoding='utf-8') as f:
                json.dump(runs, f, indent=2, ensure_ascii=False, default=str)
            return True
        except Exception:
            return False
    
    def get_connection_status(self) -> Dict[str, Any]:
        """
        Get logging service connection status
        """
        return {
            "is_connected": True,
            "db_connected": self.db.is_connected(),
            "table_counts": {
                "pipeline_runs": self._get_table_count("pipeline_runs"),
                "logs": self._get_table_count("logs")
            },
            "service_initialized": self._initialized
        }
    
    def _get_table_count(self, table_name: str) -> int:
        """
        Get count of records in table (without logging to prevent recursion)
        """
        try:
            query = f"SELECT COUNT(*) as count FROM {table_name} LIMIT 1"
            results = self.db.execute_query(query)
            return results[0]["count"] if results else 0
        except Exception:
            return 0