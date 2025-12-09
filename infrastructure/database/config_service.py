#!/usr/bin/env python3
"""
Configuration Service - Manage pipeline configurations and settings
"""

import sys
from pathlib import Path
from typing import Dict, Any, List, Optional
import json
import os
from datetime import datetime
import secrets

# Add project root to Python path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from domain.pipeline import PipelineConfig, PipelineStepConfig, StepType
from .unified_db import UnifiedDatabase

class ConfigService:
    """
    Service for managing pipeline configurations and settings
    """
    
    def __init__(self, db: UnifiedDatabase):
        self.db = db
    
    def save_pipeline_config(self, config: PipelineConfig) -> str:
        """
        Save pipeline configuration to database
        Args:
            config: Pipeline configuration to save
        Returns:
            str: Pipeline ID
        """
        pipeline_id = config.id or f"pipeline_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{secrets.token_hex(4)}"
        
        query = """
            INSERT INTO pipelines (id, name, description, config_json, schedule, source_config, target_config, version)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """
        
        params = (
            pipeline_id,
            config.name,
            config.description,
            json.dumps(config.to_dict(), ensure_ascii=False),
            config.schedule,
            json.dumps(config.source_config, ensure_ascii=False),
            json.dumps(config.target_config, ensure_ascii=False),
            config.version
        )
        
        self.db.execute_update(query, params)
        return pipeline_id
    
    def load_pipeline_config(self, pipeline_id: str) -> Optional[PipelineConfig]:
        """
        Load pipeline configuration from database
        Args:
            pipeline_id: Pipeline identifier
        Returns:
            PipelineConfig: Loaded configuration or None if not found
        """
        query = "SELECT * FROM pipelines WHERE id = ? AND is_active = 1"
        results = self.db.execute_query(query, (pipeline_id,))
        
        if not results:
            return None
        
        row = results[0]
        
        # Parse configuration from JSON
        config_data = json.loads(row["config_json"])
        
        # Create steps from configuration
        steps = []
        for step_data in config_data.get("steps", []):
            step = PipelineStepConfig(
                id=step_data.get("id"),
                type=StepType(step_data["type"]),
                name=step_data.get("name", ""),
                params=step_data.get("params", {}),
                input_step_id=step_data.get("input_step_id"),
                depends_on=step_data.get("depends_on", [])
            )
            steps.append(step)
        
        config = PipelineConfig(
            id=row["id"],
            name=row["name"],
            description=row["description"],
            steps=steps,
            schedule=row["schedule"],
            source_config=json.loads(row["source_config"]),
            target_config=json.loads(row["target_config"]),
            version=row["version"]
        )
        
        return config
    
    def update_pipeline_config(self, pipeline_id: str, config: PipelineConfig) -> bool:
        """
        Update existing pipeline configuration
        Args:
            pipeline_id: Pipeline identifier
            config: Updated configuration
        Returns:
            bool: True if updated successfully
        """
        query = """
            UPDATE pipelines 
            SET name=?, description=?, config_json=?, schedule=?, source_config=?, target_config=?, updated_at=CURRENT_TIMESTAMP
            WHERE id=? AND is_active=1
        """
        
        params = (
            config.name,
            config.description,
            json.dumps(config.to_dict(), ensure_ascii=False),
            config.schedule,
            json.dumps(config.source_config, ensure_ascii=False),
            json.dumps(config.target_config, ensure_ascii=False),
            pipeline_id
        )
        
        rows_affected = self.db.execute_update(query, params)
        return rows_affected > 0
    
    def delete_pipeline_config(self, pipeline_id: str) -> bool:
        """
        Delete pipeline configuration (soft delete)
        Args:
            pipeline_id: Pipeline identifier
        Returns:
            bool: True if deleted successfully
        """
        query = "UPDATE pipelines SET is_active = 0, updated_at = CURRENT_TIMESTAMP WHERE id = ?"
        rows_affected = self.db.execute_update(query, (pipeline_id,))
        return rows_affected > 0
    
    def list_pipeline_configs(self, active_only: bool = True) -> List[Dict[str, Any]]:
        """
        List all pipeline configurations
        Args:
            active_only: Only return active configurations
        Returns:
            List of pipeline metadata
        """
        where_clause = "WHERE is_active = 1" if active_only else ""
        query = f"""
            SELECT id, name, description, schedule, created_at, updated_at, version 
            FROM pipelines 
            {where_clause} 
            ORDER BY created_at DESC
        """
        
        return self.db.execute_query(query)
    
    def save_db_connection_config(self, config: Dict[str, Any]) -> bool:
        """
        Save database connection configuration
        Args:
            config: Connection configuration with 'name', 'type', 'config_json'
        Returns:
            bool: True if saved successfully
        """
        connection_id = config.get("id", f"conn_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{secrets.token_hex(4)}")
        
        query = """
            INSERT INTO db_connections (id, name, type, config_json)
            VALUES (?, ?, ?, ?)
        """
        
        params = (
            connection_id,
            config["name"],
            config["type"],
            json.dumps(config.get("config", {}), ensure_ascii=False)
        )
        
        try:
            self.db.execute_update(query, params)
            return True
        except Exception:
            return False
    
    def load_db_connection_config(self, connection_id: str) -> Optional[Dict[str, Any]]:
        """
        Load database connection configuration
        Args:
            connection_id: Connection identifier
        Returns:
            Dict with connection configuration or None if not found
        """
        query = "SELECT * FROM db_connections WHERE id = ? AND is_active = 1"
        results = self.db.execute_query(query, (connection_id,))
        
        if not results:
            return None
        
        row = results[0]
        config = json.loads(row["config_json"])
        config["id"] = row["id"]
        config["name"] = row["name"]
        config["type"] = row["type"]
        
        return config
    
    def list_db_connection_configs(self) -> List[Dict[str, Any]]:
        """
        List all database connection configurations
        Returns:
            List of connection configurations
        """
        query = "SELECT id, name, type, created_at, updated_at FROM db_connections WHERE is_active = 1 ORDER BY created_at DESC"
        return self.db.execute_query(query)
    
    def delete_db_connection_config(self, connection_id: str) -> bool:
        """
        Delete database connection configuration (soft delete)
        Args:
            connection_id: Connection identifier
        Returns:
            bool: True if deleted successfully
        """
        query = "UPDATE db_connections SET is_active = 0, updated_at = CURRENT_TIMESTAMP WHERE id = ?"
        rows_affected = self.db.execute_update(query, (connection_id,))
        return rows_affected > 0
    
    def get_pipeline_statistics(self, pipeline_id: str) -> Dict[str, Any]:
        """
        Get statistics for a specific pipeline
        Args:
            pipeline_id: Pipeline identifier
        Returns:
            Dict with pipeline statistics
        """
        from .logging_service import LoggingService
        logging_service = LoggingService(self.db)
        
        # Get run history for this pipeline
        runs = logging_service.get_run_history(pipeline_id, limit=1000)
        
        # Calculate statistics
        total_runs = len(runs)
        completed_runs = sum(1 for run in runs if run.get("status") == "COMPLETED")
        failed_runs = sum(1 for run in runs if run.get("status") == "FAILED")
        
        # Calculate average processing time
        processing_times = []
        for run in runs:
            if run.get("start_time") and run.get("end_time"):
                start_time = datetime.fromisoformat(run["start_time"])
                end_time = datetime.fromisoformat(run["end_time"])
                duration = (end_time - start_time).total_seconds()
                processing_times.append(duration)
        
        avg_processing_time = sum(processing_times) / len(processing_times) if processing_times else 0
        
        return {
            "total_runs": total_runs,
            "completed_runs": completed_runs,
            "failed_runs": failed_runs,
            "success_rate": completed_runs / total_runs * 100 if total_runs > 0 else 0,
            "average_processing_time": avg_processing_time,
            "recent_runs": runs[:10]  # Last 10 runs
        }
    
    def get_pipeline_name(self, pipeline_id: str) -> str:
        """
        Get pipeline name by ID
        Args:
            pipeline_id: Pipeline identifier
        Returns:
            str: Pipeline name or empty string if not found
        """
        query = "SELECT name FROM pipelines WHERE id = ? AND is_active = 1"
        results = self.db.execute_query(query, (pipeline_id,))
        
        return results[0]["name"] if results else ""