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

from domain.pipeline import PipelineConfig, PipelineStepConfig, StepType, PipelineStatus
from domain.document import DocumentFormat
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
            INSERT OR REPLACE INTO pipelines 
            (id, name, description, config_json, schedule, source_config, target_config, version, created_at, updated_at, is_active)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1)
        """
        
        params = (
            pipeline_id,
            config.name,
            config.description,
            json.dumps(config.to_dict(), ensure_ascii=False),
            config.schedule,
            json.dumps(config.source_config, ensure_ascii=False),
            json.dumps(config.target_config, ensure_ascii=False),
            config.version,
            config.created_at.isoformat() if hasattr(config, 'created_at') else datetime.now().isoformat(),
            config.updated_at.isoformat() if hasattr(config, 'updated_at') else datetime.now().isoformat()
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
        
        try:
            # Parse configuration from JSON
            config_data = json.loads(row["config_json"])
            
            # Create pipeline steps from configuration
            steps = []
            for step_data in config_data.get("steps", []):
                step = PipelineStepConfig(
                    type=StepType(step_data["type"]),
                    id=step_data.get("id", f"step_{secrets.token_hex(4)}"),
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
                schedule=row.get("schedule", ""),
                source_config=json.loads(row["source_config"]) if row.get("source_config") else {},
                target_config=json.loads(row["target_config"]) if row.get("target_config") else {},
                version=row.get("version", 1)
            )
            
            return config
        except Exception as e:
            print(f"Error loading pipeline config: {e}")
            return None
    
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
            SET name=?, description=?, config_json=?, schedule=?, source_config=?, target_config=?, 
                version=?, updated_at=CURRENT_TIMESTAMP
            WHERE id=? AND is_active=1
        """
        
        params = (
            config.name,
            config.description,
            json.dumps(config.to_dict(), ensure_ascii=False),
            config.schedule,
            json.dumps(config.source_config, ensure_ascii=False),
            json.dumps(config.target_config, ensure_ascii=False),
            config.version,
            pipeline_id
        )
        
        rows_affected = self.db.execute_update(query, params)
        return rows_affected > 0
    
    def delete_pipeline_config(self, pipeline_id: str) -> bool:
        """
        Delete pipeline configuration (soft delete)
        Args:
            pipeline_id: Pipeline identifier to delete
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
    
    def get_pipeline_statistics(self, pipeline_id: str) -> Dict[str, Any]:
        """
        Get statistics for a specific pipeline
        Args:
            pipeline_id: Pipeline identifier
        Returns:
            Dict with pipeline statistics
        """
        query = "SELECT * FROM pipelines WHERE id = ? AND is_active = 1"
        results = self.db.execute_query(query, (pipeline_id,))
        
        if not results:
            return {"error": "Pipeline not found"}
        
        row = results[0]
        
        # Get run history for this pipeline
        from .logging_service import LoggingService
        logging_service = LoggingService(self.db)
        run_history = logging_service.get_run_history(pipeline_id, limit=100)
        
        return {
            "pipeline_info": row,
            "run_count": len(run_history),
            "recent_runs": run_history[:10],  # Last 10 runs
            "status_distribution": self._calculate_status_distribution(run_history)
        }
    
    def _calculate_status_distribution(self, run_history: List[Dict[str, Any]]) -> Dict[str, int]:
        """
        Calculate distribution of run statuses
        """
        status_counts = {}
        for run in run_history:
            status = run.get("status", "UNKNOWN").lower()
            status_counts[status] = status_counts.get(status, 0) + 1
        return status_counts
    
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
        
        return results[0]["name"] if results else "Unknown Pipeline"
    
    def save_db_connection_config(self, config: Dict[str, Any]) -> bool:
        """
        Save database connection configuration
        Args:
            config: Connection configuration
        Returns:
            bool: True if saved successfully
        """
        connection_id = config.get("id", f"conn_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{secrets.token_hex(4)}")
        
        query = """
            INSERT OR REPLACE INTO db_connections 
            (id, name, type, config_json, created_at, updated_at, is_active)
            VALUES (?, ?, ?, ?, ?, ?, 1)
        """
        
        params = (
            connection_id,
            config["name"],
            config["type"],
            json.dumps(config.get("config", {}), ensure_ascii=False),
            datetime.now().isoformat(),
            datetime.now().isoformat()
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