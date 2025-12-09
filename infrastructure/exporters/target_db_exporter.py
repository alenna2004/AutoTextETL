#!/usr/bin/env python3
"""
Target Database Exporter - Abstract base class for all database exporters
"""

from abc import ABC, abstractmethod
from typing import List, Dict, Any, Optional, Union
from domain.interfaces import IDbExporter  # ← USE CORRECT INTERFACE PATH
from domain.chunk import Chunk
from domain.pipeline import PipelineRun, PipelineStatus
import json
from datetime import datetime

class TargetDbExporter(IDbExporter):  # ← INHERIT FROM CORRECT INTERFACE
    """
    Abstract base class for all database exporters
    Provides common functionality for chunk insertion and metadata handling
    """
    
    def __init__(self):
        self.connection = None
        self.is_connected = False
        self.connection_config = None
        self._connected_at = None
    
    def connect(self, config: Dict[str, Any]):
        """
        Connect to database with configuration
        Args:
            config: Database connection configuration
        """
        self.connection_config = config
        self._establish_connection(config)
        self.is_connected = True
        self._connected_at = datetime.now()
    
    @abstractmethod
    def _establish_connection(self, config: Dict[str, Any]):
        """
        Establish actual database connection (implementation specific)
        """
        pass
    
    def batch_insert(self, chunks: List[Chunk], table_name: str = "chunks"):
        """
        Batch insert chunks to database
        Args:
            chunks: List of chunks to insert
            table_name: Target table name
        """
        if not self.is_connected:
            raise RuntimeError("Database not connected. Call connect() first.")
        
        if not chunks:
            return  # Nothing to insert
        
        # Prepare data for insertion
        prepared_data = []
        for chunk in chunks:
            row_data = self._prepare_chunk_for_insertion(chunk)
            prepared_data.append(row_data)
        
        # Execute batch insert
        self._execute_batch_insert(prepared_data, table_name)
    
    def _prepare_chunk_for_insertion(self, chunk: Chunk) -> Dict[str, Any]:
        """
        Prepare chunk data for database insertion
        Args:
            chunk: Chunk to prepare
        Returns:
            Dict with prepared data
        """
        return {
            "id": chunk.id,
            "text_content": chunk.text,
            "document_id": chunk.meta.document_id,
            "page_num": chunk.meta.page_num,
            "section_id": chunk.meta.section_id,
            "section_title": chunk.meta.section_title,
            "section_level": chunk.meta.section_level,
            "chunk_type": chunk.meta.chunk_type.value if hasattr(chunk.meta.chunk_type, 'value') else str(chunk.meta.chunk_type),
            "pipeline_run_id": chunk.meta.pipeline_run_id,
            "source_type": chunk.meta.source_type,
            "line_num": chunk.meta.line_num,
            "extraction_results": json.dumps(chunk.extraction_results, ensure_ascii=False),
            "created_at": datetime.now().isoformat()
        }
    
    @abstractmethod
    def _execute_batch_insert(self, prepared_ : List[Dict[str, Any]], table_name: str):
        """
        Execute actual batch insertion (implementation specific)
        Args:
            prepared_ Prepared data rows
            table_name: Target table name
        """
        pass
    
    def export_run_metadata(self, run: PipelineRun):
        """
        Export pipeline run metadata to database
        Args:
            run: Pipeline run instance
        """
        if not self.is_connected:
            raise RuntimeError("Database not connected. Call connect() first.")
        
        run_metadata = {
            "id": run.id,
            "pipeline_id": run.pipeline_id,
            "start_time": run.start_time.isoformat() if run.start_time else None,
            "end_time": run.end_time.isoformat() if run.end_time else None,
            "status": run.status.value if hasattr(run.status, 'value') else str(run.status),
            "processed_count": run.processed_count,
            "success_count": run.success_count,
            "error_count": run.error_count,
            "errors": json.dumps(run.errors, ensure_ascii=False),
            "metadata": json.dumps(run.metadata, ensure_ascii=False),
            "exported_at": datetime.now().isoformat()
        }
        
        # Insert run metadata
        self._execute_run_metadata_insert(run_metadata)
    
    @abstractmethod
    def _execute_run_metadata_insert(self, run_meta : Dict[str, Any]):
        """
        Execute run metadata insertion (implementation specific)
        Args:
            run_metadata: Run metadata to insert
        """
        pass
    
    def close(self):
        """
        Close database connection
        """
        if self.connection and self.is_connected:
            self._close_connection()
            self.is_connected = False
            self.connection = None
            self.connection_config = None
            self._connected_at = None
    
    @abstractmethod
    def _close_connection(self):
        """
        Close actual database connection (implementation specific)
        """
        pass
    
    def ensure_table_exists(self, table_name: str, schema: Dict[str, str]):
        """
        Ensure target table exists with specified schema
        Args:
            table_name: Table name to check/create
            schema: Column definitions as {column_name: column_type}
        """
        if not self.is_connected:
            raise RuntimeError("Database not connected. Call connect() first.")
        
        self._create_table_if_not_exists(table_name, schema)
    
    @abstractmethod
    def _create_table_if_not_exists(self, table_name: str, schema: Dict[str, str]):
        """
        Create table if it doesn't exist (implementation specific)
        Args:
            table_name: Table name
            schema: Column definitions
        """
        pass
    
    def get_connection_status(self) -> Dict[str, Any]:
        """
        Get current connection status
        Returns:
            Dict with connection information
        """
        return {
            "is_connected": self.is_connected,
            "connection_config": self.connection_config,
            "connected_at": self._connected_at.isoformat() if self._connected_at else None,
            "active_queries": getattr(self, '_active_queries', 0)
        }
    
    def test_connection(self) -> bool:
        """
        Test database connection
        Returns:
            bool: True if connection is successful
        """
        if not self.is_connected:
            return False
        
        try:
            # Execute a simple query to test connection
            cursor = self.connection.cursor()
            cursor.execute("SELECT 1")
            cursor.fetchone()
            cursor.close()
            return True
        except Exception:
            return False