#!/usr/bin/env python3
"""
SQLite Exporter - Export chunks to SQLite database
"""

import sqlite3
from typing import List, Dict, Any
from domain.chunk import Chunk
from .target_db_exporter import TargetDbExporter
import os
import json
from datetime import datetime

class SqliteExporter(TargetDbExporter):
    """
    SQLite database exporter implementation
    """
    
    def _establish_connection(self, config: Dict[str, Any]):
        """
        Establish SQLite connection
        Args:
            config: Configuration with 'path' for database file
        """
        db_path = config.get("path", config.get("database_path", "chunks.db"))
        
        # Ensure directory exists
        db_dir = os.path.dirname(db_path)
        if db_dir:
            os.makedirs(db_dir, exist_ok=True)
        
        self.connection = sqlite3.connect(db_path, check_same_thread=False)
        self.connection.row_factory = sqlite3.Row  # Enable dict-like access
        self._connected_at = datetime.now()
        
        # Create required tables if they don't exist
        self._create_default_tables()
    
    def _create_default_tables(self):
        """
        Create default tables for chunk storage
        """
        # Chunks table
        self.connection.execute("""
            CREATE TABLE IF NOT EXISTS chunks (
                id TEXT PRIMARY KEY,
                text_content TEXT NOT NULL,
                document_id TEXT,
                page_num INTEGER,
                section_id TEXT,
                section_title TEXT,
                section_level INTEGER,
                chunk_type TEXT,
                pipeline_run_id TEXT,
                source_type TEXT,
                line_num INTEGER,
                extraction_results TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # Pipeline runs table
        self.connection.execute("""
            CREATE TABLE IF NOT EXISTS pipeline_runs (
                id TEXT PRIMARY KEY,
                pipeline_id TEXT NOT NULL,
                start_time TIMESTAMP,
                end_time TIMESTAMP,
                status TEXT,
                processed_count INTEGER DEFAULT 0,
                success_count INTEGER DEFAULT 0,
                error_count INTEGER DEFAULT 0,
                errors TEXT,
                metadata TEXT,
                exported_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # Create indexes for performance
        self.connection.execute("CREATE INDEX IF NOT EXISTS idx_chunks_document ON chunks(document_id)")
        self.connection.execute("CREATE INDEX IF NOT EXISTS idx_chunks_page ON chunks(page_num)")
        self.connection.execute("CREATE INDEX IF NOT EXISTS idx_chunks_run ON chunks(pipeline_run_id)")
        self.connection.execute("CREATE INDEX IF NOT EXISTS idx_chunks_created ON chunks(created_at)")
        self.connection.execute("CREATE INDEX IF NOT EXISTS idx_runs_pipeline ON pipeline_runs(pipeline_id)")
        self.connection.execute("CREATE INDEX IF NOT EXISTS idx_runs_status ON pipeline_runs(status)")
        
        self.connection.commit()
    
    def _execute_batch_insert(self, prepared_data: List[Dict[str, Any]], table_name: str):
        """
        Execute batch insertion for SQLite
        """
        if not prepared_data:
            return
        
        # Use executemany for efficient batch insertion
        columns = list(prepared_data[0].keys())
        placeholders = ','.join(['?' for _ in columns])
        column_names = ','.join(columns)
        
        query = f"INSERT OR REPLACE INTO {table_name} ({column_names}) VALUES ({placeholders})"
        
        # Prepare data for SQLite (convert dicts to tuples in correct order)
        values = []
        for row in prepared_data:
            row_values = tuple(row[col] for col in columns)
            values.append(row_values)
        
        cursor = self.connection.cursor()
        cursor.executemany(query, values)
        self.connection.commit()
        cursor.close()
    
    def _execute_run_metadata_insert(self, run_metadata: Dict[str, Any]):
        """
        Execute run metadata insertion for SQLite
        """
        columns = list(run_metadata.keys())
        placeholders = ','.join(['?' for _ in columns])
        column_names = ','.join(columns)
        
        query = f"INSERT OR REPLACE INTO pipeline_runs ({column_names}) VALUES ({placeholders})"
        
        values = tuple(run_metadata[col] for col in columns)
        
        cursor = self.connection.cursor()
        cursor.execute(query, values)
        self.connection.commit()
        cursor.close()
    
    def _close_connection(self):
        """
        Close SQLite connection
        """
        if self.connection:
            self.connection.close()
    
    def _create_table_if_not_exists(self, table_name: str, schema: Dict[str, str]):
        """
        Create table if it doesn't exist in SQLite
        Args:
            table_name: Table name to create
            schema: Column definitions as {column_name: column_type}
        """
        if not self.is_connected:
            raise RuntimeError("Database not connected")
        
        # Build CREATE TABLE statement
        columns_def = []
        for col_name, col_type in schema.items():
            columns_def.append(f"{col_name} {col_type}")
        
        query = f"CREATE TABLE IF NOT EXISTS {table_name} ({', '.join(columns_def)})"
        
        cursor = self.connection.cursor()
        cursor.execute(query)
        self.connection.commit()
        cursor.close()
    
    def get_connection_status(self) -> Dict[str, Any]:
        """
        Get SQLite connection status
        """
        status = super().get_connection_status()
        if self.is_connected and self.connection:
            try:
                cursor = self.connection.cursor()
                cursor.execute("PRAGMA database_list")
                db_info = cursor.fetchall()
                cursor.close()
                
                status["database_info"] = [{"name": row[1], "file": row[2]} for row in db_info]
            except Exception as e:
                status["error"] = str(e)
        
        return status
    
    def get_table_info(self, table_name: str) -> List[Dict[str, Any]]:
        """
        Get information about table structure
        Args:
            table_name: Name of table to inspect
        Returns:
            List of column information
        """
        if not self.is_connected:
            raise RuntimeError("Database not connected")
        
        cursor = self.connection.cursor()
        cursor.execute(f"PRAGMA table_info({table_name})")
        columns = cursor.fetchall()
        cursor.close()
        
        return [
            {
                "cid": col[0],
                "name": col[1],
                "type": col[2],
                "not_null": bool(col[3]),
                "default_value": col[4],
                "primary_key": bool(col[5])
            }
            for col in columns
        ]
    
    def get_row_count(self, table_name: str) -> int:
        """
        Get count of rows in table
        Args:
            table_name: Table name
        Returns:
            int: Row count
        """
        if not self.is_connected:
            raise RuntimeError("Database not connected")
        
        cursor = self.connection.cursor()
        cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
        count = cursor.fetchone()[0]
        cursor.close()
        
        return count
    
    def execute_query(self, query: str, params: tuple = ()) -> List[Dict[str, Any]]:
        """
        Execute arbitrary query and return results
        Args:
            query: SQL query string
            params: Query parameters
        Returns:
            List of result rows as dictionaries
        """
        if not self.is_connected:
            raise RuntimeError("Database not connected")
        
        cursor = self.connection.cursor()
        cursor.execute(query, params)
        rows = cursor.fetchall()
        cursor.close()
        
        # Convert rows to dictionaries
        return [dict(row) for row in rows]
    
    def backup_database(self, backup_path: str):
        """
        Create backup of SQLite database
        Args:
            backup_path: Path for backup file
        """
        if not self.is_connected or not self.connection:
            raise RuntimeError("Database not connected")
        
        backup_conn = sqlite3.connect(backup_path)
        self.connection.backup(backup_conn)
        backup_conn.close()
    
    def restore_from_backup(self, backup_path: str):
        """
        Restore database from backup
        Args:
            backup_path: Path to backup file
        """
        if not os.path.exists(backup_path):
            raise FileNotFoundError(f"Backup file not found: {backup_path}")
        
        # Close current connection
        if self.connection:
            self.connection.close()
        
        # Copy backup file to current database location
        current_db_path = self.connection_config.get("path", "chunks.db")
        import shutil
        shutil.copy2(backup_path, current_db_path)
        
        # Reconnect to restored database
        self._establish_connection({"path": current_db_path})