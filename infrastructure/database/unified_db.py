#!/usr/bin/env python3
"""
Unified Database - Single SQLite file for all app data
Stores: configurations, scripts, logs, and results
"""

import sqlite3
import json
import os
from pathlib import Path
from typing import Dict, Any, List, Optional
from datetime import datetime
import threading
from cryptography.fernet import Fernet
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC
import base64
import secrets

class UnifiedDatabase:
    """
    Unified SQLite database for all application data
    """
    
    def __init__(self, db_path: str = "unified_storage.sqlite"):
        self.db_path = db_path
        self.lock = threading.Lock()  # Thread safety for database operations
        
        # Initialize database
        self._ensure_database_exists()
    
    def _ensure_database_exists(self):
        """
        Create database and tables if they don't exist
        """
        with self._get_connection() as conn:
            cursor = conn.cursor()
            
            # Pipelines table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS pipelines (
                    id TEXT PRIMARY KEY,
                    name TEXT NOT NULL,
                    description TEXT,
                    config_json TEXT NOT NULL,
                    schedule TEXT,
                    source_config TEXT,
                    target_config TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    version INTEGER DEFAULT 1,
                    is_active BOOLEAN DEFAULT 1
                )
            """)
            
            # Scripts table (with encryption)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS user_scripts (
                    id TEXT PRIMARY KEY,
                    name TEXT NOT NULL,
                    code_encrypted TEXT NOT NULL,
                    checksum TEXT NOT NULL,
                    pipeline_id TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    version INTEGER DEFAULT 1,
                    is_active BOOLEAN DEFAULT 1,
                    FOREIGN KEY (pipeline_id) REFERENCES pipelines (id)
                )
            """)
            
            # Pipeline runs table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS pipeline_runs (
                    id TEXT PRIMARY KEY,
                    pipeline_id TEXT NOT NULL,
                    start_time TIMESTAMP NOT NULL,
                    end_time TIMESTAMP,
                    status TEXT NOT NULL,
                    processed_count INTEGER DEFAULT 0,
                    success_count INTEGER DEFAULT 0,
                    error_count INTEGER DEFAULT 0,
                    errors_json TEXT,
                    metadata_json TEXT,
                    FOREIGN KEY (pipeline_id) REFERENCES pipelines (id)
                )
            """)
            
            # Chunks table (for extracted data)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS chunks (
                    id TEXT PRIMARY KEY,
                    pipeline_run_id TEXT NOT NULL,
                    document_id TEXT,
                    page_num INTEGER,
                    section_id TEXT,
                    section_title TEXT,
                    section_level INTEGER,
                    text_content TEXT,
                    chunk_type TEXT,
                    extraction_results_json TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (pipeline_run_id) REFERENCES pipeline_runs (id)
                )
            """)
            
            # DB Connections table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS db_connections (
                    id TEXT PRIMARY KEY,
                    name TEXT NOT NULL,
                    type TEXT NOT NULL,  -- sqlite, postgresql, mysql, mongodb
                    config_json TEXT NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    is_active BOOLEAN DEFAULT 1
                )
            """)
            
            # Changelog for tracking changes
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS changelog (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    table_name TEXT NOT NULL,
                    record_id TEXT NOT NULL,
                    action TEXT NOT NULL,
                    old_values_json TEXT,
                    new_values_json TEXT,
                    changed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    user_id TEXT
                )
            """)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS logs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    level TEXT NOT NULL,
                    message TEXT NOT NULL,
                    pipeline_id TEXT,
                    pipeline_run_id TEXT,
                    document_path TEXT,
                    extra_data_json TEXT,
                    logged_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (pipeline_id) REFERENCES pipelines (id),
                    FOREIGN KEY (pipeline_run_id) REFERENCES pipeline_runs (id)
                )
            """)
        
            # Create indexes for logs
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_logs_pipeline ON logs(pipeline_id)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_logs_run ON logs(pipeline_run_id)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_logs_level ON logs(level)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_logs_time ON logs(logged_at)")
        
            # Indexes for performance
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_pipelines_name ON pipelines(name)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_pipeline_runs_status ON pipeline_runs(status)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_pipeline_runs_time ON pipeline_runs(start_time)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_chunks_run_id ON chunks(pipeline_run_id)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_chunks_document ON chunks(document_id)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_scripts_pipeline ON user_scripts(pipeline_id)")
            
            conn.commit()
    
    def initialize_schema(self):
        """
        Public method to initialize database schema
        """
        self._ensure_database_exists()
    
    def create_default_configs(self):
        """
        Create default configurations if they don't exist
        """
        # This could create default pipelines, default DB connections, etc.
        pass
    
    def _get_connection(self) -> sqlite3.Connection:
        """
        Get database connection (thread-safe)
        """
        return sqlite3.connect(self.db_path, check_same_thread=False)
    
    def execute_query(self, query: str, params: tuple = ()) -> List[Dict[str, Any]]:
        """
        Execute SELECT query and return results as list of dictionaries
        """
        with self._get_connection() as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            cursor.execute(query, params)
            return [dict(row) for row in cursor.fetchall()]
    
    def execute_update(self, query: str, params: tuple = ()) -> int:
        """
        Execute INSERT/UPDATE/DELETE and return affected rows
        """
        with self.lock:
            with self._get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(query, params)
                conn.commit()
                return cursor.rowcount
    
    def backup_database(self, backup_path: str) -> bool:
        """
        Backup database to another file
        """
        try:
            source_conn = sqlite3.connect(self.db_path)
            backup_conn = sqlite3.connect(backup_path)
            
            source_conn.backup(backup_conn)
            
            source_conn.close()
            backup_conn.close()
            
            return True
        except Exception as e:
            print(f"Backup failed: {e}")
            return False
    
    def get_database_stats(self) -> Dict[str, Any]:
        """
        Get database statistics
        """
        stats = {}
        
        # Count records in each table
        tables = ['pipelines', 'user_scripts', 'pipeline_runs', 'chunks', 'db_connections', 'changelog']
        
        for table in tables:
            result = self.execute_query(f"SELECT COUNT(*) as count FROM {table}")
            stats[table] = result[0]['count'] if result else 0
        
        # Get database file size
        stats['file_size_mb'] = round(os.path.getsize(self.db_path) / (1024 * 1024), 2)
        
        return stats
    
    def vacuum_database(self):
        """
        Optimize database by removing fragmentation
        """
        with self._get_connection() as conn:
            conn.execute("VACUUM")
            conn.commit()
    
    def is_connected(self) -> bool:
        """
        Check if database is accessible
        """
        try:
            with self._get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT 1")
                cursor.fetchone()
                return True
        except:
            return False
    
    def close(self):
        """
        Close database connections
        """
        # SQLite doesn't need explicit connection management
        # The connections are automatically closed when the connection object is garbage collected
        pass

class DatabaseManager:
    """
    Manager for database operations
    """
    
    def __init__(self, db: UnifiedDatabase):
        self.db = db
    
    def insert_pipeline(self, pipeline_data: Dict[str, Any]) -> str:
        """
        Insert new pipeline configuration
        """
        pipeline_id = pipeline_data.get('id', f"pipeline_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{secrets.token_hex(4)}")
        
        query = """
            INSERT INTO pipelines (id, name, description, config_json, schedule, source_config, target_config, version)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """
        
        params = (
            pipeline_id,
            pipeline_data['name'],
            pipeline_data.get('description', ''),
            json.dumps(pipeline_data.get('config', {}), ensure_ascii=False),
            pipeline_data.get('schedule', ''),
            json.dumps(pipeline_data.get('source_config', {}), ensure_ascii=False),
            json.dumps(pipeline_data.get('target_config', {}), ensure_ascii=False),
            pipeline_data.get('version', 1)
        )
        
        self.db.execute_update(query, params)
        return pipeline_id
    
    def update_pipeline(self, pipeline_id: str, pipeline_data: Dict[str, Any]) -> int:
        """
        Update existing pipeline
        """
        query = """
            UPDATE pipelines 
            SET name=?, description=?, config_json=?, schedule=?, source_config=?, target_config=?, updated_at=CURRENT_TIMESTAMP
            WHERE id=?
        """
        
        params = (
            pipeline_data['name'],
            pipeline_data.get('description', ''),
            json.dumps(pipeline_data.get('config', {}), ensure_ascii=False),
            pipeline_data.get('schedule', ''),
            json.dumps(pipeline_data.get('source_config', {}), ensure_ascii=False),
            json.dumps(pipeline_data.get('target_config', {}), ensure_ascii=False),
            pipeline_id
        )
        
        return self.db.execute_update(query, params)
    
    def insert_pipeline_run(self, run_data: Dict[str, Any]) -> str:
        """
        Insert new pipeline run
        """
        run_id = run_data.get('id', f"run_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{secrets.token_hex(4)}")
        
        query = """
            INSERT INTO pipeline_runs (id, pipeline_id, start_time, end_time, status, processed_count, success_count, error_count, errors_json, metadata_json)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        
        params = (
            run_id,
            run_data['pipeline_id'],
            run_data['start_time'],
            run_data.get('end_time'),
            run_data['status'],
            run_data.get('processed_count', 0),
            run_data.get('success_count', 0),
            run_data.get('error_count', 0),
            json.dumps(run_data.get('errors', []), ensure_ascii=False),
            json.dumps(run_data.get('metadata', {}), ensure_ascii=False)
        )
        
        self.db.execute_update(query, params)
        return run_id
    
    def insert_chunks(self, chunks: List[Dict[str, Any]]) -> int:
        """
        Insert multiple chunks efficiently
        """
        if not chunks:
            return 0
        
        query = """
            INSERT INTO chunks (id, pipeline_run_id, document_id, page_num, section_id, section_title, section_level, text_content, chunk_type, extraction_results_json)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        
        params_list = []
        for chunk in chunks:
            params = (
                chunk.get('id', f"chunk_{secrets.token_hex(8)}"),
                chunk['pipeline_run_id'],
                chunk.get('document_id'),
                chunk.get('page_num'),
                chunk.get('section_id'),
                chunk.get('section_title'),
                chunk.get('section_level'),
                chunk.get('text_content'),
                chunk.get('chunk_type'),
                json.dumps(chunk.get('extraction_results', {}), ensure_ascii=False)
            )
            params_list.append(params)
        
        with self.db.lock:
            with self.db._get_connection() as conn:
                cursor = conn.cursor()
                cursor.executemany(query, params_list)
                conn.commit()
                return cursor.rowcount