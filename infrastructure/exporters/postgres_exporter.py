#!/usr/bin/env python3
"""
PostgreSQL Exporter - Export chunks to PostgreSQL database
"""

from typing import List, Dict, Any
from domain.chunk import Chunk
from .target_db_exporter import TargetDbExporter
import psycopg2
from psycopg2.extras import execute_batch, RealDictCursor
import json
from datetime import datetime

class PostgresExporter(TargetDbExporter):
    """
    PostgreSQL database exporter implementation
    """
    
    def _establish_connection(self, config: Dict[str, Any]):
        """
        Establish PostgreSQL connection
        Args:
            config: Configuration with PostgreSQL connection details
        """
        connection_params = {
            "host": config.get("host", "localhost"),
            "port": config.get("port", 5432),
            "database": config.get("database", "chunks"),
            "user": config.get("user", "postgres"),
            "password": config.get("password", ""),
            "sslmode": config.get("sslmode", "prefer"),
            "connect_timeout": config.get("timeout", 30)
        }
        
        self.connection = psycopg2.connect(**connection_params)
        self.connection.autocommit = False
        self._connected_at = datetime.now()
        
        # Create required tables if they don't exist
        self._create_default_tables()
    
    def _create_default_tables(self):
        """
        Create default tables for chunk storage in PostgreSQL
        """
        cursor = self.connection.cursor()
        
        try:
            # Chunks table
            cursor.execute("""
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
                    extraction_results JSONB,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # Pipeline runs table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS pipeline_runs (
                    id TEXT PRIMARY KEY,
                    pipeline_id TEXT NOT NULL,
                    start_time TIMESTAMP,
                    end_time TIMESTAMP,
                    status TEXT,
                    processed_count INTEGER DEFAULT 0,
                    success_count INTEGER DEFAULT 0,
                    error_count INTEGER DEFAULT 0,
                    errors JSONB,
                    metadata JSONB,
                    exported_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # Create indexes for performance
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_chunks_document ON chunks USING HASH (document_id)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_chunks_page ON chunks (page_num)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_chunks_run ON chunks USING HASH (pipeline_run_id)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_chunks_created ON chunks (created_at)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_runs_pipeline ON pipeline_runs USING HASH (pipeline_id)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_runs_status ON pipeline_runs (status)")
            
            self.connection.commit()
        except Exception:
            self.connection.rollback()
            raise
        finally:
            cursor.close()
    
    def _execute_batch_insert(self, prepared_data: List[Dict[str, Any]], table_name: str):
        """
        Execute batch insertion for PostgreSQL
        """
        if not prepared_data:
            return
        
        # Use execute_batch for efficient PostgreSQL insertion
        columns = list(prepared_data[0].keys())
        placeholders = ','.join([f'%({col})s' for col in columns])
        column_names = ','.join(columns)
        
        query = f"INSERT INTO {table_name} ({column_names}) VALUES ({placeholders}) ON CONFLICT (id) DO UPDATE SET "
        update_clause = ', '.join([f"{col} = EXCLUDED.{col}" for col in columns if col != 'id'])
        query += update_clause
        
        cursor = self.connection.cursor()
        try:
            execute_batch(cursor, query, prepared_data, page_size=1000)
            self.connection.commit()
        except Exception:
            self.connection.rollback()
            raise
        finally:
            cursor.close()
    
    def _execute_run_metadata_insert(self, run_metadata: Dict[str, Any]):
        """
        Execute run metadata insertion for PostgreSQL
        """
        columns = list(run_metadata.keys())
        placeholders = ','.join([f'%({col})s' for col in columns])
        column_names = ','.join(columns)
        
        query = f"""
            INSERT INTO pipeline_runs ({column_names}) 
            VALUES ({placeholders}) 
            ON CONFLICT (id) 
            DO UPDATE SET 
                end_time = EXCLUDED.end_time,
                status = EXCLUDED.status,
                processed_count = EXCLUDED.processed_count,
                success_count = EXCLUDED.success_count,
                error_count = EXCLUDED.error_count,
                errors = EXCLUDED.errors,
                metadata = EXCLUDED.metadata,
                exported_at = CURRENT_TIMESTAMP
        """
        
        cursor = self.connection.cursor()
        try:
            cursor.execute(query, run_metadata)
            self.connection.commit()
        except Exception:
            self.connection.rollback()
            raise
        finally:
            cursor.close()
    
    def _close_connection(self):
        """
        Close PostgreSQL connection
        """
        if self.connection:
            self.connection.close()
    
    def _create_table_if_not_exists(self, table_name: str, schema: Dict[str, str]):
        """
        Create table if it doesn't exist in PostgreSQL
        Args:
            table_name: Table name to create
            schema: Column definitions as {column_name: column_type}
        """
        if not self.is_connected:
            raise RuntimeError("Database not connected")
        
        # Build CREATE TABLE statement
        columns_def = []
        for col_name, col_type in schema.items():
            columns_def.append(f'"{col_name}" {col_type}')
        
        query = f'CREATE TABLE IF NOT EXISTS "{table_name}" ({", ".join(columns_def)})'
        
        cursor = self.connection.cursor()
        try:
            cursor.execute(query)
            self.connection.commit()
        except Exception:
            self.connection.rollback()
            raise
        finally:
            cursor.close()
    
    def get_connection_status(self) -> Dict[str, Any]:
        """
        Get PostgreSQL connection status
        """
        status = super().get_connection_status()
        if self.is_connected and self.connection:
            try:
                cursor = self.connection.cursor()
                cursor.execute("SELECT version();")
                pg_version = cursor.fetchone()[0]
                cursor.execute("SELECT current_database();")
                current_db = cursor.fetchone()[0]
                cursor.execute("SELECT current_user;")
                current_user = cursor.fetchone()[0]
                cursor.close()
                
                status["postgres_version"] = pg_version
                status["database"] = current_db
                status["user"] = current_user
            except Exception as e:
                status["error"] = str(e)
        
        return status
    
    def get_table_info(self, table_name: str) -> List[Dict[str, Any]]:
        """
        Get information about PostgreSQL table structure
        Args:
            table_name: Name of table to inspect
        Returns:
            List of column information
        """
        if not self.is_connected:
            raise RuntimeError("Database not connected")
        
        query = """
            SELECT 
                column_name,
                data_type,
                is_nullable,
                column_default,
                is_identity,
                identity_generation
            FROM information_schema.columns 
            WHERE table_name = %s
            ORDER BY ordinal_position
        """
        
        cursor = self.connection.cursor(cursor_factory=RealDictCursor)
        cursor.execute(query, (table_name,))
        columns = cursor.fetchall()
        cursor.close()
        
        return [dict(row) for row in columns]
    
    def get_row_count(self, table_name: str) -> int:
        """
        Get count of rows in PostgreSQL table
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
        
        cursor = self.connection.cursor(cursor_factory=RealDictCursor)
        cursor.execute(query, params)
        rows = cursor.fetchall()
        cursor.close()
        
        return [dict(row) for row in rows]
    
    def vacuum_table(self, table_name: str):
        """
        Vacuum (optimize) PostgreSQL table
        Args:
            table_name: Table to vacuum
        """
        if not self.is_connected:
            raise RuntimeError("Database not connected")
        
        cursor = self.connection.cursor()
        # Use autocommit for VACUUM commands
        old_autocommit = self.connection.autocommit
        self.connection.autocommit = True
        
        try:
            cursor.execute(f"VACUUM ANALYZE {table_name}")
        finally:
            self.connection.autocommit = old_autocommit
            cursor.close()
    
    def get_table_size(self, table_name: str) -> int:
        """
        Get size of table in bytes
        Args:
            table_name: Table name
        Returns:
            int: Size in bytes
        """
        if not self.is_connected:
            raise RuntimeError("Database not connected")
        
        query = "SELECT pg_total_relation_size(%s) as size"
        cursor = self.connection.cursor()
        cursor.execute(query, (table_name,))
        size = cursor.fetchone()[0]
        cursor.close()
        
        return size