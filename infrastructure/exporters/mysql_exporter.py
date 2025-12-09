#!/usr/bin/env python3
"""
MySQL Exporter - Export chunks to MySQL database
"""

from typing import List, Dict, Any
from domain.chunk import Chunk
from .target_db_exporter import TargetDbExporter
import mysql.connector
from mysql.connector import pooling
import json
from datetime import datetime

class MysqlExporter(TargetDbExporter):
    """
    MySQL database exporter implementation
    """
    
    def __init__(self):
        super().__init__()
        self.connection_pool = None
    
    def _establish_connection(self, config: Dict[str, Any]):
        """
        Establish MySQL connection
        Args:
            config: Configuration with MySQL connection details
        """
        connection_config = {
            "host": config.get("host", "localhost"),
            "port": config.get("port", 3306),
            "database": config.get("database", "chunks"),
            "user": config.get("user", "root"),
            "password": config.get("password", ""),
            "charset": config.get("charset", "utf8mb4"),
            "autocommit": False,
            "raise_on_warnings": True,
            "connection_timeout": config.get("timeout", 30)
        }
        
        # Create connection pool for better performance
        pool_config = {
            "pool_name": "chunk_export_pool",
            "pool_size": config.get("pool_size", 5),
            "pool_reset_session": True
        }
        
        self.connection_pool = pooling.MySQLConnectionPool(**pool_config, **connection_config)
        self.connection = self.connection_pool.get_connection()
        self._connected_at = datetime.now()
        
        # Create required tables if they don't exist
        self._create_default_tables()
    
    def _create_default_tables(self):
        """
        Create default tables for chunk storage in MySQL
        """
        cursor = self.connection.cursor()
        
        try:
            # Chunks table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS chunks (
                    id VARCHAR(255) PRIMARY KEY,
                    text_content LONGTEXT NOT NULL,
                    document_id VARCHAR(255),
                    page_num INT,
                    section_id VARCHAR(255),
                    section_title VARCHAR(500),
                    section_level INT,
                    chunk_type VARCHAR(50),
                    pipeline_run_id VARCHAR(255),
                    source_type VARCHAR(50),
                    line_num INT,
                    extraction_results JSON,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
            """)
            
            # Pipeline runs table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS pipeline_runs (
                    id VARCHAR(255) PRIMARY KEY,
                    pipeline_id VARCHAR(255) NOT NULL,
                    start_time TIMESTAMP,
                    end_time TIMESTAMP,
                    status VARCHAR(50),
                    processed_count INT DEFAULT 0,
                    success_count INT DEFAULT 0,
                    error_count INT DEFAULT 0,
                    errors JSON,
                    metadata JSON,
                    exported_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
            """)
            
            # Create indexes for performance
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_chunks_document ON chunks (document_id)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_chunks_page ON chunks (page_num)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_chunks_run ON chunks (pipeline_run_id)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_chunks_created ON chunks (created_at)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_runs_pipeline ON pipeline_runs (pipeline_id)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_runs_status ON pipeline_runs (status)")
            
            self.connection.commit()
        except Exception:
            self.connection.rollback()
            raise
        finally:
            cursor.close()
    
    def _execute_batch_insert(self, prepared_data: List[Dict[str, Any]], table_name: str):
        """
        Execute batch insertion for MySQL
        """
        if not prepared_data:
            return
        
        # Use executemany for efficient MySQL batch insertion
        columns = list(prepared_data[0].keys())
        placeholders = ','.join(['%s' for _ in columns])
        column_names = ','.join([f"`{col}`" for col in columns])
        
        query = f"INSERT INTO `{table_name}` ({column_names}) VALUES ({placeholders}) ON DUPLICATE KEY UPDATE "
        update_clause = ', '.join([f"`{col}` = VALUES(`{col}`)" for col in columns if col != 'id'])
        query += update_clause
        
        # Prepare data for MySQL (convert dicts to tuples in correct order)
        values = []
        for row in prepared_data:
            row_values = tuple(row[col] for col in columns)
            values.append(row_values)
        
        cursor = self.connection.cursor()
        try:
            cursor.executemany(query, values)
            self.connection.commit()
        except Exception:
            self.connection.rollback()
            raise
        finally:
            cursor.close()
    
    def _execute_run_metadata_insert(self, run_metadata: Dict[str, Any]):
        """
        Execute run metadata insertion for MySQL
        """
        columns = list(run_metadata.keys())
        placeholders = ','.join(['%s' for _ in columns])
        column_names = ','.join([f"`{col}`" for col in columns])
        
        query = f"""
            INSERT INTO `pipeline_runs` ({column_names}) 
            VALUES ({placeholders}) 
            ON DUPLICATE KEY UPDATE 
                end_time = VALUES(end_time),
                status = VALUES(status),
                processed_count = VALUES(processed_count),
                success_count = VALUES(success_count),
                error_count = VALUES(error_count),
                errors = VALUES(errors),
                metadata = VALUES(metadata),
                exported_at = CURRENT_TIMESTAMP
        """
        
        values = tuple(run_metadata[col] for col in columns)
        
        cursor = self.connection.cursor()
        try:
            cursor.execute(query, values)
            self.connection.commit()
        except Exception:
            self.connection.rollback()
            raise
        finally:
            cursor.close()
    
    def _close_connection(self):
        """
        Close MySQL connection
        """
        if self.connection and self.connection.is_connected():
            self.connection.close()
        if self.connection_pool:
            # Pool will handle connection cleanup
            pass
    
    def _create_table_if_not_exists(self, table_name: str, schema: Dict[str, str]):
        """
        Create table if it doesn't exist in MySQL
        Args:
            table_name: Table name to create
            schema: Column definitions as {column_name: column_type}
        """
        if not self.is_connected:
            raise RuntimeError("Database not connected")
        
        # Build CREATE TABLE statement
        columns_def = []
        for col_name, col_type in schema.items():
            columns_def.append(f"`{col_name}` {col_type}")
        
        query = f"CREATE TABLE IF NOT EXISTS `{table_name}` ({', '.join(columns_def)}) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci"
        
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
        Get MySQL connection status
        """
        status = super().get_connection_status()
        if self.is_connected and self.connection and self.connection.is_connected():
            try:
                cursor = self.connection.cursor()
                cursor.execute("SELECT VERSION() as version")
                mysql_version = cursor.fetchone()[0]
                cursor.execute("SELECT DATABASE() as database_name")
                current_db = cursor.fetchone()[0]
                cursor.execute("SELECT USER() as user")
                current_user = cursor.fetchone()[0]
                cursor.execute("SELECT CONNECTION_ID() as connection_id")
                connection_id = cursor.fetchone()[0]
                cursor.close()
                
                status["mysql_version"] = mysql_version
                status["database"] = current_db
                status["user"] = current_user
                status["connection_id"] = connection_id
            except Exception as e:
                status["error"] = str(e)
        
        return status
    
    def get_table_info(self, table_name: str) -> List[Dict[str, Any]]:
        """
        Get information about MySQL table structure
        Args:
            table_name: Name of table to inspect
        Returns:
            List of column information
        """
        if not self.is_connected:
            raise RuntimeError("Database not connected")
        
        query = """
            SELECT 
                COLUMN_NAME as column_name,
                DATA_TYPE as data_type,
                IS_NULLABLE as is_nullable,
                COLUMN_DEFAULT as column_default,
                EXTRA as extra_info
            FROM INFORMATION_SCHEMA.COLUMNS 
            WHERE TABLE_NAME = %s AND TABLE_SCHEMA = DATABASE()
            ORDER BY ORDINAL_POSITION
        """
        
        cursor = self.connection.cursor(dictionary=True)
        cursor.execute(query, (table_name,))
        columns = cursor.fetchall()
        cursor.close()
        
        return columns
    
    def get_row_count(self, table_name: str) -> int:
        """
        Get count of rows in MySQL table
        Args:
            table_name: Table name
        Returns:
            int: Row count
        """
        if not self.is_connected:
            raise RuntimeError("Database not connected")
        
        cursor = self.connection.cursor()
        cursor.execute(f"SELECT COUNT(*) FROM `{table_name}`")
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
        
        cursor = self.connection.cursor(dictionary=True)
        cursor.execute(query, params)
        rows = cursor.fetchall()
        cursor.close()
        
        return rows
    
    def optimize_table(self, table_name: str):
        """
        Optimize MySQL table
        Args:
            table_name: Table to optimize
        """
        if not self.is_connected:
            raise RuntimeError("Database not connected")
        
        cursor = self.connection.cursor()
        try:
            cursor.execute(f"OPTIMIZE TABLE `{table_name}`")
            self.connection.commit()
        except Exception:
            self.connection.rollback()
            raise
        finally:
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
        
        query = """
            SELECT 
                (DATA_LENGTH + INDEX_LENGTH) AS size
            FROM information_schema.TABLES 
            WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = %s
        """
        cursor = self.connection.cursor()
        cursor.execute(query, (table_name,))
        result = cursor.fetchone()
        cursor.close()
        
        return result[0] if result else 0