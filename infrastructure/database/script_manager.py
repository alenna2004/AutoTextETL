#!/usr/bin/env python3
"""
Script Manager - Secure script storage and execution with encryption and sandboxing
"""

import sys
import os
from pathlib import Path
from typing import Dict, Any, List, Optional
from domain.interfaces import IDocumentLoader
from domain.document import Document, Page, Section, DocumentFormat
from infrastructure.database.unified_db import DatabaseManager, UnifiedDatabase
from infrastructure.security.crypto_service import CryptoService, get_crypto_service
from infrastructure.security.script_sandbox import ScriptSandbox, ScriptSecurityValidator, SecurityError, ScriptExecutionError, ScriptExecutionTimeout
import secrets
from datetime import datetime
import hashlib

class ScriptManager:
    """
    Secure script manager with encryption and sandboxed execution
    """
    
    def __init__(self, db: UnifiedDatabase, crypto_service: Optional[CryptoService] = None):
        self.db = db
        self.db_manager = DatabaseManager(db)
        self.crypto_service = crypto_service or get_crypto_service()
        self.script_sandbox = ScriptSandbox(timeout=30, memory_limit_mb=100)
    
    def save_script(self, name: str, code: str, pipeline_id: Optional[str] = None) -> str:
        """
        Save encrypted script to database with security validation
        Args:
            name: Script name
            code: Script code
            pipeline_id: Associated pipeline ID
        Returns:
            str: Script ID
        Raises:
            SecurityError: If script fails security validation
        """
        # Validate script security first
        security_errors = ScriptSecurityValidator.validate_script_security(code)
        if security_errors:
            raise SecurityError(f"Script security validation failed: {security_errors}")
        
        # Calculate checksum for integrity verification
        checksum = self._calculate_checksum(code)
        
        # Encrypt the script code
        encrypted_code = self.crypto_service.encrypt(code)
        
        # Generate unique script ID
        script_id = f"script_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{secrets.token_hex(4)}"
        
        # Insert into database
        query = """
            INSERT INTO user_scripts 
            (id, name, code_encrypted, checksum, pipeline_id, version, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, 1, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
        """
        
        params = (
            script_id,
            name,
            encrypted_code,
            checksum,
            pipeline_id
        )
        
        self.db.execute_update(query, params)
        return script_id
    
    def load_script(self, script_id: str) -> Optional[Dict[str, Any]]:
        """
        Load and decrypt script from database
        Args:
            script_id: Script identifier
        Returns:
            Dict with script data or None if not found
        Raises:
            SecurityError: If decryption fails or integrity check fails
        """
        query = "SELECT * FROM user_scripts WHERE id = ? AND is_active = 1"
        results = self.db.execute_query(query, (script_id,))
        
        if not results:
            return None
        
        row = results[0]
        
        try:
            # Decrypt the script code
            decrypted_code = self.crypto_service.decrypt(row['code_encrypted'])
            
            # Verify checksum for integrity
            calculated_checksum = self._calculate_checksum(decrypted_code)
            if calculated_checksum != row['checksum']:
                raise SecurityError(
                    f"Script integrity check failed: checksum mismatch for script {script_id}. "
                    f"Expected: {row['checksum'][:8]}..., Got: {calculated_checksum[:8]}..."
                )
            
            return {
                "id": row['id'],
                "name": row['name'],
                "code": decrypted_code,
                "checksum": row['checksum'],
                "pipeline_id": row['pipeline_id'],
                "created_at": row['created_at'],
                "updated_at": row['updated_at'],
                "version": row['version']
            }
        except Exception as e:
            # Decryption failed - possibly tampered with
            raise SecurityError(f"Script decryption failed for {script_id}: {str(e)}")
    
    def validate_and_execute_script(self, script_id: str, context: Dict[str, Any]) -> Any:
        """
        Validate script security and execute in sandbox
        Args:
            script_id: Script identifier
            context: Execution context
        Returns:
            Any: Script execution result
        Raises:
            SecurityError: If script validation fails
            ScriptExecutionError: If script execution fails
            ScriptExecutionTimeout: If script times out
        """
        # Load script
        script_data = self.load_script(script_id)
        if not script_id:
            raise ValueError(f"Script not found or invalid: {script_id}")
        
        # Re-validate security (double-check for runtime)
        security_errors = ScriptSecurityValidator.validate_script_security(script_data['code'])
        if security_errors:
            raise SecurityError(f"Script security validation failed: {security_errors}")
        
        # Execute in secure sandbox
        try:
            result = self.script_sandbox.execute_script(script_data['code'], context)
            return result
        except ScriptExecutionTimeout:
            raise
        except ScriptExecutionError:
            raise
        except Exception as e:
            raise ScriptExecutionError(f"Script execution failed: {str(e)}")
    
    def execute_script_code(self, code: str, context: Dict[str, Any]) -> Any:
        """
        Execute script code directly (for temporary scripts)
        Args:
            code: Script code to execute
            context: Execution context
        Returns:
            Any: Script execution result
        """
        # Validate security first
        security_errors = ScriptSecurityValidator.validate_script_security(code)
        if security_errors:
            raise SecurityError(f"Script security validation failed: {security_errors}")
        
        # Execute in sandbox
        try:
            result = self.script_sandbox.execute_script(code, context)
            return result
        except Exception as e:
            raise ScriptExecutionError(f"Script execution failed: {str(e)}")
    
    def list_scripts(self, pipeline_id: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        List all scripts (optionally filtered by pipeline)
        Args:
            pipeline_id: Filter by pipeline ID (None for all)
        Returns:
            List of script metadata
        """
        if pipeline_id is None:
            query = """
                SELECT id, name, pipeline_id, created_at, updated_at, version 
                FROM user_scripts 
                WHERE is_active = 1 
                ORDER BY created_at DESC
            """
            params = ()
        else:
            query = """
                SELECT id, name, pipeline_id, created_at, updated_at, version 
                FROM user_scripts 
                WHERE is_active = 1 AND pipeline_id = ?
                ORDER BY created_at DESC
            """
            params = (pipeline_id,)
        
        return self.db.execute_query(query, params)
    
    def update_script(self, script_id: str, name: str, code: str) -> bool:
        """
        Update existing script with security validation
        Args:
            script_id: Script identifier
            name: New script name
            code: New script code
        Returns:
            bool: True if updated successfully
        Raises:
            SecurityError: If script validation fails
        """
        # Validate security
        security_errors = ScriptSecurityValidator.validate_script_security(code)
        if security_errors:
            raise SecurityError(f"Script security validation failed: {security_errors}")
        
        # Calculate new checksum
        checksum = self._calculate_checksum(code)
        
        # Encrypt the new code
        encrypted_code = self.crypto_service.encrypt(code)
        
        query = """
            UPDATE user_scripts 
            SET name = ?, code_encrypted = ?, checksum = ?, updated_at = CURRENT_TIMESTAMP
            WHERE id = ?
        """
        
        params = (name, encrypted_code, checksum, script_id)
        rows_affected = self.db.execute_update(query, params)
        return rows_affected > 0
    
    def delete_script(self, script_id: str) -> bool:
        """
        Delete script (soft delete)
        Args:
            script_id: Script identifier
        Returns:
            bool: True if deleted successfully
        """
        query = """
            UPDATE user_scripts 
            SET is_active = 0, updated_at = CURRENT_TIMESTAMP 
            WHERE id = ?
        """
        rows_affected = self.db.execute_update(query, (script_id,))
        return rows_affected > 0
    
    def get_script_stats(self) -> Dict[str, int]:
        """
        Get statistics about stored scripts
        Returns:
            Dict with script statistics
        """
        query = """
            SELECT 
                COUNT(*) as total_scripts,
                COUNT(CASE WHEN pipeline_id IS NOT NULL THEN 1 END) as assigned_scripts,
                COUNT(CASE WHEN pipeline_id IS NULL THEN 1 END) as unassigned_scripts,
                SUM(LENGTH(code_encrypted)) as total_encrypted_size
            FROM user_scripts 
            WHERE is_active = 1
        """
        result = self.db.execute_query(query)[0] if self.db.execute_query(query) else {}
        
        return {
            "total_scripts": result.get('total_scripts', 0),
            "assigned_scripts": result.get('assigned_scripts', 0),
            "unassigned_scripts": result.get('unassigned_scripts', 0),
            "total_encrypted_size_bytes": result.get('total_encrypted_size', 0)
        }
    
    def _calculate_checksum(self, script_code: str) -> str:
        """
        Calculate SHA-256 checksum of script code
        Args:
            script_code: Script code to hash
        Returns:
            str: SHA-256 checksum (hex)
        """
        return hashlib.sha256(script_code.encode('utf-8')).hexdigest()

# Exception classes with proper implementations
class SecurityError(Exception):
    """
    Raised when script security validation fails
    This occurs when script contains dangerous operations or violates security policies
    """
    def __init__(self, message: str, violation_type: str = "SECURITY_VIOLATION", details: Optional[Dict[str, Any]] = None):
        super().__init__(message)
        self.message = message
        self.violation_type = violation_type
        self.details = details or {}
        self.timestamp = datetime.now()
    
    def __str__(self):
        return f"[{self.violation_type}] {self.message}"
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for logging/serialization"""
        return {
            "type": "SecurityError",
            "message": self.message,
            "violation_type": self.violation_type,
            "details": self.details,
            "timestamp": self.timestamp.isoformat()
        }

class ScriptExecutionError(Exception):
    """
    Raised when script execution fails
    This occurs during runtime when script encounters an error
    """
    def __init__(self, message: str, script_id: Optional[str] = None, 
                 execution_context: Optional[Dict[str, Any]] = None, 
                 original_error: Optional[Exception] = None):
        super().__init__(message)
        self.message = message
        self.script_id = script_id
        self.execution_context = execution_context or {}
        self.original_error = original_error
        self.timestamp = datetime.now()
    
    def __str__(self):
        script_info = f" (Script: {self.script_id})" if self.script_id else ""
        return f"ScriptExecutionError{script_info}: {self.message}"
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for logging/serialization"""
        return {
            "type": "ScriptExecutionError",
            "message": self.message,
            "script_id": self.script_id,
            "execution_context": self.execution_context,
            "original_error_type": type(self.original_error).__name__ if self.original_error else None,
            "original_error_message": str(self.original_error) if self.original_error else None,
            "timestamp": self.timestamp.isoformat()
        }

class ScriptExecutionTimeout(Exception):
    """
    Raised when script execution times out
    This occurs when script runs longer than allowed timeout
    """
    def __init__(self, message: str, timeout_seconds: int = 30, script_id: Optional[str] = None):
        super().__init__(message)
        self.message = message
        self.timeout_seconds = timeout_seconds
        self.script_id = script_id
        self.timestamp = datetime.now()
    
    def __str__(self):
        script_info = f" (Script: {self.script_id})" if self.script_id else ""
        return f"ScriptExecutionTimeout{script_info}: {self.message} (Timeout: {self.timeout_seconds}s)"
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for logging/serialization"""
        return {
            "type": "ScriptExecutionTimeout",
            "message": self.message,
            "timeout_seconds": self.timeout_seconds,
            "script_id": self.script_id,
            "timestamp": self.timestamp.isoformat()
        }

def create_default_script_manager(db: UnifiedDatabase) -> ScriptManager:
    """
    Create default script manager with standard configuration
    Args:
        db: Database instance
    Returns:
        ScriptManager: Configured script manager
    """
    crypto_service = get_crypto_service()
    return ScriptManager(db, crypto_service)

# For backward compatibility
__all__ = [
    'ScriptManager',
    'SecurityError',
    'ScriptExecutionError',
    'ScriptExecutionTimeout',
    'create_default_script_manager'
]