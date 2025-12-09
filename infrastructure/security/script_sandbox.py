#!/usr/bin/env python3
"""
Script Sandbox - Secure execution environment for user scripts
Implements isolation and security restrictions
"""

import ast
import sys
import subprocess
import threading
from typing import Dict, Any, Optional, Callable, List
from io import StringIO
import contextlib
import signal
import time
import tempfile
import os
import pickle
import marshal
from multiprocessing import Process, Queue, TimeoutError as MPTimeoutError
from datetime import datetime
import math
import json
import datetime
import collections
import string
import re


# Define the missing exception classes
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
    Raised when script execution fails during runtime
    This occurs when script encounters an error during execution
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
    Raised when script execution exceeds timeout limit
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

class ScriptSecurityValidator:
    """
    Validates script code for security compliance
    """
    
    @staticmethod
    def validate_script_security(script_code: str) -> List[str]:
        """
        Validate script code for security compliance
        Returns list of security violations
        """
        errors = []
        
        # Parse AST to analyze code structure
        try:
            tree = ast.parse(script_code)
        except SyntaxError as e:
            errors.append(f"Syntax error: {e}")
            return errors
        
        # Walk AST and check for dangerous patterns
        visitor = SecurityASTVisitor()
        visitor.visit(tree)
        
        errors.extend(visitor.security_violations)
        
        # Additional checks
        lines = script_code.split('\n')
        for i, line in enumerate(lines, 1):
            stripped = line.strip()
            
            # Check for dangerous imports
            if stripped.startswith(('import ', 'from ')):
                dangerous_imports = [
                    'os', 'subprocess', 'sys', 'importlib', 'exec', 'eval', 'compile',
                    'open', 'file', 'socket', 'urllib', 'requests', 'pickle', 'marshal',
                    'shutil', 'glob', 'ftplib', 'smtplib', 'telnetlib', 'xmlrpc',
                    'ctypes', 'multiprocessing', 'threading', 'concurrent', 'asyncio',
                    'platform', 'resource', 'signal', 'faulthandler', 'pdb', 'code',
                    'trace', 'profile', 'cProfile', 'pstats', 'dis', 'inspect', 'traceback'
                ]
                
                for imp in dangerous_imports:
                    if f'import {imp}' in stripped.lower() or f'from {imp}' in stripped.lower():
                        errors.append(f"Line {i}: Dangerous import detected: {imp}")
            
            # Check for dangerous function calls
            dangerous_calls = [
                'exec(', 'eval(', 'compile(', 'execfile(', 'open(', 'file(',
                'input(', 'raw_input(', 'exec(', 'eval(', 'compile(',
                'subprocess.', 'os.', 'sys.', 'importlib.'
            ]
            
            line_lower = stripped.lower()
            for call in dangerous_calls:
                if call in line_lower:
                    errors.append(f"Line {i}: Dangerous call detected: {stripped.strip()}")
        
        return errors

class SecurityASTVisitor(ast.NodeVisitor):
    """
    AST visitor to detect security violations
    """
    
    def __init__(self):
        self.security_violations = []
    
    def visit_Import(self, node):
        """Check import statements"""
        for alias in node.names:
            if self._is_dangerous_import(alias.name):
                self.security_violations.append(f"Dangerous import: {alias.name}")
        self.generic_visit(node)
    
    def visit_ImportFrom(self, node):
        """Check from-import statements"""
        if self._is_dangerous_import(node.module):
            self.security_violations.append(f"Dangerous import: from {node.module}")
        self.generic_visit(node)
    
    def visit_Call(self, node):
        """Check function calls"""
        if isinstance(node.func, ast.Name):
            if self._is_dangerous_function(node.func.id):
                self.security_violations.append(f"Dangerous function call: {node.func.id}")
        elif isinstance(node.func, ast.Attribute):
            if self._is_dangerous_attribute_call(node.func.attr):
                self.security_violations.append(f"Dangerous attribute call: {node.func.attr}")
        self.generic_visit(node)
    
    def visit_Attribute(self, node):
        """Check attribute access"""
        if isinstance(node.ctx, ast.Load):  # Reading attribute
            if self._is_dangerous_attribute_access(node.attr):
                self.security_violations.append(f"Dangerous attribute access: {node.attr}")
        self.generic_visit(node)
    
    def _is_dangerous_import(self, module_name: str) -> bool:
        """Check if import is dangerous"""
        if not module_name:
            return False
        
        dangerous_modules = {
            'os', 'subprocess', 'sys', 'importlib', 'socket', 'urllib', 'requests',
            'pickle', 'marshal', 'shutil', 'glob', 'ftplib', 'smtplib', 'telnetlib',
            'xmlrpc', 'ctypes', 'multiprocessing', 'threading', 'concurrent', 'asyncio',
            'platform', 'resource', 'signal', 'faulthandler', 'pdb', 'code',
            'trace', 'profile', 'cProfile', 'pstats', 'dis', 'inspect', 'traceback'
        }
        return module_name.split('.')[0] in dangerous_modules
    
    def _is_dangerous_function(self, func_name: str) -> bool:
        """Check if function call is dangerous"""
        dangerous_functions = {
            'exec', 'eval', 'compile', 'execfile', 'open', 'file', 'input', 'raw_input',
            'getattr', 'setattr', 'delattr', 'hasattr', 'globals', 'locals', 'vars',
            'help', 'dir', 'type', 'id', 'memoryview', 'bytearray'
        }
        return func_name in dangerous_functions
    
    def _is_dangerous_attribute_call(self, attr_name: str) -> bool:
        """Check if attribute call is dangerous"""
        dangerous_attributes = {
            '__import__', '__build_class__', '__loader__', '__spec__',
            'system', 'popen', 'exec', 'eval', 'compile', 'open', 'read', 'write',
            'remove', 'unlink', 'rmdir', 'makedirs', 'mkdir', 'chmod', 'chown'
        }
        return attr_name in dangerous_attributes
    
    def _is_dangerous_attribute_access(self, attr_name: str) -> bool:
        """Check if attribute access is dangerous"""
        dangerous_attributes = {
            '__import__', '__build_class__', '__loader__', '__spec__',
            '__dict__', '__class__', '__bases__', '__mro__', '__subclasses__',
            '__globals__', '__code__', '__closure__', '__func__',
            'system', 'popen', 'exec', 'eval', 'compile', 'open', 'read', 'write'
        }
        return attr_name in dangerous_attributes

class ScriptSandbox:
    """
    Secure execution environment for user scripts
    """
    
    def __init__(self, timeout: int = 30, memory_limit_mb: int = 100):
        self.timeout = timeout
        self.memory_limit_mb = memory_limit_mb
        self.secure_builtins = self._create_secure_builtins()
    
    def _create_secure_builtins(self) -> dict:
        """
        Create secure built-in functions for sandbox
        """
        
        return {
            # Basic functions
            'len': len,
            'str': str,
            'int': int,
            'float': float,
            'bool': bool,
            'list': list,
            'dict': dict,
            'tuple': tuple,
            'set': set,
            'frozenset': frozenset,
            'range': range,
            'enumerate': enumerate,
            'zip': zip,
            'map': map,
            'filter': filter,
            'sum': sum,
            'min': min,
            'max': max,
            'abs': abs,
            'round': round,
            'sorted': sorted,
            'reversed': reversed,
            'any': any,
            'all': all,
            'pow': pow,
            'divmod': divmod,
            'ord': ord,
            'chr': chr,
            'hex': hex,
            'oct': oct,
            'bin': bin,
            'ascii': ascii,
            'repr': repr,
            'format': format,
            
            # Mathematical functions
            'math': {
                'pi': 3.141592653589793,
                'e': 2.718281828459045,
                'sqrt': lambda x: x ** 0.5,
                'ceil': lambda x: int(x) + (1 if x > int(x) else 0),
                'floor': int,
                'abs': abs,
                'exp': lambda x: 2.718281828459045 ** x,
                'log': lambda x: x.__log__() if hasattr(x, '__log__') else None,
                'sin': lambda x: __import__('math').sin(x),
                'cos': lambda x: __import__('math').cos(x),
                'tan': lambda x: __import__('math').tan(x),
            },
            
            # Regular expressions
            're': {
                'search': re.search,
                'match': re.match,
                'findall': re.findall,
                'sub': re.sub,
                'split': re.split,
                'compile': re.compile
            },
            
            # JSON operations
            'json': {
                'loads': json.loads,
                'dumps': json.dumps,
                'load': json.load,
                'dump': json.dump
            },
            
            # Date/time operations
            'datetime': {
                'datetime': datetime.datetime,
                'date': datetime.date,
                'time': datetime.time,
                'timedelta': datetime.timedelta
            },
            
            # Collections
            'collections': {
                'Counter': collections.Counter,
                'defaultdict': collections.defaultdict,
                'OrderedDict': collections.OrderedDict,
                'namedtuple': collections.namedtuple
            },
            
            # String operations
            'string': {
                'ascii_letters': string.ascii_letters,
                'ascii_lowercase': string.ascii_lowercase,
                'ascii_uppercase': string.ascii_uppercase,
                'digits': string.digits,
                'punctuation': string.punctuation
            },
            
            # Exceptions (safe to reference)
            'Exception': Exception,
            'ValueError': ValueError,
            'TypeError': TypeError,
            'KeyError': KeyError,
            'IndexError': IndexError,
            'AttributeError': AttributeError,
            'RuntimeError': RuntimeError,
            'StopIteration': StopIteration,
            'StopAsyncIteration': StopIteration,
            'NotImplemented': NotImplemented,
            'Ellipsis': Ellipsis,
            'False': False,
            'True': True,
            'None': None
        }
    
    def execute_script(self, script_code: str, context: Dict[str, Any]) -> Any:
        """
        Execute script in secure sandbox
        Args:
            script_code: Python code to execute
            context: Input context for the script
        Returns:
            Any: Result of script execution
        Raises:
            SecurityError: If script fails security validation
            ScriptExecutionError: If script execution fails
            ScriptExecutionTimeout: If script times out
        """
        # Validate security first
        security_errors = ScriptSecurityValidator.validate_script_security(script_code)
        if security_errors:
            raise SecurityError(f"Script security validation failed: {security_errors}")
        
        # Execute in subprocess for complete isolation
        return self._execute_in_subprocess(script_code, context)
    
    def _execute_in_subprocess(self, script_code: str, context: Dict[str, Any]) -> Any:
        """
        Execute script in separate process for complete isolation
        """
        # Create queues for communication
        result_queue = Queue()
        error_queue = Queue()
        
        def execute_function(code, ctx, result_q, error_q):
            try:
                # Capture stdout
                old_stdout = sys.stdout
                captured_output = StringIO()
                sys.stdout = captured_output
                
                try:
                    # Create secure globals and locals
                    secure_globals = {
                        '__builtins__': self.secure_builtins
                    }
                    secure_locals = ctx.copy()
                    
                    # Execute script
                    exec(code, secure_globals, secure_locals)
                    
                    # Get result if defined
                    result = secure_locals.get('result')
                    
                    # Capture output
                    output = captured_output.getvalue()
                    if output:
                        if result is None:
                            result = output
                        else:
                            result = {"output": output, "result": result}
                    
                    result_q.put(("success", result))
                finally:
                    # Restore stdout
                    sys.stdout = old_stdout
            except Exception as e:
                error_q.put(("error", str(e)))
        
        # Start process with timeout protection
        process = Process(
            target=execute_function,
            args=(script_code, context, result_queue, error_queue)
        )
        
        try:
            process.start()
            process.join(timeout=self.timeout)
            
            if process.is_alive():
                # Timeout occurred
                process.terminate()
                process.join()
                raise ScriptExecutionTimeout(
                    f"Script execution timed out after {self.timeout} seconds",
                    timeout_seconds=self.timeout
                )
            
            if not error_queue.empty():
                error_type, error_msg = error_queue.get()
                raise ScriptExecutionError(f"Script execution failed: {error_msg}")
            
            if not result_queue.empty():
                result_type, result = result_queue.get()
                if result_type == "success":
                    return result
                else:
                    raise ScriptExecutionError(f"Script execution failed: {result}")
            
            raise ScriptExecutionError("Script execution failed: no result or error returned")
        
        except ScriptExecutionTimeout:
            raise
        except Exception as e:
            # Ensure process is terminated
            if process.is_alive():
                process.terminate()
                process.join(timeout=2)
                if process.is_alive():
                    process.kill()
            raise ScriptExecutionError(f"Script execution failed: {str(e)}")
    
    def execute_script_with_timeout(self, script_code: str, context: Dict[str, Any], timeout: int) -> Any:
        """
        Execute script with custom timeout
        Args:
            script_code: Python code to execute
            context: Input context
            timeout: Timeout in seconds
        Returns:
            Any: Script execution result
        """
        old_timeout = self.timeout
        try:
            self.timeout = timeout
            return self.execute_script(script_code, context)
        finally:
            self.timeout = old_timeout

# For backward compatibility
__all__ = [
    'ScriptSandbox',
    'ScriptSecurityValidator',
    'SecurityError',
    'ScriptExecutionError',
    'ScriptExecutionTimeout'
]