from dataclasses import dataclass, field
from typing import Dict, Any, List, Optional, Callable
from uuid import uuid4
from datetime import datetime  
from .chunk import Chunk, Metadata, ChunkType

@dataclass
class UserScriptContext:
    """
    Context for user script execution
    Attributes:
        chunk: Current chunk being processed
        pipeline_run_id: Pipeline run ID
        pipeline_id: Pipeline configuration ID
        script_id: Script ID
        emit: Function to send results
        storage: Temporary storage for script
        meta: Additional metadata
    """
    chunk: Chunk
    pipeline_run_id: str
    pipeline_id: str
    script_id: str
    emit: Callable[[Dict[str, Any]], None]
    storage: Dict[str, Any] = field(default_factory=dict)
    meta: Dict[str, Any] = field(default_factory=dict)
    
    def log(self, message: str, level: str = "INFO"):
        """Log message from script"""
        self.storage.setdefault("logs", []).append({
            "timestamp": datetime.now().isoformat(),
            "level": level,
            "message": message
        })
    
    def get_storage_value(self, key: str, default: Any = None) -> Any:
        """Get value from temporary storage"""
        return self.storage.get(key, default)
    
    def set_storage_value(self, key: str, value: Any):
        """Set value in temporary storage"""
        self.storage[key] = value
    
    def get_global_storage(self, key: str) -> Any:
        """Get value from global storage (for all documents)"""
        # In real implementation this would access external storage
        return self.meta.get(f"global_{key}")
    
    def set_global_storage(self, key: str, value: Any):
        """Set value in global storage"""
        self.meta[f"global_{key}"] = value