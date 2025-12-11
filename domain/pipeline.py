from dataclasses import dataclass, field
from typing import List, Dict, Any, Optional
from enum import Enum
from datetime import datetime
import uuid

class PipelineStatus(Enum):
    """Pipeline execution status"""
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    PARTIAL_SUCCESS = "partial_success"
    PAUSED = "paused"
    CANCELLED = "cancelled"

class StepType(Enum):
    """Pipeline step types"""
    DOCUMENT_LOADER = "document_loader"
    LINE_SPLITTER = "line_splitter"
    DELIMITER_SPLITTER = "delimiter_splitter"
    PARAGRAPH_SPLITTER = "paragraph_splitter"
    SENTENCE_SPLITTER = "sentence_splitter"
    REGEX_EXTRACTOR = "regex_extractor"
    USER_SCRIPT = "user_script"
    DB_EXPORTER = "db_exporter"
    FILE_EXPORTER = "file_exporter"
    JSON_EXPORTER = "json_exporter"
    METADATA_PROPAGATOR = "metadata_propagator"

@dataclass
class PipelineStepConfig:
    """
    Pipeline step configuration
    """
    type: StepType  # Non-default argument first
    id: str = field(default_factory=lambda: f"step_{str(uuid.uuid4())[:8]}")
    name: str = ""
    params: Dict[str, Any] = field(default_factory=dict)
    input_step_id: Optional[str] = None
    depends_on: List[str] = field(default_factory=list)
    
    def __post_init__(self):
        if not self.name:
            self.name = f"{self.type.value.replace('_', ' ').title()} Step"
        if self.input_step_id and self.depends_on:
            raise ValueError("Cannot specify both input_step_id and depends_on")

@dataclass
class PipelineConfig:
    """
    ETL pipeline configuration
    """
    name: str
    steps: List[PipelineStepConfig] = field(default_factory=list)
    id: str = field(default_factory=lambda: f"pipeline_{str(uuid.uuid4())[:8]}")
    description: str = ""
    schedule: str = ""  # cron format: "0 2 * * *"
    source_config: Dict[str, Any] = field(default_factory=dict)
    target_config: Dict[str, Any] = field(default_factory=dict)
    created_at: datetime = field(default_factory=datetime.now)
    updated_at: datetime = field(default_factory=datetime.now)
    version: int = 1
    
    def validate(self):
        """Validate configuration"""
        if not self.steps:
            raise ValueError("Pipeline must contain at least one step")
        
        if len(self.steps) > 1 and not any(step.input_step_id for step in self.steps[1:]):
            raise ValueError("Steps must be connected via input_step_id or depends_on")
        
        # Validate cron schedule
        if self.schedule and not self._is_valid_cron(self.schedule):
            raise ValueError(f"Invalid cron schedule format: {self.schedule}")
    
    def _is_valid_cron(self, cron_expr: str) -> bool:
        """Validate cron expression (simplified version)"""
        parts = cron_expr.strip().split()
        if len(parts) != 5:
            return False
        
        # Validate minutes (0-59)
        if not self._is_valid_cron_part(parts[0], 0, 59):
            return False
        
        # Validate hours (0-23)
        if not self._is_valid_cron_part(parts[1], 0, 23):
            return False
        
        # Validate day of month (1-31)
        if not self._is_valid_cron_part(parts[2], 1, 31):
            return False
        
        # Validate month (1-12)
        if not self._is_valid_cron_part(parts[3], 1, 12):
            return False
        
        # Validate day of week (0-7, where 0 and 7 = Sunday)
        if not self._is_valid_cron_part(parts[4], 0, 7):
            return False
        
        return True
    
    def _is_valid_cron_part(self, part: str, min_val: int, max_val: int) -> bool:
        """Validate cron expression part"""
        if part == "*":
            return True
        
        # Validate ranges (e.g., 1-5)
        if "-" in part:
            try:
                start, end = map(int, part.split("-"))
                return min_val <= start <= end <= max_val
            except ValueError:
                return False
        
        # Validate lists (e.g., 1,3,5)
        if "," in part:
            try:
                values = [int(v) for v in part.split(",")]
                return all(min_val <= v <= max_val for v in values)
            except ValueError:
                return False
        
        # Validate steps (e.g., */2)
        if "/" in part:
            base, step = part.split("/")
            if base != "*" and not self._is_valid_cron_part(base, min_val, max_val):
                return False
            try:
                step_val = int(step)
                return step_val > 0
            except ValueError:
                return False
        
        # Validate single value
        try:
            val = int(part)
            return min_val <= val <= max_val
        except ValueError:
            return False
    
    def to_dict(self) -> Dict[str, Any]:
        """Serialize configuration to dictionary"""
        return {
            "id": self.id,
            "name": self.name,
            "description": self.description,
            "steps": [{
                "id": step.id,
                "type": step.type.value,
                "name": step.name,
                "params": step.params,
                "input_step_id": step.input_step_id,
                "depends_on": step.depends_on
            } for step in self.steps],
            "schedule": self.schedule,
            "source_config": self.source_config,
            "target_config": self.target_config,
            "created_at": self.created_at.isoformat(),
            "updated_at": self.updated_at.isoformat(),
            "version": self.version
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'PipelineConfig':
        """Create configuration from dictionary"""
        config = cls(
            name=data.get("name", "New Pipeline"),
            description=data.get("description", ""),
            schedule=data.get("schedule", ""),
            source_config=data.get("source_config", {}),
            target_config=data.get("target_config", {}),
            version=data.get("version", 1)
        )
        
        # Restore ID if exists
        if "id" in data:
            config.id = data["id"]
        
        # Restore steps
        for step_data in data.get("steps", []):
            step = PipelineStepConfig(
                type=StepType(step_data["type"]),
                id=step_data.get("id", f"step_{str(uuid.uuid4())[:8]}"),
                name=step_data.get("name", ""),
                params=step_data.get("params", {}),
                input_step_id=step_data.get("input_step_id"),
                depends_on=step_data.get("depends_on", [])
            )
            config.steps.append(step)
        
        return config

@dataclass
class PipelineRun:
    """
    Pipeline execution instance
    """
    id: str = field(default_factory=lambda: f"run_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{str(uuid.uuid4())[:8]}")
    pipeline_id: str = ""
    start_time: datetime = field(default_factory=datetime.now)
    end_time: Optional[datetime] = None
    status: PipelineStatus = PipelineStatus.PENDING
    document_paths: List[str] = field(default_factory=list)  # ✅ FIXED: Correct parameter name
    processed_count: int = 0
    success_count: int = 0
    error_count: int = 0
    errors: List[Dict[str, Any]] = field(default_factory=list)
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    def complete(self, status: Optional[PipelineStatus] = None):
        """Complete pipeline execution"""
        self.end_time = datetime.now()
        if status:
            self.status = status
    
    def add_error(self, document_path: str, error: Exception, traceback: str = ""):
        """Add error to execution log"""
        self.error_count += 1
        self.errors.append({
            "timestamp": datetime.now().isoformat(),
            "document_path": document_path,
            "error_type": type(error).__name__,
            "error_message": str(error),
            "traceback": traceback
        })
    
    def to_dict(self) -> Dict[str, Any]:
        """Serialize run to dictionary"""
        return {
            "id": self.id,
            "pipeline_id": self.pipeline_id,
            "start_time": self.start_time.isoformat(),
            "end_time": self.end_time.isoformat() if self.end_time else None,
            "status": self.status.value,
            "document_paths": self.document_paths,
            "processed_count": self.processed_count,
            "success_count": self.success_count,
            "error_count": self.error_count,
            "error_count": len(self.errors),  # Fixed: duplicate field name
            "metadata": self.meta
        }