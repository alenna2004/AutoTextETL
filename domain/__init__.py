"""
Domain Layer - Pure business entities and interfaces
All classes in this layer do not depend on external libraries and frameworks
"""

# Enums
from .enums import LogLevel

# Document structure
from .document import (
    DocumentFormat,
    Section,
    Page,
    Document
)

# Text chunks
from .chunk import (
    ChunkType,
    Metadata,
    Chunk
)

# Pipelines
from .pipeline import (
    PipelineStatus,
    StepType,
    PipelineStepConfig,
    PipelineConfig,
    PipelineRun
)

# Script context
from .script_context import (
    UserScriptContext
)

# Interfaces
from .interfaces import (
    IDocumentLoader,
    IChunkProcessor,
    IDbExporter,
    IPipelineExecutor,
    ITaskScheduler,
    IConfigManager,
    IScriptManager,
    ILogger,
    IDataProcessor,
    IFileHandler
)

__all__ = [
    # Enums
    'LogLevel',
    
    # Document
    'DocumentFormat', 'Section', 'Page', 'Document',
    
    # Chunk
    'ChunkType', 'Metadata', 'Chunk',
    
    # Pipeline
    'PipelineStatus', 'StepType', 'PipelineStepConfig', 'PipelineConfig', 'PipelineRun',
    
    # Script
    'UserScriptContext',
    
    # Interfaces
    'IDocumentLoader', 'IChunkProcessor', 'IDbExporter', 'IPipelineExecutor',
    'ITaskScheduler', 'IConfigManager', 'IScriptManager', 'ILogger',
    'IDataProcessor', 'IFileHandler'
]