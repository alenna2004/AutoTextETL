from abc import ABC, abstractmethod
from typing import List, Dict, Any, Optional, Union
from .document import Document
from .chunk import Chunk
from .pipeline import PipelineConfig, PipelineRun, PipelineStatus
from .enums import LogLevel  # ← Add missing import

class IDocumentLoader(ABC):
    """
    Document loader interface
    All loaders must implement this interface
    """
    @abstractmethod
    def load(self, source: Union[str, Dict[str, Any]]) -> Document:
        """
        Load document from source
        Args:
            source: File path or source configuration
        Returns:
            Document: Structured document
        """
        pass
    
    @abstractmethod
    def supports_format(self, path: str) -> bool:
        """
        Check if loader supports given format
        Args:
            path: File path
        Returns:
            bool: True if format is supported
        """
        pass
    
    @abstractmethod
    def get_document_metadata(self, path: str) -> Dict[str, Any]:
        """
        Extract document metadata without full loading
        Args:
            path: File path
        Returns:
            Dict[str, Any]: Document metadata
        """
        pass

class IChunkProcessor(ABC):
    """
    Text chunk processor interface
    All splitters and extractors must implement this interface
    """
    @abstractmethod
    def process(self, input_: Union[Document, Chunk, List[Chunk]], 
                config: Optional[Dict[str, Any]] = None) -> List[Chunk]:
        """
        Process input data and return chunks
        Args:
            input_: Input data (document or chunks)
            config: Processor configuration
        Returns:
            List[Chunk]: Processed chunks
        """
        pass
    
    @abstractmethod
    def get_required_context(self) -> List[str]:
        """
        Return required metadata keys
        Returns:
            List[str]: Metadata keys list
        """
        pass

class IDbExporter(ABC):
    """
    Database exporter interface
    All exporters must implement this interface
    """
    @abstractmethod
    def connect(self, config: Dict[str, Any]):
        """
        Establish database connection
        Args:
            config: Connection configuration
        """
        pass
    
    @abstractmethod
    def batch_insert(self, chunks: List[Chunk], table_or_collection_name: str = "chunks"):
        """
        Batch insert chunks to database
        Args:
            chunks: List of chunks to insert
            table_or_collection_name: Target table/collection name
        """
        pass
    
    @abstractmethod
    def export_run_metadata(self, run: PipelineRun):
        """
        Export pipeline run metadata
        Args:
            run: Run instance
        """
        pass
    
    @abstractmethod
    def close(self):
        """Close database connection"""
        pass

class IPipelineExecutor(ABC):
    """
    Pipeline executor interface
    """
    @abstractmethod
    def execute(self, config: PipelineConfig, document_paths: List[str]) -> PipelineRun:
        """
        Execute pipeline for list of documents
        Args:
            config: Pipeline configuration
            document_paths: List of document paths
        Returns:
            PipelineRun: Execution result
        """
        pass
    
    @abstractmethod
    def validate_pipeline(self, config: PipelineConfig) -> bool:
        """
        Validate pipeline configuration
        Args:
            config: Pipeline configuration
        Returns:
            bool: True if valid
        """
        pass

class ITaskScheduler(ABC):
    """
    Task scheduler interface
    """
    @abstractmethod
    def schedule_pipeline(self, config: PipelineConfig, cron_expression: str):
        """
        Schedule pipeline execution
        Args:
            config: Pipeline configuration
            cron_expression: Cron expression for scheduling
        """
        pass
    
    @abstractmethod
    def cancel_schedule(self, pipeline_id: str):
        """
        Cancel scheduled pipeline
        Args:
            pipeline_id: Pipeline identifier
        """
        pass
    
    @abstractmethod
    def get_scheduled_pipelines(self) -> List[Dict[str, Any]]:
        """
        Get list of scheduled pipelines
        Returns:
            List[Dict[str, Any]]: Scheduled pipeline information
        """
        pass

class IConfigManager(ABC):
    """
    Configuration manager interface
    """
    @abstractmethod
    def save_pipeline_config(self, config: PipelineConfig):
        """
        Save pipeline configuration
        Args:
            config: Pipeline configuration to save
        """
        pass
    
    @abstractmethod
    def load_pipeline_config(self, pipeline_id: str) -> PipelineConfig:
        """
        Load pipeline configuration
        Args:
            pipeline_id: Pipeline identifier
        Returns:
            PipelineConfig: Loaded configuration
        """
        pass
    
    @abstractmethod
    def delete_pipeline_config(self, pipeline_id: str):
        """
        Delete pipeline configuration
        Args:
            pipeline_id: Pipeline identifier to delete
        """
        pass
    
    @abstractmethod
    def list_pipeline_configs(self) -> List[PipelineConfig]:
        """
        List all pipeline configurations
        Returns:
            List[PipelineConfig]: All configurations
        """
        pass

class IScriptManager(ABC):
    """
    Script manager interface
    """
    @abstractmethod
    def save_script(self, name: str, code: str, pipeline_id: Optional[str] = None) -> str:
        """
        Save user script
        Args:
            name: Script name
            code: Script code
            pipeline_id: Associated pipeline ID
        Returns:
            str: Script ID
        """
        pass
    
    @abstractmethod
    def load_script(self, script_id: str) -> str:
        """
        Load user script
        Args:
            script_id: Script identifier
        Returns:
            str: Script code
        """
        pass
    
    @abstractmethod
    def execute_script(self, script_id: str, context: Dict[str, Any]) -> Any:
        """
        Execute user script in secure context
        Args:
            script_id: Script identifier
            context: Execution context
        Returns:
            Any: Script execution result
        """
        pass

class ILogger(ABC):
    """
    Logger interface
    """
    @abstractmethod
    def log_pipeline_run(self, run: PipelineRun):
        """
        Log pipeline execution
        Args:
            run: Pipeline run information
        """
        pass
    
    @abstractmethod
    def log_message(self, level: LogLevel, message: str, 
                   pipeline_id: Optional[str] = None,
                   pipeline_run_id: Optional[str] = None,
                   document_path: Optional[str] = None,
                   extra_data: Optional[Dict[str, Any]] = None):
        """
        Log general message
        Args:
            level: Log level
            message: Log message
            pipeline_id: Associated pipeline ID
            pipeline_run_id: Associated run ID
            document_path: Associated document path
            extra_data: Additional context
        """
        pass
    
    @abstractmethod
    def get_run_history(self, pipeline_id: str) -> List[PipelineRun]:
        """
        Get pipeline execution history
        Args:
            pipeline_id: Pipeline identifier
        Returns:
            List[PipelineRun]: Execution history
        """
        pass

class IDataProcessor(ABC):
    """
    Data processor interface for complex operations
    """
    @abstractmethod
    def process_chunks(self, chunks: List[Chunk], operation: str, 
                      config: Optional[Dict[str, Any]] = None) -> List[Chunk]:
        """
        Process chunks with specific operation
        Args:
            chunks: Input chunks
            operation: Operation type
            config: Operation configuration
        Returns:
            List[Chunk]: Processed chunks
        """
        pass
    
    @abstractmethod
    def validate_operation(self, operation: str, config: Dict[str, Any]) -> bool:
        """
        Validate operation configuration
        Args:
            operation: Operation type
            config: Configuration to validate
        Returns:
            bool: True if valid
        """
        pass

class IFileHandler(ABC):
    """
    File handler interface for file operations
    """
    @abstractmethod
    def scan_directory(self, path: str, extensions: List[str]) -> List[str]:
        """
        Scan directory for files with specific extensions
        Args:
            path: Directory path
            extensions: File extensions to look for
        Returns:
            List[str]: File paths
        """
        pass
    
    @abstractmethod
    def validate_file(self, file_path: str) -> bool:
        """
        Validate file format and accessibility
        Args:
            file_path: File path to validate
        Returns:
            bool: True if valid
        """
        pass
    
    @abstractmethod
    def get_file_size(self, file_path: str) -> int:
        """
        Get file size in bytes
        Args:
            file_path: File path
        Returns:
            int: File size in bytes
        """
        pass