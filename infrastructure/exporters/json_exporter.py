#!/usr/bin/env python3
"""
JSON Exporter - Export chunks to JSON files
"""

import json
import os
from pathlib import Path
from typing import List, Dict, Any, Optional
from domain.interfaces import IDbExporter
from domain.document import Document, Section
from domain.chunk import Chunk, Metadata, ChunkType
from domain.pipeline import PipelineRun, PipelineStatus
from datetime import datetime, timezone
import gzip

class JsonExporter(IDbExporter):
    """
    JSON file exporter implementation
    Supports both regular JSON and compressed JSON.gz formats
    """
    
    def __init__(self, output_dir: Optional[str] = None, compress: bool = False):
        self.output_dir = output_dir or "./output"
        self.compress = compress
        self.is_connected = True  # Always "connected" since it's file-based
        self.connection_config = {"output_dir": self.output_dir, "compress": self.compress}
        self._connected_at = datetime.now()
        
        # Create output directory if it doesn't exist
        Path(self.output_dir).mkdir(parents=True, exist_ok=True)
    
    def connect(self, config: Dict[str, Any]):
        """
        Configure JSON export settings
        Args:
            config: Configuration with 'output_dir' and 'compress' options
        """
        self.output_dir = config.get("output_dir", self.output_dir)
        self.compress = config.get("compress", self.compress)
        self.connection_config = config
        self._connected_at = datetime.now()
        
        # Create output directory
        Path(self.output_dir).mkdir(parents=True, exist_ok=True)
    
    def batch_insert(self, chunks: List[Chunk], file_name: str = "chunks.json"):
        """
        Export chunks to JSON file
        Args:
            chunks: List of chunks to export
            file_name: Output file name
        """
        if not chunks:
            return  # Nothing to export
        
        # Prepare data for export
        chunk_data = []
        for chunk in chunks:
            chunk_dict = {
                "id": chunk.id,
                "text": chunk.text,
                "meta": {
                    "document_id": chunk.meta.document_id,
                    "page_num": chunk.meta.page_num,
                    "section_id": chunk.meta.section_id,
                    "section_title": chunk.meta.section_title,
                    "section_level": chunk.meta.section_level,
                    "chunk_type": chunk.meta.chunk_type.value if hasattr(chunk.meta.chunk_type, 'value') else str(chunk.meta.chunk_type),
                    "pipeline_run_id": chunk.meta.pipeline_run_id,
                    "source_type": chunk.meta.source_type,
                    "line_num": chunk.meta.line_num
                },
                "extraction_results": chunk.extraction_results,
                "exported_at": datetime.now(timezone.utc).isoformat()
            }
            chunk_data.append(chunk_dict)
        
        # Export to file
        file_path = os.path.join(self.output_dir, file_name)
        self._write_json_data(chunk_data, file_path)
    
    def export_run_metadata(self, run: PipelineRun):
        """
        Export pipeline run metadata to JSON file
        Args:
            run: Pipeline run instance
        """
        run_data = {
            "id": run.id,
            "pipeline_id": run.pipeline_id,
            "start_time": run.start_time.isoformat() if run.start_time else None,
            "end_time": run.end_time.isoformat() if run.end_time else None,
            "status": run.status.value if hasattr(run.status, 'value') else str(run.status),
            "processed_count": run.processed_count,
            "success_count": run.success_count,
            "error_count": run.error_count,
            "errors": run.errors,
            "metadata": run.metadata,
            "exported_at": datetime.now(timezone.utc).isoformat()
        }
        
        # Create filename based on run ID and timestamp
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        file_name = f"run_{run.id}_{timestamp}.json"
        file_path = os.path.join(self.output_dir, file_name)
        
        self._write_json_data(run_data, file_path)
    
    def _write_json_data(self, data: Any, file_path: str):
        """
        Write JSON data to file (with optional compression)
        Args:
            data: Data to write
            file_path: Output file path
        """
        if self.compress:
            # Write compressed JSON
            compressed_path = file_path + ".gz"
            with gzip.open(compressed_path, 'wt', encoding='utf-8') as f:
                json.dump(data, f, indent=2, ensure_ascii=False, default=self._json_serializer)
        else:
            # Write regular JSON
            with open(file_path, 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=2, ensure_ascii=False, default=self._json_serializer)
    
    def _json_serializer(self, obj):
        """
        JSON serializer for datetime and other non-serializable objects
        """
        if isinstance(obj, datetime):
            return obj.isoformat()
        elif isinstance(obj, (Chunk, Metadata, Section, PipelineRun)):
            return obj.__dict__  # Convert domain objects to dict
        raise TypeError(f"Object of type {type(obj)} is not JSON serializable")
    
    def close(self):
        """
        Close JSON exporter (cleanup if needed)
        """
        # No cleanup needed for file-based export
        pass
    
    def get_connection_status(self) -> Dict[str, Any]:
        """
        Get JSON exporter status
        """
        return {
            "is_connected": True,
            "output_dir": self.output_dir,
            "compress": self.compress,
            "connected_at": self._connected_at.isoformat(),
            "file_count": len(list(Path(self.output_dir).glob("*.json*"))),
            "total_size": self._get_output_directory_size()
        }
    
    def _get_output_directory_size(self) -> int:
        """
        Get total size of output directory in bytes
        """
        total_size = 0
        for file_path in Path(self.output_dir).rglob("*"):
            if file_path.is_file():
                total_size += file_path.stat().st_size
        return total_size
    
    def export_to_stream(self, chunks: List[Chunk], stream) -> int:
        """
        Export chunks to JSON stream (for streaming applications)
        Args:
            chunks: List of chunks to export
            stream: Output stream (file object or similar)
        Returns:
            int: Number of chunks exported
        """
        chunk_data = []
        for chunk in chunks:
            chunk_dict = {
                "id": chunk.id,
                "text": chunk.text,
                "meta": chunk.meta.__dict__,
                "extraction_results": chunk.extraction_results,
                "exported_at": datetime.now(timezone.utc).isoformat()
            }
            chunk_data.append(chunk_dict)
        
        json.dump(chunk_data, stream, indent=2, ensure_ascii=False, default=self._json_serializer)
        return len(chunks)
    
    def export_batch_to_separate_files(self, chunks: List[Chunk], 
                                     base_filename: str = "batch", 
                                     batch_size: int = 1000):
        """
        Export chunks in batches to separate files
        Args:
            chunks: List of chunks to export
            base_filename: Base name for output files
            batch_size: Number of chunks per file
        Returns:
            List of created file paths
        """
        file_paths = []
        
        for i in range(0, len(chunks), batch_size):
            batch = chunks[i:i + batch_size]
            batch_num = (i // batch_size) + 1
            
            file_name = f"{base_filename}_batch_{batch_num:03d}.json"
            if self.compress:
                file_name += ".gz"
            
            file_path = os.path.join(self.output_dir, file_name)
            
            batch_data = []
            for chunk in batch:
                chunk_dict = {
                    "id": chunk.id,
                    "text": chunk.text,
                    "meta": chunk.meta.__dict__,
                    "extraction_results": chunk.extraction_results,
                    "exported_at": datetime.now(timezone.utc).isoformat()
                }
                batch_data.append(chunk_dict)
            
            self._write_json_data(batch_data, file_path)
            file_paths.append(file_path)
        
        return file_paths
    
    def export_with_custom_format(self, chunks: List[Chunk], 
                                file_name: str, 
                                format_function: callable):
        """
        Export chunks with custom JSON format
        Args:
            chunks: List of chunks to export
            file_name: Output file name
            format_function: Function to transform chunk to desired format
        """
        formatted_data = []
        for chunk in chunks:
            formatted_item = format_function(chunk)
            formatted_data.append(formatted_item)
        
        file_path = os.path.join(self.output_dir, file_name)
        self._write_json_data(formatted_data, file_path)
    
    def get_exported_files(self) -> List[Dict[str, Any]]:
        """
        Get list of exported JSON files
        Returns:
            List of file information
        """
        files = []
        for file_path in Path(self.output_dir).glob("*.json*"):
            stat = file_path.stat()
            files.append({
                "name": file_path.name,
                "size": stat.st_size,
                "modified": datetime.fromtimestamp(stat.st_mtime).isoformat(),
                "compressed": file_path.suffix == ".gz",
                "path": str(file_path)
            })
        
        return sorted(files, key=lambda f: f["modified"], reverse=True)
    
    def clear_output_directory(self):
        """
        Clear all exported files from output directory
        """
        for file_path in Path(self.output_dir).glob("*.json*"):
            try:
                file_path.unlink()
            except OSError:
                pass  # File might be in use