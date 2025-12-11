#!/usr/bin/env python3
"""
File Exporter - Export chunks to various file formats
"""

from typing import List, Dict, Any, Optional, Union
from domain.interfaces import IDbExporter
from domain.chunk import Chunk
from domain.pipeline import PipelineRun
import os
import json
import csv
import gzip
from pathlib import Path
from datetime import datetime

class FileExporter(IDbExporter):
    """
    File exporter implementation supporting multiple formats
    """
    
    def __init__(self):
        self.is_connected = True  # Always "connected" since it's file-based
        self.connection_config = None
    
    def connect(self, config: Dict[str, Any]):
        """
        Configure file export settings
        Args:
            config: Configuration with 'output_dir', 'format', etc.
        """
        self.connection_config = config
    
    def batch_insert(self, chunks: List[Chunk], output_format: str = "json", 
                    output_dir: str = "./output", file_name: str = "output.json"):
        """
        Export chunks to file
        Args:
            chunks: List of chunks to export
            output_format: Output format (json, csv, txt, xml)
            output_dir: Output directory
            file_name: Output file name
        """
        # Create output directory if it doesn't exist
        Path(output_dir).mkdir(parents=True, exist_ok=True)
        
        file_path = os.path.join(output_dir, file_name)
        
        if output_format.lower() == "json":
            self._export_to_json(chunks, file_path)
        elif output_format.lower() == "csv":
            self._export_to_csv(chunks, file_path)
        elif output_format.lower() == "txt":
            self._export_to_txt(chunks, file_path)
        else:
            raise ValueError(f"Unsupported output format: {output_format}")
    
    def _export_to_json(self, chunks: List[Chunk], file_path: str):
        """
        Export chunks to JSON file
        """
        chunk_data = []
        for chunk in chunks:
            chunk_dict = {
                "id": chunk.id,
                "text": chunk.text,
                "meta": chunk.meta.__dict__,
                "extraction_results": chunk.extraction_results,
                "exported_at": datetime.now().isoformat()
            }
            chunk_data.append(chunk_dict)
        
        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(chunk_data, f, indent=2, ensure_ascii=False, default=str)
    
    def _export_to_csv(self, chunks: List[Chunk], file_path: str):
        """
        Export chunks to CSV file
        """
        with open(file_path, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(['ID', 'Text', 'Document ID', 'Page Num', 'Section ID', 'Section Title', 'Section Level', 'Chunk Type', 'Exported At'])
            
            for chunk in chunks:
                writer.writerow([
                    chunk.id,
                    chunk.text,
                    chunk.meta.document_id,
                    chunk.meta.page_num,
                    chunk.meta.section_id,
                    chunk.meta.section_title,
                    chunk.meta.section_level,
                    chunk.meta.chunk_type.value if hasattr(chunk.meta.chunk_type, 'value') else str(chunk.meta.chunk_type),
                    datetime.now().isoformat()
                ])
    
    def _export_to_txt(self, chunks: List[Chunk], file_path: str):
        """
        Export chunks to TXT file
        """
        with open(file_path, 'w', encoding='utf-8') as f:
            for chunk in chunks:
                f.write(f"--- Chunk {chunk.id} ---\n")
                f.write(f"Document: {chunk.meta.document_id}\n")
                f.write(f"Page: {chunk.meta.page_num}\n")
                f.write(f"Section: {chunk.meta.section_title} (Level {chunk.meta.section_level})\n")
                f.write(f"Text:\n{chunk.text}\n")
                f.write(f"Extraction Results: {chunk.extraction_results}\n")
                f.write("---\n\n")
    
    def export_run_metadata(self, run: PipelineRun):
        """
        Export pipeline run metadata to file
        Args:
            run: Pipeline run instance
        """
        # This would typically export to a separate file or include in output
        pass
    
    def close(self):
        """
        Close file exporter (cleanup if needed)
        """
        pass
    
    def export_to_file(self, chunks: List[Chunk], output_format: str, 
                      output_path: str, file_name: str, compress: bool = False):
        """
        Export chunks to file with optional compression
        Args:
            chunks: List of chunks to export
            output_format: Output format
            output_path: Output directory
            file_name: Output file name
            compress: Whether to compress the output
        """
        self.batch_insert(chunks, output_format, output_path, file_name)
        
        if compress:
            self._compress_file(os.path.join(output_path, file_name))
    
    def _compress_file(self, file_path: str):
        """
        Compress file using gzip
        Args:
            file_path: Path to file to compress
        """
        compressed_path = file_path + ".gz"
        
        with open(file_path, 'rb') as f_in:
            with gzip.open(compressed_path, 'wb') as f_out:
                f_out.writelines(f_in)
        
        # Optionally remove original file after compression
        # os.unlink(file_path)
    
    def get_connection_status(self) -> Dict[str, Any]:
        """
        Get file exporter status
        """
        return {
            "is_connected": True,
            "connection_config": self.connection_config,
            "output_dir": self.connection_config.get("output_dir", "./output") if self.connection_config else "./output"
        }