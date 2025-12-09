from typing import List, Dict, Any, Optional, Union
from domain.interfaces import IChunkProcessor
from domain.document import Document
from domain.chunk import Chunk, Metadata, ChunkType
from domain.pipeline import PipelineStepConfig
import re

class LineSplitter(IChunkProcessor):
    """
    Line splitter processor
    Splits text into individual lines
    """
    
    def process(self, input_: Union[Document, Chunk, List[Chunk]], 
                config: Optional[Dict[str, Any]] = None) -> List[Chunk]:
        """
        Split input into lines
        Args:
            input_: Input document, chunk, or list of chunks
            config: Configuration (not used for line splitting)
        Returns:
            List[Chunk]: List of line chunks
        """
        if isinstance(input_, Document):
            # Process all pages in document
            all_chunks = []
            for page in input_.pages:
                chunks = self._split_page_to_lines(page, input_)
                all_chunks.extend(chunks)
            return all_chunks
        
        elif isinstance(input_, Chunk):
            # Process single chunk
            return self._split_chunk_to_lines(input_)
        
        elif isinstance(input_, list):
            # Process list of chunks
            all_chunks = []
            for chunk in input_:
                if isinstance(chunk, Chunk):
                    all_chunks.extend(self._split_chunk_to_lines(chunk))
            return all_chunks
        
        else:
            raise ValueError(f"Unsupported input type: {type(input_)}")
    
    def _split_page_to_lines(self, page, document: Document) -> List[Chunk]:
        """Split page content into lines"""
        lines = page.raw_text.split('\n')
        chunks = []
        
        for i, line in enumerate(lines, 1):
            if line.strip():  # Skip empty lines
                chunk = Chunk(
                    text=line.strip(),
                    meta=Metadata(
                        document_id=document.id,
                        section_id="unknown",  # Will be updated by propagator
                        section_title="unknown",
                        section_level=1,
                        page_num=page.number,
                        line_num=i,
                        chunk_type=ChunkType.LINE
                    )
                )
                chunks.append(chunk)
        
        return chunks
    
    def _split_chunk_to_lines(self, chunk: Chunk) -> List[Chunk]:
        """Split a single chunk into lines"""
        lines = chunk.text.split('\n')
        chunks = []
        
        # Determine starting line number based on parent metadata
        start_line_num = chunk.meta.line_num if chunk.meta.line_num else 1
        
        for i, line in enumerate(lines, start_line_num):
            if line.strip():  # Skip empty lines
                new_chunk = Chunk(
                    text=line.strip(),
                    meta=Metadata(
                        document_id=chunk.meta.document_id,
                        section_id=chunk.meta.section_id,
                        section_title=chunk.meta.section_title,
                        section_level=chunk.meta.section_level,
                        page_num=chunk.meta.page_num,
                        line_num=i,
                        chunk_type=ChunkType.LINE,
                        pipeline_run_id=chunk.meta.pipeline_run_id,
                        source_type=chunk.meta.source_type
                    )
                )
                chunks.append(new_chunk)
        
        return chunks
    
    def get_required_context(self) -> List[str]:
        """Return required metadata keys"""
        return ["document_id", "page_num", "section_id"]