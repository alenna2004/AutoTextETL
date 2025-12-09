from typing import List, Dict, Any, Optional, Union
from domain.interfaces import IChunkProcessor
from domain.document import Document
from domain.chunk import Chunk, Metadata, ChunkType
import re

class DelimiterSplitter(IChunkProcessor):
    """
    Delimiter splitter processor
    Splits text by custom delimiter
    """
    
    def process(self, input_: Union[Document, Chunk, List[Chunk]], 
                config: Optional[Dict[str, Any]] = None) -> List[Chunk]:
        """
        Split input by delimiter
        Args:
            input_: Input document, chunk, or list of chunks
            config: Configuration with 'delimiter' and 'use_regex' keys
        Returns:
            List[Chunk]: List of delimiter-separated chunks
        """
        if config is None:
            config = {}
        
        delimiter = config.get("delimiter", ";")
        use_regex = config.get("use_regex", False)
        
        if isinstance(input_, Document):
            # Process all pages in document
            all_chunks = []
            for page in input_.pages:
                chunks = self._split_page_by_delimiter(page, input_, delimiter, use_regex)
                all_chunks.extend(chunks)
            return all_chunks
        
        elif isinstance(input_, Chunk):
            # Process single chunk
            return self._split_chunk_by_delimiter(input_, delimiter, use_regex)
        
        elif isinstance(input_, list):
            # Process list of chunks
            all_chunks = []
            for chunk in input_:
                if isinstance(chunk, Chunk):
                    all_chunks.extend(self._split_chunk_by_delimiter(chunk, delimiter, use_regex))
            return all_chunks
        
        else:
            raise ValueError(f"Unsupported input type: {type(input_)}")
    
    def _split_page_by_delimiter(self, page, document: Document, delimiter: str, use_regex: bool) -> List[Chunk]:
        """Split page content by delimiter"""
        if use_regex:
            parts = re.split(delimiter, page.raw_text)
        else:
            parts = page.raw_text.split(delimiter)
        
        chunks = []
        
        for i, part in enumerate(parts):
            part = part.strip()
            if part:  # Skip empty parts
                chunk = Chunk(
                    text=part,
                    meta=Metadata(
                        document_id=document.id,
                        section_id="unknown",  # Will be updated by propagator
                        section_title="unknown",
                        section_level=1,
                        page_num=page.number,
                        chunk_type=ChunkType.CUSTOM
                    )
                )
                chunks.append(chunk)
        
        return chunks
    
    def _split_chunk_by_delimiter(self, chunk: Chunk, delimiter: str, use_regex: bool) -> List[Chunk]:
        """Split a single chunk by delimiter"""
        if use_regex:
            parts = re.split(delimiter, chunk.text)
        else:
            parts = chunk.text.split(delimiter)
        
        chunks = []
        
        for part in parts:
            part = part.strip()
            if part:  # Skip empty parts
                new_chunk = Chunk(
                    text=part,
                    meta=Metadata(
                        document_id=chunk.meta.document_id,
                        section_id=chunk.meta.section_id,
                        section_title=chunk.meta.section_title,
                        section_level=chunk.meta.section_level,
                        page_num=chunk.meta.page_num,
                        chunk_type=ChunkType.CUSTOM,
                        pipeline_run_id=chunk.meta.pipeline_run_id,
                        source_type=chunk.meta.source_type
                    )
                )
                chunks.append(new_chunk)
        
        return chunks
    
    def get_required_context(self) -> List[str]:
        """Return required metadata keys"""
        return ["document_id", "page_num", "section_id"]