from typing import List, Dict, Any, Optional, Union
from domain.interfaces import IChunkProcessor
from domain.document import Document
from domain.chunk import Chunk, Metadata, ChunkType
import re

class ParagraphSplitter(IChunkProcessor):
    """
    Paragraph splitter processor
    Splits text into paragraphs (separated by empty lines)
    """
    
    def process(self, input_: Union[Document, Chunk, List[Chunk]], 
                config: Optional[Dict[str, Any]] = None) -> List[Chunk]:
        """
        Split input into paragraphs
        Args:
            input_: Input document, chunk, or list of chunks
            config: Configuration (not used for paragraph splitting)
        Returns:
            List[Chunk]: List of paragraph chunks
        """
        if isinstance(input_, Document):
            # Process all pages in document
            all_chunks = []
            for page in input_.pages:
                chunks = self._split_page_to_paragraphs(page, input_)
                all_chunks.extend(chunks)
            return all_chunks
        
        elif isinstance(input_, Chunk):
            # Process single chunk
            return self._split_chunk_to_paragraphs(input_)
        
        elif isinstance(input_, list):
            # Process list of chunks
            all_chunks = []
            for chunk in input_:
                if isinstance(chunk, Chunk):
                    all_chunks.extend(self._split_chunk_to_paragraphs(chunk))
            return all_chunks
        
        else:
            raise ValueError(f"Unsupported input type: {type(input_)}")
    
    def _split_page_to_paragraphs(self, page, document: Document) -> List[Chunk]:
        """Split page content into paragraphs"""
        # Split by multiple newlines (paragraphs)
        paragraphs = re.split(r'\n\s*\n', page.raw_text)
        chunks = []
        
        for i, para in enumerate(paragraphs, 1):
            para = para.strip()
            if para:  # Skip empty paragraphs
                chunk = Chunk(
                    text=para,
                    meta=Metadata(
                        document_id=document.id,
                        section_id="unknown",  # Will be updated by propagator
                        section_title="unknown",
                        section_level=1,
                        page_num=page.number,
                        chunk_type=ChunkType.PARAGRAPH
                    )
                )
                chunks.append(chunk)
        
        return chunks
    
    def _split_chunk_to_paragraphs(self, chunk: Chunk) -> List[Chunk]:
        """Split a single chunk into paragraphs"""
        # Split by multiple newlines (paragraphs)
        paragraphs = re.split(r'\n\s*\n', chunk.text)
        chunks = []
        
        for para in paragraphs:
            para = para.strip()
            if para:  # Skip empty paragraphs
                new_chunk = Chunk(
                    text=para,
                    meta=Metadata(
                        document_id=chunk.meta.document_id,
                        section_id=chunk.meta.section_id,
                        section_title=chunk.meta.section_title,
                        section_level=chunk.meta.section_level,
                        page_num=chunk.meta.page_num,
                        chunk_type=ChunkType.PARAGRAPH,
                        pipeline_run_id=chunk.meta.pipeline_run_id,
                        source_type=chunk.meta.source_type
                    )
                )
                chunks.append(new_chunk)
        
        return chunks
    
    def get_required_context(self) -> List[str]:
        """Return required metadata keys"""
        return ["document_id", "page_num", "section_id"]