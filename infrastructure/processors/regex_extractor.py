from typing import List, Dict, Any, Optional, Union
from domain.interfaces import IChunkProcessor
from domain.document import Document
from domain.chunk import Chunk, Metadata, ChunkType
import re

class RegexExtractor(IChunkProcessor):
    """
    Regex extractor processor
    Extracts data from text using regular expressions
    """
    
    def process(self, input_: Union[Document, Chunk, List[Chunk]], 
                config: Optional[Dict[str, Any]] = None) -> List[Chunk]:
        """
        Extract data using regex patterns
        Args:
            input_: Input document, chunk, or list of chunks
            config: Configuration with 'patterns' key containing regex patterns
        Returns:
            List[Chunk]: List of extracted chunks with extracted data
        """
        if config is None:
            config = {}
        
        patterns = config.get("patterns", [])
        if not patterns:
            return []  # Return empty list if no patterns provided
        
        if isinstance(input_, Document):
            # Process all pages in document
            all_chunks = []
            for page in input_.pages:
                chunks = self._extract_from_page(page, input_, patterns)
                all_chunks.extend(chunks)
            return all_chunks
        
        elif isinstance(input_, Chunk):
            # Process single chunk
            return self._extract_from_chunk(input_, patterns)
        
        elif isinstance(input_, list):
            # Process list of chunks
            all_chunks = []
            for chunk in input_:
                if isinstance(chunk, Chunk):
                    all_chunks.extend(self._extract_from_chunk(chunk, patterns))
            return all_chunks
        
        else:
            raise ValueError(f"Unsupported input type: {type(input_)}")
    
    def _extract_from_page(self, page, document: Document, patterns: list) -> List[Chunk]:
        """Extract data from page content using regex"""
        chunks = []
        
        for pattern in patterns:
            if isinstance(pattern, str):
                # Simple pattern string
                matches = re.finditer(pattern, page.raw_text)
                for match in matches:
                    extracted_text = match.group(0)
                    chunk = Chunk(
                        text=extracted_text,
                        meta=Metadata(
                            document_id=document.id,
                            section_id="unknown",  # Will be updated by propagator
                            section_title="unknown",
                            section_level=1,
                            page_num=page.number,
                            chunk_type=ChunkType.CUSTOM
                        ),
                        extraction_results={
                            "pattern": pattern,
                            "matched_groups": [match.group(i) for i in range(len(match.groups()) + 1)],
                            "match_start": match.start(),
                            "match_end": match.end()
                        }
                    )
                    chunks.append(chunk)
            elif isinstance(pattern, dict):
                # Named pattern with capture groups
                pattern_str = pattern.get("pattern", "")
                name = pattern.get("name", "unnamed")
                
                matches = re.finditer(pattern_str, page.raw_text)
                for match in matches:
                    extracted_text = match.group(0)
                    chunk = Chunk(
                        text=extracted_text,
                        meta=Metadata(
                            document_id=document.id,
                            section_id="unknown",
                            section_title="unknown",
                            section_level=1,
                            page_num=page.number,
                            chunk_type=ChunkType.CUSTOM
                        ),
                        extraction_results={
                            "name": name,
                            "pattern": pattern_str,
                            "matched_groups": {i: match.group(i) for i in range(len(match.groups()) + 1)},
                            "match_start": match.start(),
                            "match_end": match.end()
                        }
                    )
                    chunks.append(chunk)
        
        return chunks
    
    def _extract_from_chunk(self, chunk: Chunk, patterns: list) -> List[Chunk]:
        """Extract data from a single chunk using regex"""
        chunks = []
        
        for pattern in patterns:
            if isinstance(pattern, str):
                # Simple pattern string
                matches = re.finditer(pattern, chunk.text)
                for match in matches:
                    extracted_text = match.group(0)
                    new_chunk = Chunk(
                        text=extracted_text,
                        meta=Metadata(
                            document_id=chunk.meta.document_id,
                            section_id=chunk.meta.section_id,
                            section_title=chunk.meta.section_title,
                            section_level=chunk.meta.section_level,
                            page_num=chunk.meta.page_num,
                            chunk_type=ChunkType.CUSTOM,
                            pipeline_run_id=chunk.meta.pipeline_run_id,
                            source_type=chunk.meta.source_type
                        ),
                        extraction_results={
                            "pattern": pattern,
                            "matched_groups": [match.group(i) for i in range(len(match.groups()) + 1)],
                            "match_start": match.start(),
                            "match_end": match.end()
                        }
                    )
                    chunks.append(new_chunk)
            elif isinstance(pattern, dict):
                # Named pattern with capture groups
                pattern_str = pattern.get("pattern", "")
                name = pattern.get("name", "unnamed")
                
                matches = re.finditer(pattern_str, chunk.text)
                for match in matches:
                    extracted_text = match.group(0)
                    new_chunk = Chunk(
                        text=extracted_text,
                        meta=Metadata(
                            document_id=chunk.meta.document_id,
                            section_id=chunk.meta.section_id,
                            section_title=chunk.meta.section_title,
                            section_level=chunk.meta.section_level,
                            page_num=chunk.meta.page_num,
                            chunk_type=ChunkType.CUSTOM,
                            pipeline_run_id=chunk.meta.pipeline_run_id,
                            source_type=chunk.meta.source_type
                        ),
                        extraction_results={
                            "name": name,
                            "pattern": pattern_str,
                            "matched_groups": {i: match.group(i) for i in range(len(match.groups()) + 1)},
                            "match_start": match.start(),
                            "match_end": match.end()
                        }
                    )
                    chunks.append(new_chunk)
        
        return chunks
    
    def get_required_context(self) -> List[str]:
        """Return required metadata keys"""
        return ["document_id", "page_num", "section_id"]