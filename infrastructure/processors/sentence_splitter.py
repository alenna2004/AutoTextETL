from typing import List, Dict, Any, Optional, Union
from domain.interfaces import IChunkProcessor
from domain.document import Document
from domain.chunk import Chunk, Metadata, ChunkType
import re

class SentenceSplitter(IChunkProcessor):
    """
    Sentence splitter processor
    Splits text into sentences using common sentence boundaries
    """
    
    def process(self, input_: Union[Document, Chunk, List[Chunk]], 
                config: Optional[Dict[str, Any]] = None) -> List[Chunk]:
        """
        Split input into sentences
        Args:
            input_: Input document, chunk, or list of chunks
            config: Configuration (not used for sentence splitting)
        Returns:
            List[Chunk]: List of sentence chunks
        """
        if isinstance(input_, Document):
            # Process all pages in document
            all_chunks = []
            for page in input_.pages:
                chunks = self._split_page_to_sentences(page, input_)
                all_chunks.extend(chunks)
            return all_chunks
        
        elif isinstance(input_, Chunk):
            # Process single chunk
            return self._split_chunk_to_sentences(input_)
        
        elif isinstance(input_, list):
            # Process list of chunks
            all_chunks = []
            for chunk in input_:
                if isinstance(chunk, Chunk):
                    all_chunks.extend(self._split_chunk_to_sentences(chunk))
            return all_chunks
        
        else:
            raise ValueError(f"Unsupported input type: {type(input_)}")
    
    def _split_page_to_sentences(self, page, document: Document) -> List[Chunk]:
        """Split page content into sentences"""
        sentences = self._split_text_to_sentences(page.raw_text)
        chunks = []
        
        for i, sent in enumerate(sentences, 1):
            sent = sent.strip()
            if sent:  # Skip empty sentences
                chunk = Chunk(
                    text=sent,
                    meta=Metadata(
                        document_id=document.id,
                        section_id="unknown",  # Will be updated by propagator
                        section_title="unknown",
                        section_level=1,
                        page_num=page.number,
                        chunk_type=ChunkType.SENTENCE
                    )
                )
                chunks.append(chunk)
        
        return chunks
    
    def _split_chunk_to_sentences(self, chunk: Chunk) -> List[Chunk]:
        """Split a single chunk into sentences"""
        sentences = self._split_text_to_sentences(chunk.text)
        chunks = []
        
        for sent in sentences:
            sent = sent.strip()
            if sent:  # Skip empty sentences
                new_chunk = Chunk(
                    text=sent,
                    meta=Metadata(
                        document_id=chunk.meta.document_id,
                        section_id=chunk.meta.section_id,
                        section_title=chunk.meta.section_title,
                        section_level=chunk.meta.section_level,
                        page_num=chunk.meta.page_num,
                        chunk_type=ChunkType.SENTENCE,
                        pipeline_run_id=chunk.meta.pipeline_run_id,
                        source_type=chunk.meta.source_type
                    )
                )
                chunks.append(new_chunk)
        
        return chunks
    
    def _split_text_to_sentences(self, text: str) -> List[str]:
        """
        Split text into sentences using regex
        Handles common sentence boundaries: . ! ? followed by space/capital letter
        """
        # Pattern to match sentence endings
        # Handles abbreviations and common edge cases
        sentence_pattern = r'(?<!\w\.\w.)(?<![A-Z][a-z]\.)(?<=\.|\!|\?|\:)\s+'
        
        sentences = re.split(sentence_pattern, text)
        
        # Clean up sentences
        cleaned_sentences = []
        for sent in sentences:
            sent = sent.strip()
            if sent:
                cleaned_sentences.append(sent)
        
        return cleaned_sentences
    
    def get_required_context(self) -> List[str]:
        """Return required metadata keys"""
        return ["document_id", "page_num", "section_id"]