from dataclasses import dataclass, field
from typing import List, Optional, Dict, Any, TYPE_CHECKING
from uuid import uuid4
from enum import Enum
from datetime import datetime

# For avoiding circular imports when using type annotations
if TYPE_CHECKING:
    from .chunk import Chunk

class ChunkType(Enum):
    """Types of text chunks"""
    LINE = "line"           # One line of text
    PARAGRAPH = "paragraph" # Paragraph
    SENTENCE = "sentence"   # Sentence
    CUSTOM = "custom"       # Custom fragment (after splitter)
    DOCUMENT = "document"   # Entire document

@dataclass
class Metadata:
    """
    Contextual information about text chunk
    Attributes:
        document_id: Original document ID
        page_num: Page number (if applicable)
        section_id: ID of section this chunk belongs to
        section_title: Section name
        section_level: Section nesting level
        line_num: Line number on page
        chunk_type: Chunk type (line, paragraph, etc.)
        pipeline_run_id: Pipeline run ID
        source_type: Source type (pdf, docx, api)
    """
    document_id: str
    section_id: str
    section_title: str
    section_level: int
    page_num: Optional[int] = None
    line_num: Optional[int] = None
    chunk_type: ChunkType = ChunkType.CUSTOM
    pipeline_run_id: Optional[str] = None
    source_type: str = "unknown"
    
    def __post_init__(self):
        """Validate data when creating object"""
        if not self.document_id:
            raise ValueError("document_id cannot be empty")
        if not self.section_id:
            raise ValueError("section_id cannot be empty")
        if self.section_level < 1:
            raise ValueError("section_level cannot be less than 1")
        if self.page_num is not None and self.page_num < 1:
            raise ValueError("page_num cannot be less than 1")
        if self.line_num is not None and self.line_num < 1:
            raise ValueError("line_num cannot be less than 1")
        
        # Convert string to ChunkType if string value is passed
        if isinstance(self.chunk_type, str):
            try:
                self.chunk_type = ChunkType(self.chunk_type)
            except ValueError:
                raise ValueError(f"Invalid chunk_type value: {self.chunk_type}. "
                               f"Valid values: {[t.value for t in ChunkType]}")

@dataclass
class Chunk:
    """
    Logical text fragment with full context
    Attributes:
        id: Unique identifier
        text: Fragment text
        meta: Metadata - Contextual information
        parent_id: Parent fragment ID (if exists)
        children: Child fragments (result of splitting)
        extraction_results: Data extraction results
    """
    id: str = field(default_factory=lambda: str(uuid4()))
    text: str = ""
    meta: Metadata = field(default_factory=lambda: Metadata(
        document_id="temp_doc",
        section_id="temp_section",
        section_title="Temporary",
        section_level=1
    ))
    parent_id: Optional[str] = None
    children: List['Chunk'] = field(default_factory=list)
    extraction_results: Dict[str, Any] = field(default_factory=dict)
    
    def __post_init__(self):
        """Additional validation after initialization"""
        if isinstance(self.meta, dict):
            # If meta is passed as dict, convert to Metadata object
            try:
                self.meta = Metadata(**self.meta)
            except TypeError as e:
                raise ValueError(f"Error creating Metadata from dict: {e}")
    
    def add_child(self, child: 'Chunk'):
        """Add child fragment"""
        if child.parent_id and child.parent_id != self.id:
            raise ValueError(f"Parent ID mismatch: expected {self.id}, got {child.parent_id}")
        child.parent_id = self.id
        self.children.append(child)
    
    def has_children(self) -> bool:
        """Check if there are child fragments"""
        return len(self.children) > 0
    
    def get_all_descendants(self) -> List['Chunk']:
        """Return all descendants (recursively)"""
        result = self.children.copy()
        for child in self.children:
            result.extend(child.get_all_descendants())
        return result
    
    def to_dict(self) -> Dict[str, Any]:
        """Serialize fragment to dictionary"""
        # For display purposes in serialization, truncate text to 50 chars + ...
        truncated_text = self.text
        if len(self.text) > 50:
            truncated_text = self.text[:50] + "..."
        
        return {
            "id": self.id,
            "text": truncated_text,  # Truncated for display
            "original_text": self.text,  # Store original for deserialization
            "metadata": {
                "document_id": self.meta.document_id,
                "page_num": self.meta.page_num,
                "section_id": self.meta.section_id,
                "section_title": self.meta.section_title,
                "section_level": self.meta.section_level,
                "line_num": self.meta.line_num,
                "chunk_type": self.meta.chunk_type.value,
                "pipeline_run_id": self.meta.pipeline_run_id,
                "source_type": self.meta.source_type
            },
            "parent_id": self.parent_id,
            "children_count": len(self.children),
            "extraction_count": len(self.extraction_results),  # Add the field expected by test
            "extraction_results": self.extraction_results  # Include extraction results
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any], parent_id: Optional[str] = None) -> 'Chunk':
        """Create fragment from dictionary"""
        # Process metadata
        metadata_data = data.get("metadata", {})
        if not metadata_data:
            # Try to find metadata in other places
            metadata_data = {
                "document_id": data.get("document_id", "unknown_doc"),
                "section_id": data.get("section_id", "unknown_section"),
                "section_title": data.get("section_title", "Unknown"),
                "section_level": data.get("section_level", 1),
                "page_num": data.get("page_num"),
                "line_num": data.get("line_num"),
                "chunk_type": data.get("chunk_type", "custom"),
                "pipeline_run_id": data.get("pipeline_run_id"),
                "source_type": data.get("source_type", "unknown")
            }
        
        # Create Metadata object
        metadata = Metadata(
            document_id=metadata_data.get("document_id", "unknown_doc"),
            section_id=metadata_data.get("section_id", "unknown_section"),
            section_title=metadata_data.get("section_title", "Unknown"),
            section_level=int(metadata_data.get("section_level", 1)),
            page_num=int(metadata_data["page_num"]) if metadata_data.get("page_num") is not None else None,
            line_num=int(metadata_data["line_num"]) if metadata_data.get("line_num") is not None else None,
            chunk_type=ChunkType(metadata_data.get("chunk_type", "custom")),
            pipeline_run_id=metadata_data.get("pipeline_run_id"),
            source_type=metadata_data.get("source_type", "unknown")
        )
        
        # Use original text if available, otherwise use the (possibly truncated) text field
        original_text = data.get("original_text", data.get("text", ""))
        
        # Create fragment
        chunk = cls(
            id=data.get("id", str(uuid4())),
            text=original_text,  # Use original text
            meta=metadata,
            parent_id=parent_id or data.get("parent_id"),
            extraction_results=data.get("extraction_results", {})  # Include extraction results
        )
        
        # Recursively create child fragments
        children_data = data.get("children", [])
        for child_data in children_data:
            child = cls.from_dict(child_data, parent_id=chunk.id)
            chunk.add_child(child)
        
        return chunk
    
    def __str__(self) -> str:
        """Human-readable representation of fragment"""
        # Truncate section title to exactly 20 characters if longer
        truncated_section = self.meta.section_title
        if len(self.meta.section_title) > 20:
            truncated_section = self.meta.section_title[:20]
        
        return (f"Chunk(id={self.id[:8]}, type={self.meta.chunk_type.value}, "
                f"page={self.meta.page_num or 'N/A'}, section={truncated_section})")
    
    def __repr__(self) -> str:
        """Detailed representation for debugging"""
        return self.__str__()