from dataclasses import dataclass, field
from typing import List, Optional, Dict, Any
from datetime import datetime
import uuid
from enum import Enum

# Import from domain layer only (no infrastructure imports!)
from .chunk import Chunk, Metadata, ChunkType
from .pipeline import PipelineConfig, PipelineRun, PipelineStatus, StepType  # ← Remove LogLevel import
from .enums import LogLevel  # ← Import from enums module instead

class DocumentFormat(Enum):
    """Supported document formats"""
    PDF = "pdf"
    DOCX = "docx"
    TXT = "txt"
    HTML = "html"
    UNKNOWN = "unknown"

@dataclass
class Section:
    """
    Document section with hierarchical structure
    """
    title: str
    level: int
    id: str = field(default_factory=lambda: f"sec_{str(uuid.uuid4())[:8]}")
    start_page: int = 1
    end_page: int = 1
    parent_id: Optional[str] = None
    start_position: Optional[Dict[str, float]] = None  # Coordinates in document
    end_position: Optional[Dict[str, float]] = None
    meta: Dict[str, Any] = field(default_factory=dict)
    
    def __post_init__(self):
        if self.level < 1:
            raise ValueError("Section level must be at least 1")
        if self.start_page > self.end_page:
            raise ValueError("Start page cannot be greater than end page")

@dataclass
class Page:
    """
    Document page with content and blocks
    """
    number: int
    raw_text: str = ""
    id: str = field(default_factory=lambda: f"page_{str(uuid.uuid4())[:8]}")
    sections: List[Section] = field(default_factory=list)
    blocks: List[Dict[str, Any]] = field(default_factory=list)  # Structured content blocks
    meta: Dict[str, Any] = field(default_factory=dict)
    
    def __post_init__(self):
        if self.number < 1:
            raise ValueError("Page number must be at least 1")

class Document:
    """
    Main document entity with pages and sections
    """
    def __init__(self, path: str, format: DocumentFormat = DocumentFormat.UNKNOWN):
        self.id = str(uuid.uuid4())
        self.path = path
        self.format = format
        self.title = ""
        self.author = ""
        self.created_at = datetime.now()
        self.pages: List[Page] = []
        self.sections: List[Section] = []
        self.meta: Dict[str, Any] = {}
    
    def add_page(self, page: Page):
        """Add page to document"""
        if any(p.number == page.number for p in self.pages):
            raise ValueError(f"Page with number {page.number} already exists")
        self.pages.append(page)
        self.pages.sort(key=lambda p: p.number)
    
    def add_section(self, section: Section):
        """Add section to document"""
        if any(s.id == section.id for s in self.sections):
            raise ValueError(f"Section with ID {section.id} already exists")
        self.sections.append(section)
    
    def get_section_by_id(self, section_id: str) -> Optional[Section]:
        """Get section by ID"""
        return next((s for s in self.sections if s.id == section_id), None)
    
    def get_sections_for_page(self, page_num: int) -> List[Section]:
        """Get all sections on specified page"""
        return [s for s in self.sections 
                if s.start_page <= page_num <= s.end_page]
    
    def get_parent_section(self, section_id: str) -> Optional[Section]:
        """Get parent section"""
        section = self.get_section_by_id(section_id)
        if not section or not section.parent_id:
            return None
        return self.get_section_by_id(section.parent_id)
    
    def get_all_child_sections(self, section_id: str) -> List[Section]:
        """Get all child sections (recursively)"""
        children = [s for s in self.sections if s.parent_id == section_id]
        result = children.copy()
        for child in children:
            result.extend(self.get_all_child_sections(child.id))
        return result
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary representation"""
        return {
            "id": self.id,
            "path": self.path,
            "format": self.format.value,
            "title": self.title,
            "author": self.author,
            "created_at": self.created_at.isoformat(),
            "page_count": len(self.pages),
            "section_count": len(self.sections),
            "pages": [
                {
                    "number": p.number,
                    "raw_text_preview": p.raw_text[:100] + "..." if len(p.raw_text) > 100 else p.raw_text,
                    "section_count": len(p.sections),
                    "block_count": len(p.blocks)
                }
                for p in self.pages
            ]
        }