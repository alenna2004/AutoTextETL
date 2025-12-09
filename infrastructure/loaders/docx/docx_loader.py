from typing import Dict, Any, Union, List, Optional
from domain.interfaces import IDocumentLoader
from domain.document import Document, Page, Section, DocumentFormat
from .virtual_paginator import VirtualPaginator
from infrastructure.processors.metadata_propagator import HeaderStyleDefinition, StyleBasedHeaderDetector
from utilities.header_filter import HeaderFilter
from docx import Document as DocxDocument
import os
import json
from datetime import datetime
import tempfile

class DocxLoader(IDocumentLoader):
    """
    DOCX document loader - processes DOCX files directly (no conversion needed)
    """
    
    def __init__(self, header_style_definitions: Optional[List[HeaderStyleDefinition]] = None):
        self.header_style_definitions = header_style_definitions or []
        self.header_detector = StyleBasedHeaderDetector(self.header_style_definitions) if self.header_style_definitions else None
    
    def load(self, source: Union[str, Dict[str, Any]]) -> Document:
        """
        Load DOCX document directly (no conversion needed)
        Args:
            source: File path string or configuration dict
        Returns:
            Document: Structured DOCX document
        """
        if isinstance(source, str):
            file_path = source
            config = {}
        else:
            file_path = source.get("path", "")
            config = source
        
        # Check for style definitions in config
        if "header_style_definitions" in config:
            self._update_style_definitions(config["header_style_definitions"])
        elif "style_config_path" in config:
            # Load from configuration file
            self._load_style_config(config["style_config_path"])
        
        doc = DocxDocument(file_path)
        document = Document(file_path, DocumentFormat.DOCX)
        document.title = self._extract_title(doc)
        document.author = self._extract_author(doc)
        
        # Create virtual pages using the specialized paginator
        virtual_pages = VirtualPaginator.create_virtual_pages(doc, paragraphs_per_page=50)
        for page in virtual_pages:
            document.add_page(page)
        
        # Detect sections based on user-defined styles
        self._detect_sections_by_styles(doc, document)
        
        return document
    
    def _detect_sections_by_styles(self, doc, document: Document):
        """
        Detect sections in DOCX based on user-defined style patterns
        """
        if not self.header_detector:
            return  # No style definitions provided
        
        # Analyze paragraphs to find headers based on configured styles
        for para in doc.paragraphs:
            text = para.text.strip()
            if not text:
                continue
            
            # Extract font information from paragraph
            font_info = self._extract_font_info(para)
            
            header_level = self.header_detector.detect_header_level(
                text, 
                font_info.get('font_size'), 
                font_info.get('font_flags')
            )
            
            if header_level is not None:
                # Check additional filtering criteria
                if self._passes_filters(text, header_level):
                    # Create section
                    section = Section(
                        title=text,
                        level=header_level,
                        start_page=self._estimate_page_number(para, doc),  # Rough estimate
                        end_page=self._estimate_page_number(para, doc)
                    )
                    document.add_section(section)
    
    def _extract_font_info(self, para) -> Dict[str, Any]:
        """Extract font information from paragraph"""
        font_info = {"font_size": None, "font_flags": 0}
        
        if para.runs:
            run = para.runs[0]  # Use first run for main properties
            if run.font.size:
                font_info["font_size"] = run.font.size.pt if hasattr(run.font.size, 'pt') else run.font.size / 12700  # Convert from half-points
            
            # Check bold/italic flags
            if run.font.bold:
                font_info["font_flags"] |= 2**4  # Bold flag
            if run.font.italic:
                font_info["font_flags"] |= 2**1  # Italic flag
        
        return font_info
    
    def _passes_filters(self, text: str, level: int) -> bool:
        """
        Check if text passes additional filtering criteria
        """
        if not self.header_detector:
            return True  # If no detector, accept everything
        
        # The detector's internal filtering handles include/exclude words/regex
        return True  # Actual filtering is done in the detector
    
    def _estimate_page_number(self, para, doc) -> int:
        """Estimate page number based on paragraph position"""
        all_paragraphs = list(doc.paragraphs)
        para_index = all_paragraphs.index(para)
        return max(1, (para_index // 50) + 1)  # 50 paragraphs per page estimate
    
    def supports_format(self, path: str) -> bool:
        """
        Check if loader supports DOCX format
        """
        return path.lower().endswith('.docx')
    
    def get_document_metadata(self, path: str) -> Dict[str, Any]:
        """
        Extract DOCX metadata without full loading
        """
        doc = DocxDocument(path)
        
        # Get core properties (safe access)
        core_props = doc.core_properties
        
        # Count paragraphs and tables
        paragraph_count = len(list(doc.paragraphs))
        table_count = len(list(doc.tables))
        
        return {
            "format": "DOCX",
            "title": getattr(core_props, 'title', '') or "",
            "author": getattr(core_props, 'author', '') or "",
            "subject": getattr(core_props, 'subject', '') or "",
            "creator": getattr(core_props, 'creator', '') or "",
            "description": getattr(core_props, 'description', '') or "",
            "modified": getattr(core_props, 'modified', None),
            "created": getattr(core_props, 'created', None),
            "paragraph_count": paragraph_count,
            "table_count": table_count,
            "file_size": self._get_file_size(path)
        }
    
    def _extract_title(self, doc) -> str:
        """Extract document title from core properties"""
        return getattr(doc.core_properties, 'title', '') or ""
    
    def _extract_author(self, doc) -> str:
        """Extract document author from core properties"""
        return getattr(doc.core_properties, 'author', '') or ""
    
    def _get_file_size(self, path: str) -> int:
        """Get file size in bytes"""
        return os.path.getsize(path)
    
    def _update_style_definitions(self, style_configs: List[Dict[str, Any]]):
        """Update style definitions from configuration"""
        style_defs = []
        for config in style_configs:
            # Create header filter from config
            filter_config = {
                'include_words': config.get('include_words', []),
                'exclude_words': config.get('exclude_words', []),
                'include_regex': config.get('include_regex'),
                'exclude_regex': config.get('exclude_regex'),
                'min_length': config.get('min_length'),
                'max_length': config.get('max_length'),
                'starts_with': config.get('starts_with'),
                'ends_with': config.get('ends_with'),
                'contains_pattern': config.get('contains_pattern')
            }
            
            header_filter = HeaderFilter(**filter_config)
            
            style_def = HeaderStyleDefinition(
                level=config['level'],
                font_size=config.get('font_size'),
                is_bold=config.get('is_bold'),
                is_italic=config.get('is_italic'),
                starts_with_pattern=config.get('starts_with_pattern'),
                contains_pattern=config.get('contains_pattern'),
                header_filter=header_filter
            )
            style_defs.append(style_def)
        
        self.header_style_definitions = style_defs
        self.header_detector = StyleBasedHeaderDetector(self.header_style_definitions)
    
    def _load_style_config(self, config_path: str):
        """Load style configuration from JSON file"""
        import json
        
        with open(config_path, 'r', encoding='utf-8') as f:
            config_data = json.load(f)
        
        style_defs = []
        for item in config_data.get("header_assignments", []):
            style_data = item.get("style", {})
            
            # Create header filter from JSON config
            filter_config = {
                'include_words': item.get('include_words', []),
                'exclude_words': item.get('exclude_words', []),
                'include_regex': item.get('include_regex'),
                'exclude_regex': item.get('exclude_regex'),
                'min_length': item.get('min_length'),
                'max_length': item.get('max_length'),
                'starts_with': item.get('starts_with'),
                'ends_with': item.get('ends_with'),
                'contains_pattern': item.get('contains_pattern')
            }
            
            header_filter = HeaderFilter(**filter_config)
            
            style_def = HeaderStyleDefinition(
                level=item["level"],
                font_size=style_data.get("font_size"),
                is_bold=style_data.get("is_bold"),
                is_italic=style_data.get("is_italic"),
                starts_with_pattern=style_data.get("starts_with_pattern"),
                contains_pattern=style_data.get("contains_pattern"),
                header_filter=header_filter
            )
            style_defs.append(style_def)
        
        self.header_style_definitions = style_defs
        self.header_detector = StyleBasedHeaderDetector(self.header_style_definitions)