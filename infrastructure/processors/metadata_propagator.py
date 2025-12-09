from typing import List, Dict, Any, Optional, Union
from domain.interfaces import IChunkProcessor
from domain.document import Document, Section
from domain.chunk import Chunk, Metadata, ChunkType
import re

# Import the filtering utility
from utilities.header_filter import HeaderFilter, ExactHeadingRule, ExactHeadingDetector

class HeaderStyleDefinition:
    """
    Defines style patterns for header detection with filtering and exact headings
    """
    def __init__(self, level: int, font_size: Optional[int] = None, 
                 is_bold: Optional[bool] = None, is_italic: Optional[bool] = None,
                 starts_with_pattern: Optional[str] = None, 
                 contains_pattern: Optional[str] = None,
                 header_filter: Optional[HeaderFilter] = None,
                 exact_heading_rules: Optional[List[ExactHeadingRule]] = None):
        self.level = level
        self.font_size = font_size
        self.is_bold = is_bold
        self.is_italic = is_italic
        self.starts_with_pattern = starts_with_pattern  # e.g., r'^\d+\.\s+', r'^#\s+'
        self.contains_pattern = contains_pattern
        self.header_filter = header_filter  # Use the separate filtering utility
        self.exact_heading_rules = exact_heading_rules or []  # Exact heading rules

class StyleBasedHeaderDetector:
    """
    Detects headers based on user-defined style patterns and exact headings
    """
    def __init__(self, style_definitions: List[HeaderStyleDefinition]):
        self.style_definitions = style_definitions
        self.exact_detector = self._build_exact_detector()
    
    def _build_exact_detector(self) -> ExactHeadingDetector:
        """
        Build exact heading detector from all style definitions
        """
        detector = ExactHeadingDetector()
        
        for style_def in self.style_definitions:
            for rule in style_def.exact_heading_rules:
                detector.add_rule(rule)
        
        return detector
    
    def detect_header_level(self, text: str, font_size: Optional[float] = None, 
                           font_flags: Optional[int] = None) -> Optional[int]:
        """
        Detect header level based on style definitions and exact headings
        """
        # First check for exact headings
        exact_matches = self.exact_detector.detect_exact_headings(text)
        if exact_matches:
            # Return the level of the first exact match
            return exact_matches[0][1]
        
        # Then check for style-based matches
        for style_def in self.style_definitions:
            if self._matches_style(text, font_size, font_flags, style_def):
                # Additional filtering using the separate utility
                if style_def.header_filter is None or style_def.header_filter.should_include(text):
                    return style_def.level
        
        return None
    
    def _matches_style(self, text: str, font_size: Optional[float], 
                      font_flags: Optional[int], style_def: HeaderStyleDefinition) -> bool:
        """
        Check if text matches the style definition
        """
        # Check font size
        if style_def.font_size is not None and font_size is not None:
            if abs(style_def.font_size - font_size) > 0.1:  # Allow small rounding differences
                return False
        
        # Check bold flag (font_flags & 2**4 for bold in PyMuPDF)
        if style_def.is_bold is not None and font_flags is not None:
            is_bold = bool(font_flags & 2**4)
            if style_def.is_bold != is_bold:
                return False
        
        # Check italic flag (font_flags & 2**1 for italic in PyMuPDF)
        if style_def.is_italic is not None and font_flags is not None:
            is_italic = bool(font_flags & 2**1)
            if style_def.is_italic != is_italic:
                return False
        
        # Check starts with pattern
        if style_def.starts_with_pattern:
            if not re.match(style_def.starts_with_pattern, text.strip()):
                return False
        
        # Check contains pattern
        if style_def.contains_pattern:
            if not re.search(style_def.contains_pattern, text):
                return False
        
        return True

class MetadataPropagator(IChunkProcessor):
    """
    Enhanced Metadata propagator with style-based section detection and exact headings
    """
    
    def __init__(self, header_style_definitions: Optional[List[HeaderStyleDefinition]] = None):
        self.header_style_definitions = header_style_definitions or []
        self.header_detector = StyleBasedHeaderDetector(self.header_style_definitions) if self.header_style_definitions else None
    
    def process(self, input_: Union[Document, Chunk, List[Chunk]], 
                config: Optional[Dict[str, Any]] = None) -> List[Chunk]:
        """
        Propagate metadata and detect sections based on user-defined styles and exact headings
        Args:
            input_: Input document, chunk, or list of chunks
            config: Configuration with 'header_style_definitions' for style-based detection
        Returns:
            List[Chunk]: List of chunks with propagated metadata and section info
        """
        if config and 'header_style_definitions' in config:
            # Update style definitions from config
            style_defs = []
            for def_data in config['header_style_definitions']:
                # Create header filter from config
                filter_config = {
                    'include_words': def_data.get('include_words', []),
                    'exclude_words': def_data.get('exclude_words', []),
                    'include_regex': def_data.get('include_regex'),
                    'exclude_regex': def_data.get('exclude_regex'),
                    'min_length': def_data.get('min_length'),
                    'max_length': def_data.get('max_length'),
                    'starts_with': def_data.get('starts_with'),
                    'ends_with': def_data.get('ends_with'),
                    'contains_pattern': def_data.get('contains_pattern')
                }
                
                header_filter = HeaderFilter(**filter_config)
                
                # Create exact heading rules
                exact_rules = []
                for rule_data in def_data.get('exact_heading_rules', []):
                    rule = ExactHeadingRule(
                        heading_text=rule_data['heading_text'],
                        level=rule_data.get('level', def_data['level']),
                        case_sensitive=rule_data.get('case_sensitive', False),
                        whole_word=rule_data.get('whole_word', True)
                    )
                    exact_rules.append(rule)
                
                style_def = HeaderStyleDefinition(
                    level=def_data['level'],
                    font_size=def_data.get('font_size'),
                    is_bold=def_data.get('is_bold'),
                    is_italic=def_data.get('is_italic'),
                    starts_with_pattern=def_data.get('starts_with_pattern'),
                    contains_pattern=def_data.get('contains_pattern'),
                    header_filter=header_filter,
                    exact_heading_rules=exact_rules
                )
                style_defs.append(style_def)
            
            self.header_style_definitions = style_defs
            self.header_detector = StyleBasedHeaderDetector(self.header_style_definitions)
        
        if isinstance(input_, Document):
            # Process document - detect sections and propagate metadata
            self._detect_sections_in_document(input_)
            return []
        
        elif isinstance(input_, Chunk):
            # Process single chunk
            return [self._propagate_metadata_to_chunk(input_)]
        
        elif isinstance(input_, list):
            # Process list of chunks
            return [self._propagate_metadata_to_chunk(chunk) for chunk in input_ if isinstance(chunk, Chunk)]
        
        else:
            raise ValueError(f"Unsupported input type: {type(input_)}")
    
    def _detect_sections_in_document(self, document: Document):
        """
        Detect sections in document based on user-defined styles and exact headings
        """
        if not self.header_detector:
            return  # No style definitions provided
        
        # Process each page to find headers based on styles and exact headings
        for page in document.pages:
            self._detect_headers_in_page(page, document)
    
    def _detect_headers_in_page(self, page, document: Document):
        """
        Detect headers in page blocks based on style definitions and exact headings
        """
        if not hasattr(page, 'blocks') or not page.blocks:
            return
        
        # Combine spans that might form multiline headers
        processed_texts = set()
        
        for block in page.blocks:
            if block.get('type') == 'text':
                all_spans = block.get('all_spans', [])
                
                if all_spans:
                    # Process spans to detect potential multiline headers
                    self._process_spans_for_headers(all_spans, document, page.number)
                else:
                    # Fallback to single text processing
                    text = block.get('text', '')
                    if text and text not in processed_texts:
                        font_size = block.get('font_size')
                        font_flags = block.get('font_flags')
                        
                        header_level = self.header_detector.detect_header_level(
                            text, font_size, font_flags
                        )
                        
                        if header_level is not None:
                            # Create section
                            section = Section(
                                title=text.strip(),
                                level=header_level,
                                start_page=page.number,
                                end_page=page.number
                            )
                            document.add_section(section)
                        processed_texts.add(text)
    
    def _process_spans_for_headers(self, spans: List[Dict], document: Document, page_num: int):
        """
        Process multiple spans to detect headers (handles multiline headers)
        """
        if not spans:
            return
        
        # Group spans that might form multiline headers
        for span in spans:
            text = span["text"].strip()
            if not text:
                continue
            
            # Extract font properties
            font_size = round(span["size"], 1)  # Round to avoid floating point issues
            flags = span["flags"]
            
            # Determine bold/italic from flags
            is_bold = bool(flags & 2**4)  # Bit 4 = bold
            is_italic = bool(flags & 2**1)  # Bit 1 = italic
            is_underline = bool(flags & 2**6)  # Bit 6 = underline
            is_strikeout = bool(flags & 2**7)  # Bit 7 = strikeout
            
            # Create font_flags that matches the detector expectation
            combined_flags = 0
            if is_bold:
                combined_flags |= 2**4
            if is_italic:
                combined_flags |= 2**1
            if is_underline:
                combined_flags |= 2**6
            if is_strikeout:
                combined_flags |= 2**7
            
            # Detect header level based on style and exact headings
            header_level = self.header_detector.detect_header_level(
                text, font_size, combined_flags
            )
            
            if header_level is not None:
                # Create section
                section = Section(
                    title=text,
                    level=header_level,
                    start_page=page_num,
                    end_page=page_num
                )
                document.add_section(section)
    
    def _propagate_metadata_to_chunk(self, chunk: Chunk) -> Chunk:
        """
        Ensure chunk has proper metadata by propagating from parent context
        """
        # Ensure required metadata exists
        if not chunk.meta.document_id:
            chunk.meta.document_id = "unknown"
        
        if not chunk.meta.section_id:
            chunk.meta.section_id = "unknown"
        
        if not chunk.meta.section_title:
            chunk.meta.section_title = "unknown"
        
        if chunk.meta.section_level < 1:
            chunk.meta.section_level = 1
        
        return chunk
    
    def propagate_from_parent(self, parent_chunk: Chunk, child_chunks: List[Chunk]) -> List[Chunk]:
        """
        Explicitly propagate metadata from parent to children
        """
        updated_children = []
        
        for child in child_chunks:
            # Copy essential metadata from parent
            child.meta.document_id = parent_chunk.meta.document_id
            child.meta.section_id = parent_chunk.meta.section_id
            child.meta.section_title = parent_chunk.meta.section_title
            child.meta.section_level = parent_chunk.meta.section_level
            child.meta.page_num = parent_chunk.meta.page_num
            child.meta.pipeline_run_id = parent_chunk.meta.pipeline_run_id
            child.meta.source_type = parent_chunk.meta.source_type
            
            # Preserve child-specific metadata
            if child.meta.line_num is None:
                child.meta.line_num = parent_chunk.meta.line_num
            
            # Set appropriate chunk type
            if child.meta.chunk_type == ChunkType.CUSTOM:
                child.meta.chunk_type = parent_chunk.meta.chunk_type
            
            updated_children.append(child)
        
        return updated_children
    
    def get_required_context(self) -> List[str]:
        """Return required metadata keys"""
        return ["document_id", "page_num", "section_id", "section_title", "section_level"]