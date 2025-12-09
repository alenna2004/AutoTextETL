"""
Utilities for document processing
"""
from .header_filter import HeaderFilter, HeaderFilterManager, HeaderFilterGroup, apply_header_filters, create_default_header_filters
from .document_style_analyzer import DocumentStyleAnalyzer, interactive_style_configuration, save_style_configuration

__all__ = [
    # Header filtering utilities
    'HeaderFilter',
    'HeaderFilterManager', 
    'HeaderFilterGroup',
    'apply_header_filters',
    'create_default_header_filters',
    
    # Document analysis utilities
    'DocumentStyleAnalyzer',
    'interactive_style_configuration',
    'save_style_configuration'
]