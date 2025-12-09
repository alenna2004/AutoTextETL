"""
DOCX-specific loaders and utilities
"""
from .docx_loader import DocxLoader
from .virtual_paginator import VirtualPaginator

__all__ = [
    'DocxLoader',
    'VirtualPaginator'
]