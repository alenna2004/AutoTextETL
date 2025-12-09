#!/usr/bin/env python3
"""
Metadata Inspector Component - Inspect and analyze document metadata
"""

from PyQt6.QtWidgets import (QWidget, QVBoxLayout, QHBoxLayout, QTreeWidget, 
                           QTreeWidgetItem, QGroupBox, QLabel, QTextEdit,
                           QFormLayout, QSplitter, QHeaderView, QTabWidget)
from PyQt6.QtCore import Qt
from PyQt6.QtGui import QAction, QKeySequence, QIcon
from typing import Dict, Any, List, Optional
from domain.chunk import Chunk, Metadata
from domain.document import Document, Page, Section
import json

class MetadataInspector(QWidget):
    """
    Component for inspecting document and chunk metadata
    """
    
    def __init__(self):
        super().__init__()
        self.current_document = None
        self.current_chunk = None
        
        self.setup_ui()
        self.setup_connections()
    
    def setup_ui(self):
        """
        Set up the user interface
        """
        layout = QVBoxLayout(self)
        
        # Main splitter
        splitter = QSplitter(Qt.Orientation.Horizontal)
        
        # Left panel - Document structure
        left_panel = self._create_document_structure_panel()
        splitter.addWidget(left_panel)
        
        # Right panel - Metadata details
        right_panel = self._create_metadata_details_panel()
        splitter.addWidget(right_panel)
        
        splitter.setSizes([400, 600])
        layout.addWidget(splitter)
    
    def _create_document_structure_panel(self) -> QWidget:
        """
        Create left panel with document structure
        """
        panel = QWidget()
        layout = QVBoxLayout(panel)
        
        # Document structure tree
        structure_group = QGroupBox("Document Structure")
        structure_layout = QVBoxLayout(structure_group)
        
        self.structure_tree = QTreeWidget()
        self.structure_tree.setHeaderLabels(["Element", "Type", "Count"])
        self.structure_tree.itemClicked.connect(self.on_element_selected)
        
        structure_layout.addWidget(self.structure_tree)
        
        layout.addWidget(structure_group)
        
        return panel
    
    def _create_metadata_details_panel(self) -> QWidget:
        """
        Create right panel with metadata details
        """
        panel = QWidget()
        layout = QVBoxLayout(panel)
        
        # Tabs for different metadata views
        tabs = QTabWidget()
        
        # Basic metadata tab
        basic_tab = QWidget()
        basic_layout = QFormLayout(basic_tab)
        
        self.id_label = QLabel("N/A")
        basic_layout.addRow("ID:", self.id_label)
        
        self.document_id_label = QLabel("N/A")
        basic_layout.addRow("Document ID:", self.document_id_label)
        
        self.page_num_label = QLabel("N/A")
        basic_layout.addRow("Page Number:", self.page_num_label)
        
        self.section_id_label = QLabel("N/A")
        basic_layout.addRow("Section ID:", self.section_id_label)
        
        self.section_title_label = QLabel("N/A")
        basic_layout.addRow("Section Title:", self.section_title_label)
        
        self.section_level_label = QLabel("N/A")
        basic_layout.addRow("Section Level:", self.section_level_label)
        
        tabs.addTab(basic_tab, "Basic")
        
        # Raw metadata tab
        raw_tab = QWidget()
        raw_layout = QVBoxLayout(raw_tab)
        
        self.raw_metadata_view = QTextEdit()
        self.raw_metadata_view.setReadOnly(True)
        self.raw_metadata_view.setFontFamily("Consolas")
        self.raw_metadata_view.setFontPointSize(10)
        
        raw_layout.addWidget(self.raw_metadata_view)
        tabs.addTab(raw_tab, "Raw JSON")
        
        # Extended metadata tab
        extended_tab = QWidget()
        extended_layout = QFormLayout(extended_tab)
        
        self.chunk_type_label = QLabel("N/A")
        extended_layout.addRow("Chunk Type:", self.chunk_type_label)
        
        self.pipeline_run_label = QLabel("N/A")
        extended_layout.addRow("Pipeline Run ID:", self.pipeline_run_label)
        
        self.source_type_label = QLabel("N/A")
        extended_layout.addRow("Source Type:", self.source_type_label)
        
        self.line_num_label = QLabel("N/A")
        extended_layout.addRow("Line Number:", self.line_num_label)
        
        tabs.addTab(extended_tab, "Extended")
        
        layout.addWidget(tabs)
        
        return panel
    
    def setup_connections(self):
        """
        Set up signal connections
        """
        pass
    
    def inspect_document(self, document: Document):
        """
        Inspect document structure and metadata
        """
        self.current_document = document
        
        # Clear structure tree
        self.structure_tree.clear()
        
        # Add document root
        doc_item = QTreeWidgetItem(self.structure_tree)
        doc_item.setText(0, document.title or "Untitled Document")
        doc_item.setText(1, "Document")
        doc_item.setText(2, "")
        
        # Add pages
        pages_item = QTreeWidgetItem(doc_item)
        pages_item.setText(0, "Pages")
        pages_item.setText(1, "Collection")
        pages_item.setText(2, str(len(document.pages)))
        
        for page in document.pages:
            page_item = QTreeWidgetItem(pages_item)
            page_item.setText(0, f"Page {page.number}")
            page_item.setText(1, "Page")
            page_item.setText(2, f"{len(page.blocks)} blocks")
        
        # Add sections
        sections_item = QTreeWidgetItem(doc_item)
        sections_item.setText(0, "Sections")
        sections_item.setText(1, "Collection")
        sections_item.setText(2, str(len(document.sections)))
        
        for section in document.sections:
            section_item = QTreeWidgetItem(sections_item)
            section_item.setText(0, section.title[:50] + "..." if len(section.title) > 50 else section.title)
            section_item.setText(1, f"Level {section.level}")
            section_item.setText(2, f"pg {section.start_page}-{section.end_page}")
        
        # Expand all items
        self.structure_tree.expandAll()
    
    def inspect_chunk(self, chunk: Chunk):
        """
        Inspect chunk metadata
        """
        self.current_chunk = chunk
        meta = chunk.meta
        
        # Update basic metadata labels
        self.id_label.setText(chunk.id)
        self.document_id_label.setText(meta.document_id)
        self.page_num_label.setText(str(meta.page_num) if meta.page_num else "N/A")
        self.section_id_label.setText(meta.section_id)
        self.section_title_label.setText(meta.section_title)
        self.section_level_label.setText(str(meta.section_level))
        
        # Update extended metadata labels
        self.chunk_type_label.setText(str(meta.chunk_type))
        self.pipeline_run_label.setText(meta.pipeline_run_id or "N/A")
        self.source_type_label.setText(meta.source_type)
        self.line_num_label.setText(str(meta.line_num) if meta.line_num else "N/A")
        
        # Update raw metadata view
        raw_data = {
            "chunk_id": chunk.id,
            "text_preview": chunk.text[:100] + "..." if len(chunk.text) > 100 else chunk.text,
            "metadata": meta.__dict__,
            "extraction_results": chunk.extraction_results
        }
        
        self.raw_metadata_view.setText(json.dumps(raw_data, indent=2, ensure_ascii=False, default=str))
    
    def on_element_selected(self, item: QTreeWidgetItem, column: int):
        """
        Handle element selection in structure tree
        """
        # This would trigger inspection of the selected element
        element_name = item.text(0)
        element_type = item.text(1)
        
        # For now, just update the status
        self.status_label.setText(f"Selected: {element_name} ({element_type})")
    
    def clear_inspection(self):
        """
        Clear current inspection
        """
        self.current_document = None
        self.current_chunk = None
        self.structure_tree.clear()
        
        # Reset all labels
        for label in [self.id_label, self.document_id_label, self.page_num_label,
                     self.section_id_label, self.section_title_label, self.section_level_label,
                     self.chunk_type_label, self.pipeline_run_label, self.source_type_label,
                     self.line_num_label]:
            label.setText("N/A")
        
        self.raw_metadata_view.clear()
    
    def refresh(self):
        """
        Refresh the metadata inspector
        """
        if self.current_document:
            self.inspect_document(self.current_document)
        elif self.current_chunk:
            self.inspect_chunk(self.current_chunk)