#!/usr/bin/env python3
"""
Document Uploader Widget - Upload and analyze document styles with header configuration
Supports responsive layout and scrolling
"""

import sys
from pathlib import Path
from typing import List, Dict, Any, Optional, Union
import os
import json
import tempfile
from datetime import datetime
import re

# Add project root to Python path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from PyQt6.QtWidgets import (QWidget, QVBoxLayout, QHBoxLayout, QTableWidget, 
                           QTableWidgetItem, QHeaderView, QGroupBox, QFileDialog, 
                           QProgressBar, QLabel, QCheckBox, QSpinBox, QLineEdit, 
                           QFormLayout, QComboBox, QMessageBox, QSplitter, QTextEdit,
                           QTabWidget, QTreeWidget, QTreeWidgetItem, QInputDialog,
                           QRadioButton, QButtonGroup, QSizePolicy, QScrollArea,
                           QAbstractItemView, QDialog, QDialogButtonBox, QPushButton,
                           QStatusBar, QTreeWidgetItem)
from PyQt6.QtCore import Qt, QThread, pyqtSignal, QTimer
from PyQt6.QtGui import QFont, QColor, QTextCharFormat, QAction, QKeySequence

from domain.chunk import Chunk, Metadata, ChunkType
from domain.pipeline import PipelineConfig, PipelineStepConfig, PipelineRun, PipelineStatus, StepType
from domain.document import Document, Page, Section, DocumentFormat
from infrastructure.loaders.document_factory import DocumentFactory
from utilities.document_style_analyzer import DocumentStyleAnalyzer, HeaderAssignment

class DocumentUploadWorker(QThread):
    """
    Worker thread for document analysis
    """
    progress_signal = pyqtSignal(int, str)  # (progress%, message)
    result_signal = pyqtSignal(list)  # List of analysis results
    error_signal = pyqtSignal(str)  # Error message
    
    def __init__(self, document_paths: List[str], pipeline_manager):
        super().__init__()
        self.document_paths = document_paths
        self.pipeline_manager = pipeline_manager
    
    def run(self):
        """
        Analyze documents in background thread
        """
        try:
            results = []
            total = len(self.document_paths)
            
            for i, path in enumerate(self.document_paths):
                # Update progress
                progress = int((i / total) * 100)
                self.progress_signal.emit(progress, f"Analyzing {os.path.basename(path)}...")
                
                try:
                    # Use DocumentFactory to create loader and get metadata
                    from infrastructure.loaders.document_factory import DocumentFactory
                    loader = DocumentFactory.create_loader(path)
                    metadata = loader.get_document_metadata(path)
                    
                    # Analyze document styles using the analyzer
                    styles = DocumentStyleAnalyzer.analyze_document_styles(path)
                    
                    results.append({
                        "path": path,
                        "success": True,
                        "styles": styles,
                        "style_count": len(styles),
                        "style_samples": [s.text_sample[:50] for s in styles[:5]],  # First 5 samples
                        "metadata": metadata,
                        "analysis_time": datetime.now().isoformat()
                    })
                except Exception as e:
                    results.append({
                        "path": path,
                        "success": False,
                        "error": str(e),
                        "styles": [],
                        "style_count": 0,
                        "analysis_time": datetime.now().isoformat()
                    })
            
            self.result_signal.emit(results)
            self.progress_signal.emit(100, "Analysis complete!")
            
        except Exception as e:
            self.error_signal.emit(str(e))

class DocumentUploader(QWidget):
    """
    Widget for uploading and analyzing documents with header configuration
    Features responsive layout and scrolling
    """
    
    def __init__(self, db, pipeline_manager):
        super().__init__()
        self.db = db
        self.pipeline_manager = pipeline_manager
        self.current_document_path = ""
        self.current_styles = []
        self.current_header_config = None
        self.analysis_worker = None
        self.selected_document_row = -1
        
        # Store full paths for all documents
        self.document_paths: Dict[int, str] = {}  # row_index -> full_path
        
        self.setup_ui()
        self.setup_connections()
    
    def setup_ui(self):
        """
        Set up the user interface with responsive layout and scrolling
        """
        layout = QVBoxLayout(self)
        
        # Controls group
        controls_group = QGroupBox("Upload Controls")
        controls_layout = QHBoxLayout(controls_group)
        
        self.upload_button = QPushButton("Select Documents")
        self.upload_button.clicked.connect(self.select_documents)
        controls_layout.addWidget(self.upload_button)
        
        self.analyze_button = QPushButton("Analyze Styles")
        self.analyze_button.clicked.connect(self.analyze_current_document)
        self.analyze_button.setEnabled(False)  # Initially disabled
        controls_layout.addWidget(self.analyze_button)
        
        self.config_button = QPushButton("Configure Headers")
        self.config_button.clicked.connect(self.configure_header_detection)
        # Enable config button even without document analysis
        self.config_button.setEnabled(True)
        controls_layout.addWidget(self.config_button)
        
        self.save_config_button = QPushButton("Save Configuration")
        self.save_config_button.clicked.connect(self.save_header_configuration)
        self.save_config_button.setEnabled(False)  # Initially disabled
        controls_layout.addWidget(self.save_config_button)
        
        # Delete buttons
        self.delete_selected_button = QPushButton("Delete Selected")
        self.delete_selected_button.clicked.connect(self.delete_selected_documents)
        self.delete_selected_button.setStyleSheet("QPushButton { background-color: #f44336; color: white; }")
        controls_layout.addWidget(self.delete_selected_button)
        
        self.clear_all_button = QPushButton("Clear All")
        self.clear_all_button.clicked.connect(self.clear_all_documents)
        self.clear_all_button.setStyleSheet("QPushButton { background-color: #f44336; color: white; }")
        controls_layout.addWidget(self.clear_all_button)
        
        controls_layout.addStretch()
        
        layout.addWidget(controls_group)
        
        # Main content area with splitter
        main_splitter = QSplitter(Qt.Orientation.Horizontal)
        
        # Left panel - Document selection and styles (with scrolling)
        left_panel = self._create_document_selection_panel()
        main_splitter.addWidget(left_panel)
        
        # Right panel - Header configuration (with scrolling)
        right_panel = self._create_header_configuration_panel()
        main_splitter.addWidget(right_panel)
        
        main_splitter.setSizes([600, 800])
        layout.addWidget(main_splitter)
        
        # Progress bar and status
        self.progress_bar = QProgressBar()
        self.progress_bar.setVisible(False)
        layout.addWidget(self.progress_bar)
        
        # Add status bar to layout
        self.status_bar = QStatusBar()
        layout.addWidget(self.status_bar)
        
        # Status label for additional information
        self.status_label = QLabel("Ready")
        layout.addWidget(self.status_label)
    
    def _create_document_selection_panel(self) -> QWidget:
        """
        Create left panel with document selection and style analysis
        """
        panel = QWidget()
        layout = QVBoxLayout(panel)
        
        # Uploaded documents table - Add scrolling
        table_group = QGroupBox("Uploaded Documents")
        table_layout = QVBoxLayout(table_group)
        
        # Create scroll area for table
        table_scroll = QScrollArea()
        table_scroll.setWidgetResizable(True)
        
        self.documents_table = QTableWidget()
        self.documents_table.setColumnCount(5)
        self.documents_table.setHorizontalHeaderLabels(["File Name", "Format", "Size", "Status", "Path"])
        self.documents_table.horizontalHeader().setSectionResizeMode(0, QHeaderView.ResizeMode.Stretch)
        self.documents_table.horizontalHeader().setSectionResizeMode(1, QHeaderView.ResizeMode.ResizeToContents)
        self.documents_table.horizontalHeader().setSectionResizeMode(2, QHeaderView.ResizeMode.ResizeToContents)
        self.documents_table.horizontalHeader().setSectionResizeMode(3, QHeaderView.ResizeMode.ResizeToContents)
        self.documents_table.horizontalHeader().setSectionResizeMode(4, QHeaderView.ResizeMode.Stretch)
        self.documents_table.setSelectionBehavior(QAbstractItemView.SelectionBehavior.SelectRows)
        self.documents_table.itemClicked.connect(self.on_document_selected)
        
        # Set table as scroll area widget
        table_scroll.setWidget(self.documents_table)
        table_layout.addWidget(table_scroll)
        
        layout.addWidget(table_group)
        
        # Style analysis results - Add scrolling
        styles_group = QGroupBox("Document Styles")
        styles_layout = QVBoxLayout(styles_group)
        
        # Tab widget for different analysis views with scrolling
        self.styles_tabs = QTabWidget()
        
        # Styles table tab with scrolling
        styles_table_widget = QWidget()
        styles_table_layout = QVBoxLayout(styles_table_widget)
        
        # Scroll area for styles table
        styles_table_scroll = QScrollArea()
        styles_table_scroll.setWidgetResizable(True)
        
        self.styles_table = QTableWidget()
        self.styles_table.setColumnCount(6)
        self.styles_table.setHorizontalHeaderLabels(["Font", "Size", "Bold", "Italic", "Color", "Sample"])
        self.styles_table.horizontalHeader().setSectionResizeMode(0, QHeaderView.ResizeMode.Stretch)
        self.styles_table.horizontalHeader().setSectionResizeMode(1, QHeaderView.ResizeMode.ResizeToContents)
        self.styles_table.horizontalHeader().setSectionResizeMode(2, QHeaderView.ResizeMode.ResizeToContents)
        self.styles_table.horizontalHeader().setSectionResizeMode(3, QHeaderView.ResizeMode.ResizeToContents)
        self.styles_table.horizontalHeader().setSectionResizeMode(4, QHeaderView.ResizeMode.ResizeToContents)
        self.styles_table.horizontalHeader().setSectionResizeMode(5, QHeaderView.ResizeMode.Stretch)
        
        # Set table as scroll area widget
        styles_table_scroll.setWidget(self.styles_table)
        styles_table_layout.addWidget(styles_table_scroll)
        self.styles_tabs.addTab(styles_table_widget, "Styles Table")
        
        # Styles tree view tab with scrolling
        styles_tree_widget = QWidget()
        styles_tree_layout = QVBoxLayout(styles_tree_widget)
        
        # Scroll area for styles tree
        styles_tree_scroll = QScrollArea()
        styles_tree_scroll.setWidgetResizable(True)
        
        self.styles_tree = QTreeWidget()
        self.styles_tree.setHeaderLabels(["Style", "Properties", "Usage Count", "Sample Text"])
        self.styles_tree.header().setSectionResizeMode(0, QHeaderView.ResizeMode.Stretch)
        self.styles_tree.header().setSectionResizeMode(1, QHeaderView.ResizeMode.Stretch)
        self.styles_tree.header().setSectionResizeMode(2, QHeaderView.ResizeMode.ResizeToContents)
        self.styles_tree.header().setSectionResizeMode(3, QHeaderView.ResizeMode.Stretch)
        
        # Set tree as scroll area widget
        styles_tree_scroll.setWidget(self.styles_tree)
        styles_tree_layout.addWidget(styles_tree_scroll)
        self.styles_tabs.addTab(styles_tree_widget, "Styles Tree")
        
        styles_layout.addWidget(self.styles_tabs)
        layout.addWidget(styles_group)
        
        return panel
    
    def _create_header_configuration_panel(self) -> QWidget:
        """
        Create right panel for header configuration with scrolling
        """
        panel = QWidget()
        layout = QVBoxLayout(panel)
        
        # Configuration method selection
        method_group = QGroupBox("Header Detection Method")
        method_layout = QVBoxLayout(method_group)
        
        self.method_button_group = QButtonGroup()
        
        self.style_based_radio = QRadioButton("Detect from Document Styles")
        self.style_based_radio.setChecked(True)
        self.method_button_group.addButton(self.style_based_radio)
        method_layout.addWidget(self.style_based_radio)
        
        self.exact_phrases_radio = QRadioButton("Define Exact Phrases")
        self.method_button_group.addButton(self.exact_phrases_radio)
        method_layout.addWidget(self.exact_phrases_radio)
        
        layout.addWidget(method_group)
        
        # Configuration area with scrolling
        config_group = QGroupBox("Header Configuration")
        config_layout = QVBoxLayout(config_group)
        
        # Create scroll area for configuration
        config_scroll = QScrollArea()
        config_scroll.setWidgetResizable(True)
        
        # Configuration widget
        config_widget = QWidget()
        config_widget_layout = QVBoxLayout(config_widget)
        
        # For style-based detection
        self.style_config_widget = self._create_style_config_widget()
        config_widget_layout.addWidget(self.style_config_widget)
        
        # For exact phrases detection
        self.phrase_config_widget = self._create_phrase_config_widget()
        self.phrase_config_widget.setVisible(False)  # Hidden initially
        config_widget_layout.addWidget(self.phrase_config_widget)
        
        # Set config widget in scroll area
        config_scroll.setWidget(config_widget)
        config_layout.addWidget(config_scroll)
        
        layout.addWidget(config_group)
        
        # Filtering options
        filter_group = QGroupBox("Global Filtering Options")
        filter_layout = QFormLayout(filter_group)
        
        self.min_length_spin = QSpinBox()
        self.min_length_spin.setMinimum(1)
        self.min_length_spin.setMaximum(1000)
        self.min_length_spin.setValue(5)
        filter_layout.addRow("Min Length:", self.min_length_spin)
        
        self.max_length_spin = QSpinBox()
        self.max_length_spin.setMinimum(10)
        self.max_length_spin.setMaximum(10000)
        self.max_length_spin.setValue(200)
        filter_layout.addRow("Max Length:", self.max_length_spin)
        
        self.include_words_edit = QLineEdit()
        self.include_words_edit.setPlaceholderText("comma, separated, words")
        filter_layout.addRow("Include Words:", self.include_words_edit)
        
        self.exclude_words_edit = QLineEdit()
        self.exclude_words_edit.setPlaceholderText("spam, footer, page")
        filter_layout.addRow("Exclude Words:", self.exclude_words_edit)
        
        self.include_regex_edit = QLineEdit()
        self.include_regex_edit.setPlaceholderText(r"^\d+\.\s+.*$")  # Example: "1. Title"
        filter_layout.addRow("Include Regex:", self.include_regex_edit)
        
        self.exclude_regex_edit = QLineEdit()
        self.exclude_regex_edit.setPlaceholderText(r"(?i)section|table|figure")
        filter_layout.addRow("Exclude Regex:", self.exclude_regex_edit)
        
        layout.addWidget(filter_group)
        
        # Header assignments preview with scrolling
        preview_group = QGroupBox("Header Assignments Preview")
        preview_layout = QVBoxLayout(preview_group)
        
        # Scroll area for preview
        preview_scroll = QScrollArea()
        preview_scroll.setWidgetResizable(True)
        
        self.assignments_preview = QTextEdit()
        self.assignments_preview.setReadOnly(True)
        self.assignments_preview.setFont(QFont("Consolas", 10))
        
        # Set preview in scroll area
        preview_scroll.setWidget(self.assignments_preview)
        preview_layout.addWidget(preview_scroll)
        
        layout.addWidget(preview_group)
        
        return panel
    
    def _create_style_config_widget(self) -> QWidget:
        """
        Create widget for style-based header configuration with scrolling
        """
        widget = QWidget()
        layout = QVBoxLayout(widget)
        
        # Instructions
        instructions = QLabel(
            "Configure header styles:\n"
            "• After analyzing document, detected styles will appear in the table below\n"
            "• Select a row in the assignment table\n"
            "• Use the dropdown to select header level (1-5)\n"
            "• Add optional filtering rules\n\n"
            "Or add custom styles manually without document analysis:"
        )
        instructions.setWordWrap(True)
        instructions.setStyleSheet("background-color: #f0f8ff; padding: 10px; border: 1px solid #ccc;")
        layout.addWidget(instructions)
        
        # Add manual style button
        self.add_manual_style_button = QPushButton("Add Custom Style Manually")
        self.add_manual_style_button.clicked.connect(self.add_custom_style_manually)
        layout.addWidget(self.add_manual_style_button)
        
        # Style assignment table with scrolling
        assignment_group = QGroupBox("Style Assignments")
        assignment_layout = QVBoxLayout(assignment_group)
        
        # Scroll area for assignment table
        assignment_scroll = QScrollArea()
        assignment_scroll.setWidgetResizable(True)
        
        self.style_assignment_table = QTableWidget()
        self.style_assignment_table.setColumnCount(3)
        self.style_assignment_table.setHorizontalHeaderLabels(["Style", "Header Level", "Filters"])
        self.style_assignment_table.horizontalHeader().setSectionResizeMode(0, QHeaderView.ResizeMode.Stretch)
        self.style_assignment_table.horizontalHeader().setSectionResizeMode(1, QHeaderView.ResizeMode.ResizeToContents)
        self.style_assignment_table.horizontalHeader().setSectionResizeMode(2, QHeaderView.ResizeMode.ResizeToContents)
        
        # Make the table taller for better visibility
        self.style_assignment_table.setMinimumHeight(300)
        self.style_assignment_table.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Expanding)
        
        # Set table in scroll area
        assignment_scroll.setWidget(self.style_assignment_table)
        assignment_layout.addWidget(assignment_scroll)
        
        # Assignment controls
        assignment_controls = QHBoxLayout()
        
        self.assign_level_button = QPushButton("Assign Level")
        self.assign_level_button.clicked.connect(self.assign_style_to_level)
        assignment_controls.addWidget(self.assign_level_button)
        
        self.remove_assignment_button = QPushButton("Remove Assignment")
        self.remove_assignment_button.clicked.connect(self.remove_style_assignment)
        assignment_controls.addWidget(self.remove_assignment_button)
        
        assignment_controls.addStretch()
        
        assignment_layout.addLayout(assignment_controls)
        
        layout.addWidget(assignment_group)
        
        return widget
    
    def _create_phrase_config_widget(self) -> QWidget:
        """
        Create widget for exact phrase header configuration with scrolling
        """
        widget = QWidget()
        layout = QVBoxLayout(widget)
        
        # Instructions
        instructions = QLabel(
            "Define exact header phrases:\n"
            "• Enter phrases for each header level\n"
            "• Use regex patterns if needed\n"
            "• Phrases will be matched exactly"
        )
        instructions.setWordWrap(True)
        instructions.setStyleSheet("background-color: #f5f5f5; padding: 10px; border: 1px solid #ccc;")
        layout.addWidget(instructions)
        
        # Phrase assignment table with scrolling
        phrase_group = QGroupBox("Phrase Assignments")
        phrase_layout = QVBoxLayout(phrase_group)
        
        # Scroll area for phrase table
        phrase_scroll = QScrollArea()
        phrase_scroll.setWidgetResizable(True)
        
        self.phrase_assignment_table = QTableWidget()
        self.phrase_assignment_table.setColumnCount(4)
        self.phrase_assignment_table.setHorizontalHeaderLabels(["Level", "Phrase/Pattern", "Case Sensitive", "Actions"])
        self.phrase_assignment_table.horizontalHeader().setSectionResizeMode(0, QHeaderView.ResizeMode.ResizeToContents)
        self.phrase_assignment_table.horizontalHeader().setSectionResizeMode(1, QHeaderView.ResizeMode.Stretch)
        self.phrase_assignment_table.horizontalHeader().setSectionResizeMode(2, QHeaderView.ResizeMode.ResizeToContents)
        self.phrase_assignment_table.horizontalHeader().setSectionResizeMode(3, QHeaderView.ResizeMode.ResizeToContents)
        
        # Make the table taller
        self.phrase_assignment_table.setMinimumHeight(300)
        self.phrase_assignment_table.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Expanding)
        
        # Set table in scroll area
        phrase_scroll.setWidget(self.phrase_assignment_table)
        phrase_layout.addWidget(phrase_scroll)
        
        # Assignment controls
        phrase_controls = QHBoxLayout()
        
        self.add_phrase_button = QPushButton("Add Phrase")
        self.add_phrase_button.clicked.connect(self.add_phrase_assignment)
        phrase_controls.addWidget(self.add_phrase_button)
        
        self.remove_phrase_button = QPushButton("Remove Phrase")
        self.remove_phrase_button.clicked.connect(self.remove_phrase_assignment)
        phrase_controls.addWidget(self.remove_phrase_button)
        
        phrase_controls.addStretch()
        
        phrase_layout.addLayout(phrase_controls)
        
        layout.addWidget(phrase_group)
        
        return widget
    
    def setup_connections(self):
        """
        Set up signal connections
        """
        # Connect radio buttons to show/hide config widgets and enable save button
        self.style_based_radio.toggled.connect(self._on_method_toggled)
        self.exact_phrases_radio.toggled.connect(self._on_method_toggled)
    
    def _on_method_toggled(self, checked: bool):
        """
        Handle header detection method toggle
        Args:
            checked: Whether the radio button is checked
        """
        if not checked:
            return  # Only react when a button is checked, not unchecked
        
        if self.style_based_radio.isChecked():
            self.style_config_widget.setVisible(True)
            self.phrase_config_widget.setVisible(False)
        else:  # exact_phrases_radio is checked
            self.style_config_widget.setVisible(False)
            self.phrase_config_widget.setVisible(True)
        
        # Enable save button when method is selected
        self.save_config_button.setEnabled(True)
    
    def select_documents(self):
        """
        Open file dialog to select documents from any location
        """
        file_paths, _ = QFileDialog.getOpenFileNames(
            self, 
            "Select Documents", 
            "", 
            "Documents (*.pdf *.docx *.txt *.rtf *.odt);;PDF Files (*.pdf);;DOCX Files (*.docx);;TXT Files (*.txt);;All Files (*)"
        )
        
        if file_paths:
            self._add_documents_to_table(file_paths)
    
    def _add_documents_to_table(self, file_paths: List[str]):
        """
        Add document paths to table with full path information
        """
        current_row = self.documents_table.rowCount()
        self.documents_table.setRowCount(current_row + len(file_paths))
        
        for i, path in enumerate(file_paths):
            row = current_row + i
            abs_path = os.path.abspath(path)  # Use absolute path
            file_name = os.path.basename(path)  
            
            # Store full path for this row
            self.document_paths[row] = abs_path
            
            # File name
            self.documents_table.setItem(row, 0, QTableWidgetItem(file_name))
            
            # Format
            ext = Path(path).suffix.lower()
            self.documents_table.setItem(row, 1, QTableWidgetItem(ext[1:]))
            
            # Size
            size_mb = os.path.getsize(abs_path) / (1024 * 1024)
            self.documents_table.setItem(row, 2, QTableWidgetItem(f"{size_mb:.2f} MB"))
            
            # Status
            self.documents_table.setItem(row, 3, QTableWidgetItem("Ready"))
            
            # Full path (hidden in GUI but available for analysis)
            self.documents_table.setItem(row, 4, QTableWidgetItem(abs_path))
        
        # Enable analyze button if documents are selected
        self.analyze_button.setEnabled(True)
        self.selected_document_row = 0  # Select first document
        
        # Update both status_bar and status_label
        self.status_bar.showMessage(f"Added {len(file_paths)} documents to list")
        self.status_label.setText(f"Ready - {len(file_paths)} documents loaded")
    
    def on_document_selected(self, item: QTableWidgetItem):
        """
        Handle document selection
        """
        self.selected_document_row = item.row()
        file_name = self.documents_table.item(self.selected_document_row, 0).text()
        file_ext = self.documents_table.item(self.selected_document_row, 1).text()
        
        # Enable analyze button
        self.analyze_button.setEnabled(True)
        
        # Update both status_bar and status_label
        self.status_bar.showMessage(f"Selected: {file_name}")
        self.status_label.setText(f"Selected: {file_name}")
        
        # Clear previous analysis results
        self.current_styles = []
        self.current_header_config = None
        self.config_button.setEnabled(True)  # Always enable config button
        self.save_config_button.setEnabled(False)
        
        # Clear tables
        self.styles_table.setRowCount(0)
        self.styles_tree.clear()
    
    def analyze_current_document(self):
        """
        Analyze currently selected document for styles (using full path)
        """
        if self.selected_document_row < 0 or self.selected_document_row >= self.documents_table.rowCount():
            QMessageBox.warning(self, "Warning", "Please select a document to analyze")
            return
        
        # Get full path from stored paths
        file_path = self.document_paths.get(self.selected_document_row)
        if not file_path or not os.path.exists(file_path):
            QMessageBox.warning(self, "Warning", "Document path not found or invalid")
            return
        
        self.current_document_path = file_path
        file_name = os.path.basename(file_path)  
        
        # Start analysis in background thread
        self.analysis_worker = DocumentUploadWorker([file_path], self.pipeline_manager)
        self.analysis_worker.progress_signal.connect(self._on_analysis_progress)
        self.analysis_worker.result_signal.connect(self._on_analysis_complete)
        self.analysis_worker.error_signal.connect(self._on_analysis_error)
        
        # Show progress bar and update status
        self.progress_bar.setVisible(True)
        self.progress_bar.setValue(0)
        self.status_bar.showMessage(f"Analyzing document: {file_name}")
        self.status_label.setText(f"Analyzing: {file_name}")
        
        self.analysis_worker.start()
    
    def _on_analysis_progress(self, progress: int, message: str):
        """
        Handle analysis progress update
        """
        self.progress_bar.setValue(progress)
        # Update both status_bar and status_label
        self.status_bar.showMessage(message)
        self.status_label.setText(message)
    
    def _on_analysis_complete(self, results: List[Dict[str, Any]]):
        """
        Handle analysis completion
        """
        self.progress_bar.setVisible(False)
        
        if results and len(results) > 0:
            result = results[0]  # First result since we analyze one document
            if result["success"]:
                self.current_styles = result["styles"]
                
                # Update styles table
                self._populate_styles_table(self.current_styles)
                
                # Update styles tree
                self._populate_styles_tree(self.current_styles)
                
                # Enable configuration button
                self.config_button.setEnabled(True)
                
                # Update status in table
                if self.selected_document_row >= 0:
                    self.documents_table.setItem(
                        self.selected_document_row, 
                        3, 
                        QTableWidgetItem(f"Analyzed ({result['style_count']} styles)")
                    )
                
                QMessageBox.information(
                    self, 
                    "Analysis Complete", 
                    f"Found {result['style_count']} unique text styles in the document!"
                )
                
                # Update both status_bar and status_label
                self.status_bar.showMessage(f"Analysis complete: {result['style_count']} styles detected in {result['path']}")
                self.status_label.setText(f"Analysis complete: {result['style_count']} styles detected")
            else:
                QMessageBox.critical(
                    self, 
                    "Analysis Error", 
                    f"Failed to analyze document: {result.get('error', 'Unknown error')}"
                )
                
                # Update both status_bar and status_label
                self.status_bar.showMessage(f"Analysis failed: {result.get('error', 'Unknown error')}")
                self.status_label.setText(f"Analysis failed: {result.get('error', 'Unknown error')}")
        
        # Clean up worker
        self.analysis_worker = None
    
    def _on_analysis_error(self, error: str):
        """
        Handle analysis error
        """
        self.progress_bar.setVisible(False)
        
        QMessageBox.critical(
            self, 
            "Analysis Error", 
            f"Failed to analyze document: {error}"
        )
        
        # Update both status_bar and status_label
        self.status_bar.showMessage(f"Analysis failed: {error}")
        self.status_label.setText(f"Analysis failed: {error}")
        
        # Clean up worker
        if self.analysis_worker:
            self.analysis_worker = None
    
    def _populate_styles_table(self, styles: List):
        """
        Populate styles table with analyzed styles
        """
        self.styles_table.setRowCount(len(styles))
        
        for i, style in enumerate(styles):
            self.styles_table.setItem(i, 0, QTableWidgetItem(
                style.style_name or style.font_name or style.font_family or "Unknown"
            ))
            self.styles_table.setItem(i, 1, QTableWidgetItem(
                str(style.font_size) if style.font_size else "N/A"
            ))
            self.styles_table.setItem(i, 2, QTableWidgetItem(
                "Yes" if style.is_bold else "No"
            ))
            self.styles_table.setItem(i, 3, QTableWidgetItem(
                "Yes" if style.is_italic else "No"
            ))
            self.styles_table.setItem(i, 4, QTableWidgetItem(
                style.text_color or "N/A"
            ))
            self.styles_table.setItem(i, 5, QTableWidgetItem(
                style.text_sample[:50] + "..." if len(style.text_sample) > 50 else style.text_sample
            ))
    
    def _populate_styles_tree(self, styles: List):
        """
        Populate styles tree with analyzed styles
        """
        self.styles_tree.clear()
        
        for style in styles:
            item = QTreeWidgetItem(self.styles_tree)
            item.setText(0, style.style_name or style.font_name or style.font_family or "Unknown")
            item.setText(1, f"Size: {style.font_size}px, Bold: {style.is_bold}, Italic: {style.is_italic}")
            item.setText(2, "1")  # Usage count (would need to calculate from document)
            item.setText(3, style.text_sample[:100] + "..." if len(style.text_sample) > 100 else style.text_sample)
    
    def configure_header_detection(self):
        """
        Configure header detection method and parameters
        This can now work with or without document analysis
        """
        if self.style_based_radio.isChecked():
            if not self.current_styles:
                # No document analyzed yet, but user can still configure manually
                QMessageBox.information(
                    self, 
                    "Info", 
                    "No document analyzed yet. You can configure headers manually using the 'Add Custom Style' button."
                )
            else:
                # Document analyzed, use detected styles
                self._configure_style_based_headers()
        else:
            # Phrase-based configuration doesn't require document analysis
            self._configure_phrase_based_headers()
        
        # Enable save button after configuration
        self.save_config_button.setEnabled(True)
        
        # Update both status_bar and status_label
        self.status_bar.showMessage("Header configuration ready")
        self.status_label.setText("Header configuration ready")
    
    def _configure_style_based_headers(self):
        """
        Configure header detection based on document styles
        """
        if not self.current_styles:
            return  # No styles to configure
        
        # Update style assignment table with current styles
        self.style_assignment_table.setRowCount(len(self.current_styles))
        
        for i, style in enumerate(self.current_styles):
            # Style description
            style_desc = f"{style.style_name or style.font_name or style.font_family} ({style.font_size}px)"
            self.style_assignment_table.setItem(i, 0, QTableWidgetItem(style_desc))
            
            # Level combo box
            level_combo = QComboBox()
            level_combo.addItems(["None", "Level 1", "Level 2", "Level 3", "Level 4", "Level 5"])
            level_combo.setCurrentIndex(0)  # Default to "None"
            self.style_assignment_table.setCellWidget(i, 1, level_combo)
            
            # Filters button
            filters_btn = QPushButton("Edit Filters")
            filters_btn.clicked.connect(lambda checked, s=style, row=i: self._edit_style_filters(s, row))
            self.style_assignment_table.setCellWidget(i, 2, filters_btn)
    
    def _configure_phrase_based_headers(self):
        """
        Configure header detection based on exact phrases
        """
        # Show phrase configuration interface (already created)
        # No document analysis needed for phrase-based configuration
        pass
    
    def _edit_style_filters(self, style, row: int):
        """
        Edit filtering options for a specific style
        """
        from PyQt6.QtWidgets import QDialog, QVBoxLayout, QDialogButtonBox, QFormLayout, QSpinBox
        
        dialog = QDialog(self)
        dialog.setWindowTitle(f"Edit Filters for Style: {style.style_name or style.font_name or style.font_family}")
        dialog.setGeometry(200, 200, 500, 400)
        
        layout = QVBoxLayout(dialog)
        form_layout = QFormLayout()
        
        # Include words
        include_words = QLineEdit()
        include_words.setPlaceholderText("comma, separated, words")
        form_layout.addRow("Include Words:", include_words)
        
        # Exclude words
        exclude_words = QLineEdit()
        exclude_words.setPlaceholderText("spam, footer, page")
        form_layout.addRow("Exclude Words:", exclude_words)
        
        # Include regex
        include_regex = QLineEdit()
        include_regex.setPlaceholderText(r"^\d+\.\s+.*$")
        form_layout.addRow("Include Regex:", include_regex)
        
        # Exclude regex
        exclude_regex = QLineEdit()
        exclude_regex.setPlaceholderText(r"(?i)section|table|figure")
        form_layout.addRow("Exclude Regex:", exclude_regex)
        
        # Min/max length
        min_length = QSpinBox()
        min_length.setMinimum(1)
        min_length.setMaximum(1000)
        min_length.setValue(5)
        form_layout.addRow("Min Length:", min_length)
        
        max_length = QSpinBox()
        max_length.setMinimum(10)
        max_length.setMaximum(10000)
        max_length.setValue(200)
        form_layout.addRow("Max Length:", max_length)
        
        layout.addLayout(form_layout)
        
        # Buttons
        buttons = QDialogButtonBox(QDialogButtonBox.StandardButton.Ok | QDialogButtonBox.StandardButton.Cancel)
        buttons.accepted.connect(dialog.accept)
        buttons.rejected.connect(dialog.reject)
        layout.addWidget(buttons)
        
        if dialog.exec() == QDialog.DialogCode.Accepted:
            # Store filters in the style object (or configuration)
            print(f"Filters applied to style {style.style_name}: include_words={include_words.text()}")
            QMessageBox.information(
                self, 
                "Filters Updated", 
                f"Filters applied to style: {style.style_name or style.font_name}"
            )
    
    def add_custom_style_manually(self):
        """
        Add custom style manually without document analysis
        """
        from PyQt6.QtWidgets import QDialog, QVBoxLayout, QDialogButtonBox, QFormLayout, QSpinBox
        
        dialog = QDialog(self)
        dialog.setWindowTitle("Add Custom Style")
        dialog.setGeometry(200, 200, 400, 300)
        
        layout = QVBoxLayout(dialog)
        form_layout = QFormLayout()
        
        # Style name
        style_name = QLineEdit()
        style_name.setPlaceholderText("e.g., 'Main Header', 'Section Title'")
        form_layout.addRow("Style Name:", style_name)
        
        # Font size
        font_size = QSpinBox()
        font_size.setMinimum(8)
        font_size.setMaximum(72)
        font_size.setValue(14)
        form_layout.addRow("Font Size:", font_size)
        
        # Bold checkbox
        is_bold = QCheckBox("Bold")
        form_layout.addRow("", is_bold)
        
        # Italic checkbox
        is_italic = QCheckBox("Italic")
        form_layout.addRow("", is_italic)
        
        # Font name
        font_name = QLineEdit()
        font_name.setPlaceholderText("e.g., Arial, Times New Roman")
        form_layout.addRow("Font Name:", font_name)
        
        layout.addLayout(form_layout)
        
        # Buttons
        buttons = QDialogButtonBox(QDialogButtonBox.StandardButton.Ok | QDialogButtonBox.StandardButton.Cancel)
        buttons.accepted.connect(dialog.accept)
        buttons.rejected.connect(dialog.reject)
        layout.addWidget(buttons)
        
        if dialog.exec() == QDialog.DialogCode.Accepted:
            # Add to assignment table
            row = self.style_assignment_table.rowCount()
            self.style_assignment_table.setRowCount(row + 1)
            
            # Style name
            self.style_assignment_table.setItem(row, 0, QTableWidgetItem(style_name.text()))
            
            # Level combo box
            level_combo = QComboBox()
            level_combo.addItems(["None", "Level 1", "Level 2", "Level 3", "Level 4", "Level 5"])
            self.style_assignment_table.setCellWidget(row, 1, level_combo)
            
            # Filters button
            filters_btn = QPushButton("Edit Filters")
            filters_btn.clicked.connect(lambda checked, row=row: self._edit_manual_style_filters(row))
            self.style_assignment_table.setCellWidget(row, 2, filters_btn)
            
            QMessageBox.information(
                self, 
                "Style Added", 
                f"Added custom style: {style_name.text()}"
            )
    
    def _edit_manual_style_filters(self, row: int):
        """
        Edit filters for manually added style
        """
        style_item = self.style_assignment_table.item(row, 0)
        style_name = style_item.text() if style_item else f"Style {row + 1}"
        
        from PyQt6.QtWidgets import QDialog, QVBoxLayout, QDialogButtonBox, QFormLayout, QSpinBox
        
        dialog = QDialog(self)
        dialog.setWindowTitle(f"Edit Filters for Style: {style_name}")
        dialog.setGeometry(200, 200, 500, 400)
        
        layout = QVBoxLayout(dialog)
        form_layout = QFormLayout()
        
        # Include words
        include_words = QLineEdit()
        include_words.setPlaceholderText("comma, separated, words")
        form_layout.addRow("Include Words:", include_words)
        
        # Exclude words
        exclude_words = QLineEdit()
        exclude_words.setPlaceholderText("spam, footer, page")
        form_layout.addRow("Exclude Words:", exclude_words)
        
        # Include regex
        include_regex = QLineEdit()
        include_regex.setPlaceholderText(r"^\d+\.\s+.*$")
        form_layout.addRow("Include Regex:", include_regex)
        
        # Exclude regex
        exclude_regex = QLineEdit()
        exclude_regex.setPlaceholderText(r"(?i)section|table|figure")
        form_layout.addRow("Exclude Regex:", exclude_regex)
        
        # Min/max length
        min_length = QSpinBox()
        min_length.setMinimum(1)
        min_length.setMaximum(1000)
        min_length.setValue(5)
        form_layout.addRow("Min Length:", min_length)
        
        max_length = QSpinBox()
        max_length.setMinimum(10)
        max_length.setMaximum(10000)
        max_length.setValue(200)
        form_layout.addRow("Max Length:", max_length)
        
        layout.addLayout(form_layout)
        
        # Buttons
        buttons = QDialogButtonBox(QDialogButtonBox.StandardButton.Ok | QDialogButtonBox.StandardButton.Cancel)
        buttons.accepted.connect(dialog.accept)
        buttons.rejected.connect(dialog.reject)
        layout.addWidget(buttons)
        
        if dialog.exec() == QDialog.DialogCode.Accepted:
            print(f"Filters applied to style {style_name}: include_words={include_words.text()}")
            QMessageBox.information(
                self, 
                "Filters Updated", 
                f"Filters applied to style: {style_name}"
            )
    
    def assign_style_to_level(self):
        """
        Assign selected style to header level
        """
        selected_items = self.style_assignment_table.selectedItems()
        if not selected_items:
            QMessageBox.warning(self, "Warning", "Please select a style to assign")
            return
        
        row = selected_items[0].row()
        level_combo = self.style_assignment_table.cellWidget(row, 1)  # Level combo box
        
        if level_combo:
            level_text = level_combo.currentText()
            if level_text != "None":
                QMessageBox.information(
                    self, 
                    "Assignment Complete", 
                    f"Assigned style to {level_text}"
                )
                self._update_assignments_preview()
            else:
                QMessageBox.information(self, "Info", "Style assignment removed")
                self._update_assignments_preview()
    
    def remove_style_assignment(self):
        """
        Remove selected style assignment
        """
        selected_items = self.style_assignment_table.selectedItems()
        if not selected_items:
            QMessageBox.warning(self, "Warning", "Please select a style to remove")
            return
        
        row = selected_items[0].row()
        level_combo = self.style_assignment_table.cellWidget(row, 1)
        if level_combo:
            level_combo.setCurrentIndex(0)  # Set to "None"
        
        self._update_assignments_preview()
    
    def add_phrase_assignment(self):
        """
        Add phrase-based header assignment
        """
        from PyQt6.QtWidgets import QDialog, QVBoxLayout, QDialogButtonBox, QFormLayout, QSpinBox
        
        dialog = QDialog(self)
        dialog.setWindowTitle("Add Phrase Assignment")
        dialog.setGeometry(200, 200, 400, 300)
        
        layout = QVBoxLayout(dialog)
        form_layout = QFormLayout()
        
        # Level selection
        level_spin = QSpinBox()
        level_spin.setMinimum(1)
        level_spin.setMaximum(5)
        level_spin.setValue(1)
        form_layout.addRow("Header Level:", level_spin)
        
        # Phrase input
        phrase_edit = QLineEdit()
        phrase_edit.setPlaceholderText("Enter exact phrase or regex pattern")
        form_layout.addRow("Phrase/Pattern:", phrase_edit)
        
        # Case sensitivity
        case_combo = QComboBox()
        case_combo.addItems(["Case Sensitive", "Case Insensitive"])
        form_layout.addRow("Case Sensitivity:", case_combo)
        
        # Include/Exclude option
        type_combo = QComboBox()
        type_combo.addItems(["Include (match)", "Exclude (skip)"])
        form_layout.addRow("Type:", type_combo)
        
        layout.addLayout(form_layout)
        
        # Buttons
        buttons = QDialogButtonBox(QDialogButtonBox.StandardButton.Ok | QDialogButtonBox.StandardButton.Cancel)
        buttons.accepted.connect(dialog.accept)
        buttons.rejected.connect(dialog.reject)
        layout.addWidget(buttons)
        
        if dialog.exec() == QDialog.DialogCode.Accepted:
            # Add to table
            row = self.phrase_assignment_table.rowCount()
            self.phrase_assignment_table.setRowCount(row + 1)
            
            # Level
            self.phrase_assignment_table.setItem(row, 0, QTableWidgetItem(f"Level {level_spin.value()}"))
            
            # Phrase
            self.phrase_assignment_table.setItem(row, 1, QTableWidgetItem(phrase_edit.text()))
            
            # Case sensitivity
            self.phrase_assignment_table.setItem(row, 2, QTableWidgetItem(case_combo.currentText()))
            
            # Actions column - Add remove button
            remove_btn = QPushButton("Remove")
            remove_btn.clicked.connect(lambda: self._remove_phrase_row(row))
            self.phrase_assignment_table.setCellWidget(row, 3, remove_btn)
            
            self._update_assignments_preview()
    
    def _remove_phrase_row(self, row: int):
        """
        Remove specific phrase assignment row
        """
        self.phrase_assignment_table.removeRow(row)
        self._update_assignments_preview()
    
    def remove_phrase_assignment(self):
        """
        Remove selected phrase assignment
        """
        selected_items = self.phrase_assignment_table.selectedItems()
        if not selected_items:
            QMessageBox.warning(self, "Warning", "Please select a phrase to remove")
            return
        
        row = selected_items[0].row()
        self.phrase_assignment_table.removeRow(row)
        self._update_assignments_preview()
    
    def _update_assignments_preview(self):
        """
        Update header assignments preview
        """
        preview_text = "Header Configuration Preview:\n\n"
        
        if self.style_based_radio.isChecked():
            # Style-based preview
            for i in range(self.style_assignment_table.rowCount()):
                style_item = self.style_assignment_table.item(i, 0)
                level_combo = self.style_assignment_table.cellWidget(i, 1)
                
                if style_item and level_combo and level_combo.currentText() != "None":
                    preview_text += f"• {style_item.text()} → {level_combo.currentText()}\n"
        else:
            # Phrase-based preview
            for i in range(self.phrase_assignment_table.rowCount()):
                level_item = self.phrase_assignment_table.item(i, 0)
                phrase_item = self.phrase_assignment_table.item(i, 1)
                case_item = self.phrase_assignment_table.item(i, 2)
                
                if level_item and phrase_item:
                    case_text = f" ({case_item.text()})" if case_item else ""
                    preview_text += f"• {phrase_item.text()} → {level_item.text()}{case_text}\n"
        
        self.assignments_preview.setText(
            preview_text if preview_text.strip() != "Header Configuration Preview:\n\n" 
            else "No header assignments configured yet."
        )
    
    def save_header_configuration(self):
        """
        Save header configuration to file
        This now works with both document analysis and manual configuration
        """
        # Generate configuration based on current settings
        config = self._generate_header_config()
        
        if not config:
            QMessageBox.warning(self, "Warning", "No header configuration to save")
            return
        
        # Save configuration file
        config_path, _ = QFileDialog.getSaveFileName(
            self, 
            "Save Header Configuration", 
            "", 
            "JSON Files (*.json);;All Files (*)"
        )
        
        if config_path:
            try:
                with open(config_path, 'w', encoding='utf-8') as f:
                    json.dump(config, f, indent=2, ensure_ascii=False, default=str)
                
                QMessageBox.information(
                    self, 
                    "Success", 
                    f"Header configuration saved to: {config_path}"
                )
                
                self.current_header_config = config
                self.save_config_button.setEnabled(False)  # Disable until changes are made
                
                #  Update both status_bar and status_label
                self.status_bar.showMessage(f"Configuration saved: {config_path}")
                self.status_label.setText(f"Configuration saved: {os.path.basename(config_path)}")
                
            except Exception as e:
                QMessageBox.critical(self, "Error", f"Failed to save configuration: {str(e)}")
    
    def _generate_header_config(self) -> Optional[Dict[str, Any]]:
        """
        Generate header configuration from current settings
        Works with both style-based and phrase-based configurations
        """
        if self.style_based_radio.isChecked():
            # Generate style-based configuration
            assignments = []
            
            for i in range(self.style_assignment_table.rowCount()):
                style_item = self.style_assignment_table.item(i, 0)
                level_combo = self.style_assignment_table.cellWidget(i, 1)
                
                if style_item and level_combo and level_combo.currentText() != "None":
                    level_text = level_combo.currentText()
                    level_num = int(level_text.split()[-1])  # "Level 1" -> 1
                    
                    # Get style properties from current_styles or create from table
                    if i < len(self.current_styles) and self.current_styles:
                        # Use analyzed style
                        style = self.current_styles[i]
                        assignment = {
                            "level": level_num,
                            "description": f"Level {level_num} header from style: {style_item.text()}",
                            "style": {
                                "font_size": style.font_size,
                                "is_bold": style.is_bold,
                                "is_italic": style.is_italic,
                                "font_name": style.font_name
                            }
                        }
                    else:
                        # Manual configuration - create style from user input
                        assignment = {
                            "level": level_num,
                            "description": f"Manual configuration: Level {level_num} header",
                            "manual_style": {
                                "style_name": style_item.text() if style_item else "Manual Style",
                                "font_size": 12,  # Default values
                                "is_bold": True,
                                "is_italic": False
                            }
                        }
                    
                    assignments.append(assignment)
        
            if assignments:
                return {
                    "header_assignments": assignments,
                    "detection_method": "style_based",
                    "global_filters": {
                        "min_length": self.min_length_spin.value(),
                        "max_length": self.max_length_spin.value(),
                        "include_words": [w.strip() for w in self.include_words_edit.text().split(',') if w.strip()],
                        "exclude_words": [w.strip() for w in self.exclude_words_edit.text().split(',') if w.strip()],
                        "include_regex": self.include_regex_edit.text() if self.include_regex_edit.text() else None,
                        "exclude_regex": self.exclude_regex_edit.text() if self.exclude_regex_edit.text() else None
                    }
                }
        else:  # phrase-based configuration
            # Generate phrase-based configuration
            assignments = []
            
            for i in range(self.phrase_assignment_table.rowCount()):
                level_item = self.phrase_assignment_table.item(i, 0)
                phrase_item = self.phrase_assignment_table.item(i, 1)
                case_item = self.phrase_assignment_table.item(i, 2)
                
                if level_item and phrase_item:
                    level_text = level_item.text()
                    level_num = int(level_text.split()[-1])  # "Level 1" -> 1
                    
                    assignment = {
                        "level": level_num,
                        "pattern": phrase_item.text(),
                        "case_sensitive": case_item.text() == "Case Sensitive" if case_item else True,
                        "description": f"Level {level_num} header: {phrase_item.text()}"
                    }
                    assignments.append(assignment)
            
            if assignments:
                return {
                    "header_assignments": assignments,
                    "detection_method": "phrase_based",
                    "global_filters": {
                        "min_length": self.min_length_spin.value(),
                        "max_length": self.max_length_spin.value(),
                        "include_words": [w.strip() for w in self.include_words_edit.text().split(',') if w.strip()],
                        "exclude_words": [w.strip() for w in self.exclude_words_edit.text().split(',') if w.strip()],
                        "include_regex": self.include_regex_edit.text() if self.include_regex_edit.text() else None,
                        "exclude_regex": self.exclude_regex_edit.text() if self.exclude_regex_edit.text() else None
                    }
                }
        
        return None
    
    def delete_selected_documents(self):
        """
        Delete selected documents from the table
        """
        selected_items = self.documents_table.selectedItems()
        if not selected_items:
            QMessageBox.warning(self, "Warning", "Please select documents to delete")
            return
        
        selected_rows = set(item.row() for item in selected_items)
        
        reply = QMessageBox.question(
            self, 
            'Confirm Deletion',
            f'Delete {len(selected_rows)} selected documents from the list?',
            QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No,
            QMessageBox.StandardButton.No
        )
        
        if reply == QMessageBox.StandardButton.Yes:
            # Remove in reverse order to maintain correct indices
            for row in sorted(selected_rows, reverse=True):
                self.documents_table.removeRow(row)
                # Remove from stored paths dictionary
                if row in self.document_paths:
                    del self.document_paths[row]
                
                # Adjust higher indices in the dictionary
                new_paths = {}
                for old_row, path in self.document_paths.items():
                    if old_row < row:
                        new_paths[old_row] = path
                    elif old_row > row:
                        new_paths[old_row - 1] = path  # Shift down by 1
                
                self.document_paths = new_paths
            
            # Update analyze button state
            if self.documents_table.rowCount() == 0:
                self.analyze_button.setEnabled(False)
                self.config_button.setEnabled(True)  # Still allow manual config
                self.save_config_button.setEnabled(False)
            
            # Update both status_bar and status_label
            self.status_bar.showMessage(f"Deleted {len(selected_rows)} documents")
            self.status_label.setText(f"Deleted {len(selected_rows)} documents")
    
    def clear_all_documents(self):
        """
        Clear all documents from the table
        """
        if self.documents_table.rowCount() == 0:
            return
        
        reply = QMessageBox.question(
            self, 
            'Confirm Clear All',
            f'Clear all {self.documents_table.rowCount()} documents from the list?',
            QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No,
            QMessageBox.StandardButton.No
        )
        
        if reply == QMessageBox.StandardButton.Yes:
            self.documents_table.setRowCount(0)
            self.document_paths.clear()
            self.selected_document_row = -1
            self.analyze_button.setEnabled(False)
            
            #  Update both status_bar and status_label
            self.status_bar.showMessage("All documents cleared")
            self.status_label.setText("All documents cleared")
    
    def refresh(self):
        """
        Refresh the document uploader
        """
        # Clear all selections and tables
        self.documents_table.clearSelection()
        self.styles_table.setRowCount(0)
        self.styles_tree.clear()
        self.style_assignment_table.setRowCount(0)
        self.phrase_assignment_table.setRowCount(0)
        self.assignments_preview.clear()
        
        # Disable buttons
        self.analyze_button.setEnabled(False)
        self.config_button.setEnabled(True)  # Keep config button enabled
        self.save_config_button.setEnabled(False)
        
        # Update both status_bar and status_label
        self.status_bar.showMessage("Refreshed")
        self.status_label.setText("Ready")