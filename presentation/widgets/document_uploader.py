#!/usr/bin/env python3
"""
Document Uploader Widget - Upload and analyze document styles with header configuration
"""

from PyQt6.QtWidgets import (QWidget, QVBoxLayout, QHBoxLayout, QPushButton, 
                           QTableWidget, QTableWidgetItem, QHeaderView, 
                           QGroupBox, QFileDialog, QProgressBar, QLabel,
                           QCheckBox, QSpinBox, QLineEdit, QFormLayout,
                           QComboBox, QMessageBox, QSplitter, QTextEdit,
                           QTabWidget, QTreeWidget, QTreeWidgetItem, QInputDialog,
                           QRadioButton, QButtonGroup, QSizePolicy)
from PyQt6.QtCore import Qt, QThread, pyqtSignal, QTimer
from PyQt6.QtGui import QFont, QColor, QTextCharFormat
from typing import List, Dict, Any, Optional
import os
from pathlib import Path
import tempfile
import json

from utilities.document_style_analyzer import DocumentStyleAnalyzer, TextStyle, HeaderAssignment

class StyleAnalysisWorker(QThread):
    """
    Worker thread for analyzing document styles
    """
    progress_signal = pyqtSignal(int, str)  # (progress%, message)
    result_signal = pyqtSignal(list)  # List of TextStyle objects
    error_signal = pyqtSignal(str)  # Error message
    
    def __init__(self, document_path: str):
        super().__init__()
        self.document_path = document_path
    
    def run(self):
        """
        Analyze document styles in background thread
        """
        try:
            self.progress_signal.emit(10, "Analyzing document structure...")
            
            # Analyze document styles
            styles = DocumentStyleAnalyzer.analyze_document_styles(self.document_path)
            
            self.progress_signal.emit(90, "Preparing results...")
            self.result_signal.emit(styles)
            self.progress_signal.emit(100, "Analysis complete!")
            
        except Exception as e:
            self.error_signal.emit(str(e))

class DocumentUploader(QWidget):
    """
    Widget for uploading and analyzing documents with header configuration
    """
    
    def __init__(self, db, pipeline_manager):
        super().__init__()
        self.db = db
        self.pipeline_manager = pipeline_manager
        self.current_document_path = ""
        self.current_styles = []
        self.current_header_config = None
        self.analysis_worker = None
        self.selected_document_row = -1  # Track selected document
        
        self.setup_ui()
        self.setup_connections()
    
    def setup_ui(self):
        """
        Set up the user interface
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
        self.config_button.setEnabled(False)  # Initially disabled
        controls_layout.addWidget(self.config_button)
        
        self.save_config_button = QPushButton("Save Configuration")
        self.save_config_button.clicked.connect(self.save_header_configuration)
        self.save_config_button.setEnabled(False)  # Initially disabled
        controls_layout.addWidget(self.save_config_button)
        
        controls_layout.addStretch()
        
        layout.addWidget(controls_group)
        
        # Main content area with splitter
        splitter = QSplitter(Qt.Orientation.Horizontal)
        
        # Left panel - Document selection and styles
        left_panel = self._create_document_selection_panel()
        splitter.addWidget(left_panel)
        
        # Right panel - Header configuration
        right_panel = self._create_header_configuration_panel()
        splitter.addWidget(right_panel)
        
        splitter.setSizes([600, 800])
        layout.addWidget(splitter)
        
        # Progress bar and status
        self.progress_bar = QProgressBar()
        self.progress_bar.setVisible(False)
        layout.addWidget(self.progress_bar)
        
        self.status_label = QLabel("Ready")
        layout.addWidget(self.status_label)
    
    def _create_document_selection_panel(self) -> QWidget:
        """
        Create left panel with document selection and style analysis
        """
        panel = QWidget()
        layout = QVBoxLayout(panel)
        
        # Uploaded documents table
        table_group = QGroupBox("Uploaded Documents")
        table_layout = QVBoxLayout(table_group)
        
        self.documents_table = QTableWidget()
        self.documents_table.setColumnCount(4)  # Changed to 4 columns
        self.documents_table.setHorizontalHeaderLabels(["File Name", "Format", "Size", "Status"])
        self.documents_table.horizontalHeader().setSectionResizeMode(0, QHeaderView.ResizeMode.Stretch)
        self.documents_table.horizontalHeader().setSectionResizeMode(1, QHeaderView.ResizeMode.ResizeToContents)
        self.documents_table.horizontalHeader().setSectionResizeMode(2, QHeaderView.ResizeMode.ResizeToContents)
        self.documents_table.horizontalHeader().setSectionResizeMode(3, QHeaderView.ResizeMode.ResizeToContents)
        self.documents_table.itemClicked.connect(self.on_document_selected)
        
        table_layout.addWidget(self.documents_table)
        
        # Style analysis results
        styles_group = QGroupBox("Document Styles")
        styles_layout = QVBoxLayout(styles_group)
        
        # Tab widget for different analysis views
        self.styles_tabs = QTabWidget()
        
        # Styles table tab
        styles_table_widget = QWidget()
        styles_table_layout = QVBoxLayout(styles_table_widget)
        
        self.styles_table = QTableWidget()
        self.styles_table.setColumnCount(6)  # Increased columns
        self.styles_table.setHorizontalHeaderLabels(["Font", "Size", "Bold", "Italic", "Color", "Sample"])
        self.styles_table.horizontalHeader().setSectionResizeMode(0, QHeaderView.ResizeMode.Stretch)
        self.styles_table.horizontalHeader().setSectionResizeMode(1, QHeaderView.ResizeMode.ResizeToContents)
        self.styles_table.horizontalHeader().setSectionResizeMode(2, QHeaderView.ResizeMode.ResizeToContents)
        self.styles_table.horizontalHeader().setSectionResizeMode(3, QHeaderView.ResizeMode.ResizeToContents)
        self.styles_table.horizontalHeader().setSectionResizeMode(4, QHeaderView.ResizeMode.ResizeToContents)
        self.styles_table.horizontalHeader().setSectionResizeMode(5, QHeaderView.ResizeMode.Stretch)
        
        styles_table_layout.addWidget(self.styles_table)
        self.styles_tabs.addTab(styles_table_widget, "Styles Table")
        
        # Styles tree view tab
        styles_tree_widget = QWidget()
        styles_tree_layout = QVBoxLayout(styles_tree_widget)
        
        self.styles_tree = QTreeWidget()
        self.styles_tree.setHeaderLabels(["Style", "Properties", "Usage Count", "Sample Text"])
        self.styles_tree.header().setSectionResizeMode(0, QHeaderView.ResizeMode.Stretch)
        self.styles_tree.header().setSectionResizeMode(1, QHeaderView.ResizeMode.Stretch)
        self.styles_tree.header().setSectionResizeMode(2, QHeaderView.ResizeMode.ResizeToContents)
        self.styles_tree.header().setSectionResizeMode(3, QHeaderView.ResizeMode.Stretch)
        
        styles_tree_layout.addWidget(self.styles_tree)
        self.styles_tabs.addTab(styles_tree_widget, "Styles Tree")
        
        styles_layout.addWidget(self.styles_tabs)
        
        layout.addWidget(table_group)
        layout.addWidget(styles_group)
        
        return panel
    
    def _create_header_configuration_panel(self) -> QWidget:
        """
        Create right panel for header configuration
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
        
        # Configuration area
        config_group = QGroupBox("Header Configuration")
        config_layout = QVBoxLayout(config_group)
        
        # For style-based detection
        self.style_config_widget = self._create_style_config_widget()
        config_layout.addWidget(self.style_config_widget)
        
        # For exact phrases detection
        self.phrase_config_widget = self._create_phrase_config_widget()
        self.phrase_config_widget.setVisible(False)  # Hidden initially
        config_layout.addWidget(self.phrase_config_widget)
        
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
        
        # Header assignments preview
        preview_group = QGroupBox("Header Assignments Preview")
        preview_layout = QVBoxLayout(preview_group)
        
        self.assignments_preview = QTextEdit()
        self.assignments_preview.setReadOnly(True)
        self.assignments_preview.setFont(QFont("Consolas", 10))
        
        preview_layout.addWidget(self.assignments_preview)
        
        layout.addWidget(preview_group)
        
        return panel
    
    def _create_style_config_widget(self) -> QWidget:
        """
        Create widget for style-based header configuration
        """
        widget = QWidget()
        layout = QVBoxLayout(widget)
        
        # Instructions
        instructions = QLabel(
            "Assign document styles to header levels:\n"
            "• Select a style from the table\n"
            "• Choose header level (1-5)\n"
            "• Add optional filtering rules"
        )
        instructions.setWordWrap(True)
        layout.addWidget(instructions)
        
        # Style assignment table - FIXED: Better column sizing
        self.style_assignment_table = QTableWidget()
        self.style_assignment_table.setColumnCount(3)
        self.style_assignment_table.setHorizontalHeaderLabels(["Style", "Header Level", "Filters"])
        self.style_assignment_table.horizontalHeader().setSectionResizeMode(0, QHeaderView.ResizeMode.Stretch)
        self.style_assignment_table.horizontalHeader().setSectionResizeMode(1, QHeaderView.ResizeMode.ResizeToContents)
        self.style_assignment_table.horizontalHeader().setSectionResizeMode(2, QHeaderView.ResizeMode.ResizeToContents)
        
        # Make the table taller
        self.style_assignment_table.setMinimumHeight(200)
        self.style_assignment_table.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Expanding)
        
        layout.addWidget(self.style_assignment_table)
        
        # Assignment controls
        assignment_layout = QHBoxLayout()
        
        self.assign_level_button = QPushButton("Assign Level")
        self.assign_level_button.clicked.connect(self.assign_style_to_level)
        assignment_layout.addWidget(self.assign_level_button)
        
        self.remove_assignment_button = QPushButton("Remove Assignment")
        self.remove_assignment_button.clicked.connect(self.remove_style_assignment)
        assignment_layout.addWidget(self.remove_assignment_button)
        
        assignment_layout.addStretch()
        
        layout.addLayout(assignment_layout)
        
        return widget
    
    def _create_phrase_config_widget(self) -> QWidget:
        """
        Create widget for exact phrase header configuration
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
        layout.addWidget(instructions)
        
        # Phrase assignment table - FIXED: Made larger
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
        
        layout.addWidget(self.phrase_assignment_table)
        
        # Assignment controls
        phrase_layout = QHBoxLayout()
        
        self.add_phrase_button = QPushButton("Add Phrase")
        self.add_phrase_button.clicked.connect(self.add_phrase_assignment)
        phrase_layout.addWidget(self.add_phrase_button)
        
        self.remove_phrase_button = QPushButton("Remove Phrase")
        self.remove_phrase_button.clicked.connect(self.remove_phrase_assignment)
        phrase_layout.addWidget(self.remove_phrase_button)
        
        phrase_layout.addStretch()
        
        layout.addLayout(phrase_layout)
        
        return widget
    
    def setup_connections(self):
        """
        Set up signal connections
        """
        # FIX: Use correct signal connections for radio buttons
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
    
    def select_documents(self):
        """
        Open file dialog to select documents
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
        Add document paths to table
        """
        current_row = self.documents_table.rowCount()
        self.documents_table.setRowCount(current_row + len(file_paths))
        
        for i, path in enumerate(file_paths):
            row = current_row + i
            
            # File name
            self.documents_table.setItem(row, 0, QTableWidgetItem(os.path.basename(path)))
            
            # Format
            ext = Path(path).suffix.lower()
            self.documents_table.setItem(row, 1, QTableWidgetItem(ext[1:]))
            
            # Size
            size_mb = os.path.getsize(path) / (1024 * 1024)
            self.documents_table.setItem(row, 2, QTableWidgetItem(f"{size_mb:.2f} MB"))
            
            # Status
            self.documents_table.setItem(row, 3, QTableWidgetItem("Ready"))
        
        # Enable analyze button if documents are selected
        self.analyze_button.setEnabled(True)
        self.selected_document_row = 0  # Select first document
    
    def on_document_selected(self, item: QTableWidgetItem):
        """
        Handle document selection
        """
        self.selected_document_row = item.row()
        file_name = self.documents_table.item(self.selected_document_row, 0).text()
        file_ext = self.documents_table.item(self.selected_document_row, 1).text()
        
        # Enable analyze button
        self.analyze_button.setEnabled(True)
        self.status_label.setText(f"Selected: {file_name}")
        
        # Clear previous analysis results
        self.current_styles = []
        self.current_header_config = None
        self.config_button.setEnabled(False)
        self.save_config_button.setEnabled(False)
        
        # Clear tables
        self.styles_table.setRowCount(0)
        self.phrase_assignment_table.setRowCount(0)
        self.style_assignment_table.setRowCount(0)
        self.assignments_preview.clear()
    
    def analyze_current_document(self):
        """
        Analyze currently selected document for styles
        """
        if self.selected_document_row < 0 or self.selected_document_row >= self.documents_table.rowCount():
            QMessageBox.warning(self, "Warning", "Please select a document to analyze")
            return
        
        file_name = self.documents_table.item(self.selected_document_row, 0).text()
        file_ext = self.documents_table.item(self.selected_document_row, 1).text()
        
        # Find the full path by searching through all documents
        # In real implementation, you'd store full paths separately
        # For now, let's assume the file is in current directory or prompt user
        full_path, ok = QInputDialog.getText(
            self, 
            "Document Path", 
            f"Enter full path for '{file_name}':",
            text=file_name  # Default suggestion
        )
        
        if not ok or not full_path or not os.path.exists(full_path):
            QMessageBox.warning(self, "Warning", "Invalid document path")
            return
        
        self.current_document_path = full_path
        
        # Start analysis in background thread
        self.analysis_worker = StyleAnalysisWorker(full_path)
        self.analysis_worker.progress_signal.connect(self._on_analysis_progress)
        self.analysis_worker.result_signal.connect(self._on_analysis_complete)
        self.analysis_worker.error_signal.connect(self._on_analysis_error)
        
        self.progress_bar.setVisible(True)
        self.progress_bar.setValue(0)
        self.status_label.setText("Analyzing document styles...")
        
        self.analysis_worker.start()
    
    def _on_analysis_progress(self, progress: int, message: str):
        """
        Handle analysis progress update
        """
        self.progress_bar.setValue(progress)
        self.status_label.setText(message)
    
    def _on_analysis_complete(self, styles: List[TextStyle]):
        """
        Handle analysis completion
        """
        self.progress_bar.setVisible(False)
        self.status_label.setText("Analysis complete")
        
        # Store analyzed styles
        self.current_styles = styles
        
        # Update styles table
        self._populate_styles_table(styles)
        
        # Update styles tree
        self._populate_styles_tree(styles)
        
        # Enable configuration button
        self.config_button.setEnabled(True)
        
        # Update status in table
        if self.selected_document_row >= 0:
            self.documents_table.setItem(
                self.selected_document_row, 
                3, 
                QTableWidgetItem(f"Analyzed ({len(styles)} styles)")
            )
        
        QMessageBox.information(
            self, 
            "Analysis Complete", 
            f"Found {len(styles)} unique text styles in the document!"
        )
        
        # Clean up worker
        self.analysis_worker = None
    
    def _on_analysis_error(self, error: str):
        """
        Handle analysis error
        """
        self.progress_bar.setVisible(False)
        self.status_label.setText("Analysis failed")
        
        QMessageBox.critical(self, "Analysis Error", f"Failed to analyze document: {error}")
        
        # Clean up worker
        self.analysis_worker = None
    
    def _populate_styles_table(self, styles: List[TextStyle]):
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
    
    def _populate_styles_tree(self, styles: List[TextStyle]):
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
        """
        if not self.current_styles:
            QMessageBox.warning(self, "Warning", "No styles analyzed. Analyze a document first.")
            return
        
        # Show configuration options based on selected method
        if self.style_based_radio.isChecked():
            self._configure_style_based_headers()
        else:
            self._configure_phrase_based_headers()
        
        # Enable save button
        self.save_config_button.setEnabled(True)
    
    def _configure_style_based_headers(self):
        """
        Configure header detection based on document styles
        """
        # Update style assignment table with current styles
        self.style_assignment_table.setRowCount(len(self.current_styles))
        
        for i, style in enumerate(self.current_styles):
            # Style description
            style_desc = f"{style.style_name or style.font_name or style.font_family} ({style.font_size}px)"
            self.style_assignment_table.setItem(i, 0, QTableWidgetItem(style_desc))
            
            # Level combo box - FIXED: Make it properly selectable
            level_combo = QComboBox()
            level_combo.addItems(["None", "Level 1", "Level 2", "Level 3", "Level 4", "Level 5"])
            level_combo.setCurrentIndex(0)  # Default to "None"
            self.style_assignment_table.setCellWidget(i, 1, level_combo)
            
            # Filters button
            filters_btn = QPushButton("Edit Filters")
            filters_btn.clicked.connect(lambda checked, s=style, row=i: self.edit_style_filters(s, row))
            self.style_assignment_table.setCellWidget(i, 2, filters_btn)
    
    def _configure_phrase_based_headers(self):
        """
        Configure header detection based on exact phrases
        """
        # Show phrase configuration interface
        # The table is already created, just make sure it's visible
        pass
    
    def edit_style_filters(self, style: TextStyle, row: int):
        """
        Edit filtering options for a specific style
        """
        from PyQt6.QtWidgets import QDialog, QVBoxLayout, QDialogButtonBox, QFormLayout
        
        dialog = QDialog(self)
        dialog.setWindowTitle(f"Edit Filters for Style: {style.style_name or style.font_name or style.font_family}")
        dialog.setGeometry(200, 200, 500, 400)
        
        layout = QVBoxLayout(dialog)
        
        # Create filter configuration form
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
            # In real implementation, you'd associate these with the specific row
            print(f"Filters applied to style {style.style_name}: include_words={include_words.text()}")
            QMessageBox.information(
                self, 
                "Filters Updated", 
                f"Filters applied to style: {style.style_name or style.font_name}"
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
        style_combo = self.style_assignment_table.cellWidget(row, 1)  # Level combo box
        
        if style_combo:
            level_text = style_combo.currentText()
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
    
    def add_phrase_assignment(self):
        """
        Add phrase-based header assignment
        """
        # Create dialog for adding phrase assignment
        from PyQt6.QtWidgets import QDialog, QVBoxLayout, QDialogButtonBox, QFormLayout
        
        dialog = QDialog(self)
        dialog.setWindowTitle("Add Phrase Assignment")
        dialog.setGeometry(200, 200, 400, 300)
        
        layout = QVBoxLayout(dialog)
        form_layout = QFormLayout()
        
        # Level selection
        level_combo = QComboBox()
        level_combo.addItems(["Level 1", "Level 2", "Level 3", "Level 4", "Level 5"])
        form_layout.addRow("Header Level:", level_combo)
        
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
            self.phrase_assignment_table.setItem(row, 0, QTableWidgetItem(level_combo.currentText()))
            
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
    
    def remove_style_assignment(self):
        """
        Remove selected style assignment
        """
        selected_items = self.style_assignment_table.selectedItems()
        if not selected_items:
            QMessageBox.warning(self, "Warning", "Please select a style to remove")
            return
        
        row = selected_items[0].row()
        style_combo = self.style_assignment_table.cellWidget(row, 1)
        if style_combo:
            style_combo.setCurrentIndex(0)  # Set to "None"
        
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
        """
        if not self.current_document_path:
            QMessageBox.warning(self, "Warning", "No document analyzed yet")
            return
        
        # Generate configuration based on current settings
        config = self._generate_header_config()
        
        if not config:
            QMessageBox.warning(self, "Warning", "No header configuration to save")
            return
        
        # Save configuration file
        config_path, _ = QFileDialog.getSaveFileName(
            self, "Save Header Configuration", "", "JSON Files (*.json);;All Files (*)"
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
                
            except Exception as e:
                QMessageBox.critical(self, "Error", f"Failed to save configuration: {str(e)}")
    
    def _generate_header_config(self) -> Optional[Dict[str, Any]]:
        """
        Generate header configuration from current settings
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
                    
                    # Get style properties from current_styles
                    assignment = {
                        "level": level_num,
                        "description": f"Level {level_num} header from style: {style_item.text()}",
                        "style": {
                            "font_size": 12,  # Would get from actual style analysis
                            "is_bold": True,  # Would get from actual style analysis
                            "is_italic": False,
                            "font_name": "Unknown"
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
        
        else:
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
                        "case_sensitive": case_item.text() == "Case Sensitive" if case_item else False,
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
    
    def refresh(self):
        """
        Refresh the document uploader
        """
        # Clear all selections and tables
        self.documents_table.clearSelection()
        self.styles_table.setRowCount(0)
        self.styles_tree.clear()
        self.phrase_assignment_table.setRowCount(0)
        self.style_assignment_table.setRowCount(0)
        self.assignments_preview.clear()
        
        # Disable buttons
        self.analyze_button.setEnabled(False)
        self.config_button.setEnabled(False)
        self.save_config_button.setEnabled(False)
        
        self.status_label.setText("Ready")