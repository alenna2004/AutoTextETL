#!/usr/bin/env python3
"""
Pipeline Designer - Visual pipeline construction with clear step configuration
"""

import sys
from pathlib import Path
from typing import Dict, Any, List, Optional, Union
import os
import json
import tempfile
from datetime import datetime
import secrets

# Add project root to Python path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from PyQt6.QtWidgets import (QWidget, QVBoxLayout, QHBoxLayout, QTreeWidget, 
                           QTreeWidgetItem, QPushButton, QLineEdit, QLabel, QSplitter,
                           QGroupBox, QFormLayout, QTableWidget, QTableWidgetItem,
                           QHeaderView, QTabWidget, QMessageBox, QFileDialog,
                           QProgressBar, QStatusBar, QMenuBar, QToolBar, 
                           QInputDialog, QListWidget, QScrollArea, QSpinBox, QCheckBox,
                           QComboBox, QTextEdit, QListWidgetItem)
from PyQt6.QtCore import Qt, pyqtSignal, QTimer, QRect
from PyQt6.QtGui import QFont, QAction, QKeySequence, QIcon, QColor, QPen, QPainter
import uuid
from domain.chunk import Chunk, Metadata, ChunkType
from domain.pipeline import PipelineConfig, PipelineStepConfig, PipelineRun, PipelineStatus, StepType
from domain.interfaces import IDbExporter
from infrastructure.loaders.document_factory import DocumentFactory

class PipelineStepItem:
    """
    Represents a pipeline step item in the visual designer
    """
    def __init__(self, step_id: str, step_type: str, name: str = "", params: Dict[str, Any] = None):
        self.id = step_id
        self.type = step_type
        self.name = name or f"{step_type.replace('_', ' ').title()} Step"
        self.params = params or {}
        self.input_step_id = None  # ID of previous step this connects to
        self.depends_on = []       # Conditional dependencies
        self.position = (0, 0)     # Position in visual designer
        self.connected_inputs = [] # List of input connections
        self.connected_outputs = [] # List of output connections
        self.output_steps = []     # Added missing attribute - list of output step IDs

class VisualConnection:
    """
    Represents a visual connection between steps
    """
    def __init__(self, from_step_id: str, to_step_id: str, connection_type: str = "data"):
        self.from_step_id = from_step_id
        self.to_step_id = to_step_id
        self.connection_type = connection_type  # "data", "control", "error"
        self.properties = {}  # Additional connection properties

class VisualCanvasWidget(QWidget):
    """
    Custom widget for visual pipeline canvas with step connections
    """
    step_clicked = pyqtSignal(str)  # Emits step_id when clicked
    connection_requested = pyqtSignal(str, str)  # Emits (from_step_id, to_step_id)
    
    def __init__(self):
        super().__init__()
        self.steps: Dict[str, PipelineStepItem] = {}
        self.connections: List[VisualConnection] = []
        self.connection_mode = False
        self.selected_step_id = None
        self.first_step_for_connection = None
        self.steps_positions: Dict[str, tuple] = {}
        self.grid_size = 50
        
        self.setMinimumSize(800, 600)
        self.setStyleSheet("background-color: #f5f5f5;")
    
    def add_step(self, step: PipelineStepItem, position: Optional[tuple] = None):
        """
        Add step to canvas with optional position
        """
        self.steps[step.id] = step
        if position:
            self.steps_positions[step.id] = position
        else:
            # Auto-position: arrange in grid
            count = len(self.steps) - 1
            x = 100 + (count % 4) * 250
            y = 100 + (count // 4) * 100
            self.steps_positions[step.id] = (x, y)
        self.update()
    
    def remove_step(self, step_id: str):
        """
        Remove step and all its connections
        """
        if step_id in self.steps:
            # Remove connections involving this step
            self.connections = [
                conn for conn in self.connections 
                if conn.from_step_id != step_id and conn.to_step_id != step_id
            ]
            
            # Remove from other steps' connections
            for step in self.steps.values():
                if step.input_step_id == step_id:
                    step.input_step_id = None
                step.output_steps = [sid for sid in step.output_steps if sid != step_id]
            
            # Remove step
            del self.steps[step_id]
            if step_id in self.steps_positions:
                del self.steps_positions[step_id]
            
            # Update connections
            self._update_step_connections()
            self.update()
    
    def clear_steps(self):
        """
        Clear all steps from canvas
        """
        self.steps.clear()
        self.connections.clear()
        self.steps_positions.clear()
        self.selected_step_id = None
        self.first_step_for_connection = None
        self.update()
    
    def set_connection_mode(self, enabled: bool):
        """
        Enable/disable connection mode
        """
        self.connection_mode = enabled
        if not enabled:
            self.first_step_for_connection = None
        self.update()
    
    def start_connection(self, step_id: str):
        """
        Start connection from selected step
        """
        if step_id in self.steps:
            self.first_step_for_connection = step_id
            self.update()
    
    def complete_connection(self, step_id: str):
        """
        Complete connection to selected step
        """
        if (self.first_step_for_connection and 
            self.first_step_for_connection != step_id and 
            step_id in self.steps):
            
            # Check if connection already exists
            existing_conn = next(
                (conn for conn in self.connections 
                 if conn.from_step_id == self.first_step_for_connection and conn.to_step_id == step_id), 
                None
            )
            
            if not existing_conn:
                # Create connection
                connection = VisualConnection(self.first_step_for_connection, step_id)
                self.connections.append(connection)
                
                # Update step connections
                from_step = self.steps[self.first_step_for_connection]
                to_step = self.steps[step_id]
                
                from_step.output_steps.append(step_id)
                to_step.input_step_id = self.first_step_for_connection
            
            self.first_step_for_connection = None
            self.connection_mode = False
            self.update()
    
    def remove_connection(self, from_step_id: str, to_step_id: str):
        """
        Remove specific connection
        """
        self.connections = [
            conn for conn in self.connections 
            if not (conn.from_step_id == from_step_id and conn.to_step_id == to_step_id)
        ]
        
        # Update step connections
        if from_step_id in self.steps:
            from_step = self.steps[from_step_id]
            from_step.output_steps = [sid for sid in from_step.output_steps if sid != to_step_id]
        
        if to_step_id in self.steps:
            to_step = self.steps[to_step_id]
            if to_step.input_step_id == from_step_id:
                to_step.input_step_id = None
        
        self.update()
    
    def update_connections(self, connections: List[VisualConnection]):
        """
        Update connections list
        """
        self.connections = connections
        self._update_step_connections()
        self.update()
    
    def _update_step_connections(self):
        """
        Update step connections based on visual connections
        """
        # Clear all existing connections in steps
        for step in self.steps.values():
            step.input_step_id = None
            # Use getattr with default to safely handle missing attribute
            if hasattr(step, 'output_steps'):
                step.output_steps.clear()
            else:
                step.output_steps = []  # Create if missing
    
        # Set up connections based on visual connections
        for conn in self.connections:
            if conn.from_step_id in self.steps and conn.to_step_id in self.steps:
                from_step = self.steps[conn.from_step_id]
                to_step = self.steps[conn.to_step_id]
            
                # ✅ FIXED: Ensure output_steps exists
                if not hasattr(from_step, 'output_steps'):
                    from_step.output_steps = []
                if not hasattr(to_step, 'output_steps'):
                    to_step.output_steps = []
            
                from_step.output_steps.append(conn.to_step_id)
                to_step.input_step_id = conn.from_step_id
    
    def update_step_name(self, step_id: str, new_name: str):
        """
        Update step name in canvas
        """
        if step_id in self.steps:
            self.steps[step_id].name = new_name
            self.update()
    
    def paintEvent(self, event):
        """
        Draw the visual canvas with steps and connections
        """
        painter = QPainter(self)
        painter.setRenderHint(QPainter.RenderHint.Antialiasing)
        
        # Draw grid
        painter.setPen(QPen(QColor("#e0e0e0"), 1))
        for x in range(0, self.width(), self.grid_size):
            painter.drawLine(x, 0, x, self.height())
        for y in range(0, self.height(), self.grid_size):
            painter.drawLine(0, y, self.width(), y)
        
        # Draw connections (under steps)
        painter.setPen(QPen(QColor("#4CAF50"), 3))
        
        for conn in self.connections:
            if (conn.from_step_id in self.steps_positions and 
                conn.to_step_id in self.steps_positions):
                
                from_pos = self.steps_positions[conn.from_step_id]
                to_pos = self.steps_positions[conn.to_step_id]
                
                # Draw arrow from right side of from_step to left side of to_step
                from_x = int(from_pos[0] + 180)  # Right side of step box
                from_y = int(from_pos[1] + 25)   # Middle of step box
                to_x = int(to_pos[0])            # Left side of step box
                to_y = int(to_pos[1] + 25)       # Middle of step box
                
                # Draw line with arrow
                painter.drawLine(from_x, from_y, to_x, to_y)
                
                # Draw arrowhead
                self._draw_arrowhead(painter, from_x, from_y, to_x, to_y)
        
        # Draw steps
        for step_id, step in self.steps.items():
            if step_id in self.steps_positions:
                x_pos, y_pos = self.steps_positions[step_id]
                
                # Determine color based on selection and connection mode
                if step_id == self.selected_step_id:
                    fill_color = QColor("#E8F5E8")  # Selected green
                    border_color = QColor("#4CAF50")
                elif step_id == self.first_step_for_connection:
                    fill_color = QColor("#FFF3CD")  # Connection start yellow
                    border_color = QColor("#FFC107")
                else:
                    # Different colors for different step types
                    type_colors = {
                        "document_loader": QColor("#E3F2FD"),
                        "line_splitter": QColor("#F3E5F5"),
                        "delimiter_splitter": QColor("#F3E5F5"),
                        "paragraph_splitter": QColor("#F3E5F5"),
                        "sentence_splitter": QColor("#F3E5F5"),
                        "regex_extractor": QColor("#E8F5E8"),
                        "user_script": QColor("#FFF3E0"),
                        "db_exporter": QColor("#FCE4EC"),
                        "file_exporter": QColor("#E0F2F1"),
                        "json_exporter": QColor("#E0F2F1")
                    }
                    fill_color = type_colors.get(step.type, QColor("#F5F5F5"))
                    border_color = QColor("#CCCCCC")
                
                # Draw step box
                rect = QRect(int(x_pos), int(y_pos), 180, 50)
                painter.fillRect(rect, fill_color)
                painter.setPen(QPen(border_color, 2))
                painter.drawRect(rect)
                
                # Draw step name
                painter.setPen(QColor("#333333"))
                painter.setFont(QFont("Arial", 10, QFont.Weight.Bold))
                painter.drawText(rect.adjusted(5, 5, -5, -5), 
                               Qt.AlignmentFlag.AlignLeft | Qt.AlignmentFlag.AlignTop, 
                               step.name)
                
                # Draw step type
                painter.setFont(QFont("Arial", 8, QFont.Weight.Normal))
                painter.setPen(QColor("#666666"))
                painter.drawText(rect.adjusted(5, 25, -5, -5), 
                               Qt.AlignmentFlag.AlignLeft | Qt.AlignmentFlag.AlignTop, 
                               step.type)
    
    def _draw_arrowhead(self, painter: QPainter, x1: int, y1: int, x2: int, y2: int):
        """
        Draw arrowhead at end of line
        """
        import math
        
        # Calculate angle of line
        angle = math.atan2(y2 - y1, x2 - x1)
        
        # Arrowhead size
        head_size = 10
        
        # Calculate arrowhead points
        head_x1 = x2 - head_size * math.cos(angle - math.pi/6)
        head_y1 = y2 - head_size * math.sin(angle - math.pi/6)
        head_x2 = x2 - head_size * math.cos(angle + math.pi/6)
        head_y2 = y2 - head_size * math.sin(angle + math.pi/6)
        
        # Convert to integers for PyQt6
        head_x1 = int(head_x1)
        head_y1 = int(head_y1)
        head_x2 = int(head_x2)
        head_y2 = int(head_y2)
        x2 = int(x2)
        y2 = int(y2)
        
        # Draw arrowhead
        painter.drawLine(x2, y2, head_x1, head_y1)
        painter.drawLine(x2, y2, head_x2, head_y2)
    
    def mousePressEvent(self, event):
        """
        Handle mouse clicks on steps
        """
        pos = event.position().toPoint()
        
        # Check if clicked on any step
        clicked_step_id = None
        for step_id, step in self.steps.items():
            if step_id in self.steps_positions:
                x_pos, y_pos = self.steps_positions[step_id]
                
                rect = QRect(int(x_pos), int(y_pos), 180, 50)
                if rect.contains(pos):
                    clicked_step_id = step_id
                    break
        
        if clicked_step_id:
            if self.connection_mode and self.first_step_for_connection is None:
                self.start_connection(clicked_step_id)
            elif self.connection_mode and self.first_step_for_connection is not None:
                if self.first_step_for_connection != clicked_step_id:
                    self.connection_requested.emit(self.first_step_for_connection, clicked_step_id)
                self.first_step_for_connection = None
                self.connection_mode = False
                self.update()
            else:
                self.selected_step_id = clicked_step_id
                self.step_clicked.emit(clicked_step_id)
                self.update()
        else:
            if not self.connection_mode:
                self.selected_step_id = None
                self.step_clicked.emit("")  # Emit empty to clear selection
                self.update()

class PipelineDesigner(QWidget):
    """
    Visual pipeline designer with clear step configuration
    """
    
    def __init__(self, db, pipeline_manager):
        super().__init__()
        self.db = db
        self.pipeline_manager = pipeline_manager
        self.current_pipeline_id = None
        self.current_pipeline_config = None
        
        # Track steps and connections
        self.steps: Dict[str, PipelineStepItem] = {}
        self.connections: List[VisualConnection] = []
        self.selected_step_id = None
        
        self.setup_ui()
        self.setup_connections()
    
    def setup_ui(self):
        """
        Set up the user interface with clear step configuration
        """
        layout = QVBoxLayout(self)
        
        # Top controls
        controls_layout = QHBoxLayout()
        
        self.pipeline_name_edit = QLineEdit()
        self.pipeline_name_edit.setPlaceholderText("Pipeline Name")
        self.pipeline_name_edit.setMaximumWidth(300)
        controls_layout.addWidget(QLabel("Name:"))
        controls_layout.addWidget(self.pipeline_name_edit)
        
        self.pipeline_description_edit = QLineEdit()
        self.pipeline_description_edit.setPlaceholderText("Pipeline Description")
        controls_layout.addWidget(QLabel("Description:"))
        controls_layout.addWidget(self.pipeline_description_edit)
        
        # Buttons
        self.save_button = QPushButton("Save Pipeline")
        self.save_button.clicked.connect(self.save_pipeline)
        controls_layout.addWidget(self.save_button)
        
        self.load_button = QPushButton("Load Pipeline")
        self.load_button.clicked.connect(self.load_pipeline)
        controls_layout.addWidget(self.load_button)
        
        self.run_button = QPushButton("Run Pipeline")
        self.run_button.clicked.connect(self.run_pipeline)
        self.run_button.setStyleSheet("QPushButton { background-color: #4CAF50; color: white; }")
        controls_layout.addWidget(self.run_button)
        
        self.results_button = QPushButton("View Results")
        self.results_button.clicked.connect(self.view_pipeline_results)
        self.results_button.setStyleSheet("QPushButton { background-color: #2196F3; color: white; }")
        controls_layout.addWidget(self.results_button)
        
        self.clear_button = QPushButton("Clear")
        self.clear_button.clicked.connect(self.clear_pipeline)
        controls_layout.addWidget(self.clear_button)
        
        controls_layout.addStretch()
        
        layout.addLayout(controls_layout)
        
        # Main content area with splitter
        main_splitter = QSplitter(Qt.Orientation.Horizontal)
        
        # Left panel - Available steps
        left_panel = self._create_available_steps_panel()
        main_splitter.addWidget(left_panel)
        
        # Center panel - Visual canvas
        center_panel = self._create_visual_canvas()
        main_splitter.addWidget(center_panel)
        
        # Right panel - Step configuration
        right_panel = self._create_step_config_panel()
        main_splitter.addWidget(right_panel)
        
        main_splitter.setSizes([250, 600, 350])
        layout.addWidget(main_splitter)
        
        # Results view tab (hidden initially)
        self.results_tab = self._create_results_view_panel()
        self.results_tab.setVisible(False)
        layout.addWidget(self.results_tab)
        
        # Status bar
        self.status_bar = QStatusBar()
        layout.addWidget(self.status_bar)
    
    def _create_available_steps_panel(self) -> QWidget:
        """
        Create left panel with available pipeline steps
        """
        panel = QWidget()
        layout = QVBoxLayout(panel)
    
        # Available steps group
        steps_group = QGroupBox("Available Steps")
        steps_layout = QVBoxLayout(steps_group)
    
        # Step categories with detailed descriptions
        self.steps_tree = QTreeWidget()
        self.steps_tree.setHeaderLabels(["Step Type", "Description"])
        self.steps_tree.setDragEnabled(True)
        self.steps_tree.setDropIndicatorShown(True)
        self.steps_tree.setDragDropMode(QTreeWidget.DragDropMode.DragOnly)
    
        # Add step categories with clear descriptions
        loader_category = QTreeWidgetItem(self.steps_tree, ["Document Loaders", "Load and analyze documents"])
        loader_category.addChild(QTreeWidgetItem(["document_loader", "Load PDF/TXT/DOCX files with style analysis"]))
    
        processor_category = QTreeWidgetItem(self.steps_tree, ["Processors", "Text processing operations"])
        processor_category.addChild(QTreeWidgetItem(["line_splitter", "Split text by lines"]))
        processor_category.addChild(QTreeWidgetItem(["delimiter_splitter", "Split by custom delimiter"]))
        processor_category.addChild(QTreeWidgetItem(["paragraph_splitter", "Split by paragraphs"]))
        processor_category.addChild(QTreeWidgetItem(["sentence_splitter", "Split by sentences"]))
        processor_category.addChild(QTreeWidgetItem(["regex_extractor", "Extract with regex patterns"]))
        processor_category.addChild(QTreeWidgetItem(["user_script", "Execute custom Python script"]))
        processor_category.addChild(QTreeWidgetItem(["metadata_propagator", "Propagate document metadata"]))
    
        exporter_category = QTreeWidgetItem(self.steps_tree, ["Exporters", "Data output operations"])
        exporter_category.addChild(QTreeWidgetItem(["db_exporter", "Export to database"]))
        exporter_category.addChild(QTreeWidgetItem(["file_exporter", "Export to file"]))
        exporter_category.addChild(QTreeWidgetItem(["json_exporter", "Export to JSON"]))
    
        self.steps_tree.expandAll()
        steps_layout.addWidget(self.steps_tree)
    
        # Action buttons
        action_layout = QHBoxLayout()
    
        self.add_step_button = QPushButton("Add Selected Step")
        self.add_step_button.clicked.connect(self.add_selected_step)
        action_layout.addWidget(self.add_step_button)
    
        layout.addWidget(steps_group)
        layout.addLayout(action_layout)
    
        return panel
    
    def _create_visual_canvas(self) -> QWidget:
        """
        Create visual canvas for step connections
        """
        canvas_widget = QWidget()
        canvas_layout = QVBoxLayout(canvas_widget)
        
        # Canvas group
        canvas_group = QGroupBox("Pipeline Canvas")
        canvas_group_layout = QVBoxLayout(canvas_group)
        
        # Canvas widget with drawing capabilities
        self.canvas = VisualCanvasWidget()
        self.canvas.step_clicked.connect(self.on_step_clicked)
        self.canvas.connection_requested.connect(self.on_connection_requested)
        
        # Scroll area for canvas
        scroll_area = QScrollArea()
        scroll_area.setWidget(self.canvas)
        scroll_area.setWidgetResizable(True)
        
        canvas_group_layout.addWidget(scroll_area)
        
        # Connection controls
        conn_controls = QHBoxLayout()
        
        self.connect_steps_button = QPushButton("Connect Steps")
        self.connect_steps_button.clicked.connect(self.start_connection_mode)
        self.connect_steps_button.setStyleSheet("QPushButton { background-color: #2196F3; color: white; }")
        conn_controls.addWidget(self.connect_steps_button)
        
        self.disconnect_steps_button = QPushButton("Disconnect Steps")
        self.disconnect_steps_button.clicked.connect(self.disconnect_selected_steps)
        self.disconnect_steps_button.setStyleSheet("QPushButton { background-color: #f44336; color: white; }")
        conn_controls.addWidget(self.disconnect_steps_button)
        
        self.remove_step_button = QPushButton("Remove Selected")
        self.remove_step_button.clicked.connect(self.remove_selected_step)
        self.remove_step_button.setStyleSheet("QPushButton { background-color: #f44336; color: white; }")
        conn_controls.addWidget(self.remove_step_button)
        
        self.connection_mode_label = QLabel("Connection Mode: OFF")
        self.connection_mode_label.setStyleSheet("color: red; font-weight: bold;")
        conn_controls.addWidget(self.connection_mode_label)
        
        conn_controls.addStretch()
        
        canvas_group_layout.addLayout(conn_controls)
        
        canvas_layout.addWidget(canvas_group)
        
        return canvas_widget
    
    def _create_step_config_panel(self) -> QWidget:
        """
        Create right panel for step configuration with clear parameters
        """
        panel = QWidget()
        layout = QVBoxLayout(panel)
        
        # Selected step configuration
        config_group = QGroupBox("Step Configuration")
        config_layout = QFormLayout(config_group)
        
        self.step_id_label = QLabel("No step selected")
        config_layout.addRow("Step ID:", self.step_id_label)
        
        self.step_type_label = QLabel("No step selected")
        config_layout.addRow("Step Type:", self.step_type_label)
        
        self.step_name_edit = QLineEdit()
        self.step_name_edit.setPlaceholderText("Step name")
        self.step_name_edit.textChanged.connect(self.on_step_name_changed)
        config_layout.addRow("Step Name:", self.step_name_edit)
        
        # Dynamic parameter configuration based on step type
        self.param_config_widget = self._create_parameter_config_widget()
        config_layout.addRow("Parameters:", self.param_config_widget)
        
        # Input/output configuration
        io_group = QGroupBox("Step Connections")
        io_layout = QVBoxLayout(io_group)
        
        # Input connection
        input_layout = QHBoxLayout()
        input_layout.addWidget(QLabel("Input From:"))
        self.input_combo = QComboBox()
        self.input_combo.currentTextChanged.connect(self.on_input_connection_changed)
        input_layout.addWidget(self.input_combo)
        
        # Output connections
        output_layout = QHBoxLayout()
        output_layout.addWidget(QLabel("Outputs To:"))
        self.output_list = QListWidget()
        io_layout.addLayout(output_layout)
        io_layout.addWidget(self.output_list)
        
        # Connection buttons
        conn_buttons = QHBoxLayout()
        
        self.add_output_button = QPushButton("Add Output")
        self.add_output_button.clicked.connect(self.add_output_connection)  # ← ADDED METHOD
        conn_buttons.addWidget(self.add_output_button)
        
        self.remove_output_button = QPushButton("Remove Output")
        self.remove_output_button.clicked.connect(self.remove_output_connection)  # ← ADDED METHOD
        conn_buttons.addWidget(self.remove_output_button)
        
        io_layout.addLayout(conn_buttons)
        
        layout.addWidget(config_group)
        layout.addWidget(io_group)
        
        return panel
    
    def _create_parameter_config_widget(self) -> QWidget:
        """
        Create dynamic parameter configuration widget based on step type
        """
        widget = QWidget()
        layout = QVBoxLayout(widget)
        
        # Document Loader specific parameters
        self.doc_loader_group = QGroupBox("Document Loader Parameters")
        doc_layout = QFormLayout(self.doc_loader_group)
        
        # Document selection button
        self.doc_selection_button = QPushButton("Select Documents...")
        self.doc_selection_button.clicked.connect(self.select_documents_for_loader)
        doc_layout.addRow("Document Selection:", self.doc_selection_button)
        
        self.selected_docs_text = QTextEdit()
        self.selected_docs_text.setMaximumHeight(80)
        self.selected_docs_text.setPlaceholderText("Selected documents will appear here...")
        self.selected_docs_text.setReadOnly(True)
        doc_layout.addRow("Selected Docs:", self.selected_docs_text)
        
        self.style_config_path_edit = QLineEdit()
        self.style_config_path_edit.setPlaceholderText("Path to header style configuration")
        doc_layout.addRow("Style Config:", self.style_config_path_edit)
        
        self.batch_size_spin = QSpinBox()
        self.batch_size_spin.setMinimum(1)
        self.batch_size_spin.setMaximum(10000)
        self.batch_size_spin.setValue(100)
        doc_layout.addRow("Batch Size:", self.batch_size_spin)
        
        self.parallel_workers_spin = QSpinBox()
        self.parallel_workers_spin.setMinimum(1)
        self.parallel_workers_spin.setMaximum(8)
        self.parallel_workers_spin.setValue(4)
        doc_layout.addRow("Parallel Workers:", self.parallel_workers_spin)
        
        # User Script specific parameters
        self.script_group = QGroupBox("Script Parameters")
        script_layout = QFormLayout(self.script_group)
        
        self.script_id_edit = QLineEdit()
        self.script_id_edit.setPlaceholderText("Select script ID from saved scripts")
        script_layout.addRow("Script ID:", self.script_id_edit)
        
        self.timeout_spin = QSpinBox()
        self.timeout_spin.setMinimum(1)
        self.timeout_spin.setMaximum(3600)
        self.timeout_spin.setValue(60)
        script_layout.addRow("Timeout (sec):", self.timeout_spin)
        
        self.memory_limit_spin = QSpinBox()
        self.memory_limit_spin.setMinimum(1)
        self.memory_limit_spin.setMaximum(2000)
        self.memory_limit_spin.setValue(200)
        script_layout.addRow("Memory Limit (MB):", self.memory_limit_spin)
        
        # Delimiter Splitter parameters
        self.delimiter_group = QGroupBox("Delimiter Parameters")
        delimiter_layout = QFormLayout(self.delimiter_group)
        
        self.delimiter_edit = QLineEdit()
        self.delimiter_edit.setPlaceholderText(";")
        delimiter_layout.addRow("Delimiter:", self.delimiter_edit)
        
        self.use_regex_checkbox = QCheckBox("Use Regular Expression")
        delimiter_layout.addRow("", self.use_regex_checkbox)
        
        self.preserve_delimiter_checkbox = QCheckBox("Preserve Delimiter")
        delimiter_layout.addRow("", self.preserve_delimiter_checkbox)
        
        # Regex Extractor parameters
        self.regex_group = QGroupBox("Regex Parameters")
        regex_layout = QFormLayout(self.regex_group)
        
        self.pattern_edit = QLineEdit()
        self.pattern_edit.setPlaceholderText(r"\$\d+,\d+\.\d{2}")  # Example: $1,234.56
        regex_layout.addRow("Pattern:", self.pattern_edit)
        
        self.named_groups_checkbox = QCheckBox("Named Groups Only")
        regex_layout.addRow("", self.named_groups_checkbox)
        
        self.case_insensitive_checkbox = QCheckBox("Case Insensitive")
        regex_layout.addRow("", self.case_insensitive_checkbox)
        
        # Database Exporter parameters
        self.db_exporter_group = QGroupBox("Database Export Parameters")
        db_layout = QFormLayout(self.db_exporter_group)
        
        # Database type selection
        self.db_type_combo = QComboBox()
        self.db_type_combo.addItems(["SQLite", "PostgreSQL", "MySQL", "MongoDB"])
        self.db_type_combo.currentTextChanged.connect(self.on_db_type_changed)
        db_layout.addRow("Database Type:", self.db_type_combo)
        
        # Connection configuration
        self.db_host_edit = QLineEdit()
        self.db_host_edit.setPlaceholderText("localhost")
        db_layout.addRow("Host:", self.db_host_edit)
        
        self.db_port_spin = QSpinBox()
        self.db_port_spin.setMinimum(1)
        self.db_port_spin.setMaximum(65535)
        self.db_port_spin.setValue(5432)
        db_layout.addRow("Port:", self.db_port_spin)
        
        self.db_name_edit = QLineEdit()
        self.db_name_edit.setPlaceholderText("chunks_db")
        db_layout.addRow("Database:", self.db_name_edit)
        
        self.db_username_edit = QLineEdit()
        self.db_username_edit.setPlaceholderText("username")
        db_layout.addRow("Username:", self.db_username_edit)
        
        self.db_password_edit = QLineEdit()
        self.db_password_edit.setEchoMode(QLineEdit.EchoMode.Password)
        self.db_password_edit.setPlaceholderText("password")
        db_layout.addRow("Password:", self.db_password_edit)
        
        self.table_name_edit = QLineEdit()
        self.table_name_edit.setPlaceholderText("chunks")
        db_layout.addRow("Table Name:", self.table_name_edit)
        
        self.batch_insert_size_spin = QSpinBox()
        self.batch_insert_size_spin.setMinimum(1)
        self.batch_insert_size_spin.setMaximum(10000)
        self.batch_insert_size_spin.setValue(1000)
        db_layout.addRow("Batch Insert Size:", self.batch_insert_size_spin)
        
        # File Exporter parameters
        self.file_exporter_group = QGroupBox("File Export Parameters")
        file_layout = QFormLayout(self.file_exporter_group)
        
        self.output_format_combo = QComboBox()
        self.output_format_combo.addItems(["JSON", "CSV", "TXT", "XML"])
        file_layout.addRow("Output Format:", self.output_format_combo)
        
        self.output_path_edit = QLineEdit()
        self.output_path_edit.setPlaceholderText("./output")
        file_layout.addRow("Output Path:", self.output_path_edit)
        
        self.output_path_button = QPushButton("Browse...")
        self.output_path_button.clicked.connect(self.browse_output_path)
        file_layout.addRow("", self.output_path_button)
        
        self.compression_checkbox = QCheckBox("Compress Output")
        file_layout.addRow("", self.compression_checkbox)
        
        self.pretty_print_checkbox = QCheckBox("Pretty Print (JSON)")
        file_layout.addRow("", self.pretty_print_checkbox)
        
        # Add all parameter groups
        layout.addWidget(self.doc_loader_group)
        layout.addWidget(self.script_group)
        layout.addWidget(self.delimiter_group)
        layout.addWidget(self.regex_group)
        layout.addWidget(self.db_exporter_group)
        layout.addWidget(self.file_exporter_group)
        
        # Hide all groups initially
        self._hide_all_param_groups()
        
        return widget
    
    def _hide_all_param_groups(self):
        """
        Hide all parameter configuration groups
        """
        for group in [self.doc_loader_group, self.script_group, self.delimiter_group, 
                     self.regex_group, self.db_exporter_group, self.file_exporter_group]:
            group.setVisible(False)
    
    def _show_param_group_for_type(self, step_type: str):
        """
        Show appropriate parameter group based on step type
        """
        self._hide_all_param_groups()
        
        if step_type == "document_loader":
            self.doc_loader_group.setVisible(True)
        elif step_type == "user_script":
            self.script_group.setVisible(True)
        elif step_type in ["delimiter_splitter", "line_splitter"]:
            self.delimiter_group.setVisible(True)
        elif step_type == "regex_extractor":
            self.regex_group.setVisible(True)
        elif step_type in ["db_exporter", "postgres_exporter", "mysql_exporter", "sqlite_exporter", "mongodb_exporter"]:
            self.db_exporter_group.setVisible(True)
            # Set appropriate database type
            if step_type == "sqlite_exporter":
                self.db_type_combo.setCurrentText("SQLite")
            elif step_type == "postgres_exporter":
                self.db_type_combo.setCurrentText("PostgreSQL")
            elif step_type == "mysql_exporter":
                self.db_type_combo.setCurrentText("MySQL")
            elif step_type == "mongodb_exporter":
                self.db_type_combo.setCurrentText("MongoDB")
        elif step_type in ["file_exporter", "json_exporter"]:
            self.file_exporter_group.setVisible(True)
            # Set appropriate format
            if step_type == "json_exporter":
                self.output_format_combo.setCurrentText("JSON")
    
    def on_db_type_changed(self, db_type: str):
        """
        Handle database type change - update default port and enable/disable fields
        """
        if db_type == "SQLite":
            # SQLite doesn't need host/port/credentials
            self.db_host_edit.setEnabled(False)
            self.db_port_spin.setEnabled(False)
            self.db_username_edit.setEnabled(False)
            self.db_password_edit.setEnabled(False)
            
            # Clear fields
            self.db_host_edit.clear()
            self.db_port_spin.setValue(0)
            self.db_username_edit.clear()
            self.db_password_edit.clear()
        else:
            # Other databases need connection details
            self.db_host_edit.setEnabled(True)
            self.db_port_spin.setEnabled(True)
            self.db_username_edit.setEnabled(True)
            self.db_password_edit.setEnabled(True)
            
            # Set default ports
            if db_type == "PostgreSQL":
                self.db_port_spin.setValue(5432)
            elif db_type == "MySQL":
                self.db_port_spin.setValue(3306)
            elif db_type == "MongoDB":
                self.db_port_spin.setValue(27017)
    
    def browse_output_path(self):
        """
        Browse for output directory
        """
        directory = QFileDialog.getExistingDirectory(
            self, 
            "Select Output Directory", 
            "", 
            QFileDialog.Option.ShowDirsOnly
        )
        if directory:
            self.output_path_edit.setText(directory)
    
    def select_documents_for_loader(self):
        """
        Open file dialog to select documents for document loader
        """
        if not self.selected_step_id:
            QMessageBox.warning(self, "Warning", "Please select a document loader step first")
            return
        
        # Check if selected step is a document loader
        step = self.steps.get(self.selected_step_id)
        if not step or step.type != "document_loader":
            QMessageBox.warning(self, "Warning", "Please select a document loader step")
            return
        
        # Open file dialog
        file_paths, _ = QFileDialog.getOpenFileNames(
            self, 
            "Select Documents for Processing", 
            "", 
            "Documents (*.pdf *.docx *.txt *.rtf *.odt);;PDF Files (*.pdf);;DOCX Files (*.docx);;TXT Files (*.txt);;All Files (*)"
        )
        
        if file_paths:
            # Update step parameters
            step.params["document_paths"] = file_paths
            
            # Update UI display
            self.selected_docs_text.setPlainText("\n".join(file_paths))
            
            # Update step in canvas
            self.canvas.update_step_name(self.selected_step_id, step.name)
            
            self.status_bar.showMessage(f"Selected {len(file_paths)} documents for processing")
    
    def setup_connections(self):
        """
        Set up signal connections
        """
        pass
    
    def add_selected_step(self):
        """
        Add selected step from available steps list
        """
        selected_items = self.steps_tree.selectedItems()
        if not selected_items:
            QMessageBox.warning(self, "Warning", "Please select a step to add")
            return
        
        step_type = selected_items[0].text(0)  # Get step type from tree
        step_description = selected_items[0].text(1)  # Get description
        
        # Check if this is a category (not an actual step)
        if step_type in ["Document Loaders", "Processors", "Exporters"]:
            QMessageBox.information(self, "Info", "Select a specific step, not a category")
            return
        
        # Create new step with default parameters
        step_id = f"step_{str(uuid.uuid4())[:8]}"
        step = PipelineStepItem(
            step_id=step_id,
            step_type=step_type,
            name=f"{step_type.replace('_', ' ').title()} Step",
            params=self._get_default_params_for_step_type(step_type)
        )
        
        # Add to steps dictionary
        self.steps[step_id] = step
        
        # Add to visual canvas
        self.canvas.add_step(step)
        
        # Update connection dropdowns
        self._update_connection_dropdowns()
        
        self.status_bar.showMessage(f"Added step: {step.name} ({step_type})")
    
    def _get_default_params_for_step_type(self, step_type: str) -> Dict[str, Any]:
        """
        Get default parameters for different step types
        """
        defaults = {
            "document_loader": {
                "document_paths": [],  # ← CRITICAL: Required for validation
                "style_config_path": "",
                "batch_size": 100,
                "parallel_workers": 4
            },
            "line_splitter": {
                "preserve_empty": True,
                "include_line_numbers": True
            },
            "delimiter_splitter": {
                "delimiter": ";",
                "use_regex": False,
                "preserve_delimiter": False
            },
            "paragraph_splitter": {
                "min_lines": 1,
                "preserve_formatting": True
            },
            "sentence_splitter": {
                "language": "en",
                "preserve_punctuation": True
            },
            "regex_extractor": {
                "pattern": "",
                "named_groups_only": True,
                "case_insensitive": True
            },
            "user_script": {
                "script_id": "",  # ← CRITICAL: Required for validation
                "timeout_seconds": 60,
                "memory_limit_mb": 200
            },
            "db_exporter": {
                "db_type": "sqlite",
                "host": "localhost",
                "port": 5432,
                "database": "chunks_db",
                "username": "",
                "password": "",
                "table_name": "chunks",  # ← CRITICAL: Required for validation
                "batch_size": 1000
            },
            "postgres_exporter": {
                "db_type": "postgresql",
                "host": "localhost",
                "port": 5432,
                "database": "chunks_db",
                "username": "postgres",
                "password": "",
                "table_name": "chunks",
                "batch_size": 1000
            },
            "mysql_exporter": {
                "db_type": "mysql",
                "host": "localhost",
                "port": 3306,
                "database": "chunks_db",
                "username": "root",
                "password": "",
                "table_name": "chunks",
                "batch_size": 1000
            },
            "sqlite_exporter": {
                "db_type": "sqlite",
                "path": "./chunks.db",
                "table_name": "chunks",
                "batch_size": 1000
            },
            "mongodb_exporter": {
                "db_type": "mongodb",
                "host": "localhost",
                "port": 27017,
                "database": "chunks_db",
                "username": "",
                "password": "",
                "collection_name": "chunks",
                "batch_size": 1000
            },
            "json_exporter": {
                "output_format": "json",
                "output_path": "./output",
                "file_name": "chunks.json",
                "compress": False,
                "pretty_print": True
            },
            "file_exporter": {
                "output_format": "json",
                "output_path": "./output",
                "file_name": "chunks.json",
                "compress": False,
                "pretty_print": True
            },
            "metadata_propagator": {
                "preserve_original_context": True,
                "inherit_parent_metadata": True
            }
        }
        return defaults.get(step_type, {})
    
    def on_step_clicked(self, step_id: str):
        """
        Handle step click in visual canvas
        """
        if not step_id:  # Clicked outside
            self.selected_step_id = None
            self._clear_step_config_ui()
            return
        
        # Check if step exists before accessing
        if step_id not in self.steps:
            QMessageBox.warning(self, "Warning", f"Step {step_id} not found in internal storage")
            return
        
        self.selected_step_id = step_id
        step = self.steps[step_id]
        
        # Update step configuration panel
        self.step_id_label.setText(step.id)
        self.step_type_label.setText(step.type)
        self.step_name_edit.setText(step.name)
        
        # Show appropriate parameter group
        self._show_param_group_for_type(step.type)
        
        # Load parameters into specific widgets
        self._load_step_params_into_widgets(step.params, step.type)
        
        # Update input/output connections
        self.input_combo.clear()
        self.input_combo.addItem("None", "")
        
        # Add all other steps as potential inputs
        for other_id, other_step in self.steps.items():
            if other_id != step_id:
                self.input_combo.addItem(other_step.name, other_id)
        
        # Set current input if exists
        if step.input_step_id:
            for i in range(self.input_combo.count()):
                if self.input_combo.itemData(i) == step.input_step_id:
                    self.input_combo.setCurrentIndex(i)
                    break
        
        # Update output list
        self.output_list.clear()
        for output_id in step.output_steps:
            if output_id in self.steps:
                self.output_list.addItem(self.steps[output_id].name)
    
    def _load_step_params_into_widgets(self, params: Dict[str, Any], step_type: str):
        """
        Load step parameters into appropriate configuration widgets
        """
        if step_type == "document_loader":
            # Load document paths
            doc_paths = params.get("document_paths", [])
            self.selected_docs_text.setPlainText("\n".join(doc_paths))
            
            # Load style config path
            self.style_config_path_edit.setText(params.get("style_config_path", ""))
            
            # Load batch size
            self.batch_size_spin.setValue(params.get("batch_size", 100))
            
            # Load parallel workers
            self.parallel_workers_spin.setValue(params.get("parallel_workers", 4))
        
        elif step_type == "user_script":
            self.script_id_edit.setText(params.get("script_id", ""))
            self.timeout_spin.setValue(params.get("timeout_seconds", 60))
            self.memory_limit_spin.setValue(params.get("memory_limit_mb", 200))
        
        elif step_type in ["delimiter_splitter", "line_splitter"]:
            self.delimiter_edit.setText(params.get("delimiter", ";"))
            self.use_regex_checkbox.setChecked(params.get("use_regex", False))
            self.preserve_delimiter_checkbox.setChecked(params.get("preserve_delimiter", False))
        
        elif step_type == "regex_extractor":
            self.pattern_edit.setText(params.get("pattern", ""))
            self.named_groups_checkbox.setChecked(params.get("named_groups_only", True))
            self.case_insensitive_checkbox.setChecked(params.get("case_insensitive", True))
        
        elif step_type in ["db_exporter", "postgres_exporter", "mysql_exporter", "sqlite_exporter", "mongodb_exporter"]:
            # Load database parameters
            self.db_type_combo.setCurrentText(params.get("db_type", "SQLite").title())
            
            if params.get("db_type") != "sqlite":
                self.db_host_edit.setText(params.get("host", "localhost"))
                self.db_port_spin.setValue(params.get("port", 5432))
                self.db_name_edit.setText(params.get("database", "chunks_db"))
                self.db_username_edit.setText(params.get("username", ""))
                self.db_password_edit.setText(params.get("password", ""))
            
            # Load table/collection name
            table_name = params.get("table_name", params.get("collection_name", "chunks"))
            self.table_name_edit.setText(table_name)
            
            # Load batch size
            self.batch_insert_size_spin.setValue(params.get("batch_size", 1000))
        
        elif step_type in ["file_exporter", "json_exporter"]:
            # Load file export parameters
            self.output_format_combo.setCurrentText(params.get("output_format", "JSON").title())
            self.output_path_edit.setText(params.get("output_path", "./output"))
            self.compression_checkbox.setChecked(params.get("compress", False))
            self.pretty_print_checkbox.setChecked(params.get("pretty_print", True))
    
    def on_step_name_changed(self, text: str):
        """
        Handle step name change
        """
        if self.selected_step_id and self.selected_step_id in self.steps:
            self.steps[self.selected_step_id].name = text
            self.canvas.update_step_name(self.selected_step_id, text)
            self._update_connection_dropdowns()
    
    def on_input_connection_changed(self, text: str):
        """
        Handle input connection change
        """
        if not self.selected_step_id:
            return
        
        # Check if step exists in self.steps before accessing
        if self.selected_step_id not in self.steps:
            QMessageBox.warning(self, "Warning", f"Step {self.selected_step_id} not found in internal storage")
            return
        
        # Get selected input step ID
        input_step_id = self.input_combo.currentData()
        
        # Update step input
        self.steps[self.selected_step_id].input_step_id = input_step_id
        
        # Update connections list
        self.connections = [conn for conn in self.connections 
                           if conn.to_step_id != self.selected_step_id]
        
        if input_step_id:
            # Add new connection
            connection = VisualConnection(input_step_id, self.selected_step_id)
            self.connections.append(connection)
        
        self.canvas.update_connections(self.connections)
        self.status_bar.showMessage(f"Updated input connection for {self.selected_step_id}")
    
    def on_connection_requested(self, from_step_id: str, to_step_id: str):
        """
        Handle connection request between steps
        """
        if (from_step_id in self.steps and to_step_id in self.steps and 
            from_step_id != to_step_id):
            
            # Check if connection already exists
            existing_conn = next(
                (conn for conn in self.connections 
                 if conn.from_step_id == from_step_id and conn.to_step_id == to_step_id), 
                None
            )
            
            if existing_conn:
                QMessageBox.information(self, "Info", "Connection already exists")
                return
            
            # Create visual connection
            connection = VisualConnection(from_step_id, to_step_id)
            self.connections.append(connection)
            
            # Update step connections
            from_step = self.steps[from_step_id]
            to_step = self.steps[to_step_id]
            
            from_step.output_steps.append(to_step_id)
            to_step.input_step_id = from_step_id
            
            # Update canvas and UI
            self.canvas.update_connections(self.connections)
            self._update_step_connections_in_ui(to_step_id)
            
            self.status_bar.showMessage(f"Connected: {from_step_id} → {to_step_id}")
    
    def start_connection_mode(self):
        """
        Start visual connection mode
        """
        if not self.selected_step_id:
            QMessageBox.warning(self, "Warning", "Please select a step first")
            return
        
        self.canvas.set_connection_mode(True)
        self.connection_mode_label.setText("Connection Mode: ON")
        self.connection_mode_label.setStyleSheet("color: green; font-weight: bold;")
        self.status_bar.showMessage("Click on first step, then on second step to create connection")
    
    def disconnect_selected_steps(self):
        """
        Disconnect selected steps
        """
        if not self.selected_step_id:
            QMessageBox.warning(self, "Warning", "Please select a step first")
            return
        
        # Check if step exists before accessing
        if self.selected_step_id not in self.steps:
            QMessageBox.warning(self, "Warning", f"Step {self.selected_step_id} not found in internal storage")
            return
        
        # Remove all connections for this step
        step = self.steps[self.selected_step_id]
        
        # Remove connections from global list
        for conn in self.connections[:]:  # Copy list to iterate safely
            if (conn.from_step_id == self.selected_step_id or 
                conn.to_step_id == self.selected_step_id):
                self.connections.remove(conn)
        
        # Clear step connections
        step.input_step_id = None
        step.output_steps.clear()
        
        # Update canvas
        self.canvas.update_connections(self.connections)
        self._update_step_connections_in_ui(self.selected_step_id)
        self.connection_mode_label.setText("Connection Mode: OFF")
        self.connection_mode_label.setStyleSheet("color: red; font-weight: bold;")
        self.status_bar.showMessage("Step disconnected")
    
    def remove_selected_step(self):
        """
        Remove selected step from pipeline
        """
        if not self.selected_step_id:
            QMessageBox.warning(self, "Warning", "Please select a step to remove")
            return
        
        # Check if step exists in self.steps before accessing
        if self.selected_step_id not in self.steps:
            QMessageBox.warning(self, "Warning", f"Step {self.selected_step_id} not found in internal storage")
            return
        
        reply = QMessageBox.question(
            self, 
            'Confirm Removal',
            f'Are you sure you want to remove step: {self.steps[self.selected_step_id].name}?',
            QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No,
            QMessageBox.StandardButton.No
        )
        
        if reply == QMessageBox.StandardButton.Yes:
            # Remove from canvas
            self.canvas.remove_step(self.selected_step_id)
            
            # Remove from internal storage
            del self.steps[self.selected_step_id]
            
            # Remove from connections
            self.connections = [
                conn for conn in self.connections 
                if not (conn.from_step_id == self.selected_step_id or conn.to_step_id == self.selected_step_id)
            ]
            
            # Update connection dropdowns
            self._update_connection_dropdowns()
            
            # Clear selection
            self.selected_step_id = None
            self._clear_step_config_ui()
            
            self.status_bar.showMessage(f"Step removed: {self.selected_step_id}")
    
    def add_output_connection(self):
        """
        Add output connection from selected step
        """
        if not self.selected_step_id:
            QMessageBox.warning(self, "Warning", "Please select a step first")
            return
        
        # Check if step exists before accessing
        if self.selected_step_id not in self.steps:
            QMessageBox.warning(self, "Warning", f"Step {self.selected_step_id} not found in internal storage")
            return
        
        # Create dialog to select target step
        from PyQt6.QtWidgets import QDialog, QVBoxLayout, QListWidget, QPushButton, QDialogButtonBox
        
        dialog = QDialog(self)
        dialog.setWindowTitle("Add Output Connection")
        dialog.setGeometry(200, 200, 400, 300)
        
        layout = QVBoxLayout(dialog)
        
        # List of available steps (excluding current step and already connected outputs)
        available_list = QListWidget()
        
        current_step = self.steps[self.selected_step_id]
        for step_id, step in self.steps.items():
            if (step_id != self.selected_step_id and 
                step_id not in current_step.output_steps):
                item = QListWidgetItem(step.name)
                item.setData(Qt.ItemDataRole.UserRole, step_id)
                available_list.addItem(item)
        
        layout.addWidget(available_list)
        
        # Buttons
        buttons = QDialogButtonBox(QDialogButtonBox.StandardButton.Ok | QDialogButtonBox.StandardButton.Cancel)
        buttons.accepted.connect(dialog.accept)
        buttons.rejected.connect(dialog.reject)
        layout.addWidget(buttons)
        
        if dialog.exec() == QDialog.DialogCode.Accepted:
            selected_items = available_list.selectedItems()
            if selected_items:
                target_step_id = selected_items[0].data(Qt.ItemDataRole.UserRole)
                
                # Add connection
                connection = VisualConnection(self.selected_step_id, target_step_id)
                self.connections.append(connection)
                
                # Update step connections
                from_step = self.steps[self.selected_step_id]
                to_step = self.steps[target_step_id]
                
                from_step.output_steps.append(target_step_id)
                to_step.input_step_id = self.selected_step_id
                
                # Update canvas
                self.canvas.update_connections(self.connections)
                self._update_step_connections_in_ui(self.selected_step_id)
                
                self.status_bar.showMessage(f"Added output connection: {self.selected_step_id} → {target_step_id}")
    
    def remove_output_connection(self):
        """
        Remove selected output connection
        """
        if not self.selected_step_id:
            QMessageBox.warning(self, "Warning", "Please select a step first")
            return
        
        selected_items = self.output_list.selectedItems()
        if not selected_items:
            QMessageBox.warning(self, "Warning", "Please select an output connection to remove")
            return
        
        output_name = selected_items[0].text()
        
        # Find the step ID for the selected output
        target_step_id = None
        for step_id, step in self.steps.items():
            if step.name == output_name:
                target_step_id = step_id
                break
        
        if target_step_id:
            # Remove from step connections
            self.steps[self.selected_step_id].output_steps = [
                sid for sid in self.steps[self.selected_step_id].output_steps 
                if sid != target_step_id
            ]
            
            # Remove from target step's input
            if self.steps[target_step_id].input_step_id == self.selected_step_id:
                self.steps[target_step_id].input_step_id = None
            
            # Remove from global connections
            self.connections = [
                conn for conn in self.connections 
                if not (conn.from_step_id == self.selected_step_id and conn.to_step_id == target_step_id)
            ]
            
            # Update canvas and UI
            self.canvas.update_connections(self.connections)
            self._update_step_connections_in_ui(self.selected_step_id)
            
            self.status_bar.showMessage(f"Removed output connection: {self.selected_step_id} → {target_step_id}")
    
    def _update_connection_dropdowns(self):
        """
        Update connection dropdowns with all available steps
        """
        # This updates the connection dropdowns when steps are added/removed
        pass
    
    def _update_step_connections_in_ui(self, step_id: str):
        """
        Update step connections display in UI
        """
        if step_id not in self.steps:
            return
        
        step = self.steps[step_id]
        
        # Update output list
        self.output_list.clear()
        for output_id in step.output_steps:
            if output_id in self.steps:
                self.output_list.addItem(self.steps[output_id].name)
    
    def _clear_step_config_ui(self):
        """
        Clear step configuration UI
        """
        self.step_id_label.setText("No step selected")
        self.step_type_label.setText("No step selected")
        self.step_name_edit.clear()
        
        # Clear all parameter widgets
        self.selected_docs_text.clear()
        self.style_config_path_edit.clear()
        self.batch_size_spin.setValue(100)
        self.parallel_workers_spin.setValue(4)
        
        self.script_id_edit.clear()
        self.timeout_spin.setValue(60)
        self.memory_limit_spin.setValue(200)
        
        self.delimiter_edit.setText(";")
        self.use_regex_checkbox.setChecked(False)
        self.preserve_delimiter_checkbox.setChecked(False)
        
        self.pattern_edit.clear()
        self.named_groups_checkbox.setChecked(True)
        self.case_insensitive_checkbox.setChecked(True)
        
        self.db_type_combo.setCurrentText("SQLite")
        self.db_host_edit.setText("localhost")
        self.db_port_spin.setValue(5432)
        self.db_name_edit.setText("chunks_db")
        self.db_username_edit.setText("")
        self.db_password_edit.setText("")
        self.table_name_edit.setText("chunks")
        self.batch_insert_size_spin.setValue(1000)
        
        self.output_format_combo.setCurrentText("JSON")
        self.output_path_edit.setText("./output")
        self.compression_checkbox.setChecked(False)
        self.pretty_print_checkbox.setChecked(True)
        
        # Hide all parameter groups
        self._hide_all_param_groups()
        
        # Clear input/output connections
        self.input_combo.clear()
        self.output_list.clear()
    
    def save_pipeline(self):
        """
        Save current pipeline configuration
        """
        if not self.steps:
            QMessageBox.warning(self, "Warning", "No steps in pipeline")
            return
    
        # Generate pipeline config
        pipeline_config = self._generate_pipeline_config()
    
        # Validate configuration
        try:
            validation_errors = self.pipeline_manager.validate_pipeline_config(pipeline_config)
            if validation_errors:
                error_msg = "\n".join(validation_errors)
                QMessageBox.critical(
                    self, 
                    "Validation Error", 
                    f"Pipeline configuration validation failed:\n{error_msg}"
                )
                return
        except Exception as e:
            QMessageBox.critical(self, "Error", f"Validation failed: {str(e)}")
            return
    
        # Save using pipeline manager and update current pipeline ID
        try:
            if self.current_pipeline_id:
                # Update existing pipeline
                success = self.pipeline_manager.update_pipeline(self.current_pipeline_id, pipeline_config)
                if success:
                    QMessageBox.information(self, "Success", f"Pipeline updated successfully!")
                    self.status_bar.showMessage(f"Pipeline updated: {self.current_pipeline_id}")
            else:
                # Create new pipeline
                self.current_pipeline_id = self.pipeline_manager.create_pipeline(pipeline_config)
                QMessageBox.information(self, "Success", f"Pipeline saved successfully!")
                self.status_bar.showMessage(f"Pipeline saved: {self.current_pipeline_id}")
        except Exception as e:
            QMessageBox.critical(self, "Error", f"Failed to save pipeline: {str(e)}")

    
    def _generate_pipeline_config(self) -> PipelineConfig:
        """
        Generate pipeline configuration from current steps and connections
        """
        from domain.pipeline import PipelineConfig, PipelineStepConfig, StepType
    
        # Create step configs
        step_configs = []
        for step_id, step in self.steps.items():
            # Create step config - use either input_step_id OR depends_on, not both
            step_config = PipelineStepConfig(
                type=StepType(step.type),
                id=step.id,
                name=step.name,
                params=step.params,
                input_step_id=step.input_step_id,  # For data flow (input from previous step)
                depends_on=[]  # For conditional dependencies - leave empty for now
            )
            step_configs.append(step_config)
    
        config = PipelineConfig(
            name=self.pipeline_name_edit.text() or "Untitled Pipeline",
            description=self.pipeline_description_edit.text(),
            steps=step_configs,
            schedule="",  # Will be set in scheduler tab
            source_config={},
            target_config={}
        )
    
        return config
    
    def load_pipeline(self):
        """
        Load pipeline configuration
        """
        file_path, _ = QFileDialog.getOpenFileName(
            self, 
            "Load Pipeline Configuration", 
            "", 
            "JSON Files (*.json);;All Files (*)"
        )
        
        if file_path:
            try:
                with open(file_path, 'r', encoding='utf-8') as f:
                    config_data = json.load(f)
                
                self._load_pipeline_from_data(config_data)
                QMessageBox.information(self, "Success", f"Pipeline loaded from: {file_path}")
                
            except Exception as e:
                QMessageBox.critical(self, "Error", f"Failed to load pipeline: {str(e)}")
    
    def _load_pipeline_from_data(self, config_data: Dict[str, Any]):
        """
        Load pipeline configuration from data dictionary
        """
        self.pipeline_name_edit.setText(config_data.get("name", "Untitled Pipeline"))
        self.pipeline_description_edit.setText(config_data.get("description", ""))
        
        # Clear existing steps
        self.steps.clear()
        self.connections.clear()
        self.canvas.clear_steps()
        
        # Load steps
        for step_data in config_data.get("steps", []):
            step = PipelineStepItem(
                step_id=step_data["id"],
                step_type=step_data["type"],
                name=step_data.get("name", ""),
                params=step_data.get("params", {})
            )
            step.input_step_id = step_data.get("input_step_id")
            step.output_steps = step_data.get("depends_on", [])  # Using depends_on for outputs
            
            self.steps[step.id] = step
            self.canvas.add_step(step)
        
        # Load connections based on step relationships
        for step_id, step in self.steps.items():
            if step.input_step_id:
                connection = VisualConnection(step.input_step_id, step_id)
                self.connections.append(connection)
        
        self.canvas.update_connections(self.connections)
        self._update_connection_dropdowns()
    
    def clear_pipeline(self):
        """
        Clear current pipeline
        """
        reply = QMessageBox.question(
            self, 
            'Confirm Clear',
            'Are you sure you want to clear the current pipeline?',
            QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No,
            QMessageBox.StandardButton.No
        )
        
        if reply == QMessageBox.StandardButton.Yes:
            self.steps.clear()
            self.connections.clear()
            self.selected_step_id = None
            self.current_pipeline_id = None
            self.canvas.clear_steps()
            self.pipeline_name_edit.clear()
            self.pipeline_description_edit.clear()
            self._clear_step_config_ui()
            self.status_bar.showMessage("Pipeline cleared")
    
    def run_pipeline(self):
        """
        Run current pipeline
        ✅ FIXED: Use current_pipeline_id to run the correct pipeline
        """
        if not self.steps:
            QMessageBox.warning(self, "Warning", "No steps in pipeline")
            return
    
        if not self.current_pipeline_id:
            QMessageBox.warning(self, "Warning", "Pipeline not saved yet. Please save pipeline first.")
            return
    
        # Get document paths to process
        doc_paths = self._get_document_paths_for_pipeline()
    
        if not doc_paths:
            QMessageBox.information(self, "Info", "No documents selected for processing")
            return
    
        # ✅ FIXED: Use the current_pipeline_id to run the correct pipeline
        try:
            run_id = self.pipeline_manager.execute_pipeline(
                self.current_pipeline_id,  # ← Use the saved pipeline ID
                doc_paths
            )
            QMessageBox.information(self, "Success", f"Pipeline executed successfully! Run ID: {run_id}")
        except Exception as e:
            QMessageBox.critical(self, "Error", f"Pipeline execution failed: {str(e)}")
    
    def _get_document_paths_for_pipeline(self) -> List[str]:
        """
        Get document paths for pipeline execution
        """
        # Check if any document loader step has paths configured
        for step in self.steps.values():
            if (step.type == "document_loader" and 
                "document_paths" in step.params and 
                step.params["document_paths"]):
                return step.params["document_paths"]
    
        # If no paths configured, prompt user
        file_paths, _ = QFileDialog.getOpenFileNames(
            self, 
            "Select Documents for Processing", 
            "", 
            "Documents (*.pdf *.docx *.txt);;PDF Files (*.pdf);;DOCX Files (*.docx);;TXT Files (*.txt);;All Files (*)"
        )
        return file_paths
    
    def view_pipeline_results(self):
        """
        View pipeline results and where they are saved
        """
        if not self.current_pipeline_id:
            QMessageBox.warning(self, "Warning", "No pipeline loaded or created yet")
            return
        
        # Show pipeline results in separate tab
        self.results_tab.setVisible(True)
        self._load_pipeline_results()
    
    def _create_results_view_panel(self) -> QWidget:
        """
        Create panel for viewing pipeline results
        """
        panel = QWidget()
        layout = QVBoxLayout(panel)
        
        # Results group
        results_group = QGroupBox("Pipeline Results")
        results_layout = QVBoxLayout(results_group)
        
        # Results summary
        self.results_summary = QTextEdit()
        self.results_summary.setReadOnly(True)
        self.results_summary.setFont(QFont("Consolas", 10))
        results_layout.addWidget(self.results_summary)
        
        # Results location info
        location_group = QGroupBox("Results Location")
        location_layout = QFormLayout(location_group)
        
        self.results_location_label = QLabel("Results location information will appear here")
        location_layout.addRow("Location:", self.results_location_label)
        
        self.results_format_label = QLabel("Format: Unknown")
        location_layout.addRow("Format:", self.results_format_label)
        
        results_layout.addWidget(location_group)
        
        # Action buttons
        action_layout = QHBoxLayout()
        
        self.refresh_results_button = QPushButton("Refresh Results")
        self.refresh_results_button.clicked.connect(self._load_pipeline_results)
        action_layout.addWidget(self.refresh_results_button)
        
        self.view_in_db_button = QPushButton("View in Database")
        self.view_in_db_button.clicked.connect(self._view_results_in_db)
        action_layout.addWidget(self.view_in_db_button)
        
        self.export_results_button = QPushButton("Export Results")
        self.export_results_button.clicked.connect(self._export_results)
        action_layout.addWidget(self.export_results_button)
        
        action_layout.addStretch()
        
        results_layout.addLayout(action_layout)
        
        layout.addWidget(results_group)
        
        return panel
    
    def _load_pipeline_results(self):
        """
        Load and display pipeline results
        """
        try:
            # Get run history for current pipeline from logging service
            from infrastructure.database.logging_service import LoggingService
            logging_service = LoggingService(self.db)
            runs = logging_service.get_run_history(self.current_pipeline_id, limit=10)
            
            # Display results summary
            summary = f"Pipeline Results for: {self.current_pipeline_id}\n"
            summary += "="*60 + "\n\n"
            
            if runs:
                for run in runs:
                    summary += f"Run ID: {run.get('id', 'Unknown')}\n"
                    summary += f"Status: {run.get('status', 'Unknown')}\n"
                    summary += f"Start Time: {run.get('start_time', 'Unknown')}\n"
                    summary += f"End Time: {run.get('end_time', 'Unknown')}\n"
                    summary += f"Processed: {run.get('processed_count', 0)}\n"
                    summary += f"Success: {run.get('success_count', 0)}\n"
                    summary += f"Errors: {run.get('error_count', 0)}\n"
                    
                    if run.get('errors'):
                        summary += "Errors:\n"
                        for error in run['errors'][:3]:  # Show first 3 errors
                            summary += f"  - {error.get('error_message', 'Unknown error')}\n"
                        if len(run['errors']) > 3:
                            summary += f"  ... and {len(run['errors']) - 3} more errors\n"
                    
                    summary += "-"*40 + "\n"
            else:
                summary += "No runs found for this pipeline.\n"
            
            self.results_summary.setText(summary)
            
            # Update location info
            from infrastructure.database.unified_db import UnifiedDatabase
            db_path = getattr(self.db, 'db_path', 'Unknown')
            self.results_location_label.setText(f"Database: {db_path}")
            self.results_format_label.setText("Format: SQLite")
            
        except Exception as e:
            # Handle error gracefully
            self.results_summary.setText(f"Error loading results: {str(e)}")
            self.results_location_label.setText("Error loading location info")
            self.results_format_label.setText("Error")
    
    def _view_results_in_db(self):
        """
        View results in database directly
        """
        QMessageBox.information(
            self, 
            "Database Viewer", 
            "Database viewer functionality would open here to see extracted chunks and results."
        )
    
    def _export_results(self):
        """
        Export results to external format
        """
        export_path, _ = QFileDialog.getSaveFileName(
            self, 
            "Export Results", 
            "", 
            "JSON Files (*.json);;CSV Files (*.csv);;All Files (*)"
        )
        
        if export_path:
            try:
                from infrastructure.database.logging_service import LoggingService
                logging_service = LoggingService(self.db)
                
                if export_path.lower().endswith('.json'):
                    runs = logging_service.get_run_history(self.current_pipeline_id, limit=100)
                    with open(export_path, 'w', encoding='utf-8') as f:
                        json.dump(runs, f, indent=2, ensure_ascii=False, default=str)
                else:
                    # CSV export
                    runs = logging_service.get_run_history(self.current_pipeline_id, limit=100)
                    import csv
                    with open(export_path, 'w', newline='', encoding='utf-8') as f:
                        writer = csv.writer(f)
                        writer.writerow(['Run ID', 'Status', 'Start Time', 'End Time', 'Processed', 'Success', 'Errors'])
                        for run in runs:
                            writer.writerow([
                                run.get('id', ''),
                                run.get('status', ''),
                                run.get('start_time', ''),
                                run.get('end_time', ''),
                                run.get('processed_count', 0),
                                run.get('success_count', 0),
                                run.get('error_count', 0)
                            ])
                
                QMessageBox.information(
                    self, 
                    "Success", 
                    f"Results exported to: {export_path}"
                )
            except Exception as e:
                QMessageBox.critical(self, "Error", f"Failed to export results: {str(e)}")
    
    def refresh(self):
        """
        Refresh the pipeline designer
        """
        pass