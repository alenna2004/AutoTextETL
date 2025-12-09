#!/usr/bin/env python3
"""
Pipeline Designer - Visual pipeline construction interface
"""

from PyQt6.QtWidgets import (QWidget, QVBoxLayout, QHBoxLayout, QGridLayout, 
                           QTreeWidget, QTreeWidgetItem, QPushButton, QLineEdit,
                           QComboBox, QGroupBox, QScrollArea, QLabel, QFrame,
                           QSplitter, QListWidget, QListWidgetItem, QCheckBox,
                           QSpinBox, QDoubleSpinBox, QTextEdit, QFormLayout,
                           QFileDialog, QMessageBox, QTabWidget, QTableWidget,
                           QTableWidgetItem, QHeaderView)
from PyQt6.QtCore import Qt, pyqtSignal
from PyQt6.QtGui import QFont, QPalette
from typing import Dict, Any, List
import json
import uuid

class PipelineDesigner(QWidget):
    """
    Visual pipeline designer with drag-and-drop functionality
    """
    
    pipeline_changed = pyqtSignal(str)  # Emits pipeline_id when pipeline is modified
    
    def __init__(self, db, pipeline_manager):
        super().__init__()
        self.db = db
        self.pipeline_manager = pipeline_manager
        self.current_pipeline_id = None
        self.current_pipeline_config = None
        
        self.setup_ui()
        self.setup_connections()
    
    def setup_ui(self):
        """
        Set up the user interface
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
        
        self.save_button = QPushButton("Save")
        self.save_button.clicked.connect(self.save_pipeline)
        controls_layout.addWidget(self.save_button)
        
        self.load_button = QPushButton("Load")
        self.load_button.clicked.connect(self.load_pipeline_dialog)
        controls_layout.addWidget(self.load_button)
        
        self.run_button = QPushButton("Run")
        self.run_button.clicked.connect(self.run_pipeline)
        self.run_button.setStyleSheet("QPushButton { background-color: #4CAF50; color: white; }")
        controls_layout.addWidget(self.run_button)
        
        self.stop_button = QPushButton("Stop")
        self.stop_button.clicked.connect(self.stop_pipeline)
        self.stop_button.setStyleSheet("QPushButton { background-color: #f44336; color: white; }")
        controls_layout.addWidget(self.stop_button)
        
        layout.addLayout(controls_layout)
        
        # Main content area with splitter
        splitter = QSplitter(Qt.Orientation.Horizontal)
        
        # Left panel - Available steps
        left_panel = self._create_left_panel()
        splitter.addWidget(left_panel)
        
        # Right panel - Pipeline canvas
        right_panel = self._create_right_panel()
        splitter.addWidget(right_panel)
        
        splitter.setSizes([300, 900])
        layout.addWidget(splitter)
    
    def _create_left_panel(self) -> QWidget:
        """
        Create left panel with available pipeline steps
        """
        panel = QWidget()
        layout = QVBoxLayout(panel)
        
        # Available steps group
        steps_group = QGroupBox("Available Steps")
        steps_layout = QVBoxLayout(steps_group)
        
        self.steps_tree = QTreeWidget()
        self.steps_tree.setHeaderLabels(["Step Type", "Description"])
        self.steps_tree.setDragEnabled(True)
        self.steps_tree.setDropIndicatorShown(True)
        
        # Add step categories
        loader_category = QTreeWidgetItem(self.steps_tree, ["Document Loaders", ""])
        loader_category.addChild(QTreeWidgetItem(["> Document Loader", "Load PDF/TXT/DOCX files"]))
        loader_category.addChild(QTreeWidgetItem(["> Line Splitter", "Split text by lines"]))
        loader_category.addChild(QTreeWidgetItem(["> Delimiter Splitter", "Split by custom delimiter"]))
        
        processor_category = QTreeWidgetItem(self.steps_tree, ["Processors", ""])
        processor_category.addChild(QTreeWidgetItem(["> Paragraph Splitter", "Split by paragraphs"]))
        processor_category.addChild(QTreeWidgetItem(["> Sentence Splitter", "Split by sentences"]))
        processor_category.addChild(QTreeWidgetItem(["> Regex Extractor", "Extract data with regex"]))
        processor_category.addChild(QTreeWidgetItem(["> User Script", "Execute custom Python script"]))
        
        exporter_category = QTreeWidgetItem(self.steps_tree, ["Exporters", ""])
        exporter_category.addChild(QTreeWidgetItem(["> DB Exporter", "Export to database"]))
        exporter_category.addChild(QTreeWidgetItem(["> File Exporter", "Export to file"]))
        
        self.steps_tree.expandAll()
        steps_layout.addWidget(self.steps_tree)
        
        layout.addWidget(steps_group)
        
        # Parameters editor
        params_group = QGroupBox("Step Parameters")
        params_layout = QVBoxLayout(params_group)
        
        self.params_editor = QTextEdit()
        self.params_editor.setPlaceholderText("Parameters will appear here when step is selected")
        params_layout.addWidget(self.params_editor)
        
        layout.addWidget(params_group)
        
        return panel
    
    def _create_right_panel(self) -> QWidget:
        """
        Create right panel with pipeline canvas
        """
        panel = QWidget()
        layout = QVBoxLayout(panel)
        
        # Pipeline steps canvas
        canvas_group = QGroupBox("Pipeline Steps")
        canvas_layout = QVBoxLayout(canvas_group)
        
        self.pipeline_canvas = QTreeWidget()
        self.pipeline_canvas.setHeaderLabels(["Step", "Type", "Parameters", "Connections"])
        self.pipeline_canvas.setAcceptDrops(True)
        self.pipeline_canvas.setDragDropMode(QTreeWidget.DragDropMode.InternalMove)
        
        canvas_layout.addWidget(self.pipeline_canvas)
        
        # Buttons
        buttons_layout = QHBoxLayout()
        
        self.add_step_button = QPushButton("Add Step")
        self.add_step_button.clicked.connect(self.add_step)
        buttons_layout.addWidget(self.add_step_button)
        
        self.remove_step_button = QPushButton("Remove Step")
        self.remove_step_button.clicked.connect(self.remove_selected_step)
        buttons_layout.addWidget(self.remove_step_button)
        
        self.clear_pipeline_button = QPushButton("Clear Pipeline")
        self.clear_pipeline_button.clicked.connect(self.clear_pipeline)
        buttons_layout.addWidget(self.clear_pipeline_button)
        
        canvas_layout.addLayout(buttons_layout)
        
        layout.addWidget(canvas_group)
        
        return panel
    
    def setup_connections(self):
        """
        Set up signal connections
        """
        self.pipeline_canvas.itemChanged.connect(self.on_step_changed)
        self.pipeline_canvas.itemSelectionChanged.connect(self.on_step_selected)
    
    def add_step(self):
        """
        Add step to pipeline canvas
        """
        # Get selected step from left panel
        selected_items = self.steps_tree.selectedItems()
        if not selected_items:
            QMessageBox.warning(self, "Warning", "Please select a step type to add")
            return
        
        step_item = selected_items[0]
        step_type = step_item.text(0).lstrip('> ').strip()
        
        # Create new step item
        step_id = f"step_{uuid.uuid4().hex[:8]}"
        step_item = QTreeWidgetItem(self.pipeline_canvas)
        step_item.setText(0, step_id)
        step_item.setText(1, step_type)
        step_item.setFlags(step_item.flags() | Qt.ItemFlag.ItemIsEditable)
        
        # Add default parameters based on step type
        self._set_default_parameters(step_item, step_type)
    
    def _set_default_parameters(self, step_item: QTreeWidgetItem, step_type: str):
        """
        Set default parameters for different step types
        """
        default_params = {
            "Document Loader": {
                "source_path": "",
                "style_config_path": ""
            },
            "Line Splitter": {
                "preserve_empty": True
            },
            "Delimiter Splitter": {
                "delimiter": ";",
                "use_regex": False
            },
            "Paragraph Splitter": {
                "min_lines": 1,
                "preserve_formatting": True
            },
            "Sentence Splitter": {
                "language": "en"
            },
            "Regex Extractor": {
                "pattern": "",
                "named_groups_only": False
            },
            "User Script": {
                "script_id": "",
                "timeout_seconds": 60
            },
            "DB Exporter": {
                "target_db_config": {},
                "table_name": "chunks",
                "batch_size": 1000
            },
            "File Exporter": {
                "output_format": "json",
                "output_path": "./output"
            }
        }
        
        params = default_params.get(step_type, {})
        step_item.setText(2, json.dumps(params, indent=2))
    
    def remove_selected_step(self):
        """
        Remove selected step from pipeline
        """
        selected_items = self.pipeline_canvas.selectedItems()
        if selected_items:
            item = selected_items[0]
            self.pipeline_canvas.takeTopLevelItem(self.pipeline_canvas.indexOfTopLevelItem(item))
    
    def clear_pipeline(self):
        """
        Clear all steps from pipeline
        """
        reply = QMessageBox.question(
            self, 
            "Confirm Clear", 
            "Are you sure you want to clear the entire pipeline?",
            QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No
        )
        
        if reply == QMessageBox.StandardButton.Yes:
            self.pipeline_canvas.clear()
            self.current_pipeline_id = None
            self.current_pipeline_config = None
    
    def on_step_changed(self, item: QTreeWidgetItem, column: int):
        """
        Handle step property changes
        """
        if column == 0:  # Step ID changed
            self.pipeline_changed.emit(self.current_pipeline_id or "new_pipeline")
    
    def on_step_selected(self):
        """
        Handle step selection - show parameters
        """
        selected_items = self.pipeline_canvas.selectedItems()
        if selected_items:
            item = selected_items[0]
            params_text = item.text(2)
            self.params_editor.setPlainText(params_text)
    
    def new_pipeline(self):
        """
        Create new pipeline
        """
        self.clear_pipeline()
        self.pipeline_name_edit.clear()
        self.pipeline_description_edit.clear()
        self.current_pipeline_id = None
        self.current_pipeline_config = None
    
    def load_pipeline(self, file_path: str = None):
        """
        Load pipeline from file or show dialog
        """
        if file_path is None:
            file_path, _ = QFileDialog.getOpenFileName(
                self, "Load Pipeline Configuration", "", "JSON Files (*.json);;All Files (*)"
            )
            if not file_path:
                return
        
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                config_data = json.load(f)
            
            self._load_pipeline_from_data(config_data)
            QMessageBox.information(self, "Success", f"Pipeline loaded from: {file_path}")
        except Exception as e:
            QMessageBox.critical(self, "Error", f"Failed to load pipeline: {str(e)}")
    
    def load_pipeline_dialog(self):
        """
        Show dialog to load pipeline
        """
        self.load_pipeline()
    
    def _load_pipeline_from_data(self, config_data: Dict[str, Any]):
        """
        Load pipeline configuration from data
        """
        self.pipeline_name_edit.setText(config_data.get("name", ""))
        self.pipeline_description_edit.setText(config_data.get("description", ""))
        
        # Clear existing steps
        self.pipeline_canvas.clear()
        
        # Add steps from configuration
        for step_data in config_data.get("steps", []):
            step_item = QTreeWidgetItem(self.pipeline_canvas)
            step_item.setText(0, step_data.get("id", f"step_{uuid.uuid4().hex[:8]}"))
            step_item.setText(1, step_data.get("type", "Unknown"))
            step_item.setText(2, json.dumps(step_data.get("params", {}), indent=2))
        
        self.current_pipeline_id = config_data.get("id")
        self.current_pipeline_config = config_data
    
    def save_pipeline(self, file_path: str = None):
        """
        Save pipeline to file or show dialog
        """
        if file_path is None:
            file_path, _ = QFileDialog.getSaveFileName(
                self, "Save Pipeline Configuration", "", "JSON Files (*.json);;All Files (*)"
            )
            if not file_path:
                return
        
        try:
            config_data = self._get_pipeline_config_data()
            
            with open(file_path, 'w', encoding='utf-8') as f:
                json.dump(config_data, f, indent=2, ensure_ascii=False)
            
            QMessageBox.information(self, "Success", f"Pipeline saved to: {file_path}")
        except Exception as e:
            QMessageBox.critical(self, "Error", f"Failed to save pipeline: {str(e)}")
    
    def _get_pipeline_config_data(self) -> Dict[str, Any]:
        """
        Get pipeline configuration data from current UI state
        """
        steps = []
        for i in range(self.pipeline_canvas.topLevelItemCount()):
            item = self.pipeline_canvas.topLevelItem(i)
            
            # Parse parameters from JSON text
            try:
                params = json.loads(item.text(2))
            except json.JSONDecodeError:
                params = {}
            
            step_data = {
                "id": item.text(0),
                "type": item.text(1),
                "params": params,
                "input_step_id": None,  # Will be set based on connections
                "depends_on": []  # Will be set based on dependencies
            }
            steps.append(step_data)
        
        return {
            "id": self.current_pipeline_id or f"pipeline_{uuid.uuid4().hex[:8]}",
            "name": self.pipeline_name_edit.text(),
            "description": self.pipeline_description_edit.text(),
            "steps": steps,
            "schedule": "",  # Will be set in scheduler tab
            "source_config": {},  # Will be set based on source
            "target_config": {},  # Will be set based on target
            "version": 1
        }
    
    def run_pipeline(self):
        """
        Run current pipeline
        """
        if not self.pipeline_canvas.topLevelItemCount():
            QMessageBox.warning(self, "Warning", "Pipeline is empty. Add some steps first.")
            return
        
        # Get document paths from document uploader
        document_paths = self._get_document_paths()
        if not document_paths:
            QMessageBox.warning(self, "Warning", "No documents selected for processing.")
            return
        
        try:
            # Create pipeline configuration
            config_data = self._get_pipeline_config_data()
            
            # Execute pipeline
            pipeline_id = self.pipeline_manager.create_pipeline(config_data)
            run_id = self.pipeline_manager.execute_pipeline(pipeline_id, document_paths)
            
            QMessageBox.information(self, "Success", f"Pipeline started. Run ID: {run_id}")
        except Exception as e:
            QMessageBox.critical(self, "Error", f"Failed to run pipeline: {str(e)}")
    
    def stop_pipeline(self):
        """
        Stop running pipeline
        """
        if self.current_pipeline_id:
            success = self.pipeline_manager.cancel_running_pipeline(self.current_pipeline_id)
            if success:
                QMessageBox.information(self, "Success", "Pipeline stopped successfully")
            else:
                QMessageBox.information(self, "Info", "No running pipeline to stop")
        else:
            QMessageBox.information(self, "Info", "No pipeline selected")
    
    def _get_document_paths(self) -> List[str]:
        """
        Get document paths from document uploader (or show dialog)
        """
        # This would typically get paths from the document uploader tab
        # For now, show a simple dialog
        file_paths, _ = QFileDialog.getOpenFileNames(
            self, "Select Documents to Process", "", "Documents (*.pdf *.docx *.txt);;All Files (*)"
        )
        return file_paths
    
    def refresh(self):
        """
        Refresh the pipeline designer
        """
        pass  # No special refresh needed for this widget