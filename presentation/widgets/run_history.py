#!/usr/bin/env python3
"""
Run History Widget - View pipeline execution history
"""

from PyQt6.QtWidgets import (QWidget, QVBoxLayout, QHBoxLayout, QTableWidget, 
                           QTableWidgetItem, QPushButton, QComboBox, QLineEdit,
                           QGroupBox, QFormLayout, QLabel, QHeaderView, QCheckBox,
                           QProgressBar, QSplitter, QTextEdit, QMessageBox)
from PyQt6.QtCore import Qt, QTimer
from typing import List, Dict, Any, Optional
import json
from datetime import datetime

class RunHistoryWidget(QWidget):
    """
    Widget for viewing pipeline execution history
    """
    
    def __init__(self, db):
        super().__init__()
        self.db = db
        
        # Create logging service without circular imports
        from infrastructure.database.logging_service import LoggingService
        self.logging_service = LoggingService(db)
        
        self.setup_ui()
        self.setup_connections()
        # Load history after widget is created to avoid recursion during init
        QTimer.singleShot(100, self.load_run_history)  # Small delay to allow widget to initialize
    
    def setup_ui(self):
        """
        Set up the user interface
        """
        layout = QVBoxLayout(self)
        
        # Top controls
        controls_layout = QHBoxLayout()
        
        self.pipeline_filter_combo = QComboBox()
        self.pipeline_filter_combo.addItem("All Pipelines", "all")
        self.pipeline_filter_combo.currentTextChanged.connect(self.on_pipeline_filter_changed)
        controls_layout.addWidget(QLabel("Filter Pipeline:"))
        controls_layout.addWidget(self.pipeline_filter_combo)
        
        self.status_filter_combo = QComboBox()
        self.status_filter_combo.addItems(["All", "Completed", "Failed", "Running", "Pending"])
        self.status_filter_combo.currentTextChanged.connect(self.on_status_filter_changed)
        controls_layout.addWidget(QLabel("Filter Status:"))
        controls_layout.addWidget(self.status_filter_combo)
        
        self.search_edit = QLineEdit()
        self.search_edit.setPlaceholderText("Search runs...")
        self.search_edit.textChanged.connect(self.on_search_changed)
        controls_layout.addWidget(QLabel("Search:"))
        controls_layout.addWidget(self.search_edit)
        
        self.refresh_button = QPushButton("Refresh")
        self.refresh_button.clicked.connect(self.load_run_history)
        controls_layout.addWidget(self.refresh_button)
        
        self.auto_refresh_checkbox = QCheckBox("Auto-refresh")
        self.auto_refresh_checkbox.stateChanged.connect(self.on_auto_refresh_toggled)
        controls_layout.addWidget(self.auto_refresh_checkbox)
        
        layout.addLayout(controls_layout)
        
        # Main content area
        splitter = QSplitter(Qt.Orientation.Vertical)
        
        # History table
        table_group = QGroupBox("Execution History")
        table_layout = QVBoxLayout(table_group)
        
        self.history_table = QTableWidget()
        self.history_table.setColumnCount(8)
        self.history_table.setHorizontalHeaderLabels([
            "Run ID", "Pipeline", "Start Time", "End Time", "Status", 
            "Processed", "Success", "Errors"
        ])
        self.history_table.horizontalHeader().setSectionResizeMode(QHeaderView.ResizeMode.Stretch)
        self.history_table.setSelectionBehavior(QTableWidget.SelectionBehavior.SelectRows)
        self.history_table.itemSelectionChanged.connect(self.on_run_selected)
        
        table_layout.addWidget(self.history_table)
        
        # Detail view
        detail_group = QGroupBox("Run Details")
        detail_layout = QVBoxLayout(detail_group)
        
        self.detail_view = QTextEdit()
        self.detail_view.setReadOnly(True)
        self.detail_view.setFontFamily("Consolas")
        self.detail_view.setFontPointSize(10)
        
        detail_layout.addWidget(self.detail_view)
        
        # Progress bar for ongoing runs
        self.progress_bar = QProgressBar()
        self.progress_bar.setVisible(False)
        detail_layout.addWidget(self.progress_bar)
        
        splitter.addWidget(table_group)
        splitter.addWidget(detail_group)
        splitter.setSizes([400, 300])
        
        layout.addWidget(splitter)
    
    def setup_connections(self):
        """
        Set up signal connections
        """
        pass
    
    def load_run_history(self):
        """
        Load run history from database using logging_service
        This method is designed to avoid recursion
        """
        try:
            # Get filter criteria
            pipeline_filter = self.pipeline_filter_combo.currentData()
            status_filter = self.status_filter_combo.currentText()
            search_term = self.search_edit.text().lower()
            
            # Load runs using logging_service (no recursion)
            all_runs = self.logging_service.get_run_history(
                pipeline_filter if pipeline_filter != "all" else None, 
                limit=100
            )
            
            # Apply status filter
            if status_filter != "All":
                all_runs = [
                    run for run in all_runs 
                    if run.get("status", "").lower() == status_filter.lower()
                ]
            
            # Apply search filter
            if search_term:
                all_runs = [
                    run for run in all_runs 
                    if (search_term in run.get("id", "").lower() or 
                        search_term in run.get("pipeline_id", "").lower())
                ]
            
            # Update table
            self.history_table.setRowCount(len(all_runs))
            
            for i, run in enumerate(all_runs):
                self.history_table.setItem(i, 0, QTableWidgetItem(run.get("id", "")))
                self.history_table.setItem(i, 1, QTableWidgetItem(run.get("pipeline_id", "")))
                self.history_table.setItem(i, 2, QTableWidgetItem(str(run.get("start_time", ""))[:19]))
                self.history_table.setItem(i, 3, QTableWidgetItem(str(run.get("end_time", ""))[:19]))
                self.history_table.setItem(i, 4, QTableWidgetItem(run.get("status", "")))
                self.history_table.setItem(i, 5, QTableWidgetItem(str(run.get("processed_count", 0))))
                self.history_table.setItem(i, 6, QTableWidgetItem(str(run.get("success_count", 0))))
                self.history_table.setItem(i, 7, QTableWidgetItem(str(run.get("error_count", 0))))
            
            # Update pipeline filter dropdown with available pipelines
            self._update_pipeline_filter_options(all_runs)
            
        except Exception as e:
            # Handle error gracefully without recursion
            print(f"Error loading run history: {e}")  # Use print instead of logging
            # Still show empty table
            self.history_table.setRowCount(0)
    
    def _update_pipeline_filter_options(self, runs: List[Dict[str, Any]]):
        """
        Update pipeline filter dropdown with available pipeline IDs
        """
        if not runs:
            return
        
        # Get unique pipeline IDs
        pipeline_ids = list(set(run.get("pipeline_id", "") for run in runs if run.get("pipeline_id")))
        
        # Clear and repopulate combo box
        self.pipeline_filter_combo.clear()
        self.pipeline_filter_combo.addItem("All Pipelines", "all")
        
        for pipeline_id in sorted(pipeline_ids):
            self.pipeline_filter_combo.addItem(pipeline_id, pipeline_id)
    
    def on_run_selected(self):
        """
        Handle run selection in table
        """
        selected_items = self.history_table.selectedItems()
        if not selected_items:
            return
        
        row = selected_items[0].row()
        run_id = self.history_table.item(row, 0).text()
        
        try:
            # Get run details using logging_service (no recursion)
            run_details = self.logging_service.get_run_details(run_id)
            
            # Display run details
            if run_details:
                detail_text = f"Run Details: {run_id}\n"
                detail_text += "="*50 + "\n"
                
                for key, value in run_details.items():
                    if key == "errors" and isinstance(value, list):
                        detail_text += f"{key.title()}: {len(value)} errors\n"
                        for error in value[:3]:  # Show first 3 errors
                            detail_text += f"  - {error.get('error_message', 'Unknown error')}\n"
                        if len(value) > 3:
                            detail_text += f"  ... and {len(value) - 3} more errors\n"
                    elif key not in ["id", "pipeline_id", "start_time", "end_time", "status", "processed_count", "success_count", "error_count"]:
                        detail_text += f"{key.title()}: {value}\n"
                
                self.detail_view.setText(detail_text)
                
                # Update progress bar for ongoing runs
                status = run_details.get("status", "").lower()
                if status == "running":
                    self.progress_bar.setVisible(True)
                    processed = run_details.get("processed_count", 0)
                    total = run_details.get("total_documents", 1)  # If not set, assume 1
                    progress = int((processed / total) * 100) if total > 0 else 0
                    self.progress_bar.setValue(progress)
                else:
                    self.progress_bar.setVisible(False)
            else:
                self.detail_view.setText(f"Run details not found for: {run_id}")
                
        except Exception as e:
            # Handle error without recursion
            self.detail_view.setText(f"Error loading run details: {str(e)}")
    
    def on_pipeline_filter_changed(self, text: str):
        """
        Handle pipeline filter change
        """
        self.load_run_history()
    
    def on_status_filter_changed(self, text: str):
        """
        Handle status filter change
        """
        self.load_run_history()
    
    def on_search_changed(self, text: str):
        """
        Handle search text change
        """
        self.load_run_history()
    
    def on_auto_refresh_toggled(self, state):
        """
        Handle auto-refresh toggle
        """
        if state == Qt.CheckState.Checked.value:
            self.refresh_timer = QTimer()
            self.refresh_timer.timeout.connect(self.load_run_history)
            self.refresh_timer.start(5000)  # Refresh every 5 seconds
        else:
            if hasattr(self, 'refresh_timer') and self.refresh_timer:
                self.refresh_timer.stop()
                self.refresh_timer = None
    
    def refresh(self):
        """
        Refresh the run history
        """
        self.load_run_history()