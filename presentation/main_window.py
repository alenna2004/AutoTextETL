#!/usr/bin/env python3
"""
Main Window - Entry point for the desktop application
"""

import sys
from pathlib import Path
from typing import Dict, Any, List, Optional

# Add project root to Python path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from PyQt6.QtWidgets import (QApplication, QMainWindow, QTabWidget, QVBoxLayout, 
                           QHBoxLayout, QWidget, QMenuBar, QStatusBar, QMessageBox,
                           QToolBar, QFileDialog, QLabel, QProgressBar)
from PyQt6.QtCore import Qt, QTimer
from PyQt6.QtGui import QIcon, QKeySequence, QAction

class MainWindow(QMainWindow):
    """
    Main application window with tabbed interface
    """
    
    def __init__(self, db, pipeline_manager):
        super().__init__()
        self.db = db
        self.pipeline_manager = pipeline_manager
        
        self.setWindowTitle("AutoTextETL - Document Processing Pipeline")
        self.setGeometry(100, 100, 1400, 900)
        
        self.setup_ui()
        self.setup_menu()
        self.setup_toolbar()
        self.setup_status_bar()
        
        # Timer for status updates
        self.status_timer = QTimer()
        self.status_timer.timeout.connect(self.update_status)
        self.status_timer.start(5000)  # Update every 5 seconds
    
    def setup_ui(self):
        """
        Set up the main user interface
        """
        # Central widget with tabs
        central_widget = QWidget()
        self.setCentralWidget(central_widget)
        
        layout = QVBoxLayout(central_widget)
        
        # Tab widget
        self.tab_widget = QTabWidget()
        
        # Create widgets lazily (only when needed) to avoid circular imports
        self._initialize_widgets()
        
        layout.addWidget(self.tab_widget)
    
    def _initialize_widgets(self):
        """
        Initialize widgets lazily to avoid circular imports
        """
        # Import only when creating widgets
        from .widgets.pipeline_designer import PipelineDesigner
        from .widgets.script_editor import ScriptEditor
        from .widgets.scheduler_config import SchedulerConfig
        from .widgets.db_connection import DbConnectionDialog
        from .widgets.run_history import RunHistoryWidget
        from .widgets.document_uploader import DocumentUploader
        from .components.metadata_inspector import MetadataInspector
        from .components.real_time_logger import RealTimeLogger
        
        # Create widgets with proper initialization
        self.pipeline_designer = PipelineDesigner(self.db, self.pipeline_manager)
        self.script_editor = ScriptEditor(self.db)
        self.scheduler_config = SchedulerConfig(self.db, self.pipeline_manager)
        self.db_connection_dialog = DbConnectionDialog(self.db)
        self.run_history = RunHistoryWidget(self.db)
        self.document_uploader = DocumentUploader(self.db, self.pipeline_manager)
        self.metadata_inspector = MetadataInspector()
        self.real_time_logger = RealTimeLogger()
        
        # Add tabs
        self.tab_widget.addTab(self.pipeline_designer, "Pipeline Designer")
        self.tab_widget.addTab(self.script_editor, "Script Editor")
        self.tab_widget.addTab(self.scheduler_config, "Scheduler")
        self.tab_widget.addTab(self.db_connection_dialog, "DB Connections")
        self.tab_widget.addTab(self.run_history, "Run History")
        self.tab_widget.addTab(self.document_uploader, "Upload Documents")
        self.tab_widget.addTab(self.metadata_inspector, "Metadata Inspector")
        self.tab_widget.addTab(self.real_time_logger, "Logs")
    
    def setup_menu(self):
        """
        Set up the main menu bar
        """
        menubar = self.menuBar()
        
        # File menu
        file_menu = menubar.addMenu("&File")
        
        new_action = QAction("&New Pipeline", self)
        new_action.setShortcut(QKeySequence.StandardKey.New)
        new_action.triggered.connect(self.new_pipeline)
        file_menu.addAction(new_action)
        
        open_action = QAction("&Open Pipeline", self)
        open_action.setShortcut(QKeySequence.StandardKey.Open)
        open_action.triggered.connect(self.open_pipeline)
        file_menu.addAction(open_action)
        
        save_action = QAction("&Save Pipeline", self)
        save_action.setShortcut(QKeySequence.StandardKey.Save)
        save_action.triggered.connect(self.save_pipeline)
        file_menu.addAction(save_action)
        
        file_menu.addSeparator()
        
        exit_action = QAction("&Exit", self)
        exit_action.setShortcut(QKeySequence.StandardKey.Quit)
        exit_action.triggered.connect(self.close)
        file_menu.addAction(exit_action)
        
        # Edit menu
        edit_menu = menubar.addMenu("&Edit")
        
        undo_action = QAction("&Undo", self)
        undo_action.setShortcut(QKeySequence.StandardKey.Undo)
        edit_menu.addAction(undo_action)
        
        redo_action = QAction("&Redo", self)
        redo_action.setShortcut(QKeySequence.StandardKey.Redo)
        edit_menu.addAction(redo_action)
        
        edit_menu.addSeparator()
        
        preferences_action = QAction("&Preferences", self)
        preferences_action.triggered.connect(self.preferences)
        edit_menu.addAction(preferences_action)
        
        # Tools menu
        tools_menu = menubar.addMenu("&Tools")
        
        style_analyzer_action = QAction("&Style Analyzer", self)
        style_analyzer_action.triggered.connect(self.open_style_analyzer)
        tools_menu.addAction(style_analyzer_action)
        
        config_manager_action = QAction("&Configuration Manager", self)
        config_manager_action.triggered.connect(self.open_config_manager)
        tools_menu.addAction(config_manager_action)
        
        # Help menu
        help_menu = menubar.addMenu("&Help")
        
        about_action = QAction("&About", self)
        about_action.triggered.connect(self.about)
        help_menu.addAction(about_action)
    
    def setup_toolbar(self):
        """
        Set up the toolbar
        """
        toolbar = self.addToolBar("Main Toolbar")
        toolbar.setMovable(False)
        
        # Add actions to toolbar
        run_action = QAction("Run Pipeline", self)
        run_action.triggered.connect(self.run_current_pipeline)
        toolbar.addAction(run_action)
        
        toolbar.addSeparator()
        
        stop_action = QAction("Stop Pipeline", self)
        stop_action.triggered.connect(self.stop_current_pipeline)
        toolbar.addAction(stop_action)
        
        toolbar.addSeparator()
        
        refresh_action = QAction("Refresh", self)
        refresh_action.triggered.connect(self.refresh_all_tabs)
        toolbar.addAction(refresh_action)
    
    def setup_status_bar(self):
        """
        Set up the status bar
        """
        self.status_bar = QStatusBar()
        self.setStatusBar(self.status_bar)
        
        # Add status labels
        self.status_label = QLabel("Ready")
        self.status_bar.addWidget(self.status_label)
        
        self.db_status_label = QLabel("DB: Connected")
        self.status_bar.addPermanentWidget(self.db_status_label)
        
        self.pipeline_status_label = QLabel("Pipelines: 0")
        self.status_bar.addPermanentWidget(self.pipeline_status_label)
        
        self.progress_bar = QProgressBar()
        self.progress_bar.setVisible(False)
        self.status_bar.addPermanentWidget(self.progress_bar)
    
    def update_status(self):
        """
        Update status bar information periodically
        Avoid recursion by not using logging services during status updates
        """
        # Update database status
        try:
            # Test database connection briefly
            cursor = self.db.connection.cursor()
            cursor.execute("SELECT 1")
            cursor.fetchone()
            cursor.close()
            self.db_status_label.setText("DB: Connected")
        except:
            self.db_status_label.setText("DB: Disconnected")
    
        # Update pipeline count
        try:
            # Use pipeline_manager directly without additional logging
            pipelines = self.pipeline_manager.list_pipelines()
            self.pipeline_status_label.setText(f"Pipelines: {len(pipelines)}")
        except Exception:
            self.pipeline_status_label.setText("Pipelines: Error")
    def new_pipeline(self):
        """Create new pipeline"""
        current_tab = self.tab_widget.currentWidget()
        if hasattr(current_tab, 'new_pipeline'):
            current_tab.new_pipeline()
        else:
            from .widgets.pipeline_designer import PipelineDesigner
            self.pipeline_designer.new_pipeline()
    
    def open_pipeline(self):
        """Open existing pipeline"""
        current_tab = self.tab_widget.currentWidget()
        if hasattr(current_tab, 'load_pipeline'):
            current_tab.load_pipeline()
        else:
            file_path, _ = QFileDialog.getOpenFileName(
                self, "Open Pipeline Configuration", "", "JSON Files (*.json);;All Files (*)"
            )
            if file_path:
                from .widgets.pipeline_designer import PipelineDesigner
                self.pipeline_designer.load_pipeline(file_path)
    
    def save_pipeline(self):
        """Save current pipeline"""
        current_tab = self.tab_widget.currentWidget()
        if hasattr(current_tab, 'save_pipeline'):
            current_tab.save_pipeline()
        else:
            file_path, _ = QFileDialog.getSaveFileName(
                self, "Save Pipeline Configuration", "", "JSON Files (*.json);;All Files (*)"
            )
            if file_path:
                from .widgets.pipeline_designer import PipelineDesigner
                self.pipeline_designer.save_pipeline(file_path)
    
    def run_current_pipeline(self):
        """Run currently selected pipeline"""
        current_tab = self.tab_widget.currentWidget()
        if hasattr(current_tab, 'run_pipeline'):
            current_tab.run_pipeline()
    
    def stop_current_pipeline(self):
        """Stop currently running pipeline"""
        current_tab = self.tab_widget.currentWidget()
        if hasattr(current_tab, 'stop_pipeline'):
            current_tab.stop_pipeline()
    
    def refresh_all_tabs(self):
        """Refresh all tabs"""
        for i in range(self.tab_widget.count()):
            widget = self.tab_widget.widget(i)
            if hasattr(widget, 'refresh'):
                widget.refresh()
    
    def preferences(self):
        """Open preferences dialog"""
        QMessageBox.information(self, "Preferences", "Preferences dialog coming soon!")
    
    def open_style_analyzer(self):
        """Open style analyzer tool"""
        from utilities.document_style_analyzer import interactive_style_configuration
        QMessageBox.information(self, "Style Analyzer", 
                              "Use the 'Upload Documents' tab to configure document styles!")
    
    def open_config_manager(self):
        """Open configuration manager"""
        QMessageBox.information(self, "Configuration Manager", 
                              "Configuration management features available in Pipeline Designer!")
    
    def about(self):
        """Show about dialog"""
        QMessageBox.about(
            self, 
            "About AutoTextETL", 
            "AutoTextETL - Automated Document Processing Pipeline\n\n"
            "Version: 1.0.0\n"
            "Framework: PyQt6\n"
            "Python: " + sys.version
        )
    
    def closeEvent(self, event):
        """
        Handle application close event
        """
        reply = QMessageBox.question(
            self, 
            'Confirm Exit',
            'Are you sure you want to quit?',
            QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No,
            QMessageBox.StandardButton.No
        )
        
        if reply == QMessageBox.StandardButton.Yes:
            # Close database connection
            self.db.close()
            
            # Close all child windows
            for widget in QApplication.topLevelWidgets():
                if widget != self:
                    widget.close()
            
            event.accept()
        else:
            event.ignore()