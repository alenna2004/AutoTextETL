# presentation/widgets/scheduler_config.py
from PyQt6.QtWidgets import (QWidget, QVBoxLayout, QHBoxLayout, QTableWidget, 
                           QTableWidgetItem, QPushButton, QLineEdit, QComboBox,
                           QGroupBox, QFormLayout, QLabel, QCheckBox, QSpinBox,
                           QMessageBox, QHeaderView, QDateTimeEdit, QCalendarWidget)
from PyQt6.QtCore import Qt, QDateTime, QTimer
from PyQt6.QtGui import QAction, QKeySequence, QIcon

class SchedulerConfig(QWidget):
    """
    Pipeline scheduling configuration
    """
    
    def __init__(self, db, pipeline_manager):
        super().__init__()
        self.db = db
        self.pipeline_manager = pipeline_manager
        
        self.setup_ui()
        self.setup_connections()
        self.load_scheduled_pipelines()
    
    def setup_ui(self):
        """
        Set up the user interface
        """
        layout = QVBoxLayout(self)
        
        # Schedule configuration group
        config_group = QGroupBox("Schedule Configuration")
        config_layout = QFormLayout(config_group)
        
        # Pipeline selection
        self.pipeline_combo = QComboBox()
        self._load_pipeline_options()
        config_layout.addRow("Pipeline:", self.pipeline_combo)
        
        # Schedule type
        self.schedule_type_combo = QComboBox()
        self.schedule_type_combo.addItems(["Cron Expression", "Interval", "Daily", "Weekly"])
        self.schedule_type_combo.currentTextChanged.connect(self.on_schedule_type_changed)
        config_layout.addRow("Schedule Type:", self.schedule_type_combo)
        
        # Schedule expression
        self.schedule_expression = QLineEdit()
        self.schedule_expression.setPlaceholderText("Enter cron expression or interval")
        config_layout.addRow("Expression:", self.schedule_expression)
        
        # Document source
        self.document_source = QLineEdit()
        self.document_source.setPlaceholderText("Path to documents to process (wildcards supported)")
        config_layout.addRow("Document Source:", self.document_source)
        
        # Schedule buttons
        button_layout = QHBoxLayout()
        
        self.add_schedule_button = QPushButton("Add Schedule")
        self.add_schedule_button.clicked.connect(self.add_schedule)
        button_layout.addWidget(self.add_schedule_button)
        
        self.remove_schedule_button = QPushButton("Remove Selected")
        self.remove_schedule_button.clicked.connect(self.remove_selected_schedule)
        button_layout.addWidget(self.remove_schedule_button)
        
        config_layout.addRow("", button_layout)
        
        layout.addWidget(config_group)
        
        # Scheduled pipelines table
        table_group = QGroupBox("Scheduled Pipelines")
        table_layout = QVBoxLayout(table_group)
        
        self.scheduled_table = QTableWidget()
        self.scheduled_table.setColumnCount(5)
        self.scheduled_table.setHorizontalHeaderLabels(["Pipeline", "Schedule", "Next Run", "Status", "Actions"])
        self.scheduled_table.horizontalHeader().setSectionResizeMode(QHeaderView.ResizeMode.Stretch)
        
        table_layout.addWidget(self.scheduled_table)
        
        layout.addWidget(table_group)
    
    def setup_connections(self):
        """
        Set up signal connections
        """
        pass
    
    def _load_pipeline_options(self):
        """
        Load available pipelines into combo box
        """
        try:
            pipelines = self.pipeline_manager.list_pipelines()
            for pipeline in pipelines:
                self.pipeline_combo.addItem(pipeline.get("name", "Unknown"), pipeline.get("id"))
        except Exception:
            # If pipeline manager fails, add placeholder
            self.pipeline_combo.addItem("No pipelines available", "")
    
    def on_schedule_type_changed(self, schedule_type: str):
        """
        Handle schedule type change
        """
        if schedule_type == "Cron Expression":
            self.schedule_expression.setPlaceholderText("Enter cron expression (e.g., 0 2 * * * for daily at 2 AM)")
        elif schedule_type == "Interval":
            self.schedule_expression.setPlaceholderText("Enter interval in seconds (e.g., 3600 for hourly)")
        elif schedule_type == "Daily":
            self.schedule_expression.setPlaceholderText("Enter time (HH:MM, e.g., 02:00)")
        elif schedule_type == "Weekly":
            self.schedule_expression.setPlaceholderText("Enter day and time (Day HH:MM, e.g., Monday 02:00)")
    
    def add_schedule(self):
        """
        Add new schedule
        """
        pipeline_id = self.pipeline_combo.currentData()
        if not pipeline_id:
            QMessageBox.warning(self, "Warning", "Please select a pipeline")
            return
        
        schedule_expr = self.schedule_expression.text().strip()
        if not schedule_expr:
            QMessageBox.warning(self, "Warning", "Please enter a schedule expression")
            return
        
        document_source = self.document_source.text().strip()
        if not document_source:
            QMessageBox.warning(self, "Warning", "Please enter document source path")
            return
        
        try:
            # Validate cron expression if it's cron type
            schedule_type = self.schedule_type_combo.currentText()
            if schedule_type == "Cron Expression":
                self._validate_cron_expression(schedule_expr)
            
            # Add schedule using scheduler service
            from application.scheduler_service import SchedulerService
            scheduler = SchedulerService(self.db)
            
            job_id = scheduler.schedule_pipeline(pipeline_id, schedule_expr, [document_source])
            
            QMessageBox.information(self, "Success", f"Pipeline scheduled successfully. Job ID: {job_id}")
            self.load_scheduled_pipelines()
            
        except Exception as e:
            QMessageBox.critical(self, "Error", f"Failed to schedule pipeline: {str(e)}")
    
    def _validate_cron_expression(self, cron_expr: str):
        """
        Validate cron expression format
        """
        parts = cron_expr.strip().split()
        if len(parts) != 5:
            raise ValueError(f"Cron expression must have 5 parts, got {len(parts)}")
        
        # Validate each part
        for i, part in enumerate(parts):
            if part == "*":
                continue
            elif part.isdigit():
                continue
            elif "-" in part:  # Range
                start, end = part.split("-")
                if not (start.isdigit() and end.isdigit()):
                    raise ValueError(f"Invalid range in cron expression: {part}")
            elif "," in part:  # List
                values = part.split(",")
                for val in values:
                    if not val.isdigit():
                        raise ValueError(f"Invalid list value in cron expression: {val}")
            elif "/" in part:  # Step
                base, step = part.split("/")
                if base != "*" and not base.isdigit():
                    raise ValueError(f"Invalid step base in cron expression: {base}")
                if not step.isdigit():
                    raise ValueError(f"Invalid step value in cron expression: {step}")
            else:
                raise ValueError(f"Invalid cron expression part: {part}")
    
    def remove_selected_schedule(self):
        """
        Remove selected schedule
        """
        selected_rows = self.scheduled_table.selectionModel().selectedRows()
        if not selected_rows:
            QMessageBox.warning(self, "Warning", "Please select a schedule to remove")
            return
        
        row = selected_rows[0].row()
        job_id = self.scheduled_table.item(row, 0).text()  # Assuming job_id is in first column
        
        try:
            from application.scheduler_service import SchedulerService
            scheduler = SchedulerService(self.db)
            
            success = scheduler.cancel_scheduled_pipeline(job_id)
            if success:
                self.scheduled_table.removeRow(row)
                QMessageBox.information(self, "Success", "Schedule removed successfully")
            else:
                QMessageBox.warning(self, "Warning", "Failed to remove schedule")
        except Exception as e:
            QMessageBox.critical(self, "Error", f"Failed to remove schedule: {str(e)}")
    
    def load_scheduled_pipelines(self):
        """
        Load scheduled pipelines into table
        """
        try:
            from application.scheduler_service import SchedulerService
            scheduler = SchedulerService(self.db)
            
            scheduled_pipelines = scheduler.get_scheduled_pipelines()
            
            self.scheduled_table.setRowCount(len(scheduled_pipelines))
            
            for i, job in enumerate(scheduled_pipelines):
                self.scheduled_table.setItem(i, 0, QTableWidgetItem(job.get("job_id", "")))
                self.scheduled_table.setItem(i, 1, QTableWidgetItem(job.get("pipeline_name", "")))
                self.scheduled_table.setItem(i, 2, QTableWidgetItem(job.get("cron_expression", "")))
                self.scheduled_table.setItem(i, 3, QTableWidgetItem(job.get("next_run_time", "")))
                self.scheduled_table.setItem(i, 4, QTableWidgetItem(job.get("status", "ACTIVE")))
            
        except Exception as e:
            QMessageBox.warning(self, "Warning", f"Could not load scheduled pipelines: {str(e)}")
    
    def refresh(self):
        """
        Refresh the scheduler configuration
        """
        self.load_scheduled_pipelines()