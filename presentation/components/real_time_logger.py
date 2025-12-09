from PyQt6.QtWidgets import (QWidget, QVBoxLayout, QTextEdit, QPushButton, 
                           QHBoxLayout, QLabel, QProgressBar, QCheckBox,
                           QComboBox)
from PyQt6.QtCore import Qt, pyqtSignal, QThread, QTimer
from PyQt6.QtGui import QFont, QColor, QTextCharFormat, QAction, QKeySequence, QIcon
import logging
import queue
import threading
from datetime import datetime

class LogConsumerThread(QThread):
    """
    Thread to consume log messages from queue
    """
    log_signal = pyqtSignal(str, str)  # (message, level)
    
    def __init__(self, log_queue):
        super().__init__()
        self.log_queue = log_queue
        self.running = True
    
    def run(self):
        """
        Consume log messages from queue
        """
        while self.running:
            try:
                log_entry = self.log_queue.get(timeout=0.1)
                if log_entry is None:  # Shutdown signal
                    break
                
                message, level = log_entry
                self.log_signal.emit(message, level)
            except queue.Empty:
                continue
    
    def stop(self):
        """
        Stop the consumer thread
        """
        self.running = False
        self.log_queue.put(None)  # Send shutdown signal

class RealTimeLogger(QWidget):
    """
    Real-time log viewer component
    """
    
    def __init__(self):
        super().__init__()
        self.log_queue = queue.Queue()
        self.consumer_thread = None
        
        self.setup_ui()
        self.setup_logging()
        self.start_consumer()
    
    def setup_ui(self):
        """
        Set up the user interface
        """
        layout = QVBoxLayout(self)
        
        # Controls
        controls_layout = QHBoxLayout()
        
        self.clear_button = QPushButton("Clear")
        self.clear_button.clicked.connect(self.clear_logs)
        controls_layout.addWidget(self.clear_button)
        
        self.level_combo = QComboBox()
        self.level_combo.addItems(["ALL", "INFO", "WARNING", "ERROR", "CRITICAL"])
        self.level_combo.currentTextChanged.connect(self.on_level_filter_changed)
        controls_layout.addWidget(QLabel("Filter:"))
        controls_layout.addWidget(self.level_combo)
        
        self.auto_scroll_checkbox = QCheckBox("Auto-scroll")
        self.auto_scroll_checkbox.setChecked(True)
        controls_layout.addWidget(self.auto_scroll_checkbox)
        
        controls_layout.addStretch()
        
        layout.addLayout(controls_layout)
        
        # Log display
        self.log_display = QTextEdit()
        self.log_display.setReadOnly(True)
        self.log_display.setFont(QFont("Consolas", 10))
        
        layout.addWidget(self.log_display)
        
        # Status bar
        status_layout = QHBoxLayout()
        
        self.status_label = QLabel("Ready")
        status_layout.addWidget(self.status_label)
        
        self.message_count_label = QLabel("Messages: 0")
        status_layout.addWidget(self.message_count_label)
        
        layout.addLayout(status_layout)
    
    def setup_logging(self):
        """
        Set up logging to feed messages to queue
        """
        # Create custom handler that puts messages in queue
        class QueueHandler(logging.Handler):
            def __init__(self, log_queue):
                super().__init__()
                self.log_queue = log_queue
            
            def emit(self, record):
                try:
                    msg = self.format(record)
                    level = record.levelname
                    self.log_queue.put((msg, level))
                except Exception:
                    self.handleError(record)
        
        # Configure logger
        self.logger = logging.getLogger("AutoTextETL")
        self.logger.setLevel(logging.DEBUG)
        
        # Add queue handler
        queue_handler = QueueHandler(self.log_queue)
        queue_handler.setFormatter(logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        ))
        self.logger.addHandler(queue_handler)
    
    def start_consumer(self):
        """
        Start log consumer thread
        """
        self.consumer_thread = LogConsumerThread(self.log_queue)
        self.consumer_thread.log_signal.connect(self.append_log_message)
        self.consumer_thread.start()
    
    def append_log_message(self, message: str, level: str):
        """
        Append log message to display
        """
        # Apply color based on level
        cursor = self.log_display.textCursor()
        cursor.movePosition(cursor.MoveOperation.End)
        
        format_ = QTextCharFormat()
        
        if level == "ERROR" or level == "CRITICAL":
            format_.setForeground(QColor("#FF0000"))  # Red
        elif level == "WARNING":
            format_.setForeground(QColor("#FFA500"))  # Orange
        elif level == "INFO":
            format_.setForeground(QColor("#0000FF"))  # Blue
        else:
            format_.setForeground(QColor("#000000"))  # Black
        
        cursor.insertText(message + "\n", format_)
        
        # Auto-scroll if enabled
        if self.auto_scroll_checkbox.isChecked():
            self.log_display.verticalScrollBar().setValue(
                self.log_display.verticalScrollBar().maximum()
            )
        
        # Update message count
        self.message_count += 1
        self.message_count_label.setText(f"Messages: {self.message_count}")
        
        # Update status
        self.status_label.setText(f"Last: {datetime.now().strftime('%H:%M:%S')}")
    
    def on_level_filter_changed(self, level: str):
        """
        Handle level filter change
        """
        # For simplicity, we'll just clear and show all messages
        # In a real implementation, you'd filter messages
        pass
    
    def clear_logs(self):
        """
        Clear all log messages
        """
        self.log_display.clear()
        self.message_count = 0
        self.message_count_label.setText("Messages: 0")
        self.status_label.setText("Cleared")
    
    def log_message(self, message: str, level: str = "INFO"):
        """
        Log a message (can be called from other components)
        """
        self.logger.log(getattr(logging, level.upper()), message)
    
    def closeEvent(self, event):
        """
        Clean up when closing
        """
        if self.consumer_thread:
            self.consumer_thread.stop()
            self.consumer_thread.wait()
        event.accept()
    
    def refresh(self):
        """
        Refresh the logger
        """
        pass