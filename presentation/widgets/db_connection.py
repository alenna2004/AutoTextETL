#!/usr/bin/env python3
"""
Database Connection Widget - Configure target database connections
"""

from PyQt6.QtWidgets import (QWidget, QVBoxLayout, QHBoxLayout, QFormLayout, 
                           QGroupBox, QLineEdit, QPushButton, QComboBox, 
                           QSpinBox, QCheckBox, QLabel, QTableWidget, 
                           QTableWidgetItem, QHeaderView, QMessageBox, QInputDialog)
from PyQt6.QtCore import Qt, QTimer
from typing import Dict, Any, List, Optional
import json
import os

class DbConnectionDialog(QWidget):
    """
    Widget for configuring database connections
    """
    
    def __init__(self, db):
        super().__init__()
        self.db = db
        
        # Create config service directly (avoid circular imports)
        from infrastructure.database.config_service import ConfigService
        self.config_service = ConfigService(db)
        
        self.setup_ui()
        # Load connections in a separate thread to avoid blocking UI
        QTimer.singleShot(0, self.load_saved_connections)
    
    def setup_ui(self):
        """
        Set up the user interface
        """
        layout = QVBoxLayout(self)
        
        # Connection configuration group
        config_group = QGroupBox("Database Connection Configuration")
        config_layout = QFormLayout(config_group)
        
        # Database type selection
        self.db_type_combo = QComboBox()
        self.db_type_combo.addItems(["SQLite", "PostgreSQL", "MySQL", "MongoDB"])
        self.db_type_combo.currentTextChanged.connect(self.on_db_type_changed)
        config_layout.addRow("Database Type:", self.db_type_combo)
        
        # Host field
        self.host_edit = QLineEdit()
        self.host_edit.setPlaceholderText("localhost")
        config_layout.addRow("Host:", self.host_edit)
        
        # Port field
        self.port_spin = QSpinBox()
        self.port_spin.setMinimum(1)
        self.port_spin.setMaximum(65535)
        self.port_spin.setValue(5432)  # Default for PostgreSQL
        config_layout.addRow("Port:", self.port_spin)
        
        # Database name
        self.db_name_edit = QLineEdit()
        self.db_name_edit.setPlaceholderText("database_name")
        config_layout.addRow("Database:", self.db_name_edit)
        
        # Username
        self.username_edit = QLineEdit()
        self.username_edit.setPlaceholderText("username")
        config_layout.addRow("Username:", self.username_edit)
        
        # Password
        self.password_edit = QLineEdit()
        self.password_edit.setEchoMode(QLineEdit.EchoMode.Password)
        self.password_edit.setPlaceholderText("password")
        config_layout.addRow("Password:", self.password_edit)
        
        # SSL option
        self.ssl_checkbox = QCheckBox("Use SSL")
        config_layout.addRow("", self.ssl_checkbox)
        
        # Buttons
        button_layout = QHBoxLayout()
        
        self.test_button = QPushButton("Test Connection")
        self.test_button.clicked.connect(self.test_connection)
        button_layout.addWidget(self.test_button)
        
        self.save_button = QPushButton("Save Connection")
        self.save_button.clicked.connect(self.save_connection)
        button_layout.addWidget(self.save_button)
        
        self.load_button = QPushButton("Load Saved")
        self.load_button.clicked.connect(self.load_connection)
        button_layout.addWidget(self.load_button)
        
        config_layout.addRow("", button_layout)
        
        layout.addWidget(config_group)
        
        # Saved connections table
        table_group = QGroupBox("Saved Connections")
        table_layout = QVBoxLayout(table_group)
        
        self.connections_table = QTableWidget()
        self.connections_table.setColumnCount(4)
        self.connections_table.setHorizontalHeaderLabels(["Name", "Type", "Host", "Status"])
        self.connections_table.horizontalHeader().setSectionResizeMode(QHeaderView.ResizeMode.Stretch)
        
        table_layout.addWidget(self.connections_table)
        
        layout.addWidget(table_group)
        
        # Initialize with default values
        self.on_db_type_changed("SQLite")
    
    def on_db_type_changed(self, db_type: str):
        """
        Handle database type change
        """
        # Reset fields based on database type
        if db_type == "SQLite":
            self.host_edit.setEnabled(False)
            self.port_spin.setEnabled(False)
            self.username_edit.setEnabled(False)
            self.password_edit.setEnabled(False)
            self.ssl_checkbox.setEnabled(False)
            self.host_edit.setText("")
            self.port_spin.setValue(0)
            self.username_edit.setText("")
            self.password_edit.setText("")
        else:
            self.host_edit.setEnabled(True)
            self.port_spin.setEnabled(True)
            self.username_edit.setEnabled(True)
            self.password_edit.setEnabled(True)
            self.ssl_checkbox.setEnabled(True)
            
            # Set default ports
            if db_type == "PostgreSQL":
                self.port_spin.setValue(5432)
            elif db_type == "MySQL":
                self.port_spin.setValue(3306)
            elif db_type == "MongoDB":
                self.port_spin.setValue(27017)
    
    def test_connection(self):
        """
        Test database connection
        """
        config = self._get_current_config()
        
        try:
            # Test connection based on type
            db_type = config["type"]
            
            if db_type == "sqlite":
                import sqlite3
                conn = sqlite3.connect(config["path"])
                conn.execute("SELECT 1")
                conn.close()
            elif db_type == "postgresql":
                import psycopg2
                conn = psycopg2.connect(
                    host=config["host"],
                    port=config["port"],
                    database=config["database"],
                    user=config["user"],
                    password=config["password"]
                )
                conn.close()
            elif db_type == "mysql":
                import mysql.connector
                conn = mysql.connector.connect(**config)
                conn.close()
            elif db_type == "mongodb":
                import pymongo
                client = pymongo.MongoClient(config["uri"])
                client.admin.command('ping')
                client.close()
            
            QMessageBox.information(self, "Success", f"Connection to {db_type} successful!")
            
        except Exception as e:
            QMessageBox.critical(self, "Error", f"Connection failed: {str(e)}")
    
    def save_connection(self):
        """
        Save current connection configuration
        """
        name, ok = QInputDialog.getText(self, "Save Connection", "Connection Name:")
        if not ok or not name:
            return
        
        config = self._get_current_config()
        config["name"] = name
        
        # Save using config_service
        success = self.config_service.save_db_connection_config(config)
        if success:
            self.load_saved_connections()
            QMessageBox.information(self, "Success", f"Connection '{name}' saved successfully!")
        else:
            QMessageBox.warning(self, "Warning", "Failed to save connection")
    
    def load_connection(self):
        """
        Load selected connection from table
        """
        selected_items = self.connections_table.selectedItems()
        if not selected_items:
            QMessageBox.warning(self, "Warning", "Please select a connection to load")
            return
        
        row = selected_items[0].row()
        connection_name = self.connections_table.item(row, 0).text()
        
        # Load from database using config_service
        connections = self.config_service.list_db_connection_configs()
        connection = next((conn for conn in connections if conn["name"] == connection_name), None)
        
        if connection:
            self._load_config(connection)
            QMessageBox.information(self, "Success", f"Loaded connection: {connection_name}")
        else:
            QMessageBox.warning(self, "Warning", f"Connection '{connection_name}' not found")
    
    def load_saved_connections(self):
        """
        Load saved connections from database into table
        This method avoids recursion by using direct database queries
        """
        try:
            # Use config_service to get connections
            connections = self.config_service.list_db_connection_configs()
            
            self.connections_table.setRowCount(len(connections))
            
            for i, conn in enumerate(connections):
                self.connections_table.setItem(i, 0, QTableWidgetItem(conn.get("name", "Unnamed")))
                self.connections_table.setItem(i, 1, QTableWidgetItem(conn.get("type", "Unknown").title()))
                self.connections_table.setItem(i, 2, QTableWidgetItem(conn.get("host", "localhost")))
                
                # Test connection status
                try:
                    # Try to connect briefly to check status
                    status = "Active" if self._test_connection_quick(conn) else "Inactive"
                except:
                    status = "Error"
                
                self.connections_table.setItem(i, 3, QTableWidgetItem(status))
                
        except Exception as e:
            # Handle error gracefully without recursion
            QMessageBox.warning(self, "Warning", f"Could not load saved connections: {str(e)}")
            # Still show empty table
            self.connections_table.setRowCount(0)
    
    def _get_current_config(self) -> Dict[str, Any]:
        """
        Get current configuration from UI fields
        """
        db_type = self.db_type_combo.currentText().lower()
        
        if db_type == "sqlite":
            return {
                "type": "sqlite",
                "path": self.db_name_edit.text() or "chunks.db",
                "uri": f"sqlite:///{self.db_name_edit.text() or 'chunks.db'}"
            }
        else:
            config = {
                "type": db_type,
                "host": self.host_edit.text() or "localhost",
                "port": self.port_spin.value(),
                "database": self.db_name_edit.text(),
                "user": self.username_edit.text(),
                "password": self.password_edit.text(),
                "ssl": self.ssl_checkbox.isChecked()
            }
            
            # Build connection URI based on type
            if db_type == "postgresql":
                config["uri"] = f"postgresql://{config['user']}:{config['password']}@{config['host']}:{config['port']}/{config['database']}"
            elif db_type == "mysql":
                config["uri"] = f"mysql://{config['user']}:{config['password']}@{config['host']}:{config['port']}/{config['database']}"
            elif db_type == "mongodb":
                ssl_param = "?ssl=true" if config["ssl"] else ""
                config["uri"] = f"mongodb://{config['user']}:{config['password']}@{config['host']}:{config['port']}/{config['database']}{ssl_param}"
            
            return config
    
    def _load_config(self, config: Dict[str, Any]):
        """
        Load configuration into UI fields
        """
        self.db_type_combo.setCurrentText(config["type"].title())
        
        if config["type"] == "sqlite":
            self.db_name_edit.setText(config.get("path", "chunks.db"))
        else:
            self.host_edit.setText(config.get("host", "localhost"))
            self.port_spin.setValue(config.get("port", 5432))
            self.db_name_edit.setText(config.get("database", ""))
            self.username_edit.setText(config.get("user", ""))
            self.password_edit.setText(config.get("password", ""))
            self.ssl_checkbox.setChecked(config.get("ssl", False))
    
    def _test_connection_quick(self, config: Dict[str, Any]) -> bool:
        """
        Quick test of connection without full establishment
        """
        try:
            if config["type"] == "sqlite":
                import sqlite3
                conn = sqlite3.connect(config["path"])
                conn.close()
            elif config["type"] == "postgresql":
                import psycopg2
                conn = psycopg2.connect(
                    host=config["host"],
                    port=config["port"],
                    database=config["database"],
                    user=config["user"],
                    password=config["password"],
                    connect_timeout=5
                )
                conn.close()
            elif config["type"] == "mysql":
                import mysql.connector
                conn = mysql.connector.connect(**config, connection_timeout=5)
                conn.close()
            elif config["type"] == "mongodb":
                import pymongo
                client = pymongo.MongoClient(config["uri"], serverSelectionTimeoutMS=5000)
                client.admin.command('ping')
                client.close()
            
            return True
        except:
            return False