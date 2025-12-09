from PyQt6.QtWidgets import (QWidget, QVBoxLayout, QHBoxLayout, QTextEdit, 
                           QPushButton, QLineEdit, QLabel, QSplitter,
                           QGroupBox, QTreeWidget, QTreeWidgetItem, QTabWidget,
                           QMessageBox, QFileDialog, QStatusBar, QComboBox,
                           QCheckBox, QSpinBox, QDoubleSpinBox, QFormLayout,
                           QTableWidget, QTableWidgetItem, QHeaderView, QMenuBar,
                           QToolBar, QInputDialog, QCalendarWidget)
from PyQt6.QtCore import Qt, pyqtSignal, QThread, QTimer
from PyQt6.QtGui import (QFont, QTextCharFormat, QColor, QSyntaxHighlighter, QFontDatabase, 
                       QKeySequence, QShortcut, QAction, QIcon)
import re
import ast
# Import from domain layer
from domain.chunk import Chunk, Metadata, ChunkType
from domain.pipeline import PipelineRun, PipelineStatus

# Import from infrastructure
from infrastructure.security.script_sandbox import ScriptSandbox
from infrastructure.security.script_sandbox import ScriptSecurityValidator, SecurityError

class PythonSyntaxHighlighter(QSyntaxHighlighter):
    """
    Syntax highlighter for Python code
    """
    
    def __init__(self, parent=None):
        super().__init__(parent)
        
        # Keyword format
        keyword_format = QTextCharFormat()
        keyword_format.setForeground(QColor("#0000FF"))  # Blue
        keyword_format.setFontWeight(QFont.Weight.Bold)
        
        keywords = [
            'and', 'as', 'assert', 'break', 'class', 'continue', 'def', 'del', 'elif', 'else',
            'except', 'exec', 'finally', 'for', 'from', 'global', 'if', 'import', 'in', 'is',
            'lambda', 'not', 'or', 'pass', 'print', 'raise', 'return', 'try', 'while', 'with', 'yield'
        ]
        
        self.highlighting_rules = []
        for keyword in keywords:
            pattern = re.compile(r'\b' + keyword + r'\b')
            self.highlighting_rules.append((pattern, keyword_format))
        
        # String format
        string_format = QTextCharFormat()
        string_format.setForeground(QColor("#00AA00"))  # Green
        self.highlighting_rules.append((re.compile(r'"[^"]*"'), string_format))
        self.highlighting_rules.append((re.compile(r"'[^']*'"), string_format))
        
        # Comment format
        comment_format = QTextCharFormat()
        comment_format.setForeground(QColor("#808080"))  # Gray
        self.highlighting_rules.append((re.compile(r'#.*'), comment_format))
    
    def highlightBlock(self, text):
        """
        Highlight a block of text
        """
        for pattern, format_ in self.highlighting_rules:
            for match in pattern.finditer(text):
                start, end = match.span()
                self.setFormat(start, end - start, format_)

class ScriptEditor(QWidget):
    """
    Python script editor with syntax highlighting and validation
    """
    
    script_saved = pyqtSignal(str)  # Emits script_id when saved
    
    def __init__(self, db):
        super().__init__()
        self.db = db
        self.current_script_id = None
        self.current_script_name = ""
        
        self.setup_ui()
        self.setup_connections()
        self.load_script_list()
    
    def setup_ui(self):
        """
        Set up the user interface
        """
        layout = QVBoxLayout(self)
        
        # Top controls
        controls_layout = QHBoxLayout()
        
        self.script_name_edit = QLineEdit()
        self.script_name_edit.setPlaceholderText("Script Name")
        controls_layout.addWidget(QLabel("Name:"))
        controls_layout.addWidget(self.script_name_edit)
        
        self.save_button = QPushButton("Save Script")
        self.save_button.clicked.connect(self.save_script)
        controls_layout.addWidget(self.save_button)
        
        self.load_button = QPushButton("Load Script")
        self.load_button.clicked.connect(self.load_selected_script)  # ← FIXED: Method now exists
        controls_layout.addWidget(self.load_button)
        
        self.test_button = QPushButton("Test Script")
        self.test_button.clicked.connect(self.test_script)
        controls_layout.addWidget(self.test_button)
        
        self.validate_button = QPushButton("Validate Security")
        self.validate_button.clicked.connect(self.validate_script_security)
        controls_layout.addWidget(self.validate_button)
        
        layout.addLayout(controls_layout)
        
        # Main content area
        splitter = QSplitter(Qt.Orientation.Horizontal)
        
        # Left panel - Script list
        left_panel = self._create_script_list_panel()
        splitter.addWidget(left_panel)
        
        # Right panel - Editor
        right_panel = self._create_editor_panel()
        splitter.addWidget(right_panel)
        
        splitter.setSizes([250, 750])
        layout.addWidget(splitter)
        
        # Status bar
        self.status_bar = QStatusBar()
        layout.addWidget(self.status_bar)
    
    def _create_script_list_panel(self) -> QWidget:
        """
        Create left panel with script list
        """
        panel = QWidget()
        layout = QVBoxLayout(panel)
        
        # Script list group
        list_group = QGroupBox("Saved Scripts")
        list_layout = QVBoxLayout(list_group)
        
        self.script_list = QTreeWidget()
        self.script_list.setHeaderLabels(["Name", "Created", "Updated"])
        self.script_list.itemClicked.connect(self.on_script_selected)
        
        list_layout.addWidget(self.script_list)
        
        layout.addWidget(list_group)
        
        return panel
    
    def _create_editor_panel(self) -> QWidget:
        """
        Create right panel with code editor
        """
        panel = QWidget()
        layout = QVBoxLayout(panel)
        
        # Code editor group
        editor_group = QGroupBox("Script Editor")
        editor_layout = QVBoxLayout(editor_group)
        
        # Use regular QTextEdit with syntax highlighting
        self.code_editor = QTextEdit()
        self.code_editor.setFontFamily("Consolas")
        self.code_editor.setFontPointSize(10)
        
        # Apply syntax highlighting
        self.syntax_highlighter = PythonSyntaxHighlighter(self.code_editor.document())
        
        editor_layout.addWidget(self.code_editor)
        
        # Security status
        security_group = QGroupBox("Security Status")
        security_layout = QHBoxLayout(security_group)
        
        self.security_status_label = QLabel("Status: Ready")
        security_layout.addWidget(self.security_status_label)
        
        layout.addWidget(editor_group)
        layout.addWidget(security_group)
        
        return panel
    
    def setup_connections(self):
        """
        Set up signal connections
        """
        pass
    
    def load_script_list(self):
        """
        Load list of saved scripts from database
        """
        try:
            from infrastructure.database.script_manager import ScriptManager
            script_manager = ScriptManager(self.db)
            
            scripts = script_manager.list_scripts()
            
            self.script_list.clear()
            for script in scripts:
                item = QTreeWidgetItem(self.script_list)
                item.setText(0, script.get("name", "Untitled"))
                item.setText(1, script.get("created_at", "")[:19])
                item.setText(2, script.get("updated_at", "")[:19])
                item.setData(0, Qt.ItemDataRole.UserRole, script.get("id"))
            
        except Exception as e:
            QMessageBox.warning(self, "Warning", f"Could not load script list: {str(e)}")
    
    def on_script_selected(self, item: QTreeWidgetItem, column: int):
        """
        Handle script selection from list
        """
        script_id = item.data(0, Qt.ItemDataRole.UserRole)
        if script_id:
            self.load_script(script_id)
    
    def load_script(self, script_id: str):
        """
        Load script from database
        """
        try:
            from infrastructure.database.script_manager import ScriptManager
            script_manager = ScriptManager(self.db)
            
            script_data = script_manager.load_script(script_id)
            if script_data:
                self.current_script_id = script_id
                self.current_script_name = script_data.get("name", "")
                self.script_name_edit.setText(self.current_script_name)
                self.code_editor.setPlainText(script_data["code"])
                
                self.status_bar.showMessage(f"Loaded script: {self.current_script_name}")
            else:
                QMessageBox.warning(self, "Warning", "Script not found")
        except Exception as e:
            QMessageBox.critical(self, "Error", f"Failed to load script: {str(e)}")
    
    def load_selected_script(self):
        """
        Load selected script from list (callback for load button)
        """
        selected_items = self.script_list.selectedItems()
        if not selected_items:
            QMessageBox.warning(self, "Warning", "Please select a script to load")
            return
        
        item = selected_items[0]
        script_id = item.data(0, Qt.ItemDataRole.UserRole)
        if script_id:
            self.load_script(script_id)
    
    def save_script(self):
        """
        Save current script to database
        """
        script_name = self.script_name_edit.text().strip()
        if not script_name:
            QMessageBox.warning(self, "Warning", "Please enter a script name")
            return
        
        script_code = self.code_editor.toPlainText()
        if not script_code.strip():
            QMessageBox.warning(self, "Warning", "Script is empty")
            return
        
        try:
            # Validate syntax first
            ast.parse(script_code)
        except SyntaxError as e:
            QMessageBox.critical(self, "Syntax Error", f"Invalid Python syntax: {str(e)}")
            return
        
        try:
            from infrastructure.database.script_manager import ScriptManager
            script_manager = ScriptManager(self.db)
            
            if self.current_script_id:
                # Update existing script
                success = script_manager.update_script(self.current_script_id, script_name, script_code)
            else:
                # Create new script
                self.current_script_id = script_manager.save_script(script_name, script_code)
                success = True
            
            if success:
                self.status_bar.showMessage(f"Script saved: {script_name}")
                self.script_saved.emit(self.current_script_id)
                self.load_script_list()  # Refresh list
                QMessageBox.information(self, "Success", f"Script saved: {script_name}")
            else:
                QMessageBox.critical(self, "Error", "Failed to save script")
                
        except Exception as e:
            QMessageBox.critical(self, "Error", f"Failed to save script: {str(e)}")
    
    def test_script(self):
        """
        Test script execution in sandbox
        """
        script_code = self.code_editor.toPlainText()
        if not script_code.strip():
            QMessageBox.warning(self, "Warning", "Script is empty")
            return
        
        try:
            # Validate syntax
            ast.parse(script_code)
        except SyntaxError as e:
            QMessageBox.critical(self, "Syntax Error", f"Invalid Python syntax: {str(e)}")
            return
        
        try:
            from infrastructure.security.script_sandbox import ScriptSandbox
            sandbox = ScriptSandbox(timeout=30, memory_limit_mb=200)
            
            # Create test context
            test_context = {
                "input": {
                    "text": "Test input text for validation",
                    "metadata": {
                        "document_id": "test_doc",
                        "page_num": 1,
                        "section_id": "test_section"
                    }
                },
                "pipeline_run": None,
                "step_config": None
            }
            
            result = sandbox.execute_script(script_code, test_context)
            
            QMessageBox.information(
                self, 
                "Test Successful", 
                f"Script executed successfully!\nResult: {str(result)[:100]}..."
            )
            self.status_bar.showMessage("Script test completed successfully")
            
        except Exception as e:
            QMessageBox.critical(self, "Test Failed", f"Script execution failed: {str(e)}")
            self.status_bar.showMessage(f"Script test failed: {str(e)}")
    
    def validate_script_security(self):
        """
        Validate script security (no dangerous imports/functions)
        """
        script_code = self.code_editor.toPlainText()
        if not script_code.strip():
            self.security_status_label.setText("Status: Empty script")
            return
        
        try:
            from infrastructure.security.script_sandbox import ScriptSecurityValidator
            security_errors = ScriptSecurityValidator.validate_script_security(script_code)
            
            if security_errors:
                self.security_status_label.setText(f"Status: ❌ {len(security_errors)} security issues")
                error_text = "\n".join(security_errors)
                QMessageBox.warning(self, "Security Issues", f"Security validation failed:\n{error_text}")
            else:
                self.security_status_label.setText("Status: ✅ No security issues")
                QMessageBox.information(self, "Security Check", "Script is secure!")
                
        except Exception as e:
            self.security_status_label.setText(f"Status: ❌ Validation error: {str(e)}")
            QMessageBox.critical(self, "Error", f"Security validation failed: {str(e)}")
    
    def refresh(self):
        """
        Refresh the script editor
        """
        self.load_script_list()