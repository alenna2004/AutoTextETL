"""
Presentation Layer - GUI and user interface components
Uses PyQt6 for desktop application
"""
from .main_window import MainWindow
from .widgets.pipeline_designer import PipelineDesigner
from .widgets.script_editor import ScriptEditor
from .widgets.scheduler_config import SchedulerConfig
from .widgets.db_connection import DbConnectionDialog
from .widgets.run_history import RunHistoryWidget
from .widgets.document_uploader import DocumentUploader
from .components.real_time_logger import RealTimeLogger
from .components.metadata_inspector import MetadataInspector

__all__ = [
    'MainWindow',
    'PipelineDesigner',
    'ScriptEditor',
    'SchedulerConfig',
    'DbConnectionDialog',
    'RunHistoryWidget',
    'DocumentUploader',
    'RealTimeLogger',
    'MetadataInspector'
]