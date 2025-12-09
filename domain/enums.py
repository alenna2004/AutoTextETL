from enum import Enum

class LogLevel(Enum):
    """
    Log levels for application logging
    """
    DEBUG = "DEBUG"
    INFO = "INFO"
    WARNING = "WARNING"
    ERROR = "ERROR"
    CRITICAL = "CRITICAL"