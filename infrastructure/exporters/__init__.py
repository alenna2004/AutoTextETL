"""
Database exporters implementations
"""
# Don't import classes here to avoid circular imports
# Only define what's available

__all__ = [
    'TargetDbExporter',
    'IDbExporter',
    'SqliteExporter',
    'PostgresExporter', 
    'MysqlExporter',
    'MongoDbExporter',
    'JsonExporter'
]

# Lazy loading function
def get_exporter_class(format_name: str):
    """
    Get exporter class by format name (lazy loading to avoid circular imports)
    """
    format_lower = format_name.lower()
    
    if format_lower == "sqlite":
        from .sqlite_exporter import SqliteExporter
        return SqliteExporter
    elif format_lower == "postgres":
        from .postgres_exporter import PostgresExporter
        return PostgresExporter
    elif format_lower == "mysql":
        from .mysql_exporter import MysqlExporter
        return MysqlExporter
    elif format_lower == "mongodb":
        from .mongodb_exporter import MongoDbExporter
        return MongoDbExporter
    elif format_lower == "json":
        from .json_exporter import JsonExporter
        return JsonExporter
    else:
        raise ValueError(f"Unsupported format: {format_name}")