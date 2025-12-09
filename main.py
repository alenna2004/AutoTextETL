#!/usr/bin/env python3
"""
AutoTextETL - Automated Document Processing Pipeline
Desktop application for extracting structured data from documents
"""

import sys
import os
from pathlib import Path
import argparse
import logging
from datetime import datetime
import tempfile

# Add project root to Python path
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

def setup_logging():
    """
    Set up application logging
    """
    # Create logs directory if it doesn't exist
    logs_dir = project_root / "logs"
    logs_dir.mkdir(exist_ok=True)
    
    # Configure logging
    log_file = logs_dir / f"app_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file, encoding='utf-8'),
            logging.StreamHandler(sys.stdout)
        ]
    )
    
    return logging.getLogger("AutoTextETL")

def create_directories():
    """
    Create required directories
    """
    directories = [
        "logs",
        "output",
        "config",
        "temp",
        "backups"
    ]
    
    for directory in directories:
        path = project_root / directory
        path.mkdir(exist_ok=True)

def initialize_database(db_path: str = "unified_storage.sqlite"):
    """
    Initialize database with required tables
    """
    from infrastructure.database.unified_db import UnifiedDatabase
    
    try:
        db = UnifiedDatabase(db_path)
        db.initialize_schema()  # ← FIXED: This method now exists
        db.create_default_configs()  # ← FIXED: This method now exists
        return db
    except Exception as e:
        # Use simple print since Qt may not be initialized yet
        print(f"ERROR: Failed to initialize database: {e}")
        import traceback
        traceback.print_exc()
        raise

def parse_arguments():
    """
    Parse command line arguments
    """
    parser = argparse.ArgumentParser(description="AutoTextETL - Document Processing Pipeline")
    parser.add_argument("--db-path", default="unified_storage.sqlite", help="Path to database file")
    parser.add_argument("--log-level", choices=["DEBUG", "INFO", "WARNING", "ERROR"], 
                       default="INFO", help="Logging level")
    parser.add_argument("--start-scheduler", action="store_true", 
                       help="Start background scheduler service")
    parser.add_argument("--config-file", help="Path to configuration file")
    parser.add_argument("--batch-mode", action="store_true", 
                       help="Run in batch mode without GUI")
    parser.add_argument("--input-files", nargs="*", help="Input files for batch processing")
    
    return parser.parse_args()

def initialize_services(db, start_scheduler: bool = True):
    """
    Initialize all application services
    """
    from application.pipeline_manager import PipelineManager
    from application.scheduler_service import SchedulerService
    from application.error_recovery import ErrorRecoveryService
    from application.resource_monitor import ResourceMonitor
    from infrastructure.loaders.document_factory import DocumentFactory
    
    services = {}
    
    # Initialize core services
    services["pipeline_manager"] = PipelineManager(db)
    services["scheduler_service"] = SchedulerService(db) if start_scheduler else None
    services["error_recovery"] = ErrorRecoveryService(db)
    services["resource_monitor"] = ResourceMonitor()
    
    # Initialize document factory
    DocumentFactory.initialize()
    
    # Start scheduler if requested
    if start_scheduler and services["scheduler_service"]:
        try:
            services["scheduler_service"].start()
            logging.info("Scheduler service started")
        except Exception as e:
            logging.error(f"Failed to start scheduler: {e}")
    
    return services

def run_batch_mode(args, db, services):
    """
    Run application in batch mode (command-line processing)
    """
    if not args.input_files:
        print("Error: --input-files required in batch mode")
        return 1
    
    pipeline_manager = services["pipeline_manager"]
    
    # Find default pipeline or use specified one
    pipeline_id = args.config_file or "default"
    
    try:
        # Load pipeline configuration
        if args.config_file:
            # Load from file
            config = pipeline_manager.load_pipeline_from_file(args.config_file)
        else:
            # Use default pipeline
            config = pipeline_manager.get_default_pipeline_config()
        
        # Process files
        print(f"Processing {len(args.input_files)} files...")
        for file_path in args.input_files:
            if not os.path.exists(file_path):
                print(f"Warning: File not found: {file_path}")
                continue
            
            print(f"Processing: {file_path}")
            run_id = pipeline_manager.execute_pipeline(config.id, [file_path])
            print(f"  Completed: {run_id}")
        
        print("Batch processing completed successfully!")
        return 0
        
    except Exception as e:
        print(f"Error in batch mode: {e}")
        import traceback
        traceback.print_exc()
        return 1

def run_gui_mode(db, services):
    """
    Run GUI application
    """
    from PyQt6.QtWidgets import QApplication, QMessageBox
    from PyQt6.QtGui import QIcon
    from presentation.main_window import MainWindow  # ← Import only when needed
    
    # Create QApplication FIRST (before any widgets)
    app = QApplication(sys.argv)
    app.setApplicationName("AutoTextETL")
    app.setApplicationVersion("1.0.0")
    
    # Set window icon if available
    icon_path = project_root / "resources" / "icons" / "app.ico"
    if icon_path.exists():
        app.setWindowIcon(QIcon(str(icon_path)))
    
    try:
        # Create main window
        main_window = MainWindow(db, services["pipeline_manager"])
        
        # Show main window
        main_window.show()
        
        logging.info("GUI application started successfully")
        
        # Run application event loop
        exit_code = app.exec()
        
        # Clean up services
        logging.info("Shutting down services")
        
        # Stop scheduler
        if services["scheduler_service"]:
            services["scheduler_service"].shutdown()
        
        # Close database
        db.close()
        
        logging.info("Application shutdown completed")
        
        return exit_code
        
    except Exception as e:
        logging.error(f"GUI application error: {e}")
        import traceback
        traceback.print_exc()
        
        # Show error dialog
        QMessageBox.critical(
            None, 
            "Application Error", 
            f"Failed to start GUI application:\n{str(e)}\n\nCheck logs for details."
        )
        
        return 1

def main():
    """
    Main application entry point
    """
    # Parse command line arguments
    args = parse_arguments()
    
    # Set up logging
    logger = setup_logging()
    logger.info("Starting AutoTextETL application")
    
    # Create required directories
    create_directories()
    
    try:
        # Initialize database
        logger.info(f"Initializing database: {args.db_path}")
        db = initialize_database(args.db_path)
        
        # Initialize services
        logger.info("Initializing application services")
        services = initialize_services(db, args.start_scheduler)
        
        # Check if running in batch mode
        if args.batch_mode:
            logger.info("Running in batch mode")
            return run_batch_mode(args, db, services)
        
        # Run GUI mode
        logger.info("Starting GUI application")
        return run_gui_mode(db, services)
        
    except Exception as e:
        logger.error(f"Application startup failed: {e}")
        import traceback
        traceback.print_exc()
        
        print(f"Error: {e}")
        return 1

def run_with_error_handling():
    """
    Run main function with comprehensive error handling
    """
    try:
        return main()
    except KeyboardInterrupt:
        print("\nApplication interrupted by user")
        return 0
    except Exception as e:
        print(f"Fatal error: {e}")
        import traceback
        traceback.print_exc()
        return 1

if __name__ == "__main__":
    sys.exit(run_with_error_handling())