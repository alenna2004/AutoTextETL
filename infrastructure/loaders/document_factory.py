from typing import Dict, Type, Union
from domain.interfaces import IDocumentLoader
import importlib

class DocumentFactory:
    """
    Factory for creating document loaders
    """
    _loaders: Dict[str, str] = {}  # Store module.class strings, not actual classes
    
    @classmethod
    def register_loader(cls, format: str, module_class_path: str):
        """
        Register a new loader for a format
        Args:
            format: Format name (e.g., 'pdf', 'docx', 'txt')
            module_class_path: Full path to class (e.g., 'infrastructure.loaders.pdf_loader.PdfLoader')
        """
        cls._loaders[format.lower()] = module_class_path
    
    @classmethod
    def create_loader(cls, path: str) -> IDocumentLoader:
        """
        Create loader based on file extension
        Args:
            path: File path
        Returns:
            IDocumentLoader: Configured loader instance
        """
        ext = path.split('.')[-1].lower()
        
        if ext not in cls._loaders:
            supported = ", ".join(cls._loaders.keys())
            raise ValueError(f"Unsupported format: {ext}. Supported: {supported}")
        
        # Import the class dynamically to avoid circular imports
        module_path, class_name = cls._loaders[ext].rsplit('.', 1)
        module = importlib.import_module(module_path)
        loader_class = getattr(module, class_name)
        
        return loader_class()
    
    @classmethod
    def initialize(cls):
        """
        Register all available loaders
        """
        cls.register_loader("pdf", "infrastructure.loaders.pdf_loader.PdfLoader")
        cls.register_loader("docx", "infrastructure.loaders.docx.docx_loader.DocxLoader")  # ← CORRECT PATH
        cls.register_loader("txt", "infrastructure.loaders.txt_loader.TxtLoader")
    
    @classmethod
    def get_supported_formats(cls) -> list:
        """
        Get list of supported formats
        """
        return list(cls._loaders.keys())
    
    @classmethod
    def supports_format(cls, path: str) -> bool:
        """
        Check if format is supported
        """
        ext = path.split('.')[-1].lower()
        return ext in cls._loaders

# Initialize factory with default loaders
DocumentFactory.initialize()