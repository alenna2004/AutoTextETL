from typing import Dict, Type, Union
from domain.interfaces import IDocumentLoader

class DocumentFactory:
    """
    Factory for creating document loaders
    """
    _loaders: Dict[str, Type[IDocumentLoader]] = {}
    
    @classmethod
    def register_loader(cls, format: str, loader_class: Type[IDocumentLoader]):
        """
        Register a new loader for a format
        """
        cls._loaders[format.lower()] = loader_class
    
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
        
        loader_class = cls._loaders[ext]
        return loader_class()
    
    @classmethod
    def initialize(cls):
        """
        Register all available loaders
        """
        # Import here to avoid circular imports
        from .pdf_loader import PdfLoader
        from .docx.docx_loader import DocxLoader  
        from .txt_loader import TxtLoader
        
        cls.register_loader("pdf", PdfLoader)
        cls.register_loader("docx", DocxLoader)  
        cls.register_loader("txt", TxtLoader)
    
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