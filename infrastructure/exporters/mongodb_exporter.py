#!/usr/bin/env python3
"""
MongoDB Exporter - Export chunks to MongoDB database
"""

from typing import List, Dict, Any, Optional
from domain.interfaces import IDbExporter  # ← Fixed import path
from domain.document import Document, Section
from domain.chunk import Chunk, Metadata, ChunkType
from domain.pipeline import PipelineRun, PipelineStatus
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure, DuplicateKeyError
import json
from datetime import datetime
import uuid

class MongoDbExporter(IDbExporter):
    """
    MongoDB database exporter implementation
    """
    
    def __init__(self):
        self.client = None
        self.database = None
        self.is_connected = False
        self.connection_config = None
        self._connected_at = None
    
    def connect(self, config: Dict[str, Any]):
        """
        Connect to MongoDB
        Args:
            config: MongoDB connection configuration
        """
        self.connection_config = config
        
        # Build connection URI
        host = config.get("host", "localhost")
        port = config.get("port", 27017)
        database_name = config.get("database", "chunks_db")
        username = config.get("username")
        password = config.get("password")
        auth_source = config.get("auth_source", "admin")
        
        if username and password:
            uri = f"mongodb://{username}:{password}@{host}:{port}/{database_name}?authSource={auth_source}"
        else:
            uri = f"mongodb://{host}:{port}/{database_name}"
        
        # Additional options
        options = {}
        if "ssl" in config:
            options["ssl"] = config["ssl"]
        if "replica_set" in config:
            options["replicaSet"] = config["replica_set"]
        if "read_preference" in config:
            options["readPreference"] = config["read_preference"]
        
        # Connect to MongoDB
        self.client = MongoClient(uri, **options)
        
        try:
            # Test connection
            self.client.admin.command('ping')
        except ConnectionFailure:
            raise RuntimeError("Failed to connect to MongoDB")
        
        self.database = self.client[database_name]
        self.is_connected = True
        self._connected_at = datetime.now()
        
        # Create indexes for performance
        self._create_indexes()
    
    def _create_indexes(self):
        """
        Create indexes for better query performance
        """
        # Chunks collection indexes
        chunks_collection = self.database.chunks
        chunks_collection.create_index([("document_id", 1)])
        chunks_collection.create_index([("page_num", 1)])
        chunks_collection.create_index([("pipeline_run_id", 1)])
        chunks_collection.create_index([("created_at", -1)])  # Descending for recent queries
        
        # Pipeline runs collection indexes
        runs_collection = self.database.pipeline_runs
        runs_collection.create_index([("pipeline_id", 1)])
        runs_collection.create_index([("status", 1)])
        runs_collection.create_index([("start_time", -1)])
    
    def batch_insert(self, chunks: List[Chunk], collection_name: str = "chunks"):
        """
        Batch insert chunks to MongoDB
        Args:
            chunks: List of chunks to insert
            collection_name: Target collection name
        """
        if not self.is_connected:
            raise RuntimeError("MongoDB not connected. Call connect() first.")
        
        if not chunks:
            return  # Nothing to insert
        
        collection = self.database[collection_name]
        
        # Prepare documents for MongoDB insertion
        documents = []
        for chunk in chunks:
            doc = {
                "_id": chunk.id,
                "text_content": chunk.text,
                "document_id": chunk.meta.document_id,
                "page_num": chunk.meta.page_num,
                "section_id": chunk.meta.section_id,
                "section_title": chunk.meta.section_title,
                "section_level": chunk.meta.section_level,
                "chunk_type": chunk.meta.chunk_type.value if hasattr(chunk.meta.chunk_type, 'value') else str(chunk.meta.chunk_type),
                "pipeline_run_id": chunk.meta.pipeline_run_id,
                "source_type": chunk.meta.source_type,
                "line_num": chunk.meta.line_num,
                "extraction_results": chunk.extraction_results,
                "created_at": datetime.now()
            }
            documents.append(doc)
        
        # Perform bulk insert
        try:
            result = collection.insert_many(documents, ordered=False)  # Continue on error
            return len(result.inserted_ids)
        except DuplicateKeyError:
            # Handle duplicate IDs - insert one by one to skip duplicates
            inserted_count = 0
            for doc in documents:
                try:
                    collection.insert_one(doc)
                    inserted_count += 1
                except DuplicateKeyError:
                    # Skip duplicate documents
                    continue
            return inserted_count
    
    def export_run_metadata(self, run: PipelineRun):
        """
        Export pipeline run metadata to MongoDB
        Args:
            run: Pipeline run instance
        """
        if not self.is_connected:
            raise RuntimeError("MongoDB not connected. Call connect() first.")
        
        run_document = {
            "_id": run.id,
            "pipeline_id": run.pipeline_id,
            "start_time": run.start_time,
            "end_time": run.end_time,
            "status": run.status.value if hasattr(run.status, 'value') else str(run.status),
            "processed_count": run.processed_count,
            "success_count": run.success_count,
            "error_count": run.error_count,
            "errors": run.errors,
            "metadata": run.metadata,
            "exported_at": datetime.now()
        }
        
        collection = self.database.pipeline_runs
        try:
            collection.replace_one(
                {"_id": run.id}, 
                run_document, 
                upsert=True  # Insert if doesn't exist, update if exists
            )
        except Exception as e:
            raise RuntimeError(f"Failed to export run meta {str(e)}")
    
    def close(self):
        """
        Close MongoDB connection
        """
        if self.client:
            self.client.close()
            self.is_connected = False
            self.client = None
            self.database = None
            self.connection_config = None
            self._connected_at = None
    
    def get_connection_status(self) -> Dict[str, Any]:
        """
        Get MongoDB connection status
        """
        status = {
            "is_connected": self.is_connected,
            "connection_config": self.connection_config,
            "connected_at": self._connected_at.isoformat() if self._connected_at else None
        }
        
        if self.is_connected:
            try:
                server_info = self.client.server_info()
                status["server_version"] = server_info.get("version", "unknown")
                
                # Get database stats
                db_stats = self.client.admin.command("dbStats")
                status["database_name"] = self.database.name
                status["database_size"] = db_stats.get("dataSize", 0)
                status["collections_count"] = db_stats.get("collections", 0)
                
                # Get collection counts
                status["collections"] = {}
                for collection_name in self.database.list_collection_names():
                    count = self.database[collection_name].count_documents({})
                    status["collections"][collection_name] = count
            
            except Exception as e:
                status["error"] = str(e)
        
        return status
    
    def get_collection_stats(self, collection_name: str) -> Dict[str, Any]:
        """
        Get statistics for MongoDB collection
        Args:
            collection_name: Collection name
        Returns:
            Dict with collection statistics
        """
        if not self.is_connected:
            raise RuntimeError("Database not connected")
        
        try:
            stats = self.database.command("collStats", collection_name)
            return {
                "name": collection_name,
                "document_count": stats.get("count", 0),
                "size_bytes": stats.get("size", 0),
                "storage_size": stats.get("storageSize", 0),
                "index_count": stats.get("nindexes", 0),
                "indexes_size": stats.get("totalIndexSize", 0),
                "avg_object_size": stats.get("avgObjSize", 0)
            }
        except Exception as e:
            raise RuntimeError(f"Failed to get collection stats: {str(e)}")
    
    def query_chunks(self, query: Dict[str, Any], limit: int = 100) -> List[Dict[str, Any]]:
        """
        Query chunks collection with MongoDB query
        Args:
            query: MongoDB query document
            limit: Maximum number of results
        Returns:
            List of matching documents
        """
        if not self.is_connected:
            raise RuntimeError("Database not connected")
        
        collection = self.database.chunks
        cursor = collection.find(query).limit(limit)
        return list(cursor)
    
    def query_runs(self, query: Dict[str, Any], limit: int = 100) -> List[Dict[str, Any]]:
        """
        Query pipeline runs collection
        Args:
            query: MongoDB query document
            limit: Maximum number of results
        Returns:
            List of matching run documents
        """
        if not self.is_connected:
            raise RuntimeError("Database not connected")
        
        collection = self.database.pipeline_runs
        cursor = collection.find(query).limit(limit)
        return list(cursor)
    
    def delete_chunks_by_document(self, document_id: str) -> int:
        """
        Delete all chunks for a specific document
        Args:
            document_id: Document ID to delete chunks for
        Returns:
            int: Number of deleted documents
        """
        if not self.is_connected:
            raise RuntimeError("Database not connected")
        
        collection = self.database.chunks
        result = collection.delete_many({"document_id": document_id})
        return result.deleted_count
    
    def get_chunk_count(self, query: Optional[Dict[str, Any]] = None) -> int:
        """
        Get count of chunks matching query
        Args:
            query: MongoDB query (None for all chunks)
        Returns:
            int: Count of matching chunks
        """
        if not self.is_connected:
            raise RuntimeError("Database not connected")
        
        collection = self.database.chunks
        query = query or {}
        return collection.count_documents(query)
    
    def aggregate_chunks(self, pipeline: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Perform aggregation on chunks collection
        Args:
            pipeline: MongoDB aggregation pipeline
        Returns:
            List of aggregation results
        """
        if not self.is_connected:
            raise RuntimeError("Database not connected")
        
        collection = self.database.chunks
        cursor = collection.aggregate(pipeline)
        return list(cursor)