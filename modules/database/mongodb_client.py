"""
MongoDB Client Module

Centralized MongoDB connection and client management for the Stockelper Airflow project.
Provides consistent connection handling across all DAGs and modules.

Author: Stockelper Team
License: MIT
"""

import os
import logging
from pymongo import MongoClient
from pymongo.errors import PyMongoError
from typing import Optional

# Configure logging
logger = logging.getLogger(__name__)

# MongoDB configuration constants
MONGODB_HOST = os.environ.get("MONGODB_HOST", "localhost")
MONGODB_PORT = int(os.environ.get("MONGODB_PORT", "27017"))
MONGODB_URI = os.environ.get("MONGODB_URI", f"mongodb://{MONGODB_HOST}:{MONGODB_PORT}/")
MONGODB_DATABASE = os.environ.get("MONGODB_DATABASE", "stockelper")

# Connection timeout settings
CONNECTION_TIMEOUT_MS = 5000
SERVER_SELECTION_TIMEOUT_MS = 5000


class MongoDBClient:
    """
    MongoDB client wrapper with connection management and error handling.
    """

    def __init__(self, uri: Optional[str] = None, database: Optional[str] = None):
        """
        Initialize MongoDB client.

        Args:
            uri (str, optional): MongoDB connection URI
            database (str, optional): Database name
        """
        self.uri = uri or MONGODB_URI
        self.database_name = database or MONGODB_DATABASE
        self.client = None
        self.database = None

    def connect(self) -> bool:
        """
        Establish connection to MongoDB.

        Returns:
            bool: True if connection successful, False otherwise
        """
        try:
            self.client = MongoClient(
                self.uri,
                serverSelectionTimeoutMS=SERVER_SELECTION_TIMEOUT_MS,
                connectTimeoutMS=CONNECTION_TIMEOUT_MS
            )

            # Test connection
            self.client.server_info()
            self.database = self.client[self.database_name]

            logger.info(f"Successfully connected to MongoDB: {self.database_name}")
            return True

        except PyMongoError as e:
            logger.error(f"Failed to connect to MongoDB: {e}")
            logger.error("Please check:")
            logger.error("- MongoDB server is running")
            logger.error(f"- URI is correct: {self.uri}")
            return False

    def get_collection(self, collection_name: str):
        """
        Get MongoDB collection.

        Args:
            collection_name (str): Name of the collection

        Returns:
            Collection: MongoDB collection object or None
        """
        if not self.database:
            if not self.connect():
                return None

        return self.database[collection_name]

    def close(self):
        """Close MongoDB connection."""
        if self.client:
            self.client.close()
            logger.info("MongoDB connection closed")

    def __enter__(self):
        """Context manager entry."""
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()


def get_mongodb_client(uri: Optional[str] = None, database: Optional[str] = None) -> MongoDBClient:
    """
    Factory function to create MongoDB client instance.

    Args:
        uri (str, optional): MongoDB connection URI
        database (str, optional): Database name

    Returns:
        MongoDBClient: MongoDB client instance
    """
    return MongoDBClient(uri, database)


def test_connection(uri: Optional[str] = None) -> bool:
    """
    Test MongoDB connection.

    Args:
        uri (str, optional): MongoDB connection URI

    Returns:
        bool: True if connection successful, False otherwise
    """
    client = MongoDBClient(uri)
    return client.connect()