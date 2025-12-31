"""
Storage interface module handling ArcticDB and Redis connections.
"""
import os
import logging
from typing import Optional, Any
import pandas as pd
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Conditional imports to handle missing dependencies during setup
try:
    import arcticdb
except ImportError:
    arcticdb = None

try:
    import redis
except ImportError:
    redis = None

logger = logging.getLogger(__name__)

class StorageEngine:
    """
    Manages connections to ArcticDB (Historical) and Redis (Live).
    """
    def __init__(self, arctic_uri: str = None, redis_host: str = "localhost", redis_port: int = 6379):
        if arctic_uri is None:
            # Default to src/data/arctic_data relative to this file
            base_dir = os.path.dirname(os.path.abspath(__file__))
            data_dir = os.path.join(base_dir, "arctic_data")
            self.arctic_uri = f"lmdb://{data_dir}"
        else:
            self.arctic_uri = arctic_uri
            
        self.redis_host = redis_host
        self.redis_port = redis_port
        self._arctic: Optional[Any] = None
        self._redis: Optional[Any] = None

    def connect(self):
        """Initializes database connections."""
        if arcticdb:
            try:
                self._arctic = arcticdb.Arctic(self.arctic_uri)
                logger.info(f"Connected to ArcticDB at {self.arctic_uri}")
            except Exception as e:
                logger.error(f"Failed to connect to ArcticDB: {e}")
        else:
            logger.warning("ArcticDB library not found.")

        if redis:
            try:
                self._redis = redis.Redis(host=self.redis_host, port=self.redis_port, decode_responses=True)
                self._redis.ping()
                logger.info(f"Connected to Redis at {self.redis_host}:{self.redis_port}")
            except Exception as e:
                logger.error(f"Failed to connect to Redis: {e}")
                self._redis = None
        else:
            logger.warning("Redis library not found.")

    def get_library(self, library_name: str, create_if_missing: bool = False):
        """Target for ArcticDB library retrieval."""
        if not self._arctic:
            raise ConnectionError("ArcticDB not connected")
        
        if library_name not in self._arctic.list_libraries():
            if create_if_missing:
                self._arctic.create_library(library_name)
                logger.info(f"Created ArcticDB library: {library_name}")
            else:
                raise ValueError(f"Library {library_name} does not exist")
        
        return self._arctic[library_name]

    def set_live_value(self, key: str, value: str):
        """Sets a value in Redis."""
        if not self._redis:
            logger.warning("Redis not connected, skipping set_live_value")
            return
        self._redis.set(key, value)

    def get_live_value(self, key: str) -> Optional[str]:
        """Gets a value from Redis."""
        if not self._redis:
            logger.warning("Redis not connected, returning None")
            return None
        return self._redis.get(key)
