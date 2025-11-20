"""Database client for batch processing"""

import logging
from typing import Dict, List
import psycopg2
from psycopg2.pool import SimpleConnectionPool
from psycopg2.extras import RealDictCursor

from config import settings

logger = logging.getLogger(__name__)


class DatabaseClient:
    """
    Database client for connecting to all shards

    Features:
    - Connection pooling per shard
    - Execute queries across shards
    - Transaction support
    """

    def __init__(self):
        self.pools: Dict[int, SimpleConnectionPool] = {}

    async def connect_all_shards(self):
        """Connect to all database shards"""
        logger.info(f"Connecting to {settings.TOTAL_SHARDS} shards...")

        for shard_id in range(settings.TOTAL_SHARDS):
            config = settings.get_shard_config(shard_id)

            try:
                pool = SimpleConnectionPool(
                    minconn=2,
                    maxconn=10,
                    host=config["host"],
                    port=config["port"],
                    database=config["database"],
                    user=config["user"],
                    password=config["password"]
                )

                self.pools[shard_id] = pool
                logger.info(f"Connected to shard {shard_id}: {config['host']}:{config['port']}")

            except Exception as e:
                logger.error(f"Failed to connect to shard {shard_id}: {e}")
                raise

        logger.info("All shards connected successfully")

    def get_connection(self, shard_id: int):
        """Get a connection from the pool for a shard"""
        if shard_id not in self.pools:
            raise ValueError(f"Shard {shard_id} not connected")

        return self.pools[shard_id].getconn()

    def return_connection(self, shard_id: int, conn):
        """Return a connection to the pool"""
        if shard_id in self.pools:
            self.pools[shard_id].putconn(conn)

    def execute_on_shard(self, shard_id: int, query: str, params: tuple = None) -> List[dict]:
        """Execute a query on a specific shard"""
        conn = None
        try:
            conn = self.get_connection(shard_id)
            cursor = conn.cursor(cursor_factory=RealDictCursor)

            cursor.execute(query, params)

            # Fetch results if SELECT query
            if query.strip().upper().startswith("SELECT"):
                results = cursor.fetchall()
                return [dict(row) for row in results]
            else:
                conn.commit()
                return []

        except Exception as e:
            if conn:
                conn.rollback()
            logger.error(f"Error executing query on shard {shard_id}: {e}")
            raise

        finally:
            if cursor:
                cursor.close()
            if conn:
                self.return_connection(shard_id, conn)

    def execute_on_all_shards(self, query: str, params: tuple = None) -> Dict[int, List[dict]]:
        """Execute a query on all shards"""
        results = {}

        for shard_id in self.pools.keys():
            try:
                results[shard_id] = self.execute_on_shard(shard_id, query, params)
            except Exception as e:
                logger.error(f"Error executing on shard {shard_id}: {e}")
                results[shard_id] = []

        return results

    async def close_all(self):
        """Close all connection pools"""
        logger.info("Closing all database connections...")

        for shard_id, pool in self.pools.items():
            try:
                pool.closeall()
                logger.info(f"Closed connections for shard {shard_id}")
            except Exception as e:
                logger.error(f"Error closing shard {shard_id}: {e}")

        self.pools.clear()
        logger.info("All database connections closed")
