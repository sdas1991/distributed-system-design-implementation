"""Database client for DLQ processor"""

import logging
import psycopg2
from psycopg2.pool import SimpleConnectionPool
from config import settings

logger = logging.getLogger(__name__)


class DatabaseClient:
    """Database client for storing failed messages"""

    def __init__(self):
        self.pool = None

    async def connect(self):
        """Connect to database"""
        try:
            self.pool = SimpleConnectionPool(
                minconn=1,
                maxconn=5,
                host=settings.DB_HOST,
                port=settings.DB_PORT,
                database=settings.DB_NAME,
                user=settings.DB_USER,
                password=settings.DB_PASSWORD
            )

            logger.info(f"Connected to database at {settings.DB_HOST}:{settings.DB_PORT}")

        except Exception as e:
            logger.error(f"Failed to connect to database: {e}")
            raise

    async def create_tables(self):
        """Create failed_messages table if not exists"""
        conn = None
        try:
            conn = self.pool.getconn()
            cursor = conn.cursor()

            cursor.execute("""
                CREATE TABLE IF NOT EXISTS failed_messages (
                    id SERIAL PRIMARY KEY,
                    topic VARCHAR(255) NOT NULL,
                    payload TEXT NOT NULL,
                    error TEXT,
                    failed_at BIGINT,
                    retries INTEGER,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)

            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_failed_messages_topic
                ON failed_messages(topic)
            """)

            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_failed_messages_created_at
                ON failed_messages(created_at DESC)
            """)

            conn.commit()
            cursor.close()

            logger.info("Created/verified failed_messages table")

        except Exception as e:
            if conn:
                conn.rollback()
            logger.error(f"Failed to create tables: {e}")
            raise

        finally:
            if conn:
                self.pool.putconn(conn)

    async def insert_failed_message(self, topic: str, payload: str, error: str, failed_at: int, retries: int):
        """Insert a failed message"""
        conn = None
        try:
            conn = self.pool.getconn()
            cursor = conn.cursor()

            cursor.execute("""
                INSERT INTO failed_messages (topic, payload, error, failed_at, retries)
                VALUES (%s, %s, %s, %s, %s)
            """, (topic, str(payload), error, failed_at, retries))

            conn.commit()
            cursor.close()

        except Exception as e:
            if conn:
                conn.rollback()
            logger.error(f"Failed to insert failed message: {e}")
            raise

        finally:
            if conn:
                self.pool.putconn(conn)

    async def close(self):
        """Close database connections"""
        if self.pool:
            try:
                self.pool.closeall()
                logger.info("Closed database connections")
            except Exception as e:
                logger.error(f"Error closing database: {e}")
