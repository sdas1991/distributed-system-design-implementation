"""Batch processors for different types of operations"""

import logging
import hashlib
from datetime import datetime, timedelta
from typing import Dict

from db_client import DatabaseClient
from config import settings

logger = logging.getLogger(__name__)


class DelayedUpdateProcessor:
    """
    Process delayed update events

    These are updates that should be processed after a delay (e.g., 5 minutes)
    for batch optimization or eventual consistency
    """

    def __init__(self, db_client: DatabaseClient):
        self.db_client = db_client

    def _get_shard_id(self, customer_id: str) -> int:
        """Calculate shard ID for a customer (consistent hashing)"""
        hash_value = int(hashlib.md5(customer_id.encode()).hexdigest()[:8], 16)
        return hash_value % settings.TOTAL_SHARDS

    async def process(self, event: Dict):
        """Process a delayed update event"""
        customer_id = event.get("customerId")
        entity_id = event.get("entityId")
        update_type = event.get("updateType")

        if not customer_id:
            logger.warning("Delayed update missing customerId")
            return

        shard_id = self._get_shard_id(customer_id)

        logger.info(f"Processing delayed update for customer {customer_id} on shard {shard_id}")

        try:
            # Mark the event as processed
            query = """
                UPDATE events
                SET processed_at = NOW()
                WHERE id = %s AND customer_id = %s
            """

            self.db_client.execute_on_shard(
                shard_id,
                query,
                (entity_id, customer_id)
            )

            logger.debug(f"Marked event {entity_id} as processed")

        except Exception as e:
            logger.error(f"Error processing delayed update: {e}")
            raise


class AggregationProcessor:
    """
    Aggregate events into hourly metrics

    This processor calculates hourly statistics and stores them
    in the metrics_hourly table for fast querying
    """

    def __init__(self, db_client: DatabaseClient):
        self.db_client = db_client

    async def aggregate_hourly(self):
        """Aggregate events from the previous hour"""
        logger.info("Starting hourly aggregation...")

        # Calculate the hour to aggregate (previous completed hour)
        now = datetime.now()
        hour_to_aggregate = (now - timedelta(hours=1)).replace(minute=0, second=0, microsecond=0)

        logger.info(f"Aggregating data for hour: {hour_to_aggregate}")

        aggregation_query = """
            INSERT INTO metrics_hourly (customer_id, event_type, hour_bucket, count, last_updated)
            SELECT
                customer_id,
                event_type,
                DATE_TRUNC('hour', created_at) as hour_bucket,
                COUNT(*) as count,
                NOW() as last_updated
            FROM events
            WHERE created_at >= %s
              AND created_at < %s
              AND processed_at IS NOT NULL
            GROUP BY customer_id, event_type, DATE_TRUNC('hour', created_at)
            ON CONFLICT (customer_id, event_type, hour_bucket)
            DO UPDATE SET
                count = metrics_hourly.count + EXCLUDED.count,
                last_updated = EXCLUDED.last_updated
        """

        hour_start = hour_to_aggregate
        hour_end = hour_to_aggregate + timedelta(hours=1)

        # Execute on all shards
        results = self.db_client.execute_on_all_shards(
            aggregation_query,
            (hour_start, hour_end)
        )

        total_aggregated = sum(len(rows) for rows in results.values())
        logger.info(f"Hourly aggregation complete. Processed {total_aggregated} metric rows")


class CompactionProcessor:
    """
    Compact old data to save space

    This processor:
    - Deletes old events beyond retention period
    - Keeps aggregated metrics
    - Runs maintenance tasks
    """

    def __init__(self, db_client: DatabaseClient):
        self.db_client = db_client

    async def compact_old_data(self):
        """Compact data older than retention period"""
        logger.info("Starting data compaction...")

        retention_date = datetime.now() - timedelta(days=settings.COMPACTION_RETENTION_DAYS)

        logger.info(f"Deleting events older than {retention_date}")

        delete_query = """
            DELETE FROM events
            WHERE created_at < %s
              AND processed_at IS NOT NULL
        """

        # Execute on all shards
        results = self.db_client.execute_on_all_shards(
            delete_query,
            (retention_date,)
        )

        logger.info(f"Compaction complete for events older than {retention_date}")

        # Vacuum tables for space reclamation (PostgreSQL specific)
        await self._vacuum_tables()

    async def _vacuum_tables(self):
        """Run VACUUM on tables to reclaim space"""
        logger.info("Running VACUUM on all shards...")

        vacuum_query = "VACUUM ANALYZE events"

        for shard_id in range(settings.TOTAL_SHARDS):
            try:
                self.db_client.execute_on_shard(shard_id, vacuum_query)
                logger.info(f"VACUUM completed on shard {shard_id}")
            except Exception as e:
                logger.error(f"VACUUM failed on shard {shard_id}: {e}")
