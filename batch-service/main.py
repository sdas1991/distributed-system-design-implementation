"""
Batch Processing Service - Handles delayed updates and aggregation jobs

Features:
- Consume delayed update events
- Aggregate metrics hourly
- Compaction jobs
- Scheduled batch processing
"""

import asyncio
import json
import logging
import signal
import sys
from datetime import datetime, timedelta

from config import settings
from nats_client import NATSClient
from db_client import DatabaseClient
from processors import DelayedUpdateProcessor, AggregationProcessor, CompactionProcessor

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class BatchService:
    """Main batch processing service"""

    def __init__(self):
        self.nats_client = NATSClient(settings.NATS_URL)
        self.db_client = DatabaseClient()
        self.running = False

        # Processors
        self.delayed_processor = DelayedUpdateProcessor(self.db_client)
        self.aggregation_processor = AggregationProcessor(self.db_client)
        self.compaction_processor = CompactionProcessor(self.db_client)

    async def start(self):
        """Start the batch service"""
        logger.info("Starting Batch Processing Service...")

        self.running = True

        try:
            # Connect to NATS
            await self.nats_client.connect()

            # Connect to databases
            await self.db_client.connect_all_shards()

            # Start consumers
            await self.start_consumers()

            # Start scheduled jobs
            asyncio.create_task(self.run_scheduled_jobs())

            logger.info("Batch Processing Service started successfully")

            # Keep running
            while self.running:
                await asyncio.sleep(1)

        except Exception as e:
            logger.error(f"Error in batch service: {e}")
            raise

        finally:
            await self.shutdown()

    async def start_consumers(self):
        """Start NATS consumers for delayed updates"""
        logger.info("Starting NATS consumers...")

        async def delayed_update_handler(msg):
            """Handle delayed update events"""
            try:
                data = json.loads(msg.data.decode())

                # Wait for configured delay (simulating delayed processing)
                # In production, you'd use NATS delayed delivery or scheduled jobs
                await asyncio.sleep(settings.DELAYED_UPDATE_DELAY_SECONDS)

                # Process the update
                await self.delayed_processor.process(data)

                # Acknowledge
                await msg.ack()

                logger.debug(f"Processed delayed update: {data.get('eventId')}")

            except Exception as e:
                logger.error(f"Error processing delayed update: {e}")
                await msg.nak()

        # Subscribe to delayed updates
        await self.nats_client.subscribe(
            subject="updates.delayed",
            durable_name="batch-service-delayed-consumer",
            callback=delayed_update_handler
        )

        logger.info("NATS consumers started")

    async def run_scheduled_jobs(self):
        """Run scheduled batch jobs"""
        logger.info("Starting scheduled jobs...")

        while self.running:
            try:
                current_time = datetime.now()

                # Run hourly aggregation at the start of each hour
                if current_time.minute == 0:
                    logger.info("Running hourly aggregation job...")
                    await self.aggregation_processor.aggregate_hourly()

                # Run daily compaction at midnight
                if current_time.hour == 0 and current_time.minute == 0:
                    logger.info("Running daily compaction job...")
                    await self.compaction_processor.compact_old_data()

                # Sleep for 60 seconds before next check
                await asyncio.sleep(60)

            except Exception as e:
                logger.error(f"Error in scheduled jobs: {e}")
                await asyncio.sleep(60)

    async def shutdown(self):
        """Graceful shutdown"""
        logger.info("Shutting down Batch Processing Service...")

        self.running = False

        # Disconnect from NATS
        if self.nats_client:
            await self.nats_client.disconnect()

        # Close database connections
        if self.db_client:
            await self.db_client.close_all()

        logger.info("Batch Processing Service shutdown complete")


async def main():
    """Main entry point"""
    service = BatchService()

    # Handle signals for graceful shutdown
    def signal_handler(sig, frame):
        logger.info(f"Received signal {sig}, shutting down...")
        service.running = False

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    # Start service
    await service.start()


if __name__ == "__main__":
    asyncio.run(main())
