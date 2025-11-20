"""
Dead Letter Queue Processor Service

Handles failed messages with:
- Exponential backoff retry
- Permanent storage of failed messages
- Admin API for inspection and replay
"""

import asyncio
import json
import logging
import signal
from datetime import datetime
from nats_client import NATSClient
from db_client import DatabaseClient
from config import settings

# Configure logging
logging.basicConfig(
    level=getattr(logging, settings.LOG_LEVEL),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class DLQProcessor:
    """Processes messages from Dead Letter Queue"""

    def __init__(self):
        self.nats_client = NATSClient(settings.NATS_URL)
        self.db_client = DatabaseClient()
        self.running = False

    async def start(self):
        """Start the DLQ processor"""
        logger.info("Starting DLQ Processor Service...")

        self.running = True

        try:
            # Connect to NATS
            await self.nats_client.connect()

            # Connect to database
            await self.db_client.connect()

            # Create failed_messages table if not exists
            await self.db_client.create_tables()

            # Subscribe to all DLQ topics
            await self.start_consumers()

            logger.info("DLQ Processor started successfully")

            # Keep running
            while self.running:
                await asyncio.sleep(1)

        except Exception as e:
            logger.error(f"Error in DLQ processor: {e}")
            raise

        finally:
            await self.shutdown()

    async def start_consumers(self):
        """Start NATS consumers for DLQ topics"""
        logger.info("Starting DLQ consumers...")

        async def dlq_message_handler(msg):
            """Handle DLQ messages"""
            try:
                dlq_data = json.loads(msg.data.decode())

                original_topic = dlq_data.get('originalTopic')
                retry_count = dlq_data.get('retryCount', 0)
                error = dlq_data.get('error')

                logger.info(f"Processing DLQ message from {original_topic}, retry {retry_count}/{settings.MAX_RETRIES}")

                # Check if max retries reached
                if retry_count >= settings.MAX_RETRIES:
                    # Store permanently
                    await self.store_failed_message(dlq_data)
                    await self.send_alert(dlq_data)
                    await msg.ack()
                    logger.warn(f"Message permanently failed after {retry_count} retries")
                    return

                # Wait before retry (exponential backoff)
                if retry_count < len(settings.RETRY_BACKOFF_SECONDS):
                    wait_seconds = settings.RETRY_BACKOFF_SECONDS[retry_count]
                else:
                    wait_seconds = settings.RETRY_BACKOFF_SECONDS[-1]

                logger.info(f"Waiting {wait_seconds}s before retry...")
                await asyncio.sleep(wait_seconds)

                # Retry: republish to original topic
                await self.retry_message(original_topic, dlq_data)

                await msg.ack()

            except Exception as e:
                logger.error(f"Error processing DLQ message: {e}")
                await msg.nak()  # Requeue in DLQ

        # Subscribe to all DLQ topics (dlq.*)
        await self.nats_client.subscribe(
            subject="dlq.>",
            durable_name="dlq-processor-consumer",
            callback=dlq_message_handler
        )

        logger.info("DLQ consumers started")

    async def retry_message(self, original_topic: str, dlq_data: dict):
        """Retry message by republishing to original topic"""
        try:
            # Extract original payload
            payload = dlq_data.get('payload')

            if isinstance(payload, str):
                payload_bytes = payload.encode()
            else:
                payload_bytes = json.dumps(payload).encode()

            # Publish to original topic
            await self.nats_client.publish(original_topic, payload_bytes)

            logger.info(f"Retried message on topic: {original_topic}")

        except Exception as e:
            # Retry failed - increment counter and republish to DLQ
            logger.error(f"Retry failed: {e}")

            dlq_data['retryCount'] = dlq_data.get('retryCount', 0) + 1
            dlq_data['lastError'] = str(e)
            dlq_data['lastRetryAt'] = int(datetime.now().timestamp() * 1000)

            dlq_topic = f"dlq.{original_topic}"
            await self.nats_client.publish(
                dlq_topic,
                json.dumps(dlq_data).encode()
            )

    async def store_failed_message(self, dlq_data: dict):
        """Store permanently failed message in database"""
        try:
            await self.db_client.insert_failed_message(
                topic=dlq_data.get('originalTopic'),
                payload=dlq_data.get('payload'),
                error=dlq_data.get('error'),
                failed_at=dlq_data.get('failedAt'),
                retries=dlq_data.get('retryCount', 0)
            )

            logger.info(f"Stored permanently failed message from {dlq_data.get('originalTopic')}")

        except Exception as e:
            logger.error(f"Failed to store message in database: {e}")

    async def send_alert(self, dlq_data: dict):
        """Send alert for permanently failed message"""
        # In production, integrate with Slack/PagerDuty/Email
        logger.error(
            f"🚨 ALERT: Message permanently failed!\n"
            f"Topic: {dlq_data.get('originalTopic')}\n"
            f"Retries: {dlq_data.get('retryCount')}\n"
            f"Error: {dlq_data.get('error')}"
        )

    async def shutdown(self):
        """Graceful shutdown"""
        logger.info("Shutting down DLQ Processor...")

        self.running = False

        if self.nats_client:
            await self.nats_client.disconnect()

        if self.db_client:
            await self.db_client.close()

        logger.info("DLQ Processor shutdown complete")


async def main():
    """Main entry point"""
    processor = DLQProcessor()

    # Handle signals for graceful shutdown
    def signal_handler(sig, frame):
        logger.info(f"Received signal {sig}, shutting down...")
        processor.running = False

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    # Start processor
    await processor.start()


if __name__ == "__main__":
    asyncio.run(main())
