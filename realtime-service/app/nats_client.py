"""NATS JetStream client for Python"""

import asyncio
import logging
from typing import Callable, Optional

import nats
from nats.aio.client import Client as NATSClient
from nats.js import JetStreamContext

logger = logging.getLogger(__name__)


class NATSClient:
    """
    NATS JetStream client wrapper

    Features:
    - Connection management
    - Auto-reconnect
    - Subscribe to streams
    - Publish messages
    """

    def __init__(self, nats_url: str):
        self.nats_url = nats_url
        self.nc: Optional[NATSClient] = None
        self.js: Optional[JetStreamContext] = None
        self._connected = False

    async def connect(self):
        """Connect to NATS server"""
        try:
            self.nc = await nats.connect(
                servers=[self.nats_url],
                reconnect_time_wait=2,
                max_reconnect_attempts=-1,  # Infinite reconnects
                error_cb=self._error_callback,
                disconnected_cb=self._disconnected_callback,
                reconnected_cb=self._reconnected_callback
            )

            # Get JetStream context
            self.js = self.nc.jetstream()

            self._connected = True
            logger.info(f"Connected to NATS at {self.nats_url}")

        except Exception as e:
            logger.error(f"Failed to connect to NATS: {e}")
            raise

    async def disconnect(self):
        """Disconnect from NATS server"""
        if self.nc:
            try:
                await self.nc.drain()
                await self.nc.close()
                self._connected = False
                logger.info("Disconnected from NATS")
            except Exception as e:
                logger.error(f"Error disconnecting from NATS: {e}")

    def is_connected(self) -> bool:
        """Check if connected to NATS"""
        return self._connected and self.nc is not None and self.nc.is_connected

    async def publish(self, subject: str, data: bytes):
        """Publish a message to a subject"""
        if not self.js:
            raise RuntimeError("Not connected to JetStream")

        try:
            ack = await self.js.publish(subject, data)
            logger.debug(f"Published to {subject}: seq={ack.seq}")
            return ack

        except Exception as e:
            logger.error(f"Failed to publish to {subject}: {e}")
            raise

    async def subscribe(
        self,
        subject: str,
        durable_name: str,
        callback: Callable,
        queue_group: Optional[str] = None
    ):
        """
        Subscribe to a subject with a durable consumer

        Args:
            subject: Subject to subscribe to
            durable_name: Durable consumer name
            callback: Async callback function for messages
            queue_group: Optional queue group for load balancing
        """
        if not self.js:
            raise RuntimeError("Not connected to JetStream")

        try:
            # Create or get consumer configuration
            psub = await self.js.pull_subscribe(
                subject=subject,
                durable=durable_name
            )

            logger.info(f"Subscribed to {subject} with durable consumer {durable_name}")

            # Start message processing loop
            asyncio.create_task(self._consume_messages(psub, callback))

        except Exception as e:
            logger.error(f"Failed to subscribe to {subject}: {e}")
            raise

    async def _consume_messages(self, subscription, callback):
        """
        Continuously consume messages from a pull subscription
        """
        while self.is_connected():
            try:
                # Fetch messages in batches
                messages = await subscription.fetch(batch=10, timeout=1)

                for msg in messages:
                    try:
                        # Process message with callback
                        await callback(msg)

                    except Exception as e:
                        logger.error(f"Error in message callback: {e}")
                        # Negative acknowledgment - requeue
                        await msg.nak()

            except TimeoutError:
                # No messages available, continue
                continue

            except Exception as e:
                logger.error(f"Error fetching messages: {e}")
                await asyncio.sleep(1)

    async def _error_callback(self, error):
        """Called when NATS error occurs"""
        logger.error(f"NATS error: {error}")

    async def _disconnected_callback(self):
        """Called when disconnected from NATS"""
        logger.warning("Disconnected from NATS")
        self._connected = False

    async def _reconnected_callback(self):
        """Called when reconnected to NATS"""
        logger.info("Reconnected to NATS")
        self._connected = True
