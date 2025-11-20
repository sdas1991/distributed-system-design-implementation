"""NATS client for DLQ processor"""

import asyncio
import logging
from typing import Callable

import nats
from nats.aio.client import Client as NATS

logger = logging.getLogger(__name__)


class NATSClient:
    """NATS JetStream client"""

    def __init__(self, nats_url: str):
        self.nats_url = nats_url
        self.nc: NATS = None
        self.js = None
        self._connected = False

    async def connect(self):
        """Connect to NATS"""
        try:
            self.nc = await nats.connect(
                servers=[self.nats_url],
                reconnect_time_wait=2,
                max_reconnect_attempts=-1,
            )

            self.js = self.nc.jetstream()
            self._connected = True

            logger.info(f"Connected to NATS at {self.nats_url}")

        except Exception as e:
            logger.error(f"Failed to connect to NATS: {e}")
            raise

    async def disconnect(self):
        """Disconnect from NATS"""
        if self.nc:
            try:
                await self.nc.drain()
                await self.nc.close()
                self._connected = False
                logger.info("Disconnected from NATS")
            except Exception as e:
                logger.error(f"Error disconnecting: {e}")

    def is_connected(self) -> bool:
        """Check connection status"""
        return self._connected and self.nc is not None and self.nc.is_connected

    async def publish(self, subject: str, data: bytes):
        """Publish message"""
        if not self.js:
            raise RuntimeError("Not connected to JetStream")

        try:
            await self.js.publish(subject, data)
            logger.debug(f"Published to {subject}")
        except Exception as e:
            logger.error(f"Failed to publish to {subject}: {e}")
            raise

    async def subscribe(self, subject: str, durable_name: str, callback: Callable):
        """Subscribe with pull consumer"""
        if not self.js:
            raise RuntimeError("Not connected to JetStream")

        try:
            psub = await self.js.pull_subscribe(
                subject=subject,
                durable=durable_name
            )

            logger.info(f"Subscribed to {subject}")

            # Start consuming
            asyncio.create_task(self._consume_messages(psub, callback))

        except Exception as e:
            logger.error(f"Failed to subscribe: {e}")
            raise

    async def _consume_messages(self, subscription, callback):
        """Consume messages"""
        while self.is_connected():
            try:
                messages = await subscription.fetch(batch=10, timeout=1)

                for msg in messages:
                    try:
                        await callback(msg)
                    except Exception as e:
                        logger.error(f"Callback error: {e}")
                        await msg.nak()

            except TimeoutError:
                continue
            except Exception as e:
                logger.error(f"Error fetching messages: {e}")
                await asyncio.sleep(1)
