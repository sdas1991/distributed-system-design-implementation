"""WebSocket connection manager"""

import asyncio
import logging
from collections import defaultdict
from typing import Dict, Set
from fastapi import WebSocket

logger = logging.getLogger(__name__)


class ConnectionManager:
    """
    Manages WebSocket connections for customers

    Features:
    - Multiple connections per customer
    - Broadcast to customer
    - Connection tracking
    - Metrics
    """

    def __init__(self):
        # customer_id -> set of WebSocket connections
        self.active_connections: Dict[str, Set[WebSocket]] = defaultdict(set)
        self.total_messages_sent = 0
        self._lock = asyncio.Lock()

    async def connect(self, customer_id: str, websocket: WebSocket):
        """Register a new WebSocket connection"""
        async with self._lock:
            self.active_connections[customer_id].add(websocket)
            logger.info(
                f"Customer {customer_id} connected. "
                f"Total connections for customer: {len(self.active_connections[customer_id])}"
            )

    async def disconnect(self, customer_id: str, websocket: WebSocket):
        """Unregister a WebSocket connection"""
        async with self._lock:
            if customer_id in self.active_connections:
                self.active_connections[customer_id].discard(websocket)

                # Clean up empty sets
                if not self.active_connections[customer_id]:
                    del self.active_connections[customer_id]

                logger.info(
                    f"Customer {customer_id} disconnected. "
                    f"Remaining connections: {len(self.active_connections.get(customer_id, []))}"
                )

    async def send_to_customer(self, customer_id: str, message: dict):
        """
        Send a message to all connections for a specific customer
        """
        if customer_id not in self.active_connections:
            logger.debug(f"No active connections for customer {customer_id}")
            return

        connections = list(self.active_connections[customer_id])
        disconnected = []

        for websocket in connections:
            try:
                await websocket.send_json(message)
                self.total_messages_sent += 1

            except Exception as e:
                logger.error(f"Failed to send message to customer {customer_id}: {e}")
                disconnected.append(websocket)

        # Clean up disconnected websockets
        if disconnected:
            async with self._lock:
                for ws in disconnected:
                    self.active_connections[customer_id].discard(ws)

    async def broadcast(self, message: dict):
        """Broadcast a message to all connected clients"""
        for customer_id in list(self.active_connections.keys()):
            await self.send_to_customer(customer_id, message)

    def get_active_count(self) -> int:
        """Get total number of active connections"""
        return sum(len(connections) for connections in self.active_connections.values())

    def get_stats(self) -> dict:
        """Get connection statistics"""
        return {
            "active_connections": self.get_active_count(),
            "total_messages_sent": self.total_messages_sent,
            "connections_by_customer": {
                customer_id: len(connections)
                for customer_id, connections in self.active_connections.items()
            }
        }

    async def disconnect_all(self):
        """Disconnect all WebSocket connections"""
        logger.info("Disconnecting all WebSocket connections...")

        for customer_id, connections in list(self.active_connections.items()):
            for websocket in list(connections):
                try:
                    await websocket.close()
                except Exception as e:
                    logger.error(f"Error closing websocket: {e}")

        self.active_connections.clear()
        logger.info("All WebSocket connections closed")
