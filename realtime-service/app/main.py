"""
Realtime Update Service - Subscribes to NATS events and pushes to WebSocket clients

Features:
- WebSocket connections for real-time updates
- NATS JetStream consumer
- Customer-specific subscriptions
- Connection management
- Heartbeat/ping-pong
"""

import asyncio
import json
import logging
import os
from contextlib import asynccontextmanager
from typing import Dict, Set

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import JSONResponse

from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.resources import Resource
from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor

from app.nats_client import NATSClient
from app.connection_manager import ConnectionManager
from app.config import settings

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Initialize OpenTelemetry tracing
def setup_tracing():
    """Setup OpenTelemetry distributed tracing"""
    try:
        jaeger_endpoint = os.getenv("JAEGER_ENDPOINT", "http://jaeger:4317")

        resource = Resource.create({"service.name": "realtime-service"})
        provider = TracerProvider(resource=resource)

        otlp_exporter = OTLPSpanExporter(endpoint=jaeger_endpoint, insecure=True)
        provider.add_span_processor(BatchSpanProcessor(otlp_exporter))

        trace.set_tracer_provider(provider)
        logger.info(f"OpenTelemetry tracing enabled, exporting to {jaeger_endpoint}")

        return trace.get_tracer(__name__)
    except Exception as e:
        logger.warning(f"Failed to setup tracing: {e}")
        return None

tracer = setup_tracing()

# Global instances
nats_client: NATSClient = None
connection_manager = ConnectionManager()


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Startup and shutdown lifecycle"""
    global nats_client

    # Startup
    logger.info("Starting Realtime Update Service...")

    # Connect to NATS
    nats_client = NATSClient(settings.NATS_URL)
    await nats_client.connect()

    # Start consuming realtime updates
    asyncio.create_task(consume_realtime_updates())

    logger.info("Realtime Update Service started successfully")

    yield

    # Shutdown
    logger.info("Shutting down Realtime Update Service...")
    await nats_client.disconnect()
    await connection_manager.disconnect_all()


app = FastAPI(
    title="ShardStream Realtime Service",
    version="1.0.0",
    lifespan=lifespan
)

# Instrument FastAPI for automatic tracing
if tracer:
    FastAPIInstrumentor.instrument_app(app)


@app.get("/health")
async def health_check():
    """Health check endpoint"""
    nats_connected = nats_client.is_connected() if nats_client else False
    active_connections = connection_manager.get_active_count()

    return JSONResponse({
        "status": "healthy" if nats_connected else "degraded",
        "nats_connected": nats_connected,
        "active_websocket_connections": active_connections,
        "timestamp": asyncio.get_event_loop().time()
    })


@app.get("/metrics")
async def metrics():
    """Metrics endpoint"""
    stats = connection_manager.get_stats()

    return JSONResponse({
        "active_connections": stats["active_connections"],
        "total_messages_sent": stats["total_messages_sent"],
        "connections_by_customer": stats["connections_by_customer"]
    })


@app.websocket("/ws/{customer_id}")
async def websocket_endpoint(websocket: WebSocket, customer_id: str):
    """
    WebSocket endpoint for real-time updates

    Clients connect with: ws://host:port/ws/{customer_id}
    They receive real-time events for their customer_id
    """
    await websocket.accept()
    logger.info(f"WebSocket connection accepted for customer: {customer_id}")

    # Register connection
    await connection_manager.connect(customer_id, websocket)

    try:
        # Send welcome message
        await websocket.send_json({
            "type": "connected",
            "customer_id": customer_id,
            "message": "Connected to ShardStream realtime updates"
        })

        # Keep connection alive and handle incoming messages
        while True:
            try:
                # Receive message with timeout (for heartbeat)
                data = await asyncio.wait_for(
                    websocket.receive_text(),
                    timeout=30.0
                )

                # Handle ping/pong
                message = json.loads(data)
                if message.get("type") == "ping":
                    await websocket.send_json({"type": "pong"})

            except asyncio.TimeoutError:
                # Send heartbeat
                try:
                    await websocket.send_json({"type": "heartbeat"})
                except Exception:
                    break

    except WebSocketDisconnect:
        logger.info(f"WebSocket disconnected for customer: {customer_id}")

    except Exception as e:
        logger.error(f"WebSocket error for customer {customer_id}: {e}")

    finally:
        await connection_manager.disconnect(customer_id, websocket)


async def consume_realtime_updates():
    """
    Consume realtime update events from NATS and broadcast to WebSocket clients
    """
    logger.info("Starting realtime updates consumer...")

    async def message_handler(msg):
        """Handle incoming NATS messages"""
        try:
            data = json.loads(msg.data.decode())

            event_type = data.get("eventType", "")
            customer_id = data.get("customerId")

            if not customer_id:
                logger.warning(f"Received event without customerId: {data}")
                return

            # Only process realtime events
            if "realtime" not in event_type.lower():
                return

            # Prepare WebSocket message
            ws_message = {
                "type": "event",
                "event_id": data.get("eventId"),
                "event_type": event_type,
                "customer_id": customer_id,
                "entity_id": data.get("entityId"),
                "data": json.loads(data.get("data", "{}")),
                "timestamp": data.get("timestamp")
            }

            # Broadcast to connected clients for this customer
            await connection_manager.send_to_customer(customer_id, ws_message)

            logger.debug(f"Sent realtime update to customer {customer_id}: {event_type}")

            # Acknowledge message
            await msg.ack()

        except json.JSONDecodeError as e:
            logger.error(f"Failed to decode message: {e}")
            await msg.nak()

        except Exception as e:
            logger.error(f"Error processing message: {e}")
            await msg.nak()

    # Subscribe to realtime updates topic
    try:
        await nats_client.subscribe(
            subject="updates.realtime",
            durable_name="realtime-service-consumer",
            callback=message_handler
        )

        logger.info("Subscribed to updates.realtime topic")

    except Exception as e:
        logger.error(f"Failed to subscribe to NATS: {e}")
        raise


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        "app.main:app",
        host="0.0.0.0",
        port=8083,
        log_level="info",
        reload=False
    )
