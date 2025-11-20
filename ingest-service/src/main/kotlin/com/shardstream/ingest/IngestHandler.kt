package com.shardstream.ingest

import com.shardstream.events.EventClient
import com.shardstream.events.IngestEvent
import com.shardstream.events.UpdateEvent
import com.shardstream.observability.ResilienceManager
import com.shardstream.router.ConnectionPoolManager
import com.shardstream.router.ShardRouter
import io.github.oshai.kotlinlogging.KotlinLogging
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.withContext
import kotlinx.serialization.encodeToString
import kotlinx.serialization.json.Json
import java.sql.Connection
import java.sql.Timestamp
import java.util.*

private val logger = KotlinLogging.logger {}

/**
 * Ingest Handler - Core business logic for ingesting events
 *
 * Features:
 * - Write to sharded database
 * - Publish to event bus
 * - Batch processing
 * - Resilience patterns
 */
class IngestHandler(
    private val shardRouter: ShardRouter,
    private val connectionPool: ConnectionPoolManager,
    private val eventClient: EventClient,
    private val resilienceManager: ResilienceManager
) {
    private val json = Json { prettyPrint = false }

    /**
     * Handle single event ingestion
     */
    suspend fun handleIngest(request: IngestRequest): IngestResponse = withContext(Dispatchers.IO) {
        val eventId = UUID.randomUUID().toString()
        val timestamp = System.currentTimeMillis()
        val shardId = shardRouter.getShardId(request.customerId)

        logger.info { "Ingesting event $eventId for customer ${request.customerId} to shard $shardId" }

        try {
            // Execute with resilience patterns
            resilienceManager.executeResilient("ingest-shard-$shardId") {
                // 1. Write to database
                writeToDatabase(shardId, eventId, request, timestamp)

                // 2. Publish to event bus (async, fire-and-forget for real-time)
                publishToEventBus(eventId, request, timestamp)
            }

            IngestResponse(
                eventId = eventId,
                shardId = shardId,
                success = true,
                timestamp = timestamp
            )
        } catch (e: Exception) {
            logger.error(e) { "Failed to ingest event $eventId" }
            IngestResponse(
                eventId = eventId,
                shardId = shardId,
                success = false,
                timestamp = timestamp
            )
        }
    }

    /**
     * Handle batch ingestion
     */
    suspend fun handleBatchIngest(requests: List<IngestRequest>): List<IngestResponse> = withContext(Dispatchers.IO) {
        logger.info { "Processing batch of ${requests.size} events" }

        // Process in parallel
        requests.map { request ->
            async {
                try {
                    handleIngest(request)
                } catch (e: Exception) {
                    logger.error(e) { "Failed to process event in batch" }
                    IngestResponse(
                        eventId = UUID.randomUUID().toString(),
                        shardId = -1,
                        success = false,
                        timestamp = System.currentTimeMillis()
                    )
                }
            }
        }.awaitAll()
    }

    /**
     * Write event to database shard
     */
    private fun writeToDatabase(
        shardId: Int,
        eventId: String,
        request: IngestRequest,
        timestamp: Long
    ) {
        connectionPool.getWriteDataSource(shardId).connection.use { conn ->
            conn.autoCommit = false

            try {
                // Insert event
                val insertSql = """
                    INSERT INTO events (id, customer_id, event_type, payload, metadata, created_at, shard_key)
                    VALUES (?, ?, ?, ?::jsonb, ?::jsonb, ?, ?)
                """.trimIndent()

                conn.prepareStatement(insertSql).use { stmt ->
                    stmt.setObject(1, UUID.fromString(eventId))
                    stmt.setString(2, request.customerId)
                    stmt.setString(3, request.eventType)
                    stmt.setString(4, json.encodeToString(request.payload))
                    stmt.setString(5, json.encodeToString(request.metadata))
                    stmt.setTimestamp(6, Timestamp(timestamp))
                    stmt.setString(7, request.customerId)

                    stmt.executeUpdate()
                }

                // Update shard metadata
                updateShardMetadata(conn, shardId, timestamp)

                conn.commit()

                logger.debug { "Wrote event $eventId to shard $shardId" }
            } catch (e: Exception) {
                conn.rollback()
                logger.error(e) { "Database write failed for event $eventId" }
                throw e
            }
        }
    }

    /**
     * Update shard metadata statistics
     */
    private fun updateShardMetadata(conn: Connection, shardId: Int, timestamp: Long) {
        val updateSql = """
            INSERT INTO shard_metadata (shard_id, total_events, last_event_at, updated_at)
            VALUES (?, 1, ?, ?)
            ON CONFLICT (shard_id) DO UPDATE
            SET total_events = shard_metadata.total_events + 1,
                last_event_at = EXCLUDED.last_event_at,
                updated_at = EXCLUDED.updated_at
        """.trimIndent()

        conn.prepareStatement(updateSql).use { stmt ->
            stmt.setInt(1, shardId)
            stmt.setTimestamp(2, Timestamp(timestamp))
            stmt.setTimestamp(3, Timestamp(System.currentTimeMillis()))
            stmt.executeUpdate()
        }
    }

    /**
     * Publish events to NATS event bus
     */
    private suspend fun publishToEventBus(
        eventId: String,
        request: IngestRequest,
        timestamp: Long
    ) {
        // Publish raw ingest event
        val ingestEvent = IngestEvent(
            eventId = eventId,
            eventType = "ingest.${request.eventType}",
            timestamp = timestamp,
            customerId = request.customerId,
            payload = json.encodeToString(request.payload),
            metadata = request.metadata
        )

        eventClient.publish(EventClient.TOPIC_INGEST_RAW, ingestEvent)

        // Publish real-time update event
        val realtimeEvent = UpdateEvent(
            eventId = UUID.randomUUID().toString(),
            eventType = "update.realtime.${request.eventType}",
            timestamp = timestamp,
            customerId = request.customerId,
            entityId = eventId,
            updateType = request.eventType,
            data = json.encodeToString(request.payload),
            isRealtime = true
        )

        eventClient.publish(EventClient.TOPIC_UPDATES_REALTIME, realtimeEvent)

        // Publish delayed update event (for batch processing)
        val delayedEvent = UpdateEvent(
            eventId = UUID.randomUUID().toString(),
            eventType = "update.delayed.${request.eventType}",
            timestamp = timestamp,
            customerId = request.customerId,
            entityId = eventId,
            updateType = request.eventType,
            data = json.encodeToString(request.payload),
            isRealtime = false
        )

        eventClient.publish(EventClient.TOPIC_UPDATES_DELAYED, delayedEvent)

        logger.debug { "Published events for $eventId to event bus" }
    }
}
