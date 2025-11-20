package com.shardstream.query

import com.shardstream.observability.ResilienceManager
import com.shardstream.router.ConnectionPoolManager
import com.shardstream.router.ShardRouter
import io.github.oshai.kotlinlogging.KotlinLogging
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.withContext
import java.sql.ResultSet
import java.sql.Timestamp
import java.util.*

private val logger = KotlinLogging.logger {}

/**
 * Query Handler - Handles read operations from sharded database
 *
 * Features:
 * - Read from read replicas
 * - Scatter-gather queries across shards
 * - Aggregation
 * - Pagination
 */
class QueryHandler(
    private val shardRouter: ShardRouter,
    private val connectionPool: ConnectionPoolManager,
    private val resilienceManager: ResilienceManager
) {
    /**
     * Get events for a specific customer (single shard query)
     */
    suspend fun getEventsByCustomer(
        customerId: String,
        limit: Int,
        offset: Int,
        eventType: String? = null
    ): List<EventResponse> = withContext(Dispatchers.IO) {
        val shardId = shardRouter.getShardId(customerId)

        logger.info { "Querying events for customer $customerId from shard $shardId" }

        resilienceManager.executeResilient("query-shard-$shardId") {
            connectionPool.getReadDataSource(shardId).connection.use { conn ->
                val sql = buildString {
                    append("SELECT id, customer_id, event_type, payload, metadata, created_at ")
                    append("FROM events WHERE customer_id = ? ")
                    if (eventType != null) {
                        append("AND event_type = ? ")
                    }
                    append("ORDER BY created_at DESC ")
                    append("LIMIT ? OFFSET ?")
                }

                conn.prepareStatement(sql).use { stmt ->
                    var paramIndex = 1
                    stmt.setString(paramIndex++, customerId)
                    if (eventType != null) {
                        stmt.setString(paramIndex++, eventType)
                    }
                    stmt.setInt(paramIndex++, limit)
                    stmt.setInt(paramIndex, offset)

                    stmt.executeQuery().use { rs ->
                        val events = mutableListOf<EventResponse>()
                        while (rs.next()) {
                            events.add(rs.toEventResponse(shardId))
                        }
                        events
                    }
                }
            }
        }
    }

    /**
     * Get events by time range (scatter-gather across all shards)
     */
    suspend fun getEventsByTimeRange(
        startTime: Long,
        endTime: Long,
        limit: Int,
        eventType: String? = null
    ): List<EventResponse> = withContext(Dispatchers.IO) {
        logger.info { "Scatter-gather query for time range $startTime to $endTime" }

        // Query all shards in parallel
        val results = shardRouter.getAllShardIds().map { shardId ->
            async {
                try {
                    queryShardByTimeRange(shardId, startTime, endTime, limit, eventType)
                } catch (e: Exception) {
                    logger.error(e) { "Failed to query shard $shardId" }
                    emptyList()
                }
            }
        }.awaitAll()

        // Merge and sort results from all shards
        results.flatten()
            .sortedByDescending { it.createdAt }
            .take(limit)
    }

    /**
     * Query a single shard by time range
     */
    private suspend fun queryShardByTimeRange(
        shardId: Int,
        startTime: Long,
        endTime: Long,
        limit: Int,
        eventType: String?
    ): List<EventResponse> = withContext(Dispatchers.IO) {
        resilienceManager.executeResilient("query-shard-$shardId") {
            connectionPool.getReadDataSource(shardId).connection.use { conn ->
                val sql = buildString {
                    append("SELECT id, customer_id, event_type, payload, metadata, created_at ")
                    append("FROM events WHERE created_at >= ? AND created_at <= ? ")
                    if (eventType != null) {
                        append("AND event_type = ? ")
                    }
                    append("ORDER BY created_at DESC ")
                    append("LIMIT ?")
                }

                conn.prepareStatement(sql).use { stmt ->
                    var paramIndex = 1
                    stmt.setTimestamp(paramIndex++, Timestamp(startTime))
                    stmt.setTimestamp(paramIndex++, Timestamp(endTime))
                    if (eventType != null) {
                        stmt.setString(paramIndex++, eventType)
                    }
                    stmt.setInt(paramIndex, limit)

                    stmt.executeQuery().use { rs ->
                        val events = mutableListOf<EventResponse>()
                        while (rs.next()) {
                            events.add(rs.toEventResponse(shardId))
                        }
                        events
                    }
                }
            }
        }
    }

    /**
     * Get aggregated metrics for a customer
     */
    suspend fun getMetrics(customerId: String): MetricsResponse = withContext(Dispatchers.IO) {
        val shardId = shardRouter.getShardId(customerId)

        logger.info { "Querying metrics for customer $customerId from shard $shardId" }

        resilienceManager.executeResilient("query-shard-$shardId") {
            connectionPool.getReadDataSource(shardId).connection.use { conn ->
                // Get total events
                val totalEvents = conn.prepareStatement(
                    "SELECT COUNT(*) FROM events WHERE customer_id = ?"
                ).use { stmt ->
                    stmt.setString(1, customerId)
                    stmt.executeQuery().use { rs ->
                        if (rs.next()) rs.getLong(1) else 0L
                    }
                }

                // Get events by type
                val eventsByType = conn.prepareStatement(
                    "SELECT event_type, COUNT(*) FROM events WHERE customer_id = ? GROUP BY event_type"
                ).use { stmt ->
                    stmt.setString(1, customerId)
                    stmt.executeQuery().use { rs ->
                        val map = mutableMapOf<String, Long>()
                        while (rs.next()) {
                            map[rs.getString(1)] = rs.getLong(2)
                        }
                        map
                    }
                }

                // Get hourly breakdown
                val hourlyBreakdown = conn.prepareStatement("""
                    SELECT
                        event_type,
                        hour_bucket,
                        count
                    FROM metrics_hourly
                    WHERE customer_id = ?
                    ORDER BY hour_bucket DESC
                    LIMIT 24
                """.trimIndent()).use { stmt ->
                    stmt.setString(1, customerId)
                    stmt.executeQuery().use { rs ->
                        val list = mutableListOf<HourlyMetric>()
                        while (rs.next()) {
                            list.add(HourlyMetric(
                                eventType = rs.getString(1),
                                hour = rs.getTimestamp(2).toString(),
                                count = rs.getLong(3)
                            ))
                        }
                        list
                    }
                }

                MetricsResponse(
                    customerId = customerId,
                    totalEvents = totalEvents,
                    eventsByType = eventsByType,
                    hourlyBreakdown = hourlyBreakdown
                )
            }
        }
    }

    /**
     * Get a specific event by ID
     */
    suspend fun getEventById(eventId: String, customerId: String): EventResponse? = withContext(Dispatchers.IO) {
        val shardId = shardRouter.getShardId(customerId)

        logger.info { "Querying event $eventId from shard $shardId" }

        resilienceManager.executeResilient("query-shard-$shardId") {
            connectionPool.getReadDataSource(shardId).connection.use { conn ->
                val sql = """
                    SELECT id, customer_id, event_type, payload, metadata, created_at
                    FROM events
                    WHERE id = ? AND customer_id = ?
                """.trimIndent()

                conn.prepareStatement(sql).use { stmt ->
                    stmt.setObject(1, UUID.fromString(eventId))
                    stmt.setString(2, customerId)

                    stmt.executeQuery().use { rs ->
                        if (rs.next()) {
                            rs.toEventResponse(shardId)
                        } else {
                            null
                        }
                    }
                }
            }
        }
    }

    /**
     * Extension function to convert ResultSet to EventResponse
     */
    private fun ResultSet.toEventResponse(shardId: Int): EventResponse {
        return EventResponse(
            id = getObject(1, UUID::class.java).toString(),
            customerId = getString(2),
            eventType = getString(3),
            payload = getString(4),
            metadata = getString(5),
            createdAt = getTimestamp(6).time,
            shardId = shardId
        )
    }
}
