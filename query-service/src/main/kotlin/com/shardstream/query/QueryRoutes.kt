package com.shardstream.query

import com.shardstream.observability.ResilienceManager
import com.shardstream.router.ConnectionPoolManager
import io.ktor.http.*
import io.ktor.server.application.*
import io.ktor.server.response.*
import io.ktor.server.routing.*
import kotlinx.serialization.Serializable

fun Route.query(handler: QueryHandler) {
    route("/query") {
        // Get events for a specific customer
        get("/events/{customerId}") {
            try {
                val customerId = call.parameters["customerId"]
                    ?: return@get call.respond(HttpStatusCode.BadRequest, mapOf("error" to "customerId required"))

                val limit = call.parameters["limit"]?.toIntOrNull() ?: 100
                val offset = call.parameters["offset"]?.toIntOrNull() ?: 0
                val eventType = call.parameters["eventType"]

                val events = handler.getEventsByCustomer(customerId, limit, offset, eventType)

                call.respond(HttpStatusCode.OK, mapOf(
                    "customerId" to customerId,
                    "count" to events.size,
                    "limit" to limit,
                    "offset" to offset,
                    "events" to events
                ))
            } catch (e: Exception) {
                call.respond(
                    HttpStatusCode.InternalServerError,
                    mapOf("error" to (e.message ?: "Query failed"))
                )
            }
        }

        // Get events by time range (scatter-gather across all shards)
        get("/events/range") {
            try {
                val startTime = call.parameters["startTime"]?.toLongOrNull()
                    ?: return@get call.respond(HttpStatusCode.BadRequest, mapOf("error" to "startTime required"))

                val endTime = call.parameters["endTime"]?.toLongOrNull()
                    ?: return@get call.respond(HttpStatusCode.BadRequest, mapOf("error" to "endTime required"))

                val limit = call.parameters["limit"]?.toIntOrNull() ?: 100
                val eventType = call.parameters["eventType"]

                val events = handler.getEventsByTimeRange(startTime, endTime, limit, eventType)

                call.respond(HttpStatusCode.OK, mapOf(
                    "startTime" to startTime,
                    "endTime" to endTime,
                    "count" to events.size,
                    "limit" to limit,
                    "events" to events
                ))
            } catch (e: Exception) {
                call.respond(
                    HttpStatusCode.InternalServerError,
                    mapOf("error" to (e.message ?: "Query failed"))
                )
            }
        }

        // Get aggregated metrics
        get("/metrics/{customerId}") {
            try {
                val customerId = call.parameters["customerId"]
                    ?: return@get call.respond(HttpStatusCode.BadRequest, mapOf("error" to "customerId required"))

                val metrics = handler.getMetrics(customerId)

                call.respond(HttpStatusCode.OK, metrics)
            } catch (e: Exception) {
                call.respond(
                    HttpStatusCode.InternalServerError,
                    mapOf("error" to (e.message ?: "Metrics query failed"))
                )
            }
        }

        // Get specific event by ID
        get("/event/{eventId}") {
            try {
                val eventId = call.parameters["eventId"]
                    ?: return@get call.respond(HttpStatusCode.BadRequest, mapOf("error" to "eventId required"))

                val customerId = call.parameters["customerId"]
                    ?: return@get call.respond(HttpStatusCode.BadRequest, mapOf("error" to "customerId required for sharding"))

                val event = handler.getEventById(eventId, customerId)

                if (event != null) {
                    call.respond(HttpStatusCode.OK, event)
                } else {
                    call.respond(HttpStatusCode.NotFound, mapOf("error" to "Event not found"))
                }
            } catch (e: Exception) {
                call.respond(
                    HttpStatusCode.InternalServerError,
                    mapOf("error" to (e.message ?: "Query failed"))
                )
            }
        }
    }
}

fun Route.health(
    connectionPool: ConnectionPoolManager,
    resilienceManager: ResilienceManager,
    cacheManager: CacheManager?
) {
    route("/health") {
        get {
            val dbHealth = connectionPool.healthCheck()
            val resilienceHealth = resilienceManager.getHealthStatus()

            val overall = dbHealth.values.all { it } && resilienceHealth.healthy

            val status = if (overall) HttpStatusCode.OK else HttpStatusCode.ServiceUnavailable

            call.respond(status, mapOf(
                "status" to if (overall) "healthy" else "unhealthy",
                "database" to dbHealth,
                "resilience" to mapOf(
                    "healthy" to resilienceHealth.healthy,
                    "openCircuits" to resilienceHealth.openCircuits
                ),
                "cache" to mapOf(
                    "enabled" to (cacheManager != null),
                    "stats" to (cacheManager?.getStats() ?: mapOf("message" to "cache disabled"))
                ),
                "timestamp" to System.currentTimeMillis()
            ))
        }

        get("/metrics") {
            val poolStats = connectionPool.getPoolStats()
            val cbMetrics = resilienceManager.getHealthStatus().circuitBreakerMetrics
            val retryMetrics = resilienceManager.getHealthStatus().retryMetrics
            val cacheStats = cacheManager?.getStats()

            call.respond(HttpStatusCode.OK, mapOf(
                "connectionPools" to poolStats,
                "circuitBreakers" to cbMetrics,
                "retries" to retryMetrics,
                "cache" to (cacheStats ?: mapOf("enabled" to false))
            ))
        }
    }
}

@Serializable
data class EventResponse(
    val id: String,
    val customerId: String,
    val eventType: String,
    val payload: String,
    val metadata: String?,
    val createdAt: Long,
    val shardId: Int
)

@Serializable
data class MetricsResponse(
    val customerId: String,
    val totalEvents: Long,
    val eventsByType: Map<String, Long>,
    val hourlyBreakdown: List<HourlyMetric>
)

@Serializable
data class HourlyMetric(
    val hour: String,
    val count: Long,
    val eventType: String
)
