package com.shardstream.ingest

import com.shardstream.events.EventClient
import com.shardstream.observability.ResilienceManager
import com.shardstream.router.ConnectionPoolManager
import io.ktor.http.*
import io.ktor.server.application.*
import io.ktor.server.request.*
import io.ktor.server.response.*
import io.ktor.server.routing.*
import kotlinx.serialization.Serializable

fun Route.ingest(handler: IngestHandler) {
    route("/ingest") {
        post {
            try {
                val request = call.receive<IngestRequest>()

                // Validate request
                if (request.customerId.isBlank()) {
                    call.respond(HttpStatusCode.BadRequest, mapOf("error" to "customerId is required"))
                    return@post
                }
                if (request.eventType.isBlank()) {
                    call.respond(HttpStatusCode.BadRequest, mapOf("error" to "eventType is required"))
                    return@post
                }

                // Process ingest
                val result = handler.handleIngest(request)

                call.respond(HttpStatusCode.Created, result)
            } catch (e: Exception) {
                call.respond(
                    HttpStatusCode.InternalServerError,
                    mapOf("error" to (e.message ?: "Failed to ingest event"))
                )
            }
        }

        post("/batch") {
            try {
                val requests = call.receive<List<IngestRequest>>()

                // Validate batch
                if (requests.isEmpty()) {
                    call.respond(HttpStatusCode.BadRequest, mapOf("error" to "Empty batch"))
                    return@post
                }
                if (requests.size > 1000) {
                    call.respond(HttpStatusCode.BadRequest, mapOf("error" to "Batch too large (max 1000)"))
                    return@post
                }

                // Process batch
                val results = handler.handleBatchIngest(requests)

                call.respond(HttpStatusCode.Created, mapOf(
                    "total" to requests.size,
                    "successful" to results.count { it.success },
                    "failed" to results.count { !it.success },
                    "results" to results
                ))
            } catch (e: Exception) {
                call.respond(
                    HttpStatusCode.InternalServerError,
                    mapOf("error" to (e.message ?: "Failed to process batch"))
                )
            }
        }
    }
}

fun Route.health(
    connectionPool: ConnectionPoolManager,
    eventClient: EventClient,
    resilienceManager: ResilienceManager
) {
    route("/health") {
        get {
            val dbHealth = connectionPool.healthCheck()
            val natsHealth = eventClient.isConnected()
            val resilienceHealth = resilienceManager.getHealthStatus()

            val overall = dbHealth.values.all { it } && natsHealth && resilienceHealth.healthy

            val status = if (overall) HttpStatusCode.OK else HttpStatusCode.ServiceUnavailable

            call.respond(status, mapOf(
                "status" to if (overall) "healthy" else "unhealthy",
                "database" to dbHealth,
                "nats" to natsHealth,
                "resilience" to mapOf(
                    "healthy" to resilienceHealth.healthy,
                    "openCircuits" to resilienceHealth.openCircuits
                ),
                "timestamp" to System.currentTimeMillis()
            ))
        }

        get("/metrics") {
            val poolStats = connectionPool.getPoolStats()
            val cbMetrics = resilienceManager.getHealthStatus().circuitBreakerMetrics
            val retryMetrics = resilienceManager.getHealthStatus().retryMetrics

            call.respond(HttpStatusCode.OK, mapOf(
                "connectionPools" to poolStats,
                "circuitBreakers" to cbMetrics,
                "retries" to retryMetrics
            ))
        }
    }
}

@Serializable
data class IngestRequest(
    val customerId: String,
    val eventType: String,
    val payload: Map<String, String>,
    val metadata: Map<String, String> = emptyMap()
)

@Serializable
data class IngestResponse(
    val eventId: String,
    val shardId: Int,
    val success: Boolean,
    val timestamp: Long
)
