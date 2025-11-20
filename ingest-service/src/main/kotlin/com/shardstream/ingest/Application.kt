package com.shardstream.ingest

import com.shardstream.events.EventClient
import com.shardstream.observability.CircuitBreakerManager
import com.shardstream.observability.ResilienceManager
import com.shardstream.observability.RetryStrategyManager
import com.shardstream.router.ConnectionPoolManager
import com.shardstream.router.ShardRouter
import io.github.oshai.kotlinlogging.KotlinLogging
import io.ktor.serialization.kotlinx.json.*
import io.ktor.server.application.*
import io.ktor.server.engine.*
import io.ktor.server.netty.*
import io.ktor.server.plugins.contentnegotiation.*
import io.ktor.server.plugins.calllogging.*
import io.ktor.server.plugins.statuspages.*
import io.ktor.server.response.*
import io.ktor.server.routing.*
import io.ktor.http.*
import kotlinx.coroutines.runBlocking

private val logger = KotlinLogging.logger {}

fun main() {
    logger.info { "Starting Ingest Service..." }

    // Initialize infrastructure
    val shardRouter = ShardRouter(totalShards = 4)
    val connectionPool = ConnectionPoolManager(shardRouter)
    val eventClient = EventClient(clientName = "ingest-service")
    val resilienceManager = ResilienceManager()

    // Connect to event bus
    runBlocking {
        eventClient.connect()
    }

    // Start server
    embeddedServer(Netty, port = 8081, host = "0.0.0.0") {
        configureRouting(shardRouter, connectionPool, eventClient, resilienceManager)
        configureSerialization()
        configureMonitoring()
        configureStatusPages()
    }.start(wait = true)

    // Cleanup on shutdown
    Runtime.getRuntime().addShutdownHook(Thread {
        logger.info { "Shutting down Ingest Service..." }
        connectionPool.close()
        eventClient.disconnect()
    })
}

fun Application.configureSerialization() {
    install(ContentNegotiation) {
        json()
    }
}

fun Application.configureMonitoring() {
    install(CallLogging) {
        level = org.slf4j.event.Level.INFO
        filter { call -> call.request.origin.uri.startsWith("/api") }
    }
}

fun Application.configureStatusPages() {
    install(StatusPages) {
        exception<Throwable> { call, cause ->
            logger.error(cause) { "Unhandled exception" }
            call.respond(
                HttpStatusCode.InternalServerError,
                mapOf("error" to (cause.message ?: "Internal server error"))
            )
        }
    }
}

fun Application.configureRouting(
    shardRouter: ShardRouter,
    connectionPool: ConnectionPoolManager,
    eventClient: EventClient,
    resilienceManager: ResilienceManager
) {
    val ingestHandler = IngestHandler(shardRouter, connectionPool, eventClient, resilienceManager)

    routing {
        route("/api/v1") {
            ingest(ingestHandler)
            health(connectionPool, eventClient, resilienceManager)
        }
    }
}
