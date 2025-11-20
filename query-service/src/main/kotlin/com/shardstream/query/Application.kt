package com.shardstream.query

import com.shardstream.cache.CacheManager
import com.shardstream.observability.ResilienceManager
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

private val logger = KotlinLogging.logger {}

fun main() {
    logger.info { "Starting Query Service..." }

    // Initialize infrastructure
    val shardRouter = ShardRouter(totalShards = 4)
    val connectionPool = ConnectionPoolManager(shardRouter)
    val resilienceManager = ResilienceManager()

    // Initialize distributed cache
    val cacheManager = try {
        CacheManager(
            redisUrl = System.getenv("REDIS_URL") ?: "redis://redis:6379",
            defaultTtlSeconds = 300 // 5 minutes
        ).also {
            logger.info { "Redis cache enabled" }
        }
    } catch (e: Exception) {
        logger.warn(e) { "Redis not available, running without cache" }
        null
    }

    // Start server
    embeddedServer(Netty, port = 8082, host = "0.0.0.0") {
        configureRouting(shardRouter, connectionPool, resilienceManager, cacheManager)
        configureSerialization()
        configureMonitoring()
        configureStatusPages()
    }.start(wait = true)

    // Cleanup on shutdown
    Runtime.getRuntime().addShutdownHook(Thread {
        logger.info { "Shutting down Query Service..." }
        connectionPool.close()
        cacheManager?.close()
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
    resilienceManager: ResilienceManager,
    cacheManager: CacheManager?
) {
    val queryHandler = QueryHandler(shardRouter, connectionPool, resilienceManager, cacheManager)

    routing {
        route("/api/v1") {
            query(queryHandler)
            health(connectionPool, resilienceManager, cacheManager)
        }
    }
}
