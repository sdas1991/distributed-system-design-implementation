package com.shardstream.cache

import io.github.oshai.kotlinlogging.KotlinLogging
import io.lettuce.core.RedisClient
import io.lettuce.core.RedisURI
import io.lettuce.core.api.StatefulRedisConnection
import io.lettuce.core.api.sync.RedisCommands
import io.lettuce.core.cluster.RedisClusterClient
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection
import io.lettuce.core.cluster.api.sync.RedisAdvancedClusterCommands
import kotlinx.serialization.encodeToString
import kotlinx.serialization.json.Json
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicLong

private val logger = KotlinLogging.logger {}

/**
 * Distributed Cache Manager using Redis
 *
 * Features:
 * - Cache-aside pattern
 * - TTL-based expiration
 * - Cache invalidation
 * - Hit/miss metrics
 * - Cluster support
 */
class CacheManager(
    private val redisUrl: String = System.getenv("REDIS_URL") ?: "redis://localhost:6379",
    private val defaultTtlSeconds: Long = 300, // 5 minutes
    private val useCluster: Boolean = false
) {
    private var client: RedisClient? = null
    private var clusterClient: RedisClusterClient? = null
    private var connection: StatefulRedisConnection<String, String>? = null
    private var clusterConnection: StatefulRedisClusterConnection<String, String>? = null
    private val json = Json { ignoreUnknownKeys = true }

    // Metrics
    private val hits = AtomicLong(0)
    private val misses = AtomicLong(0)
    private val writes = AtomicLong(0)
    private val deletes = AtomicLong(0)

    init {
        connect()
    }

    /**
     * Connect to Redis
     */
    private fun connect() {
        try {
            if (useCluster) {
                val uris = redisUrl.split(",").map { RedisURI.create(it.trim()) }
                clusterClient = RedisClusterClient.create(uris)
                clusterConnection = clusterClient?.connect()
                logger.info { "Connected to Redis Cluster: $redisUrl" }
            } else {
                client = RedisClient.create(redisUrl)
                connection = client?.connect()
                logger.info { "Connected to Redis: $redisUrl" }
            }
        } catch (e: Exception) {
            logger.error(e) { "Failed to connect to Redis" }
            throw e
        }
    }

    /**
     * Get value from cache
     */
    fun get(key: String): String? {
        return try {
            val value = if (useCluster) {
                clusterConnection?.sync()?.get(key)
            } else {
                connection?.sync()?.get(key)
            }

            if (value != null) {
                hits.incrementAndGet()
                logger.debug { "Cache HIT: $key" }
            } else {
                misses.incrementAndGet()
                logger.debug { "Cache MISS: $key" }
            }

            value
        } catch (e: Exception) {
            logger.error(e) { "Failed to get from cache: $key" }
            misses.incrementAndGet()
            null
        }
    }

    /**
     * Get and deserialize JSON value
     */
    inline fun <reified T> getObject(key: String): T? {
        val jsonString = get(key) ?: return null
        return try {
            json.decodeFromString<T>(jsonString)
        } catch (e: Exception) {
            logger.error(e) { "Failed to deserialize cache value for key: $key" }
            null
        }
    }

    /**
     * Set value in cache with TTL
     */
    fun set(key: String, value: String, ttlSeconds: Long = defaultTtlSeconds): Boolean {
        return try {
            if (useCluster) {
                clusterConnection?.sync()?.setex(key, ttlSeconds, value)
            } else {
                connection?.sync()?.setex(key, ttlSeconds, value)
            }

            writes.incrementAndGet()
            logger.debug { "Cache SET: $key (TTL: ${ttlSeconds}s)" }
            true
        } catch (e: Exception) {
            logger.error(e) { "Failed to set cache: $key" }
            false
        }
    }

    /**
     * Serialize and set object in cache
     */
    inline fun <reified T> setObject(key: String, value: T, ttlSeconds: Long = defaultTtlSeconds): Boolean {
        return try {
            val jsonString = json.encodeToString(value)
            set(key, jsonString, ttlSeconds)
        } catch (e: Exception) {
            logger.error(e) { "Failed to serialize and cache object: $key" }
            false
        }
    }

    /**
     * Delete key from cache
     */
    fun delete(key: String): Boolean {
        return try {
            val deleted = if (useCluster) {
                clusterConnection?.sync()?.del(key) ?: 0
            } else {
                connection?.sync()?.del(key) ?: 0
            }

            if (deleted > 0) {
                deletes.incrementAndGet()
                logger.debug { "Cache DELETE: $key" }
                true
            } else {
                false
            }
        } catch (e: Exception) {
            logger.error(e) { "Failed to delete from cache: $key" }
            false
        }
    }

    /**
     * Delete keys matching pattern
     */
    fun deletePattern(pattern: String): Long {
        return try {
            val keys = if (useCluster) {
                clusterConnection?.sync()?.keys(pattern) ?: emptyList()
            } else {
                connection?.sync()?.keys(pattern) ?: emptyList()
            }

            if (keys.isEmpty()) {
                return 0
            }

            val deleted = if (useCluster) {
                clusterConnection?.sync()?.del(*keys.toTypedArray()) ?: 0
            } else {
                connection?.sync()?.del(*keys.toTypedArray()) ?: 0
            }

            deletes.addAndGet(deleted)
            logger.info { "Cache DELETE pattern '$pattern': $deleted keys deleted" }
            deleted
        } catch (e: Exception) {
            logger.error(e) { "Failed to delete pattern from cache: $pattern" }
            0
        }
    }

    /**
     * Check if key exists
     */
    fun exists(key: String): Boolean {
        return try {
            val exists = if (useCluster) {
                clusterConnection?.sync()?.exists(key) ?: 0
            } else {
                connection?.sync()?.exists(key) ?: 0
            }
            exists > 0
        } catch (e: Exception) {
            logger.error(e) { "Failed to check existence in cache: $key" }
            false
        }
    }

    /**
     * Increment counter
     */
    fun increment(key: String, by: Long = 1): Long? {
        return try {
            val result = if (useCluster) {
                clusterConnection?.sync()?.incrby(key, by)
            } else {
                connection?.sync()?.incrby(key, by)
            }
            result
        } catch (e: Exception) {
            logger.error(e) { "Failed to increment cache key: $key" }
            null
        }
    }

    /**
     * Get multiple keys
     */
    fun mget(vararg keys: String): List<String?> {
        return try {
            if (useCluster) {
                clusterConnection?.sync()?.mget(*keys)?.map { it.value } ?: emptyList()
            } else {
                connection?.sync()?.mget(*keys)?.map { it.value } ?: emptyList()
            }
        } catch (e: Exception) {
            logger.error(e) { "Failed to mget from cache" }
            emptyList()
        }
    }

    /**
     * Get cache statistics
     */
    fun getStats(): CacheStats {
        val totalRequests = hits.get() + misses.get()
        val hitRate = if (totalRequests > 0) {
            (hits.get().toDouble() / totalRequests) * 100
        } else {
            0.0
        }

        return CacheStats(
            hits = hits.get(),
            misses = misses.get(),
            writes = writes.get(),
            deletes = deletes.get(),
            hitRate = hitRate,
            totalRequests = totalRequests
        )
    }

    /**
     * Reset statistics
     */
    fun resetStats() {
        hits.set(0)
        misses.set(0)
        writes.set(0)
        deletes.set(0)
        logger.info { "Cache statistics reset" }
    }

    /**
     * Flush all cache
     */
    fun flushAll(): Boolean {
        return try {
            if (useCluster) {
                clusterConnection?.sync()?.flushall()
            } else {
                connection?.sync()?.flushall()
            }
            logger.warn { "Cache flushed (all keys deleted)" }
            true
        } catch (e: Exception) {
            logger.error(e) { "Failed to flush cache" }
            false
        }
    }

    /**
     * Close connections
     */
    fun close() {
        try {
            connection?.close()
            clusterConnection?.close()
            client?.shutdown()
            clusterClient?.shutdown()
            logger.info { "Redis connections closed" }
        } catch (e: Exception) {
            logger.error(e) { "Error closing Redis connections" }
        }
    }
}

/**
 * Cache statistics
 */
data class CacheStats(
    val hits: Long,
    val misses: Long,
    val writes: Long,
    val deletes: Long,
    val hitRate: Double,
    val totalRequests: Long
)

/**
 * Cache key builder for consistent naming
 */
object CacheKeys {
    private const val PREFIX = "shardstream"

    fun customerEvents(customerId: String, limit: Int, offset: Int, eventType: String? = null): String {
        return "$PREFIX:customer:$customerId:events:l$limit:o$offset${eventType?.let { ":t$it" } ?: ""}"
    }

    fun event(eventId: String, customerId: String): String {
        return "$PREFIX:event:$eventId:$customerId"
    }

    fun customerMetrics(customerId: String): String {
        return "$PREFIX:metrics:$customerId"
    }

    fun customerPattern(customerId: String): String {
        return "$PREFIX:*:$customerId*"
    }

    fun allEvents(): String {
        return "$PREFIX:*:events:*"
    }
}
