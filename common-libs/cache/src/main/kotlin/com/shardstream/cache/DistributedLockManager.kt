package com.shardstream.cache

import io.github.oshai.kotlinlogging.KotlinLogging
import io.lettuce.core.RedisClient
import io.lettuce.core.ScriptOutputType
import io.lettuce.core.SetArgs
import io.lettuce.core.api.StatefulRedisConnection
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.delay
import kotlinx.coroutines.withContext
import java.util.*

private val logger = KotlinLogging.logger {}

/**
 * Distributed Lock Manager using Redis
 *
 * Implements a simplified version of Redlock algorithm for distributed locking.
 * Features:
 * - Acquire locks with automatic expiration
 * - Release locks safely using Lua scripts
 * - Lock extension (refresh)
 * - Deadlock prevention
 *
 * Use cases:
 * - Leader election in distributed services
 * - Prevent duplicate job execution
 * - Coordination between service instances
 */
class DistributedLockManager(
    private val redisUrl: String = "redis://localhost:6379"
) {
    private val client: RedisClient = RedisClient.create(redisUrl)
    private val connection: StatefulRedisConnection<String, String> = client.connect()
    private val syncCommands = connection.sync()

    // Lua script for safe lock release
    private val releaseLockScript = """
        if redis.call("get", KEYS[1]) == ARGV[1] then
            return redis.call("del", KEYS[1])
        else
            return 0
        end
    """.trimIndent()

    // Lua script for lock extension
    private val extendLockScript = """
        if redis.call("get", KEYS[1]) == ARGV[1] then
            return redis.call("pexpire", KEYS[1], ARGV[2])
        else
            return 0
        end
    """.trimIndent()

    /**
     * Acquire a distributed lock
     *
     * @param lockKey Unique identifier for the lock
     * @param ttlMillis Lock expiration time in milliseconds
     * @return Lock token if acquired, null if failed
     */
    suspend fun acquireLock(
        lockKey: String,
        ttlMillis: Long = 30000 // 30 seconds default
    ): String? = withContext(Dispatchers.IO) {
        val lockToken = UUID.randomUUID().toString()
        val fullKey = "lock:$lockKey"

        try {
            // Try to set the key with NX (only if not exists) and PX (expiration)
            val result = syncCommands.set(
                fullKey,
                lockToken,
                SetArgs().nx().px(ttlMillis)
            )

            if (result == "OK") {
                logger.debug { "Acquired lock: $lockKey (token: $lockToken, ttl: ${ttlMillis}ms)" }
                lockToken
            } else {
                logger.debug { "Failed to acquire lock: $lockKey (already held)" }
                null
            }
        } catch (e: Exception) {
            logger.error(e) { "Error acquiring lock: $lockKey" }
            null
        }
    }

    /**
     * Release a distributed lock
     *
     * @param lockKey Lock identifier
     * @param lockToken Token returned from acquireLock
     * @return true if released successfully, false otherwise
     */
    suspend fun releaseLock(lockKey: String, lockToken: String): Boolean = withContext(Dispatchers.IO) {
        val fullKey = "lock:$lockKey"

        try {
            val result = syncCommands.eval<Long>(
                releaseLockScript,
                ScriptOutputType.INTEGER,
                arrayOf(fullKey),
                lockToken
            )

            if (result == 1L) {
                logger.debug { "Released lock: $lockKey (token: $lockToken)" }
                true
            } else {
                logger.warn { "Failed to release lock: $lockKey (token mismatch or expired)" }
                false
            }
        } catch (e: Exception) {
            logger.error(e) { "Error releasing lock: $lockKey" }
            false
        }
    }

    /**
     * Extend/refresh a lock's TTL
     *
     * @param lockKey Lock identifier
     * @param lockToken Token returned from acquireLock
     * @param ttlMillis New TTL in milliseconds
     * @return true if extended successfully, false otherwise
     */
    suspend fun extendLock(
        lockKey: String,
        lockToken: String,
        ttlMillis: Long = 30000
    ): Boolean = withContext(Dispatchers.IO) {
        val fullKey = "lock:$lockKey"

        try {
            val result = syncCommands.eval<Long>(
                extendLockScript,
                ScriptOutputType.INTEGER,
                arrayOf(fullKey),
                lockToken,
                ttlMillis.toString()
            )

            if (result == 1L) {
                logger.debug { "Extended lock: $lockKey (token: $lockToken, ttl: ${ttlMillis}ms)" }
                true
            } else {
                logger.warn { "Failed to extend lock: $lockKey (token mismatch or expired)" }
                false
            }
        } catch (e: Exception) {
            logger.error(e) { "Error extending lock: $lockKey" }
            false
        }
    }

    /**
     * Try to acquire a lock with retries
     *
     * @param lockKey Lock identifier
     * @param ttlMillis Lock expiration time
     * @param maxRetries Maximum number of retries
     * @param retryDelayMillis Delay between retries
     * @return Lock token if acquired, null if failed after all retries
     */
    suspend fun acquireLockWithRetry(
        lockKey: String,
        ttlMillis: Long = 30000,
        maxRetries: Int = 3,
        retryDelayMillis: Long = 100
    ): String? {
        repeat(maxRetries) { attempt ->
            val token = acquireLock(lockKey, ttlMillis)
            if (token != null) {
                return token
            }

            if (attempt < maxRetries - 1) {
                logger.debug { "Lock acquisition attempt ${attempt + 1} failed, retrying..." }
                delay(retryDelayMillis)
            }
        }

        logger.warn { "Failed to acquire lock after $maxRetries attempts: $lockKey" }
        return null
    }

    /**
     * Execute a block of code while holding a lock
     *
     * @param lockKey Lock identifier
     * @param ttlMillis Lock expiration time
     * @param block Code to execute while holding the lock
     * @return Result of the block, or null if lock acquisition failed
     */
    suspend fun <T> withLock(
        lockKey: String,
        ttlMillis: Long = 30000,
        block: suspend () -> T
    ): T? {
        val token = acquireLock(lockKey, ttlMillis) ?: return null

        return try {
            block()
        } finally {
            releaseLock(lockKey, token)
        }
    }

    /**
     * Check if a lock is currently held
     *
     * @param lockKey Lock identifier
     * @return true if lock exists, false otherwise
     */
    suspend fun isLocked(lockKey: String): Boolean = withContext(Dispatchers.IO) {
        val fullKey = "lock:$lockKey"

        try {
            syncCommands.exists(fullKey) > 0
        } catch (e: Exception) {
            logger.error(e) { "Error checking lock status: $lockKey" }
            false
        }
    }

    /**
     * Close the lock manager and release resources
     */
    fun close() {
        try {
            connection.close()
            client.shutdown()
            logger.info { "Distributed lock manager closed" }
        } catch (e: Exception) {
            logger.error(e) { "Error closing lock manager" }
        }
    }
}

/**
 * Leader Election using Distributed Locks
 *
 * Helper class for leader election pattern in distributed services.
 * One instance becomes the leader and periodically refreshes the lock.
 */
class LeaderElection(
    private val lockManager: DistributedLockManager,
    private val serviceName: String,
    private val instanceId: String = UUID.randomUUID().toString(),
    private val leaseDurationMillis: Long = 30000 // 30 seconds
) {
    private var leaderToken: String? = null
    private var isLeader = false

    /**
     * Try to become the leader
     *
     * @return true if elected as leader, false otherwise
     */
    suspend fun tryBecomeLeader(): Boolean {
        val lockKey = "leader:$serviceName"
        val token = lockManager.acquireLock(lockKey, leaseDurationMillis)

        if (token != null) {
            leaderToken = token
            isLeader = true
            logger.info { "Instance $instanceId became leader for $serviceName" }
            return true
        }

        isLeader = false
        return false
    }

    /**
     * Renew leadership (extend the lock)
     *
     * @return true if renewed successfully, false if lost leadership
     */
    suspend fun renewLeadership(): Boolean {
        val lockKey = "leader:$serviceName"
        val token = leaderToken ?: return false

        val renewed = lockManager.extendLock(lockKey, token, leaseDurationMillis)

        if (!renewed) {
            logger.warn { "Instance $instanceId lost leadership for $serviceName" }
            isLeader = false
            leaderToken = null
        }

        return renewed
    }

    /**
     * Step down from leadership
     */
    suspend fun stepDown() {
        if (!isLeader) return

        val lockKey = "leader:$serviceName"
        val token = leaderToken ?: return

        lockManager.releaseLock(lockKey, token)
        isLeader = false
        leaderToken = null

        logger.info { "Instance $instanceId stepped down from leadership for $serviceName" }
    }

    /**
     * Check if this instance is the current leader
     */
    fun isCurrentLeader(): Boolean = isLeader
}
