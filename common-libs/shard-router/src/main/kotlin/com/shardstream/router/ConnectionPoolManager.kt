package com.shardstream.router

import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import io.github.oshai.kotlinlogging.KotlinLogging
import java.sql.Connection
import java.util.concurrent.ConcurrentHashMap
import javax.sql.DataSource

private val logger = KotlinLogging.logger {}

/**
 * Connection Pool Manager - Manages connection pools for all shards
 *
 * Features:
 * - Connection pooling using HikariCP
 * - Automatic pool creation per shard
 * - Health monitoring
 * - Graceful shutdown
 */
class ConnectionPoolManager(
    private val shardRouter: ShardRouter,
    private val poolConfig: PoolConfig = PoolConfig()
) {
    private val writePools = ConcurrentHashMap<Int, HikariDataSource>()
    private val readPools = ConcurrentHashMap<String, HikariDataSource>()

    init {
        initializePools()
    }

    /**
     * Initialize connection pools for all shards
     */
    private fun initializePools() {
        shardRouter.getAllShardIds().forEach { shardId ->
            // Create write pool
            val writeNode = shardRouter.getWriteNode(shardId.toString())
            writePools[shardId] = createDataSource(writeNode, "shard-$shardId-write")

            // Create read pools
            val shardInfo = shardRouter.getAllReadNodes()
                .filter { it.first == shardId }

            shardInfo.forEachIndexed { index, (_, readNode) ->
                val poolKey = "$shardId-read-$index"
                readPools[poolKey] = createDataSource(readNode, "shard-$shardId-read-$index")
            }
        }

        logger.info { "Initialized connection pools: ${writePools.size} write, ${readPools.size} read" }
    }

    /**
     * Create a HikariCP data source
     */
    private fun createDataSource(node: ShardNode, poolName: String): HikariDataSource {
        val config = HikariConfig().apply {
            jdbcUrl = node.jdbcUrl()
            username = node.username
            password = node.password
            this.poolName = poolName

            // Connection pool settings
            maximumPoolSize = poolConfig.maxPoolSize
            minimumIdle = poolConfig.minIdleConnections
            connectionTimeout = poolConfig.connectionTimeoutMs
            idleTimeout = poolConfig.idleTimeoutMs
            maxLifetime = poolConfig.maxLifetimeMs

            // Performance optimizations
            isAutoCommit = poolConfig.autoCommit
            transactionIsolation = poolConfig.transactionIsolation

            // Health check
            connectionTestQuery = "SELECT 1"
            validationTimeout = 5000

            // Leak detection (helps catch connection leaks in development)
            leakDetectionThreshold = poolConfig.leakDetectionThresholdMs

            // Additional PostgreSQL-specific settings
            addDataSourceProperty("cachePrepStmts", "true")
            addDataSourceProperty("prepStmtCacheSize", "250")
            addDataSourceProperty("prepStmtCacheSqlLimit", "2048")
            addDataSourceProperty("useServerPrepStmts", "true")
            addDataSourceProperty("reWriteBatchedInserts", "true")
        }

        return HikariDataSource(config).also {
            logger.info { "Created connection pool '$poolName' for ${node.host}:${node.port}/${node.database}" }
        }
    }

    /**
     * Get a write connection for a sharding key
     */
    fun getWriteConnection(shardingKey: String): Connection {
        val shardId = shardRouter.getShardId(shardingKey)
        val dataSource = writePools[shardId]
            ?: throw IllegalStateException("Write pool for shard $shardId not found")

        return dataSource.connection.also {
            logger.debug { "Acquired write connection for shard $shardId (key: $shardingKey)" }
        }
    }

    /**
     * Get a read connection for a sharding key
     */
    fun getReadConnection(shardingKey: String, replicaIndex: Int? = null): Connection {
        val shardId = shardRouter.getShardId(shardingKey)

        // Select replica (round-robin if not specified)
        val actualReplicaIndex = replicaIndex
            ?: (System.currentTimeMillis() % 2).toInt()

        val poolKey = "$shardId-read-$actualReplicaIndex"
        val dataSource = readPools[poolKey]
            ?: readPools["$shardId-read-0"] // Fallback to first replica
            ?: throw IllegalStateException("Read pool for shard $shardId not found")

        return dataSource.connection.also {
            logger.debug { "Acquired read connection for shard $shardId replica $actualReplicaIndex (key: $shardingKey)" }
        }
    }

    /**
     * Get write data source (for bulk operations)
     */
    fun getWriteDataSource(shardId: Int): DataSource =
        writePools[shardId] ?: throw IllegalStateException("Write pool for shard $shardId not found")

    /**
     * Get read data source (for bulk operations)
     */
    fun getReadDataSource(shardId: Int, replicaIndex: Int = 0): DataSource {
        val poolKey = "$shardId-read-$replicaIndex"
        return readPools[poolKey]
            ?: throw IllegalStateException("Read pool $poolKey not found")
    }

    /**
     * Get pool statistics for monitoring
     */
    fun getPoolStats(): Map<String, PoolStats> {
        val stats = mutableMapOf<String, PoolStats>()

        writePools.forEach { (shardId, pool) ->
            stats["shard-$shardId-write"] = PoolStats(
                poolName = pool.poolName,
                activeConnections = pool.hikariPoolMXBean?.activeConnections ?: 0,
                idleConnections = pool.hikariPoolMXBean?.idleConnections ?: 0,
                totalConnections = pool.hikariPoolMXBean?.totalConnections ?: 0,
                threadsAwaitingConnection = pool.hikariPoolMXBean?.threadsAwaitingConnection ?: 0
            )
        }

        readPools.forEach { (poolKey, pool) ->
            stats[poolKey] = PoolStats(
                poolName = pool.poolName,
                activeConnections = pool.hikariPoolMXBean?.activeConnections ?: 0,
                idleConnections = pool.hikariPoolMXBean?.idleConnections ?: 0,
                totalConnections = pool.hikariPoolMXBean?.totalConnections ?: 0,
                threadsAwaitingConnection = pool.hikariPoolMXBean?.threadsAwaitingConnection ?: 0
            )
        }

        return stats
    }

    /**
     * Health check for all pools
     */
    fun healthCheck(): Map<String, Boolean> {
        val health = mutableMapOf<String, Boolean>()

        writePools.forEach { (shardId, pool) ->
            health["shard-$shardId-write"] = try {
                pool.connection.use { it.isValid(5) }
            } catch (e: Exception) {
                logger.error(e) { "Health check failed for shard-$shardId-write" }
                false
            }
        }

        readPools.forEach { (poolKey, pool) ->
            health[poolKey] = try {
                pool.connection.use { it.isValid(5) }
            } catch (e: Exception) {
                logger.error(e) { "Health check failed for $poolKey" }
                false
            }
        }

        return health
    }

    /**
     * Close all connection pools
     */
    fun close() {
        logger.info { "Closing all connection pools..." }

        writePools.values.forEach { it.close() }
        readPools.values.forEach { it.close() }

        writePools.clear()
        readPools.clear()

        logger.info { "All connection pools closed" }
    }
}

/**
 * Connection pool configuration
 */
data class PoolConfig(
    val maxPoolSize: Int = 20,
    val minIdleConnections: Int = 5,
    val connectionTimeoutMs: Long = 30_000,
    val idleTimeoutMs: Long = 600_000,
    val maxLifetimeMs: Long = 1_800_000,
    val leakDetectionThresholdMs: Long = 60_000,
    val autoCommit: Boolean = false,
    val transactionIsolation: String = "TRANSACTION_READ_COMMITTED"
)

/**
 * Pool statistics for monitoring
 */
data class PoolStats(
    val poolName: String,
    val activeConnections: Int,
    val idleConnections: Int,
    val totalConnections: Int,
    val threadsAwaitingConnection: Int
)
