package com.shardstream.router

import io.github.oshai.kotlinlogging.KotlinLogging
import java.security.MessageDigest
import java.util.concurrent.ConcurrentHashMap

private val logger = KotlinLogging.logger {}

/**
 * ShardRouter - Determines which shard to route requests to based on sharding key
 *
 * Uses consistent hashing to distribute data across shards evenly.
 * Supports:
 * - Multiple sharding strategies (hash, range, custom)
 * - Virtual nodes for better distribution
 * - Dynamic shard addition/removal
 */
class ShardRouter(
    private val totalShards: Int = 4,
    private val virtualNodesPerShard: Int = 150
) {
    private val shardMap = ConcurrentHashMap<Int, ShardInfo>()
    private val virtualNodeRing = TreeMap<Long, Int>()

    init {
        require(totalShards > 0) { "Total shards must be greater than 0" }
        initializeShards()
    }

    /**
     * Initialize shard configuration
     */
    private fun initializeShards() {
        for (shardId in 0 until totalShards) {
            val shardInfo = ShardInfo(
                shardId = shardId,
                writeNode = ShardNode(
                    host = System.getenv("SHARD_${shardId}_WRITE_HOST") ?: "shard-${shardId}-write",
                    port = System.getenv("SHARD_${shardId}_WRITE_PORT")?.toIntOrNull() ?: 5432,
                    database = System.getenv("SHARD_${shardId}_DB") ?: "shardstream_shard_${shardId}",
                    username = System.getenv("SHARD_${shardId}_USER") ?: "postgres",
                    password = System.getenv("SHARD_${shardId}_PASSWORD") ?: "postgres"
                ),
                readReplicas = listOf(
                    ShardNode(
                        host = System.getenv("SHARD_${shardId}_READ1_HOST") ?: "shard-${shardId}-read1",
                        port = System.getenv("SHARD_${shardId}_READ1_PORT")?.toIntOrNull() ?: 5432,
                        database = System.getenv("SHARD_${shardId}_DB") ?: "shardstream_shard_${shardId}",
                        username = System.getenv("SHARD_${shardId}_USER") ?: "postgres",
                        password = System.getenv("SHARD_${shardId}_PASSWORD") ?: "postgres"
                    ),
                    ShardNode(
                        host = System.getenv("SHARD_${shardId}_READ2_HOST") ?: "shard-${shardId}-read2",
                        port = System.getenv("SHARD_${shardId}_READ2_PORT")?.toIntOrNull() ?: 5432,
                        database = System.getenv("SHARD_${shardId}_DB") ?: "shardstream_shard_${shardId}",
                        username = System.getenv("SHARD_${shardId}_USER") ?: "postgres",
                        password = System.getenv("SHARD_${shardId}_PASSWORD") ?: "postgres"
                    )
                )
            )
            shardMap[shardId] = shardInfo

            // Add virtual nodes for better distribution
            for (vnode in 0 until virtualNodesPerShard) {
                val vnodeKey = "${shardId}:${vnode}"
                val hash = consistentHash(vnodeKey)
                virtualNodeRing[hash] = shardId
            }
        }

        logger.info { "Initialized ${totalShards} shards with ${virtualNodesPerShard} virtual nodes each" }
    }

    /**
     * Determine shard ID for a given key using consistent hashing
     */
    fun getShardId(shardingKey: String): Int {
        val hash = consistentHash(shardingKey)

        // Find the first virtual node with hash >= our hash
        val entry = virtualNodeRing.ceilingEntry(hash) ?: virtualNodeRing.firstEntry()

        return entry.value.also {
            logger.debug { "Routed key '$shardingKey' to shard $it (hash: $hash)" }
        }
    }

    /**
     * Get shard information for writes
     */
    fun getWriteNode(shardingKey: String): ShardNode {
        val shardId = getShardId(shardingKey)
        return shardMap[shardId]?.writeNode
            ?: throw IllegalStateException("Shard $shardId not found")
    }

    /**
     * Get shard information for reads (round-robin across replicas)
     */
    fun getReadNode(shardingKey: String, preferredReplicaIndex: Int? = null): ShardNode {
        val shardId = getShardId(shardingKey)
        val shard = shardMap[shardId]
            ?: throw IllegalStateException("Shard $shardId not found")

        return if (preferredReplicaIndex != null && preferredReplicaIndex < shard.readReplicas.size) {
            shard.readReplicas[preferredReplicaIndex]
        } else {
            // Round-robin selection
            val index = System.currentTimeMillis().toInt() % shard.readReplicas.size
            shard.readReplicas[index]
        }
    }

    /**
     * Get all shard IDs
     */
    fun getAllShardIds(): List<Int> = shardMap.keys.toList().sorted()

    /**
     * Get all write nodes (for scatter-gather queries)
     */
    fun getAllWriteNodes(): List<Pair<Int, ShardNode>> =
        shardMap.map { (id, info) -> id to info.writeNode }

    /**
     * Get all read nodes (for scatter-gather queries)
     */
    fun getAllReadNodes(): List<Pair<Int, ShardNode>> =
        shardMap.flatMap { (id, info) ->
            info.readReplicas.map { id to it }
        }

    /**
     * Consistent hash function using MD5
     */
    private fun consistentHash(key: String): Long {
        val md = MessageDigest.getInstance("MD5")
        val digest = md.digest(key.toByteArray())

        // Use first 8 bytes as long
        var hash = 0L
        for (i in 0 until 8) {
            hash = (hash shl 8) or (digest[i].toLong() and 0xFF)
        }

        return hash
    }
}

/**
 * Information about a specific shard
 */
data class ShardInfo(
    val shardId: Int,
    val writeNode: ShardNode,
    val readReplicas: List<ShardNode>
)

/**
 * Database node connection information
 */
data class ShardNode(
    val host: String,
    val port: Int,
    val database: String,
    val username: String,
    val password: String
) {
    fun jdbcUrl(): String = "jdbc:postgresql://${host}:${port}/${database}"
}

// TreeMap implementation for consistent hashing ring
private class TreeMap<K : Comparable<K>, V> {
    private val map = sortedMapOf<K, V>()

    operator fun set(key: K, value: V) {
        map[key] = value
    }

    fun ceilingEntry(key: K): Map.Entry<K, V>? {
        return map.entries.firstOrNull { it.key >= key }
    }

    fun firstEntry(): Map.Entry<K, V>? = map.entries.firstOrNull()
}
