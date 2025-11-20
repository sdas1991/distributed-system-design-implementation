package com.shardstream.events

import io.github.oshai.kotlinlogging.KotlinLogging
import io.nats.client.*
import io.nats.client.api.RetentionPolicy
import io.nats.client.api.StorageType
import kotlinx.coroutines.*
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.callbackFlow
import kotlinx.serialization.Serializable
import kotlinx.serialization.encodeToString
import kotlinx.serialization.json.Json
import java.time.Duration
import java.util.concurrent.atomic.AtomicBoolean

private val logger = KotlinLogging.logger {}

/**
 * Event Client - High-level client for NATS JetStream
 *
 * Features:
 * - Publish to multiple topics
 * - Subscribe with durable consumers
 * - Automatic reconnection
 * - Structured event types
 * - Acknowledgment handling
 */
class EventClient(
    private val natsUrl: String = System.getenv("NATS_URL") ?: "nats://localhost:4222",
    private val clientName: String = "shardstream-client"
) {
    private var connection: Connection? = null
    private var jetStream: JetStream? = null
    private val isConnected = AtomicBoolean(false)
    private val json = Json { ignoreUnknownKeys = true }

    companion object {
        // Topic definitions
        const val TOPIC_INGEST_RAW = "ingest.raw"
        const val TOPIC_UPDATES_REALTIME = "updates.realtime"
        const val TOPIC_UPDATES_DELAYED = "updates.delayed"
        const val TOPIC_ENTITY_REPAIR = "entity.repair"
        const val TOPIC_SHARD_SYNC = "shard-sync.events"

        // Stream definitions
        const val STREAM_INGEST = "INGEST"
        const val STREAM_UPDATES = "UPDATES"
        const val STREAM_SYSTEM = "SYSTEM"
    }

    /**
     * Connect to NATS server and initialize JetStream
     */
    suspend fun connect() = withContext(Dispatchers.IO) {
        if (isConnected.get()) {
            logger.warn { "Already connected to NATS" }
            return@withContext
        }

        try {
            val options = Options.Builder()
                .server(natsUrl)
                .connectionName(clientName)
                .maxReconnects(-1) // Infinite reconnects
                .reconnectWait(Duration.ofSeconds(2))
                .connectionListener { conn, type ->
                    when (type) {
                        ConnectionListener.Events.CONNECTED -> {
                            logger.info { "Connected to NATS at ${conn.serverInfo?.host}" }
                            isConnected.set(true)
                        }
                        ConnectionListener.Events.DISCONNECTED -> {
                            logger.warn { "Disconnected from NATS" }
                            isConnected.set(false)
                        }
                        ConnectionListener.Events.RECONNECTED -> {
                            logger.info { "Reconnected to NATS" }
                            isConnected.set(true)
                        }
                        else -> logger.debug { "NATS connection event: $type" }
                    }
                }
                .errorListener { _, error ->
                    logger.error { "NATS error: ${error.message}" }
                }
                .build()

            connection = Nats.connect(options)
            jetStream = connection?.jetStream()

            // Initialize streams
            initializeStreams()

            logger.info { "NATS EventClient initialized successfully" }
        } catch (e: Exception) {
            logger.error(e) { "Failed to connect to NATS" }
            throw e
        }
    }

    /**
     * Initialize JetStream streams and subjects
     */
    private fun initializeStreams() {
        val jsm = connection?.jetStreamManagement() ?: return

        try {
            // Create INGEST stream
            createStreamIfNotExists(
                jsm,
                streamName = STREAM_INGEST,
                subjects = listOf("$TOPIC_INGEST_RAW.>"),
                description = "Raw ingestion events",
                maxAge = Duration.ofDays(7)
            )

            // Create UPDATES stream
            createStreamIfNotExists(
                jsm,
                streamName = STREAM_UPDATES,
                subjects = listOf("$TOPIC_UPDATES_REALTIME.>", "$TOPIC_UPDATES_DELAYED.>"),
                description = "Real-time and delayed update events",
                maxAge = Duration.ofHours(24)
            )

            // Create SYSTEM stream
            createStreamIfNotExists(
                jsm,
                streamName = STREAM_SYSTEM,
                subjects = listOf("$TOPIC_ENTITY_REPAIR.>", "$TOPIC_SHARD_SYNC.>"),
                description = "System events for repairs and sync",
                maxAge = Duration.ofDays(30)
            )

            logger.info { "JetStream streams initialized" }
        } catch (e: Exception) {
            logger.error(e) { "Failed to initialize streams" }
        }
    }

    /**
     * Create a stream if it doesn't exist
     */
    private fun createStreamIfNotExists(
        jsm: JetStreamManagement,
        streamName: String,
        subjects: List<String>,
        description: String,
        maxAge: Duration
    ) {
        try {
            jsm.getStreamInfo(streamName)
            logger.debug { "Stream $streamName already exists" }
        } catch (e: JetStreamApiException) {
            if (e.errorCode == 404) {
                val streamConfig = io.nats.client.api.StreamConfiguration.builder()
                    .name(streamName)
                    .subjects(subjects)
                    .description(description)
                    .retentionPolicy(RetentionPolicy.Limits)
                    .storageType(StorageType.File)
                    .maxAge(maxAge)
                    .replicas(1)
                    .build()

                jsm.addStream(streamConfig)
                logger.info { "Created stream: $streamName with subjects: $subjects" }
            } else {
                throw e
            }
        }
    }

    /**
     * Publish an event to a topic
     */
    suspend fun <T : Event> publish(topic: String, event: T): Boolean = withContext(Dispatchers.IO) {
        val js = jetStream ?: throw IllegalStateException("Not connected to JetStream")

        try {
            val eventJson = json.encodeToString(event)
            val message = Message.builder()
                .subject(topic)
                .data(eventJson.toByteArray())
                .build()

            val ack = js.publish(message)

            logger.debug {
                "Published event to $topic: ${event.eventId} (stream: ${ack.stream}, seq: ${ack.seqno})"
            }

            true
        } catch (e: Exception) {
            logger.error(e) { "Failed to publish event to $topic" }
            false
        }
    }

    /**
     * Subscribe to a topic with a durable consumer
     */
    fun <T : Event> subscribe(
        topic: String,
        consumerName: String,
        eventClass: Class<T>,
        batchSize: Int = 10
    ): Flow<T> = callbackFlow {
        val js = jetStream ?: throw IllegalStateException("Not connected to JetStream")

        val subscription = js.subscribe(
            topic,
            PushSubscribeOptions.builder()
                .durable(consumerName)
                .build()
        )

        val dispatcher = connection?.createDispatcher { msg ->
            try {
                val eventJson = String(msg.data)
                val event = json.decodeFromString(eventClass, eventJson)

                // Send to flow
                trySend(event).isSuccess

                // Acknowledge message
                msg.ack()

                logger.debug { "Received and acknowledged event: ${event.eventId}" }
            } catch (e: Exception) {
                logger.error(e) { "Error processing message from $topic" }
                // Negative acknowledgment - requeue message
                msg.nak()
            }
        }

        dispatcher?.subscribe(topic)

        awaitClose {
            subscription.unsubscribe()
            dispatcher?.unsubscribe(topic)
            logger.info { "Unsubscribed from $topic" }
        }
    }

    /**
     * Disconnect from NATS
     */
    fun disconnect() {
        try {
            connection?.close()
            isConnected.set(false)
            logger.info { "Disconnected from NATS" }
        } catch (e: Exception) {
            logger.error(e) { "Error disconnecting from NATS" }
        }
    }

    /**
     * Check if connected
     */
    fun isConnected(): Boolean = isConnected.get()
}

/**
 * Base event interface
 */
@Serializable
interface Event {
    val eventId: String
    val eventType: String
    val timestamp: Long
}

/**
 * Ingest event - raw data coming into the system
 */
@Serializable
data class IngestEvent(
    override val eventId: String,
    override val eventType: String,
    override val timestamp: Long,
    val customerId: String,
    val payload: String, // JSON string
    val metadata: Map<String, String> = emptyMap()
) : Event

/**
 * Update event - for real-time or delayed updates
 */
@Serializable
data class UpdateEvent(
    override val eventId: String,
    override val eventType: String,
    override val timestamp: Long,
    val customerId: String,
    val entityId: String,
    val updateType: String,
    val data: String, // JSON string
    val isRealtime: Boolean = true
) : Event

/**
 * System event - for repairs and synchronization
 */
@Serializable
data class SystemEvent(
    override val eventId: String,
    override val eventType: String,
    override val timestamp: Long,
    val shardId: Int,
    val action: String,
    val details: String
) : Event
