package com.shardstream.observability

import io.github.oshai.kotlinlogging.KotlinLogging
import io.github.resilience4j.circuitbreaker.CircuitBreaker
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry
import io.github.resilience4j.kotlin.circuitbreaker.executeSuspendFunction
import java.time.Duration
import java.util.concurrent.ConcurrentHashMap

private val logger = KotlinLogging.logger {}

/**
 * Circuit Breaker Manager - Manages circuit breakers for different services/shards
 *
 * Features:
 * - Per-shard circuit breakers
 * - Configurable failure thresholds
 * - Automatic state transitions
 * - Health monitoring
 */
class CircuitBreakerManager(
    private val config: CircuitBreakerConfiguration = CircuitBreakerConfiguration()
) {
    private val circuitBreakers = ConcurrentHashMap<String, CircuitBreaker>()
    private val registry: CircuitBreakerRegistry

    init {
        val cbConfig = CircuitBreakerConfig.custom()
            .failureRateThreshold(config.failureRateThreshold)
            .slowCallRateThreshold(config.slowCallRateThreshold)
            .slowCallDurationThreshold(Duration.ofMillis(config.slowCallDurationMs))
            .permittedNumberOfCallsInHalfOpenState(config.permittedCallsInHalfOpen)
            .maxWaitDurationInHalfOpenState(Duration.ofMillis(config.maxWaitInHalfOpenMs))
            .slidingWindowType(CircuitBreakerConfig.SlidingWindowType.COUNT_BASED)
            .slidingWindowSize(config.slidingWindowSize)
            .minimumNumberOfCalls(config.minimumNumberOfCalls)
            .waitDurationInOpenState(Duration.ofMillis(config.waitDurationInOpenMs))
            .recordExceptions(
                java.io.IOException::class.java,
                java.sql.SQLException::class.java,
                java.util.concurrent.TimeoutException::class.java
            )
            .build()

        registry = CircuitBreakerRegistry.of(cbConfig)

        logger.info { "CircuitBreakerManager initialized with config: $config" }
    }

    /**
     * Get or create a circuit breaker for a specific name (e.g., "shard-0-write")
     */
    fun getCircuitBreaker(name: String): CircuitBreaker {
        return circuitBreakers.computeIfAbsent(name) {
            registry.circuitBreaker(name).also { cb ->
                // Register event listeners
                cb.eventPublisher
                    .onStateTransition { event ->
                        logger.warn {
                            "Circuit breaker '$name' state transition: ${event.stateTransition}"
                        }
                    }
                    .onError { event ->
                        logger.debug {
                            "Circuit breaker '$name' recorded error: ${event.throwable?.message}"
                        }
                    }

                logger.info { "Created circuit breaker: $name" }
            }
        }
    }

    /**
     * Execute a suspending function with circuit breaker protection
     */
    suspend fun <T> executeWithCircuitBreaker(
        name: String,
        block: suspend () -> T
    ): T {
        val circuitBreaker = getCircuitBreaker(name)
        return circuitBreaker.executeSuspendFunction(block)
    }

    /**
     * Get circuit breaker state
     */
    fun getState(name: String): CircuitBreaker.State? {
        return circuitBreakers[name]?.state
    }

    /**
     * Get all circuit breaker states
     */
    fun getAllStates(): Map<String, CircuitBreaker.State> {
        return circuitBreakers.mapValues { it.value.state }
    }

    /**
     * Get circuit breaker metrics
     */
    fun getMetrics(name: String): CircuitBreakerMetrics? {
        val cb = circuitBreakers[name] ?: return null
        val metrics = cb.metrics

        return CircuitBreakerMetrics(
            name = name,
            state = cb.state.toString(),
            failureRate = metrics.failureRate,
            slowCallRate = metrics.slowCallRate,
            numberOfSuccessfulCalls = metrics.numberOfSuccessfulCalls,
            numberOfFailedCalls = metrics.numberOfFailedCalls,
            numberOfSlowCalls = metrics.numberOfSlowCalls,
            numberOfNotPermittedCalls = metrics.numberOfNotPermittedCalls
        )
    }

    /**
     * Get all circuit breaker metrics
     */
    fun getAllMetrics(): List<CircuitBreakerMetrics> {
        return circuitBreakers.keys.mapNotNull { getMetrics(it) }
    }

    /**
     * Reset a circuit breaker
     */
    fun reset(name: String) {
        circuitBreakers[name]?.reset()
        logger.info { "Reset circuit breaker: $name" }
    }

    /**
     * Reset all circuit breakers
     */
    fun resetAll() {
        circuitBreakers.forEach { (name, cb) ->
            cb.reset()
            logger.info { "Reset circuit breaker: $name" }
        }
    }
}

/**
 * Circuit breaker configuration
 */
data class CircuitBreakerConfiguration(
    val failureRateThreshold: Float = 50f, // 50% failure rate opens circuit
    val slowCallRateThreshold: Float = 100f, // 100% slow calls opens circuit
    val slowCallDurationMs: Long = 5000, // Calls slower than 5s are considered slow
    val permittedCallsInHalfOpen: Int = 3, // Test 3 calls in half-open state
    val maxWaitInHalfOpenMs: Long = 10000, // Max wait in half-open state
    val slidingWindowSize: Int = 10, // Look at last 10 calls
    val minimumNumberOfCalls: Int = 5, // Need at least 5 calls before calculating rates
    val waitDurationInOpenMs: Long = 60000 // Wait 60s before transitioning to half-open
)

/**
 * Circuit breaker metrics
 */
data class CircuitBreakerMetrics(
    val name: String,
    val state: String,
    val failureRate: Float,
    val slowCallRate: Float,
    val numberOfSuccessfulCalls: Int,
    val numberOfFailedCalls: Int,
    val numberOfSlowCalls: Int,
    val numberOfNotPermittedCalls: Long
)
