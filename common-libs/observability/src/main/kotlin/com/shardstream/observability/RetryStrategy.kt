package com.shardstream.observability

import io.github.oshai.kotlinlogging.KotlinLogging
import io.github.resilience4j.kotlin.retry.executeSuspendFunction
import io.github.resilience4j.retry.Retry
import io.github.resilience4j.retry.RetryConfig
import io.github.resilience4j.retry.RetryRegistry
import java.time.Duration
import java.util.concurrent.ConcurrentHashMap

private val logger = KotlinLogging.logger {}

/**
 * Retry Strategy Manager - Manages retry policies for different operations
 *
 * Features:
 * - Exponential backoff
 * - Configurable max attempts
 * - Per-operation retry policies
 * - Retry metrics
 */
class RetryStrategyManager(
    private val config: RetryConfiguration = RetryConfiguration()
) {
    private val retryPolicies = ConcurrentHashMap<String, Retry>()
    private val registry: RetryRegistry

    init {
        val retryConfig = RetryConfig.custom<Any>()
            .maxAttempts(config.maxAttempts)
            .waitDuration(Duration.ofMillis(config.waitDurationMs))
            .intervalFunction { attemptNumber ->
                // Exponential backoff: wait * (multiplier ^ attempt)
                val backoff = (config.waitDurationMs * Math.pow(config.backoffMultiplier, attemptNumber.toDouble() - 1)).toLong()
                Math.min(backoff, config.maxWaitDurationMs)
            }
            .retryExceptions(
                java.io.IOException::class.java,
                java.sql.SQLException::class.java,
                java.util.concurrent.TimeoutException::class.java
            )
            .ignoreExceptions(
                IllegalArgumentException::class.java,
                IllegalStateException::class.java
            )
            .build()

        registry = RetryRegistry.of(retryConfig)

        logger.info { "RetryStrategyManager initialized with config: $config" }
    }

    /**
     * Get or create a retry policy for a specific operation
     */
    fun getRetryPolicy(name: String): Retry {
        return retryPolicies.computeIfAbsent(name) {
            registry.retry(name).also { retry ->
                // Register event listeners
                retry.eventPublisher
                    .onRetry { event ->
                        logger.warn {
                            "Retry '$name' attempt ${event.numberOfRetryAttempts}/${config.maxAttempts}: ${event.lastThrowable?.message}"
                        }
                    }
                    .onSuccess { event ->
                        if (event.numberOfRetryAttempts > 0) {
                            logger.info {
                                "Retry '$name' succeeded after ${event.numberOfRetryAttempts} attempts"
                            }
                        }
                    }
                    .onError { event ->
                        logger.error {
                            "Retry '$name' failed after ${event.numberOfRetryAttempts} attempts: ${event.lastThrowable?.message}"
                        }
                    }

                logger.info { "Created retry policy: $name" }
            }
        }
    }

    /**
     * Execute a suspending function with retry
     */
    suspend fun <T> executeWithRetry(
        name: String,
        block: suspend () -> T
    ): T {
        val retry = getRetryPolicy(name)
        return retry.executeSuspendFunction(block)
    }

    /**
     * Get retry metrics
     */
    fun getMetrics(name: String): RetryMetrics? {
        val retry = retryPolicies[name] ?: return null
        val metrics = retry.metrics

        return RetryMetrics(
            name = name,
            numberOfSuccessfulCallsWithoutRetryAttempt = metrics.numberOfSuccessfulCallsWithoutRetryAttempt,
            numberOfSuccessfulCallsWithRetryAttempt = metrics.numberOfSuccessfulCallsWithRetryAttempt,
            numberOfFailedCallsWithoutRetryAttempt = metrics.numberOfFailedCallsWithoutRetryAttempt,
            numberOfFailedCallsWithRetryAttempt = metrics.numberOfFailedCallsWithRetryAttempt
        )
    }

    /**
     * Get all retry metrics
     */
    fun getAllMetrics(): List<RetryMetrics> {
        return retryPolicies.keys.mapNotNull { getMetrics(it) }
    }
}

/**
 * Retry configuration
 */
data class RetryConfiguration(
    val maxAttempts: Int = 3,
    val waitDurationMs: Long = 1000, // Initial wait: 1 second
    val maxWaitDurationMs: Long = 30000, // Max wait: 30 seconds
    val backoffMultiplier: Double = 2.0 // Exponential backoff multiplier
)

/**
 * Retry metrics
 */
data class RetryMetrics(
    val name: String,
    val numberOfSuccessfulCallsWithoutRetryAttempt: Long,
    val numberOfSuccessfulCallsWithRetryAttempt: Long,
    val numberOfFailedCallsWithoutRetryAttempt: Long,
    val numberOfFailedCallsWithRetryAttempt: Long
) {
    val totalCalls: Long
        get() = numberOfSuccessfulCallsWithoutRetryAttempt +
                numberOfSuccessfulCallsWithRetryAttempt +
                numberOfFailedCallsWithoutRetryAttempt +
                numberOfFailedCallsWithRetryAttempt

    val successRate: Double
        get() {
            val total = totalCalls
            if (total == 0L) return 0.0
            val successful = numberOfSuccessfulCallsWithoutRetryAttempt + numberOfSuccessfulCallsWithRetryAttempt
            return (successful.toDouble() / total) * 100
        }
}

/**
 * Combined resilience manager that uses both circuit breaker and retry
 */
class ResilienceManager(
    private val circuitBreakerManager: CircuitBreakerManager = CircuitBreakerManager(),
    private val retryManager: RetryStrategyManager = RetryStrategyManager()
) {
    /**
     * Execute with both circuit breaker and retry protection
     */
    suspend fun <T> executeResilient(
        name: String,
        block: suspend () -> T
    ): T {
        // First apply circuit breaker, then retry
        return circuitBreakerManager.executeWithCircuitBreaker(name) {
            retryManager.executeWithRetry(name, block)
        }
    }

    /**
     * Get combined health status
     */
    fun getHealthStatus(): HealthStatus {
        val cbStates = circuitBreakerManager.getAllStates()
        val cbMetrics = circuitBreakerManager.getAllMetrics()
        val retryMetrics = retryManager.getAllMetrics()

        val openCircuits = cbStates.filter { it.value == io.github.resilience4j.circuitbreaker.CircuitBreaker.State.OPEN }

        return HealthStatus(
            healthy = openCircuits.isEmpty(),
            circuitBreakerStates = cbStates,
            circuitBreakerMetrics = cbMetrics,
            retryMetrics = retryMetrics,
            openCircuits = openCircuits.keys.toList()
        )
    }
}

/**
 * Overall health status
 */
data class HealthStatus(
    val healthy: Boolean,
    val circuitBreakerStates: Map<String, io.github.resilience4j.circuitbreaker.CircuitBreaker.State>,
    val circuitBreakerMetrics: List<CircuitBreakerMetrics>,
    val retryMetrics: List<RetryMetrics>,
    val openCircuits: List<String>
)
