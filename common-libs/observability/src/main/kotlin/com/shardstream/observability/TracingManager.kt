package com.shardstream.observability

import io.github.oshai.kotlinlogging.KotlinLogging
import io.opentelemetry.api.OpenTelemetry
import io.opentelemetry.api.trace.Span
import io.opentelemetry.api.trace.StatusCode
import io.opentelemetry.api.trace.Tracer
import io.opentelemetry.api.trace.propagation.W3CTraceContextPropagator
import io.opentelemetry.context.Context
import io.opentelemetry.context.propagation.ContextPropagators
import io.opentelemetry.exporter.otlp.trace.OtlpGrpcSpanExporter
import io.opentelemetry.sdk.OpenTelemetrySdk
import io.opentelemetry.sdk.resources.Resource
import io.opentelemetry.sdk.trace.SdkTracerProvider
import io.opentelemetry.sdk.trace.export.BatchSpanProcessor
import io.opentelemetry.semconv.ResourceAttributes
import kotlinx.coroutines.withContext
import kotlin.coroutines.CoroutineContext
import kotlin.coroutines.coroutineContext

private val logger = KotlinLogging.logger {}

/**
 * Tracing Manager - OpenTelemetry distributed tracing
 *
 * Features:
 * - Automatic trace context propagation
 * - Span creation and management
 * - Export to Jaeger via OTLP
 * - Coroutine-aware tracing
 */
class TracingManager(
    private val serviceName: String,
    private val jaegerEndpoint: String = System.getenv("JAEGER_ENDPOINT") ?: "http://jaeger:4317"
) {
    private val openTelemetry: OpenTelemetry
    val tracer: Tracer

    init {
        // Configure OTLP exporter to Jaeger
        val otlpExporter = OtlpGrpcSpanExporter.builder()
            .setEndpoint(jaegerEndpoint)
            .build()

        // Create resource with service name
        val resource = Resource.getDefault()
            .merge(Resource.create(
                io.opentelemetry.api.common.Attributes.of(
                    ResourceAttributes.SERVICE_NAME, serviceName
                )
            ))

        // Create tracer provider
        val sdkTracerProvider = SdkTracerProvider.builder()
            .addSpanProcessor(BatchSpanProcessor.builder(otlpExporter).build())
            .setResource(resource)
            .build()

        // Build OpenTelemetry SDK
        openTelemetry = OpenTelemetrySdk.builder()
            .setTracerProvider(sdkTracerProvider)
            .setPropagators(ContextPropagators.create(W3CTraceContextPropagator.getInstance()))
            .buildAndRegisterGlobal()

        tracer = openTelemetry.getTracer(serviceName)

        logger.info { "OpenTelemetry tracing initialized for $serviceName -> $jaegerEndpoint" }
    }

    /**
     * Create a new span
     */
    fun createSpan(name: String): Span {
        return tracer.spanBuilder(name)
            .setParent(Context.current())
            .startSpan()
    }

    /**
     * Execute code block with a span (automatically closes span)
     */
    inline fun <T> withSpan(name: String, crossinline block: (Span) -> T): T {
        val span = createSpan(name)
        return try {
            span.makeCurrent().use {
                val result = block(span)
                span.setStatus(StatusCode.OK)
                result
            }
        } catch (e: Exception) {
            span.recordException(e)
            span.setStatus(StatusCode.ERROR, e.message ?: "Unknown error")
            throw e
        } finally {
            span.end()
        }
    }

    /**
     * Execute suspending code block with a span (coroutine-aware)
     */
    suspend inline fun <T> withSpanSuspend(name: String, crossinline block: suspend (Span) -> T): T {
        val span = createSpan(name)
        return try {
            span.makeCurrent().use {
                val result = block(span)
                span.setStatus(StatusCode.OK)
                result
            }
        } catch (e: Exception) {
            span.recordException(e)
            span.setStatus(StatusCode.ERROR, e.message ?: "Unknown error")
            throw e
        } finally {
            span.end()
        }
    }

    /**
     * Add attribute to current span
     */
    fun addAttribute(key: String, value: String) {
        Span.current().setAttribute(key, value)
    }

    fun addAttribute(key: String, value: Long) {
        Span.current().setAttribute(key, value)
    }

    fun addAttribute(key: String, value: Boolean) {
        Span.current().setAttribute(key, value)
    }

    /**
     * Add event to current span
     */
    fun addEvent(name: String) {
        Span.current().addEvent(name)
    }

    /**
     * Record exception in current span
     */
    fun recordException(exception: Exception) {
        Span.current().recordException(exception)
    }

    /**
     * Get current span
     */
    fun currentSpan(): Span {
        return Span.current()
    }

    /**
     * Shutdown tracing (flush all spans)
     */
    fun shutdown() {
        (openTelemetry as? OpenTelemetrySdk)?.sdkTracerProvider?.close()
        logger.info { "Tracing shut down for $serviceName" }
    }
}
