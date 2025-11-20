package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/gofiber/fiber/v2/middleware/cors"
	"github.com/gofiber/fiber/v2/middleware/logger"
	"github.com/gofiber/fiber/v2/middleware/recover"
	"go.uber.org/zap"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.21.0"
	"go.opentelemetry.io/contrib/instrumentation/github.com/gofiber/fiber/otelfiber"
)

var (
	zapLogger *zap.Logger
)

func setupTracing(ctx context.Context) (*sdktrace.TracerProvider, error) {
	jaegerEndpoint := os.Getenv("JAEGER_ENDPOINT")
	if jaegerEndpoint == "" {
		jaegerEndpoint = "jaeger:4317"
	}

	exporter, err := otlptracegrpc.New(
		ctx,
		otlptracegrpc.WithEndpoint(jaegerEndpoint),
		otlptracegrpc.WithInsecure(),
	)
	if err != nil {
		return nil, err
	}

	res, err := resource.New(ctx,
		resource.WithAttributes(semconv.ServiceName("gateway-service")),
	)
	if err != nil {
		return nil, err
	}

	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exporter),
		sdktrace.WithResource(res),
	)

	otel.SetTracerProvider(tp)
	return tp, nil
}

func main() {
	// Initialize logger
	var err error
	zapLogger, err = zap.NewProduction()
	if err != nil {
		log.Fatalf("Failed to initialize logger: %v", err)
	}
	defer zapLogger.Sync()

	zapLogger.Info("Starting ShardStream API Gateway...")

	// Setup OpenTelemetry tracing
	ctx := context.Background()
	tp, err := setupTracing(ctx)
	if err != nil {
		zapLogger.Warn("Failed to setup tracing", zap.Error(err))
	} else {
		zapLogger.Info("OpenTelemetry tracing enabled")
		defer func() {
			if err := tp.Shutdown(ctx); err != nil {
				zapLogger.Error("Error shutting down tracer provider", zap.Error(err))
			}
		}()
	}

	// Initialize configuration
	config := LoadConfig()

	// Initialize services
	rateLimiter := NewRateLimiter(config)
	loadBalancer := NewLoadBalancer(config)

	// Create Fiber app
	app := fiber.New(fiber.Config{
		ServerHeader: "ShardStream-Gateway",
		AppName:      "ShardStream API Gateway v1.0.0",
	})

	// Middleware
	app.Use(recover.New())
	app.Use(otelfiber.Middleware())
	app.Use(logger.New(logger.Config{
		Format: "${time} ${status} - ${method} ${path} ${latency}\n",
	}))
	app.Use(cors.New(cors.Config{
		AllowOrigins: "*",
		AllowMethods: "GET,POST,PUT,DELETE,OPTIONS",
		AllowHeaders: "Origin, Content-Type, Accept, Authorization",
	}))

	// Custom rate limiting middleware
	app.Use(func(c *fiber.Ctx) error {
		// Extract customer ID from header or query param
		customerID := c.Get("X-Customer-ID")
		if customerID == "" {
			customerID = c.Query("customer_id", "anonymous")
		}

		// Check rate limit
		allowed, err := rateLimiter.Allow(c.Context(), customerID)
		if err != nil {
			zapLogger.Error("Rate limiter error", zap.Error(err))
			return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
				"error": "Internal server error",
			})
		}

		if !allowed {
			return c.Status(fiber.StatusTooManyRequests).JSON(fiber.Map{
				"error": "Rate limit exceeded",
			})
		}

		return c.Next()
	})

	// Health check
	app.Get("/health", func(c *fiber.Ctx) error {
		return c.JSON(fiber.Map{
			"status":    "healthy",
			"service":   "api-gateway",
			"timestamp": time.Now().Unix(),
		})
	})

	// Route: Ingest Service (POST)
	app.Post("/api/v1/ingest", func(c *fiber.Ctx) error {
		return loadBalancer.ProxyToIngest(c)
	})

	app.Post("/api/v1/ingest/batch", func(c *fiber.Ctx) error {
		return loadBalancer.ProxyToIngest(c)
	})

	// Route: Query Service (GET)
	app.Get("/api/v1/query/*", func(c *fiber.Ctx) error {
		return loadBalancer.ProxyToQuery(c)
	})

	// Route: Realtime WebSocket (upgrade)
	app.Get("/ws/:customer_id", func(c *fiber.Ctx) error {
		return loadBalancer.ProxyToRealtime(c)
	})

	// Metrics endpoint
	app.Get("/metrics", func(c *fiber.Ctx) error {
		metrics := rateLimiter.GetMetrics()
		lbMetrics := loadBalancer.GetMetrics()

		return c.JSON(fiber.Map{
			"rate_limiter":  metrics,
			"load_balancer": lbMetrics,
		})
	})

	// Start server
	port := os.Getenv("GATEWAY_PORT")
	if port == "" {
		port = "8080"
	}

	// Graceful shutdown
	go func() {
		if err := app.Listen(":" + port); err != nil {
			zapLogger.Fatal("Failed to start server", zap.Error(err))
		}
	}()

	zapLogger.Info("API Gateway started", zap.String("port", port))

	// Wait for interrupt signal
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	zapLogger.Info("Shutting down API Gateway...")

	// Graceful shutdown with timeout
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := app.ShutdownWithContext(ctx); err != nil {
		zapLogger.Error("Server forced to shutdown", zap.Error(err))
	}

	zapLogger.Info("API Gateway stopped")
}
