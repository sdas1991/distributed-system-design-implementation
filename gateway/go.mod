module github.com/shardstream/gateway

go 1.21

require (
	github.com/gofiber/fiber/v2 v2.52.0
	github.com/gofiber/contrib/fiberzap v1.0.2
	go.uber.org/zap v1.26.0
	github.com/redis/go-redis/v9 v9.4.0
	golang.org/x/time v0.5.0
	go.opentelemetry.io/otel v1.22.0
	go.opentelemetry.io/otel/sdk v1.22.0
	go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc v1.22.0
	go.opentelemetry.io/contrib/instrumentation/github.com/gofiber/fiber/otelfiber v0.47.0
)
