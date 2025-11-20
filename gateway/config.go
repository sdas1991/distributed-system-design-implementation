package main

import (
	"os"
	"strconv"
)

type Config struct {
	// Rate limiting
	RateLimitEnabled     bool
	RateLimitPerMinute   int
	RateLimitBurstSize   int
	RateLimitRedisURL    string

	// Backend services
	IngestServiceURLs   []string
	QueryServiceURLs    []string
	RealtimeServiceURLs []string

	// Load balancing
	LoadBalancingStrategy string // "round-robin", "least-connections", "random"
}

func LoadConfig() *Config {
	return &Config{
		RateLimitEnabled:   getEnvBool("RATE_LIMIT_ENABLED", true),
		RateLimitPerMinute: getEnvInt("RATE_LIMIT_PER_MINUTE", 1000),
		RateLimitBurstSize: getEnvInt("RATE_LIMIT_BURST_SIZE", 100),
		RateLimitRedisURL:  getEnv("REDIS_URL", "redis://localhost:6379"),

		IngestServiceURLs: []string{
			getEnv("INGEST_SERVICE_URL", "http://ingest-service:8081"),
		},
		QueryServiceURLs: []string{
			getEnv("QUERY_SERVICE_URL", "http://query-service:8082"),
		},
		RealtimeServiceURLs: []string{
			getEnv("REALTIME_SERVICE_URL", "http://realtime-service:8083"),
		},

		LoadBalancingStrategy: getEnv("LOAD_BALANCING_STRATEGY", "round-robin"),
	}
}

func getEnv(key, defaultValue string) string {
	value := os.Getenv(key)
	if value == "" {
		return defaultValue
	}
	return value
}

func getEnvInt(key string, defaultValue int) int {
	valueStr := os.Getenv(key)
	if valueStr == "" {
		return defaultValue
	}

	value, err := strconv.Atoi(valueStr)
	if err != nil {
		return defaultValue
	}

	return value
}

func getEnvBool(key string, defaultValue bool) bool {
	valueStr := os.Getenv(key)
	if valueStr == "" {
		return defaultValue
	}

	value, err := strconv.ParseBool(valueStr)
	if err != nil {
		return defaultValue
	}

	return value
}
