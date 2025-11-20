package main

import (
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gofiber/fiber/v2"
	"go.uber.org/zap"
)

// LoadBalancer handles routing requests to backend services
type LoadBalancer struct {
	config  *Config
	metrics *LoadBalancerMetrics

	ingestIndex   uint64
	queryIndex    uint64
	realtimeIndex uint64

	httpClient *http.Client
}

type LoadBalancerMetrics struct {
	IngestRequests   int64
	QueryRequests    int64
	RealtimeRequests int64
	TotalErrors      int64
	mu               sync.RWMutex
}

func NewLoadBalancer(config *Config) *LoadBalancer {
	return &LoadBalancer{
		config:  config,
		metrics: &LoadBalancerMetrics{},
		httpClient: &http.Client{
			Timeout: 30 * time.Second,
			Transport: &http.Transport{
				MaxIdleConns:        100,
				MaxIdleConnsPerHost: 10,
				IdleConnTimeout:     90 * time.Second,
			},
		},
	}
}

// ProxyToIngest proxies requests to ingest service
func (lb *LoadBalancer) ProxyToIngest(c *fiber.Ctx) error {
	atomic.AddInt64(&lb.metrics.IngestRequests, 1)

	target := lb.selectBackend(lb.config.IngestServiceURLs, &lb.ingestIndex)
	return lb.proxy(c, target)
}

// ProxyToQuery proxies requests to query service
func (lb *LoadBalancer) ProxyToQuery(c *fiber.Ctx) error {
	atomic.AddInt64(&lb.metrics.QueryRequests, 1)

	target := lb.selectBackend(lb.config.QueryServiceURLs, &lb.queryIndex)
	return lb.proxy(c, target)
}

// ProxyToRealtime proxies WebSocket upgrade requests to realtime service
func (lb *LoadBalancer) ProxyToRealtime(c *fiber.Ctx) error {
	atomic.AddInt64(&lb.metrics.RealtimeRequests, 1)

	target := lb.selectBackend(lb.config.RealtimeServiceURLs, &lb.realtimeIndex)

	// For WebSocket, we need to redirect the client
	customerID := c.Params("customer_id")
	wsURL := fmt.Sprintf("%s/ws/%s", target, customerID)

	return c.Redirect(wsURL, fiber.StatusTemporaryRedirect)
}

// selectBackend selects a backend URL based on load balancing strategy
func (lb *LoadBalancer) selectBackend(backends []string, index *uint64) string {
	if len(backends) == 0 {
		return ""
	}

	if len(backends) == 1 {
		return backends[0]
	}

	switch lb.config.LoadBalancingStrategy {
	case "round-robin":
		idx := atomic.AddUint64(index, 1)
		return backends[idx%uint64(len(backends))]

	case "random":
		return backends[rand.Intn(len(backends))]

	default:
		// Default to round-robin
		idx := atomic.AddUint64(index, 1)
		return backends[idx%uint64(len(backends))]
	}
}

// proxy forwards the request to the target backend
func (lb *LoadBalancer) proxy(c *fiber.Ctx, targetURL string) error {
	if targetURL == "" {
		atomic.AddInt64(&lb.metrics.TotalErrors, 1)
		return c.Status(fiber.StatusServiceUnavailable).JSON(fiber.Map{
			"error": "No backend services available",
		})
	}

	// Build target URL
	fullURL := targetURL + string(c.Request().URI().Path())
	if len(c.Request().URI().QueryString()) > 0 {
		fullURL += "?" + string(c.Request().URI().QueryString())
	}

	// Create request
	req, err := http.NewRequest(
		c.Method(),
		fullURL,
		nil,
	)
	if err != nil {
		atomic.AddInt64(&lb.metrics.TotalErrors, 1)
		zapLogger.Error("Failed to create request", zap.Error(err))
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"error": "Failed to proxy request",
		})
	}

	// Copy headers
	c.Request().Header.VisitAll(func(key, value []byte) {
		req.Header.Add(string(key), string(value))
	})

	// Copy body for POST/PUT requests
	if c.Method() == "POST" || c.Method() == "PUT" {
		req.Body = io.NopCloser(c.Request().BodyStream())
	}

	// Execute request
	resp, err := lb.httpClient.Do(req)
	if err != nil {
		atomic.AddInt64(&lb.metrics.TotalErrors, 1)
		zapLogger.Error("Backend request failed",
			zap.String("url", fullURL),
			zap.Error(err))
		return c.Status(fiber.StatusBadGateway).JSON(fiber.Map{
			"error": "Backend service unavailable",
		})
	}
	defer resp.Body.Close()

	// Copy response headers
	for key, values := range resp.Header {
		for _, value := range values {
			c.Set(key, value)
		}
	}

	// Set status code
	c.Status(resp.StatusCode)

	// Copy response body
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		atomic.AddInt64(&lb.metrics.TotalErrors, 1)
		zapLogger.Error("Failed to read response", zap.Error(err))
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"error": "Failed to read backend response",
		})
	}

	return c.Send(body)
}

// GetMetrics returns load balancer metrics
func (lb *LoadBalancer) GetMetrics() map[string]interface{} {
	return map[string]interface{}{
		"ingest_requests":   atomic.LoadInt64(&lb.metrics.IngestRequests),
		"query_requests":    atomic.LoadInt64(&lb.metrics.QueryRequests),
		"realtime_requests": atomic.LoadInt64(&lb.metrics.RealtimeRequests),
		"total_errors":      atomic.LoadInt64(&lb.metrics.TotalErrors),
		"strategy":          lb.config.LoadBalancingStrategy,
	}
}
