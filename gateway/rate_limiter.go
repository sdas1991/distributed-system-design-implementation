package main

import (
	"context"
	"fmt"
	"sync"
	"time"

	"golang.org/x/time/rate"
)

// RateLimiter implements token bucket rate limiting per customer
type RateLimiter struct {
	config   *Config
	limiters map[string]*rate.Limiter
	mu       sync.RWMutex
	metrics  *RateLimiterMetrics
}

type RateLimiterMetrics struct {
	TotalRequests  int64
	AllowedRequests int64
	BlockedRequests int64
	mu             sync.RWMutex
}

func NewRateLimiter(config *Config) *RateLimiter {
	return &RateLimiter{
		config:   config,
		limiters: make(map[string]*rate.Limiter),
		metrics:  &RateLimiterMetrics{},
	}
}

// Allow checks if a request should be allowed for a customer
func (rl *RateLimiter) Allow(ctx context.Context, customerID string) (bool, error) {
	if !rl.config.RateLimitEnabled {
		return true, nil
	}

	limiter := rl.getLimiter(customerID)

	rl.metrics.mu.Lock()
	rl.metrics.TotalRequests++
	rl.metrics.mu.Unlock()

	allowed := limiter.Allow()

	rl.metrics.mu.Lock()
	if allowed {
		rl.metrics.AllowedRequests++
	} else {
		rl.metrics.BlockedRequests++
	}
	rl.metrics.mu.Unlock()

	return allowed, nil
}

// getLimiter returns or creates a rate limiter for a customer
func (rl *RateLimiter) getLimiter(customerID string) *rate.Limiter {
	rl.mu.RLock()
	limiter, exists := rl.limiters[customerID]
	rl.mu.RUnlock()

	if exists {
		return limiter
	}

	rl.mu.Lock()
	defer rl.mu.Unlock()

	// Double-check after acquiring write lock
	limiter, exists = rl.limiters[customerID]
	if exists {
		return limiter
	}

	// Create new limiter with token bucket algorithm
	// Rate: tokens per minute / 60 = tokens per second
	ratePerSecond := rate.Limit(float64(rl.config.RateLimitPerMinute) / 60.0)
	limiter = rate.NewLimiter(ratePerSecond, rl.config.RateLimitBurstSize)

	rl.limiters[customerID] = limiter

	// Start cleanup goroutine for this customer (after 5 minutes of inactivity)
	go rl.cleanupLimiter(customerID, 5*time.Minute)

	return limiter
}

// cleanupLimiter removes inactive limiters to prevent memory leaks
func (rl *RateLimiter) cleanupLimiter(customerID string, timeout time.Duration) {
	time.Sleep(timeout)

	rl.mu.Lock()
	defer rl.mu.Unlock()

	delete(rl.limiters, customerID)
}

// GetMetrics returns current rate limiter metrics
func (rl *RateLimiter) GetMetrics() map[string]interface{} {
	rl.metrics.mu.RLock()
	defer rl.metrics.mu.RUnlock()

	rl.mu.RLock()
	activeLimiters := len(rl.limiters)
	rl.mu.RUnlock()

	var blockedRate float64
	if rl.metrics.TotalRequests > 0 {
		blockedRate = (float64(rl.metrics.BlockedRequests) / float64(rl.metrics.TotalRequests)) * 100
	}

	return map[string]interface{}{
		"total_requests":   rl.metrics.TotalRequests,
		"allowed_requests": rl.metrics.AllowedRequests,
		"blocked_requests": rl.metrics.BlockedRequests,
		"blocked_rate_pct": fmt.Sprintf("%.2f", blockedRate),
		"active_limiters":  activeLimiters,
	}
}
