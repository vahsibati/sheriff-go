package sheriff

import (
	"fmt"
	"math"
	"sync"
	"time"
)

// Config holds configuration parameters for the RateLimiter.
type Config struct {
	Capacity        float64
	RefillRate      float64
	MaxRequests     int
	Period          time.Duration
	CleanupInterval time.Duration
}

// RateLimiter tracks individual token buckets and manages memory cleanup.
type RateLimiter struct {
	capacity        float64
	refillRate      float64
	cleanupInterval time.Duration

	mu          sync.RWMutex
	buckets     map[string]*tokenBucket
	lastCleanup time.Time
	now         func() time.Time // clock function for testing
}

// tokenBucket represents a single token bucket.
type tokenBucket struct {
	capacity    float64
	refillRate  float64
	tokens      float64
	lastUpdated time.Time
	mu          sync.Mutex
	now         func() time.Time // clock function for testing
}

// New creates a new RateLimiter instance.
func New(cfg Config) (*RateLimiter, error) {
	if cfg.MaxRequests < 0 {
		return nil, fmt.Errorf("max requests cannot be negative")
	}
	if cfg.Period < 0 {
		return nil, fmt.Errorf("period cannot be negative")
	}
	if cfg.Capacity < 0 {
		return nil, fmt.Errorf("capacity cannot be negative")
	}
	if cfg.RefillRate < 0 {
		return nil, fmt.Errorf("refill rate cannot be negative")
	}
	if cfg.CleanupInterval < 0 {
		return nil, fmt.Errorf("cleanup interval cannot be negative")
	}

	// Map MaxRequests and Period to Capacity and RefillRate if provided
	if cfg.MaxRequests > 0 {
		cfg.Capacity = float64(cfg.MaxRequests)
		if cfg.Period > 0 {
			cfg.RefillRate = cfg.Capacity / cfg.Period.Seconds()
		} else {
			cfg.RefillRate = cfg.Capacity
		}
	}

	// Apply default values if zero
	if cfg.Capacity == 0 {
		cfg.Capacity = 10.0
	}
	if cfg.RefillRate == 0 {
		cfg.RefillRate = 1.0
	}
	if cfg.CleanupInterval == 0 {
		cfg.CleanupInterval = 60 * time.Second
	}

	rl := &RateLimiter{
		capacity:        cfg.Capacity,
		refillRate:      cfg.RefillRate,
		cleanupInterval: cfg.CleanupInterval,
		buckets:         make(map[string]*tokenBucket),
		now:             time.Now,
	}
	rl.lastCleanup = rl.now()
	return rl, nil
}

// getBucket retrieves or creates a tokenBucket for the given key in a thread-safe manner.
func (l *RateLimiter) getBucket(key string) *tokenBucket {
	l.mu.RLock()
	now := l.now()
	needCleanup := now.Sub(l.lastCleanup) >= l.cleanupInterval
	bucket, exists := l.buckets[key]
	l.mu.RUnlock()

	if !exists || needCleanup {
		l.mu.Lock()
		// Double-check under write lock
		now = l.now()
		if now.Sub(l.lastCleanup) >= l.cleanupInterval {
			l.cleanup(now)
			l.lastCleanup = now
		}
		bucket, exists = l.buckets[key]
		if !exists {
			bucket = &tokenBucket{
				capacity:    l.capacity,
				refillRate:  l.refillRate,
				tokens:      l.capacity,
				lastUpdated: now,
				now:         l.now,
			}
			l.buckets[key] = bucket
		}
		l.mu.Unlock()
	}

	return bucket
}

// cleanup removes fully replenished buckets from memory. Must be called under write lock.
func (l *RateLimiter) cleanup(now time.Time) {
	for k, bucket := range l.buckets {
		if bucket.isFull(now) {
			delete(l.buckets, k)
		}
	}
}

// consume consumes tokens from a bucket.
func (b *tokenBucket) consume(tokens float64) (bool, time.Duration) {
	b.mu.Lock()
	defer b.mu.Unlock()

	now := b.now()
	elapsed := now.Sub(b.lastUpdated)
	if elapsed > 0 {
		b.tokens = math.Min(b.capacity, b.tokens+elapsed.Seconds()*b.refillRate)
		b.lastUpdated = now
	}

	if b.tokens >= tokens {
		b.tokens -= tokens
		return true, 0
	}

	needed := tokens - b.tokens
	retryAfterSecs := needed / b.refillRate
	retryAfter := time.Duration(retryAfterSecs * float64(time.Second))
	return false, retryAfter
}

// getTokens returns the current tokens after lazy replenishment.
func (b *tokenBucket) getTokens() float64 {
	b.mu.Lock()
	defer b.mu.Unlock()

	now := b.now()
	elapsed := now.Sub(b.lastUpdated)
	if elapsed > 0 {
		b.tokens = math.Min(b.capacity, b.tokens+elapsed.Seconds()*b.refillRate)
		b.lastUpdated = now
	}
	return b.tokens
}

// reset resets the bucket to full capacity.
func (b *tokenBucket) reset() {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.tokens = b.capacity
	b.lastUpdated = b.now()
}

// isFull checks if the bucket is fully replenished without mutating state.
func (b *tokenBucket) isFull(now time.Time) bool {
	b.mu.Lock()
	defer b.mu.Unlock()

	elapsed := now.Sub(b.lastUpdated)
	if elapsed > 0 {
		currentTokens := math.Min(b.capacity, b.tokens+elapsed.Seconds()*b.refillRate)
		return currentTokens >= b.capacity
	}
	return b.tokens >= b.capacity
}

// IsAllowed checks if the request is allowed under the rate limit for a key (using 1 token).
func (l *RateLimiter) IsAllowed(key string) bool {
	return l.IsAllowedN(key, 1.0)
}

// IsAllowedN checks if the request is allowed under the rate limit for a key with custom tokens.
func (l *RateLimiter) IsAllowedN(key string, tokens float64) bool {
	allowed, _ := l.ConsumeN(key, tokens)
	return allowed
}

// Check checks if the request is allowed, returning a RateLimitExceededError if not (using 1 token).
func (l *RateLimiter) Check(key string) error {
	return l.CheckN(key, 1.0)
}

// CheckN checks if the request is allowed with custom tokens, returning a RateLimitExceededError if not.
func (l *RateLimiter) CheckN(key string, tokens float64) error {
	allowed, retryAfter := l.ConsumeN(key, tokens)
	if !allowed {
		return &RateLimitExceededError{
			Message:    fmt.Sprintf("rate limit exceeded for key: %s", key),
			RetryAfter: retryAfter,
		}
	}
	return nil
}

// Consume consumes 1 token for the key, returning allowed status and retry after duration.
func (l *RateLimiter) Consume(key string) (bool, time.Duration) {
	return l.ConsumeN(key, 1.0)
}

// ConsumeN consumes custom tokens for the key, returning allowed status and retry after duration.
func (l *RateLimiter) ConsumeN(key string, tokens float64) (bool, time.Duration) {
	bucket := l.getBucket(key)
	return bucket.consume(tokens)
}

// GetTokens returns the currently available tokens for the key.
func (l *RateLimiter) GetTokens(key string) float64 {
	bucket := l.getBucket(key)
	return bucket.getTokens()
}

// Reset resets the rate limit bucket for a specific key.
func (l *RateLimiter) Reset(key string) {
	l.mu.RLock()
	bucket, exists := l.buckets[key]
	l.mu.RUnlock()

	if exists {
		bucket.reset()
	}
}

// ResetAll resets all rate limit buckets.
func (l *RateLimiter) ResetAll() {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.buckets = make(map[string]*tokenBucket)
}
