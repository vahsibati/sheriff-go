package sheriff

import (
	"errors"
	"sync"
	"testing"
	"time"
)

func TestNewRateLimiter_Validation(t *testing.T) {
	tests := []struct {
		name    string
		config  Config
		wantErr bool
	}{
		{"Negative MaxRequests", Config{MaxRequests: -1}, true},
		{"Negative Period", Config{Period: -1}, true},
		{"Negative Capacity", Config{Capacity: -1}, true},
		{"Negative RefillRate", Config{RefillRate: -1}, true},
		{"Negative CleanupInterval", Config{CleanupInterval: -1}, true},
		{"Valid Custom Config", Config{Capacity: 10, RefillRate: 1}, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := New(tt.config)
			if (err != nil) != tt.wantErr {
				t.Errorf("New() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestRateLimiter_Defaults(t *testing.T) {
	limiter, err := New(Config{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if limiter.capacity != 10.0 {
		t.Errorf("expected default capacity 10.0, got %v", limiter.capacity)
	}
	if limiter.refillRate != 1.0 {
		t.Errorf("expected default refillRate 1.0, got %v", limiter.refillRate)
	}
	if limiter.cleanupInterval != 60*time.Second {
		t.Errorf("expected default cleanupInterval 60s, got %v", limiter.cleanupInterval)
	}
}

func TestRateLimiter_MaxRequests(t *testing.T) {
	limiter, err := New(Config{MaxRequests: 100, Period: 60 * time.Second})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if limiter.capacity != 100.0 {
		t.Errorf("expected capacity 100.0, got %v", limiter.capacity)
	}
	expectedRefill := 100.0 / 60.0
	if limiter.refillRate != expectedRefill {
		t.Errorf("expected refillRate %v, got %v", expectedRefill, limiter.refillRate)
	}
}

func TestRateLimiter_IsAllowed(t *testing.T) {
	limiter, _ := New(Config{Capacity: 5, RefillRate: 1})

	// Consume 5 tokens
	for i := 0; i < 5; i++ {
		if !limiter.IsAllowed("user-1") {
			t.Errorf("expected request %d to be allowed", i+1)
		}
	}

	// 6th request should fail
	if limiter.IsAllowed("user-1") {
		t.Errorf("expected 6th request to be denied")
	}
}

func TestRateLimiter_IsAllowedN(t *testing.T) {
	limiter, _ := New(Config{Capacity: 10, RefillRate: 1})

	if !limiter.IsAllowedN("user-1", 5.0) {
		t.Errorf("expected 5 tokens consumption to be allowed")
	}

	if !limiter.IsAllowedN("user-1", 5.0) {
		t.Errorf("expected subsequent 5 tokens consumption to be allowed")
	}

	if limiter.IsAllowedN("user-1", 1.0) {
		t.Errorf("expected further consumption to be denied")
	}
}

func TestRateLimiter_Check(t *testing.T) {
	limiter, _ := New(Config{Capacity: 2, RefillRate: 1})

	if err := limiter.Check("user-1"); err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if err := limiter.Check("user-1"); err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	err := limiter.Check("user-1")
	if err == nil {
		t.Fatalf("expected error, got nil")
	}

	var limitErr *RateLimitExceededError
	if !errors.As(err, &limitErr) {
		t.Fatalf("expected RateLimitExceededError, got %T", err)
	}

	if limitErr.RetryAfter <= 0 {
		t.Errorf("expected positive RetryAfter, got %v", limitErr.RetryAfter)
	}
}

func TestRateLimiter_Consume(t *testing.T) {
	limiter, _ := New(Config{Capacity: 1, RefillRate: 0.5})

	var mockTime time.Time
	limiter.now = func() time.Time {
		return mockTime
	}
	mockTime = time.Unix(100, 0)

	allowed, retryAfter := limiter.Consume("user-1")
	if !allowed || retryAfter != 0 {
		t.Errorf("expected allowed=true and retryAfter=0, got (%v, %v)", allowed, retryAfter)
	}

	allowed, retryAfter = limiter.Consume("user-1")
	if allowed || retryAfter != 2*time.Second {
		t.Errorf("expected allowed=false and retryAfter=2s, got (%v, %v)", allowed, retryAfter)
	}
}

func TestRateLimiter_GetTokens(t *testing.T) {
	limiter, _ := New(Config{Capacity: 5, RefillRate: 1})

	var mockTime time.Time
	limiter.now = func() time.Time {
		return mockTime
	}
	mockTime = time.Unix(100, 0)

	if tokens := limiter.GetTokens("user-1"); tokens != 5.0 {
		t.Errorf("expected 5.0 tokens, got %v", tokens)
	}

	limiter.Consume("user-1")
	if tokens := limiter.GetTokens("user-1"); tokens != 4.0 {
		t.Errorf("expected 4.0 tokens, got %v", tokens)
	}
}

func TestRateLimiter_Reset(t *testing.T) {
	limiter, _ := New(Config{Capacity: 2, RefillRate: 1})

	limiter.ConsumeN("user-1", 2.0)
	if limiter.IsAllowed("user-1") {
		t.Errorf("expected denied before reset")
	}

	limiter.Reset("user-1")
	if !limiter.IsAllowed("user-1") {
		t.Errorf("expected allowed after reset")
	}
}

func TestRateLimiter_ResetAll(t *testing.T) {
	limiter, _ := New(Config{Capacity: 2, RefillRate: 1})

	limiter.ConsumeN("user-1", 2.0)
	limiter.ConsumeN("user-2", 2.0)

	limiter.ResetAll()

	if !limiter.IsAllowed("user-1") {
		t.Errorf("expected user-1 to be allowed after ResetAll")
	}
	if !limiter.IsAllowed("user-2") {
		t.Errorf("expected user-2 to be allowed after ResetAll")
	}
}

func TestRateLimiter_TokenReplenishment(t *testing.T) {
	limiter, _ := New(Config{Capacity: 5, RefillRate: 2.0})

	// Setup custom mocked clock
	var mockTime time.Time
	limiter.now = func() time.Time {
		return mockTime
	}

	// Set initial time
	mockTime = time.Unix(100, 0)
	limiter.lastCleanup = mockTime // synchronize last cleanup time to avoid sweep triggers

	// Consume all 5 tokens
	if allowed, _ := limiter.ConsumeN("user-2", 5.0); !allowed {
		t.Fatalf("expected consumption of 5 tokens to succeed")
	}

	if limiter.IsAllowed("user-2") {
		t.Fatalf("expected further requests to be denied")
	}

	// Fast-forward mock time by 1.5 seconds.
	// We should replenish 1.5 * 2 = 3.0 tokens
	mockTime = mockTime.Add(1500 * time.Millisecond)

	if allowed, _ := limiter.ConsumeN("user-2", 3.0); !allowed {
		t.Errorf("expected to consume replenished 3 tokens")
	}

	if limiter.IsAllowed("user-2") {
		t.Errorf("expected further requests to be denied")
	}

	// Fast-forward another 2.5 seconds.
	// We should replenish 2.5 * 2 = 5.0 tokens (maxes out at capacity 5)
	mockTime = mockTime.Add(2500 * time.Millisecond)

	if allowed, _ := limiter.ConsumeN("user-2", 5.0); !allowed {
		t.Errorf("expected to consume replenished 5 tokens")
	}
}

func TestRateLimiter_MemoryCleanup(t *testing.T) {
	limiter, _ := New(Config{Capacity: 5, RefillRate: 1.0, CleanupInterval: 10 * time.Second})

	var mockTime time.Time
	limiter.now = func() time.Time {
		return mockTime
	}

	mockTime = time.Unix(100, 0)
	limiter.lastCleanup = mockTime

	// Add key1 and consume tokens to make it not full
	limiter.ConsumeN("key1", 2.0)
	// Add key2 and consume nothing (fully replenished)
	limiter.ConsumeN("key2", 0)

	// Since we did not advance time beyond cleanup interval, buckets should exist
	limiter.getBucket("key2")
	if len(limiter.buckets) != 2 {
		t.Errorf("expected 2 buckets, got %d", len(limiter.buckets))
	}

	// Advance time past the cleanup interval (11 seconds)
	mockTime = mockTime.Add(11 * time.Second)

	// Fetch another key to trigger lazy cleanup
	limiter.ConsumeN("key3", 0.0)

	// key2 is full, so it should be deleted.
	// key1 was missing 2 tokens, needs 2 seconds to refill. Since 11s elapsed, it is also fully replenished and should be deleted.
	// key3 was just accessed/added, so it is present.
	limiter.mu.RLock()
	_, key1Exists := limiter.buckets["key1"]
	_, key2Exists := limiter.buckets["key2"]
	_, key3Exists := limiter.buckets["key3"]
	limiter.mu.RUnlock()

	if key1Exists {
		t.Errorf("expected key1 to be cleaned up")
	}
	if key2Exists {
		t.Errorf("expected key2 to be cleaned up")
	}
	if !key3Exists {
		t.Errorf("expected key3 to exist")
	}
}

func TestRateLimiter_Concurrency(t *testing.T) {
	limiter, _ := New(Config{Capacity: 50.0, RefillRate: 0.0001})

	var wg sync.WaitGroup
	workers := 10
	iterations := 10
	successCount := 0
	var mu sync.Mutex

	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < iterations; j++ {
				allowed, _ := limiter.Consume("concurrent-key")
				if allowed {
					mu.Lock()
					successCount++
					mu.Unlock()
				}
			}
		}()
	}

	wg.Wait()

	// With capacity 50 and negligible refill, exactly 50 requests should succeed out of 100
	if successCount != 50 {
		t.Errorf("expected exactly 50 successful requests, got %d", successCount)
	}
}
