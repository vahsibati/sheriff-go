package sheriff

import (
	"fmt"
	"time"
)

// RateLimitExceededError is raised when a rate limit is exceeded.
type RateLimitExceededError struct {
	Message    string
	RetryAfter time.Duration
}

// Error implements the built-in error interface.
func (e *RateLimitExceededError) Error() string {
	if e.Message != "" {
		return e.Message
	}
	return fmt.Sprintf("rate limit exceeded, retry after %v", e.RetryAfter)
}
