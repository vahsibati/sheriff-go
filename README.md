# sheriff-go

An elegant, thread-safe, in-memory Token Bucket rate limiter for Go with **zero external dependencies**.

It is a direct Go port of the Python `sheriff` rate limiter, optimized for high-concurrency production Go applications.

## Features

- **Zero Dependencies**: Core library depends only on Go standard library.
- **Thread-safe & Performant**: Employs fine-grained locks per bucket and a highly concurrent optimistic read-lock path for lookups to minimize lock contention.
- **Lazy memory cleanup**: Automatically sweeps full/inactive buckets from memory at configurable intervals, preventing memory leaks when rate limiting a large amount of unique keys (e.g. IPs).
- **Flexible Configuration**: Supports custom capacity & refill rates, or simple limits defined as `MaxRequests` over a `Period`.

## Installation

```bash
go get github.com/vahsibati/sheriff-go
```

## Quick Start

```go
package main

import (
	"fmt"
	"time"

	"github.com/vahsibati/sheriff-go"
)

func main() {
	// Configure limiter: 100 requests per minute with 60-second lazy cleanup sweeps
	limiter, err := sheriff.New(sheriff.Config{
		MaxRequests:     100,
		Period:          60 * time.Second,
		CleanupInterval: 60 * time.Second,
	})
	if err != nil {
		panic(err)
	}

	key := "user-ip-address"

	// Check if request is allowed (consumes 1 token)
	if limiter.IsAllowed(key) {
		fmt.Println("Request allowed!")
	} else {
		fmt.Println("Rate limit exceeded")
	}

	// Or consume and get detailed retry-after duration
	allowed, retryAfter := limiter.Consume(key)
	if !allowed {
		fmt.Printf("Rate limited. Please retry after %v\n", retryAfter)
	}
}
```

## Integration with Gin Framework

Since `sheriff-go` is zero-dependency, you can easily write your own Gin middleware using the public API. Here is a copy-pasteable implementation:

```go
package middleware

import (
	"math"
	"net/http"
	"strconv"

	"github.com/gin-gonic/gin"
	"github.com/vahsibati/sheriff-go"
)

// RateLimiter creates a Gin middleware handler for rate limiting.
func RateLimiter(limiter *sheriff.RateLimiter) gin.HandlerFunc {
	return func(c *gin.Context) {
		// Use client IP as rate limit key
		key := c.ClientIP()

		allowed, retryAfter := limiter.Consume(key)
		if !allowed {
			// Set the Retry-After header in seconds (rounded up)
			retrySeconds := int(math.Ceil(retryAfter.Seconds()))
			c.Header("Retry-After", strconv.Itoa(retrySeconds))

			// Return HTTP 429 Too Many Requests
			c.AbortWithStatusJSON(http.StatusTooManyRequests, gin.H{
				"error": "Too many requests. Please slow down.",
			})
			return
		}

		c.Next()
	}
}
```

You can then register this middleware in your Gin router:

```go
r := gin.Default()
r.Use(RateLimiter(limiter))
```

## License

MIT
