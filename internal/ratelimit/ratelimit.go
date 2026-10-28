// Package ratelimit limits how many events each project may send.
//
// The algorithm is a token bucket per project. A bucket holds up to Burst
// tokens and refills at EventsPerSecond. A request costs one token per
// event and is accepted only if the whole cost fits; otherwise it is
// rejected and told how long until it would fit.
//
// Two implementations share that math:
//   - Redis: one bucket per project shared by every ingest instance, updated
//     by a Lua script so the read-modify-write is atomic.
//   - Local: an in-process bucket, used only while Redis is unreachable.
//
// FailOpen combines them: Redis normally, the local bucket during an outage.
package ratelimit

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sync"
	"time"
)

// Limit is a project's allowance.
type Limit struct {
	// EventsPerSecond is the long-run rate.
	EventsPerSecond float64 `json:"events_per_second"`
	// Burst is the bucket size: how many events can arrive at once after a
	// quiet period. It must be at least the largest request (100 events),
	// or a full batch could never be accepted.
	Burst int `json:"burst"`
}

// Validate checks that a request of maxCost events can ever be accepted.
func (l Limit) Validate(maxCost int) error {
	if l.EventsPerSecond <= 0 || math.IsInf(l.EventsPerSecond, 0) || math.IsNaN(l.EventsPerSecond) {
		return fmt.Errorf("events_per_second must be a positive number, got %v", l.EventsPerSecond)
	}
	if l.Burst < maxCost {
		return fmt.Errorf("burst must be at least %d (the largest batch), got %d", maxCost, l.Burst)
	}
	return nil
}

// Decision is the answer for one request.
type Decision struct {
	Allowed bool
	// RetryAfter is how long until the cost would fit. Zero when allowed.
	RetryAfter time.Duration
	// Degraded is true when Redis was unreachable and the local bucket decided.
	Degraded bool
}

// Limiter decides whether project may spend cost tokens now.
type Limiter interface {
	Allow(ctx context.Context, project string, limit Limit, cost int) (Decision, error)
}

// ErrCostTooHigh means the request can never fit in the bucket.
var ErrCostTooHigh = errors.New("ratelimit: cost exceeds burst")

// bucket is the state of one token bucket.
type bucket struct {
	tokens float64
	last   time.Time
}

// take refills b to now and tries to spend cost tokens. It is the same
// computation as the Lua script, kept side by side for review.

func (b *bucket) take(now time.Time, l Limit, cost int) Decision {
	if b.last.IsZero() {
		b.tokens, b.last = float64(l.Burst), now
	}

	if elapsed := now.Sub(b.last); elapsed > 0 {
		b.tokens = math.Min(float64(l.Burst), b.tokens+elapsed.Seconds()*l.EventsPerSecond)
		b.last = now
	}

	if b.tokens >= float64(cost) {
		b.tokens -= float64(cost)
		return Decision{Allowed: true}
	}

	missing := float64(cost) - b.tokens
	return Decision{RetryAfter: time.Duration(math.Ceil(missing / l.EventsPerSecond * float64(time.Second)))}
}

// Local is an in-process limiter. Each ingest instance has its own buckets,
// so N instances together allow N times the limit. That is acceptable for a
// fallback and the reason it is not the primary limiter.
type Local struct {
	mu      sync.Mutex
	buckets map[string]*bucket
	now     func() time.Time
}

// NewLocal creates an empty local limiter.
func NewLocal() *Local {
	return &Local{buckets: map[string]*bucket{}, now: time.Now}
}

// Allow implements Limiter. It never returns an error except for a cost
// that can never fit.
func (l *Local) Allow(_ context.Context, project string, limit Limit, cost int) (Decision, error) {
	if cost > limit.Burst {
		return Decision{}, ErrCostTooHigh
	}

	l.mu.Lock()
	defer l.mu.Unlock()
	b, ok := l.buckets[project]

	if !ok {
		b = &bucket{}
		l.buckets[project] = b
	}

	return b.take(l.now(), limit, cost), nil
}
