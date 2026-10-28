package ratelimit

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"
)

// FailOpen asks the shared limiter first and falls back to a local one when
// the shared limiter fails.
//
// Why open and not closed: when Redis is down, failing closed would reject
// every event of every project, so an outage of a helper service would lose
// exactly the error reports that explain the outage. Failing open with a
// local bucket still stops a flooding project; the cost is that N ingest
// instances together allow up to N times the limit until Redis is back.
//
// After a failure, the shared limiter is skipped for Cooldown, so requests
// don't each wait for a Redis timeout while it is down.
type FailOpen struct {
	Shared Limiter
	Local  Limiter
	// Timeout bounds each call to the shared limiter. Default 50ms.
	Timeout time.Duration
	// Cooldown is how long to skip the shared limiter after it fails.
	// Default 1s.
	Cooldown time.Duration
	Log      *slog.Logger

	mu        sync.Mutex
	skipUntil time.Time
	degraded  atomic.Uint64 // decisions made by the local limiter
	failures  atomic.Uint64 // failed calls to the shared limiter
}

// Allow implements Limiter. It returns an error only for a cost that can
// never fit; every other failure falls back to the local limiter.
func (f *FailOpen) Allow(ctx context.Context, project string, limit Limit, cost int) (Decision, error) {
	if !f.skipping() {
		timeout := f.Timeout
		if timeout <= 0 {
			timeout = 50 * time.Millisecond
		}
		sctx, cancel := context.WithTimeout(ctx, timeout)
		d, err := f.Shared.Allow(sctx, project, limit, cost)
		cancel()

		if err == nil || errors.Is(err, ErrCostTooHigh) {
			return d, err
		}

		f.failures.Add(1)
		f.startSkipping(err)
	}

	d, err := f.Local.Allow(ctx, project, limit, cost)
	if err == nil {
		d.Degraded = true
		f.degraded.Add(1)
	}
	return d, err
}

func (f *FailOpen) skipping() bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return time.Now().Before(f.skipUntil)
}

func (f *FailOpen) startSkipping(err error) {
	cooldown := f.Cooldown
	if cooldown <= 0 {
		cooldown = time.Second
	}
	f.mu.Lock()
	first := time.Now().After(f.skipUntil)
	f.skipUntil = time.Now().Add(cooldown)
	f.mu.Unlock()
	if first && f.Log != nil {
		f.Log.Warn("rate limiter: shared store unavailable, using local limits", "err", err, "retry_in", cooldown.String())
	}
}

// FailOpenStats counts how often the fallback was used.
type FailOpenStats struct {
	SharedFailures    uint64 `json:"shared_failures"`
	DegradedDecisions uint64 `json:"degraded_decisions"`
}

// Stats returns the counters.
func (f *FailOpen) Stats() FailOpenStats {
	return FailOpenStats{SharedFailures: f.failures.Load(), DegradedDecisions: f.degraded.Load()}
}
