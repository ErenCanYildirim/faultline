package faultline

import (
	"math/rand/v2"
	"time"
)

// breaker tracks the health of the ingest endpoint across batches.
//
// Closed: requests go out; failures are retried with exponential backoff.
// Open: after threshold consecutive failures, nothing is sent until the
// cooldown ends. The next request is a probe: success closes the breaker,
// failure opens it for another cooldown.
//
// Without it, every new batch would start its own fresh retries against a
// server that is known to be down.
//
// Only the sender goroutine uses it, so it needs no locking.
type breaker struct {
	threshold int
	cooldown  time.Duration
	failures  int
	openUntil time.Time
}

// wait returns how long to hold off before the next request.
func (b *breaker) wait(now time.Time) time.Duration {
	if now.Before(b.openUntil) {
		return b.openUntil.Sub(now)
	}
	return 0
}

// isOpen reports whether requests are currently held back.
func (b *breaker) isOpen(now time.Time) bool { return now.Before(b.openUntil) }

func (b *breaker) failure(now time.Time) (opened bool) {
	b.failures++
	if b.failures >= b.threshold {
		wasOpen := b.isOpen(now)
		b.openUntil = now.Add(b.cooldown)
		return !wasOpen
	}
	return false
}

func (b *breaker) success() {
	b.failures = 0
	b.openUntil = time.Time{}
}

// backoff returns the wait before retry number attempt (1-based):
// exponential growth capped at max, with "equal jitter": half the delay is
// fixed, half random. Jitter spreads out many clients that failed at the
// same moment, so they don't all retry in lockstep when ingest comes back.
func backoff(attempt int, initial, max time.Duration) time.Duration {
	d := initial
	for i := 1; i < attempt && d < max; i++ {
		d *= 2
	}
	if d > max {
		d = max
	}
	half := d / 2
	return half + rand.N(half+1)
}
