package ratelimit

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

type fakeClock struct{ t time.Time }

func (c *fakeClock) now() time.Time          { return c.t }
func (c *fakeClock) advance(d time.Duration) { c.t = c.t.Add(d) }

func newLocalWithClock() (*Local, *fakeClock) {
	c := &fakeClock{t: time.Date(2026, 10, 28, 12, 0, 0, 0, time.UTC)}
	l := NewLocal()
	l.now = c.now
	return l, c
}

var testLimit = Limit{EventsPerSecond: 100, Burst: 500}

func TestLocalBucket(t *testing.T) {
	l, clock := newLocalWithClock()
	ctx := context.Background()
	allow := func(cost int) Decision {
		t.Helper()
		d, err := l.Allow(ctx, "demo", testLimit, cost)
		if err != nil {
			t.Fatal(err)
		}
		return d
	}

	// A full bucket takes a burst of 500.
	for i := range 5 {
		if d := allow(100); !d.Allowed {
			t.Fatalf("batch %d of the initial burst rejected", i)
		}
	}
	// Empty: the next batch of 100 needs 1s of refill at 100/s.
	d := allow(100)
	if d.Allowed || d.RetryAfter != time.Second {
		t.Errorf("empty bucket: %+v, want rejected with RetryAfter 1s", d)
	}
	// A rejected request spends nothing: half a second later, 50 tokens.
	clock.advance(500 * time.Millisecond)
	if d := allow(60); d.Allowed || d.RetryAfter != 100*time.Millisecond {
		t.Errorf("60 with 50 tokens: %+v, want rejected with RetryAfter 100ms", d)
	}
	if d := allow(50); !d.Allowed {
		t.Errorf("50 with 50 tokens: %+v, want allowed", d)
	}
	// Refill never exceeds the burst.
	clock.advance(time.Hour)
	for range 5 {
		allow(100)
	}
	if d := allow(1); d.Allowed {
		t.Error("bucket refilled past its burst")
	}
}

func TestLocalProjectsAreIndependent(t *testing.T) {
	l, _ := newLocalWithClock()
	ctx := context.Background()
	for range 5 {
		_, _ = l.Allow(ctx, "noisy", testLimit, 100)
	}
	if d, _ := l.Allow(ctx, "noisy", testLimit, 1); d.Allowed {
		t.Error("noisy project should be out of tokens")
	}
	if d, _ := l.Allow(ctx, "quiet", testLimit, 100); !d.Allowed {
		t.Error("one project's flood must not affect another")
	}
}

func TestCostAboveBurstIsAnError(t *testing.T) {
	if _, err := NewLocal().Allow(context.Background(), "p", Limit{EventsPerSecond: 1, Burst: 10}, 11); !errors.Is(err, ErrCostTooHigh) {
		t.Errorf("err = %v, want ErrCostTooHigh", err)
	}
}

func TestLimitValidate(t *testing.T) {
	if err := (Limit{EventsPerSecond: 10, Burst: 100}).Validate(100); err != nil {
		t.Errorf("valid limit rejected: %v", err)
	}
	for _, l := range []Limit{{0, 100}, {-1, 100}, {10, 99}} {
		if err := l.Validate(100); err == nil {
			t.Errorf("%+v should be invalid", l)
		}
	}
}

// fakeShared fails or succeeds on demand and counts calls.
type fakeShared struct {
	mu    sync.Mutex
	err   error
	calls int
}

func (f *fakeShared) Allow(ctx context.Context, _ string, _ Limit, _ int) (Decision, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	if f.err != nil {
		return Decision{}, f.err
	}
	return Decision{Allowed: true}, nil
}

func TestFailOpenUsesLocalWhileSharedIsDown(t *testing.T) {
	shared := &fakeShared{err: errors.New("connection refused")}
	f := &FailOpen{Shared: shared, Local: NewLocal(), Cooldown: 100 * time.Millisecond}
	ctx := context.Background()

	// Down: decisions come from the local bucket, still enforcing the limit.
	allowed := 0
	for range 10 {
		d, err := f.Allow(ctx, "p", testLimit, 100)
		if err != nil {
			t.Fatal(err)
		}
		if !d.Degraded {
			t.Fatal("decision should be marked degraded")
		}
		if d.Allowed {
			allowed++
		}
	}
	if allowed != 5 {
		t.Errorf("local fallback allowed %d batches of 100, want 5 (burst 500)", allowed)
	}
	// Only the first call tried the shared limiter; the rest skipped it.
	if shared.calls != 1 {
		t.Errorf("shared limiter called %d times during cooldown, want 1", shared.calls)
	}

	// Back up: after the cooldown the shared limiter decides again.
	shared.mu.Lock()
	shared.err = nil
	shared.mu.Unlock()
	time.Sleep(150 * time.Millisecond)
	d, _ := f.Allow(ctx, "p", testLimit, 100)
	if !d.Allowed || d.Degraded {
		t.Errorf("after recovery: %+v, want allowed by the shared limiter", d)
	}
	if s := f.Stats(); s.SharedFailures != 1 || s.DegradedDecisions != 10 {
		t.Errorf("stats = %+v", s)
	}
}

func TestFailOpenPassesThroughCostTooHigh(t *testing.T) {
	f := &FailOpen{Shared: NewLocal(), Local: NewLocal()}
	_, err := f.Allow(context.Background(), "p", Limit{EventsPerSecond: 1, Burst: 10}, 11)
	if !errors.Is(err, ErrCostTooHigh) {
		t.Errorf("err = %v", err)
	}
	if f.Stats().SharedFailures != 0 {
		t.Error("a request that can never fit is not a shared-limiter failure")
	}
}
