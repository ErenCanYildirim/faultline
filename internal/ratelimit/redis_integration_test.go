//go:build integration

// Runs against a real Redis. Start the stack first, then:
//
//	go test -tags integration ./internal/ratelimit/
//
// Uses REDIS_ADDR (default localhost:6379). Each test uses its own key
// prefix, so it never touches the real buckets.
package ratelimit

import (
	"context"
	"fmt"
	"math"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

func testRedis(t *testing.T) (*redis.Client, *Redis) {
	t.Helper()
	addr := os.Getenv("REDIS_ADDR")
	if addr == "" {
		addr = "localhost:6379"
	}
	c := redis.NewClient(&redis.Options{Addr: addr, PoolSize: 64})
	t.Cleanup(func() { _ = c.Close() })
	if err := c.Ping(context.Background()).Err(); err != nil {
		t.Fatalf("Redis not reachable at %s (is the stack up?): %v", addr, err)
	}
	prefix := fmt.Sprintf("faultline-test:%d:", time.Now().UnixNano())
	t.Cleanup(func() {
		ctx := context.Background()
		keys, _ := c.Keys(ctx, prefix+"*").Result()
		if len(keys) > 0 {
			c.Del(ctx, keys...)
		}
	})
	return c, &Redis{Client: c, Prefix: prefix}
}

func TestRedisBucket(t *testing.T) {
	_, r := testRedis(t)
	ctx := context.Background()

	for i := range 5 {
		d, err := r.Allow(ctx, "demo", testLimit, 100)
		if err != nil {
			t.Fatal(err)
		}
		if !d.Allowed {
			t.Fatalf("batch %d of the initial burst rejected", i)
		}
	}
	d, err := r.Allow(ctx, "demo", testLimit, 100)
	if err != nil {
		t.Fatal(err)
	}
	// Empty bucket, 100/s: about 1s until 100 tokens. A few ms have passed
	// since the last call, so a little less than 1s is also right.
	if d.Allowed || d.RetryAfter < 900*time.Millisecond || d.RetryAfter > time.Second {
		t.Errorf("empty bucket: %+v, want rejected with RetryAfter close to 1s", d)
	}

	// Refill: after 300ms at 100/s there are about 30 tokens.
	time.Sleep(300 * time.Millisecond)
	if d, _ := r.Allow(ctx, "demo", testLimit, 25); !d.Allowed {
		t.Errorf("25 events after 300ms of refill: %+v, want allowed", d)
	}
	if d, _ := r.Allow(ctx, "demo", testLimit, 25); d.Allowed {
		t.Errorf("another 25 right after: %+v, want rejected (about 5 tokens left)", d)
	}
}

func TestRedisProjectsAreIndependent(t *testing.T) {
	_, r := testRedis(t)
	ctx := context.Background()
	for range 5 {
		_, _ = r.Allow(ctx, "noisy", testLimit, 100)
	}
	if d, _ := r.Allow(ctx, "noisy", testLimit, 50); d.Allowed {
		t.Error("noisy project should be out of tokens")
	}
	if d, _ := r.Allow(ctx, "quiet", testLimit, 100); !d.Allowed {
		t.Error("one project's flood must not affect another")
	}
}

// The reason the script exists: many concurrent requests (as from several
// ingest instances) must never spend more tokens than the bucket holds.
func TestRedisConcurrentRequestsNeverOverspend(t *testing.T) {
	_, r := testRedis(t)
	ctx := context.Background()
	// Practically no refill during the test: what's spent is the burst.
	limit := Limit{EventsPerSecond: 0.001, Burst: 1000}

	var allowed atomic.Int64
	var wg sync.WaitGroup
	for range 200 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 10 {
				d, err := r.Allow(ctx, "contended", limit, 1)
				if err != nil {
					t.Error(err)
					return
				}
				if d.Allowed {
					allowed.Add(1)
				}
			}
		}()
	}
	wg.Wait()
	if got := allowed.Load(); got != 1000 {
		t.Errorf("2000 concurrent requests for a bucket of 1000: %d allowed, want exactly 1000", got)
	}
}

// For comparison, the same bucket math done with separate read and write
// commands, which is what the script prevents. Not an assertion, because a
// race is not guaranteed to happen on every run; the log shows how often
// it overspends.
func TestNaiveReadThenWriteOverspends(t *testing.T) {
	c, _ := testRedis(t)
	ctx := context.Background()
	key := fmt.Sprintf("faultline-test:naive:%d", time.Now().UnixNano())
	defer c.Del(ctx, key)
	c.Set(ctx, key, 1000, 0)

	var allowed atomic.Int64
	var wg sync.WaitGroup
	for range 200 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 10 {
				v, _ := c.Get(ctx, key).Result()
				tokens, _ := strconv.Atoi(v)
				if tokens >= 1 {
					c.Set(ctx, key, tokens-1, 0) // another request may have spent it meanwhile
					allowed.Add(1)
				}
			}
		}()
	}
	wg.Wait()
	t.Logf("naive GET then SET: %d of 2000 requests allowed against a bucket of 1000 (the script allows exactly 1000)", allowed.Load())
}

func TestRedisKeyExpires(t *testing.T) {
	c, r := testRedis(t)
	ctx := context.Background()
	if _, err := r.Allow(ctx, "idle", testLimit, 1); err != nil {
		t.Fatal(err)
	}
	ttl, err := c.PTTL(ctx, r.Prefix+"idle").Result()
	if err != nil {
		t.Fatal(err)
	}
	// burst/rate = 5s to refill completely, plus 1s.
	if ttl <= 0 || math.Abs(ttl.Seconds()-6) > 0.5 {
		t.Errorf("TTL = %v, want about 6s so idle buckets disappear", ttl)
	}
}

func TestRedisSurvivesScriptCacheFlush(t *testing.T) {
	c, r := testRedis(t)
	ctx := context.Background()
	if _, err := r.Allow(ctx, "p", testLimit, 1); err != nil {
		t.Fatal(err)
	}
	// As after a Redis restart: the cached script is gone.
	if err := c.ScriptFlush(ctx).Err(); err != nil {
		t.Fatal(err)
	}
	if d, err := r.Allow(ctx, "p", testLimit, 1); err != nil || !d.Allowed {
		t.Errorf("after SCRIPT FLUSH: %+v, %v; Run must reload the script", d, err)
	}
}

func TestFailOpenWithRedisDown(t *testing.T) {
	down := redis.NewClient(&redis.Options{Addr: "127.0.0.1:1", DialTimeout: 100 * time.Millisecond, MaxRetries: -1})
	defer down.Close()
	f := &FailOpen{Shared: &Redis{Client: down}, Local: NewLocal()}

	start := time.Now()
	allowed := 0
	for range 10 {
		d, err := f.Allow(context.Background(), "p", testLimit, 100)
		if err != nil {
			t.Fatal(err)
		}
		if d.Allowed {
			allowed++
		}
	}
	if allowed != 5 {
		t.Errorf("allowed %d batches with Redis down, want 5 from the local burst", allowed)
	}
	// One timeout, then the cooldown skips Redis: not 10 timeouts.
	if elapsed := time.Since(start); elapsed > 300*time.Millisecond {
		t.Errorf("10 requests took %v with Redis down; the cooldown should skip Redis after the first failure", elapsed)
	}
}
