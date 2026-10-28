package ratelimit

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

// tokenBucketScript refills and spends one bucket atomically.
//
// Why a script: with two ingest instances, a separate GET then SET lets
// both read "50 tokens left" and both spend them. Redis runs a script as
// one step, so no other command can run between the read and the write.
//
// Why TIME inside the script: every ingest instance sees the same clock
// (Redis's), so clock drift between hosts can't create or destroy tokens.
//
// KEYS[1] bucket key   ARGV[1] events per second   ARGV[2] burst   ARGV[3] cost
// Returns {allowed (1/0), retry_after_microseconds}.
const tokenBucketScript = `
local rate  = tonumber(ARGV[1])
local burst = tonumber(ARGV[2])
local cost  = tonumber(ARGV[3])

local t   = redis.call('TIME')
local now = tonumber(t[1]) * 1000000 + tonumber(t[2])

local state  = redis.call('HMGET', KEYS[1], 'tokens', 'ts')
local tokens = tonumber(state[1])
local ts     = tonumber(state[2])
if tokens == nil or ts == nil then
  tokens = burst
  ts = now
end

local elapsed = now - ts
if elapsed > 0 then
  tokens = math.min(burst, tokens + elapsed * rate / 1000000)
  ts = now
end

local allowed = 0
local retry = 0
if tokens >= cost then
  tokens = tokens - cost
  allowed = 1
else
  retry = math.ceil((cost - tokens) * 1000000 / rate)
end

-- tostring keeps fractional tokens; Redis would truncate a returned or
-- stored Lua number to an integer.
redis.call('HSET', KEYS[1], 'tokens', tostring(tokens), 'ts', tostring(ts))
-- An idle bucket refills completely after burst/rate seconds; after that
-- it carries no information and can expire.
redis.call('PEXPIRE', KEYS[1], math.ceil(burst / rate * 1000) + 1000)
return {allowed, retry}
`

var script = redis.NewScript(tokenBucketScript)

// Redis is the shared limiter. Every ingest instance using the same Redis
// shares the same buckets.
type Redis struct {
	Client redis.Scripter
	// Prefix namespaces the keys; default "faultline:rl:".
	Prefix string
}

// Allow implements Limiter. Errors mean Redis could not be asked; callers
// decide what to do then (see FailOpen).
func (r *Redis) Allow(ctx context.Context, project string, limit Limit, cost int) (Decision, error) {
	if cost > limit.Burst {
		return Decision{}, ErrCostTooHigh
	}
	prefix := r.Prefix
	if prefix == "" {
		prefix = "faultline:rl:"
	}
	// Run uses EVALSHA and falls back to EVAL when Redis doesn't have the
	// script cached yet (first call, or after a Redis restart).
	res, err := script.Run(ctx, r.Client, []string{prefix + project}, limit.EventsPerSecond, limit.Burst, cost).Int64Slice()
	if err != nil {
		return Decision{}, fmt.Errorf("ratelimit: redis: %w", err)
	}
	if len(res) != 2 {
		return Decision{}, fmt.Errorf("ratelimit: unexpected script result %v", res)
	}
	return Decision{
		Allowed:    res[0] == 1,
		RetryAfter: time.Duration(res[1]) * time.Microsecond,
	}, nil
}
