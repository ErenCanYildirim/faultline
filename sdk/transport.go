package faultline

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"time"

	"github.com/erencanyildirim/faultline/event"
)

// batchOverhead is the size of `{"events":[` + `]}`.
const batchOverhead = len(`{"events":[]}`)

// maxRetryAfter caps how long a Retry-After header can make the SDK wait.
const maxRetryAfter = time.Minute

// send encodes events into one or more request bodies, each within
// MaxBatchBytes, and delivers them. It runs on the sender goroutine only.
func (c *Client) send(events []*event.Event) {
	// The sender goroutine must survive anything, or the SDK silently stops.
	defer func() {
		if r := recover(); r != nil {
			c.failed.Add(uint64(len(events)))
			c.log.Error("faultline: internal error while sending", "panic", fmt.Sprint(r))
		}
	}()

	encoded := make([]json.RawMessage, 0, len(events))
	for _, e := range events {
		data, ok := encodeWithinLimit(e)
		if !ok {
			c.failed.Add(1)
			c.log.Warn("faultline: event too large even after shrinking, dropped", "event_id", e.EventID)
			continue
		}
		encoded = append(encoded, data)
	}

	// Split into requests that respect MaxBatchBytes.
	start, size := 0, batchOverhead
	for i, data := range encoded {
		extra := len(data)
		if i > start {
			extra++ // comma
		}
		if size+extra > event.MaxBatchBytes && i > start {
			c.deliver(encoded[start:i])
			start, size = i, batchOverhead
			extra = len(data)
		}
		size += extra
	}
	if start < len(encoded) {
		c.deliver(encoded[start:])
	}
}

// deliver sends one request body, retrying retryable failures until it
// succeeds, fails permanently, runs out of retry time, or the client stops.
// While it waits, capture keeps filling the bounded buffer; that is the
// "buffer while the backend is down" behaviour, and drops happen at capture.
func (c *Client) deliver(events []json.RawMessage) {
	n := uint64(len(events))
	body, err := c.compress(events)
	if err != nil {
		c.failed.Add(n)
		c.log.Error("faultline: compressing batch failed", "err", err)
		return
	}

	deadline := time.Now().Add(c.opts.MaxRetryDuration)
	for attempt := 1; ; attempt++ {
		// While the breaker is open, wait for the cooldown before trying.
		if wait := c.breaker.wait(time.Now()); wait > 0 {
			if time.Now().Add(wait).After(deadline) {
				c.giveUp(n, "retry time ran out while the backend was unavailable")
				return
			}
			if !c.sleep(wait) {
				c.giveUp(n, "client closed while waiting to retry")
				return
			}
		}

		res := c.post(body, n)
		now := time.Now()
		switch res.kind {
		case outcomeDone:
			c.breaker.success()
			c.breakerOpen.Store(false)
			c.sent.Add(res.accepted)
			c.failed.Add(res.rejected)
			return

		case outcomePermanent:
			// The backend answered, so it is up; the request itself is wrong.
			// Sending it again would fail the same way.
			c.breaker.success()
			c.breakerOpen.Store(false)
			c.failed.Add(n)
			c.log.Warn("faultline: ingest rejected the batch", "status", res.status, "events", n, "reason", res.detail)
			return

		case outcomeRetry:
			if c.breaker.failure(now) {
				c.log.Warn("faultline: backend failing, pausing requests",
					"consecutive_failures", c.breaker.failures, "cooldown", c.opts.BreakerCooldown.String())
			}
			c.breakerOpen.Store(c.breaker.isOpen(now))

			delay := backoff(attempt, c.opts.RetryInitialBackoff, c.opts.RetryMaxBackoff)
			if res.retryAfter > delay {
				delay = res.retryAfter
			}
			remaining := deadline.Sub(now)
			if remaining <= 0 {
				c.giveUp(n, res.detail)
				return
			}
			// One last attempt right at the deadline rather than giving up early.
			delay = min(delay, remaining)
			c.retries.Add(1)
			c.log.Warn("faultline: sending batch failed, will retry",
				"events", n, "attempt", attempt, "retry_in", delay.String(), "reason", res.detail)
			if !c.sleep(delay) {
				c.giveUp(n, "client closed while waiting to retry")
				return
			}
		}
	}
}

func (c *Client) giveUp(n uint64, reason string) {
	c.failed.Add(n)
	c.log.Error("faultline: giving up on batch", "events", n, "reason", reason)
}

// sleep waits for d, or returns false early if the client is stopping.
func (c *Client) sleep(d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return true
	case <-c.stop:
		return false
	}
}

func (c *Client) compress(events []json.RawMessage) ([]byte, error) {
	c.body.Reset()
	c.gz.Reset(&c.body)
	_, _ = io.WriteString(c.gz, `{"events":[`)
	for i, data := range events {
		if i > 0 {
			_, _ = c.gz.Write([]byte{','})
		}
		_, _ = c.gz.Write(data)
	}
	_, _ = io.WriteString(c.gz, `]}`)
	if err := c.gz.Close(); err != nil {
		return nil, err
	}
	// Copy: the retry loop reuses this body while c.body is reused per batch.
	return bytes.Clone(c.body.Bytes()), nil
}

type outcomeKind int

const (
	outcomeDone      outcomeKind = iota // answered; counts in accepted/rejected
	outcomePermanent                    // won't succeed if repeated
	outcomeRetry                        // may succeed later
)

type outcome struct {
	kind               outcomeKind
	accepted, rejected uint64
	status             int
	retryAfter         time.Duration
	detail             string
}

// post makes one attempt and classifies the result.
func (c *Client) post(body []byte, n uint64) outcome {
	req, err := http.NewRequestWithContext(c.stopCtx, http.MethodPost, c.target.endpoint, bytes.NewReader(body))
	if err != nil {
		return outcome{kind: outcomePermanent, detail: err.Error()}
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Content-Encoding", "gzip")
	req.Header.Set(event.KeyHeader, c.target.key)
	req.Header.Set("User-Agent", SDKName+"/"+SDKVersion)

	resp, err := c.opts.HTTPClient.Do(req)
	if err != nil {
		// Connection refused, timeout, DNS: the backend may come back.
		return outcome{kind: outcomeRetry, detail: err.Error()}
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(io.LimitReader(resp.Body, 64<<10))

	switch code := resp.StatusCode; {
	case code == http.StatusAccepted || code == http.StatusBadRequest:
		// Both may carry a per-event BatchResponse.
		var br event.BatchResponse
		if json.Unmarshal(raw, &br) == nil && (br.Accepted > 0 || len(br.Rejected) > 0) {
			for _, r := range br.Rejected {
				c.log.Warn("faultline: event rejected by ingest", "event_id", r.EventID, "reason", r.Error)
			}
			return outcome{kind: outcomeDone, accepted: uint64(br.Accepted), rejected: uint64(len(br.Rejected))}
		}
		if code == http.StatusAccepted {
			return outcome{kind: outcomeDone, accepted: n}
		}
		return outcome{kind: outcomePermanent, status: code, detail: errorText(raw)}

	case code == http.StatusRequestTimeout, code == http.StatusTooManyRequests, code >= 500:
		return outcome{kind: outcomeRetry, status: code, retryAfter: parseRetryAfter(resp.Header.Get("Retry-After")),
			detail: fmt.Sprintf("HTTP %d: %s", code, errorText(raw))}

	default:
		// 401 wrong key, 413 too large, 415 bad encoding, and anything else
		// unexpected: retrying would repeat the same mistake.
		return outcome{kind: outcomePermanent, status: code, detail: errorText(raw)}
	}
}

// parseRetryAfter reads seconds or an HTTP date, capped at maxRetryAfter.
func parseRetryAfter(v string) time.Duration {
	if v == "" {
		return 0
	}
	var d time.Duration
	if secs, err := strconv.Atoi(v); err == nil && secs >= 0 {
		d = time.Duration(secs) * time.Second
	} else if t, err := http.ParseTime(v); err == nil {
		d = time.Until(t)
	}
	if d < 0 {
		return 0
	}
	return min(d, maxRetryAfter)
}

func errorText(raw []byte) string {
	var er event.ErrorResponse
	if json.Unmarshal(raw, &er) == nil && er.Error != "" {
		return er.Error
	}
	if len(raw) > 200 {
		raw = raw[:200]
	}
	return string(raw)
}
