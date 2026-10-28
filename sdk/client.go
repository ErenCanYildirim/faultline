package faultline

import (
	"bytes"
	"compress/gzip"
	"context"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"time"

	"github.com/erencanyildirim/faultline/event"
)

// Re-exported so applications only import this package.
type (
	Event = event.Event
	Level = event.Level
	User  = event.User
)

const (
	LevelDebug   = event.LevelDebug
	LevelInfo    = event.LevelInfo
	LevelWarning = event.LevelWarning
	LevelError   = event.LevelError
	LevelFatal   = event.LevelFatal
)

// Stats are counters an application can read to see what the SDK is doing.
//
// Every captured event is in exactly one place, so this always holds:
//
//	Captured = Sent + Failed + Queued + InFlight
//
// Dropped, Sampled and Filtered events were never captured and are counted
// separately.
type Stats struct {
	Captured uint64 // accepted into the buffer
	Sent     uint64 // acknowledged by ingest
	Failed   uint64 // captured but lost: retries exhausted, or rejected by ingest
	Queued   uint64 // waiting in the buffer right now
	InFlight uint64 // taken by the sender, outcome not known yet

	Dropped  uint64 // never captured: buffer full (count or bytes) or client closed
	Sampled  uint64 // never captured: left out by SampleRate
	Filtered uint64 // never captured: BeforeSend returned nil or panicked

	Retries     uint64 // requests retried after a retryable failure
	BreakerOpen bool   // true while the SDK holds back requests to a failing backend
}

// queued is a buffered event and its estimated memory.
type queued struct {
	e    *event.Event
	size int64
}

// Client captures events and sends them in the background. It is safe for
// concurrent use. Most applications use the package-level functions, which
// share one Client created by Init.
type Client struct {
	opts   Options
	target target
	log    *slog.Logger

	queue       chan queued
	queuedBytes atomic.Int64
	flushReq    chan chan struct{}
	stop        chan struct{}
	stopCtx     context.Context // cancelled with stop; aborts in-flight requests
	cancelStop  context.CancelFunc
	done        chan struct{}
	closing     atomic.Bool
	stopOnce    sync.Once

	captured, sent, failed     atomic.Uint64
	dropped, sampled, filtered atomic.Uint64
	retries                    atomic.Uint64
	breakerOpen                atomic.Bool

	// Used only by the sender goroutine.
	body    bytes.Buffer
	gz      *gzip.Writer
	breaker breaker
}

// New creates a Client and starts its sender goroutine. It returns an error
// for an invalid DSN or option.
func New(opts Options) (*Client, error) {
	t, err := parseDSN(opts.DSN)
	if err != nil {
		return nil, err
	}
	if err := opts.validate(); err != nil {
		return nil, err
	}
	opts = opts.withDefaults()
	stopCtx, cancel := context.WithCancel(context.Background())
	c := &Client{
		opts:       opts,
		target:     t,
		log:        opts.Logger,
		queue:      make(chan queued, opts.BufferSize),
		flushReq:   make(chan chan struct{}),
		stop:       make(chan struct{}),
		stopCtx:    stopCtx,
		cancelStop: cancel,
		done:       make(chan struct{}),
		breaker:    breaker{threshold: opts.BreakerThreshold, cooldown: opts.BreakerCooldown},
	}
	c.gz = gzip.NewWriter(&c.body)
	go c.run()
	return c, nil
}

// EventOption adds data to one captured event.
type EventOption func(*event.Event)

// WithLevel overrides the default level (error for errors, info for messages).
func WithLevel(l Level) EventOption {
	return func(e *event.Event) { e.Level = l }
}

// WithTag adds one tag.
func WithTag(key, value string) EventOption {
	return func(e *event.Event) {
		if e.Tags == nil {
			e.Tags = make(map[string]string, 4)
		}
		e.Tags[key] = value
	}
}

// WithTags adds several tags. The map is copied.
func WithTags(tags map[string]string) EventOption {
	return func(e *event.Event) {
		if e.Tags == nil {
			e.Tags = make(map[string]string, len(tags))
		}
		for k, v := range tags {
			e.Tags[k] = v
		}
	}
}

// WithUser attaches the affected user.
func WithUser(u User) EventOption {
	return func(e *event.Event) { e.User = &u }
}

// DefaultFingerprint stands for the default grouping inside WithFingerprint.
const DefaultFingerprint = "{{default}}"

// WithFingerprint overrides how the event is grouped into an issue. Events
// with the same parts become one issue, whatever their stacks:
//
//	faultline.WithFingerprint("payment-provider-timeout")
//
// Include DefaultFingerprint to refine the default grouping instead of
// replacing it, e.g. one issue per tenant:
//
//	faultline.WithFingerprint(faultline.DefaultFingerprint, tenantID)
func WithFingerprint(parts ...string) EventOption {
	return func(e *event.Event) { e.Fingerprint = append([]string(nil), parts...) }
}

// CaptureError records err with the current stack trace. It returns the
// event ID, or "" if err is nil or the event was not captured.
func (c *Client) CaptureError(err error, opts ...EventOption) string {
	if err == nil {
		return ""
	}
	if !c.keepSample() {
		return ""
	}
	e := c.newEvent(LevelError)
	e.Exception = &event.Exception{
		Type:   errorType(err),
		Value:  err.Error(),
		Frames: c.stacktrace(),
	}
	return c.capture(e, opts)
}

// CaptureMessage records a message without a stack trace. It returns the
// event ID, or "" if the event was not captured.
func (c *Client) CaptureMessage(msg string, opts ...EventOption) string {
	if !c.keepSample() {
		return ""
	}
	e := c.newEvent(LevelInfo)
	e.Message = msg
	return c.capture(e, opts)
}

// Recover captures a panic, waits up to 2 seconds for it to be sent, then
// panics again with the same value. Use it directly with defer:
//
//	defer client.Recover()
//
// Re-panicking keeps the program's behavior unchanged: a crash still
// crashes. The SDK only records it on the way out.
func (c *Client) Recover() {
	if r := recover(); r != nil {
		c.capturePanic(r)
		panic(r)
	}
}

// capturePanic is never sampled: a crash is rare and always worth knowing.
func (c *Client) capturePanic(r any) {
	e := c.newEvent(LevelFatal)
	typ := fmt.Sprintf("%T", r)
	if err, ok := r.(error); ok {
		typ = errorType(err)
	}
	e.Exception = &event.Exception{
		Type:   typ,
		Value:  fmt.Sprint(r),
		Frames: c.stacktrace(),
	}
	c.capture(e, []EventOption{WithTag("mechanism", "panic")})
	c.Flush(2 * time.Second)
}

// keepSample decides before any work is done, so sampled-out events cost
// almost nothing: no stack capture, no allocation.
func (c *Client) keepSample() bool {
	if c.opts.SampleRate >= 1 || rand.Float64() < c.opts.SampleRate {
		return true
	}
	c.sampled.Add(1)
	return false
}

func (c *Client) newEvent(level Level) *event.Event {
	return &event.Event{
		EventID:     newEventID(),
		Timestamp:   time.Now().UTC(),
		Level:       level,
		Release:     c.opts.Release,
		Environment: c.opts.Environment,
		SDK:         event.SDKInfo{Name: SDKName, Version: SDKVersion},
	}
}

// capture is the only path into the buffer. It never blocks.
func (c *Client) capture(e *event.Event, opts []EventOption) string {
	if c.closing.Load() {
		c.dropped.Add(1)
		return ""
	}
	for _, opt := range opts {
		opt(e)
	}
	if c.opts.BeforeSend != nil {
		if e = c.runBeforeSend(e); e == nil {
			c.filtered.Add(1)
			return ""
		}
	}
	enforceFieldLimits(e)

	// Reserve the bytes first, then take a slot. Either limit can refuse.
	size := approxSize(e)
	if c.queuedBytes.Add(size) > c.opts.BufferBytes {
		c.queuedBytes.Add(-size)
		c.dropped.Add(1)
		c.log.Warn("faultline: buffer memory limit reached, event dropped", "buffer_bytes", c.opts.BufferBytes)
		return ""
	}
	select {
	case c.queue <- queued{e: e, size: size}:
		c.captured.Add(1)
		return e.EventID
	default:
		// Buffer full. Dropping the new event keeps the oldest ones, which
		// are usually the first symptoms of an incident and the most useful.
		c.queuedBytes.Add(-size)
		c.dropped.Add(1)
		c.log.Warn("faultline: buffer full, event dropped", "buffer_size", c.opts.BufferSize)
		return ""
	}
}

// runBeforeSend calls the hook and turns a panic into a dropped event.
func (c *Client) runBeforeSend(e *event.Event) (out *event.Event) {
	defer func() {
		if r := recover(); r != nil {
			c.log.Error("faultline: BeforeSend panicked, event dropped", "panic", fmt.Sprint(r))
			out = nil
		}
	}()
	return c.opts.BeforeSend(e)
}

// approxSize estimates an event's memory: its text plus fixed overhead per
// event, frame and tag. An estimate is enough to bound memory; encoding the
// event exactly would cost the caller CPU on every capture.
func approxSize(e *event.Event) int64 {
	n := 512 + len(e.Message) + len(e.Release) + len(e.Environment)
	if ex := e.Exception; ex != nil {
		n += len(ex.Type) + len(ex.Value)
		for _, f := range ex.Frames {
			n += 64 + len(f.Module) + len(f.Function) + len(f.File)
		}
	}
	for k, v := range e.Tags {
		n += 32 + len(k) + len(v)
	}
	if u := e.User; u != nil {
		n += len(u.ID) + len(u.Username) + len(u.Email) + len(u.IPAddress)
	}
	return int64(n)
}

// Flush waits until every event buffered before the call has been sent, or
// until timeout. It returns false on timeout, which is expected while the
// backend is down: the SDK is still retrying.
func (c *Client) Flush(timeout time.Duration) bool {
	timer := time.NewTimer(timeout)
	defer timer.Stop()

	ack := make(chan struct{})
	select {
	case c.flushReq <- ack:
	case <-timer.C:
		return false
	case <-c.done:
		return false
	}
	select {
	case <-ack:
		return true
	case <-timer.C:
		return false
	}
}

// Close stops accepting events, sends what is buffered within timeout, and
// stops the sender goroutine. Events still waiting or retrying when the
// timeout ends are lost and counted as Failed. It returns false in that case.
// Calling Close more than once is safe.
func (c *Client) Close(timeout time.Duration) bool {
	start := time.Now()
	c.closing.Store(true)
	flushed := c.Flush(timeout)
	c.stopOnce.Do(func() {
		close(c.stop)
		c.cancelStop()
	})

	// The sender abandons any retry at once when stop closes; give it a
	// moment to record the outcome even if the flush used up the timeout.
	remaining := max(timeout-time.Since(start), 100*time.Millisecond)
	select {
	case <-c.done:
	case <-time.After(remaining):
		return false
	}
	return flushed
}

// Stats returns a snapshot of the SDK's counters. While events are moving,
// the counters are read one after another, so InFlight can be off by a few
// for an instant; once the SDK is idle, the equation in Stats is exact.
func (c *Client) Stats() Stats {
	// Read outcomes before captured, so a concurrent send can't make the
	// subtraction below go negative.
	sent := c.sent.Load()
	failed := c.failed.Load()
	queued := uint64(len(c.queue))
	captured := c.captured.Load()
	var inFlight uint64
	if done := sent + failed + queued; captured > done {
		inFlight = captured - done
	}
	return Stats{
		Captured:    captured,
		Sent:        sent,
		Failed:      failed,
		Queued:      queued,
		InFlight:    inFlight,
		Dropped:     c.dropped.Load(),
		Sampled:     c.sampled.Load(),
		Filtered:    c.filtered.Load(),
		Retries:     c.retries.Load(),
		BreakerOpen: c.breakerOpen.Load(),
	}
}

// sendAndRelease sends a batch, then releases its memory reservation.
// Bytes are held until the outcome is known, not just until the event
// leaves the queue: events waiting in the sender's batch or being retried
// still use memory, so BufferBytes bounds everything the SDK holds.
func (c *Client) sendAndRelease(batch []queued) {
	events := make([]*event.Event, len(batch))
	var bytes int64
	for i, q := range batch {
		events[i] = q.e
		bytes += q.size
	}
	c.send(events)
	c.queuedBytes.Add(-bytes)
}

// discard counts events as failed and releases their memory. Used when the
// client stops with events not yet sent, so Stats still add up after Close.
func (c *Client) discard(batch []queued) {
	for _, q := range batch {
		c.queuedBytes.Add(-q.size)
	}
	c.failed.Add(uint64(len(batch)))
}

// run is the sender goroutine. It batches by size or time, whichever comes
// first, and serves flush requests.
func (c *Client) run() {
	defer close(c.done)
	defer func() {
		for {
			select {
			case q := <-c.queue:
				c.discard([]queued{q})
			default:
				return
			}
		}
	}()

	ticker := time.NewTicker(c.opts.FlushInterval)
	defer ticker.Stop()

	batch := make([]queued, 0, c.opts.BatchSize)
	for {
		select {
		case q := <-c.queue:
			batch = append(batch, q)
			if len(batch) >= c.opts.BatchSize {
				c.sendAndRelease(batch)
				batch = batch[:0]
			}
		case <-ticker.C:
			if len(batch) > 0 {
				c.sendAndRelease(batch)
				batch = batch[:0]
			}
		case ack := <-c.flushReq:
			// Take only what is queued now. Under steady load the queue never
			// empties, and a flush must still finish.
			for n := len(c.queue); n > 0; n-- {
				batch = append(batch, <-c.queue)
			}
			for len(batch) > 0 {
				size := min(len(batch), c.opts.BatchSize)
				c.sendAndRelease(batch[:size])
				batch = batch[size:]
			}
			batch = make([]queued, 0, c.opts.BatchSize)
			close(ack)
		case <-c.stop:
			c.discard(batch)
			return
		}
	}
}
