package faultline_test

import (
	"compress/gzip"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/erencanyildirim/faultline/event"
	faultline "github.com/erencanyildirim/faultline/sdk"
)

// scriptedIngest answers each request with the next status in a script,
// then 202 for everything after it. It records when each request arrived.
type scriptedIngest struct {
	srv      *httptest.Server
	mu       sync.Mutex
	script   []int
	headers  map[int]string // status -> Retry-After value
	arrivals []time.Time
	accepted atomic.Int64
}

func newScriptedIngest(t *testing.T, script ...int) *scriptedIngest {
	s := &scriptedIngest{script: script, headers: map[int]string{}}
	s.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		s.mu.Lock()
		s.arrivals = append(s.arrivals, time.Now())
		status := http.StatusAccepted
		if len(s.script) > 0 {
			status, s.script = s.script[0], s.script[1:]
		}
		retryAfter := s.headers[status]
		s.mu.Unlock()

		if retryAfter != "" {
			w.Header().Set("Retry-After", retryAfter)
		}
		if status != http.StatusAccepted {
			w.WriteHeader(status)
			_ = json.NewEncoder(w).Encode(event.ErrorResponse{Error: "scripted failure"})
			return
		}
		var b event.Batch
		_ = decodeGzipBatch(r, &b)
		s.accepted.Add(int64(len(b.Events)))
		w.WriteHeader(http.StatusAccepted)
		_ = json.NewEncoder(w).Encode(event.BatchResponse{Accepted: len(b.Events)})
	}))
	t.Cleanup(s.srv.Close)
	return s
}

func (s *scriptedIngest) dsn() string {
	return strings.Replace(s.srv.URL, "http://", "http://"+testKey+"@", 1)
}

func (s *scriptedIngest) requests() []time.Time {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]time.Time(nil), s.arrivals...)
}

func decodeGzipBatch(r *http.Request, b *event.Batch) error {
	gz, err := gzip.NewReader(r.Body)
	if err != nil {
		return err
	}
	return json.NewDecoder(gz).Decode(b)
}

// fastRetries keeps tests quick while keeping the real retry logic.
func fastRetries(dsn string) faultline.Options {
	return faultline.Options{
		DSN:                 dsn,
		FlushInterval:       10 * time.Millisecond,
		RetryInitialBackoff: 5 * time.Millisecond,
		RetryMaxBackoff:     20 * time.Millisecond,
		MaxRetryDuration:    5 * time.Second,
		BreakerThreshold:    100, // off unless a test turns it on
	}
}

func TestRetriesUntilBackendRecovers(t *testing.T) {
	ing := newScriptedIngest(t, 503, 502, 500, 429)
	c := newClient(t, fastRetries(ing.dsn()))

	c.CaptureMessage("survives an outage")
	if !c.Flush(5 * time.Second) {
		t.Fatal("Flush timed out")
	}
	s := c.Stats()
	if s.Sent != 1 || s.Failed != 0 || s.Retries != 4 {
		t.Errorf("stats = %+v, want sent after 4 retries", s)
	}
	if got := len(ing.requests()); got != 5 {
		t.Errorf("requests = %d, want 5", got)
	}
}

func TestPermanentErrorsAreNotRetried(t *testing.T) {
	for _, status := range []int{401, 413, 415} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			ing := newScriptedIngest(t, status)
			c := newClient(t, fastRetries(ing.dsn()))
			c.CaptureMessage("x")
			c.Flush(2 * time.Second)

			s := c.Stats()
			if s.Failed != 1 || s.Retries != 0 || len(ing.requests()) != 1 {
				t.Errorf("stats = %+v, requests = %d; want one attempt, no retry", s, len(ing.requests()))
			}
		})
	}
}

func TestRetryAfterIsHonored(t *testing.T) {
	ing := newScriptedIngest(t, 503)
	ing.headers[503] = "1" // one second, far above the 5-20ms backoff
	c := newClient(t, fastRetries(ing.dsn()))

	c.CaptureMessage("x")
	c.Flush(5 * time.Second)

	reqs := ing.requests()
	if len(reqs) != 2 {
		t.Fatalf("requests = %d, want 2", len(reqs))
	}
	if gap := reqs[1].Sub(reqs[0]); gap < 900*time.Millisecond {
		t.Errorf("retried after %v; Retry-After asked for 1s", gap)
	}
}

func TestBreakerPausesRequestsToAFailingBackend(t *testing.T) {
	fails := make([]int, 100)
	for i := range fails {
		fails[i] = 503
	}
	ing := newScriptedIngest(t, fails...)
	opts := fastRetries(ing.dsn())
	opts.BreakerThreshold = 3
	opts.BreakerCooldown = 300 * time.Millisecond
	c := newClient(t, opts)

	c.CaptureMessage("x")
	time.Sleep(500 * time.Millisecond)

	reqs := ing.requests()
	// 3 failures open the breaker; one probe after the 300ms cooldown fails
	// and reopens it. Without the breaker, 5-20ms backoff would have sent ~40.
	if len(reqs) < 3 || len(reqs) > 5 {
		t.Errorf("requests in 500ms = %d, want 3 to 5 with the breaker open", len(reqs))
	}
	// The fourth request is the probe: it must wait out the cooldown.
	if len(reqs) > 3 {
		if gap := reqs[3].Sub(reqs[2]); gap < 250*time.Millisecond {
			t.Errorf("probe came %v after the third failure, before the 300ms cooldown", gap)
		}
	}
	if !c.Stats().BreakerOpen {
		t.Error("Stats().BreakerOpen should be true while the backend keeps failing")
	}
}

func TestBreakerClosesOnSuccess(t *testing.T) {
	ing := newScriptedIngest(t, 503, 503)
	opts := fastRetries(ing.dsn())
	opts.BreakerThreshold = 2
	opts.BreakerCooldown = 50 * time.Millisecond
	c := newClient(t, opts)

	c.CaptureMessage("x")
	if !c.Flush(5 * time.Second) {
		t.Fatal("Flush timed out")
	}
	if s := c.Stats(); s.Sent != 1 || s.BreakerOpen {
		t.Errorf("stats = %+v, want sent and breaker closed", s)
	}
}

func TestBufferStaysBoundedDuringOutage(t *testing.T) {
	fails := make([]int, 1000)
	for i := range fails {
		fails[i] = 503
	}
	ing := newScriptedIngest(t, fails...)
	opts := fastRetries(ing.dsn())
	opts.BufferSize = 20
	opts.BatchSize = 5
	c := newClient(t, opts)

	for range 500 {
		c.CaptureError(errors.New("boom"))
	}
	s := c.Stats()
	if s.Captured > 25 {
		t.Errorf("captured %d during an outage, above buffer (20) + one batch (5)", s.Captured)
	}
	if s.Captured+s.Dropped != 500 {
		t.Errorf("captured %d + dropped %d != 500", s.Captured, s.Dropped)
	}
}

func TestBufferBytesLimit(t *testing.T) {
	ing := newScriptedIngest(t)
	opts := fastRetries(ing.dsn())
	opts.BufferBytes = 20 << 10 // 20 KiB
	opts.FlushInterval = time.Hour
	opts.BatchSize = 100
	c := newClient(t, opts)

	big := strings.Repeat("x", 4000) // about 4.5 KiB per event with overhead
	for range 20 {
		c.CaptureMessage(big)
	}
	s := c.Stats()
	if s.Captured == 0 || s.Captured > 5 {
		t.Errorf("captured %d large events into a 20 KiB buffer, want 1 to 5", s.Captured)
	}
	if s.Dropped == 0 {
		t.Error("expected drops from the byte limit")
	}
	// Bytes are released when events leave the buffer: after a flush, new
	// events fit again.
	c.Flush(2 * time.Second)
	if id := c.CaptureMessage(big); id == "" {
		t.Error("buffer bytes were not released after sending")
	}
}

func TestSampleRate(t *testing.T) {
	ing := newScriptedIngest(t)
	opts := fastRetries(ing.dsn())
	opts.SampleRate = 0.25
	opts.BufferSize = 10000
	c := newClient(t, opts)

	for range 4000 {
		c.CaptureError(errors.New("x"))
	}
	s := c.Stats()
	if s.Captured+s.Sampled != 4000 {
		t.Fatalf("captured %d + sampled %d != 4000", s.Captured, s.Sampled)
	}
	// 1000 expected; 4 standard deviations is about +-110.
	if s.Captured < 850 || s.Captured > 1150 {
		t.Errorf("captured %d of 4000 at rate 0.25", s.Captured)
	}
}

func TestPanicsAreNeverSampled(t *testing.T) {
	ing := newScriptedIngest(t)
	opts := fastRetries(ing.dsn())
	opts.SampleRate = 0.0000001
	c := newClient(t, opts)

	func() {
		defer func() { _ = recover() }()
		defer c.Recover()
		panic("crash")
	}()
	if ing.accepted.Load() != 1 {
		t.Errorf("panic not delivered with a near-zero sample rate")
	}
}

func TestInvalidSampleRate(t *testing.T) {
	for _, rate := range []float64{-0.1, 1.5} {
		if _, err := faultline.New(faultline.Options{DSN: "http://k@localhost", SampleRate: rate}); err == nil {
			t.Errorf("SampleRate %v accepted", rate)
		}
	}
}

func TestBeforeSend(t *testing.T) {
	ing := newScriptedIngest(t)
	opts := fastRetries(ing.dsn())
	var calls atomic.Int64
	opts.BeforeSend = func(e *faultline.Event) *faultline.Event {
		calls.Add(1)
		switch e.Message {
		case "drop me":
			return nil
		case "panic":
			panic("hook bug")
		}
		e.Tags = map[string]string{"scrubbed": "yes"}
		return e
	}
	c := newClient(t, opts)

	keepID := c.CaptureMessage("keep")
	dropID := c.CaptureMessage("drop me")
	panicID := c.CaptureMessage("panic")
	c.Flush(2 * time.Second)

	if keepID == "" || dropID != "" || panicID != "" {
		t.Errorf("ids keep=%q drop=%q panic=%q", keepID, dropID, panicID)
	}
	s := c.Stats()
	if s.Sent != 1 || s.Filtered != 2 {
		t.Errorf("stats = %+v, want 1 sent and 2 filtered (a panicking hook drops the event)", s)
	}
	if calls.Load() != 3 {
		t.Errorf("hook called %d times", calls.Load())
	}
}

func TestCloseAbandonsRetriesAndAccountsForEverything(t *testing.T) {
	fails := make([]int, 1000)
	for i := range fails {
		fails[i] = 503
	}
	ing := newScriptedIngest(t, fails...)
	opts := fastRetries(ing.dsn())
	opts.MaxRetryDuration = time.Hour
	opts.BatchSize = 5
	c, err := faultline.New(opts)
	if err != nil {
		t.Fatal(err)
	}
	for range 12 {
		c.CaptureMessage("x")
	}

	start := time.Now()
	ok := c.Close(300 * time.Millisecond)
	elapsed := time.Since(start)

	if ok {
		t.Error("Close reported success while every send fails")
	}
	if elapsed > time.Second {
		t.Errorf("Close took %v with a 300ms timeout; retries must stop when the client closes", elapsed)
	}
	s := c.Stats()
	if s.Captured != s.Sent+s.Failed+s.Queued+s.InFlight || s.InFlight != 0 || s.Queued != 0 {
		t.Errorf("stats don't account for every event after Close: %+v", s)
	}
	if s.Failed != 12 {
		t.Errorf("failed = %d, want all 12", s.Failed)
	}
}
