package faultline_test

import (
	"compress/gzip"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/erencanyildirim/faultline/event"
	faultline "github.com/erencanyildirim/faultline/sdk"
)

const testKey = "test-key-0123456789"

// fakeIngest records what the SDK sends.
type fakeIngest struct {
	t        *testing.T
	srv      *httptest.Server
	mu       sync.Mutex
	requests int
	events   []event.Event
	keys     []string
	respond  func(w http.ResponseWriter, n int) // nil: 202 accepting everything
	hang     chan struct{}                      // non-nil: block until closed
}

func newFakeIngest(t *testing.T) *fakeIngest {
	f := &fakeIngest{t: t}
	f.srv = httptest.NewServer(http.HandlerFunc(f.handle))
	t.Cleanup(func() {
		if f.hang != nil {
			select {
			case <-f.hang:
			default:
				close(f.hang)
			}
		}
		f.srv.Close()
	})
	return f
}

func (f *fakeIngest) handle(w http.ResponseWriter, r *http.Request) {
	if f.hang != nil {
		<-f.hang
		return
	}
	if r.URL.Path != event.EventsPath {
		http.NotFound(w, r)
		return
	}
	var body io.Reader = r.Body
	if r.Header.Get("Content-Encoding") == "gzip" {
		gz, err := gzip.NewReader(r.Body)
		if err != nil {
			f.t.Errorf("bad gzip: %v", err)
			return
		}
		body = gz
	}
	var b event.Batch
	if err := json.NewDecoder(body).Decode(&b); err != nil {
		f.t.Errorf("bad batch json: %v", err)
		return
	}
	f.mu.Lock()
	f.requests++
	f.events = append(f.events, b.Events...)
	f.keys = append(f.keys, r.Header.Get(event.KeyHeader))
	f.mu.Unlock()

	if f.respond != nil {
		f.respond(w, len(b.Events))
		return
	}
	w.WriteHeader(http.StatusAccepted)
	_ = json.NewEncoder(w).Encode(event.BatchResponse{Accepted: len(b.Events)})
}

func (f *fakeIngest) dsn() string {
	return strings.Replace(f.srv.URL, "http://", "http://"+testKey+"@", 1)
}

func (f *fakeIngest) received() ([]event.Event, int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]event.Event(nil), f.events...), f.requests
}

func newClient(t *testing.T, opts faultline.Options) *faultline.Client {
	t.Helper()
	c, err := faultline.New(opts)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { c.Close(time.Second) })
	return c
}

// loadCart exists so the captured stack has a known innermost frame.
func loadCart(c *faultline.Client) string {
	err := fmt.Errorf("load cart: %w", errors.New("not found"))
	return c.CaptureError(err, faultline.WithTag("route", "/checkout"))
}

func TestCaptureErrorIsDeliveredWithStack(t *testing.T) {
	ing := newFakeIngest(t)
	c := newClient(t, faultline.Options{DSN: ing.dsn(), Release: "app@1.0", Environment: "test"})

	id := loadCart(c)
	if id == "" {
		t.Fatal("CaptureError returned empty id")
	}
	if !c.Flush(2 * time.Second) {
		t.Fatal("Flush timed out")
	}

	events, _ := ing.received()
	if len(events) != 1 {
		t.Fatalf("received %d events, want 1", len(events))
	}
	e := events[0]
	if e.EventID != id {
		t.Errorf("event_id = %q, want %q", e.EventID, id)
	}
	if e.Level != event.LevelError || e.Release != "app@1.0" || e.Environment != "test" {
		t.Errorf("level/release/env = %q/%q/%q", e.Level, e.Release, e.Environment)
	}
	if e.SDK.Name != faultline.SDKName {
		t.Errorf("sdk = %+v", e.SDK)
	}
	if e.Tags["route"] != "/checkout" {
		t.Errorf("tags = %v", e.Tags)
	}
	if e.Exception == nil || e.Exception.Type != "*errors.errorString" || e.Exception.Value != "load cart: not found" {
		t.Fatalf("exception = %+v", e.Exception)
	}
	f0 := e.Exception.Frames[0]
	if f0.Function != "loadCart" || !f0.InApp {
		t.Errorf("frame 0 = %+v, want in-app loadCart (SDK frames must be stripped)", f0)
	}
	if !strings.HasSuffix(f0.Module, "/sdk_test") {
		t.Errorf("frame 0 module = %q", f0.Module)
	}
	if e.ProjectID != "" || !e.ReceivedAt.IsZero() {
		t.Errorf("SDK must not set server-owned fields: %q %v", e.ProjectID, e.ReceivedAt)
	}
	if ing.keys[0] != testKey {
		t.Errorf("key header = %q", ing.keys[0])
	}
}

func TestCaptureMessage(t *testing.T) {
	ing := newFakeIngest(t)
	c := newClient(t, faultline.Options{DSN: ing.dsn()})

	c.CaptureMessage("slow payment", faultline.WithLevel(faultline.LevelWarning),
		faultline.WithUser(faultline.User{ID: "u1"}))
	c.Flush(2 * time.Second)

	events, _ := ing.received()
	if len(events) != 1 {
		t.Fatalf("received %d events", len(events))
	}
	e := events[0]
	if e.Message != "slow payment" || e.Level != event.LevelWarning || e.Exception != nil {
		t.Errorf("event = %+v", e)
	}
	if e.User == nil || e.User.ID != "u1" {
		t.Errorf("user = %+v", e.User)
	}
}

func TestBatchesBySizeAndFlushSendsTheRest(t *testing.T) {
	ing := newFakeIngest(t)
	c := newClient(t, faultline.Options{DSN: ing.dsn(), BatchSize: 10, FlushInterval: time.Hour})

	for i := range 25 {
		c.CaptureMessage(fmt.Sprintf("m%d", i))
	}
	if !c.Flush(2 * time.Second) {
		t.Fatal("Flush timed out")
	}
	events, requests := ing.received()
	if len(events) != 25 {
		t.Errorf("received %d events, want 25", len(events))
	}
	if requests != 3 {
		t.Errorf("requests = %d, want 3 (10 + 10 + 5)", requests)
	}
	if s := c.Stats(); s.Sent != 25 || s.Captured != 25 || s.Failed != 0 || s.Dropped != 0 ||
		s.Queued != 0 || s.InFlight != 0 {
		t.Errorf("stats = %+v", s)
	}
}

func TestFlushIntervalSendsPartialBatch(t *testing.T) {
	ing := newFakeIngest(t)
	c := newClient(t, faultline.Options{DSN: ing.dsn(), BatchSize: 50, FlushInterval: 50 * time.Millisecond})

	c.CaptureMessage("one")
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if events, _ := ing.received(); len(events) == 1 {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("partial batch was not sent by the flush interval")
}

// The core promise: a hung backend never blocks the application.
func TestCaptureNeverBlocksWhenBackendHangs(t *testing.T) {
	ing := newFakeIngest(t)
	ing.hang = make(chan struct{})
	c := newClient(t, faultline.Options{
		DSN:        ing.dsn(),
		BufferSize: 10,
		BatchSize:  5,
		HTTPClient: &http.Client{Timeout: 10 * time.Second},
	})

	start := time.Now()
	for range 1000 {
		c.CaptureError(errors.New("boom"))
	}
	elapsed := time.Since(start)

	if elapsed > 500*time.Millisecond {
		t.Errorf("1000 captures took %v with a hung backend; capture must not block", elapsed)
	}
	s := c.Stats()
	if s.Dropped == 0 {
		t.Errorf("expected drops with a full buffer, stats = %+v", s)
	}
	if s.Captured+s.Dropped != 1000 {
		t.Errorf("captured %d + dropped %d != 1000", s.Captured, s.Dropped)
	}
	// Buffer (10) plus at most one batch (5) taken by the sender.
	if s.Captured > 15 {
		t.Errorf("captured %d, more than buffer + one batch: memory is not bounded", s.Captured)
	}
	// Nothing was sent, so every captured event is queued or in flight.
	if s.Captured != s.Queued+s.InFlight || s.InFlight == 0 {
		t.Errorf("stats don't add up while the backend hangs: %+v", s)
	}
	close(ing.hang)
}

func TestFlushRespectsDeadline(t *testing.T) {
	ing := newFakeIngest(t)
	ing.hang = make(chan struct{})
	c := newClient(t, faultline.Options{DSN: ing.dsn(), HTTPClient: &http.Client{Timeout: 10 * time.Second}})

	c.CaptureMessage("stuck")
	start := time.Now()
	ok := c.Flush(100 * time.Millisecond)
	elapsed := time.Since(start)

	if ok {
		t.Error("Flush reported success while the backend hangs")
	}
	if elapsed > 300*time.Millisecond {
		t.Errorf("Flush(100ms) took %v", elapsed)
	}
	close(ing.hang)
}

func TestBackendDownCountsFailedAfterRetryTime(t *testing.T) {
	ing := newFakeIngest(t)
	dsn := ing.dsn()
	ing.srv.Close() // nothing listens on this address any more

	c := newClient(t, faultline.Options{
		DSN:                 dsn,
		MaxRetryDuration:    200 * time.Millisecond,
		RetryInitialBackoff: 10 * time.Millisecond,
		RetryMaxBackoff:     20 * time.Millisecond,
	})
	c.CaptureMessage("lost")
	c.Flush(2 * time.Second)

	s := c.Stats()
	if s.Failed != 1 || s.Sent != 0 {
		t.Errorf("stats = %+v, want 1 failed once retry time ran out", s)
	}
	if s.Retries == 0 {
		t.Errorf("stats = %+v, want retries before giving up", s)
	}
}

func TestRejectedEventsAreCountedAsFailed(t *testing.T) {
	ing := newFakeIngest(t)
	ing.respond = func(w http.ResponseWriter, n int) {
		w.WriteHeader(http.StatusAccepted)
		_ = json.NewEncoder(w).Encode(event.BatchResponse{
			Accepted: n - 1,
			Rejected: []event.Rejection{{Index: 0, Error: "invalid"}},
		})
	}
	c := newClient(t, faultline.Options{DSN: ing.dsn()})
	c.CaptureMessage("a")
	c.CaptureMessage("b")
	c.Flush(2 * time.Second)

	if s := c.Stats(); s.Sent != 1 || s.Failed != 1 {
		t.Errorf("stats = %+v, want 1 sent and 1 failed", s)
	}
}

func TestCloseIsIdempotentAndLaterCapturesAreDropped(t *testing.T) {
	ing := newFakeIngest(t)
	c, err := faultline.New(faultline.Options{DSN: ing.dsn()})
	if err != nil {
		t.Fatal(err)
	}
	c.CaptureMessage("before close")
	if !c.Close(2 * time.Second) {
		t.Fatal("Close did not flush in time")
	}
	c.Close(time.Second)

	if id := c.CaptureMessage("after close"); id != "" {
		t.Errorf("capture after Close returned %q", id)
	}
	events, _ := ing.received()
	if len(events) != 1 {
		t.Errorf("received %d events, want 1", len(events))
	}
	if s := c.Stats(); s.Dropped != 1 {
		t.Errorf("stats = %+v, want 1 dropped", s)
	}
}

func panicsWithNilMap() {
	var m map[string]int
	m["x"] = 1
}

func TestRecoverCapturesAndRepanics(t *testing.T) {
	ing := newFakeIngest(t)
	c := newClient(t, faultline.Options{DSN: ing.dsn()})

	var repanicked any
	func() {
		defer func() { repanicked = recover() }()
		defer c.Recover()
		panicsWithNilMap()
	}()

	if repanicked == nil {
		t.Fatal("Recover swallowed the panic; it must re-panic")
	}
	events, _ := ing.received() // Recover already flushed
	if len(events) != 1 {
		t.Fatalf("received %d events, want 1", len(events))
	}
	e := events[0]
	if e.Level != event.LevelFatal || e.Tags["mechanism"] != "panic" {
		t.Errorf("level %q tags %v", e.Level, e.Tags)
	}
	if !strings.HasPrefix(e.Exception.Type, "runtime.") {
		t.Errorf("exception type = %q", e.Exception.Type)
	}
	if f0 := e.Exception.Frames[0]; f0.Function != "panicsWithNilMap" {
		t.Errorf("frame 0 = %+v, want panicsWithNilMap (runtime and SDK frames stripped)", f0)
	}
}

func TestPackageLevelRecover(t *testing.T) {
	ing := newFakeIngest(t)
	if err := faultline.Init(faultline.Options{DSN: ing.dsn()}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { faultline.Close(time.Second) })

	func() {
		defer func() { _ = recover() }()
		defer faultline.Recover()
		panic("checkout exploded")
	}()

	events, _ := ing.received()
	if len(events) != 1 {
		t.Fatalf("received %d events, want 1", len(events))
	}
	if ex := events[0].Exception; ex.Type != "string" || ex.Value != "checkout exploded" {
		t.Errorf("exception = %+v", ex)
	}
	if fn := events[0].Exception.Frames[0].Function; !strings.HasPrefix(fn, "TestPackageLevelRecover") {
		t.Errorf("frame 0 function = %q", fn)
	}
}
