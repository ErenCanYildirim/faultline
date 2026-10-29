package loadgen

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"math"
	"net/http/httptest"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/erencanyildirim/faultline/event"
	"github.com/erencanyildirim/faultline/internal/ingest"
	"github.com/erencanyildirim/faultline/internal/queue"
)

func TestExpectedEvents(t *testing.T) {
	// No ramp: rate x time.
	if got := expectedEvents(2*time.Second, 100, 0); got != 200 {
		t.Errorf("no ramp: %v", got)
	}
	// Linear ramp over 4s to 100/s: area of the triangle is 200 at 4s,
	// 50 at 2s, then 100/s.
	for _, c := range []struct {
		at   time.Duration
		want float64
	}{{2 * time.Second, 50}, {4 * time.Second, 200}, {6 * time.Second, 400}} {
		if got := expectedEvents(c.at, 100, 4*time.Second); math.Abs(got-c.want) > 1e-9 {
			t.Errorf("at %v: %v, want %v", c.at, got, c.want)
		}
	}
	if rateAt(time.Second, 100, 4*time.Second) != 25 || rateAt(5*time.Second, 100, 4*time.Second) != 100 {
		t.Error("rateAt")
	}
}

// memQueue stands in for Redpanda. drop, if set, makes it acknowledge some
// messages without keeping them: the silent loss the run must detect.
type memQueue struct {
	mu   sync.Mutex
	msgs [][]byte
	drop func(i int) bool
	n    int
}

func (q *memQueue) Produce(_ context.Context, msgs []queue.Message) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	for _, m := range msgs {
		if q.drop == nil || !q.drop(q.n) {
			q.msgs = append(q.msgs, m.Value)
		}
		q.n++
	}
	return nil
}
func (q *memQueue) Ping(context.Context) error { return nil }
func (q *memQueue) Close()                     {}

// memStore reads the queue back as if it were ClickHouse.
type memStore struct {
	q     *memQueue
	runID string
}

func (s *memStore) seqs() map[string]bool {
	s.q.mu.Lock()
	defer s.q.mu.Unlock()
	out := map[string]bool{}
	for _, raw := range s.q.msgs {
		var e event.Event
		if json.Unmarshal(raw, &e) == nil && e.Tags["run_id"] == s.runID {
			out[e.Tags["seq"]] = true
		}
	}
	return out
}

func (s *memStore) Stored(context.Context) (uint64, error) { return uint64(len(s.seqs())), nil }

func (s *memStore) Verify(_ context.Context, generated uint64) (StoreResult, error) {
	seqs := s.seqs()
	res := StoreResult{Events: uint64(len(seqs)), Rows: uint64(len(seqs)), Seqs: uint64(len(seqs))}
	for i := uint64(0); i < generated && len(res.MissingSeqs) < 20; i++ {
		if !seqs[strconv.FormatUint(i, 10)] {
			res.MissingSeqs = append(res.MissingSeqs, i)
		}
	}
	return res, nil
}

const (
	keyA = "load-a-key-0123456789abcdef"
	keyB = "load-b-key-0123456789abcdef"
)

func newIngest(t *testing.T, q *memQueue) *httptest.Server {
	t.Helper()
	projects, err := ingest.ParseProjects([]byte(fmt.Sprintf(
		`{"projects":[{"id":"load-a","keys":[%q]},{"id":"load-b","keys":[%q]}]}`, keyA, keyB)))
	if err != nil {
		t.Fatal(err)
	}
	h := &ingest.Handler{Projects: projects, Producer: q}
	srv := httptest.NewServer(h.Routes())
	t.Cleanup(srv.Close)
	return srv
}

func testConfig(srv *httptest.Server, q *memQueue, runID string) Config {
	return Config{
		IngestURL:     srv.URL,
		Projects:      []Project{{"load-a", keyA}, {"load-b", keyB}},
		Apps:          4,
		Rate:          2000,
		Duration:      2 * time.Second,
		Ramp:          500 * time.Millisecond,
		Bugs:          50,
		Zipf:          1.2,
		FlushInterval: 200 * time.Millisecond,
		RunID:         runID,
		Store:         &memStore{q: q, runID: runID},
		DrainTimeout:  10 * time.Second,
	}
}

// The real SDK against the real ingest handler: every event is accounted
// for, and the numbers in the report agree with each other.
func TestRunAccountsForEveryEvent(t *testing.T) {
	q := &memQueue{}
	srv := newIngest(t, q)
	var progress bytes.Buffer
	cfg := testConfig(srv, q, "test-run-1")
	cfg.Progress = &progress
	cfg.ProgressEvery = time.Second

	rep, err := Run(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	g := rep.Generation
	// 2000/s for 2s with a 0.5s ramp: 3500 events.
	if g.Expected != 3500 || g.Generated < 3500 || g.Generated > 3504 {
		t.Errorf("generation = %+v, want 3500 (up to one more per app from rounding)", g)
	}
	if rep.SDK.Sent != g.Generated || rep.SDK.Failed != 0 || rep.SDK.Dropped != 0 {
		t.Errorf("sdk = %+v, generated %d", rep.SDK, g.Generated)
	}
	if rep.HTTP.Status["202"] == 0 || rep.HTTP.Status["202"] != rep.HTTP.Requests || rep.HTTP.Latency.Count != rep.HTTP.Requests {
		t.Errorf("http = %+v", rep.HTTP)
	}
	if rep.CaptureLatency.Count != g.Generated {
		t.Errorf("capture latency has %d values, want one per event (%d)", rep.CaptureLatency.Count, g.Generated)
	}
	if rep.Store == nil || rep.Store.Seqs != g.Generated {
		t.Fatalf("store = %+v", rep.Store)
	}
	if a := rep.Accounting; a.Unexplained != 0 || a.Missing != 0 || a.Stored != g.Generated {
		t.Errorf("accounting = %+v", a)
	}
	if g.DistinctBugs < 20 || g.Top5BugShare < 0.4 {
		t.Errorf("bug mix = %d distinct, top 5 %.2f", g.DistinctBugs, g.Top5BugShare)
	}
	if len(rep.Timeline) < 2 || progress.Len() == 0 {
		t.Errorf("timeline has %d samples, progress %q", len(rep.Timeline), progress.String())
	}
	if len(rep.Warnings) != 0 {
		t.Errorf("warnings: %v", rep.Warnings)
	}
	var out bytes.Buffer
	rep.Print(&out)
	if !bytes.Contains(out.Bytes(), []byte("LOSS        0")) {
		t.Errorf("report:\n%s", out.String())
	}
	t.Log(out.String())
}

// A queue that acknowledges but loses every 100th message: the run must
// count exactly those as lost and name them.
func TestRunDetectsSilentLoss(t *testing.T) {
	q := &memQueue{drop: func(i int) bool { return i%100 == 0 }}
	srv := newIngest(t, q)
	cfg := testConfig(srv, q, "test-run-2")
	cfg.DrainTimeout = 2 * time.Second

	rep, err := Run(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	lost := int64(q.n+99) / 100 // indexes 0, 100, 200, ...
	if rep.Accounting.Unexplained != lost {
		t.Errorf("unexplained loss = %d, want %d (accounting %+v)", rep.Accounting.Unexplained, lost, rep.Accounting)
	}
	if len(rep.Store.MissingSeqs) == 0 {
		t.Error("missing sequence numbers should be listed")
	}
	if len(rep.Warnings) == 0 {
		t.Error("loss must produce a warning")
	}
}

func TestRunStopsEarlyOnCancel(t *testing.T) {
	q := &memQueue{}
	srv := newIngest(t, q)
	cfg := testConfig(srv, q, "test-run-3")
	cfg.Duration = time.Minute
	cfg.Ramp = 0
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	start := time.Now()
	rep, err := Run(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	if time.Since(start) > 15*time.Second || !rep.Interrupted {
		t.Errorf("took %v, interrupted=%v", time.Since(start), rep.Interrupted)
	}
	if rep.Accounting.Unexplained != 0 || rep.Accounting.Stored != rep.Generation.Generated {
		t.Errorf("an interrupted run still flushes everything it generated: %+v", rep.Accounting)
	}
}

func TestConfigValidation(t *testing.T) {
	ok := Config{IngestURL: "http://x", Projects: []Project{{"p", "k"}}, Apps: 1, Rate: 1, Duration: time.Second, RunID: "r"}
	if err := ok.validate(); err != nil {
		t.Fatal(err)
	}
	for name, mod := range map[string]func(*Config){
		"no url":      func(c *Config) { c.IngestURL = "" },
		"no projects": func(c *Config) { c.Projects = nil },
		"no apps":     func(c *Config) { c.Apps = 0 },
		"no rate":     func(c *Config) { c.Rate = 0 },
		"ramp > dur":  func(c *Config) { c.Ramp = 2 * time.Second },
		"no run id":   func(c *Config) { c.RunID = "" },
		// Passes the Luhn check: the scrubber would store it as <card>.
		"card-like run id": func(c *Config) { c.RunID = "20261028-135758" },
	} {
		c := ok
		mod(&c)
		if err := c.validate(); err == nil {
			t.Errorf("%s: expected an error", name)
		}
	}
}
