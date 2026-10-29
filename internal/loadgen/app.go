package loadgen

import (
	"context"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	faultline "github.com/erencanyildirim/faultline/sdk"
)

// Project is one project the load is sent to.
type Project struct {
	ID  string
	Key string
}

// app is one simulated application: its own SDK client, its own HTTP
// connections, its own random stream. Separate apps share nothing but the
// sequence counter, as separate processes would.
type app struct {
	name    string
	project Project
	client  *faultline.Client
	bugs    *Bugs

	httpLatency    *Histogram // one value per HTTP request the SDK made
	captureLatency *Histogram // time spent inside CaptureError: the cost to the app

	mu            sync.Mutex
	status        map[int]uint64
	transportErrs uint64

	generated atomic.Uint64
	bugCounts []uint64 // only written by the app's own goroutine
}

// recordingTransport measures every request the SDK sends. The time runs
// until the response headers arrive, which for ingest is after the broker
// has acknowledged the events: the latency an SDK actually waits for.
type recordingTransport struct {
	next http.RoundTripper
	app  *app
}

func (t *recordingTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	start := time.Now()
	resp, err := t.next.RoundTrip(req)
	elapsed := time.Since(start)
	t.app.mu.Lock()
	defer t.app.mu.Unlock()
	if err != nil {
		t.app.transportErrs++
		return resp, err
	}
	t.app.httpLatency.Record(elapsed)
	t.app.status[resp.StatusCode]++
	return resp, err
}

func newApp(i int, p Project, cfg Config) (*app, error) {
	a := &app{
		name:           fmt.Sprintf("app-%03d", i),
		project:        p,
		httpLatency:    NewHistogram(),
		captureLatency: NewHistogram(),
		status:         map[int]uint64{},
		bugCounts:      make([]uint64, cfg.Bugs),
	}
	bugs, err := NewBugs(cfg.Bugs, cfg.Zipf, cfg.Seed+int64(i))
	if err != nil {
		return nil, err
	}
	a.bugs = bugs

	// Its own connection pool, like a separate process.
	base := http.DefaultTransport.(*http.Transport).Clone()
	base.MaxIdleConnsPerHost = 4
	dsn := strings.Replace(cfg.IngestURL, "://", "://"+p.Key+"@", 1)
	c, err := faultline.New(faultline.Options{
		DSN:           dsn,
		Release:       "loadgen@" + cfg.RunID,
		Environment:   "loadtest",
		BatchSize:     cfg.BatchSize,
		FlushInterval: cfg.FlushInterval,
		BufferSize:    cfg.BufferSize,
		HTTPClient: &http.Client{
			Timeout:   faultline.DefaultHTTPTimeout,
			Transport: &recordingTransport{next: base, app: a},
		},
	})
	if err != nil {
		return nil, err
	}
	a.client = c
	return a, nil
}

// run captures events at the app's share of the rate until ctx ends or the
// duration is over.
func (a *app) run(ctx context.Context, cfg Config, rate float64, start time.Time, seq *atomic.Uint64) {
	end := start.Add(cfg.Duration)
	var done uint64
	tick := 5 * time.Millisecond
	for {
		now := time.Now()
		if ctx.Err() != nil {
			return
		}
		if !now.Before(end) {
			// Top up to the exact total for the full duration, so a run
			// generates what its settings say, not a tick's worth less.
			for float64(done) < expectedEvents(cfg.Duration, rate, cfg.Ramp) {
				a.captureOne(cfg.RunID, seq.Add(1)-1)
				done++
			}
			return
		}
		target := expectedEvents(now.Sub(start), rate, cfg.Ramp)
		for float64(done) < target {
			a.captureOne(cfg.RunID, seq.Add(1)-1)
			done++
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(tick):
		}
	}
}

func (a *app) captureOne(runID string, seq uint64) {
	bug := a.bugs.Next()
	a.bugCounts[bug]++
	err := a.bugs.Error(bug)
	user := a.bugs.r.Intn(1000)
	u := faultline.User{ID: fmt.Sprintf("%s-user-%d", a.name, user)}
	if user%3 == 0 {
		// Some events carry personal data, so the worker's scrubber does
		// the work it would do in production.
		u.Email = fmt.Sprintf("user%d@example.com", user)
	}

	start := time.Now()
	a.client.CaptureError(err,
		faultline.WithTags(map[string]string{
			"run_id": runID,
			"seq":    strconv.FormatUint(seq, 10),
			"app":    a.name,
			"bug":    BugName(bug),
		}),
		faultline.WithUser(u),
	)
	a.captureLatency.Record(time.Since(start))
	a.generated.Add(1)
}

// expectedEvents is how many events should have been captured after
// elapsed, at the given full rate with a linear ramp-up: the rate grows
// from 0 to full over ramp, then stays there. It is the area under that
// rate curve.
func expectedEvents(elapsed time.Duration, rate float64, ramp time.Duration) float64 {
	t := elapsed.Seconds()
	r := ramp.Seconds()
	if r <= 0 {
		return rate * t
	}
	if t < r {
		return rate * t * t / (2 * r)
	}
	return rate*r/2 + rate*(t-r)
}

// rateAt is the target rate at elapsed, for progress output.
func rateAt(elapsed time.Duration, rate float64, ramp time.Duration) float64 {
	if ramp <= 0 || elapsed >= ramp {
		return rate
	}
	return rate * elapsed.Seconds() / ramp.Seconds()
}
