// Package loadgen simulates many applications sending errors through the
// real SDK, and measures what happens to those events: HTTP latency at
// ingest, consumer lag in Redpanda, end-to-end latency into ClickHouse, and
// exact loss.
//
// Exact loss works by numbering: every event carries the run ID and a
// sequence number from one counter shared by all simulated apps. After the
// run, the sequence numbers missing from ClickHouse are the events that
// didn't make it, and the SDK's own counters say how many of those it
// dropped or gave up on knowingly. Anything beyond that is real loss.
package loadgen

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/erencanyildirim/faultline/internal/scrub"
	faultline "github.com/erencanyildirim/faultline/sdk"
)

// Config describes one run.
type Config struct {
	IngestURL string
	Projects  []Project
	// Apps is the number of simulated applications, spread round-robin
	// over the projects.
	Apps int
	// Rate is the total target in events per second, reached after Ramp.
	Rate     float64
	Duration time.Duration
	Ramp     time.Duration

	// Bugs is the number of distinct errors; Zipf (> 1) is the skew.
	Bugs int
	Zipf float64

	// SDK settings for every app; zero means the SDK default.
	BatchSize     int
	FlushInterval time.Duration
	BufferSize    int
	// CloseTimeout bounds the final flush of each SDK client.
	CloseTimeout time.Duration

	RunID string
	Seed  int64

	// Lag, if set, is sampled every second during the run.
	Lag func(ctx context.Context) (int64, error)
	// Store, if set, is read after the run to count loss and latency.
	Store        Store
	DrainTimeout time.Duration

	// Progress receives a line every ProgressEvery; nil for none.
	Progress      io.Writer
	ProgressEvery time.Duration
}

func (c *Config) validate() error {
	switch {
	case c.IngestURL == "":
		return errors.New("loadgen: ingest URL is required")
	case len(c.Projects) == 0:
		return errors.New("loadgen: at least one project is required")
	case c.Apps < 1:
		return errors.New("loadgen: at least one app is required")
	case c.Rate <= 0:
		return errors.New("loadgen: rate must be positive")
	case c.Duration <= 0:
		return errors.New("loadgen: duration must be positive")
	case c.Ramp < 0 || c.Ramp > c.Duration:
		return errors.New("loadgen: ramp must be between 0 and the duration")
	case c.RunID == "":
		return errors.New("loadgen: run ID is required")
	case scrub.Text(c.RunID) != c.RunID:
		// The worker scrubs tag values. A run ID like 20261028-135758 has
		// 14 digits with a separator, looks like a card number to the
		// scrubber about one time in ten, and would be stored as <card>:
		// the run would then find none of its events.
		return fmt.Errorf("loadgen: run ID %q would be changed by the PII scrubber (to %q); use letters in it", c.RunID, scrub.Text(c.RunID))
	}
	if c.Bugs == 0 {
		c.Bugs = 200
	}
	if c.Zipf == 0 {
		c.Zipf = 1.2
	}
	if c.CloseTimeout <= 0 {
		c.CloseTimeout = 30 * time.Second
	}
	if c.DrainTimeout <= 0 {
		c.DrainTimeout = 2 * time.Minute
	}
	if c.ProgressEvery <= 0 {
		c.ProgressEvery = 5 * time.Second
	}
	return nil
}

// Run executes the load test and returns its report. Cancelling ctx stops
// generating events early; the run still flushes, drains and reports.
func Run(ctx context.Context, cfg Config) (*Report, error) {
	if err := cfg.validate(); err != nil {
		return nil, err
	}

	apps := make([]*app, cfg.Apps)
	for i := range apps {
		a, err := newApp(i, cfg.Projects[i%len(cfg.Projects)], cfg)
		if err != nil {
			for _, made := range apps[:i] {
				made.client.Close(time.Second)
			}
			return nil, err
		}
		apps[i] = a
	}

	rep := &Report{RunID: cfg.RunID, Config: summarizeConfig(cfg)}
	var seq atomic.Uint64
	start := time.Now()
	rep.Started = start.UTC()

	sampler := newSampler(cfg, apps, start)
	samplerDone := make(chan struct{})
	samplerCtx, stopSampler := context.WithCancel(context.Background())
	go func() {
		defer close(samplerDone)
		sampler.run(samplerCtx)
	}()

	// Generate.
	perApp := cfg.Rate / float64(cfg.Apps)
	var wg sync.WaitGroup
	for _, a := range apps {
		wg.Add(1)
		go func() {
			defer wg.Done()
			a.run(ctx, cfg, perApp, start, &seq)
		}()
	}
	wg.Wait()
	genEnd := time.Now()
	rep.Generation.DurationSec = genEnd.Sub(start).Seconds()
	rep.Generation.Expected = uint64(expectedEvents(min(genEnd.Sub(start), cfg.Duration), cfg.Rate, cfg.Ramp))
	rep.Interrupted = ctx.Err() != nil
	sampler.phase.Store("flush")

	// Flush every SDK client.
	var cwg sync.WaitGroup
	var unflushed atomic.Int32
	for _, a := range apps {
		cwg.Add(1)
		go func() {
			defer cwg.Done()
			if !a.client.Close(cfg.CloseTimeout) {
				unflushed.Add(1)
			}
		}()
	}
	cwg.Wait()
	rep.FlushSec = time.Since(genEnd).Seconds()
	if n := unflushed.Load(); n > 0 {
		rep.Warnings = append(rep.Warnings, fmt.Sprintf("%d SDK clients did not flush within %v; their remaining events count as failed", n, cfg.CloseTimeout))
	}

	rep.collectApps(apps)
	rep.Generation.Generated = seq.Load()
	if rep.Generation.DurationSec > 0 {
		rep.Generation.AvgRate = float64(rep.Generation.Generated) / rep.Generation.DurationSec
	}
	if rep.Generation.Expected > 0 && float64(rep.Generation.Generated) < 0.98*float64(rep.Generation.Expected) {
		rep.Warnings = append(rep.Warnings, fmt.Sprintf(
			"generated %d of %d target events: the load generator itself could not keep up; results understate the target rate",
			rep.Generation.Generated, rep.Generation.Expected))
	}

	// Wait until everything acknowledged is stored.
	sampler.phase.Store("drain")
	if cfg.Store != nil {
		rep.drain(cfg, sampler, genEnd)
	}
	stopSampler()
	<-samplerDone
	rep.Timeline, rep.Lag = sampler.result()

	if cfg.Store != nil && rep.Store == nil {
		vctx, cancel := context.WithTimeout(context.Background(), time.Minute)
		res, err := cfg.Store.Verify(vctx, rep.Generation.Generated)
		cancel()
		if err != nil {
			rep.Warnings = append(rep.Warnings, "reading results from storage failed: "+err.Error())
		} else {
			rep.Store = &res
			rep.account()
		}
	}
	return rep, nil
}

// drain polls storage until every event the SDK saw acknowledged is there.
func (r *Report) drain(cfg Config, s *sampler, genEnd time.Time) {
	ctx, cancel := context.WithTimeout(context.Background(), cfg.DrainTimeout)
	defer cancel()
	var last uint64
	lastChange := time.Now()
	for {
		stored, err := cfg.Store.Stored(ctx)
		if err == nil {
			s.stored.Store(stored)
			if stored >= r.SDK.Sent {
				r.DrainSec = time.Since(genEnd).Seconds()
				return
			}
			if stored != last {
				last, lastChange = stored, time.Now()
			}
			// Nothing new for a while and nothing left in Redpanda: whatever
			// is missing isn't coming.
			if time.Since(lastChange) > 15*time.Second && s.lastLag.Load() == 0 {
				r.Warnings = append(r.Warnings, fmt.Sprintf("storage stopped at %d of %d acknowledged events with no consumer lag", stored, r.SDK.Sent))
				return
			}
		}
		select {
		case <-ctx.Done():
			r.Warnings = append(r.Warnings, fmt.Sprintf("drain timed out after %v: %d of %d acknowledged events stored", cfg.DrainTimeout, last, r.SDK.Sent))
			return
		case <-time.After(time.Second):
		}
	}
}

// account explains every missing event or reports it as lost.
func (r *Report) account() {
	gen := r.Generation.Generated
	missing := int64(gen) - int64(r.Store.Seqs)
	explained := int64(r.SDK.Dropped + r.SDK.Failed)
	r.Accounting = Accounting{
		Generated:           gen,
		Stored:              r.Store.Seqs,
		Missing:             missing,
		DroppedBySDK:        r.SDK.Dropped,
		FailedInSDK:         r.SDK.Failed,
		Unexplained:         max(missing-explained, 0),
		StoredDespiteFailed: max(explained-missing, 0),
	}
	if r.Accounting.Unexplained > 0 {
		r.Warnings = append(r.Warnings, fmt.Sprintf("%d events lost: acknowledged by ingest but not in storage", r.Accounting.Unexplained))
	}
}

func (r *Report) collectApps(apps []*app) {
	httpAll, capAll := NewHistogram(), NewHistogram()
	status := map[int]uint64{}
	bugs := map[int]uint64{}
	for _, a := range apps {
		s := a.client.Stats()
		r.SDK.add(s)
		httpAll.Merge(a.httpLatency)
		capAll.Merge(a.captureLatency)
		a.mu.Lock()
		for code, n := range a.status {
			status[code] += n
		}
		r.HTTP.TransportErrors += a.transportErrs
		a.mu.Unlock()
		for b, n := range a.bugCounts {
			bugs[b] += n
		}
	}
	r.HTTP.Latency = httpAll.Summarize()
	r.HTTP.Requests = r.HTTP.Latency.Count + r.HTTP.TransportErrors
	r.HTTP.Status = map[string]uint64{}
	for code, n := range status {
		r.HTTP.Status[strconv.Itoa(code)] = n
	}
	r.CaptureLatency = capAll.Summarize()

	// Generated mix: share of the most common bugs, to confirm the skew.
	type kv struct {
		bug int
		n   uint64
	}
	var all []kv
	var total uint64
	for b, n := range bugs {
		all = append(all, kv{b, n})
		total += n
	}
	sort.Slice(all, func(i, j int) bool { return all[i].n > all[j].n })
	var top5 uint64
	for i := 0; i < len(all) && i < 5; i++ {
		top5 += all[i].n
	}
	distinct := 0
	for _, e := range all {
		if e.n > 0 {
			distinct++
		}
	}
	r.Generation.DistinctBugs = distinct
	if total > 0 {
		r.Generation.Top5BugShare = float64(top5) / float64(total)
	}
}

// SDKTotals sums the SDK counters over every app.
type SDKTotals struct {
	Captured uint64 `json:"captured"`
	Sent     uint64 `json:"acknowledged"`
	Failed   uint64 `json:"failed"`
	Dropped  uint64 `json:"dropped"`
	Retries  uint64 `json:"retries"`
}

func (t *SDKTotals) add(s faultline.Stats) {
	t.Captured += s.Captured
	t.Sent += s.Sent
	t.Failed += s.Failed
	t.Dropped += s.Dropped
	t.Retries += s.Retries
}
