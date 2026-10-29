package loadgen

import (
	"context"
	"fmt"
	"io"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// Report is the outcome of one run. It is printed as text and can be saved
// as JSON, so runs can be compared later (step 3.2).
type Report struct {
	RunID       string        `json:"run_id"`
	Started     time.Time     `json:"started"`
	Interrupted bool          `json:"interrupted,omitempty"`
	Config      ConfigSummary `json:"config"`

	Generation Generation `json:"generation"`
	SDK        SDKTotals  `json:"sdk"`
	HTTP       HTTPStats  `json:"http"`
	// CaptureLatency is the time CaptureError took in the app's goroutine.
	// The SDK promises never to block the app; this is the check.
	CaptureLatency Summary `json:"capture_latency"`

	// FlushSec is how long the final SDK flush took after generation ended;
	// DrainSec is from the end of generation until storage held every
	// acknowledged event.
	FlushSec float64 `json:"flush_s"`
	DrainSec float64 `json:"drain_s,omitempty"`

	Lag        LagSummary   `json:"consumer_lag"`
	Store      *StoreResult `json:"storage,omitempty"`
	Accounting Accounting   `json:"accounting"`
	Timeline   []Sample     `json:"timeline"`
	Warnings   []string     `json:"warnings,omitempty"`
}

// ConfigSummary is the run's settings, for the record.
type ConfigSummary struct {
	Apps          int      `json:"apps"`
	Projects      []string `json:"projects"`
	Rate          float64  `json:"rate_per_s"`
	DurationSec   float64  `json:"duration_s"`
	RampSec       float64  `json:"ramp_s"`
	Bugs          int      `json:"bugs"`
	Zipf          float64  `json:"zipf"`
	BatchSize     int      `json:"sdk_batch_size,omitempty"`
	FlushInterval string   `json:"sdk_flush_interval,omitempty"`
}

func summarizeConfig(c Config) ConfigSummary {
	s := ConfigSummary{
		Apps: c.Apps, Rate: c.Rate, DurationSec: c.Duration.Seconds(), RampSec: c.Ramp.Seconds(),
		Bugs: c.Bugs, Zipf: c.Zipf, BatchSize: c.BatchSize,
	}
	if c.FlushInterval > 0 {
		s.FlushInterval = c.FlushInterval.String()
	}
	for _, p := range c.Projects {
		s.Projects = append(s.Projects, p.ID)
	}
	return s
}

// Generation is what the simulated apps produced.
type Generation struct {
	Generated    uint64  `json:"generated"`
	Expected     uint64  `json:"expected"`
	DurationSec  float64 `json:"duration_s"`
	AvgRate      float64 `json:"avg_rate_per_s"`
	DistinctBugs int     `json:"distinct_bugs"`
	Top5BugShare float64 `json:"top5_bug_share"`
}

// HTTPStats covers every request the SDKs sent to ingest.
type HTTPStats struct {
	Requests        uint64            `json:"requests"`
	Status          map[string]uint64 `json:"status"`
	TransportErrors uint64            `json:"transport_errors"`
	Latency         Summary           `json:"latency"`
}

// Accounting explains where every generated event went.
//
//	Generated = Stored + Missing
//	Missing   = DroppedBySDK + FailedInSDK - StoredDespiteFailed + Unexplained
//
// StoredDespiteFailed: the SDK gave up on a batch (for example a timeout)
// that ingest had in fact written; at-least-once delivery, not a problem.
// Unexplained is real loss and should be 0.
type Accounting struct {
	Generated           uint64 `json:"generated"`
	Stored              uint64 `json:"stored"`
	Missing             int64  `json:"missing"`
	DroppedBySDK        uint64 `json:"dropped_by_sdk"`
	FailedInSDK         uint64 `json:"failed_in_sdk"`
	StoredDespiteFailed int64  `json:"stored_despite_failed"`
	Unexplained         int64  `json:"unexplained_loss"`
}

// LagSummary is the consumer lag over the run.
type LagSummary struct {
	Max     int64 `json:"max"`
	AtEnd   int64 `json:"at_end"`
	Samples int   `json:"samples"`
	Errors  int   `json:"errors"`
}

// Sample is one second of the run.
type Sample struct {
	T          float64 `json:"t_s"`
	Phase      string  `json:"phase"`
	TargetRate float64 `json:"target_rate"`
	Generated  float64 `json:"generated_per_s"`
	Acked      float64 `json:"acked_per_s"`
	Status429  uint64  `json:"http_429_total"`
	// Lag is -1 when it couldn't be read.
	Lag    int64  `json:"lag"`
	Stored uint64 `json:"stored,omitempty"`
}

// sampler records a Sample every second and prints progress.
type sampler struct {
	cfg   Config
	apps  []*app
	start time.Time

	phase   atomic.Value // string
	stored  atomic.Uint64
	lastLag atomic.Int64

	mu      sync.Mutex
	samples []Sample
	lag     LagSummary
}

func newSampler(cfg Config, apps []*app, start time.Time) *sampler {
	s := &sampler{cfg: cfg, apps: apps, start: start}
	s.phase.Store("load")
	s.lastLag.Store(-1)
	return s
}

func (s *sampler) run(ctx context.Context) {
	t := time.NewTicker(time.Second)
	defer t.Stop()
	var prevGen, prevAcked uint64
	prevTime := s.start
	every := max(int(s.cfg.ProgressEvery/time.Second), 1)
	for n := 1; ; n++ {
		select {
		case <-ctx.Done():
			return
		case now := <-t.C:
			var gen, acked, s429 uint64
			for _, a := range s.apps {
				gen += a.generated.Load()
				acked += a.client.Stats().Sent
				a.mu.Lock()
				s429 += a.status[429]
				a.mu.Unlock()
			}
			lag := int64(-1)
			if s.cfg.Lag != nil {
				lctx, cancel := context.WithTimeout(ctx, 2*time.Second)
				v, err := s.cfg.Lag(lctx)
				cancel()
				s.mu.Lock()
				if err != nil {
					s.lag.Errors++
				} else {
					lag = v
					s.lag.Samples++
					s.lag.Max = max(s.lag.Max, v)
					s.lag.AtEnd = v
				}
				s.mu.Unlock()
				if err == nil {
					s.lastLag.Store(v)
				}
			}
			dt := now.Sub(prevTime).Seconds()
			elapsed := now.Sub(s.start)
			phase := s.phase.Load().(string)
			sample := Sample{
				T:         elapsed.Seconds(),
				Phase:     phase,
				Generated: float64(gen-prevGen) / dt,
				Acked:     float64(acked-prevAcked) / dt,
				Status429: s429,
				Lag:       lag,
				Stored:    s.stored.Load(),
			}
			if phase == "load" {
				sample.TargetRate = rateAt(elapsed, s.cfg.Rate, s.cfg.Ramp)
			}
			prevGen, prevAcked, prevTime = gen, acked, now
			s.mu.Lock()
			s.samples = append(s.samples, sample)
			s.mu.Unlock()

			if s.cfg.Progress != nil && n%every == 0 {
				line := fmt.Sprintf("%6.0fs  %-5s  target %7.0f/s  generated %7.0f/s  acked %7.0f/s  429s %d",
					sample.T, phase, sample.TargetRate, sample.Generated, sample.Acked, s429)
				if lag >= 0 {
					line += fmt.Sprintf("  lag %d", lag)
				}
				if sample.Stored > 0 {
					line += fmt.Sprintf("  stored %d", sample.Stored)
				}
				fmt.Fprintln(s.cfg.Progress, line)
			}
		}
	}
}

func (s *sampler) result() ([]Sample, LagSummary) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]Sample(nil), s.samples...), s.lag
}

// Print writes the report as text.
func (r *Report) Print(w io.Writer) {
	p := func(format string, args ...any) { fmt.Fprintf(w, format+"\n", args...) }
	ms := func(s Summary) string {
		if s.Count == 0 {
			return "no data"
		}
		return fmt.Sprintf("p50 %s  p90 %s  p95 %s  p99 %s  p99.9 %s  max %s  (n=%d)",
			fmtMs(s.P50Ms), fmtMs(s.P90Ms), fmtMs(s.P95Ms), fmtMs(s.P99Ms), fmtMs(s.P999), fmtMs(s.MaxMs), s.Count)
	}
	lat := func(l LatencyMs) string {
		return fmt.Sprintf("p50 %s  p90 %s  p95 %s  p99 %s  p99.9 %s  max %s",
			fmtMs(l.P50), fmtMs(l.P90), fmtMs(l.P95), fmtMs(l.P99), fmtMs(l.P999), fmtMs(l.Max))
	}

	p("")
	p("== loadgen run %s ==", r.RunID)
	if r.Interrupted {
		p("(interrupted: generation stopped early)")
	}
	c := r.Config
	p("config      %d apps over %s, target %.0f events/s for %gs (ramp %gs), %d bugs, zipf %.2f",
		c.Apps, strings.Join(c.Projects, ","), c.Rate, c.DurationSec, c.RampSec, c.Bugs, c.Zipf)
	g := r.Generation
	p("generated   %d events in %.1fs (%.0f/s avg; target total %d), %d distinct bugs, top 5 = %.0f%% of events",
		g.Generated, g.DurationSec, g.AvgRate, g.Expected, g.DistinctBugs, g.Top5BugShare*100)
	s := r.SDK
	p("sdk         captured %d  acknowledged %d  failed %d  dropped %d  retries %d", s.Captured, s.Sent, s.Failed, s.Dropped, s.Retries)
	p("capture     %s", ms(r.CaptureLatency))
	p("http        %d requests  status %s  transport errors %d", r.HTTP.Requests, fmtStatus(r.HTTP.Status), r.HTTP.TransportErrors)
	p("http lat    %s", ms(r.HTTP.Latency))
	if r.Lag.Samples > 0 || r.Lag.Errors > 0 {
		p("lag         max %d  at end %d  (%d samples, %d errors)", r.Lag.Max, r.Lag.AtEnd, r.Lag.Samples, r.Lag.Errors)
	}
	p("flush       %.1fs after generation", r.FlushSec)
	if r.DrainSec > 0 {
		p("drained     all acknowledged events stored %.1fs after generation ended", r.DrainSec)
	}
	if st := r.Store; st != nil {
		p("stored      %d events (%d rows, %d duplicate rows not merged yet), %d issues", st.Events, st.Rows, st.Duplicates, st.Issues)
		p("end-to-end  %s   (SDK capture -> ClickHouse, includes SDK batching)", lat(st.EndToEnd))
		p("pipeline    %s   (ingest -> ClickHouse)", lat(st.Pipeline))
		var top []string
		for _, b := range st.TopBugs {
			top = append(top, fmt.Sprintf("%s=%d", b.Bug, b.Events))
		}
		p("top bugs    %s", strings.Join(top, "  "))
		a := r.Accounting
		p("accounting  generated %d = stored %d + missing %d; missing explained by SDK dropped %d + failed %d; stored despite failed %d",
			a.Generated, a.Stored, a.Missing, a.DroppedBySDK, a.FailedInSDK, a.StoredDespiteFailed)
		if a.Unexplained == 0 {
			p("LOSS        0 (every acknowledged event is stored)")
		} else {
			p("LOSS        %d events unexplained; first missing seqs: %v", a.Unexplained, st.MissingSeqs)
		}
	}
	for _, w := range r.Warnings {
		p("WARNING     %s", w)
	}
}

func fmtMs(v float64) string {
	switch {
	case v >= 10000:
		return fmt.Sprintf("%.1fs", v/1000)
	case v >= 100:
		return fmt.Sprintf("%.0fms", v)
	case v >= 1:
		return fmt.Sprintf("%.1fms", v)
	default:
		return fmt.Sprintf("%.0fµs", v*1000)
	}
}

func fmtStatus(m map[string]uint64) string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	parts := make([]string, len(keys))
	for i, k := range keys {
		parts[i] = fmt.Sprintf("%s=%d", k, m[k])
	}
	if len(parts) == 0 {
		return "none"
	}
	return strings.Join(parts, " ")
}
