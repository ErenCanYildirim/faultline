package loadgen

import (
	"strings"
	"testing"
)

func TestWritePlot(t *testing.T) {
	quiet := &Report{RunID: "run-a", Timeline: []Sample{
		{T: 1, Lag: 10, Acked: 100, Requests: 5, P99Ms: 20},
		{T: 2, Lag: -1, Acked: 100, Requests: 0}, // lag unreadable, no requests: gaps
		{T: 3, Lag: 30, Acked: 90, Requests: 4, P99Ms: 25},
	}}
	noisy := &Report{RunID: "run-b", Timeline: []Sample{
		{T: 1, Lag: -1, Status429: 0}, {T: 2, Lag: -1, Status429: 50}, {T: 3, Lag: -1, Status429: 150},
	}}
	var b strings.Builder
	err := WritePlot(&b, "Flood <test>", []PlotRun{
		{Name: "quiet", Report: quiet},
		{Name: "noisy", Offset: 15, Report: noisy},
	}, []string{"lag", "p99", "429"}, []PlotMark{{At: 15, Label: "flood starts"}})
	if err != nil {
		t.Fatal(err)
	}
	out := b.String()
	for _, want := range []string{"<svg", "Consumer lag", "Ingest latency p99", "Rate-limited", "quiet", "noisy", "flood starts", "Flood &lt;test&gt;", "run-a, run-b"} {
		if !strings.Contains(out, want) {
			t.Errorf("chart lacks %q", want)
		}
	}
	// Lag has a gap at t=2, so the quiet run's lag is two separate lines;
	// p99 likewise. The 429 panel has one line per run (the quiet run's
	// at zero), from the second sample on.
	if n := strings.Count(out, "<polyline"); n != 2+2+2 {
		t.Errorf("%d polylines, want 6", n)
	}
	if err := WritePlot(&b, "x", nil, []string{"nope"}, nil); err == nil {
		t.Error("unknown metric should fail")
	}
}

func TestNiceCeil(t *testing.T) {
	for in, want := range map[float64]float64{0: 0, 3: 5, 10: 10, 11: 20, 249: 500, 0.3: 0.5, 120000: 200000} {
		if got := niceCeil(in); got != want {
			t.Errorf("niceCeil(%v) = %v, want %v", in, got, want)
		}
	}
}

func TestNiceAxis(t *testing.T) {
	for _, c := range []struct{ max, end, step float64 }{
		{62, 70, 10}, {37000, 40000, 10000}, {150, 160, 20}, {0, 1, 0.25}, {7, 8, 2},
	} {
		n := 6
		if c.max == 37000 || c.max == 0 || c.max == 7 {
			n = 4
		}
		if end, step := niceAxis(c.max, n); end != c.end || step != c.step {
			t.Errorf("niceAxis(%v, %d) = %v, %v; want %v, %v", c.max, n, end, step, c.end, c.step)
		}
	}
	if fmtTick(12500) != "12.5k" || fmtTick(40000) != "40k" || fmtTick(1500000) != "1.5M" {
		t.Errorf("fmtTick: %s %s %s", fmtTick(12500), fmtTick(40000), fmtTick(1500000))
	}
}
