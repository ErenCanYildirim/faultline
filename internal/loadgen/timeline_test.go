package loadgen

import (
	"context"
	"testing"
	"time"
)

// The timeline records HTTP latency per second: every completed request
// lands in one second (requests after the last sample are not in it).
func TestTimelineHasLatencyPerSecond(t *testing.T) {
	q := &memQueue{}
	srv := newIngest(t, q)
	cfg := testConfig(srv, q, "test-timeline")
	cfg.Duration = 3 * time.Second
	rep, err := Run(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	var perSecond uint64
	sawP99 := false
	for _, s := range rep.Timeline {
		perSecond += s.Requests
		if s.P99Ms > 0 {
			sawP99 = true
		}
		if s.Requests == 0 && s.P99Ms != 0 {
			t.Errorf("t=%.0f: p99 %v without requests", s.T, s.P99Ms)
		}
	}
	if perSecond == 0 || perSecond > rep.HTTP.Requests || !sawP99 {
		t.Errorf("timeline has %d requests with latency (report total %d), p99 seen: %v", perSecond, rep.HTTP.Requests, sawP99)
	}
}

func TestReset(t *testing.T) {
	h := NewHistogram()
	h.Record(5 * time.Millisecond)
	h.Reset()
	if (h.Summarize() != Summary{}) {
		t.Errorf("after Reset: %+v", h.Summarize())
	}
	h.Record(2 * time.Millisecond)
	if s := h.Summarize(); s.Count != 1 || s.MinMs != 2 || s.MaxMs != 2 {
		t.Errorf("after Reset and one value: %+v", s)
	}
}
