package worker

import (
	"context"
	"testing"
	"time"

	"github.com/erencanyildirim/faultline/internal/grouping"
)

type slowStore struct {
	*fakeStore
	delay time.Duration
}

func (s slowStore) InsertEvents(ctx context.Context, events []grouping.Grouped) error {
	time.Sleep(s.delay)
	return s.fakeStore.InsertEvents(ctx, events)
}

// Time spent in a stage shows up in that stage and nowhere else.
func TestStageTimesAttributeTheWait(t *testing.T) {
	h := newHarness(deliveries(t, 30))
	h.w.Store = slowStore{fakeStore: h.store, delay: 30 * time.Millisecond}
	stop := h.run(t)
	waitFor(t, "all committed", func() bool { return h.cons.committedCount() == 30 })
	_ = stop()

	s := h.w.StageStats()
	if s.Stored != 30 || s.Batches != 3 {
		t.Fatalf("stats = %+v", s)
	}
	for _, name := range []string{"poll", "decode", "dlq", "scrub", "group", "store", "commit"} {
		if _, ok := s.Seconds[name]; !ok {
			t.Errorf("stage %q missing", name)
		}
	}
	// Three inserts of 30ms each.
	if st := s.Seconds["store"]; st < 0.09 || st > 1 {
		t.Errorf("store = %.3fs, want about 0.09s", st)
	}
	if s.Seconds["decode"] >= s.Seconds["store"] || s.Seconds["scrub"] >= s.Seconds["store"] {
		t.Errorf("CPU stages should be far below the slow insert: %v", s.Seconds)
	}
	if s.Seconds["dlq"] != 0 {
		t.Errorf("no dead letters, but dlq = %v", s.Seconds["dlq"])
	}
}
