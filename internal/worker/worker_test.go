package worker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/erencanyildirim/faultline/event"
	"github.com/erencanyildirim/faultline/internal/queue"
)

// journal records the order of side effects across the fakes, so tests can
// assert "insert happened before commit".
type journal struct {
	mu      sync.Mutex
	entries []string
}

func (j *journal) add(s string) {
	j.mu.Lock()
	defer j.mu.Unlock()
	j.entries = append(j.entries, s)
}

func (j *journal) list() []string {
	j.mu.Lock()
	defer j.mu.Unlock()
	return append([]string(nil), j.entries...)
}

type fakeConsumer struct {
	j         *journal
	mu        sync.Mutex
	pending   []queue.Delivery
	committed []queue.Delivery
	closed    bool
}

func (c *fakeConsumer) Poll(ctx context.Context, max int) ([]queue.Delivery, error) {
	for {
		c.mu.Lock()
		if c.closed {
			c.mu.Unlock()
			return nil, queue.ErrClosed
		}
		if len(c.pending) > 0 {
			n := min(max, len(c.pending))
			out := c.pending[:n]
			c.pending = c.pending[n:]
			c.mu.Unlock()
			return out, nil
		}
		c.mu.Unlock()
		select {
		case <-ctx.Done():
			return nil, nil
		case <-time.After(time.Millisecond):
		}
	}
}

func (c *fakeConsumer) Commit(_ context.Context, ds []queue.Delivery) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.committed = append(c.committed, ds...)
	c.j.add(fmt.Sprintf("commit %d", len(ds)))
	return nil
}

func (c *fakeConsumer) Close() {}

func (c *fakeConsumer) committedCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.committed)
}

type fakeStore struct {
	j        *journal
	mu       sync.Mutex
	failNext int  // fail this many calls, then succeed
	failAll  bool // fail every call
	attempts int
	batches  [][]event.Event
}

func (s *fakeStore) InsertEvents(_ context.Context, events []event.Event) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.attempts++
	if s.failAll || s.failNext > 0 {
		s.failNext--
		s.j.add("insert failed")
		return errors.New("clickhouse unavailable")
	}
	s.batches = append(s.batches, append([]event.Event(nil), events...))
	s.j.add(fmt.Sprintf("insert %d", len(events)))
	return nil
}

func (s *fakeStore) stored() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := 0
	for _, b := range s.batches {
		n += len(b)
	}
	return n
}

type fakeDLQ struct {
	j        *journal
	mu       sync.Mutex
	failNext int
	letters  []queue.DeadLetter
}

func (d *fakeDLQ) WriteDeadLetters(_ context.Context, dls []queue.DeadLetter) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.failNext > 0 {
		d.failNext--
		d.j.add("dlq failed")
		return errors.New("broker unavailable")
	}
	d.letters = append(d.letters, dls...)
	d.j.add(fmt.Sprintf("dlq %d", len(dls)))
	return nil
}

func validValue(t *testing.T, n int) []byte {
	t.Helper()
	e := event.Event{
		EventID:    fmt.Sprintf("0192f3c4-8b2a-7c1d-9e4f-%012d", n),
		ProjectID:  "demo",
		Timestamp:  time.Date(2026, 10, 27, 12, 0, 0, 0, time.UTC),
		ReceivedAt: time.Date(2026, 10, 27, 12, 0, 1, 0, time.UTC),
		Level:      event.LevelError,
		Message:    fmt.Sprintf("event %d", n),
		SDK:        event.SDKInfo{Name: "faultline-go", Version: "0.1.0"},
	}
	b, err := json.Marshal(e)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func deliveries(t *testing.T, n int) []queue.Delivery {
	out := make([]queue.Delivery, n)
	for i := range out {
		out[i] = queue.Delivery{Value: validValue(t, i), Topic: "events.raw", Partition: int32(i % 3), Offset: int64(i)}
	}
	return out
}

type harness struct {
	j     *journal
	cons  *fakeConsumer
	store *fakeStore
	dlq   *fakeDLQ
	w     *Worker
}

func newHarness(pending []queue.Delivery) *harness {
	j := &journal{}
	h := &harness{
		j:     j,
		cons:  &fakeConsumer{j: j, pending: pending},
		store: &fakeStore{j: j},
		dlq:   &fakeDLQ{j: j},
	}
	h.w = &Worker{
		Consumer:        h.cons,
		DLQ:             h.dlq,
		Store:           h.store,
		BatchSize:       10,
		FlushInterval:   50 * time.Millisecond,
		ShutdownTimeout: 200 * time.Millisecond,
		RetryInitial:    time.Millisecond,
		RetryMax:        5 * time.Millisecond,
	}
	return h
}

// run starts the worker and returns a stop function that cancels it and
// waits for Run to return.
func (h *harness) run(t *testing.T) (stop func() error) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- h.w.Run(ctx) }()
	return func() error {
		cancel()
		select {
		case err := <-done:
			return err
		case <-time.After(5 * time.Second):
			t.Fatal("worker did not stop within 5s")
			return nil
		}
	}
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

func TestInsertsThenCommits(t *testing.T) {
	h := newHarness(deliveries(t, 10))
	stop := h.run(t)
	waitFor(t, "commit", func() bool { return h.cons.committedCount() == 10 })
	if err := stop(); err != nil {
		t.Fatal(err)
	}

	got := h.j.list()
	want := []string{"insert 10", "commit 10"}
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Errorf("side effects = %v, want %v", got, want)
	}
	if s := h.w.Stats(); s.Stored != 10 || s.Batches != 1 {
		t.Errorf("stats = %+v", s)
	}
}

func TestBatchesBySizeThenByTime(t *testing.T) {
	h := newHarness(deliveries(t, 25))
	stop := h.run(t)
	waitFor(t, "all committed", func() bool { return h.cons.committedCount() == 25 })
	_ = stop()

	h.store.mu.Lock()
	defer h.store.mu.Unlock()
	var sizes []int
	for _, b := range h.store.batches {
		sizes = append(sizes, len(b))
	}
	if fmt.Sprint(sizes) != "[10 10 5]" {
		t.Errorf("batch sizes = %v, want [10 10 5]: two full batches, then the rest when the interval expires", sizes)
	}
}

// The core guarantee: while storage fails, nothing is committed.
func TestNothingCommittedUntilInsertSucceeds(t *testing.T) {
	h := newHarness(deliveries(t, 10))
	h.store.failNext = 3
	stop := h.run(t)
	waitFor(t, "commit", func() bool { return h.cons.committedCount() == 10 })
	_ = stop()

	got := strings.Join(h.j.list(), ",")
	want := "insert failed,insert failed,insert failed,insert 10,commit 10"
	if got != want {
		t.Errorf("side effects = %s\nwant           %s", got, want)
	}
}

func TestStorageDownAtShutdownLeavesBatchUncommitted(t *testing.T) {
	h := newHarness(deliveries(t, 10))
	h.store.failAll = true
	stop := h.run(t)
	waitFor(t, "a few insert attempts", func() bool {
		h.store.mu.Lock()
		defer h.store.mu.Unlock()
		return h.store.attempts >= 3
	})
	if err := stop(); err != nil {
		t.Errorf("Run returned %v; shutdown with storage down should still be clean", err)
	}
	if n := h.cons.committedCount(); n != 0 {
		t.Errorf("committed %d messages that were never stored: that is data loss", n)
	}
}

func TestUnstorableMessagesGoToDLQAndAreCommitted(t *testing.T) {
	ds := deliveries(t, 1)
	badLevel := strings.Replace(string(validValue(t, 2)), `"level":"error"`, `"level":"critical"`, 1)
	noProject := strings.Replace(string(validValue(t, 3)), `"project_id":"demo",`, "", 1)
	ds = append(ds,
		queue.Delivery{Value: []byte("{not json"), Topic: "events.raw", Offset: 1},
		queue.Delivery{Value: []byte(badLevel), Topic: "events.raw", Offset: 2},
		queue.Delivery{Value: []byte(noProject), Topic: "events.raw", Offset: 3},
	)
	h := newHarness(ds)
	h.w.BatchSize = 4
	stop := h.run(t)
	waitFor(t, "commit", func() bool { return h.cons.committedCount() == 4 })
	_ = stop()

	if got := strings.Join(h.j.list(), ","); got != "dlq 3,insert 1,commit 4" {
		t.Errorf("side effects = %s, want dead letters, then insert, then commit", got)
	}
	reasons := []string{"not a valid event JSON", "level", "project_id"}
	for i, want := range reasons {
		if !strings.Contains(h.dlq.letters[i].Reason, want) {
			t.Errorf("dead letter %d reason = %q, want it to mention %q", i, h.dlq.letters[i].Reason, want)
		}
	}
	if s := h.w.Stats(); s.Stored != 1 || s.DeadLettered != 3 {
		t.Errorf("stats = %+v", s)
	}
}

func TestDeadLettersMustBeDurableBeforeCommit(t *testing.T) {
	ds := []queue.Delivery{{Value: []byte("garbage"), Topic: "events.raw"}}
	h := newHarness(ds)
	h.dlq.failNext = 2
	h.w.BatchSize = 1
	stop := h.run(t)
	waitFor(t, "commit", func() bool { return h.cons.committedCount() == 1 })
	_ = stop()

	if got := strings.Join(h.j.list(), ","); got != "dlq failed,dlq failed,dlq 1,commit 1" {
		t.Errorf("side effects = %s", got)
	}
}

func TestShutdownStoresTheBatchInHand(t *testing.T) {
	h := newHarness(deliveries(t, 3))
	h.w.BatchSize = 100
	h.w.FlushInterval = time.Hour // would never flush on its own
	stop := h.run(t)
	waitFor(t, "messages polled", func() bool {
		h.cons.mu.Lock()
		defer h.cons.mu.Unlock()
		return len(h.cons.pending) == 0
	})
	if err := stop(); err != nil {
		t.Fatal(err)
	}
	if h.store.stored() != 3 || h.cons.committedCount() != 3 {
		t.Errorf("stored %d, committed %d; want 3 and 3", h.store.stored(), h.cons.committedCount())
	}
}

func TestClosedConsumerEndsRun(t *testing.T) {
	h := newHarness(nil)
	h.cons.closed = true
	err := h.w.Run(context.Background())
	if !errors.Is(err, queue.ErrClosed) {
		t.Errorf("Run = %v, want ErrClosed", err)
	}
}
