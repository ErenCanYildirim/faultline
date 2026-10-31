//go:build integration

// Runs against a real ClickHouse:
//
//	go test -tags integration ./internal/loadgen/
//
// Uses CLICKHOUSE_URL (default http://localhost:8123) and its own database.
package loadgen

import (
	"context"
	"fmt"
	"math"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/erencanyildirim/faultline/event"
	"github.com/erencanyildirim/faultline/internal/clickhouse"
	"github.com/erencanyildirim/faultline/internal/grouping"
	"github.com/erencanyildirim/faultline/migrations"
)

func migratedClickHouse(t *testing.T) *clickhouse.Client {
	t.Helper()
	url := os.Getenv("CLICKHOUSE_URL")
	if url == "" {
		url = "http://localhost:8123"
	}
	ctx := context.Background()
	admin, err := clickhouse.New(clickhouse.Config{URL: url, User: "faultline", Password: "faultline"})
	if err != nil {
		t.Fatal(err)
	}
	if err := admin.Ping(ctx); err != nil {
		t.Fatalf("ClickHouse not reachable at %s (is the stack up?): %v", url, err)
	}
	db := fmt.Sprintf("faultline_loadgen_it_%d", time.Now().UnixNano())
	if err := admin.Exec(ctx, "CREATE DATABASE "+db); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = admin.Exec(context.Background(), "DROP DATABASE IF EXISTS "+db+" SYNC") })
	c, err := clickhouse.New(clickhouse.Config{URL: url, Database: db, User: "faultline", Password: "faultline"})
	if err != nil {
		t.Fatal(err)
	}
	list, err := clickhouse.LoadMigrations(migrations.ClickHouse, "clickhouse")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := clickhouse.Migrate(ctx, c, list); err != nil {
		t.Fatal(err)
	}
	return c
}

// 100 events numbered 0..99 with 7 and 42 missing, one event written twice,
// and another run's events in the same table that must not count.
func TestClickHouseStoreVerify(t *testing.T) {
	c := migratedClickHouse(t)
	ctx := context.Background()
	now := time.Now().UTC()
	bugs, _ := NewBugs(5, 1.2, 1)

	mk := func(runID string, seq int) event.Event {
		bug := seq % 5
		return event.Event{
			EventID:    fmt.Sprintf("0192f3c4-8b2a-7c1d-9e4f-%012d", seq+map[string]int{"r1": 0, "other": 1000}[runID]),
			ProjectID:  "load-1",
			Timestamp:  now.Add(-2 * time.Second), // captured 2s before insert
			ReceivedAt: now.Add(-1 * time.Second), // reached ingest 1s before insert
			Level:      "error",
			Exception: &event.Exception{Type: "*errors.errorString", Value: bugs.Error(bug).Error(),
				Frames: []event.Frame{{Module: "main", Function: "f", InApp: true}}},
			Tags: map[string]string{"run_id": runID, "seq": strconv.Itoa(seq), "bug": BugName(bug)},
			SDK:  event.SDKInfo{Name: "faultline-go", Version: "0"},
		}
	}
	var evs []event.Event
	for i := range 100 {
		if i == 7 || i == 42 {
			continue
		}
		evs = append(evs, mk("r1", i))
	}
	for i := range 10 {
		evs = append(evs, mk("other", i))
	}
	store := &clickhouse.EventStore{Client: c}
	if err := store.InsertEvents(ctx, grouping.Group(evs)); err != nil {
		t.Fatal(err)
	}
	// A redelivered event, written again as a separate insert.
	dup, err := clickhouse.EncodeEvents(grouping.Group([]event.Event{mk("r1", 3)}))
	if err != nil {
		t.Fatal(err)
	}
	if err := c.InsertJSONEachRow(ctx, clickhouse.EventsTable, dup, ""); err != nil {
		t.Fatal(err)
	}

	s := &ClickHouseStore{Client: c, RunID: "r1", Projects: []string{"load-1", "load-2"}, Since: now.Add(-time.Minute)}
	// The query log saw the two inserts into this database's events table.
	ins, err := s.InsertStats(ctx, now.Add(-time.Minute), time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if ins.Inserts != 2 || ins.DurationMaxMs <= 0 || ins.DurationTotalS <= 0 {
		t.Errorf("insert stats = %+v, want 2 inserts", ins)
	}
	stored, err := s.Stored(ctx)
	if err != nil || stored != 98 {
		t.Fatalf("Stored = %d, %v; want 98", stored, err)
	}
	res, err := s.Verify(ctx, 100)
	if err != nil {
		t.Fatal(err)
	}
	if res.Rows != 99 || res.Events != 98 || res.Duplicates != 1 || res.Seqs != 98 || res.Issues != 5 {
		t.Errorf("counts = %+v", res)
	}
	if fmt.Sprint(res.MissingSeqs) != "[7 42]" {
		t.Errorf("missing = %v, want [7 42]", res.MissingSeqs)
	}
	if len(res.TopBugs) != 5 || res.TopBugs[0].Events < 19 {
		t.Errorf("top bugs = %+v", res.TopBugs)
	}
	// inserted_at is set by ClickHouse a moment after now: about 2s and 1s.
	near := func(got, want float64) bool { return got >= want && got < want+5000 }
	if !near(res.EndToEnd.P50, 2000) || !near(res.Pipeline.P50, 1000) || math.Abs(res.EndToEnd.P50-res.Pipeline.P50-1000) > 1 {
		t.Errorf("latency: end-to-end %+v, pipeline %+v", res.EndToEnd, res.Pipeline)
	}
}
