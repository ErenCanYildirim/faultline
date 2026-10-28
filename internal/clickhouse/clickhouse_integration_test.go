//go:build integration

// Runs against a real ClickHouse. Start the stack first, then:
//
//	go test -tags integration ./internal/clickhouse/
//
// Uses CLICKHOUSE_URL (default http://localhost:8123). Each run creates its
// own database and drops it at the end, so the real data is never touched.
package clickhouse

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/erencanyildirim/faultline/event"
	"github.com/erencanyildirim/faultline/internal/grouping"
	"github.com/erencanyildirim/faultline/migrations"
)

func testClient(t *testing.T) *Client {
	t.Helper()
	url := os.Getenv("CLICKHOUSE_URL")
	if url == "" {
		url = "http://localhost:8123"
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	admin, err := New(Config{URL: url, User: "faultline", Password: "faultline"})
	if err != nil {
		t.Fatal(err)
	}
	if err := admin.Ping(ctx); err != nil {
		t.Fatalf("ClickHouse not reachable at %s (is the stack up?): %v", url, err)
	}
	db := fmt.Sprintf("faultline_it_%d", time.Now().UnixNano())
	if err := admin.Exec(ctx, "CREATE DATABASE "+db); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = admin.Exec(context.Background(), "DROP DATABASE IF EXISTS "+db+" SYNC") })

	c, err := New(Config{URL: url, Database: db, User: "faultline", Password: "faultline"})
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func count(t *testing.T, c *Client, query string) string {
	t.Helper()
	out, err := c.Query(context.Background(), query)
	if err != nil {
		t.Fatalf("%s: %v", query, err)
	}
	return strings.TrimSpace(string(out))
}

func TestMigrateIsIdempotent(t *testing.T) {
	c := testClient(t)
	ctx := context.Background()
	list, err := LoadMigrations(migrations.ClickHouse, "clickhouse")
	if err != nil {
		t.Fatal(err)
	}

	applied, err := Migrate(ctx, c, list)
	if err != nil {
		t.Fatalf("first run: %v", err)
	}
	if len(applied) != len(list) {
		t.Errorf("first run applied %d of %d", len(applied), len(list))
	}
	applied, err = Migrate(ctx, c, list)
	if err != nil {
		t.Fatalf("second run: %v", err)
	}
	if len(applied) != 0 {
		t.Errorf("second run applied %v; it must be a no-op", applied)
	}
}

// The property the whole delivery model rests on: inserting the same events
// twice leaves one copy each after deduplication.
func TestDuplicateInsertsCollapse(t *testing.T) {
	c := testClient(t)
	ctx := context.Background()
	list, _ := LoadMigrations(migrations.ClickHouse, "clickhouse")
	if _, err := Migrate(ctx, c, list); err != nil {
		t.Fatal(err)
	}

	events := make([]event.Event, 50)
	for i := range events {
		e := sampleEvent()
		e.EventID = fmt.Sprintf("0192f3c4-8b2a-7c1d-9e4f-%012d", i)
		events[i] = e
	}
	store := &EventStore{Client: c}
	grouped := grouping.Group(events)

	// 1. The worker retries the exact same batch: the token stops the copy.
	for range 2 {
		if err := store.InsertEvents(ctx, grouped); err != nil {
			t.Fatalf("insert: %v", err)
		}
	}
	if got := count(t, c, "SELECT count() FROM events"); got != "50" {
		t.Errorf("raw rows after an identical retry = %s, want 50: the dedup token should have stopped it", got)
	}

	// 2. After a crash the same events come back in differently shaped
	//    batches: new tokens, so the rows are written again.
	for _, part := range [][]grouping.Grouped{grouped[:20], grouped[20:]} {
		if err := store.InsertEvents(ctx, part); err != nil {
			t.Fatalf("insert: %v", err)
		}
	}
	if got := count(t, c, "SELECT count() FROM events"); got != "100" {
		t.Errorf("raw rows after redelivery in new batches = %s, want 100 before any merge", got)
	}
	if got := count(t, c, "SELECT count() FROM events FINAL"); got != "50" {
		t.Errorf("rows with FINAL = %s, want 50", got)
	}
	// A forced merge removes the copies physically.
	if err := c.Exec(ctx, "OPTIMIZE TABLE events FINAL"); err != nil {
		t.Fatal(err)
	}
	if got := count(t, c, "SELECT count() FROM events"); got != "50" {
		t.Errorf("raw rows after merge = %s, want 50", got)
	}

	// All 50 sample events share a stack, type and message: one issue.
	if got := count(t, c, "SELECT uniqExact(fingerprint) FROM events WHERE fingerprint != ''"); got != "1" {
		t.Errorf("distinct fingerprints = %s, want 1", got)
	}

	// The stored row matches what was sent.
	row := count(t, c, `SELECT project_id, exception_type, frames.function, frames.in_app, tags['route'], user_id
		FROM events WHERE event_id = '0192f3c4-8b2a-7c1d-9e4f-000000000007' FORMAT TSV`)
	want := "demo\t*fs.PathError\t['load','main']\t[true,false]\t/checkout\tu1"
	if row != want {
		t.Errorf("stored row = %q\nwant         %q", row, want)
	}
}

func migrated(t *testing.T) (*Client, *EventStore) {
	t.Helper()
	c := testClient(t)
	list, err := LoadMigrations(migrations.ClickHouse, "clickhouse")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := Migrate(context.Background(), c, list); err != nil {
		t.Fatal(err)
	}
	return c, &EventStore{Client: c}
}

// makeEvents builds n events of one kind, a minute apart starting at start.
func makeEvents(idBase, n int, start time.Time, mutate func(*event.Event)) []event.Event {
	out := make([]event.Event, n)
	for i := range out {
		e := sampleEvent()
		e.EventID = fmt.Sprintf("0192f3c4-8b2a-7c1d-9e4f-%012d", idBase+i)
		e.Timestamp = start.Add(time.Duration(i) * time.Minute)
		if mutate != nil {
			mutate(&e)
		}
		out[i] = e
	}
	return out
}

func TestIssuesAggregateEvents(t *testing.T) {
	c, store := migrated(t)
	ctx := context.Background()
	start := time.Date(2026, 10, 27, 10, 0, 0, 0, time.UTC)

	pathErrs := makeEvents(0, 90, start, func(e *event.Event) {
		e.Exception.Frames = []event.Frame{
			{Module: "runtime", Function: "goexit"},
			{Module: "shop/cart", Function: "(*Store).Load", InApp: true},
		}
	})
	pathErrs[89].Level = event.LevelFatal // the latest event escalates
	messages := makeEvents(1000, 5, start.Add(30*time.Minute), func(e *event.Event) {
		e.Exception = nil
		e.Message = "payment provider slow: 2300ms"
		e.Level = event.LevelWarning
	})
	noAppFrames := makeEvents(2000, 3, start, func(e *event.Event) {
		e.Exception.Type = "*json.SyntaxError"
		e.Exception.Frames = []event.Frame{{Module: "encoding/json", Function: "(*decodeState).object"}}
	})
	all := append(append(pathErrs, messages...), noAppFrames...)
	if err := store.InsertEvents(ctx, grouping.Group(all)); err != nil {
		t.Fatal(err)
	}

	got := count(t, c, `SELECT title, culprit, level, events, first_seen, last_seen
		FROM issue_summary WHERE project_id = 'demo' ORDER BY events DESC FORMAT TSV`)
	want := strings.Join([]string{
		"*fs.PathError: open x: no such file\tshop/cart.(*Store).Load\tfatal\t90\t2026-10-27 10:00:00.000\t2026-10-27 11:29:00.000",
		"payment provider slow: 2300ms\t\twarning\t5\t2026-10-27 10:30:00.000\t2026-10-27 10:34:00.000",
		"*json.SyntaxError: open x: no such file\tencoding/json.(*decodeState).object\terror\t3\t2026-10-27 10:00:00.000\t2026-10-27 10:02:00.000",
	}, "\n")
	if got != want {
		t.Errorf("issue_summary =\n%s\nwant\n%s", got, want)
	}
}

// The case tokens can't cover: the same events redelivered in different
// batches. Rows are duplicated until merged; issue counts are not.
func TestIssueCountsIgnoreRedelivery(t *testing.T) {
	c, store := migrated(t)
	ctx := context.Background()
	grouped := grouping.Group(makeEvents(0, 100, time.Date(2026, 10, 27, 10, 0, 0, 0, time.UTC), nil))

	for _, batch := range [][]grouping.Grouped{grouped, grouped[:30], grouped[30:70], grouped[70:], grouped[10:20]} {
		if err := store.InsertEvents(ctx, batch); err != nil {
			t.Fatal(err)
		}
	}
	if got := count(t, c, "SELECT count() FROM events"); got != "210" {
		t.Errorf("raw event rows = %s, want 210 (100 + 100 redelivered + 10)", got)
	}
	if got := count(t, c, "SELECT events FROM issue_summary FORMAT TSV"); got != "100" {
		t.Errorf("issue count = %s, want 100: redelivered events must not be counted twice", got)
	}
	if got := count(t, c, "SELECT uniqCombined64Merge(events) FROM issue_hourly FORMAT TSV"); got != "100" {
		t.Errorf("hourly total = %s, want 100", got)
	}
}

func TestHourlyCounts(t *testing.T) {
	c, store := migrated(t)
	// 150 events a minute apart from 10:00: 60 at 10h, 60 at 11h, 30 at 12h.
	events := makeEvents(0, 150, time.Date(2026, 10, 27, 10, 0, 0, 0, time.UTC), nil)
	if err := store.InsertEvents(context.Background(), grouping.Group(events)); err != nil {
		t.Fatal(err)
	}
	got := count(t, c, `SELECT hour, uniqCombined64Merge(events) FROM issue_hourly
		GROUP BY hour ORDER BY hour FORMAT TSV`)
	want := "2026-10-27 10:00:00\t60\n2026-10-27 11:00:00\t60\n2026-10-27 12:00:00\t30"
	if got != want {
		t.Errorf("hourly counts =\n%s\nwant\n%s", got, want)
	}
}

// Events stored before migration 0002 must show up in issues, and running
// 0002 a second time (as after a failure halfway) must not change counts.
func TestIssuesBackfillIsIdempotent(t *testing.T) {
	c := testClient(t)
	ctx := context.Background()
	list, _ := LoadMigrations(migrations.ClickHouse, "clickhouse")
	if _, err := Migrate(ctx, c, list[:1]); err != nil { // events table only
		t.Fatal(err)
	}
	store := &EventStore{Client: c}
	if err := store.InsertEvents(ctx, grouping.Group(makeEvents(0, 40, time.Date(2026, 10, 27, 10, 0, 0, 0, time.UTC), nil))); err != nil {
		t.Fatal(err)
	}

	if _, err := Migrate(ctx, c, list); err != nil {
		t.Fatal(err)
	}
	if got := count(t, c, "SELECT events FROM issue_summary FORMAT TSV"); got != "40" {
		t.Errorf("after backfill: issue count = %s, want 40", got)
	}

	// Force 0002 to run again.
	if err := c.Exec(ctx, "ALTER TABLE schema_migrations DELETE WHERE version = 2 SETTINGS mutations_sync = 1"); err != nil {
		t.Fatal(err)
	}
	if applied, err := Migrate(ctx, c, list); err != nil || len(applied) != 1 {
		t.Fatalf("re-running 0002: applied %v, err %v", applied, err)
	}
	if got := count(t, c, "SELECT events, first_seen FROM issue_summary FORMAT TSV"); got != "40\t2026-10-27 10:00:00.000" {
		t.Errorf("after running 0002 twice: %s, want unchanged", got)
	}
	if got := count(t, c, "SELECT uniqCombined64Merge(events) FROM issue_hourly FORMAT TSV"); got != "40" {
		t.Errorf("hourly total after running 0002 twice = %s, want 40", got)
	}
}

func TestServerErrorsAreDistinguishable(t *testing.T) {
	c := testClient(t)
	err := c.Exec(context.Background(), "SELECT * FROM table_that_does_not_exist")
	if !IsServerError(err) {
		t.Errorf("err = %v, want a server error", err)
	}

	down, _ := New(Config{URL: "http://127.0.0.1:1"})
	err = down.Exec(context.Background(), "SELECT 1")
	if err == nil || IsServerError(err) {
		t.Errorf("err = %v, want a network error", err)
	}
}
