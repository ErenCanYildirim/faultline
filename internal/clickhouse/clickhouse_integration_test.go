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
	for range 2 { // a redelivered batch
		if err := store.InsertEvents(ctx, events); err != nil {
			t.Fatalf("insert: %v", err)
		}
	}

	if got := count(t, c, "SELECT count() FROM events"); got != "100" {
		t.Errorf("raw rows = %s, want 100 before any merge", got)
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

	// The stored row matches what was sent.
	row := count(t, c, `SELECT project_id, exception_type, frames.function, frames.in_app, tags['route'], user_id
		FROM events WHERE event_id = '0192f3c4-8b2a-7c1d-9e4f-000000000007' FORMAT TSV`)
	want := "demo\t*fs.PathError\t['load','main']\t[true,false]\t/checkout\tu1"
	if row != want {
		t.Errorf("stored row = %q\nwant         %q", row, want)
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
