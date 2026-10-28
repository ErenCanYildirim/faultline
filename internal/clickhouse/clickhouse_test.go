package clickhouse

import (
	"encoding/json"
	"strings"
	"testing"
	"testing/fstest"
	"time"

	"github.com/erencanyildirim/faultline/event"
	"github.com/erencanyildirim/faultline/internal/grouping"
	"github.com/erencanyildirim/faultline/migrations"
)

func sampleEvent() event.Event {
	return event.Event{
		EventID:    "0192f3c4-8b2a-7c1d-9e4f-5a6b7c8d9e0f",
		ProjectID:  "demo",
		Timestamp:  time.Date(2026, 10, 27, 13, 22, 20, 63_483_700, time.UTC),
		ReceivedAt: time.Date(2026, 10, 27, 13, 22, 20, 98_128_569, time.UTC),
		Level:      event.LevelError,
		Exception: &event.Exception{
			Type:  "*fs.PathError",
			Value: "open x: no such file",
			Frames: []event.Frame{
				{Module: "main", Function: "load", File: "main.go", Line: 40, InApp: true},
				{Module: "runtime", Function: "main", File: "proc.go", Line: 285},
			},
		},
		Tags: map[string]string{"route": "/checkout"},
		User: &event.User{ID: "u1"},
		SDK:  event.SDKInfo{Name: "faultline-go", Version: "0.1.0"},
	}
}

func TestEncodeEventsRowShape(t *testing.T) {
	msgOnly := event.Event{
		EventID: "0192f3c4-8b2a-7c1d-9e4f-000000000002", ProjectID: "demo",
		Timestamp: time.Date(2026, 10, 27, 0, 0, 0, 0, time.UTC), Level: event.LevelInfo,
		Message: "hi", SDK: event.SDKInfo{Name: "x", Version: "1"},
	}
	body, err := EncodeEvents(grouping.Group([]event.Event{sampleEvent(), msgOnly}))
	if err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(strings.TrimSuffix(string(body), "\n"), "\n")
	if len(lines) != 2 {
		t.Fatalf("got %d lines, want one per event", len(lines))
	}

	var row map[string]any
	if err := json.Unmarshal([]byte(lines[0]), &row); err != nil {
		t.Fatal(err)
	}
	checks := map[string]any{
		"timestamp":       "2026-10-27 13:22:20.063", // millisecond layout ClickHouse parses by default
		"received_at":     "2026-10-27 13:22:20.098",
		"exception_type":  "*fs.PathError",
		"user_id":         "u1",
		"fingerprint":     grouping.Fingerprint(&[]event.Event{sampleEvent()}[0]).Fingerprint,
		"frames.function": []any{"load", "main"},
		"frames.line":     []any{40.0, 285.0},
		"frames.in_app":   []any{true, false},
	}
	for k, want := range checks {
		if got := row[k]; !jsonEqual(got, want) {
			t.Errorf("%s = %v, want %v", k, got, want)
		}
	}
	if _, ok := row["inserted_at"]; ok {
		t.Error("inserted_at must be left to the ClickHouse default")
	}

	// A message-only event must send empty arrays and maps, never null:
	// ClickHouse rejects null for non-Nullable columns.
	for _, null := range []string{`"frames.module":null`, `"tags":null`} {
		if strings.Contains(lines[1], null) {
			t.Errorf("message-only row contains %s: %s", null, lines[1])
		}
	}
}

func jsonEqual(a, b any) bool {
	x, _ := json.Marshal(a)
	y, _ := json.Marshal(b)
	return string(x) == string(y)
}

func TestSplitStatements(t *testing.T) {
	sql := `-- comment line
CREATE TABLE a
(
    x UInt8 -- trailing comments stay, ClickHouse accepts them
);

-- another
ALTER TABLE a ADD COLUMN IF NOT EXISTS y String;
SELECT 1`
	got := SplitStatements(sql)
	if len(got) != 3 {
		t.Fatalf("got %d statements: %q", len(got), got)
	}
	if !strings.HasPrefix(got[0], "CREATE TABLE a") || strings.HasSuffix(got[0], ";") {
		t.Errorf("statement 0 = %q", got[0])
	}
	if got[2] != "SELECT 1" {
		t.Errorf("last statement without semicolon = %q", got[2])
	}
}

func TestLoadMigrationsChecksNames(t *testing.T) {
	ok := fstest.MapFS{
		"m/0002_b.sql": {Data: []byte("SELECT 2;")},
		"m/0001_a.sql": {Data: []byte("SELECT 1;")},
	}
	list, err := LoadMigrations(ok, "m")
	if err != nil {
		t.Fatal(err)
	}
	if len(list) != 2 || list[0].Version != 1 || list[1].Version != 2 {
		t.Errorf("migrations not sorted by version: %+v", list)
	}

	bad := map[string]fstest.MapFS{
		"bad name":          {"m/1_a.sql": {Data: []byte("SELECT 1;")}},
		"duplicate version": {"m/0001_a.sql": {Data: []byte("SELECT 1;")}, "m/0001_b.sql": {Data: []byte("SELECT 1;")}},
		"empty file":        {"m/0001_a.sql": {Data: []byte("-- nothing\n")}},
	}
	for name, fsys := range bad {
		t.Run(name, func(t *testing.T) {
			if _, err := LoadMigrations(fsys, "m"); err == nil {
				t.Error("expected an error")
			}
		})
	}
}

func TestRealMigrationsLoad(t *testing.T) {
	list, err := LoadMigrations(migrations.ClickHouse, "clickhouse")
	if err != nil {
		t.Fatal(err)
	}
	if len(list) == 0 || list[0].Name != "0001_events.sql" {
		t.Errorf("unexpected migrations: %+v", list)
	}
}
