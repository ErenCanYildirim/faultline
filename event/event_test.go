package event

import (
	"bytes"
	"encoding/json"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"
)

// decodeStrict fails on unknown fields, so the fixtures double as a check
// that the documented contract and the Go types agree.
func decodeStrict(t *testing.T, path string) Event {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	var e Event
	if err := dec.Decode(&e); err != nil {
		t.Fatalf("decode %s: %v", path, err)
	}
	return e
}

func TestDecodeFullEvent(t *testing.T) {
	e := decodeStrict(t, "testdata/event_full.json")

	if e.EventID != "0192f3c4-8b2a-7c1d-9e4f-5a6b7c8d9e0f" {
		t.Errorf("event_id = %q", e.EventID)
	}
	if e.Level != LevelError {
		t.Errorf("level = %q, want error", e.Level)
	}
	wantTS := time.Date(2026, 10, 27, 9, 41, 12, 345_000_000, time.UTC)
	if !e.Timestamp.Equal(wantTS) {
		t.Errorf("timestamp = %v, want %v", e.Timestamp, wantTS)
	}
	if e.Exception == nil {
		t.Fatal("exception is nil")
	}
	if got := len(e.Exception.Frames); got != 3 {
		t.Fatalf("frames = %d, want 3", got)
	}
	// Innermost first: frame 0 is where the error was captured.
	if f := e.Exception.Frames[0]; f.Function != "(*Store).Load" || !f.InApp {
		t.Errorf("frame 0 = %+v, want in-app (*Store).Load", f)
	}
	if f := e.Exception.Frames[2]; f.InApp {
		t.Errorf("frame 2 (net/http) should not be in-app")
	}
	if e.User == nil || e.User.Email != "jdoe@example.com" {
		t.Errorf("user = %+v", e.User)
	}
	if e.Tags["route"] != "/api/checkout" {
		t.Errorf("tags = %v", e.Tags)
	}
}

func TestRoundTripIsLossless(t *testing.T) {
	for _, path := range []string{"testdata/event_full.json", "testdata/event_message_only.json"} {
		t.Run(path, func(t *testing.T) {
			first := decodeStrict(t, path)
			encoded, err := json.Marshal(first)
			if err != nil {
				t.Fatalf("marshal: %v", err)
			}
			var second Event
			if err := json.Unmarshal(encoded, &second); err != nil {
				t.Fatalf("unmarshal: %v", err)
			}
			if !reflect.DeepEqual(first, second) {
				t.Errorf("round trip changed the event\nfirst:  %+v\nsecond: %+v", first, second)
			}
		})
	}
}

// What the SDK sends must not contain the fields ingest owns.
func TestServerOwnedFieldsOmittedWhenUnset(t *testing.T) {
	e := decodeStrict(t, "testdata/event_message_only.json")
	encoded, err := json.Marshal(e)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	s := string(encoded)
	for _, key := range []string{`"project_id"`, `"received_at"`, `"exception"`, `"user"`, `"tags"`} {
		if strings.Contains(s, key) {
			t.Errorf("encoded event contains %s but it was unset: %s", key, s)
		}
	}
}

func TestFixturesFitEventLimit(t *testing.T) {
	e := decodeStrict(t, "testdata/event_full.json")
	encoded, err := json.Marshal(e)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if len(encoded) > MaxEventBytes {
		t.Errorf("full fixture is %d bytes, over MaxEventBytes %d", len(encoded), MaxEventBytes)
	}
}

func TestLimitsAreConsistent(t *testing.T) {
	if MaxBatchBytes < MaxEventBytes {
		t.Errorf("MaxBatchBytes (%d) must hold at least one max-size event (%d)", MaxBatchBytes, MaxEventBytes)
	}
	// Redpanda's default max message size is 1 MiB; one event is one message.
	if MaxEventBytes >= 1<<20 {
		t.Errorf("MaxEventBytes (%d) must stay under the 1 MiB broker message limit", MaxEventBytes)
	}
	if MaxTimestampAge >= 30*24*time.Hour {
		t.Errorf("MaxTimestampAge must be below the 30-day storage TTL")
	}
}

func TestLevelValid(t *testing.T) {
	for _, l := range []Level{LevelDebug, LevelInfo, LevelWarning, LevelError, LevelFatal} {
		if !l.Valid() {
			t.Errorf("%q should be valid", l)
		}
	}
	for _, l := range []Level{"", "ERROR", "critical"} {
		if l.Valid() {
			t.Errorf("%q should be invalid", l)
		}
	}
}
