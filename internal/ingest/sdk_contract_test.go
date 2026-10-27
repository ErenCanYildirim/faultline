package ingest

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/erencanyildirim/faultline/event"
	faultline "github.com/erencanyildirim/faultline/sdk"
)

// The real SDK against the real handler. If either side changes the wire
// format, headers or response shape, this test fails.
func TestSDKAndIngestAgree(t *testing.T) {
	h, prod := newTestHandler(t)
	srv := httptest.NewServer(h)
	defer srv.Close()

	dsn := strings.Replace(srv.URL, "http://", "http://"+demoKey+"@", 1)
	c, err := faultline.New(faultline.Options{DSN: dsn, Release: "app@1", BatchSize: 7})
	if err != nil {
		t.Fatal(err)
	}

	for i := range 20 {
		c.CaptureError(fmt.Errorf("op %d: %w", i, errors.New("boom")), faultline.WithTag("i", fmt.Sprint(i)))
	}
	c.CaptureMessage(strings.Repeat("long message ", 2000)) // truncated by the SDK, accepted by ingest
	if !c.Close(5 * time.Second) {
		t.Fatal("Close did not flush")
	}

	s := c.Stats()
	if s.Sent != 21 || s.Failed != 0 || s.Dropped != 0 {
		t.Fatalf("SDK stats = %+v, want 21 sent", s)
	}
	if len(prod.msgs) != 21 {
		t.Fatalf("ingest produced %d messages, want 21", len(prod.msgs))
	}

	var e event.Event
	if err := json.Unmarshal(prod.msgs[0].Value, &e); err != nil {
		t.Fatal(err)
	}
	if e.ProjectID != "demo" || e.ReceivedAt.IsZero() || e.Release != "app@1" {
		t.Errorf("stored event = %+v", e)
	}
	if e.Exception == nil || len(e.Exception.Frames) == 0 {
		t.Errorf("stack trace lost on the way: %+v", e.Exception)
	}
}

func TestSDKCountsIngestRejectionsAsFailed(t *testing.T) {
	h, _ := newTestHandler(t)
	srv := httptest.NewServer(h)
	defer srv.Close()

	// Unknown key: ingest answers 401 for the whole batch.
	dsn := strings.Replace(srv.URL, "http://", "http://wrong-key-0123456789@", 1)
	c, err := faultline.New(faultline.Options{DSN: dsn})
	if err != nil {
		t.Fatal(err)
	}
	c.CaptureMessage("x")
	c.Close(5 * time.Second)

	if s := c.Stats(); s.Failed != 1 || s.Sent != 0 {
		t.Errorf("stats = %+v, want 1 failed", s)
	}
}
