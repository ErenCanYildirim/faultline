package ingest

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/erencanyildirim/faultline/event"
	"github.com/erencanyildirim/faultline/internal/queue"
)

const (
	demoKey  = "demo-key-0123456789"
	otherKey = "other-key-0123456789"
)

var fixedNow = time.Date(2026, 10, 27, 12, 0, 0, 0, time.UTC)

type fakeProducer struct {
	mu      sync.Mutex
	msgs    []queue.Message
	err     error
	pingErr error
}

func (f *fakeProducer) Produce(_ context.Context, msgs []queue.Message) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.err != nil {
		return f.err
	}
	f.msgs = append(f.msgs, msgs...)
	return nil
}
func (f *fakeProducer) Ping(context.Context) error { return f.pingErr }
func (f *fakeProducer) Close()                     {}

func newTestHandler(t *testing.T) (http.Handler, *fakeProducer) {
	t.Helper()
	projects, err := ParseProjects([]byte(fmt.Sprintf(
		`{"projects":[{"id":"demo","keys":[%q]},{"id":"other","keys":[%q]}]}`, demoKey, otherKey)))
	if err != nil {
		t.Fatal(err)
	}
	prod := &fakeProducer{}
	h := &Handler{Projects: projects, Producer: prod, Now: func() time.Time { return fixedNow }}
	return h.Routes(), prod
}

func goodEvent(id string) map[string]any {
	return map[string]any{
		"event_id":  id,
		"timestamp": fixedNow.Add(-time.Minute).Format(time.RFC3339Nano),
		"level":     "error",
		"exception": map[string]any{
			"type":   "*fs.PathError",
			"value":  "open x: no such file",
			"frames": []any{map[string]any{"module": "main", "function": "main", "in_app": true}},
		},
		"sdk": map[string]any{"name": "faultline-go", "version": "0.1.0"},
	}
}

func uuidN(n int) string { return fmt.Sprintf("0192f3c4-8b2a-7c1d-9e4f-%012d", n) }

func batchBody(t *testing.T, events ...any) []byte {
	t.Helper()
	b, err := json.Marshal(map[string]any{"events": events})
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func gzipBytes(t *testing.T, b []byte) []byte {
	t.Helper()
	var buf bytes.Buffer
	gz := gzip.NewWriter(&buf)
	_, _ = gz.Write(b)
	_ = gz.Close()
	return buf.Bytes()
}

func post(h http.Handler, key string, body []byte, headers ...string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodPost, event.EventsPath, bytes.NewReader(body))
	if key != "" {
		req.Header.Set(event.KeyHeader, key)
	}
	for i := 0; i+1 < len(headers); i += 2 {
		req.Header.Set(headers[i], headers[i+1])
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

func decodeBatchResponse(t *testing.T, rec *httptest.ResponseRecorder) event.BatchResponse {
	t.Helper()
	var br event.BatchResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &br); err != nil {
		t.Fatalf("response is not a BatchResponse: %s", rec.Body.String())
	}
	return br
}

func TestAuthentication(t *testing.T) {
	h, prod := newTestHandler(t)
	body := batchBody(t, goodEvent(uuidN(1)))

	for name, key := range map[string]string{"missing": "", "unknown": "not-a-real-key-at-all"} {
		t.Run(name, func(t *testing.T) {
			rec := post(h, key, body)
			if rec.Code != http.StatusUnauthorized {
				t.Errorf("status = %d, want 401", rec.Code)
			}
		})
	}
	if len(prod.msgs) != 0 {
		t.Errorf("unauthenticated requests reached the queue")
	}
}

func TestAcceptsBatchAndStampsServerFields(t *testing.T) {
	h, prod := newTestHandler(t)
	e := goodEvent(uuidN(1))
	e["project_id"] = "other"                 // client tries to write into another project
	e["received_at"] = "2020-01-01T00:00:00Z" // and to fake server time
	rec := post(h, demoKey, batchBody(t, e, goodEvent(uuidN(2))))

	if rec.Code != http.StatusAccepted {
		t.Fatalf("status = %d, body %s", rec.Code, rec.Body.String())
	}
	if br := decodeBatchResponse(t, rec); br.Accepted != 2 || len(br.Rejected) != 0 {
		t.Errorf("response = %+v", br)
	}
	if len(prod.msgs) != 2 {
		t.Fatalf("produced %d messages", len(prod.msgs))
	}
	m := prod.msgs[0]
	if string(m.Key) != uuidN(1) || m.ProjectID != "demo" {
		t.Errorf("message key %q project %q", m.Key, m.ProjectID)
	}
	var stored event.Event
	if err := json.Unmarshal(m.Value, &stored); err != nil {
		t.Fatal(err)
	}
	if stored.ProjectID != "demo" {
		t.Errorf("project_id = %q, client value must be overwritten", stored.ProjectID)
	}
	if !stored.ReceivedAt.Equal(fixedNow) {
		t.Errorf("received_at = %v, client value must be overwritten", stored.ReceivedAt)
	}
}

func TestGzipBody(t *testing.T) {
	h, prod := newTestHandler(t)
	rec := post(h, demoKey, gzipBytes(t, batchBody(t, goodEvent(uuidN(1)))), "Content-Encoding", "gzip")
	if rec.Code != http.StatusAccepted || len(prod.msgs) != 1 {
		t.Errorf("status = %d, produced %d", rec.Code, len(prod.msgs))
	}
}

func TestRejectsBadBodies(t *testing.T) {
	h, _ := newTestHandler(t)
	tooMany := make([]any, event.MaxBatchEvents+1)
	for i := range tooMany {
		tooMany[i] = goodEvent(uuidN(i))
	}
	// 5 MiB of zeros compresses to a few KiB: a small request that would
	// expand far past the limit.
	bomb := gzipBytes(t, bytes.Repeat([]byte{'0'}, 5<<20))

	tests := []struct {
		name    string
		body    []byte
		headers []string
		want    int
	}{
		{"not json", []byte("hello"), nil, http.StatusBadRequest},
		{"empty batch", []byte(`{"events":[]}`), nil, http.StatusBadRequest},
		{"too many events", batchBody(t, tooMany...), nil, http.StatusRequestEntityTooLarge},
		{"body over 1 MiB", bytes.Repeat([]byte{' '}, event.MaxBatchBytes+1), nil, http.StatusRequestEntityTooLarge},
		{"gzip bomb", bomb, []string{"Content-Encoding", "gzip"}, http.StatusRequestEntityTooLarge},
		{"invalid gzip", []byte("not gzip"), []string{"Content-Encoding", "gzip"}, http.StatusBadRequest},
		{"unsupported encoding", []byte("{}"), []string{"Content-Encoding", "br"}, http.StatusUnsupportedMediaType},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			rec := post(h, demoKey, tc.body, tc.headers...)
			if rec.Code != tc.want {
				t.Errorf("status = %d, want %d, body %s", rec.Code, tc.want, rec.Body.String())
			}
		})
	}
}

func TestInvalidEventsAreRejectedIndividually(t *testing.T) {
	h, prod := newTestHandler(t)
	bad := goodEvent(uuidN(2))
	bad["level"] = "critical"
	huge := goodEvent(uuidN(3))
	huge["message"] = strings.Repeat("x", event.MaxEventBytes)

	rec := post(h, demoKey, batchBody(t, goodEvent(uuidN(1)), bad, huge, "not an object"))
	if rec.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want 202 when at least one event is valid", rec.Code)
	}
	br := decodeBatchResponse(t, rec)
	if br.Accepted != 1 || len(br.Rejected) != 3 {
		t.Fatalf("response = %+v", br)
	}
	if r := br.Rejected[0]; r.Index != 1 || r.EventID != uuidN(2) || !strings.Contains(r.Error, "level") {
		t.Errorf("rejection 0 = %+v", r)
	}
	if r := br.Rejected[1]; r.Index != 2 || !strings.Contains(r.Error, "64 KiB") {
		t.Errorf("rejection 1 = %+v", r)
	}
	if r := br.Rejected[2]; r.Index != 3 {
		t.Errorf("rejection 2 = %+v", r)
	}
	if len(prod.msgs) != 1 {
		t.Errorf("produced %d, want 1", len(prod.msgs))
	}
}

func TestAllInvalidReturns400(t *testing.T) {
	h, prod := newTestHandler(t)
	bad := goodEvent("nope")
	rec := post(h, demoKey, batchBody(t, bad))
	if rec.Code != http.StatusBadRequest {
		t.Errorf("status = %d", rec.Code)
	}
	if br := decodeBatchResponse(t, rec); br.Accepted != 0 || len(br.Rejected) != 1 {
		t.Errorf("response = %+v", br)
	}
	if len(prod.msgs) != 0 {
		t.Error("nothing should be produced")
	}
}

func TestTimestampOutsideWindowIsReplaced(t *testing.T) {
	h, prod := newTestHandler(t)
	old := goodEvent(uuidN(1))
	old["timestamp"] = fixedNow.Add(-event.MaxTimestampAge - time.Hour).Format(time.RFC3339)
	future := goodEvent(uuidN(2))
	future["timestamp"] = fixedNow.Add(time.Hour).Format(time.RFC3339)
	ok := goodEvent(uuidN(3))

	rec := post(h, demoKey, batchBody(t, old, future, ok))
	if rec.Code != http.StatusAccepted || len(prod.msgs) != 3 {
		t.Fatalf("status = %d, produced %d", rec.Code, len(prod.msgs))
	}
	for i, want := range []time.Time{fixedNow, fixedNow, fixedNow.Add(-time.Minute)} {
		var e event.Event
		_ = json.Unmarshal(prod.msgs[i].Value, &e)
		if !e.Timestamp.Equal(want) {
			t.Errorf("event %d timestamp = %v, want %v", i, e.Timestamp, want)
		}
	}
}

func TestQueueFailureReturns503WithRetryAfter(t *testing.T) {
	h, prod := newTestHandler(t)
	prod.err = errors.New("broker down")
	rec := post(h, demoKey, batchBody(t, goodEvent(uuidN(1))))
	if rec.Code != http.StatusServiceUnavailable {
		t.Errorf("status = %d, want 503", rec.Code)
	}
	if rec.Header().Get("Retry-After") == "" {
		t.Error("missing Retry-After header")
	}
}

func TestHealthAndReadiness(t *testing.T) {
	h, prod := newTestHandler(t)

	get := func(path string) int {
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, path, nil))
		return rec.Code
	}
	if get("/healthz") != 200 || get("/readyz") != 200 {
		t.Error("expected healthy and ready")
	}
	prod.pingErr = errors.New("unreachable")
	if get("/healthz") != 200 {
		t.Error("liveness must not depend on the broker")
	}
	if get("/readyz") != 503 {
		t.Error("readiness must fail when the broker is unreachable")
	}
}

func TestParseProjectsRejectsBadConfig(t *testing.T) {
	tests := map[string]string{
		"no projects":    `{"projects":[]}`,
		"empty id":       `{"projects":[{"id":"","keys":["0123456789abcdef"]}]}`,
		"duplicate id":   `{"projects":[{"id":"a","keys":["0123456789abcdef"]},{"id":"a","keys":["fedcba9876543210"]}]}`,
		"no keys":        `{"projects":[{"id":"a","keys":[]}]}`,
		"short key":      `{"projects":[{"id":"a","keys":["short"]}]}`,
		"shared key":     `{"projects":[{"id":"a","keys":["0123456789abcdef"]},{"id":"b","keys":["0123456789abcdef"]}]}`,
		"malformed json": `{"projects":`,
	}
	for name, raw := range tests {
		t.Run(name, func(t *testing.T) {
			if _, err := ParseProjects([]byte(raw)); err == nil {
				t.Error("expected an error")
			}
		})
	}
}
