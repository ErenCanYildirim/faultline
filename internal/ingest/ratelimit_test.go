package ingest

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/erencanyildirim/faultline/internal/ratelimit"
	faultline "github.com/erencanyildirim/faultline/sdk"
)

// newLimitedHandler has two projects: "noisy" with a small limit and "quiet"
// with the default.
func newLimitedHandler(t *testing.T, limiter ratelimit.Limiter) (*Handler, http.Handler, *fakeProducer) {
	t.Helper()
	projects, err := ParseProjects([]byte(fmt.Sprintf(`{"projects":[
		{"id":"noisy","keys":[%q],"rate_limit":{"events_per_second":10,"burst":100}},
		{"id":"quiet","keys":[%q]}]}`, demoKey, otherKey)))
	if err != nil {
		t.Fatal(err)
	}
	prod := &fakeProducer{}
	h := &Handler{Projects: projects, Producer: prod, Limiter: limiter, Now: func() time.Time { return fixedNow }}
	return h, h.Routes(), prod
}

func eventsN(from, n int) []any {
	out := make([]any, n)
	for i := range n {
		out[i] = goodEvent(uuidN(from + i))
	}
	return out
}

func TestRateLimitReturns429WithRetryAfter(t *testing.T) {
	h, routes, prod := newLimitedHandler(t, ratelimit.NewLocal())

	// The burst of 100 is spent by one full batch.
	if rec := post(routes, demoKey, batchBody(t, eventsN(0, 100)...)); rec.Code != http.StatusAccepted {
		t.Fatalf("first batch: %d %s", rec.Code, rec.Body)
	}
	rec := post(routes, demoKey, batchBody(t, eventsN(100, 20)...))
	if rec.Code != http.StatusTooManyRequests {
		t.Fatalf("over the limit: %d %s, want 429", rec.Code, rec.Body)
	}
	// 20 events at 10/s: 2s.
	if got := rec.Header().Get("Retry-After"); got != "2" {
		t.Errorf("Retry-After = %q, want 2", got)
	}
	if !strings.Contains(rec.Body.String(), "rate limit") {
		t.Errorf("body = %s", rec.Body)
	}
	if len(prod.msgs) != 100 {
		t.Errorf("%d messages produced; a refused batch must not reach the queue", len(prod.msgs))
	}

	// Another project is unaffected.
	if rec := post(routes, otherKey, batchBody(t, eventsN(200, 100)...)); rec.Code != http.StatusAccepted {
		t.Errorf("quiet project: %d, one project's limit must not affect another", rec.Code)
	}

	want := []ProjectStats{
		{Project: "noisy", Accepted: 100, RateLimited: 20},
		{Project: "quiet", Accepted: 100},
	}
	if got := h.Stats.Snapshot(); fmt.Sprint(got) != fmt.Sprint(want) {
		t.Errorf("stats = %+v, want %+v", got, want)
	}
}

// The cost is the number of valid events: invalid ones are rejected before
// the limiter and don't spend tokens.
func TestRateLimitCostCountsOnlyValidEvents(t *testing.T) {
	h, routes, _ := newLimitedHandler(t, ratelimit.NewLocal())
	bad := map[string]any{"event_id": "not-a-uuid"}
	events := append(eventsN(0, 98), bad, bad)
	rec := post(routes, demoKey, batchBody(t, events...))
	if rec.Code != http.StatusAccepted {
		t.Fatalf("98 valid + 2 invalid: %d %s", rec.Code, rec.Body)
	}
	if br := decodeBatchResponse(t, rec); br.Accepted != 98 || len(br.Rejected) != 2 {
		t.Errorf("response = %+v", br)
	}
	// 2 tokens left from the burst of 100.
	if rec := post(routes, demoKey, batchBody(t, eventsN(500, 2)...)); rec.Code != http.StatusAccepted {
		t.Errorf("2 events with 2 tokens left: %d", rec.Code)
	}
	s := h.Stats.Snapshot()
	if len(s) != 1 || s[0].Accepted != 100 || s[0].Invalid != 2 || s[0].RateLimited != 0 {
		t.Errorf("stats = %+v", s)
	}
}

func TestRetryAfterSeconds(t *testing.T) {
	for d, want := range map[time.Duration]string{
		0:                       "1",
		time.Millisecond:        "1",
		time.Second:             "1",
		1001 * time.Millisecond: "2",
		10 * time.Second:        "10",
	} {
		if got := retryAfterSeconds(d); got != want {
			t.Errorf("retryAfterSeconds(%v) = %s, want %s", d, got, want)
		}
	}
}

type errLimiter struct{ err error }

func (e errLimiter) Allow(context.Context, string, ratelimit.Limit, int) (ratelimit.Decision, error) {
	return ratelimit.Decision{}, e.err
}

func TestRateLimiterErrorIs500(t *testing.T) {
	_, routes, prod := newLimitedHandler(t, errLimiter{errors.New("bug")})
	rec := post(routes, demoKey, batchBody(t, eventsN(0, 1)...))
	if rec.Code != http.StatusInternalServerError {
		t.Errorf("status = %d, want 500", rec.Code)
	}
	if len(prod.msgs) != 0 {
		t.Error("nothing should be produced when the limiter errors")
	}
}

func TestDebugStats(t *testing.T) {
	h, routes, _ := newLimitedHandler(t, ratelimit.NewLocal())
	h.LimiterStats = func() ratelimit.FailOpenStats {
		return ratelimit.FailOpenStats{SharedFailures: 3, DegradedDecisions: 7}
	}

	post(routes, demoKey, batchBody(t, eventsN(0, 100)...))
	post(routes, demoKey, batchBody(t, eventsN(100, 5)...))

	rec := httptest.NewRecorder()
	routes.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/debug/stats", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d", rec.Code)
	}
	var got map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
		t.Fatal(err)
	}
	want := `{"projects":[{"accepted":100,"invalid":0,"project":"noisy","queue_errors":0,"rate_limited":5}],` +
		`"rate_limiter":{"degraded_decisions":7,"shared_failures":3}}`
	if b, _ := json.Marshal(got); string(b) != want {
		t.Errorf("/debug/stats =\n%s\nwant\n%s", b, want)
	}
}

func TestNilLimiterMeansNoLimit(t *testing.T) {
	_, routes, _ := newLimitedHandler(t, nil)
	for i := range 3 {
		if rec := post(routes, demoKey, batchBody(t, eventsN(i*100, 100)...)); rec.Code != http.StatusAccepted {
			t.Fatalf("batch %d: %d", i, rec.Code)
		}
	}
}

func TestProjectRateLimitConfig(t *testing.T) {
	p, err := ParseProjects([]byte(`{"projects":[
		{"id":"a","keys":["0123456789abcdef"],"rate_limit":{"events_per_second":50,"burst":200}},
		{"id":"b","keys":["fedcba9876543210"]}]}`))
	if err != nil {
		t.Fatal(err)
	}
	if got := p.Limit("a"); got != (ratelimit.Limit{EventsPerSecond: 50, Burst: 200}) {
		t.Errorf("a: %+v", got)
	}
	if got := p.Limit("b"); got != DefaultLimit {
		t.Errorf("b without rate_limit: %+v, want the default", got)
	}

	for name, rl := range map[string]string{
		"zero rate":           `{"events_per_second":0,"burst":200}`,
		"negative rate":       `{"events_per_second":-5,"burst":200}`,
		"burst below a batch": `{"events_per_second":50,"burst":99}`,
	} {
		t.Run(name, func(t *testing.T) {
			raw := `{"projects":[{"id":"a","keys":["0123456789abcdef"],"rate_limit":` + rl + `}]}`
			if _, err := ParseProjects([]byte(raw)); err == nil {
				t.Error("expected an error")
			}
		})
	}
}

// The real SDK against a rate-limited handler: the 429 is retried after
// Retry-After and every event arrives, none counted as failed.
func TestSDKRetriesAfter429(t *testing.T) {
	projects, err := ParseProjects([]byte(fmt.Sprintf(
		`{"projects":[{"id":"demo","keys":[%q],"rate_limit":{"events_per_second":100,"burst":100}}]}`, demoKey)))
	if err != nil {
		t.Fatal(err)
	}
	prod := &fakeProducer{}
	h := &Handler{Projects: projects, Producer: prod, Limiter: ratelimit.NewLocal()}
	srv := httptest.NewServer(h.Routes())
	defer srv.Close()

	dsn := strings.Replace(srv.URL, "http://", "http://"+demoKey+"@", 1)
	c, err := faultline.New(faultline.Options{DSN: dsn, BatchSize: 100})
	if err != nil {
		t.Fatal(err)
	}
	// 150 events: the first 100 spend the burst, the next 50 get a 429 with
	// Retry-After 1 and go through on the retry.
	for i := range 150 {
		c.CaptureMessage(fmt.Sprintf("m%d", i))
	}
	start := time.Now()
	if !c.Close(10 * time.Second) {
		t.Fatal("Close did not flush")
	}
	s := c.Stats()
	if s.Sent != 150 || s.Failed != 0 || s.Dropped != 0 || s.Retries == 0 {
		t.Errorf("SDK stats = %+v, want 150 sent after at least one retry", s)
	}
	if len(prod.msgs) != 150 {
		t.Errorf("%d produced, want 150", len(prod.msgs))
	}
	if elapsed := time.Since(start); elapsed < 900*time.Millisecond {
		t.Errorf("finished in %v; the SDK must wait for Retry-After (1s)", elapsed)
	}
	if st := h.Stats.Snapshot(); len(st) != 1 || st[0].RateLimited == 0 || st[0].Accepted != 150 {
		t.Errorf("ingest stats = %+v", st)
	}
}
