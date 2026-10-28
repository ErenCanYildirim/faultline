// Package ingest is the HTTP API that accepts events from SDKs and writes
// them to the queue. It holds no state between requests: the project keys are
// read-only after startup and rate limits live in Redis, so any number of
// instances can run side by side.
package ingest

import (
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"math"
	"net/http"
	"strconv"
	"time"

	"github.com/erencanyildirim/faultline/event"
	"github.com/erencanyildirim/faultline/internal/queue"
	"github.com/erencanyildirim/faultline/internal/ratelimit"
)

// DefaultProduceTimeout bounds how long a request waits for the broker. It
// must stay well below the SDK's HTTP timeout (10s), so a broker outage
// reaches clients as a 503 they can act on, not as a client-side timeout.
const DefaultProduceTimeout = 5 * time.Second

// Handler serves the ingest API.
type Handler struct {
	Projects *Projects
	Producer queue.Producer
	// Limiter enforces per-project rate limits. Nil means no limiting.
	Limiter        ratelimit.Limiter
	Log            *slog.Logger
	ProduceTimeout time.Duration
	// Now is the server clock; tests replace it.
	Now func() time.Time
	// Stats counts accepted and refused events per project.
	Stats Stats
	// LimiterStats, if set, reports how often the rate limiter fell back to
	// local limits; shown on /debug/stats.
	LimiterStats func() ratelimit.FailOpenStats
}

// Routes returns the HTTP routes.
func (h *Handler) Routes() http.Handler {
	if h.Log == nil {
		h.Log = slog.New(slog.DiscardHandler)
	}
	if h.ProduceTimeout <= 0 {
		h.ProduceTimeout = DefaultProduceTimeout
	}
	if h.Now == nil {
		h.Now = time.Now
	}
	mux := http.NewServeMux()
	mux.HandleFunc("POST "+event.EventsPath, h.serveEvents)
	mux.HandleFunc("GET /healthz", h.serveHealth)
	mux.HandleFunc("GET /readyz", h.serveReady)
	mux.HandleFunc("GET /debug/stats", h.serveStats)
	return mux
}

// serveStats shows per-project counters, so rate limiting is visible.
// Served on the same port as the API: fine for local development, but in a
// real deployment it belongs on an internal port (see the decisions log).
func (h *Handler) serveStats(w http.ResponseWriter, _ *http.Request) {
	out := struct {
		Projects []ProjectStats           `json:"projects"`
		Limiter  *ratelimit.FailOpenStats `json:"rate_limiter,omitempty"`
	}{Projects: h.Stats.Snapshot()}
	if h.LimiterStats != nil {
		s := h.LimiterStats()
		out.Limiter = &s
	}
	writeJSON(w, http.StatusOK, out)
}

// serveHealth answers liveness: the process is up and serving HTTP.
func (h *Handler) serveHealth(w http.ResponseWriter, _ *http.Request) {
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok"))
}

// serveReady answers readiness: the broker is reachable, so events sent now
// can be accepted. A load balancer stops routing here while this fails.
func (h *Handler) serveReady(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()
	if err := h.Producer.Ping(ctx); err != nil {
		writeError(w, http.StatusServiceUnavailable, "queue unreachable")
		return
	}
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ready"))
}

func (h *Handler) serveEvents(w http.ResponseWriter, r *http.Request) {
	// 1. Authenticate. Missing and unknown keys get the same answer, so the
	//    response doesn't reveal which keys exist.
	projectID, ok := h.Projects.Lookup(r.Header.Get(event.KeyHeader))
	if !ok {
		writeError(w, http.StatusUnauthorized, "missing or unknown project key")
		return
	}

	// 2. Read the body with two caps: on the bytes sent, and on the bytes
	//    after decompression. The second stops a small gzip "bomb" from
	//    expanding into gigabytes in memory.
	body, status, msg := readBody(w, r)
	if status != 0 {
		writeError(w, status, msg)
		return
	}

	// 3. Split the batch, keeping each event's raw bytes for the size check.
	var batch struct {
		Events []json.RawMessage `json:"events"`
	}
	if err := json.Unmarshal(body, &batch); err != nil {
		writeError(w, http.StatusBadRequest, "body is not a valid events batch")
		return
	}
	if len(batch.Events) == 0 {
		writeError(w, http.StatusBadRequest, "batch has no events")
		return
	}
	if len(batch.Events) > event.MaxBatchEvents {
		writeError(w, http.StatusRequestEntityTooLarge, "batch has more than 100 events")
		return
	}

	// 4. Validate each event on its own. One bad event doesn't sink the rest.
	now := h.Now().UTC()
	resp := event.BatchResponse{}
	msgs := make([]queue.Message, 0, len(batch.Events))
	for i, raw := range batch.Events {
		e, err := h.prepare(raw, projectID, now)
		if err != nil {
			rej := event.Rejection{Index: i, Error: err.Error()}
			if e != nil {
				rej.EventID = e.EventID
			}
			resp.Rejected = append(resp.Rejected, rej)
			continue
		}
		value, err := json.Marshal(e)
		if err != nil {
			resp.Rejected = append(resp.Rejected, event.Rejection{Index: i, EventID: e.EventID, Error: "could not encode event"})
			continue
		}
		msgs = append(msgs, queue.Message{Key: []byte(e.EventID), Value: value, ProjectID: projectID})
	}

	counters := h.Stats.project(projectID)
	counters.invalid.Add(uint64(len(resp.Rejected)))
	if len(msgs) == 0 {
		writeJSON(w, http.StatusBadRequest, resp)
		return
	}

	// 5. Rate limit. The cost is the number of valid events, so batching
	//    can't get around the limit. All or nothing: a partly accepted batch
	//    would need new response semantics, and a 429 is already retried by
	//    the SDK after Retry-After.
	if h.Limiter != nil {
		d, err := h.Limiter.Allow(r.Context(), projectID, h.Projects.Limit(projectID), len(msgs))
		if err != nil {
			// Only ErrCostTooHigh reaches here, which config validation
			// rules out (burst >= largest batch). Treat it as a server bug.
			h.Log.Error("rate limiter error", "project", projectID, "err", err)
			writeError(w, http.StatusInternalServerError, "rate limiter error")
			return
		}
		if !d.Allowed {
			counters.rateLimited.Add(uint64(len(msgs)))
			w.Header().Set("Retry-After", retryAfterSeconds(d.RetryAfter))
			writeError(w, http.StatusTooManyRequests, "rate limit exceeded for this project")
			return
		}
	}

	// 6. Write to the queue and wait for the broker's acknowledgement. Only
	//    then answer 202: from here on, the event is durable.
	ctx, cancel := context.WithTimeout(r.Context(), h.ProduceTimeout)
	defer cancel()
	if err := h.Producer.Produce(ctx, msgs); err != nil {
		// Some messages may already be written. The client retries the whole
		// batch; duplicates are removed downstream by event ID.
		h.Log.Error("produce failed", "project", projectID, "events", len(msgs), "err", err)
		counters.queueErrors.Add(uint64(len(msgs)))
		w.Header().Set("Retry-After", "1")
		writeError(w, http.StatusServiceUnavailable, "queue unavailable, retry later")
		return
	}

	resp.Accepted = len(msgs)
	counters.accepted.Add(uint64(len(msgs)))
	writeJSON(w, http.StatusAccepted, resp)
}

// retryAfterSeconds renders a wait as whole seconds for the Retry-After
// header, rounded up so a client never retries before the tokens exist.
func retryAfterSeconds(d time.Duration) string {
	return strconv.Itoa(max(1, int(math.Ceil(d.Seconds()))))
}

// prepare decodes and validates one event and stamps the server-owned
// fields. On a validation error it still returns the decoded event when
// there is one, so the rejection can name its event_id.
func (h *Handler) prepare(raw json.RawMessage, projectID string, now time.Time) (*event.Event, error) {
	if len(raw) > event.MaxEventBytes {
		return nil, errors.New("event is larger than 64 KiB")
	}
	var e event.Event
	if err := json.Unmarshal(raw, &e); err != nil {
		return nil, errors.New("event is not valid JSON for the event schema")
	}
	if err := event.Validate(&e); err != nil {
		return &e, err
	}

	// Server-owned fields: whatever the client sent is overwritten.
	e.ProjectID = projectID
	e.ReceivedAt = now

	// A timestamp outside the window means a broken client clock. Keep the
	// event, use the server time.
	if e.Timestamp.Before(now.Add(-event.MaxTimestampAge)) || e.Timestamp.After(now.Add(event.MaxTimestampFuture)) {
		e.Timestamp = now
	}
	e.Timestamp = e.Timestamp.UTC()
	return &e, nil
}

// readBody returns the decompressed body, or a status and message on failure.
func readBody(w http.ResponseWriter, r *http.Request) (body []byte, status int, msg string) {
	limited := http.MaxBytesReader(w, r.Body, event.MaxBatchBytes)
	defer limited.Close()

	var reader io.Reader = limited
	switch r.Header.Get("Content-Encoding") {
	case "", "identity":
	case "gzip":
		gz, err := gzip.NewReader(limited)
		if err != nil {
			return nil, http.StatusBadRequest, "body is not valid gzip"
		}
		defer gz.Close()
		reader = gz
	default:
		return nil, http.StatusUnsupportedMediaType, "Content-Encoding must be gzip or none"
	}

	body, err := io.ReadAll(io.LimitReader(reader, event.MaxBatchBytes+1))
	var tooBig *http.MaxBytesError
	switch {
	case errors.As(err, &tooBig):
		return nil, http.StatusRequestEntityTooLarge, "body is larger than 1 MiB"
	case err != nil:
		return nil, http.StatusBadRequest, "could not read body"
	case len(body) > event.MaxBatchBytes:
		return nil, http.StatusRequestEntityTooLarge, "decompressed body is larger than 1 MiB"
	}
	return body, 0, ""
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, event.ErrorResponse{Error: msg})
}
