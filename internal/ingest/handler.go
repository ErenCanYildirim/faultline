package ingest

import (
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"time"

	"github.com/erencanyildirim/faultline/event"
	"github.com/erencanyildirim/faultline/internal/queue"
)

const DefaultProduceTimeout = 5 * time.Second

type Handler struct {
	Projects       *Projects
	Producer       queue.Producer
	Log            *slog.Logger
	ProduceTimeout time.Duration
	// Now is the server clock; tests replace it.
	Now func() time.Time
}

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
	return mux
}

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

	if len(msgs) == 0 {
		writeJSON(w, http.StatusBadRequest, resp)
		return
	}

	// 5. Write to the queue and wait for the broker's acknowledgement. Only
	//    then answer 202: from here on, the event is durable.
	ctx, cancel := context.WithTimeout(r.Context(), h.ProduceTimeout)
	defer cancel()
	if err := h.Producer.Produce(ctx, msgs); err != nil {
		// Some messages may already be written. The client retries the whole
		// batch; duplicates are removed downstream by event ID.
		h.Log.Error("produce failed", "project", projectID, "events", len(msgs), "err", err)
		w.Header().Set("Retry-After", "1")
		writeError(w, http.StatusServiceUnavailable, "queue unavailable, retry later")
		return
	}

	resp.Accepted = len(msgs)
	writeJSON(w, http.StatusAccepted, resp)
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
