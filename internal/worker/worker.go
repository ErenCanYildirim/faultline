// Package worker moves events from the queue into storage.
//
// The loop: poll messages until the batch is full or old enough, decode
// them, send the ones that can't be stored to the dead-letter queue, scrub
// personal data from the rest, group them, insert them into ClickHouse, and
// only then commit the offsets.
//
// Delivery is at-least-once. A crash after the insert but before the commit
// means the batch is read and inserted again on restart; the events table
// removes those duplicates by event_id. The reverse order (commit, then
// insert) would turn the same crash into lost events.
package worker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sync/atomic"
	"time"

	"github.com/erencanyildirim/faultline/event"
	"github.com/erencanyildirim/faultline/internal/grouping"
	"github.com/erencanyildirim/faultline/internal/queue"
	"github.com/erencanyildirim/faultline/internal/scrub"
)

// Defaults for Worker fields left at zero.
const (
	// ClickHouse prefers few large inserts over many small ones: each insert
	// creates a part on disk that later has to be merged.
	DefaultBatchSize       = 1000
	DefaultFlushInterval   = time.Second
	DefaultShutdownTimeout = 10 * time.Second
	DefaultRetryInitial    = 200 * time.Millisecond
	DefaultRetryMax        = 5 * time.Second
)

// EventStore is where valid events go, each with its grouping fingerprint.
type EventStore interface {
	InsertEvents(ctx context.Context, events []grouping.Grouped) error
}

// Worker runs the consume-store-commit loop.
type Worker struct {
	Consumer queue.Consumer
	DLQ      queue.DeadLetterWriter
	Store    EventStore
	Log      *slog.Logger
	// Scrubber removes personal data before grouping and storage. Nil means
	// the default: everything scrubbed, no project allowed to keep IPs.
	// There is deliberately no way to turn scrubbing off.
	Scrubber *scrub.Scrubber

	BatchSize       int
	FlushInterval   time.Duration
	ShutdownTimeout time.Duration
	RetryInitial    time.Duration
	RetryMax        time.Duration

	stored, deadLettered, batches, scrubbed atomic.Uint64
	stages                                  stageTimes
}

// Stats are cumulative counters.
type Stats struct {
	Stored       uint64 // events inserted and committed
	DeadLettered uint64 // messages written to the DLQ and committed
	Batches      uint64 // batches stored and committed
	Scrubbed     uint64 // values replaced by the scrubber (counted per attempt)
}

// Stats returns the counters.
func (w *Worker) Stats() Stats {
	return Stats{
		Stored:       w.stored.Load(),
		DeadLettered: w.deadLettered.Load(),
		Batches:      w.batches.Load(),
		Scrubbed:     w.scrubbed.Load(),
	}
}

func (w *Worker) defaults() {
	if w.Log == nil {
		w.Log = slog.New(slog.DiscardHandler)
	}
	if w.BatchSize <= 0 {
		w.BatchSize = DefaultBatchSize
	}
	if w.FlushInterval <= 0 {
		w.FlushInterval = DefaultFlushInterval
	}
	if w.ShutdownTimeout <= 0 {
		w.ShutdownTimeout = DefaultShutdownTimeout
	}
	if w.RetryInitial <= 0 {
		w.RetryInitial = DefaultRetryInitial
	}
	if w.RetryMax <= 0 {
		w.RetryMax = DefaultRetryMax
	}
	if w.Scrubber == nil {
		w.Scrubber = scrub.New(nil)
	}
}

// Run consumes until ctx is cancelled. On cancellation it tries to store and
// commit the batch in hand within ShutdownTimeout; whatever it can't is
// left uncommitted and will be redelivered.
func (w *Worker) Run(ctx context.Context) error {
	w.defaults()

	var batch []queue.Delivery
	var started time.Time

	for {
		if ctx.Err() != nil {
			return w.shutdown(batch)
		}

		// Wait for messages, but never past the moment the current batch is due.
		wait := w.FlushInterval
		if len(batch) > 0 {
			wait = w.FlushInterval - time.Since(started)
		}
		if wait > 0 {
			pollCtx, cancel := context.WithTimeout(ctx, wait)
			pollStart := time.Now()
			ds, err := w.Consumer.Poll(pollCtx, w.BatchSize-len(batch))
			w.stages.add(StagePoll, pollStart)
			cancel()
			if errors.Is(err, queue.ErrClosed) {
				return err
			}
			if err != nil {
				w.Log.Warn("poll error", "err", err)
				if len(ds) == 0 {
					sleep(ctx, time.Second) // don't spin on a broker that keeps failing
				}
			}
			if len(ds) > 0 {
				if len(batch) == 0 {
					started = time.Now()
				}
				batch = append(batch, ds...)
			}
		}

		due := len(batch) >= w.BatchSize || (len(batch) > 0 && time.Since(started) >= w.FlushInterval)
		if !due {
			continue
		}
		if err := w.flush(ctx, batch); err != nil {
			// Only cancellation stops a flush: storage errors are retried.
			return w.shutdown(batch)
		}
		batch = nil
	}
}

func (w *Worker) shutdown(batch []queue.Delivery) error {
	if len(batch) == 0 {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), w.ShutdownTimeout)
	defer cancel()
	if err := w.flush(ctx, batch); err != nil {
		w.Log.Warn("stopped before the last batch was stored; it stays uncommitted and will be redelivered",
			"messages", len(batch))
	}
	return nil
}

// flush stores one batch and commits it. It returns an error only if ctx
// ends first; in that case nothing of this batch is committed.
func (w *Worker) flush(ctx context.Context, batch []queue.Delivery) error {
	events := make([]event.Event, 0, len(batch))
	var dead []queue.DeadLetter
	t := time.Now()
	for _, d := range batch {
		e, reason := decode(d)
		if reason != "" {
			dead = append(dead, queue.DeadLetter{Delivery: d, Reason: reason})
			continue
		}
		events = append(events, e)
	}
	w.stages.add(StageDecode, t)

	// Dead letters first: they must be durable before their offsets are
	// committed, or they'd be lost along with the reason they failed.
	if len(dead) > 0 {
		t = time.Now()
		err := w.retry(ctx, "write dead letters", func(ctx context.Context) error {
			return w.DLQ.WriteDeadLetters(ctx, dead)
		})
		w.stages.add(StageDLQ, t)
		if err != nil {
			return err
		}
		for _, dl := range dead {
			w.Log.Warn("message sent to dead-letter queue", "reason", dl.Reason,
				"topic", dl.Delivery.Topic, "partition", dl.Delivery.Partition, "offset", dl.Delivery.Offset)
		}
	}

	if len(events) > 0 {
		// Scrub first, so personal data never reaches a fingerprint, an
		// issue title or a stored row.
		t = time.Now()
		for i := range events {
			w.scrubbed.Add(uint64(w.Scrubber.Event(&events[i])))
		}
		w.stages.add(StageScrub, t)
		// Grouping happens here, not in ingest: ingest stays fast and
		// stateless, and a change to the algorithm only needs new workers.
		t = time.Now()
		grouped := grouping.Group(events)
		w.stages.add(StageGroup, t)
		t = time.Now()
		err := w.retry(ctx, "insert into clickhouse", func(ctx context.Context) error {
			return w.Store.InsertEvents(ctx, grouped)
		})
		w.stages.add(StageStore, t)
		if err != nil {
			return err
		}
	}

	// Everything in the batch is durable now. A failed commit is not data
	// loss: the batch is redelivered later and its duplicates removed.
	commitCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	t = time.Now()
	if err := w.Consumer.Commit(commitCtx, batch); err != nil {
		w.Log.Warn("commit failed; the batch will be redelivered and deduplicated", "messages", len(batch), "err", err)
	}
	w.stages.add(StageCommit, t)

	w.stored.Add(uint64(len(events)))
	w.deadLettered.Add(uint64(len(dead)))
	w.batches.Add(1)
	w.Log.Debug("batch stored", "events", len(events), "dead_letters", len(dead))
	return nil
}

// retry calls fn until it succeeds or ctx ends, backing off exponentially.
// It never gives up on its own: while storage is down, the worker stops
// consuming and the backlog waits safely in the queue. That pause is the
// backpressure, and consumer lag shows it.
func (w *Worker) retry(ctx context.Context, what string, fn func(context.Context) error) error {
	delay := w.RetryInitial
	for attempt := 1; ; attempt++ {
		err := fn(ctx)
		if err == nil {
			if attempt > 1 {
				w.Log.Info("recovered", "operation", what, "attempts", attempt)
			}
			return nil
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		w.Log.Warn("operation failed, retrying", "operation", what, "attempt", attempt, "retry_in", delay.String(), "err", err)
		if !sleep(ctx, delay) {
			return ctx.Err()
		}
		delay = min(delay*2, w.RetryMax)
	}
}

// decode parses and checks one message. A non-empty reason means the message
// can never be stored and belongs in the dead-letter queue.
func decode(d queue.Delivery) (event.Event, string) {
	var e event.Event
	if err := json.Unmarshal(d.Value, &e); err != nil {
		return e, fmt.Sprintf("not a valid event JSON: %v", err)
	}
	// Ingest already validated it. Checking again protects storage from
	// anything else that writes to the topic, and from contract drift.
	if err := event.Validate(&e); err != nil {
		return e, fmt.Sprintf("invalid event: %v", err)
	}
	if e.ProjectID == "" {
		return e, "event has no project_id"
	}
	return e, ""
}

// sleep waits for d or until ctx ends. It reports whether the full time passed.
func sleep(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return true
	case <-ctx.Done():
		return false
	}
}
