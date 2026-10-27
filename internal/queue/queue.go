// Package queue is the boundary between the services and the message
// broker. Ingest and worker depend on these interfaces, not on Kafka, so a
// second broker (NATS, Phase 5) is a new implementation, not a rewrite.
package queue

import (
	"context"
	"errors"
)

// ErrClosed is returned by Consumer.Poll once the consumer is closed.
var ErrClosed = errors.New("queue: consumer closed")

// Message is one event on its way to the queue.
type Message struct {
	// Key decides the partition. Ingest uses the event ID, which spreads
	// events evenly and keeps one noisy project off a single partition.
	Key []byte
	// Value is the event encoded as JSON.
	Value []byte
	// ProjectID travels as a header so consumers can route or filter
	// without decoding the value.
	ProjectID string
}

// Producer writes messages durably.
type Producer interface {
	// Produce returns only after every message is acknowledged by the
	// broker, or with an error if any one was not. On error, some messages
	// may still have been written; callers retry the whole set and rely on
	// downstream deduplication by event ID.
	Produce(ctx context.Context, msgs []Message) error

	// Ping checks that the broker is reachable, for readiness probes.
	Ping(ctx context.Context) error

	Close()
}

// Delivery is one message read from the queue.
type Delivery struct {
	Key       []byte
	Value     []byte
	ProjectID string
	// Where it came from, for logs and dead letters.
	Topic     string
	Partition int32
	Offset    int64
	// Handle is the implementation's own record, needed to commit it.
	Handle any
}

// Consumer reads messages as part of a consumer group. Offsets are never
// committed automatically: the caller commits after it has stored what it
// read, which is what makes delivery at-least-once instead of at-most-once.
type Consumer interface {
	// Poll waits until at least one message is available or ctx is done,
	// and returns at most max messages. A done ctx is not an error: it
	// returns what it has, possibly nothing.
	Poll(ctx context.Context, max int) ([]Delivery, error)

	// Commit marks everything up to and including these deliveries as
	// processed, per partition.
	Commit(ctx context.Context, ds []Delivery) error

	Close()
}

// DeadLetter is a message the consumer could not process, with the reason.
type DeadLetter struct {
	Delivery Delivery
	Reason   string
}

// DeadLetterWriter stores messages that can't be processed, so they don't
// block their partition and can be inspected or replayed later.
type DeadLetterWriter interface {
	// WriteDeadLetters returns only after every dead letter is durable.
	WriteDeadLetters(ctx context.Context, dls []DeadLetter) error
}
