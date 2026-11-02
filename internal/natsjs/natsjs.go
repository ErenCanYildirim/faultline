// Package natsjs implements the queue interfaces on NATS JetStream, as the
// second broker next to Redpanda (package kafka). Ingest and worker don't
// know which one they use; QUEUE=nats picks this one.
//
// How the Kafka concepts map:
//
//	topic events.raw         stream EVENTS, subject events.raw
//	consumer group           one durable pull consumer, shared by every worker
//	partitions               none: the server hands each message to whichever
//	                         worker asks next, so workers scale past 6
//	commit offset            ack each message (AckExplicit)
//	rebalance on a crash     redelivery after AckWait for unacknowledged messages
//	idempotent producer      Nats-Msg-Id = event ID, deduplicated by the stream
//	                         for 2 minutes, also across separate requests
package natsjs

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// Names and defaults. The subjects mirror the Kafka topic names.
const (
	DefaultURL       = "nats://localhost:4222"
	DefaultStream    = "EVENTS"
	DefaultDLQStream = "EVENTS_DLQ"
	DefaultConsumer  = "faultline-worker"
	DefaultSubject   = "events.raw"
	DefaultDLQ       = "events.dlq"

	// ProjectHeader carries the project ID, like the Kafka record header.
	ProjectHeader = "project_id"

	// DefaultAckWait is how long the server waits for an ack before it
	// redelivers a message to any worker. The worker extends it while it
	// works on a batch (KeepAlive), so it only runs out when a worker dies.
	DefaultAckWait = 30 * time.Second
	// DefaultMaxAckPending caps the messages handed out but not yet
	// acknowledged, over all workers: the backpressure limit. It must stay
	// above workers x batch size, or workers starve.
	DefaultMaxAckPending = 100_000
)

// Headers added to dead letters, next to the original ones.
const (
	DLQReasonHeader   = "dlq_reason"
	DLQSubjectHeader  = "dlq_source_subject"
	DLQSequenceHeader = "dlq_source_sequence"
)

// connect opens a connection that keeps reconnecting forever. It returns at
// once even if the server isn't up yet, so a service can start before the
// broker; calls fail until it connects.
func connect(url, name string) (*nats.Conn, error) {
	if url == "" {
		return nil, errors.New("natsjs: no URL configured")
	}
	nc, err := nats.Connect(url,
		nats.Name(name),
		nats.RetryOnFailedConnect(true),
		nats.MaxReconnects(-1),
		nats.ReconnectWait(time.Second),
		// No buffering while disconnected: a publish fails at once and
		// ingest answers 503, instead of holding events in memory that a
		// crash would lose after the client was told nothing.
		nats.ReconnectBufSize(-1),
	)
	if err != nil {
		return nil, fmt.Errorf("natsjs: connecting to %s: %w", url, err)
	}
	return nc, nil
}

// SetupConfig names what Setup creates.
type SetupConfig struct {
	Stream, Subject       string // default EVENTS, events.raw
	DLQStream, DLQSubject string // default EVENTS_DLQ, events.dlq
	Consumer              string // default faultline-worker
	AckWait               time.Duration
	MaxAckPending         int
}

func (c *SetupConfig) defaults() {
	if c.Stream == "" {
		c.Stream = DefaultStream
	}
	if c.Subject == "" {
		c.Subject = DefaultSubject
	}
	if c.DLQStream == "" {
		c.DLQStream = DefaultDLQStream
	}
	if c.DLQSubject == "" {
		c.DLQSubject = DefaultDLQ
	}
	if c.Consumer == "" {
		c.Consumer = DefaultConsumer
	}
	if c.AckWait <= 0 {
		c.AckWait = DefaultAckWait
	}
	if c.MaxAckPending <= 0 {
		c.MaxAckPending = DefaultMaxAckPending
	}
}

// Setup creates or updates the streams and the worker's durable consumer.
// It is the NATS counterpart of the redpanda-init job, run by cmd/natsinit,
// and safe to run on every start: a changed setting is applied, an
// unchanged one left alone. The services themselves never create anything,
// so a misnamed stream fails loudly instead of being created empty.
func Setup(ctx context.Context, url string, cfg SetupConfig) error {
	cfg.defaults()
	nc, err := nats.Connect(url, nats.Name("faultline-natsinit"), nats.Timeout(10*time.Second))
	if err != nil {
		return fmt.Errorf("natsjs: connecting to %s: %w", url, err)
	}
	defer nc.Close()
	js, err := jetstream.New(nc)
	if err != nil {
		return err
	}

	// File storage, one replica (single node, like the Redpanda setup).
	// Kept 1 day: ClickHouse is the store; the stream only buffers.
	if _, err := js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name:      cfg.Stream,
		Subjects:  []string{cfg.Subject},
		Storage:   jetstream.FileStorage,
		Retention: jetstream.LimitsPolicy,
		MaxAge:    24 * time.Hour,
		Replicas:  1,
		// Window in which a repeated Nats-Msg-Id is dropped.
		Duplicates: 2 * time.Minute,
	}); err != nil {
		return fmt.Errorf("natsjs: stream %s: %w", cfg.Stream, err)
	}
	if _, err := js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name:     cfg.DLQStream,
		Subjects: []string{cfg.DLQSubject},
		Storage:  jetstream.FileStorage,
		MaxAge:   7 * 24 * time.Hour,
		Replicas: 1,
	}); err != nil {
		return fmt.Errorf("natsjs: stream %s: %w", cfg.DLQStream, err)
	}

	if _, err := js.CreateOrUpdateConsumer(ctx, cfg.Stream, jetstream.ConsumerConfig{
		Durable: cfg.Consumer,
		// Each message acknowledged by itself. AckAll ("everything up to
		// here", like a Kafka offset) would be wrong with several workers
		// on one consumer: it would acknowledge messages another worker
		// still holds.
		AckPolicy:     jetstream.AckExplicitPolicy,
		AckWait:       cfg.AckWait,
		MaxAckPending: cfg.MaxAckPending,
		// Never give up on a message: while ClickHouse is down, messages
		// may be delivered many times, and dropping them would be loss.
		MaxDeliver: -1,
		// A new consumer starts at the oldest message, so events produced
		// before the first worker ran are not skipped.
		DeliverPolicy: jetstream.DeliverAllPolicy,
	}); err != nil {
		return fmt.Errorf("natsjs: consumer %s: %w", cfg.Consumer, err)
	}
	return nil
}
