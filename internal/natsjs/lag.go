package natsjs

import (
	"context"
	"fmt"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// LagReader reads how far the worker's consumer is behind the stream.
type LagReader struct {
	nc       *nats.Conn
	js       jetstream.JetStream
	stream   string
	consumer string
}

// NewLagReader connects to url.
func NewLagReader(url, stream, consumer string) (*LagReader, error) {
	if stream == "" {
		stream = DefaultStream
	}
	if consumer == "" {
		consumer = DefaultConsumer
	}
	nc, err := connect(url, "faultline-lag")
	if err != nil {
		return nil, err
	}
	js, err := jetstream.New(nc)
	if err != nil {
		nc.Close()
		return nil, err
	}
	return &LagReader{nc: nc, js: js, stream: stream, consumer: consumer}, nil
}

// Lag is the consumer's backlog.
type Lag struct {
	// Pending: in the stream, not yet delivered to any worker.
	Pending uint64
	// AckPending: delivered, not yet acknowledged (in a worker's batch).
	AckPending int
	// Redelivered: messages currently being delivered again.
	Redelivered int
	// Total is what the Kafka lag counts: everything not yet committed.
	Total int64
}

// Read asks the server for the consumer's state.
func (r *LagReader) Read(ctx context.Context) (Lag, error) {
	cons, err := r.js.Consumer(ctx, r.stream, r.consumer)
	if err != nil {
		return Lag{}, fmt.Errorf("natsjs: consumer %s: %w", r.consumer, err)
	}
	info, err := cons.Info(ctx)
	if err != nil {
		return Lag{}, fmt.Errorf("natsjs: consumer info: %w", err)
	}
	return Lag{
		Pending:     info.NumPending,
		AckPending:  info.NumAckPending,
		Redelivered: info.NumRedelivered,
		Total:       int64(info.NumPending) + int64(info.NumAckPending),
	}, nil
}

// Close releases the connection.
func (r *LagReader) Close() { r.nc.Close() }
