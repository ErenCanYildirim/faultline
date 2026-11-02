package natsjs

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/erencanyildirim/faultline/internal/queue"
)

// Producer publishes to the events subject.
type Producer struct {
	nc  *nats.Conn
	js  jetstream.JetStream
	cfg ProducerConfig
}

// ProducerConfig configures NewProducer.
type ProducerConfig struct {
	URL     string
	Stream  string // default EVENTS; checked by Ping
	Subject string // default events.raw
}

var _ queue.Producer = (*Producer)(nil)

// NewProducer connects to url. It returns before the connection is up;
// Ping reports the state.
func NewProducer(cfg ProducerConfig) (*Producer, error) {
	if cfg.Stream == "" {
		cfg.Stream = DefaultStream
	}
	if cfg.Subject == "" {
		cfg.Subject = DefaultSubject
	}
	nc, err := connect(cfg.URL, "faultline-ingest")
	if err != nil {
		return nil, err
	}
	js, err := jetstream.New(nc,
		// Concurrent ingest requests share one connection; allow enough
		// unacknowledged publishes for all of them.
		jetstream.WithPublishAsyncMaxPending(65536),
		// A publish whose ack never comes (connection lost after sending)
		// fails after this, instead of counting against the limit forever.
		jetstream.WithPublishAsyncTimeout(10*time.Second),
	)
	if err != nil {
		nc.Close()
		return nil, err
	}
	return &Producer{nc: nc, js: js, cfg: cfg}, nil
}

// Produce publishes every message and waits until the stream has stored
// each one. The event ID goes in Nats-Msg-Id, so the stream drops a message
// it already stored in the last 2 minutes: an SDK retrying a request whose
// first attempt did get stored creates no duplicate.
func (p *Producer) Produce(ctx context.Context, msgs []queue.Message) error {
	futures := make([]jetstream.PubAckFuture, 0, len(msgs))
	for _, m := range msgs {
		msg := nats.NewMsg(p.cfg.Subject)
		msg.Data = m.Value
		msg.Header.Set(ProjectHeader, m.ProjectID)
		var opts []jetstream.PublishOpt
		if len(m.Key) > 0 {
			opts = append(opts, jetstream.WithMsgID(string(m.Key)))
		}
		f, err := p.js.PublishMsgAsync(msg, opts...)
		if err != nil {
			return fmt.Errorf("natsjs: publishing: %w", err)
		}
		futures = append(futures, f)
	}
	for _, f := range futures {
		select {
		case <-f.Ok():
		case err := <-f.Err():
			return fmt.Errorf("natsjs: publish not acknowledged: %w", err)
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return nil
}

// Ping checks that the server answers and the stream exists.
func (p *Producer) Ping(ctx context.Context) error {
	if !p.nc.IsConnected() {
		return errors.New("natsjs: not connected")
	}
	_, err := p.js.Stream(ctx, p.cfg.Stream)
	return err
}

// Close releases the connection. Produce waits for its acks, so nothing is
// left in flight.
func (p *Producer) Close() {
	p.nc.Close()
}
