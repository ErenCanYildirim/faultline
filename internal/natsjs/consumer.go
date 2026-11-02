package natsjs

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/erencanyildirim/faultline/internal/queue"
)

// ConsumerConfig configures NewConsumer.
type ConsumerConfig struct {
	URL        string
	Stream     string // default EVENTS
	Consumer   string // default faultline-worker; created by Setup
	DLQSubject string // default events.dlq
}

// Consumer reads from the shared durable consumer and writes dead letters
// to the DLQ stream with the same connection.
//
// Every Poll is its own pull request, with no client-side buffer or
// long-lived subscription. A first version used the library's continuous
// iterator with a prefetch buffer; after a NATS restart it stopped
// requesting messages (the worker polls in short calls, and the iterator
// only notices a reconnect inside one long call), and the worker stalled
// with 60,000 events waiting. One-shot pulls have no state to lose: after a
// reconnect, the next Poll simply asks again.
type Consumer struct {
	nc      *nats.Conn
	js      jetstream.JetStream
	cfg     ConsumerConfig
	closed  atomic.Bool
	mu      sync.Mutex // guards cons and ackWait
	cons    jetstream.Consumer
	ackWait time.Duration
}

var (
	_ queue.Consumer         = (*Consumer)(nil)
	_ queue.DeadLetterWriter = (*Consumer)(nil)
	_ queue.KeepAliver       = (*Consumer)(nil)
)

// NewConsumer connects. It binds to the durable consumer on the first Poll,
// so the worker can start before NATS is ready; Poll reports the error and
// the worker retries.
func NewConsumer(cfg ConsumerConfig) (*Consumer, error) {
	if cfg.Stream == "" {
		cfg.Stream = DefaultStream
	}
	if cfg.Consumer == "" {
		cfg.Consumer = DefaultConsumer
	}
	if cfg.DLQSubject == "" {
		cfg.DLQSubject = DefaultDLQ
	}
	nc, err := connect(cfg.URL, "faultline-worker")
	if err != nil {
		return nil, err
	}
	js, err := jetstream.New(nc)
	if err != nil {
		nc.Close()
		return nil, err
	}
	return &Consumer{nc: nc, js: js, cfg: cfg}, nil
}

// consumer binds to the durable consumer the first time it is needed.
func (c *Consumer) consumer(ctx context.Context) (jetstream.Consumer, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.cons != nil {
		return c.cons, nil
	}
	cons, err := c.js.Consumer(ctx, c.cfg.Stream, c.cfg.Consumer)
	if err != nil {
		return nil, fmt.Errorf("natsjs: binding to consumer %s on stream %s (created by natsinit): %w",
			c.cfg.Consumer, c.cfg.Stream, err)
	}
	c.cons = cons
	c.ackWait = cons.CachedInfo().Config.AckWait
	return cons, nil
}

// Poll waits for the first message, then takes whatever else is available
// right now, up to max. Like Kafka's poll, it returns as soon as there is
// something, rather than waiting for a full batch: two pull requests, one
// for a single message that waits until ctx is done, one that doesn't wait.
func (c *Consumer) Poll(ctx context.Context, max int) ([]queue.Delivery, error) {
	if c.closed.Load() {
		return nil, queue.ErrClosed
	}
	cons, err := c.consumer(ctx)
	if err != nil {
		if ctx.Err() != nil {
			return nil, nil
		}
		return nil, err
	}

	// The request expires on the server just before ctx does, so the
	// server doesn't send a message after this client stopped listening.
	// (If it does anyway, that message is redelivered after AckWait.)
	if _, ok := ctx.Deadline(); !ok {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
	}
	first, err := cons.Fetch(1, jetstream.FetchContext(ctx))
	if err != nil {
		return nil, c.pollError(ctx, err)
	}
	out := make([]queue.Delivery, 0, max)
	for m := range first.Messages() {
		out = append(out, delivery(m))
	}
	if err := first.Error(); err != nil {
		if perr := c.pollError(ctx, err); perr != nil || len(out) == 0 {
			return out, perr
		}
	}
	if len(out) == 0 || max <= 1 {
		return out, nil
	}

	rest, err := cons.FetchNoWait(max - 1)
	if err != nil {
		// The first message is returned anyway; the next Poll retries.
		return out, nil
	}
	for m := range rest.Messages() {
		out = append(out, delivery(m))
	}
	return out, nil
}

func (c *Consumer) pollError(ctx context.Context, err error) error {
	switch {
	case c.closed.Load():
		return queue.ErrClosed
	case ctx.Err() != nil, errors.Is(err, context.DeadlineExceeded), errors.Is(err, context.Canceled),
		errors.Is(err, nats.ErrTimeout), errors.Is(err, jetstream.ErrNoMessages):
		// The caller bounds the wait with ctx; nothing arrived.
		return nil
	case errors.Is(err, jetstream.ErrInvalidOption):
		// FetchContext with a deadline that already passed.
		return nil
	case errors.Is(err, jetstream.ErrConsumerNotFound), errors.Is(err, jetstream.ErrConsumerDeleted):
		// Bind again on the next Poll, once natsinit has recreated it.
		c.mu.Lock()
		c.cons = nil
		c.mu.Unlock()
		return fmt.Errorf("natsjs: consumer gone: %w", err)
	default:
		return fmt.Errorf("natsjs: pulling: %w", err)
	}
}

func delivery(m jetstream.Msg) queue.Delivery {
	d := queue.Delivery{
		Value:     m.Data(),
		ProjectID: m.Headers().Get(ProjectHeader),
		Topic:     m.Subject(),
		Handle:    m,
	}
	if id := m.Headers().Get(nats.MsgIdHdr); id != "" {
		d.Key = []byte(id)
	}
	if meta, err := m.Metadata(); err == nil {
		d.Offset = int64(meta.Sequence.Stream)
	}
	return d
}

func msgsOf(ds []queue.Delivery) ([]jetstream.Msg, error) {
	out := make([]jetstream.Msg, 0, len(ds))
	for _, d := range ds {
		m, ok := d.Handle.(jetstream.Msg)
		if !ok {
			return nil, errors.New("natsjs: delivery was not produced by this consumer")
		}
		out = append(out, m)
	}
	return out, nil
}

// Commit acknowledges every delivery, then flushes the connection: the
// server has read all the acks when Flush returns. An ack that is lost
// anyway (connection drops in between) means the message is delivered
// again after AckWait and its duplicate removed by ClickHouse.
func (c *Consumer) Commit(ctx context.Context, ds []queue.Delivery) error {
	msgs, err := msgsOf(ds)
	if err != nil {
		return err
	}
	for _, m := range msgs {
		if err := m.Ack(); err != nil && !errors.Is(err, jetstream.ErrMsgAlreadyAckd) {
			return fmt.Errorf("natsjs: ack: %w", err)
		}
	}
	if _, ok := ctx.Deadline(); !ok {
		// Flush needs a deadline.
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, 10*time.Second)
		defer cancel()
	}
	return c.nc.FlushWithContext(ctx)
}

// KeepAlive sends "in progress" for every delivery every third of AckWait
// until stop is called, so the server doesn't hand the batch to another
// worker while this one is still storing it (for example while it retries
// against a ClickHouse that is down).
func (c *Consumer) KeepAlive(ds []queue.Delivery) (stop func()) {
	msgs, err := msgsOf(ds)
	c.mu.Lock()
	ackWait := c.ackWait
	c.mu.Unlock()
	if err != nil || ackWait <= 0 {
		return func() {}
	}
	done := make(chan struct{})
	finished := make(chan struct{})
	go func() {
		defer close(finished)
		t := time.NewTicker(ackWait / 3)
		defer t.Stop()
		for {
			select {
			case <-done:
				return
			case <-t.C:
				for _, m := range msgs {
					// Best effort: if it fails, the message is redelivered
					// later and deduplicated, nothing is lost.
					_ = m.InProgress()
				}
			}
		}
	}()
	return func() {
		close(done)
		<-finished
	}
}

// WriteDeadLetters publishes the original messages to the DLQ stream with
// their headers, plus headers saying why and where from, and waits until
// the stream has stored every one.
func (c *Consumer) WriteDeadLetters(ctx context.Context, dls []queue.DeadLetter) error {
	futures := make([]jetstream.PubAckFuture, 0, len(dls))
	for _, dl := range dls {
		msg := nats.NewMsg(c.cfg.DLQSubject)
		msg.Data = dl.Delivery.Value
		if orig, ok := dl.Delivery.Handle.(jetstream.Msg); ok {
			for k, vs := range orig.Headers() {
				if k == nats.MsgIdHdr {
					continue // the DLQ stream must keep every copy
				}
				for _, v := range vs {
					msg.Header.Add(k, v)
				}
			}
		} else if dl.Delivery.ProjectID != "" {
			msg.Header.Set(ProjectHeader, dl.Delivery.ProjectID)
		}
		msg.Header.Set(DLQReasonHeader, dl.Reason)
		msg.Header.Set(DLQSubjectHeader, dl.Delivery.Topic)
		msg.Header.Set(DLQSequenceHeader, strconv.FormatInt(dl.Delivery.Offset, 10))
		f, err := c.js.PublishMsgAsync(msg)
		if err != nil {
			return fmt.Errorf("natsjs: publishing dead letter: %w", err)
		}
		futures = append(futures, f)
	}
	for _, f := range futures {
		select {
		case <-f.Ok():
		case err := <-f.Err():
			return fmt.Errorf("natsjs: dead letter not acknowledged: %w", err)
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return nil
}

// Close releases the connection. Nothing is buffered on the client, so
// nothing is left to hand back: what the worker didn't commit is
// redelivered after AckWait.
func (c *Consumer) Close() {
	if !c.closed.CompareAndSwap(false, true) {
		return
	}
	_ = c.nc.FlushTimeout(2 * time.Second)
	c.nc.Close()
}
