package kafka

import (
	"context"
	"errors"
	"fmt"
	"strconv"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/erencanyildirim/faultline/internal/queue"
)

// Headers added to dead letters, next to the original ones.
const (
	DLQReasonHeader    = "dlq_reason"
	DLQTopicHeader     = "dlq_source_topic"
	DLQPartitionHeader = "dlq_source_partition"
	DLQOffsetHeader    = "dlq_source_offset"
)

// Consumer reads one topic in a consumer group and writes dead letters to
// another topic with the same client.
type Consumer struct {
	client   *kgo.Client
	dlqTopic string
}

var (
	_ queue.Consumer         = (*Consumer)(nil)
	_ queue.DeadLetterWriter = (*Consumer)(nil)
)

// ConsumerConfig configures NewConsumer.
type ConsumerConfig struct {
	Brokers  []string
	Topic    string
	Group    string
	DLQTopic string
}

// NewConsumer joins the consumer group. It does not connect until first use.
func NewConsumer(cfg ConsumerConfig) (*Consumer, error) {
	if len(cfg.Brokers) == 0 || cfg.Topic == "" || cfg.Group == "" || cfg.DLQTopic == "" {
		return nil, errors.New("kafka: consumer needs brokers, topic, group and dlq topic")
	}
	client, err := kgo.NewClient(
		kgo.SeedBrokers(cfg.Brokers...),
		kgo.ConsumerGroup(cfg.Group),
		kgo.ConsumeTopics(cfg.Topic),
		// The worker commits after ClickHouse confirms the insert. Auto-commit
		// would commit on a timer, possibly before the insert, and a crash in
		// between would lose those events.
		kgo.DisableAutoCommit(),
		// A group with no committed offsets starts at the oldest message, so
		// events produced before the first worker ever ran are not skipped.
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		// For dead letters.
		kgo.RequiredAcks(kgo.AllISRAcks()),
	)
	if err != nil {
		return nil, fmt.Errorf("kafka: creating consumer: %w", err)
	}
	return &Consumer{client: client, dlqTopic: cfg.DLQTopic}, nil
}

// Poll returns up to max deliveries.
func (c *Consumer) Poll(ctx context.Context, max int) ([]queue.Delivery, error) {
	fetches := c.client.PollRecords(ctx, max)
	if fetches.IsClientClosed() {
		return nil, queue.ErrClosed
	}
	var firstErr error
	fetches.EachError(func(topic string, partition int32, err error) {
		// A deadline or cancellation on ctx is how callers bound the wait.
		if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
			return
		}
		if firstErr == nil {
			firstErr = fmt.Errorf("kafka: fetching %s[%d]: %w", topic, partition, err)
		}
	})

	out := make([]queue.Delivery, 0, fetches.NumRecords())
	fetches.EachRecord(func(r *kgo.Record) {
		d := queue.Delivery{
			Key:       r.Key,
			Value:     r.Value,
			Topic:     r.Topic,
			Partition: r.Partition,
			Offset:    r.Offset,
			Handle:    r,
		}
		for _, h := range r.Headers {
			if h.Key == ProjectHeader {
				d.ProjectID = string(h.Value)
			}
		}
		out = append(out, d)
	})
	// Records that did arrive are returned even if a partition reported an
	// error, so one bad partition doesn't stall the others.
	return out, firstErr
}

// Commit commits the highest offset per partition among ds.
func (c *Consumer) Commit(ctx context.Context, ds []queue.Delivery) error {
	records := make([]*kgo.Record, 0, len(ds))
	for _, d := range ds {
		r, ok := d.Handle.(*kgo.Record)
		if !ok {
			return errors.New("kafka: delivery was not produced by this consumer")
		}
		records = append(records, r)
	}
	return c.client.CommitRecords(ctx, records...)
}

// WriteDeadLetters writes the original messages to the DLQ topic, keeping
// key, value and headers, plus headers saying why and where from.
func (c *Consumer) WriteDeadLetters(ctx context.Context, dls []queue.DeadLetter) error {
	records := make([]*kgo.Record, 0, len(dls))
	for _, dl := range dls {
		var headers []kgo.RecordHeader
		if orig, ok := dl.Delivery.Handle.(*kgo.Record); ok {
			headers = append(headers, orig.Headers...)
		}
		headers = append(headers,
			kgo.RecordHeader{Key: DLQReasonHeader, Value: []byte(dl.Reason)},
			kgo.RecordHeader{Key: DLQTopicHeader, Value: []byte(dl.Delivery.Topic)},
			kgo.RecordHeader{Key: DLQPartitionHeader, Value: []byte(strconv.Itoa(int(dl.Delivery.Partition)))},
			kgo.RecordHeader{Key: DLQOffsetHeader, Value: []byte(strconv.FormatInt(dl.Delivery.Offset, 10))},
		)
		records = append(records, &kgo.Record{
			Topic:   c.dlqTopic,
			Key:     dl.Delivery.Key,
			Value:   dl.Delivery.Value,
			Headers: headers,
		})
	}
	return c.client.ProduceSync(ctx, records...).FirstErr()
}

// Close leaves the group, so its partitions move to other workers at once
// instead of after a session timeout.
func (c *Consumer) Close() {
	c.client.Close()
}
