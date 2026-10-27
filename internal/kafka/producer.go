// Package kafka implements queue.Producer on the Kafka protocol, which
// Redpanda speaks.
package kafka

import (
	"context"
	"errors"
	"fmt"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/erencanyildirim/faultline/internal/queue"
)

const ProjectHeader = "project_id"

// Producer writes to one topic.
type Producer struct {
	client *kgo.Client
}

var _ queue.Producer = (*Producer)(nil)

func NewProducer(brokers []string, topic string) (*Producer, error) {
	if len(brokers) == 0 {
		return nil, errors.New("kafka: no brokers configured")
	}
	if topic == "" {
		return nil, errors.New("kafka: no topic configured")
	}
	client, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.DefaultProduceTopic(topic),
		// Wait for every in-sync replica. With one Redpanda node this means
		// the leader wrote it; the setting is what production would use.
		kgo.RequiredAcks(kgo.AllISRAcks()),
		// Idempotent producer (the franz-go default with acks=all): the
		// broker drops duplicates caused by the client's own internal
		// retries, so retries inside one Produce call never duplicate.
	)
	if err != nil {
		return nil, fmt.Errorf("kafka: creating client: %w", err)
	}
	return &Producer{client: client}, nil
}

// Produce writes all messages and waits for every acknowledgement.
func (p *Producer) Produce(ctx context.Context, msgs []queue.Message) error {
	records := make([]*kgo.Record, len(msgs))
	for i, m := range msgs {
		records[i] = &kgo.Record{
			Key:   m.Key,
			Value: m.Value,
			Headers: []kgo.RecordHeader{
				{Key: ProjectHeader, Value: []byte(m.ProjectID)},
			},
		}
	}
	return p.client.ProduceSync(ctx, records...).FirstErr()
}

// Ping checks that at least one broker answers.
func (p *Producer) Ping(ctx context.Context) error {
	return p.client.Ping(ctx)
}

// Close releases connections. Produce waits for its acknowledgements, so
// nothing is left buffered by the time Close is called.
func (p *Producer) Close() {
	p.client.Close()
}
