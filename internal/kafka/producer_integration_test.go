//go:build integration

// Runs against a real broker. Start the stack first, then:
//
//	go test -tags integration ./internal/kafka/
//
// Uses KAFKA_BROKERS (default localhost:19092, the compose external listener).
// It creates its own short-lived topic and deletes it at the end, so it never
// touches events.raw or events.dlq.
package kafka

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"

	"github.com/erencanyildirim/faultline/internal/queue"
)

func brokers() []string {
	if v := os.Getenv("KAFKA_BROKERS"); v != "" {
		return strings.Split(v, ",")
	}
	return []string{"localhost:19092"}
}

func TestProducerAgainstRedpanda(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	admin, err := kgo.NewClient(kgo.SeedBrokers(brokers()...))
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close()
	if err := admin.Ping(ctx); err != nil {
		t.Fatalf("broker not reachable at %v (is the stack up?): %v", brokers(), err)
	}

	topic := fmt.Sprintf("faultline-it-%d", time.Now().UnixNano())
	createTopic(t, ctx, admin, topic)
	t.Cleanup(func() { deleteTopic(admin, topic) })

	p, err := NewProducer(brokers(), topic)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()

	if err := p.Ping(ctx); err != nil {
		t.Fatalf("Ping: %v", err)
	}

	msgs := []queue.Message{
		{Key: []byte("id-1"), Value: []byte(`{"n":1}`), ProjectID: "demo"},
		{Key: []byte("id-2"), Value: []byte(`{"n":2}`), ProjectID: "demo"},
		{Key: []byte("id-3"), Value: []byte(`{"n":3}`), ProjectID: "other"},
	}
	if err := p.Produce(ctx, msgs); err != nil {
		t.Fatalf("Produce: %v", err)
	}

	// Read everything back from the start of the topic.
	consumer, err := kgo.NewClient(
		kgo.SeedBrokers(brokers()...),
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer consumer.Close()

	got := map[string]*kgo.Record{}
	for len(got) < len(msgs) {
		fetches := consumer.PollFetches(ctx)
		if err := ctx.Err(); err != nil {
			t.Fatalf("timed out after reading %d of %d records", len(got), len(msgs))
		}
		fetches.EachRecord(func(r *kgo.Record) { got[string(r.Key)] = r })
	}

	for _, m := range msgs {
		r, ok := got[string(m.Key)]
		if !ok {
			t.Errorf("record %s not found", m.Key)
			continue
		}
		if string(r.Value) != string(m.Value) {
			t.Errorf("record %s value = %s", m.Key, r.Value)
		}
		if len(r.Headers) != 1 || r.Headers[0].Key != ProjectHeader || string(r.Headers[0].Value) != m.ProjectID {
			t.Errorf("record %s headers = %+v", m.Key, r.Headers)
		}
	}
}

func TestProduceToMissingTopicFails(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	p, err := NewProducer(brokers(), fmt.Sprintf("faultline-missing-%d", time.Now().UnixNano()))
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()

	err = p.Produce(ctx, []queue.Message{{Key: []byte("k"), Value: []byte("v")}})
	if err == nil {
		t.Fatal("producing to a topic that doesn't exist must fail, not silently create it")
	}
}

func createTopic(t *testing.T, ctx context.Context, cl *kgo.Client, name string) {
	t.Helper()
	req := kmsg.NewPtrCreateTopicsRequest()
	topic := kmsg.NewCreateTopicsRequestTopic()
	topic.Topic = name
	topic.NumPartitions = 3
	topic.ReplicationFactor = 1
	req.Topics = append(req.Topics, topic)
	resp, err := req.RequestWith(ctx, cl)
	if err != nil {
		t.Fatalf("create topic: %v", err)
	}
	if code := resp.Topics[0].ErrorCode; code != 0 {
		t.Fatalf("create topic: error code %d", code)
	}
}

func deleteTopic(cl *kgo.Client, name string) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	req := kmsg.NewPtrDeleteTopicsRequest()
	req.TopicNames = []string{name}
	t := kmsg.NewDeleteTopicsRequestTopic()
	t.Topic = &name
	req.Topics = append(req.Topics, t)
	_, _ = req.RequestWith(ctx, cl)
}
