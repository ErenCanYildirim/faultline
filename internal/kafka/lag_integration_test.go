//go:build integration

package kafka

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

// Produce 30 records over 3 partitions, commit part of them from a consumer
// group, and check the lag reader's numbers at each step.
func TestLagReaderAgainstRedpanda(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	admin, err := kgo.NewClient(kgo.SeedBrokers(brokers()...))
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close()
	topic := fmt.Sprintf("faultline-test-lag-%d", time.Now().UnixNano())
	group := topic + "-group"
	createTopic(t, ctx, admin, topic)
	defer deleteTopic(admin, topic)

	r, err := NewLagReader(brokers(), topic, group)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	// A group that has never committed: no error, no lag on an empty topic.
	l, err := r.Read(ctx)
	if err != nil {
		t.Fatalf("group that doesn't exist yet: %v", err)
	}
	if l.Total != 0 || len(l.Partitions) != 3 {
		t.Errorf("empty topic: %+v", l)
	}

	p, err := kgo.NewClient(kgo.SeedBrokers(brokers()...), kgo.DefaultProduceTopic(topic),
		kgo.RecordPartitioner(kgo.RoundRobinPartitioner()))
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	for i := range 30 {
		if err := p.ProduceSync(ctx, &kgo.Record{Value: []byte(fmt.Sprint(i))}).FirstErr(); err != nil {
			t.Fatal(err)
		}
	}
	if l, err = r.Read(ctx); err != nil || l.Total != 30 {
		t.Fatalf("30 produced, nothing committed: %+v, %v; want lag 30", l, err)
	}

	cons, err := kgo.NewClient(kgo.SeedBrokers(brokers()...), kgo.ConsumerGroup(group), kgo.ConsumeTopics(topic),
		kgo.DisableAutoCommit(), kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))
	if err != nil {
		t.Fatal(err)
	}
	defer cons.Close()
	var got []*kgo.Record
	for len(got) < 30 && ctx.Err() == nil {
		got = append(got, cons.PollFetches(ctx).Records()...)
	}

	// Commit only the first record of each partition: lag 9 per partition.
	seen := map[int32]bool{}
	var first []*kgo.Record
	for _, rec := range got {
		if !seen[rec.Partition] {
			seen[rec.Partition] = true
			first = append(first, rec)
		}
	}
	if err := cons.CommitRecords(ctx, first...); err != nil {
		t.Fatal(err)
	}
	if l, err = r.Read(ctx); err != nil || l.Total != 27 {
		t.Fatalf("one record committed per partition: %+v, %v; want lag 27", l, err)
	}

	if err := cons.CommitRecords(ctx, got...); err != nil {
		t.Fatal(err)
	}
	if l, err = r.Read(ctx); err != nil || l.Total != 0 {
		t.Fatalf("everything committed: %+v, %v; want lag 0", l, err)
	}
}
