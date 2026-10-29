package kafka

import (
	"context"
	"errors"
	"fmt"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// LagReader measures how far a consumer group is behind on one topic.
//
// Lag per partition = high watermark (offset of the next message to be
// written) - committed offset of the group (offset of the next message it
// will process). The worker commits only after ClickHouse confirms an
// insert, so this is the number of events in Redpanda that are not stored
// yet.
type LagReader struct {
	client *kgo.Client
	topic  string
	group  string
}

// PartitionLag is one partition's numbers.
type PartitionLag struct {
	Partition     int32 `json:"partition"`
	HighWatermark int64 `json:"high_watermark"`
	// Committed is -1 when the group has never committed on this partition.
	// Lag then counts every message from offset 0, which matches the
	// worker's behaviour: a new group starts at the oldest message.
	Committed int64 `json:"committed"`
	Lag       int64 `json:"lag"`
}

// Lag is a snapshot for the whole topic.
type Lag struct {
	Total      int64          `json:"total"`
	Partitions []PartitionLag `json:"partitions"`
}

// NewLagReader creates a reader. It does not join the group, so it never
// triggers a rebalance of the real consumers.
func NewLagReader(brokers []string, topic, group string) (*LagReader, error) {
	if len(brokers) == 0 || topic == "" || group == "" {
		return nil, errors.New("kafka: lag reader needs brokers, topic and group")
	}
	cl, err := kgo.NewClient(kgo.SeedBrokers(brokers...))
	if err != nil {
		return nil, fmt.Errorf("kafka: creating lag reader: %w", err)
	}
	return &LagReader{client: cl, topic: topic, group: group}, nil
}

// Close releases the connection.
func (r *LagReader) Close() { r.client.Close() }

// Read returns the current lag.
func (r *LagReader) Read(ctx context.Context) (Lag, error) {
	partitions, err := r.partitions(ctx)
	if err != nil {
		return Lag{}, err
	}
	high, err := r.highWatermarks(ctx, partitions)
	if err != nil {
		return Lag{}, err
	}
	committed, err := r.committed(ctx, partitions)
	if err != nil {
		return Lag{}, err
	}

	out := Lag{Partitions: make([]PartitionLag, 0, len(partitions))}
	for _, p := range partitions {
		pl := PartitionLag{Partition: p, HighWatermark: high[p], Committed: -1}
		if c, ok := committed[p]; ok {
			pl.Committed = c
		}
		pl.Lag = pl.HighWatermark - max(pl.Committed, 0)
		if pl.Lag < 0 {
			pl.Lag = 0 // committed read after the watermark: a moment of skew, not negative lag
		}
		out.Total += pl.Lag
		out.Partitions = append(out.Partitions, pl)
	}
	return out, nil
}

func (r *LagReader) partitions(ctx context.Context) ([]int32, error) {
	req := kmsg.NewPtrMetadataRequest()
	t := kmsg.NewMetadataRequestTopic()
	t.Topic = kmsg.StringPtr(r.topic)
	req.Topics = append(req.Topics, t)
	resp, err := req.RequestWith(ctx, r.client)
	if err != nil {
		return nil, fmt.Errorf("kafka: metadata: %w", err)
	}
	if len(resp.Topics) != 1 {
		return nil, fmt.Errorf("kafka: metadata returned %d topics", len(resp.Topics))
	}
	if err := kerr.ErrorForCode(resp.Topics[0].ErrorCode); err != nil {
		return nil, fmt.Errorf("kafka: metadata for %s: %w", r.topic, err)
	}
	ps := make([]int32, 0, len(resp.Topics[0].Partitions))
	for _, p := range resp.Topics[0].Partitions {
		ps = append(ps, p.Partition)
	}
	return ps, nil
}

func (r *LagReader) highWatermarks(ctx context.Context, partitions []int32) (map[int32]int64, error) {
	req := kmsg.NewPtrListOffsetsRequest()
	req.ReplicaID = -1
	t := kmsg.NewListOffsetsRequestTopic()
	t.Topic = r.topic
	for _, p := range partitions {
		rp := kmsg.NewListOffsetsRequestTopicPartition()
		rp.Partition = p
		rp.Timestamp = -1 // latest: the high watermark
		t.Partitions = append(t.Partitions, rp)
	}
	req.Topics = append(req.Topics, t)
	// kgo sends each partition to its leader and merges the answers.
	resp, err := req.RequestWith(ctx, r.client)
	if err != nil {
		return nil, fmt.Errorf("kafka: list offsets: %w", err)
	}
	out := make(map[int32]int64, len(partitions))
	for _, rt := range resp.Topics {
		for _, rp := range rt.Partitions {
			if err := kerr.ErrorForCode(rp.ErrorCode); err != nil {
				return nil, fmt.Errorf("kafka: list offsets for partition %d: %w", rp.Partition, err)
			}
			out[rp.Partition] = rp.Offset
		}
	}
	return out, nil
}

func (r *LagReader) committed(ctx context.Context, partitions []int32) (map[int32]int64, error) {
	req := kmsg.NewPtrOffsetFetchRequest()
	// Filled both ways: the old fields for brokers that speak OffsetFetch
	// below v8, Groups for v8 and later. kmsg encodes whichever the
	// negotiated version uses.
	req.Group = r.group
	t := kmsg.NewOffsetFetchRequestTopic()
	t.Topic = r.topic
	t.Partitions = partitions
	req.Topics = append(req.Topics, t)
	g := kmsg.NewOffsetFetchRequestGroup()
	g.Group = r.group
	gt := kmsg.NewOffsetFetchRequestGroupTopic()
	gt.Topic = r.topic
	gt.Partitions = partitions
	g.Topics = append(g.Topics, gt)
	req.Groups = append(req.Groups, g)

	resp, err := req.RequestWith(ctx, r.client)
	if err != nil {
		return nil, fmt.Errorf("kafka: offset fetch: %w", err)
	}
	out := make(map[int32]int64, len(partitions))
	add := func(p int32, offset int64, code int16) error {
		if err := kerr.ErrorForCode(code); err != nil {
			return fmt.Errorf("kafka: offset fetch for partition %d: %w", p, err)
		}
		if offset >= 0 {
			out[p] = offset
		}
		return nil
	}
	if len(resp.Groups) > 0 {
		for _, g := range resp.Groups {
			if err := groupError(g.ErrorCode); err != nil {
				return nil, fmt.Errorf("kafka: offset fetch for group %s: %w", r.group, err)
			}
			for _, t := range g.Topics {
				for _, p := range t.Partitions {
					if err := add(p.Partition, p.Offset, p.ErrorCode); err != nil {
						return nil, err
					}
				}
			}
		}
		return out, nil
	}
	if err := groupError(resp.ErrorCode); err != nil {
		return nil, fmt.Errorf("kafka: offset fetch for group %s: %w", r.group, err)
	}
	for _, t := range resp.Topics {
		for _, p := range t.Partitions {
			if err := add(p.Partition, p.Offset, p.ErrorCode); err != nil {
				return nil, err
			}
		}
	}
	return out, nil
}

// groupError treats a group that doesn't exist yet as "nothing committed":
// before the first worker starts, everything in the topic is lag. Brokers
// differ here; some answer with offsets of -1, others with this error.
func groupError(code int16) error {
	err := kerr.ErrorForCode(code)
	if errors.Is(err, kerr.GroupIDNotFound) {
		return nil
	}
	return err
}
