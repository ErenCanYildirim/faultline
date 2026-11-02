//go:build integration

// Runs against a real NATS server with JetStream. Start the stack on NATS
// first (docker-compose.nats.yml), then:
//
//	go test -tags integration ./internal/natsjs/
//
// Uses NATS_URL (default nats://localhost:4222). Each test creates its own
// streams and subjects and deletes them at the end, so it never touches
// EVENTS or EVENTS_DLQ.
package natsjs

import (
	"context"
	"fmt"
	"math/rand/v2"
	"os"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/erencanyildirim/faultline/internal/queue"
)

func natsURL() string {
	if v := os.Getenv("NATS_URL"); v != "" {
		return v
	}
	return DefaultURL
}

type env struct {
	setup SetupConfig
	js    jetstream.JetStream
}

// newEnv creates streams with unique names and subjects for one test.
func newEnv(t *testing.T, ackWait time.Duration) *env {
	t.Helper()
	id := fmt.Sprintf("T%d", rand.Int64N(1<<40))
	cfg := SetupConfig{
		Stream: id, Subject: "test." + id + ".raw",
		DLQStream: id + "_DLQ", DLQSubject: "test." + id + ".dlq",
		Consumer: "worker", AckWait: ackWait,
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := Setup(ctx, natsURL(), cfg); err != nil {
		t.Fatalf("setup (is NATS running? %s): %v", natsURL(), err)
	}
	// Running it again changes nothing and must not fail.
	if err := Setup(ctx, natsURL(), cfg); err != nil {
		t.Fatalf("second setup: %v", err)
	}
	nc, err := nats.Connect(natsURL())
	if err != nil {
		t.Fatal(err)
	}
	js, _ := jetstream.New(nc)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = js.DeleteStream(ctx, cfg.Stream)
		_ = js.DeleteStream(ctx, cfg.DLQStream)
		nc.Close()
	})
	return &env{setup: cfg, js: js}
}

func (e *env) producer(t *testing.T) *Producer {
	t.Helper()
	p, err := NewProducer(ProducerConfig{URL: natsURL(), Stream: e.setup.Stream, Subject: e.setup.Subject})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(p.Close)
	return p
}

func (e *env) consumer(t *testing.T) *Consumer {
	t.Helper()
	c, err := NewConsumer(ConsumerConfig{URL: natsURL(), Stream: e.setup.Stream, Consumer: e.setup.Consumer,
		DLQSubject: e.setup.DLQSubject})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(c.Close)
	return c
}

func (e *env) lag(t *testing.T) Lag {
	t.Helper()
	lr, err := NewLagReader(natsURL(), e.setup.Stream, e.setup.Consumer)
	if err != nil {
		t.Fatal(err)
	}
	defer lr.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	l, err := lr.Read(ctx)
	if err != nil {
		t.Fatal(err)
	}
	return l
}

func messages(from, n int) []queue.Message {
	out := make([]queue.Message, n)
	for i := range out {
		id := fmt.Sprintf("event-%04d", from+i)
		out[i] = queue.Message{Key: []byte(id), Value: []byte(`{"id":"` + id + `"}`), ProjectID: "demo"}
	}
	return out
}

// pollAll polls until n deliveries arrived or the time is up.
func pollAll(t *testing.T, c *Consumer, n int, within time.Duration) []queue.Delivery {
	t.Helper()
	var got []queue.Delivery
	deadline := time.Now().Add(within)
	for len(got) < n && time.Now().Before(deadline) {
		ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
		ds, err := c.Poll(ctx, n-len(got))
		cancel()
		if err != nil {
			t.Fatal(err)
		}
		got = append(got, ds...)
	}
	return got
}

func TestProduceConsumeCommit(t *testing.T) {
	e := newEnv(t, 5*time.Second)
	p := e.producer(t)
	ctx := context.Background()
	if err := p.Ping(ctx); err != nil {
		t.Fatalf("ping: %v", err)
	}
	if err := p.Produce(ctx, messages(0, 100)); err != nil {
		t.Fatal(err)
	}
	if l := e.lag(t); l.Total != 100 || l.Pending != 100 {
		t.Errorf("lag before consuming = %+v, want 100 pending", l)
	}

	c := e.consumer(t)
	got := pollAll(t, c, 100, 5*time.Second)
	if len(got) != 100 {
		t.Fatalf("polled %d, want 100", len(got))
	}
	d := got[0]
	if d.ProjectID != "demo" || d.Topic != e.setup.Subject || d.Offset < 1 || len(d.Key) == 0 {
		t.Errorf("delivery = %+v", d)
	}
	if err := c.Commit(ctx, got); err != nil {
		t.Fatal(err)
	}
	if l := e.lag(t); l.Total != 0 {
		t.Errorf("lag after commit = %+v, want 0", l)
	}
}

// The event ID is the message ID: publishing the same events again (an SDK
// retrying a request that did succeed) stores nothing new.
func TestRepeatedEventIDsAreDropped(t *testing.T) {
	e := newEnv(t, 5*time.Second)
	p := e.producer(t)
	ctx := context.Background()
	if err := p.Produce(ctx, messages(0, 50)); err != nil {
		t.Fatal(err)
	}
	if err := p.Produce(ctx, messages(25, 50)); err != nil { // 25 repeats, 25 new
		t.Fatal(err)
	}
	s, err := e.js.Stream(ctx, e.setup.Stream)
	if err != nil {
		t.Fatal(err)
	}
	info, _ := s.Info(ctx)
	if info.State.Msgs != 75 {
		t.Errorf("stream holds %d messages, want 75", info.State.Msgs)
	}
}

// What is polled but never committed comes back after AckWait: a worker
// that dies loses nothing.
func TestUncommittedIsRedelivered(t *testing.T) {
	e := newEnv(t, time.Second)
	if err := e.producer(t).Produce(context.Background(), messages(0, 10)); err != nil {
		t.Fatal(err)
	}
	first := e.consumer(t)
	if got := pollAll(t, first, 10, 5*time.Second); len(got) != 10 {
		t.Fatalf("polled %d, want 10", len(got))
	}
	// The first worker "dies" holding all ten. A second one gets them
	// after AckWait.
	second := e.consumer(t)
	start := time.Now()
	got := pollAll(t, second, 10, 5*time.Second)
	if len(got) != 10 {
		t.Fatalf("redelivered %d, want 10", len(got))
	}
	if waited := time.Since(start); waited < 500*time.Millisecond {
		t.Errorf("redelivered after %v, before AckWait", waited)
	}
	if err := second.Commit(context.Background(), got); err != nil {
		t.Fatal(err)
	}
}

// KeepAlive holds a batch past AckWait: nobody else gets it meanwhile.
func TestKeepAlivePreventsRedelivery(t *testing.T) {
	e := newEnv(t, time.Second)
	if err := e.producer(t).Produce(context.Background(), messages(0, 10)); err != nil {
		t.Fatal(err)
	}
	busy := e.consumer(t)
	batch := pollAll(t, busy, 10, 5*time.Second)
	if len(batch) != 10 {
		t.Fatalf("polled %d", len(batch))
	}
	stop := busy.KeepAlive(batch)

	other := e.consumer(t)
	if got := pollAll(t, other, 10, 3*time.Second); len(got) != 0 {
		t.Errorf("another worker got %d messages of a batch kept alive", len(got))
	}
	stop()
	if err := busy.Commit(context.Background(), batch); err != nil {
		t.Fatal(err)
	}
	if l := e.lag(t); l.Total != 0 {
		t.Errorf("lag = %+v", l)
	}
}

// Two workers on one consumer split the messages between them, with no
// partitions involved and no message delivered to both.
func TestWorkersShareTheConsumer(t *testing.T) {
	e := newEnv(t, 5*time.Second)
	if err := e.producer(t).Produce(context.Background(), messages(0, 400)); err != nil {
		t.Fatal(err)
	}
	a, b := e.consumer(t), e.consumer(t)
	seen := map[string]int{}
	var fromA, fromB int
	deadline := time.Now().Add(10 * time.Second)
	for len(seen) < 400 && time.Now().Before(deadline) {
		for i, c := range []*Consumer{a, b} {
			ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
			ds, err := c.Poll(ctx, 20)
			cancel()
			if err != nil {
				t.Fatal(err)
			}
			for _, d := range ds {
				seen[string(d.Key)]++
			}
			if i == 0 {
				fromA += len(ds)
			} else {
				fromB += len(ds)
			}
			if err := c.Commit(context.Background(), ds); err != nil {
				t.Fatal(err)
			}
		}
	}
	if len(seen) != 400 {
		t.Fatalf("saw %d distinct messages, want 400", len(seen))
	}
	for k, n := range seen {
		if n != 1 {
			t.Errorf("%s delivered %d times", k, n)
		}
	}
	if fromA == 0 || fromB == 0 {
		t.Errorf("one worker got nothing: a=%d b=%d", fromA, fromB)
	}
}

// Poll returns as soon as a message is there, like Kafka's poll, not when
// the wait is over: a batch starts filling at once.
func TestPollReturnsWhenSomethingArrives(t *testing.T) {
	e := newEnv(t, 5*time.Second)
	c := e.consumer(t)
	p := e.producer(t)
	go func() {
		time.Sleep(300 * time.Millisecond)
		_ = p.Produce(context.Background(), messages(0, 20))
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	start := time.Now()
	ds, err := c.Poll(ctx, 100)
	if err != nil {
		t.Fatal(err)
	}
	if took := time.Since(start); len(ds) == 0 || took > 2*time.Second {
		t.Fatalf("Poll returned %d messages after %v", len(ds), took)
	}
	// Whatever wasn't in the first response comes with the next Poll.
	got := append(ds, pollAll(t, c, 20-len(ds), 3*time.Second)...)
	if len(got) != 20 {
		t.Errorf("got %d of 20", len(got))
	}
}

// A closed consumer says so, which ends the worker loop.
func TestClosedConsumer(t *testing.T) {
	e := newEnv(t, 5*time.Second)
	c := e.consumer(t)
	c.Close()
	if _, err := c.Poll(context.Background(), 10); err != queue.ErrClosed {
		t.Errorf("Poll after Close = %v, want ErrClosed", err)
	}
}

func TestDeadLettersKeepHeaders(t *testing.T) {
	e := newEnv(t, 5*time.Second)
	if err := e.producer(t).Produce(context.Background(), messages(0, 1)); err != nil {
		t.Fatal(err)
	}
	c := e.consumer(t)
	ds := pollAll(t, c, 1, 5*time.Second)
	if len(ds) != 1 {
		t.Fatal("no message")
	}
	ctx := context.Background()
	if err := c.WriteDeadLetters(ctx, []queue.DeadLetter{{Delivery: ds[0], Reason: "not a valid event"}}); err != nil {
		t.Fatal(err)
	}
	s, _ := e.js.Stream(ctx, e.setup.DLQStream)
	m, err := s.GetLastMsgForSubject(ctx, e.setup.DLQSubject)
	if err != nil {
		t.Fatal(err)
	}
	h := m.Header
	if h.Get(DLQReasonHeader) != "not a valid event" || h.Get(ProjectHeader) != "demo" ||
		h.Get(DLQSubjectHeader) != e.setup.Subject || h.Get(DLQSequenceHeader) != "1" || string(m.Data) != string(ds[0].Value) {
		t.Errorf("dead letter = %v %s", h, m.Data)
	}
}

func TestPingFailsWithoutStream(t *testing.T) {
	p, err := NewProducer(ProducerConfig{URL: natsURL(), Stream: "NO_SUCH_STREAM", Subject: "nothing.here"})
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	time.Sleep(100 * time.Millisecond)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := p.Ping(ctx); err == nil {
		t.Error("ping succeeded without a stream")
	}
}
