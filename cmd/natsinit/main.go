// Command natsinit creates the NATS JetStream streams and the worker's
// durable consumer, then exits: the NATS counterpart of the redpanda-init
// job. Safe to run on every start.
//
// Configuration (environment variables):
//
//	NATS_URL            server               default nats://localhost:4222
//	NATS_STREAM         events stream        default EVENTS
//	NATS_CONSUMER       worker consumer      default faultline-worker
//	NATS_ACK_WAIT       redelivery deadline  default 30s
package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/erencanyildirim/faultline/internal/natsjs"
)

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	if err := run(log); err != nil {
		log.Error("natsinit failed", "err", err)
		os.Exit(1)
	}
}

func run(log *slog.Logger) error {
	url := envOr("NATS_URL", natsjs.DefaultURL)
	ackWait, err := time.ParseDuration(envOr("NATS_ACK_WAIT", natsjs.DefaultAckWait.String()))
	if err != nil || ackWait <= 0 {
		return fmt.Errorf("NATS_ACK_WAIT must be a positive duration like 30s")
	}
	cfg := natsjs.SetupConfig{
		Stream:   envOr("NATS_STREAM", natsjs.DefaultStream),
		Consumer: envOr("NATS_CONSUMER", natsjs.DefaultConsumer),
		AckWait:  ackWait,
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := natsjs.Setup(ctx, url, cfg); err != nil {
		return err
	}
	log.Info("streams and consumer ready", "url", url, "stream", cfg.Stream, "consumer", cfg.Consumer,
		"ack_wait", ackWait.String())
	return nil
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
