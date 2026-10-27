// Command worker consumes events from Redpanda and writes them to ClickHouse.
//
// Configuration (environment variables):
//
//	KAFKA_BROKERS          comma-separated brokers   default localhost:19092
//	KAFKA_TOPIC            events topic              default events.raw
//	KAFKA_DLQ_TOPIC        dead-letter topic         default events.dlq
//	KAFKA_GROUP            consumer group            default faultline-worker
//	CLICKHOUSE_URL         HTTP interface            default http://localhost:8123
//	CLICKHOUSE_DB          database                  default faultline
//	CLICKHOUSE_USER        user                      default faultline
//	CLICKHOUSE_PASSWORD    password                  default faultline
//	WORKER_BATCH_SIZE      events per insert         default 1000
//	WORKER_FLUSH_INTERVAL  max wait before insert    default 1s
//	LOG_LEVEL              debug, info, warn, error  default info
package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/erencanyildirim/faultline/internal/clickhouse"
	"github.com/erencanyildirim/faultline/internal/kafka"
	"github.com/erencanyildirim/faultline/internal/worker"
)

func main() {
	var level slog.Level
	_ = level.UnmarshalText([]byte(envOr("LOG_LEVEL", "info")))
	log := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: level}))

	if err := run(log); err != nil {
		log.Error("worker failed", "err", err)
		os.Exit(1)
	}
}

func run(log *slog.Logger) error {
	batchSize, err := strconv.Atoi(envOr("WORKER_BATCH_SIZE", strconv.Itoa(worker.DefaultBatchSize)))
	if err != nil || batchSize <= 0 {
		return fmt.Errorf("WORKER_BATCH_SIZE must be a positive integer")
	}
	flushInterval, err := time.ParseDuration(envOr("WORKER_FLUSH_INTERVAL", worker.DefaultFlushInterval.String()))
	if err != nil || flushInterval <= 0 {
		return fmt.Errorf("WORKER_FLUSH_INTERVAL must be a positive duration like 1s or 500ms")
	}

	cfg := kafka.ConsumerConfig{
		Brokers:  strings.Split(envOr("KAFKA_BROKERS", "localhost:19092"), ","),
		Topic:    envOr("KAFKA_TOPIC", "events.raw"),
		Group:    envOr("KAFKA_GROUP", "faultline-worker"),
		DLQTopic: envOr("KAFKA_DLQ_TOPIC", "events.dlq"),
	}
	consumer, err := kafka.NewConsumer(cfg)
	if err != nil {
		return err
	}
	defer consumer.Close()

	ch, err := clickhouse.New(clickhouse.Config{
		URL:      envOr("CLICKHOUSE_URL", "http://localhost:8123"),
		Database: envOr("CLICKHOUSE_DB", "faultline"),
		User:     envOr("CLICKHOUSE_USER", "faultline"),
		Password: envOr("CLICKHOUSE_PASSWORD", "faultline"),
	})
	if err != nil {
		return err
	}

	w := &worker.Worker{
		Consumer:      consumer,
		DLQ:           consumer,
		Store:         &clickhouse.EventStore{Client: ch},
		Log:           log,
		BatchSize:     batchSize,
		FlushInterval: flushInterval,
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	log.Info("worker started", "brokers", cfg.Brokers, "topic", cfg.Topic, "group", cfg.Group,
		"dlq", cfg.DLQTopic, "batch_size", batchSize, "flush_interval", flushInterval.String())

	// Log throughput every 30s, so `docker compose logs worker` shows progress
	// without a line per batch. Real metrics arrive with OpenTelemetry.
	go func() {
		t := time.NewTicker(30 * time.Second)
		defer t.Stop()
		var last worker.Stats
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				s := w.Stats()
				if s != last {
					log.Info("progress", "stored_total", s.Stored, "dead_lettered_total", s.DeadLettered,
						"stored_last_30s", s.Stored-last.Stored)
					last = s
				}
			}
		}
	}()

	err = w.Run(ctx)
	s := w.Stats()
	log.Info("worker stopped", "stored_total", s.Stored, "dead_lettered_total", s.DeadLettered)
	return err
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
