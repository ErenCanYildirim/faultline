// Command migrate applies the ClickHouse schema and exits. It runs as a
// one-shot container before the worker starts.
//
// Configuration (environment variables):
//
//	CLICKHOUSE_URL        default http://localhost:8123
//	CLICKHOUSE_DB         default faultline
//	CLICKHOUSE_USER       default faultline
//	CLICKHOUSE_PASSWORD   default faultline
package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/erencanyildirim/faultline/internal/clickhouse"
	"github.com/erencanyildirim/faultline/migrations"
)

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	if err := run(log); err != nil {
		log.Error("migrate failed", "err", err)
		os.Exit(1)
	}
}

func run(log *slog.Logger) error {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	c, err := clickhouse.New(clickhouse.Config{
		URL:      envOr("CLICKHOUSE_URL", "http://localhost:8123"),
		Database: envOr("CLICKHOUSE_DB", "faultline"),
		User:     envOr("CLICKHOUSE_USER", "faultline"),
		Password: envOr("CLICKHOUSE_PASSWORD", "faultline"),
	})
	if err != nil {
		return err
	}

	list, err := clickhouse.LoadMigrations(migrations.ClickHouse, "clickhouse")
	if err != nil {
		return err
	}

	// Compose waits for the ClickHouse healthcheck, but retrying here keeps
	// the command usable on its own, e.g. against a server still starting.
	if err := waitFor(ctx, c, log); err != nil {
		return err
	}

	applied, err := clickhouse.Migrate(ctx, c, list)
	for _, name := range applied {
		log.Info("applied migration", "name", name)
	}
	if err != nil {
		return err
	}
	log.Info("schema up to date", "migrations", len(list), "applied_now", len(applied))
	return nil
}

func waitFor(ctx context.Context, c *clickhouse.Client, log *slog.Logger) error {
	for {
		pingCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
		err := c.Ping(pingCtx)
		cancel()
		if err == nil {
			return nil
		}
		log.Info("waiting for clickhouse", "err", err)
		select {
		case <-ctx.Done():
			return fmt.Errorf("clickhouse not reachable: %w", err)
		case <-time.After(time.Second):
		}
	}
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
