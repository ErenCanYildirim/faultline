// Command ingest is the HTTP entry point for events. It authenticates each
// request by project key, validates the events and writes them to Redpanda,
// answering 202 only after the broker has acknowledged them.
//
// Configuration (environment variables):
//
//	INGEST_ADDR     listen address            default :8080
//	PROJECTS_FILE   project keys (JSON)       default /etc/faultline/projects.json
//	KAFKA_BROKERS   comma-separated brokers   default localhost:19092
//	KAFKA_TOPIC     topic for events          default events.raw
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/erencanyildirim/faultline/internal/ingest"
	"github.com/erencanyildirim/faultline/internal/kafka"
)

func main() {
	healthcheck := flag.Bool("healthcheck", false, "probe the local /healthz endpoint and exit 0 or 1 (used by the container healthcheck)")
	flag.Parse()

	addr := envOr("INGEST_ADDR", ":8080")

	// The runtime image has no shell, curl or wget, so the binary checks itself.
	if *healthcheck {
		os.Exit(probe(addr))
	}

	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	if err := run(log, addr); err != nil {
		log.Error("ingest failed", "err", err)
		os.Exit(1)
	}
}

func run(log *slog.Logger, addr string) error {
	projectsPath := envOr("PROJECTS_FILE", "/etc/faultline/projects.json")
	projects, err := ingest.LoadProjects(projectsPath)
	if err != nil {
		return err
	}

	brokers := strings.Split(envOr("KAFKA_BROKERS", "localhost:19092"), ",")
	topic := envOr("KAFKA_TOPIC", "events.raw")
	producer, err := kafka.NewProducer(brokers, topic)
	if err != nil {
		return err
	}
	defer producer.Close()

	h := &ingest.Handler{Projects: projects, Producer: producer, Log: log}
	srv := &http.Server{
		Addr:              addr,
		Handler:           h.Routes(),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       15 * time.Second,
		// Must exceed the produce timeout, or slow acks become broken responses.
		WriteTimeout: ingest.DefaultProduceTimeout + 5*time.Second,
		IdleTimeout:  60 * time.Second,
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	errc := make(chan error, 1)
	go func() {
		log.Info("ingest listening", "addr", addr, "projects_keys", projects.Len(), "brokers", brokers, "topic", topic)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errc <- err
		}
	}()

	select {
	case err := <-errc:
		return err
	case <-ctx.Done():
	}

	// Stop taking new requests, let in-flight ones finish (they may be
	// waiting for broker acks), then close the producer.
	log.Info("shutting down")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), ingest.DefaultProduceTimeout+5*time.Second)
	defer cancel()
	return srv.Shutdown(shutdownCtx)
}

func probe(addr string) int {
	host := addr
	if strings.HasPrefix(host, ":") {
		host = "127.0.0.1" + host
	}
	client := http.Client{Timeout: 2 * time.Second}
	resp, err := client.Get(fmt.Sprintf("http://%s/healthz", host))
	if err != nil {
		return 1
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return 1
	}
	return 0
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
