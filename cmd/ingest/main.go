// Command ingest is the HTTP entry point for events. It authenticates each
// request by project key, validates the events, applies the project's rate
// limit and writes them to the queue (Redpanda or NATS JetStream), answering
// 202 only after the broker has acknowledged them.
//
// Configuration (environment variables):
//
//	INGEST_ADDR     listen address            default :8080
//	PROJECTS_FILE   project keys (JSON)       default /etc/faultline/projects.json
//	QUEUE           kafka or nats             default kafka
//	KAFKA_BROKERS   comma-separated brokers   default localhost:19092
//	KAFKA_TOPIC     topic for events          default events.raw
//	NATS_URL        NATS server               default nats://localhost:4222
//	NATS_STREAM     JetStream stream          default EVENTS
//	REDIS_ADDR      rate limit store          default localhost:6379
//	DEBUG_ADDR      pprof                     default :6060; empty disables
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

	"github.com/redis/go-redis/v9"

	"github.com/erencanyildirim/faultline/internal/debugsrv"
	"github.com/erencanyildirim/faultline/internal/ingest"
	"github.com/erencanyildirim/faultline/internal/kafka"
	"github.com/erencanyildirim/faultline/internal/natsjs"
	"github.com/erencanyildirim/faultline/internal/queue"
	"github.com/erencanyildirim/faultline/internal/ratelimit"
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

	producer, queueDesc, err := newProducer()
	if err != nil {
		return err
	}
	defer producer.Close()

	// Short timeouts: the rate limiter must never make ingest slow. When
	// Redis doesn't answer in time, FailOpen uses local limits instead.
	redisAddr := envOr("REDIS_ADDR", "localhost:6379")
	rdb := redis.NewClient(&redis.Options{
		Addr:         redisAddr,
		DialTimeout:  time.Second,
		ReadTimeout:  100 * time.Millisecond,
		WriteTimeout: 100 * time.Millisecond,
		MaxRetries:   -1, // FailOpen decides what happens on failure, not a retry loop
		// Without this, go-redis ignores context deadlines on socket reads
		// and FailOpen's 50ms timeout would not apply.
		ContextTimeoutEnabled: true,
	})
	defer rdb.Close()
	limiter := &ratelimit.FailOpen{
		Shared: &ratelimit.Redis{Client: rdb},
		Local:  ratelimit.NewLocal(),
		Log:    log,
	}

	h := &ingest.Handler{
		Projects:     projects,
		Producer:     producer,
		Limiter:      limiter,
		LimiterStats: limiter.Stats,
		Log:          log,
	}
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

	debugsrv.Start(ctx, debugAddr(), log, nil)

	// Log the per-project counters every 30s when they change, so rate
	// limiting shows up in `docker compose logs ingest` without polling
	// /debug/stats. OpenTelemetry metrics replace this in Phase 4.
	go func() {
		t := time.NewTicker(30 * time.Second)
		defer t.Stop()
		last := map[string]ingest.ProjectStats{}
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				for _, s := range h.Stats.Snapshot() {
					if s != last[s.Project] {
						log.Info("project stats", "project", s.Project, "accepted", s.Accepted,
							"rate_limited", s.RateLimited, "invalid", s.Invalid, "queue_errors", s.QueueErrors)
						last[s.Project] = s
					}
				}
			}
		}
	}()

	errc := make(chan error, 1)
	go func() {
		log.Info("ingest listening", "addr", addr, "projects_keys", projects.Len(), "queue", queueDesc,
			"redis", redisAddr)
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

// newProducer picks the broker from QUEUE. Nothing else in ingest knows
// which one it is.
func newProducer() (queue.Producer, string, error) {
	switch q := envOr("QUEUE", "kafka"); q {
	case "kafka":
		brokers := strings.Split(envOr("KAFKA_BROKERS", "localhost:19092"), ",")
		topic := envOr("KAFKA_TOPIC", "events.raw")
		p, err := kafka.NewProducer(brokers, topic)
		return p, fmt.Sprintf("kafka %s topic %s", strings.Join(brokers, ","), topic), err
	case "nats":
		cfg := natsjs.ProducerConfig{URL: envOr("NATS_URL", natsjs.DefaultURL), Stream: envOr("NATS_STREAM", natsjs.DefaultStream)}
		p, err := natsjs.NewProducer(cfg)
		return p, fmt.Sprintf("nats %s stream %s", cfg.URL, cfg.Stream), err
	default:
		return nil, "", fmt.Errorf("QUEUE must be kafka or nats, got %q", q)
	}
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

// debugAddr reads DEBUG_ADDR; set but empty disables the debug server.
func debugAddr() string {
	if v, ok := os.LookupEnv("DEBUG_ADDR"); ok {
		return v
	}
	return ":6060"
}
