package faultline

import (
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"runtime/debug"
	"strings"
	"time"

	"github.com/erencanyildirim/faultline/event"
)

// SDK identity sent with every event.
const (
	SDKName    = "faultline-go"
	SDKVersion = "0.1.0"
)

// Defaults for Options fields left at their zero value.
const (
	DefaultBufferSize          = 1000
	DefaultBufferBytes         = 8 << 20 // 8 MiB
	DefaultBatchSize           = 50
	DefaultFlushInterval       = time.Second
	DefaultHTTPTimeout         = 10 * time.Second
	DefaultMaxRetryDuration    = time.Minute
	DefaultRetryInitialBackoff = 500 * time.Millisecond
	DefaultRetryMaxBackoff     = 30 * time.Second
	DefaultBreakerThreshold    = 5
	DefaultBreakerCooldown     = 30 * time.Second
)

// Options configures a Client. Only DSN is required.
type Options struct {
	// DSN says where to send events and which project they belong to:
	//
	//	http://<project-key>@host:port[/path]
	//
	// The key goes where a username would. Use https outside local development.
	DSN string

	// Release and Environment are attached to every event.
	Release     string
	Environment string

	// BufferSize is how many events can wait in memory. When full, new
	// events are dropped. Default 1000.
	BufferSize int

	// BufferBytes caps the estimated memory of all events the SDK holds:
	// buffered, batched, or being retried. Events vary from a few hundred
	// bytes to 64 KiB, so a count alone doesn't bound memory. Whichever limit
	// is hit first drops new events. Default 8 MiB.
	BufferBytes int64

	// BatchSize is the maximum number of events per request. Default 50,
	// capped at event.MaxBatchEvents.
	BatchSize int

	// FlushInterval is the longest an event waits before its batch is sent.
	// Default 1s.
	FlushInterval time.Duration

	// HTTPClient sends the batches. Default: a client with a 10s timeout.
	HTTPClient *http.Client

	// InAppPrefixes are package path prefixes that count as the
	// application's own code. Default: the main module's path. Package
	// "main" always counts. Grouping relies on this flag.
	InAppPrefixes []string

	// Logger receives the SDK's own diagnostics. Default: discarded, so the
	// SDK never writes to the application's output unless asked to.
	Logger *slog.Logger

	// SampleRate is the fraction of errors and messages to keep, from 0
	// (exclusive) to 1. 0 means the default, 1: keep everything. Panics
	// captured by Recover are never sampled out.
	SampleRate float64

	// BeforeSend is called with every event before it is buffered. It can
	// change the event, or return nil to drop it. It runs on the goroutine
	// that captured the event, so it must be fast. If it panics, the event
	// is dropped: this hook is often where personal data gets removed, and
	// sending the unmodified event would defeat that.
	BeforeSend func(*Event) *Event

	// MaxRetryDuration is how long one batch is retried before it counts
	// as failed. Default 1 minute.
	MaxRetryDuration time.Duration

	// RetryInitialBackoff and RetryMaxBackoff shape the exponential backoff
	// between attempts. Defaults 500ms and 30s.
	RetryInitialBackoff time.Duration
	RetryMaxBackoff     time.Duration

	// After BreakerThreshold consecutive failed requests the SDK stops
	// sending for BreakerCooldown, then lets one request through to probe.
	// Defaults 5 and 30s.
	BreakerThreshold int
	BreakerCooldown  time.Duration
}

// target is a parsed DSN.
type target struct {
	endpoint string
	key      string
}

func parseDSN(raw string) (target, error) {
	if raw == "" {
		return target{}, errors.New("faultline: DSN is empty")
	}
	u, err := url.Parse(raw)
	if err != nil {
		return target{}, fmt.Errorf("faultline: invalid DSN: %w", err)
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return target{}, fmt.Errorf("faultline: DSN scheme must be http or https, got %q", u.Scheme)
	}
	if u.Host == "" {
		return target{}, errors.New("faultline: DSN has no host")
	}
	if u.User == nil || u.User.Username() == "" {
		return target{}, errors.New("faultline: DSN has no project key; expected http://<key>@host")
	}
	if _, hasPassword := u.User.Password(); hasPassword {
		return target{}, errors.New("faultline: DSN must not contain a password; put only the project key before the @")
	}
	path := strings.TrimRight(u.Path, "/")
	return target{
		endpoint: fmt.Sprintf("%s://%s%s%s", u.Scheme, u.Host, path, event.EventsPath),
		key:      u.User.Username(),
	}, nil
}

// withDefaults returns a copy of o with zero values replaced by defaults.
func (o Options) withDefaults() Options {
	if o.BufferSize <= 0 {
		o.BufferSize = DefaultBufferSize
	}
	if o.BatchSize <= 0 {
		o.BatchSize = DefaultBatchSize
	}
	if o.BatchSize > event.MaxBatchEvents {
		o.BatchSize = event.MaxBatchEvents
	}
	if o.FlushInterval <= 0 {
		o.FlushInterval = DefaultFlushInterval
	}
	if o.HTTPClient == nil {
		o.HTTPClient = &http.Client{Timeout: DefaultHTTPTimeout}
	}
	if len(o.InAppPrefixes) == 0 {
		if info, ok := debug.ReadBuildInfo(); ok && info.Main.Path != "" {
			o.InAppPrefixes = []string{info.Main.Path}
		}
	}
	if o.Logger == nil {
		o.Logger = slog.New(slog.DiscardHandler)
	}
	if o.BufferBytes <= 0 {
		o.BufferBytes = DefaultBufferBytes
	}
	if o.SampleRate == 0 {
		o.SampleRate = 1
	}
	if o.MaxRetryDuration <= 0 {
		o.MaxRetryDuration = DefaultMaxRetryDuration
	}
	if o.RetryInitialBackoff <= 0 {
		o.RetryInitialBackoff = DefaultRetryInitialBackoff
	}
	if o.RetryMaxBackoff <= 0 {
		o.RetryMaxBackoff = DefaultRetryMaxBackoff
	}
	if o.RetryMaxBackoff < o.RetryInitialBackoff {
		o.RetryMaxBackoff = o.RetryInitialBackoff
	}
	if o.BreakerThreshold <= 0 {
		o.BreakerThreshold = DefaultBreakerThreshold
	}
	if o.BreakerCooldown <= 0 {
		o.BreakerCooldown = DefaultBreakerCooldown
	}
	return o
}

func (o Options) validate() error {
	if o.SampleRate < 0 || o.SampleRate > 1 {
		return fmt.Errorf("faultline: SampleRate must be between 0 and 1, got %v", o.SampleRate)
	}
	return nil
}
