// Package clickhouse talks to ClickHouse over its HTTP interface.
//
// HTTP instead of the native protocol keeps the project free of extra
// dependencies and makes every request reproducible with curl. The cost is
// throughput: JSON rows over HTTP are slower to encode and parse than the
// native binary format. Measuring that difference is a Phase 3 experiment.
package clickhouse

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// Config locates a ClickHouse server.
type Config struct {
	URL      string // e.g. http://clickhouse:8123
	Database string
	User     string
	Password string
	// HTTPClient defaults to one with a 30s timeout.
	HTTPClient *http.Client
}

// Client runs queries. It is safe for concurrent use.
type Client struct {
	base *url.URL
	cfg  Config
	http *http.Client
}

// Error is an exception reported by the server, as opposed to a network
// failure. Callers use it to tell "ClickHouse said no" from "ClickHouse is
// unreachable".
type Error struct {
	StatusCode int
	Message    string
}

func (e *Error) Error() string {
	return fmt.Sprintf("clickhouse: HTTP %d: %s", e.StatusCode, e.Message)
}

// New validates the configuration. It does not connect.
func New(cfg Config) (*Client, error) {
	u, err := url.Parse(cfg.URL)
	if err != nil || u.Scheme == "" || u.Host == "" {
		return nil, fmt.Errorf("clickhouse: invalid URL %q", cfg.URL)
	}
	if cfg.HTTPClient == nil {
		cfg.HTTPClient = &http.Client{Timeout: 30 * time.Second}
	}
	return &Client{base: u, cfg: cfg, http: cfg.HTTPClient}, nil
}

// Ping checks that the server answers.
func (c *Client) Ping(ctx context.Context) error {
	u := *c.base
	u.Path = "/ping"
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return err
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return fmt.Errorf("clickhouse: ping: %w", err)
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, resp.Body)
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("clickhouse: ping: HTTP %d", resp.StatusCode)
	}
	return nil
}

// Exec runs a statement that returns no rows (DDL, INSERT ... SELECT).
func (c *Client) Exec(ctx context.Context, query string) error {
	_, err := c.do(ctx, nil, strings.NewReader(query))
	return err
}

// Query runs a SELECT and returns the raw response body. Add a FORMAT
// clause to the query to choose the output format.
func (c *Client) Query(ctx context.Context, query string) ([]byte, error) {
	return c.do(ctx, nil, strings.NewReader(query))
}

// InsertJSONEachRow inserts rows, one JSON object per line, into table.
// One call is one INSERT, which ClickHouse writes as one part: callers
// should batch thousands of rows per call, not insert one by one.
func (c *Client) InsertJSONEachRow(ctx context.Context, table string, rows []byte) error {
	params := url.Values{"query": {"INSERT INTO " + table + " FORMAT JSONEachRow"}}
	_, err := c.do(ctx, params, bytes.NewReader(rows))
	return err
}

func (c *Client) do(ctx context.Context, params url.Values, body io.Reader) ([]byte, error) {
	u := *c.base
	q := u.Query()
	for k, vs := range params {
		for _, v := range vs {
			q.Add(k, v)
		}
	}
	if c.cfg.Database != "" {
		q.Set("database", c.cfg.Database)
	}
	// Report errors in the HTTP status even if they happen after the
	// server started sending the response.
	q.Set("wait_end_of_query", "1")
	u.RawQuery = q.Encode()

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, u.String(), body)
	if err != nil {
		return nil, err
	}
	// Credentials in headers, not in the URL, so they stay out of logs.
	if c.cfg.User != "" {
		req.Header.Set("X-ClickHouse-User", c.cfg.User)
		req.Header.Set("X-ClickHouse-Key", c.cfg.Password)
	}

	resp, err := c.http.Do(req)
	if err != nil {
		return nil, fmt.Errorf("clickhouse: %w", err)
	}
	defer resp.Body.Close()
	out, readErr := io.ReadAll(io.LimitReader(resp.Body, 64<<20))
	if resp.StatusCode != http.StatusOK {
		msg := strings.TrimSpace(string(out))
		if len(msg) > 1000 {
			msg = msg[:1000] + "..."
		}
		return nil, &Error{StatusCode: resp.StatusCode, Message: msg}
	}
	if readErr != nil {
		return nil, fmt.Errorf("clickhouse: reading response: %w", readErr)
	}
	return out, nil
}

// IsServerError reports whether err is an exception returned by ClickHouse
// rather than a network or context error.
func IsServerError(err error) bool {
	var e *Error
	return errors.As(err, &e)
}
