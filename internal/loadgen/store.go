package loadgen

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/erencanyildirim/faultline/internal/clickhouse"
)

// Store reads back what a run stored, to count loss and measure latency.
type Store interface {
	// Stored is the number of distinct events of the run in storage.
	Stored(ctx context.Context) (uint64, error)
	// Verify computes the final numbers once the run has drained.
	Verify(ctx context.Context, generated uint64) (StoreResult, error)
}

// StoreResult is what storage says about a run.
type StoreResult struct {
	// Rows counts every stored row, duplicates included; Events counts
	// distinct event IDs. Rows > Events means at-least-once delivery wrote
	// some events twice and ClickHouse hasn't merged them away yet.
	Rows       uint64 `json:"rows"`
	Events     uint64 `json:"events"`
	Duplicates uint64 `json:"duplicates"`
	// Seqs counts distinct sequence numbers. Equal to Events unless the
	// run's own bookkeeping is broken.
	Seqs uint64 `json:"distinct_seqs"`
	// MissingSeqs lists up to 20 sequence numbers that never arrived.
	MissingSeqs []uint64   `json:"missing_seqs_sample,omitempty"`
	Issues      uint64     `json:"issues"`
	TopBugs     []BugCount `json:"top_bugs"`

	// EndToEnd is from the event's timestamp (set by the SDK at capture)
	// to its insert into ClickHouse. It includes the SDK's batching wait.
	EndToEnd LatencyMs `json:"end_to_end"`
	// Pipeline is from ingest receiving the event to its insert: Redpanda,
	// the worker and ClickHouse, without the SDK.
	Pipeline LatencyMs `json:"pipeline"`
}

// BugCount is how many stored events one bug has.
type BugCount struct {
	Bug    string `json:"bug"`
	Events uint64 `json:"events"`
}

// LatencyMs holds percentiles computed by ClickHouse, in milliseconds.
type LatencyMs struct {
	P50  float64 `json:"p50_ms"`
	P90  float64 `json:"p90_ms"`
	P95  float64 `json:"p95_ms"`
	P99  float64 `json:"p99_ms"`
	P999 float64 `json:"p99_9_ms"`
	Max  float64 `json:"max_ms"`
}

// ClickHouseStore queries the events table for one run.
type ClickHouseStore struct {
	Client   *clickhouse.Client
	RunID    string
	Projects []string
	// Since bounds the scan to the partitions of the run.
	Since time.Time
}

func (s *ClickHouseStore) where() string {
	ids := make([]string, len(s.Projects))
	for i, p := range s.Projects {
		ids[i] = quote(p)
	}
	return fmt.Sprintf("project_id IN (%s) AND timestamp >= toDateTime64(%s, 3, 'UTC') AND tags['run_id'] = %s",
		strings.Join(ids, ", "), quote(s.Since.UTC().Add(-5*time.Minute).Format("2006-01-02 15:04:05.000")), quote(s.RunID))
}

// quote makes a ClickHouse string literal.
func quote(s string) string {
	return "'" + strings.NewReplacer(`\`, `\\`, `'`, `\'`).Replace(s) + "'"
}

const jsonSettings = " SETTINGS output_format_json_quote_64bit_integers = 0 FORMAT JSONEachRow"

// Stored implements Store.
func (s *ClickHouseStore) Stored(ctx context.Context) (uint64, error) {
	out, err := s.Client.Query(ctx, "SELECT uniqExact(event_id) FROM events WHERE "+s.where()+" FORMAT TabSeparated")
	if err != nil {
		return 0, err
	}
	return strconv.ParseUint(strings.TrimSpace(string(out)), 10, 64)
}

// Verify implements Store.
func (s *ClickHouseStore) Verify(ctx context.Context, generated uint64) (StoreResult, error) {
	var res StoreResult
	w := s.where()

	var counts struct {
		Rows   uint64 `json:"rows"`
		Events uint64 `json:"events"`
		Seqs   uint64 `json:"seqs"`
		Issues uint64 `json:"issues"`
	}
	if err := s.queryRow(ctx, `SELECT count() AS rows, uniqExact(event_id) AS events,
		uniqExact(tags['seq']) AS seqs, uniqExact(fingerprint) AS issues FROM events WHERE `+w, &counts); err != nil {
		return res, fmt.Errorf("counting events: %w", err)
	}
	res.Rows, res.Events, res.Seqs, res.Issues = counts.Rows, counts.Events, counts.Seqs, counts.Issues
	res.Duplicates = res.Rows - res.Events

	if res.Seqs < generated {
		out, err := s.Client.Query(ctx, fmt.Sprintf(`SELECT number FROM numbers(%d)
			WHERE toString(number) NOT IN (SELECT tags['seq'] FROM events WHERE %s)
			ORDER BY number LIMIT 20 FORMAT TabSeparated`, generated, w))
		if err != nil {
			return res, fmt.Errorf("finding missing sequence numbers: %w", err)
		}
		for _, line := range strings.Fields(string(out)) {
			if n, err := strconv.ParseUint(line, 10, 64); err == nil {
				res.MissingSeqs = append(res.MissingSeqs, n)
			}
		}
	}

	// One row per event: the first insert counts, a later duplicate doesn't.
	// The outer aliases differ from the inner columns on purpose: ClickHouse
	// would otherwise resolve e2e inside quantilesExact to the outer alias.
	var lat struct {
		E2E     []float64 `json:"e2e_q"`
		E2EMax  float64   `json:"e2e_max"`
		Pipe    []float64 `json:"pipe_q"`
		PipeMax float64   `json:"pipe_max"`
	}
	q := `SELECT quantilesExact(0.5, 0.9, 0.95, 0.99, 0.999)(e2e) AS e2e_q, max(e2e) AS e2e_max,
		quantilesExact(0.5, 0.9, 0.95, 0.99, 0.999)(pipe) AS pipe_q, max(pipe) AS pipe_max
		FROM (
			SELECT event_id,
				toFloat64(dateDiff('millisecond', any(timestamp), min(inserted_at))) AS e2e,
				toFloat64(dateDiff('millisecond', any(received_at), min(inserted_at))) AS pipe
			FROM events WHERE ` + w + ` GROUP BY event_id
		)`
	if err := s.queryRow(ctx, q, &lat); err != nil {
		return res, fmt.Errorf("measuring latency: %w", err)
	}
	res.EndToEnd = toLatency(lat.E2E, lat.E2EMax)
	res.Pipeline = toLatency(lat.Pipe, lat.PipeMax)

	out, err := s.Client.Query(ctx, `SELECT tags['bug'] AS bug, uniqExact(event_id) AS events FROM events WHERE `+w+
		` GROUP BY bug ORDER BY events DESC LIMIT 5`+jsonSettings)
	if err != nil {
		return res, fmt.Errorf("top bugs: %w", err)
	}
	for _, line := range strings.Split(strings.TrimSpace(string(out)), "\n") {
		var b BugCount
		if line != "" && json.Unmarshal([]byte(line), &b) == nil {
			res.TopBugs = append(res.TopBugs, b)
		}
	}
	return res, nil
}

func (s *ClickHouseStore) queryRow(ctx context.Context, q string, into any) error {
	out, err := s.Client.Query(ctx, q+jsonSettings)
	if err != nil {
		return err
	}
	return json.Unmarshal(out, into)
}

func toLatency(q []float64, maxMs float64) LatencyMs {
	if len(q) != 5 {
		return LatencyMs{}
	}
	return LatencyMs{P50: q[0], P90: q[1], P95: q[2], P99: q[3], P999: q[4], Max: maxMs}
}
