// Command losscheck compares what a load test run sent with what
// ClickHouse stored, by sequence number:
//
//	go run ./cmd/losscheck -run-id run-1031-140212-abcd
//	go run ./cmd/losscheck -report bench/results/run-1031-140212-abcd-kill-worker.json -optimize
//
// What was sent comes from the run's result file: every event the load
// generator created, and what the SDK knowingly dropped or gave up on.
// What was stored comes from the events table. Duplicate rows are counted
// as stored, before ClickHouse merges them away; -optimize then forces
// that merge and counts again, to show the duplicates collapse.
//
// Exit code 1 if events are missing that the SDK didn't account for.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"time"

	"github.com/erencanyildirim/faultline/internal/clickhouse"
	"github.com/erencanyildirim/faultline/internal/loadgen"
)

func main() {
	ok, err := run()
	if err != nil {
		fmt.Fprintln(os.Stderr, "losscheck:", err)
		os.Exit(2)
	}
	if !ok {
		os.Exit(1)
	}
}

func run() (bool, error) {
	var (
		reportPath = flag.String("report", "", "result file written by loadgen")
		runID      = flag.String("run-id", "", "run ID; finds its file in bench/results")
		chURL      = flag.String("clickhouse", "http://localhost:8123", "ClickHouse URL")
		chDB       = flag.String("ch-db", "faultline", "ClickHouse database")
		chUser     = flag.String("ch-user", "faultline", "ClickHouse user")
		chPassword = flag.String("ch-password", "faultline", "ClickHouse password")
		optimize   = flag.Bool("optimize", false, "force ClickHouse to merge duplicates (OPTIMIZE ... FINAL), then count again")
	)
	flag.Parse()

	path, err := findReport(*reportPath, *runID)
	if err != nil {
		return false, err
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		return false, err
	}
	var rep loadgen.Report
	if err := json.Unmarshal(raw, &rep); err != nil {
		return false, fmt.Errorf("%s is not a loadgen result file: %w", path, err)
	}

	ch, err := clickhouse.New(clickhouse.Config{URL: *chURL, Database: *chDB, User: *chUser, Password: *chPassword})
	if err != nil {
		return false, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	store := &loadgen.ClickHouseStore{Client: ch, RunID: rep.RunID, Projects: rep.Config.Projects, Since: rep.Started}

	res, err := store.Verify(ctx, rep.Generation.Generated)
	if err != nil {
		return false, err
	}
	c := loadgen.Check(rep, res)
	printCheck(path, rep, c)

	if *optimize {
		// Merges every part of the table into one, which removes rows
		// with the same sorting key: what ReplacingMergeTree does in the
		// background, now, so the result is visible right away.
		if err := ch.Exec(ctx, "OPTIMIZE TABLE events FINAL"); err != nil {
			return false, fmt.Errorf("OPTIMIZE: %w", err)
		}
		after, err := store.Verify(ctx, rep.Generation.Generated)
		if err != nil {
			return false, err
		}
		fmt.Printf("after merge   %d rows for %d distinct events: %d duplicate rows left\n",
			after.Rows, after.Events, after.Duplicates)
	}
	return c.Unexplained == 0, nil
}

func printCheck(path string, rep loadgen.Report, c loadgen.LossCheck) {
	p := func(f string, a ...any) { fmt.Printf(f+"\n", a...) }
	p("run           %s (%s)", rep.RunID, filepath.Base(path))
	p("sent          %d events generated; ingest acknowledged %d; the SDK dropped %d and gave up on %d",
		c.Generated, c.Acknowledged, c.DroppedBySDK, c.FailedInSDK)
	p("stored        %d distinct events in %d rows: %d duplicate rows not merged yet", c.Stored, c.Rows, c.DuplicateRows)
	p("missing       %d sequence numbers", c.Missing)
	p("              %d explained by the SDK (dropped or given up), %d stored although the SDK gave up on them",
		c.Explained, c.StoredDespiteFailed)
	if c.Unexplained == 0 {
		p("LOSS          0")
	} else {
		p("LOSS          %d events acknowledged or sent but not stored; first missing: %v", c.Unexplained, c.MissingSample)
	}
}

// findReport returns the result file to check: the given path, or the
// newest file in bench/results whose name starts with the run ID.
func findReport(path, runID string) (string, error) {
	if path != "" {
		return path, nil
	}
	if runID == "" {
		return "", errors.New("give -report or -run-id")
	}
	matches, _ := filepath.Glob(filepath.Join("bench", "results", runID+"*.json"))
	if len(matches) == 0 {
		return "", fmt.Errorf("no result file for %s in bench/results", runID)
	}
	sort.Strings(matches)
	return matches[len(matches)-1], nil
}
