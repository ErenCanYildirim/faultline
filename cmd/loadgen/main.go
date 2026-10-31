// Command loadgen simulates many applications sending errors through the
// real SDK, then reports throughput, latency percentiles, consumer lag and
// exact loss. Run it from the host against the compose stack:
//
//	go run ./cmd/loadgen -apps 20 -rate 2000 -duration 60s -ramp 10s
//
// The first Ctrl+C stops generating and still flushes, drains and reports;
// a second one exits at once. The exit code is 1 if any acknowledged event
// is missing from ClickHouse.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"math/rand/v2"
	"os"
	"os/signal"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/erencanyildirim/faultline/internal/clickhouse"
	"github.com/erencanyildirim/faultline/internal/kafka"
	"github.com/erencanyildirim/faultline/internal/loadgen"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "loadgen:", err)
		os.Exit(2)
	}
}

func run() error {
	var (
		ingestURL    = flag.String("ingest", "http://localhost:8080", "ingest base URL")
		projectsFile = flag.String("projects-file", "deploy/projects.json", "projects file with the keys")
		prefix       = flag.String("project-prefix", "load-", "use the projects whose ID starts with this")
		numProjects  = flag.Int("projects", 0, "number of projects to use (M); 0 = every matching project")
		apps         = flag.Int("apps", 20, "number of simulated apps (N), spread over the projects")
		rate         = flag.Float64("rate", 2000, "total target events per second, after the ramp")
		duration     = flag.Duration("duration", 60*time.Second, "how long to generate events")
		ramp         = flag.Duration("ramp", 10*time.Second, "linear ramp-up from 0 to the full rate")
		bugs         = flag.Int("bugs", 200, "number of distinct errors")
		zipf         = flag.Float64("zipf", 1.2, "skew of the error mix (> 1; higher = fewer bugs dominate)")
		batch        = flag.Int("sdk-batch", 0, "SDK batch size; 0 = SDK default")
		flush        = flag.Duration("sdk-flush", 0, "SDK flush interval; 0 = SDK default")
		buffer       = flag.Int("sdk-buffer", 0, "SDK buffer size in events; 0 = SDK default")
		closeTimeout = flag.Duration("close-timeout", 30*time.Second, "time each SDK gets for its final flush")
		brokers      = flag.String("kafka", "localhost:19092", "Redpanda brokers for consumer lag; empty = skip")
		topic        = flag.String("topic", "events.raw", "topic the worker consumes")
		group        = flag.String("group", "faultline-worker", "the worker's consumer group")
		chURL        = flag.String("clickhouse", "http://localhost:8123", "ClickHouse URL for loss and end-to-end latency; empty = skip")
		chDB         = flag.String("ch-db", "faultline", "ClickHouse database")
		chUser       = flag.String("ch-user", "faultline", "ClickHouse user")
		chPassword   = flag.String("ch-password", "faultline", "ClickHouse password")
		drainTimeout = flag.Duration("drain-timeout", 2*time.Minute, "how long to wait for storage to catch up after the run")
		runID        = flag.String("run-id", "", "run ID; default: run-MMDD-HHMMSS-xxxx from the start time")
		seed         = flag.Int64("seed", 0, "random seed; 0 = from the clock")
		out          = flag.String("out", "", `report file; default bench/results/<run-id>[-<label>].json, "-" for none`)
		progress     = flag.Duration("progress", 5*time.Second, "progress line interval")
		label        = flag.String("label", "", `what this run changes, e.g. "baseline" or "batch-10000"; part of the file name`)
		note         = flag.String("note", "", "free text saved with the results: why this run, what changed")
		machine      = flag.String("machine", "bench/machine.json", "hardware description from scripts/machine-info.ps1")
		workerDebug  = flag.String("worker-debug", workerDebugDefault(), "worker debug endpoints for stage timings and profiles; empty = skip")
		ingestDebug  = flag.String("ingest-debug", "http://localhost:6070", "ingest debug endpoint for a CPU profile; empty = skip")
		profileSecs  = flag.Int("profile-seconds", 20, "length of the CPU profiles taken mid-run; 0 = none")
	)
	flag.Parse()

	projects, err := loadProjects(*projectsFile, *prefix, *numProjects)
	if err != nil {
		return err
	}
	if *runID == "" {
		*runID = defaultRunID(time.Now())
	}
	if *seed == 0 {
		*seed = time.Now().UnixNano()
	}

	cfg := loadgen.Config{
		IngestURL:     strings.TrimRight(*ingestURL, "/"),
		Projects:      projects,
		Apps:          *apps,
		Rate:          *rate,
		Duration:      *duration,
		Ramp:          *ramp,
		Bugs:          *bugs,
		Zipf:          *zipf,
		BatchSize:     *batch,
		FlushInterval: *flush,
		BufferSize:    *buffer,
		CloseTimeout:  *closeTimeout,
		RunID:         *runID,
		Seed:          *seed,
		DrainTimeout:  *drainTimeout,
		Progress:      os.Stdout,
		ProgressEvery: *progress,
	}

	// Record what this run ran on before starting it.
	envCtx, envCancel := context.WithTimeout(context.Background(), 20*time.Second)
	env, envWarnings := loadgen.CollectEnvironment(envCtx, *machine, []string{"ingest", "worker", "redpanda", "clickhouse"})
	envCancel()

	if *brokers != "" {
		lr, err := kafka.NewLagReader(strings.Split(*brokers, ","), *topic, *group)
		if err != nil {
			return err
		}
		defer lr.Close()
		lctx, lcancel := context.WithTimeout(context.Background(), 5*time.Second)
		if l, err := lr.Read(lctx); err == nil {
			env.Partitions = len(l.Partitions)
		}
		lcancel()
		cfg.Lag = func(ctx context.Context) (int64, error) {
			l, err := lr.Read(ctx)
			return l.Total, err
		}
	}
	var store *loadgen.ClickHouseStore
	if *chURL != "" {
		ch, err := clickhouse.New(clickhouse.Config{URL: *chURL, Database: *chDB, User: *chUser, Password: *chPassword})
		if err != nil {
			return err
		}
		pctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		err = ch.Ping(pctx)
		cancel()
		if err != nil {
			return fmt.Errorf("ClickHouse not reachable (use -clickhouse \"\" to run without it): %w", err)
		}
		ids := make([]string, len(projects))
		for i, p := range projects {
			ids[i] = p.ID
		}
		store = &loadgen.ClickHouseStore{Client: ch, RunID: *runID, Projects: ids, Since: time.Now()}
		cfg.Store = store
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sig := make(chan os.Signal, 2)
	signal.Notify(sig, os.Interrupt)
	go func() {
		<-sig
		fmt.Println("stopping generation; flushing and reporting (Ctrl+C again to quit now)")
		cancel()
		<-sig
		os.Exit(130)
	}()

	ids := make([]string, len(projects))
	for i, p := range projects {
		ids[i] = p.ID
	}
	fmt.Printf("run %s: %d apps over %s, %.0f events/s for %v (ramp %v)\n",
		*runID, *apps, strings.Join(ids, ","), *rate, *duration, *ramp)

	// Worker stage timings: a snapshot now and one after the drain; the
	// difference is this run.
	var workerBases []string
	if *workerDebug != "" {
		workerBases = strings.Split(*workerDebug, ",")
	}
	before := loadgen.FetchStages(context.Background(), workerBases)
	if len(workerBases) > 0 && len(before) == 0 {
		envWarnings = append(envWarnings, "no worker debug endpoint answered at "+*workerDebug+"; no stage breakdown or worker profile")
	}

	start := time.Now()
	profileWait := startProfiles(ctx, cfg, *runID, *profileSecs, before, *ingestDebug)

	rep, err := loadgen.Run(ctx, cfg)
	if err != nil {
		return err
	}
	rep.Label, rep.Note, rep.Environment = *label, *note, &env
	rep.Warnings = append(envWarnings, rep.Warnings...)
	rep.Profiles, err = profileWait()
	if err != nil {
		rep.Warnings = append(rep.Warnings, "CPU profile: "+err.Error())
	}
	sctx, scancel := context.WithTimeout(context.Background(), 30*time.Second)
	rep.Stages = loadgen.DiffStages(before, loadgen.FetchStages(sctx, workerBases))
	if store != nil {
		if rep.Inserts, err = store.InsertStats(sctx, start, time.Now()); err != nil {
			rep.Warnings = append(rep.Warnings, "ClickHouse query log: "+err.Error())
		}
	}
	scancel()
	rep.Print(os.Stdout)

	if *out != "-" {
		path := *out
		if path == "" {
			name := *runID
			if *label != "" {
				name += "-" + *label
			}
			path = filepath.Join("bench", "results", name+".json")
		}
		b, err := json.MarshalIndent(rep, "", "  ")
		if err != nil {
			return err
		}
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			return err
		}
		if err := os.WriteFile(path, b, 0o644); err != nil {
			return err
		}
		fmt.Println("report written to", path)
	}
	if rep.Accounting.Unexplained > 0 {
		os.Exit(1)
	}
	return nil
}

// defaultRunID is readable and sortable, and safe from the PII scrubber:
// a full date and time as one digit run (20261028-135758) can pass the
// card number check and would be stored as <card>.
func defaultRunID(t time.Time) string {
	const letters = "abcdefghijklmnopqrstuvwxyz"
	suffix := make([]byte, 4)
	for i := range suffix {
		suffix[i] = letters[rand.IntN(len(letters))]
	}
	return "run-" + t.UTC().Format("0102-150405") + "-" + string(suffix)
}

// workerDebugDefault lists the ports compose publishes for up to six
// workers (one per partition).
func workerDebugDefault() string {
	var b []string
	for p := 6060; p <= 6065; p++ {
		b = append(b, fmt.Sprintf("http://localhost:%d", p))
	}
	return strings.Join(b, ",")
}

// startProfiles takes CPU profiles of every worker that answered and of
// ingest, during the steady part of the run: after the ramp and a few
// seconds to settle, ending before generation stops. The returned function
// waits for them and lists the files.
func startProfiles(ctx context.Context, cfg loadgen.Config, runID string, seconds int,
	workers map[string]loadgen.WorkerStages, ingest string) func() ([]string, error) {
	none := func() ([]string, error) { return nil, nil }
	if seconds <= 0 {
		return none
	}
	startAfter := cfg.Ramp + 5*time.Second
	if room := int((cfg.Duration - startAfter - time.Second).Seconds()); room < seconds {
		seconds = room
	}
	if seconds < 5 {
		return func() ([]string, error) {
			return nil, fmt.Errorf("run too short for a profile after the ramp; use a longer -duration")
		}
	}
	targets := map[string]string{} // name -> base URL
	for base := range workers {
		targets["worker-"+loadgen.ProfileName(base)] = base
	}
	if ingest != "" {
		targets["ingest-"+loadgen.ProfileName(ingest)] = ingest
	}
	var (
		wg    sync.WaitGroup
		mu    sync.Mutex
		files []string
		errs  []string
	)
	wg.Add(1)
	go func() {
		defer wg.Done()
		select {
		case <-ctx.Done():
			return
		case <-time.After(startAfter):
		}
		var inner sync.WaitGroup
		for name, base := range targets {
			inner.Add(1)
			go func() {
				defer inner.Done()
				path := filepath.Join("bench", "profiles", runID+"-"+name+".pprof")
				err := loadgen.CaptureProfile(context.Background(), base, seconds, path)
				mu.Lock()
				defer mu.Unlock()
				if err != nil {
					if !strings.HasPrefix(name, "ingest") || !strings.Contains(err.Error(), "connect") {
						errs = append(errs, name+": "+err.Error())
					}
					return
				}
				files = append(files, path)
			}()
		}
		inner.Wait()
	}()
	return func() ([]string, error) {
		wg.Wait()
		mu.Lock()
		defer mu.Unlock()
		sort.Strings(files)
		if len(errs) > 0 {
			return files, fmt.Errorf("%s", strings.Join(errs, "; "))
		}
		return files, nil
	}
}

// loadProjects reads the keys from the same projects file ingest uses.
func loadProjects(path, prefix string, n int) ([]loadgen.Project, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("reading projects file: %w", err)
	}
	var f struct {
		Projects []struct {
			ID   string   `json:"id"`
			Keys []string `json:"keys"`
		} `json:"projects"`
	}
	if err := json.Unmarshal(raw, &f); err != nil {
		return nil, fmt.Errorf("parsing projects file: %w", err)
	}
	var out []loadgen.Project
	for _, p := range f.Projects {
		if strings.HasPrefix(p.ID, prefix) && len(p.Keys) > 0 {
			out = append(out, loadgen.Project{ID: p.ID, Key: p.Keys[0]})
		}
	}
	if len(out) == 0 {
		return nil, errors.New("no projects in " + path + " start with " + prefix)
	}
	if n > 0 {
		if n > len(out) {
			return nil, fmt.Errorf("asked for %d projects, %s has %d starting with %q", n, path, len(out), prefix)
		}
		out = out[:n]
	}
	return out, nil
}
