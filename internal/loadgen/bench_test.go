package loadgen

import (
	"context"
	"encoding/json"
	"errors"
	"math"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestDiffStages(t *testing.T) {
	before := map[string]WorkerStages{
		"a": {Stored: 1000, Batches: 1, Seconds: map[string]float64{"poll": 1, "scrub": 0.2, "store": 0.3}},
	}
	after := map[string]WorkerStages{
		// a stored 4000 more events during the run.
		"a": {Stored: 5000, Batches: 5, Seconds: map[string]float64{"poll": 1.5, "scrub": 1.0, "store": 1.5}},
		// b started during the run: counts from zero.
		"b": {Stored: 1000, Batches: 1, Seconds: map[string]float64{"poll": 0.1, "scrub": 0.2, "store": 0.2}},
	}
	r := DiffStages(before, after)
	if r.Workers != 2 || r.Stored != 5000 || r.Batches != 5 || r.AvgRows != 1000 {
		t.Fatalf("report = %+v", r)
	}
	near := func(a, b float64) bool { return math.Abs(a-b) < 1e-9 }
	// scrub: 0.8s on a + 0.2s on b = 1.0s for 5000 events = 200ms per 1000.
	if !near(r.Seconds["scrub"], 1.0) || !near(r.MsPer1000["scrub"], 200) {
		t.Errorf("scrub = %v s, %v ms/1000", r.Seconds["scrub"], r.MsPer1000["scrub"])
	}
	// Total 0.6 + 1.0 + 1.4 = 3.0s; store is 1.4 of it.
	if !near(r.Share["store"], 1.4/3.0) {
		t.Errorf("store share = %v", r.Share["store"])
	}

	// A worker that restarted has counters lower than before: count it from zero.
	r = DiffStages(before, map[string]WorkerStages{"a": {Stored: 10, Batches: 1, Seconds: map[string]float64{"store": 0.1}}})
	if r.Stored != 10 {
		t.Errorf("restarted worker: stored %d, want 10", r.Stored)
	}
	if DiffStages(before, nil) != nil {
		t.Error("no workers answered: no report")
	}
}

func TestStageOrder(t *testing.T) {
	got := sortedKeys(map[string]float64{"store": 1, "zeta": 1, "poll": 1, "commit": 1, "decode": 1})
	if strings.Join(got, ",") != "poll,decode,store,commit,zeta" {
		t.Errorf("order = %v", got)
	}
}

func TestCollectEnvironment(t *testing.T) {
	old := runCommand
	defer func() { runCommand = old }()
	runCommand = func(_ context.Context, name string, args ...string) ([]byte, error) {
		cmd := name + " " + strings.Join(args, " ")
		switch {
		case strings.HasPrefix(cmd, "docker info"):
			return []byte(`{"NCPU":8,"MemTotal":8589934592,"OperatingSystem":"Docker Desktop","ServerVersion":"29.0.1"}`), nil
		case strings.Contains(cmd, "service=worker"):
			return []byte("abc\ndef\n"), nil
		case strings.Contains(cmd, "service=ingest"):
			return nil, errors.New("boom")
		case strings.HasPrefix(cmd, "docker inspect"):
			return []byte(`{"Image":"faultline/worker:dev","Env":["WORKER_BATCH_SIZE=1000","CLICKHOUSE_PASSWORD=faultline","PATH=/bin"]}`), nil
		}
		return nil, errors.New("unexpected " + cmd)
	}
	// With the byte order mark Windows PowerShell 5 writes.
	machine := filepath.Join(t.TempDir(), "machine.json")
	if err := os.WriteFile(machine, []byte("\xef\xbb\xbf"+`{"cpu":"Test CPU","cores":4}`), 0o644); err != nil {
		t.Fatal(err)
	}

	env, warnings := CollectEnvironment(context.Background(), machine, []string{"worker", "ingest"})
	if env.Docker == nil || env.Docker.CPUs != 8 || env.Docker.MemoryGiB != 8 {
		t.Errorf("docker = %+v", env.Docker)
	}
	w := env.Services["worker"]
	if w.Replicas != 2 || w.Env["WORKER_BATCH_SIZE"] != "1000" {
		t.Errorf("worker = %+v", w)
	}
	if _, leaked := w.Env["CLICKHOUSE_PASSWORD"]; leaked {
		t.Error("secrets must not be recorded")
	}
	if !strings.Contains(string(env.Machine), "Test CPU") {
		t.Errorf("machine = %s", env.Machine)
	}
	if len(warnings) != 1 || !strings.Contains(warnings[0], "ingest") {
		t.Errorf("warnings = %v, want one for the failing ingest lookup", warnings)
	}

	// A missing machine file is a warning, not a failure.
	_, warnings = CollectEnvironment(context.Background(), filepath.Join(t.TempDir(), "none.json"), nil)
	if len(warnings) != 1 || !strings.Contains(warnings[0], "machine-info.ps1") {
		t.Errorf("warnings = %v", warnings)
	}
}

func TestFetchStagesAndProfile(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/debug/stages", func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(WorkerStages{Stored: 7, Batches: 1, Seconds: map[string]float64{"store": 0.5}})
	})
	mux.HandleFunc("/debug/pprof/profile", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("seconds") != "5" {
			http.Error(w, "bad seconds", http.StatusBadRequest)
			return
		}
		_, _ = w.Write([]byte("profile-bytes"))
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	got := FetchStages(context.Background(), []string{srv.URL, "http://127.0.0.1:1"})
	if len(got) != 1 || got[srv.URL].Stored != 7 {
		t.Errorf("stages = %+v; unreachable workers must be skipped", got)
	}

	path := filepath.Join(t.TempDir(), "sub", "p.pprof")
	if err := CaptureProfile(context.Background(), srv.URL, 5, path); err != nil {
		t.Fatal(err)
	}
	if b, _ := os.ReadFile(path); string(b) != "profile-bytes" {
		t.Errorf("profile file = %q", b)
	}
	if ProfileName("http://localhost:6061") != "6061" {
		t.Error("ProfileName")
	}
}
