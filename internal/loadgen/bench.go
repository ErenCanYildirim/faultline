package loadgen

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"time"
)

// Benchmark evidence collected around a run: the machine and settings it
// ran on, where the worker spent its time, what ClickHouse saw, and CPU
// profiles. A result file without these says how fast, but not on what or
// why.

// Environment is the machine and configuration a run used.
type Environment struct {
	Host HostInfo `json:"host"`
	// Machine is the hardware description from scripts/machine-info.ps1:
	// CPU model, RAM, disk. Go can't read those portably.
	Machine  json.RawMessage        `json:"machine,omitempty"`
	Docker   *DockerInfo            `json:"docker,omitempty"`
	Services map[string]ServiceInfo `json:"services,omitempty"`
	// Partitions of the events topic: with more consumers than partitions,
	// the extra consumers sit idle.
	Partitions int `json:"topic_partitions,omitempty"`
}

// HostInfo is where the load generator itself ran.
type HostInfo struct {
	OS        string `json:"os"`
	Arch      string `json:"arch"`
	CPUs      int    `json:"cpus"`
	GoVersion string `json:"go_version"`
}

// DockerInfo is what Docker can use: on Docker Desktop these are the VM's
// limits, which can be far below the machine's.
type DockerInfo struct {
	CPUs            int     `json:"cpus"`
	MemoryGiB       float64 `json:"memory_gib"`
	OperatingSystem string  `json:"operating_system"`
	KernelVersion   string  `json:"kernel_version"`
	ServerVersion   string  `json:"server_version"`
}

// ServiceInfo is one compose service: how many containers ran and with
// which settings. Secrets are left out.
type ServiceInfo struct {
	Replicas int               `json:"replicas"`
	Image    string            `json:"image,omitempty"`
	Env      map[string]string `json:"env,omitempty"`
}

// runCommand runs a program and returns its output; tests replace it.
var runCommand = func(ctx context.Context, name string, args ...string) ([]byte, error) {
	var stderr bytes.Buffer
	cmd := exec.CommandContext(ctx, name, args...)
	cmd.Stderr = &stderr
	out, err := cmd.Output()
	if err != nil {
		return nil, fmt.Errorf("%s %s: %w: %s", name, strings.Join(args, " "), err, strings.TrimSpace(stderr.String()))
	}
	return out, nil
}

// CollectEnvironment gathers what it can and returns a warning for each
// part it couldn't get, rather than failing the run.
func CollectEnvironment(ctx context.Context, machineFile string, services []string) (Environment, []string) {
	env := Environment{Host: HostInfo{OS: runtime.GOOS, Arch: runtime.GOARCH, CPUs: runtime.NumCPU(), GoVersion: runtime.Version()}}
	var warnings []string

	if machineFile != "" {
		raw, err := os.ReadFile(machineFile)
		// Windows PowerShell 5 writes UTF-8 with a byte order mark, which
		// is not valid JSON.
		raw = bytes.TrimPrefix(raw, []byte("\xef\xbb\xbf"))
		switch {
		case os.IsNotExist(err):
			warnings = append(warnings, fmt.Sprintf("no %s: run scripts/machine-info.ps1 once so results record the hardware", machineFile))
		case err != nil:
			warnings = append(warnings, "reading machine file: "+err.Error())
		case !json.Valid(raw):
			warnings = append(warnings, machineFile+" is not valid JSON")
		default:
			env.Machine = bytes.TrimSpace(raw)
		}
	}

	if out, err := runCommand(ctx, "docker", "info", "--format", "{{json .}}"); err != nil {
		warnings = append(warnings, "docker info: "+err.Error())
	} else {
		var di struct {
			NCPU            int
			MemTotal        int64
			OperatingSystem string
			KernelVersion   string
			ServerVersion   string
		}
		if err := json.Unmarshal(out, &di); err != nil {
			warnings = append(warnings, "parsing docker info: "+err.Error())
		} else {
			env.Docker = &DockerInfo{CPUs: di.NCPU, MemoryGiB: float64(di.MemTotal) / (1 << 30),
				OperatingSystem: di.OperatingSystem, KernelVersion: di.KernelVersion, ServerVersion: di.ServerVersion}
		}
	}

	env.Services = map[string]ServiceInfo{}
	for _, svc := range services {
		info, err := serviceInfo(ctx, svc)
		if err != nil {
			warnings = append(warnings, fmt.Sprintf("service %s: %v", svc, err))
			continue
		}
		env.Services[svc] = info
	}
	return env, warnings
}

func serviceInfo(ctx context.Context, svc string) (ServiceInfo, error) {
	out, err := runCommand(ctx, "docker", "ps", "--filter", "label=com.docker.compose.service="+svc, "--format", "{{.ID}}")
	if err != nil {
		return ServiceInfo{}, err
	}
	ids := strings.Fields(string(out))
	info := ServiceInfo{Replicas: len(ids)}
	if len(ids) == 0 {
		return info, nil
	}
	out, err = runCommand(ctx, "docker", "inspect", "--format", "{{json .Config}}", ids[0])
	if err != nil {
		return info, err
	}
	var cfg struct {
		Image string
		Env   []string
	}
	if err := json.Unmarshal(out, &cfg); err != nil {
		return info, err
	}
	info.Image = cfg.Image
	info.Env = map[string]string{}
	for _, kv := range cfg.Env {
		k, v, _ := strings.Cut(kv, "=")
		if k == "PATH" || isSecretName(k) {
			continue
		}
		info.Env[k] = v
	}
	return info, nil
}

func isSecretName(k string) bool {
	k = strings.ToUpper(k)
	for _, s := range []string{"PASSWORD", "SECRET", "TOKEN", "KEY"} {
		if strings.Contains(k, s) {
			return true
		}
	}
	return false
}

// WorkerStages is one worker's cumulative /debug/stages answer.
type WorkerStages struct {
	Stored       uint64             `json:"stored"`
	DeadLettered uint64             `json:"dead_lettered"`
	Batches      uint64             `json:"batches"`
	Seconds      map[string]float64 `json:"seconds"`
}

// FetchStages reads /debug/stages from every worker that answers. Workers
// that don't answer are skipped: with --scale worker=2, ports 6062 to 6065
// are simply unused.
func FetchStages(ctx context.Context, bases []string) map[string]WorkerStages {
	out := map[string]WorkerStages{}
	client := &http.Client{Timeout: 2 * time.Second}
	for _, base := range bases {
		var ws WorkerStages
		if err := getJSON(ctx, client, strings.TrimRight(base, "/")+"/debug/stages", &ws); err == nil {
			out[base] = ws
		}
	}
	return out
}

func getJSON(ctx context.Context, client *http.Client, u string, into any) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return err
	}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("%s: HTTP %d", u, resp.StatusCode)
	}
	return json.NewDecoder(resp.Body).Decode(into)
}

// StageReport is where the workers spent their time during the run.
type StageReport struct {
	Workers int     `json:"workers"`
	Stored  uint64  `json:"stored"`
	Batches uint64  `json:"batches"`
	AvgRows float64 `json:"avg_events_per_batch"`
	// Seconds per stage, summed over workers.
	Seconds map[string]float64 `json:"seconds"`
	// MsPer1000 is Seconds per 1000 stored events: comparable between runs
	// of different length and rate.
	MsPer1000 map[string]float64 `json:"ms_per_1000_events"`
	// Share of the total time of all stages.
	Share map[string]float64 `json:"share"`
}

// DiffStages turns two snapshots into the breakdown for the time between
// them. A worker present only in after (it started during the run) counts
// from zero; one present only in before is ignored.
func DiffStages(before, after map[string]WorkerStages) *StageReport {
	if len(after) == 0 {
		return nil
	}
	r := &StageReport{Seconds: map[string]float64{}, MsPer1000: map[string]float64{}, Share: map[string]float64{}}
	for base, a := range after {
		b := before[base]
		if a.Stored < b.Stored { // restarted in between: counters reset
			b = WorkerStages{}
		}
		r.Workers++
		r.Stored += a.Stored - b.Stored
		r.Batches += a.Batches - b.Batches
		for k, v := range a.Seconds {
			r.Seconds[k] += v - b.Seconds[k]
		}
	}
	var total float64
	for _, v := range r.Seconds {
		total += v
	}
	for k, v := range r.Seconds {
		if r.Stored > 0 {
			r.MsPer1000[k] = v * 1e6 / float64(r.Stored)
		}
		if total > 0 {
			r.Share[k] = v / total
		}
	}
	if r.Batches > 0 {
		r.AvgRows = float64(r.Stored) / float64(r.Batches)
	}
	return r
}

// CaptureProfile fetches a CPU profile of the given length from a debug
// server and writes it to path.
func CaptureProfile(ctx context.Context, base string, seconds int, path string) error {
	u := fmt.Sprintf("%s/debug/pprof/profile?seconds=%d", strings.TrimRight(base, "/"), seconds)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return err
	}
	resp, err := (&http.Client{Timeout: time.Duration(seconds+15) * time.Second}).Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("%s: HTTP %d", u, resp.StatusCode)
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	if _, err := io.Copy(f, resp.Body); err != nil {
		f.Close()
		return err
	}
	return f.Close()
}

// ProfileName turns a debug URL into a short file name part:
// http://localhost:6060 -> 6060.
func ProfileName(base string) string {
	u, err := url.Parse(base)
	if err != nil || u.Port() == "" {
		return strings.NewReplacer(":", "-", "/", "-").Replace(base)
	}
	return u.Port()
}

// InsertStats is what ClickHouse's query log says about the worker's
// inserts into the events table during the run.
type InsertStats struct {
	Inserts uint64 `json:"inserts"`
	// Per insert, measured inside ClickHouse: parsing the rows, writing
	// the part, running the issue views.
	DurationP50Ms float64 `json:"duration_p50_ms"`
	DurationP99Ms float64 `json:"duration_p99_ms"`
	DurationMaxMs float64 `json:"duration_max_ms"`
	// DurationTotalS compared with the worker's store stage says how much
	// of the store time is ClickHouse and how much is the worker encoding
	// rows and sending them.
	DurationTotalS float64 `json:"duration_total_s"`
	AvgMemoryMiB   float64 `json:"avg_memory_mib"`
}

// InsertStats queries system.query_log for inserts into events between
// from and to.
func (s *ClickHouseStore) InsertStats(ctx context.Context, from, to time.Time) (*InsertStats, error) {
	// The query log is written in the background every few seconds; flush
	// so the last inserts of the run are in it. Needs the SYSTEM FLUSH
	// LOGS privilege; without it the last seconds may be missing.
	_ = s.Client.Exec(ctx, "SYSTEM FLUSH LOGS")
	var row struct {
		Inserts uint64    `json:"inserts"`
		Q       []float64 `json:"q"`
		Max     float64   `json:"max_ms"`
		Sum     float64   `json:"sum_ms"`
		Mem     float64   `json:"mem"`
	}
	q := fmt.Sprintf(`SELECT count() AS inserts,
			quantiles(0.5, 0.99)(query_duration_ms) AS q,
			toFloat64(max(query_duration_ms)) AS max_ms,
			toFloat64(sum(query_duration_ms)) AS sum_ms,
			avg(memory_usage) AS mem
		FROM system.query_log
		WHERE type = 'QueryFinish' AND query_kind = 'Insert'
			AND current_database = currentDatabase()
			AND startsWith(query, 'INSERT INTO events ')
			AND event_time >= toDateTime(%d) AND event_time <= toDateTime(%d)`,
		from.Unix(), to.Unix()+1)
	if err := s.queryRow(ctx, q, &row); err != nil {
		return nil, err
	}
	st := &InsertStats{Inserts: row.Inserts, DurationMaxMs: row.Max, DurationTotalS: row.Sum / 1000, AvgMemoryMiB: row.Mem / (1 << 20)}
	if len(row.Q) == 2 {
		st.DurationP50Ms, st.DurationP99Ms = row.Q[0], row.Q[1]
	}
	return st, nil
}

// sortedKeys returns m's keys in a fixed order: the worker's stage order
// first, anything else after, alphabetically.
func sortedKeys(m map[string]float64) []string {
	order := map[string]int{"poll": 0, "decode": 1, "dlq": 2, "scrub": 3, "group": 4, "store": 5, "commit": 6}
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool {
		oi, iok := order[keys[i]]
		oj, jok := order[keys[j]]
		switch {
		case iok && jok:
			return oi < oj
		case iok != jok:
			return iok
		default:
			return keys[i] < keys[j]
		}
	})
	return keys
}
