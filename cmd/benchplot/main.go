// Command benchplot draws load test timelines as an HTML page of charts:
//
//	go run ./cmd/benchplot -o bench/charts/flood.html -title "Flood one project" \
//	    -run "bench/results/run-a-quiet.json=normal projects" \
//	    -run "bench/results/run-b-noisy.json=flooding project@15" \
//	    -panel p99 -panel 429 -mark "15=flood starts" -mark "75=flood ends"
//
// -run is FILE[=NAME][@OFFSET]: OFFSET in seconds places a run that
// started later on the same time axis. -panel picks the metrics: lag,
// acked, generated, p50, p99, 429, stored. -mark is SECONDS=LABEL.
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/erencanyildirim/faultline/internal/loadgen"
)

type multi []string

func (m *multi) String() string     { return strings.Join(*m, ",") }
func (m *multi) Set(v string) error { *m = append(*m, v); return nil }

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "benchplot:", err)
		os.Exit(2)
	}
}

func run() error {
	var runs, panels, marks multi
	out := flag.String("o", "bench/charts/chart.html", "output HTML file")
	title := flag.String("title", "Load test", "page title")
	flag.Var(&runs, "run", "FILE[=NAME][@OFFSET]; repeat for several runs")
	flag.Var(&panels, "panel", "metric to draw; repeat (lag, acked, generated, p50, p99, 429, stored)")
	flag.Var(&marks, "mark", "SECONDS=LABEL; repeat")
	flag.Parse()

	if len(runs) == 0 {
		return fmt.Errorf("give at least one -run")
	}
	if len(panels) == 0 {
		panels = multi{"lag", "acked", "p99"}
	}
	var plotRuns []loadgen.PlotRun
	for _, spec := range runs {
		r, err := parseRun(spec)
		if err != nil {
			return err
		}
		plotRuns = append(plotRuns, r)
	}
	var plotMarks []loadgen.PlotMark
	for _, spec := range marks {
		at, label, ok := strings.Cut(spec, "=")
		sec, err := strconv.ParseFloat(at, 64)
		if !ok || err != nil {
			return fmt.Errorf("-mark %q: want SECONDS=LABEL", spec)
		}
		plotMarks = append(plotMarks, loadgen.PlotMark{At: sec, Label: label})
	}

	if err := os.MkdirAll(filepath.Dir(*out), 0o755); err != nil {
		return err
	}
	f, err := os.Create(*out)
	if err != nil {
		return err
	}
	if err := loadgen.WritePlot(f, *title, plotRuns, panels, plotMarks); err != nil {
		f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	fmt.Println("chart written to", *out)
	return nil
}

func parseRun(spec string) (loadgen.PlotRun, error) {
	var r loadgen.PlotRun
	if i := strings.LastIndex(spec, "@"); i >= 0 {
		off, err := strconv.ParseFloat(spec[i+1:], 64)
		if err != nil {
			return r, fmt.Errorf("-run %q: offset after @ must be seconds", spec)
		}
		r.Offset, spec = off, spec[:i]
	}
	path, name, _ := strings.Cut(spec, "=")
	raw, err := os.ReadFile(path)
	if err != nil {
		return r, err
	}
	var rep loadgen.Report
	if err := json.Unmarshal(raw, &rep); err != nil {
		return r, fmt.Errorf("%s is not a loadgen result file: %w", path, err)
	}
	if name == "" {
		name = rep.Label
	}
	if name == "" {
		name = rep.RunID
	}
	r.Name, r.Report = name, &rep
	return r, nil
}
