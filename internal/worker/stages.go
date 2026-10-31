package worker

import (
	"sync/atomic"
	"time"
)

// Stage is one step of the worker loop. Timing each one says where a
// batch's time goes, which is the first question when the worker falls
// behind: waiting for messages, CPU work on them, or waiting for storage.
type Stage int

const (
	StagePoll   Stage = iota // waiting for messages from the queue
	StageDecode              // JSON decode and validation
	StageDLQ                 // writing dead letters
	StageScrub               // PII scrubbing
	StageGroup               // fingerprinting
	StageStore               // encoding the rows and the ClickHouse insert, retries included
	StageCommit              // committing offsets
	numStages
)

var stageNames = [numStages]string{"poll", "decode", "dlq", "scrub", "group", "store", "commit"}

func (s Stage) String() string { return stageNames[s] }

type stageTimes struct {
	ns [numStages]atomic.Int64
}

func (t *stageTimes) add(s Stage, since time.Time) {
	t.ns[s].Add(int64(time.Since(since)))
}

// StageStats are cumulative seconds spent in each stage since start, with
// the counters they relate to. Two snapshots subtracted give the breakdown
// for a time window, such as a load test run.
type StageStats struct {
	Stored       uint64             `json:"stored"`
	DeadLettered uint64             `json:"dead_lettered"`
	Batches      uint64             `json:"batches"`
	Seconds      map[string]float64 `json:"seconds"`
}

// StageStats returns the cumulative stage timings.
func (w *Worker) StageStats() StageStats {
	s := StageStats{
		Stored:       w.stored.Load(),
		DeadLettered: w.deadLettered.Load(),
		Batches:      w.batches.Load(),
		Seconds:      make(map[string]float64, numStages),
	}
	for i := range numStages {
		s.Seconds[Stage(i).String()] = time.Duration(w.stages.ns[i].Load()).Seconds()
	}
	return s
}
