package ingest

import (
	"sort"
	"sync"
	"sync/atomic"
)

// projectCounters are cumulative event counts for one project.
type projectCounters struct {
	accepted    atomic.Uint64 // acknowledged by the queue
	rateLimited atomic.Uint64 // refused with 429
	invalid     atomic.Uint64 // rejected by validation
	queueErrors atomic.Uint64 // refused with 503 because the queue failed
}

// Stats keeps per-project counters so that rate limiting is visible, not
// silent: an operator can see which project is being limited and by how
// much. They are in-process and reset on restart; OpenTelemetry metrics
// replace this endpoint in Phase 4.
type Stats struct {
	projects sync.Map // project ID -> *projectCounters
}

func (s *Stats) project(id string) *projectCounters {
	if c, ok := s.projects.Load(id); ok {
		return c.(*projectCounters)
	}
	c, _ := s.projects.LoadOrStore(id, &projectCounters{})
	return c.(*projectCounters)
}

// ProjectStats is one project's counters.
type ProjectStats struct {
	Project     string `json:"project"`
	Accepted    uint64 `json:"accepted"`
	RateLimited uint64 `json:"rate_limited"`
	Invalid     uint64 `json:"invalid"`
	QueueErrors uint64 `json:"queue_errors"`
}

// Snapshot returns the counters of every project that has sent anything,
// sorted by project ID.
func (s *Stats) Snapshot() []ProjectStats {
	var out []ProjectStats
	s.projects.Range(func(k, v any) bool {
		c := v.(*projectCounters)
		out = append(out, ProjectStats{
			Project:     k.(string),
			Accepted:    c.accepted.Load(),
			RateLimited: c.rateLimited.Load(),
			Invalid:     c.invalid.Load(),
			QueueErrors: c.queueErrors.Load(),
		})
		return true
	})
	sort.Slice(out, func(i, j int) bool { return out[i].Project < out[j].Project })
	return out
}
