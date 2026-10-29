package loadgen

import (
	"math"
	"math/bits"
	"sync"
	"time"
)

// Histogram records durations with bounded relative error, in the style of
// HdrHistogram: values are grouped into buckets whose width grows with the
// value, so 50µs and 5s are both stored to within 1%.
//
// Why not averages: latency is not normally distributed. A mean hides the
// slow tail, which is what users notice, and a p99 computed from a sample
// of a few thousand values is noisy. Every value goes into a bucket here, so
// percentiles are exact up to the bucket width, whatever the count.
//
// Layout: values below 128µs get one bucket each. Above that, every power of
// two is split into 128 equal buckets, so a bucket is at most 1/128 (0.8%)
// of its value wide. Up to about 2^40µs (12 days) in about 4,400 buckets.
type Histogram struct {
	mu     sync.Mutex
	counts []uint64
	total  uint64
	min    int64
	max    int64
	sum    float64
}

const (
	subBucketBits  = 7
	subBuckets     = 1 << subBucketBits // 128
	maxExponent    = 40 - subBucketBits
	histogramSlots = (maxExponent + 2) * subBuckets
)

func NewHistogram() *Histogram {
	return &Histogram{counts: make([]uint64, histogramSlots), min: math.MaxInt64}
}

// bucketOf maps a value in microseconds to its bucket.
func bucketOf(v int64) int {
	if v < subBuckets {
		return int(max(v, 0))
	}
	exp := bits.Len64(uint64(v)) - subBucketBits - 1 // v>>exp is in [128, 256)
	if exp > maxExponent {
		return histogramSlots - 1
	}
	return (exp+1)*subBuckets + int(v>>exp) - subBuckets
}

// bucketUpper is the largest value that falls into bucket i.
func bucketUpper(i int) int64 {
	if i < subBuckets {
		return int64(i)
	}
	exp := i/subBuckets - 1
	sub := int64(i%subBuckets + subBuckets)
	return (sub+1)<<exp - 1
}

// Record adds one duration.
func (h *Histogram) Record(d time.Duration) {
	v := d.Microseconds()
	h.mu.Lock()
	h.counts[bucketOf(v)]++
	h.total++
	h.sum += float64(v)
	h.min = min(h.min, v)
	h.max = max(h.max, v)
	h.mu.Unlock()
}

// Merge adds every value recorded in other
func (h *Histogram) Merge(other *Histogram) {
	other.mu.Lock()
	counts := append([]uint64(nil), other.counts...)
	total, sum, lo, hi := other.total, other.sum, other.min, other.max
	other.mu.Unlock()

	h.mu.Lock()
	defer h.mu.Unlock()
	for i, c := range counts {
		h.counts[i] += c
	}
	h.total += total
	h.sum += sum
	h.min = min(h.min, lo)
	h.max = max(h.max, hi)
}

// Count returns the number of recorded values.
func (h *Histogram) Count() uint64 {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.total
}

// Quantile returns the value below which a fraction q of the recorded values
// fall. It reports the top of the bucket, so it never understates latency.
func (h *Histogram) Quantile(q float64) time.Duration {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.quantileLocked(q)
}

func (h *Histogram) quantileLocked(q float64) time.Duration {
	if h.total == 0 {
		return 0
	}
	rank := uint64(math.Ceil(q * float64(h.total)))
	rank = min(max(rank, 1), h.total)
	var seen uint64
	for i, c := range h.counts {
		seen += c
		if seen >= rank {
			// The top of the bucket can exceed the largest value recorded.
			return time.Duration(min(bucketUpper(i), h.max)) * time.Microsecond
		}
	}
	return time.Duration(h.max) * time.Microsecond
}

// Summary is a histogram reduced to the numbers a report needs.
type Summary struct {
	Count uint64  `json:"count"`
	MinMs float64 `json:"min_ms"`
	P50Ms float64 `json:"p50_ms"`
	P90Ms float64 `json:"p90_ms"`
	P95Ms float64 `json:"p95_ms"`
	P99Ms float64 `json:"p99_ms"`
	P999  float64 `json:"p99_9_ms"`
	MaxMs float64 `json:"max_ms"`
	// Mean is included for comparison only; the percentiles are what matter.
	MeanMs float64 `json:"mean_ms"`
}

// Summarize computes the percentiles.
func (h *Histogram) Summarize() Summary {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.total == 0 {
		return Summary{}
	}
	ms := func(d time.Duration) float64 { return float64(d.Microseconds()) / 1000 }
	return Summary{
		Count:  h.total,
		MinMs:  float64(h.min) / 1000,
		P50Ms:  ms(h.quantileLocked(0.50)),
		P90Ms:  ms(h.quantileLocked(0.90)),
		P95Ms:  ms(h.quantileLocked(0.95)),
		P99Ms:  ms(h.quantileLocked(0.99)),
		P999:   ms(h.quantileLocked(0.999)),
		MaxMs:  float64(h.max) / 1000,
		MeanMs: h.sum / float64(h.total) / 1000,
	}
}
