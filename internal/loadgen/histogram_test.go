package loadgen

import (
	"math"
	"math/rand/v2"
	"slices"
	"testing"
	"time"
)

func TestBucketsCoverEveryValueOnce(t *testing.T) {
	// Each value falls into the bucket whose upper bound is the first one
	// at or above it, and bucket bounds only increase.
	prev := int64(-1)
	for i := range histogramSlots - 1 {
		up := bucketUpper(i)
		if up <= prev {
			t.Fatalf("bucket %d upper %d not above previous %d", i, up, prev)
		}
		if got := bucketOf(up); got != i {
			t.Fatalf("bucketOf(%d) = %d, want %d", up, got, i)
		}
		if got := bucketOf(prev + 1); got != i {
			t.Fatalf("bucketOf(%d) = %d, want %d", prev+1, got, i)
		}
		prev = up
	}
}

func TestBucketWidthWithinOnePercent(t *testing.T) {
	for i := subBuckets; i < histogramSlots-1; i++ {
		lower := bucketUpper(i-1) + 1
		width := bucketUpper(i) - lower + 1
		if rel := float64(width) / float64(lower); rel > 1.0/subBuckets+1e-9 {
			t.Fatalf("bucket %d [%d..%d] is %.2f%% wide", i, lower, bucketUpper(i), rel*100)
		}
	}
}

// Against exact percentiles of a skewed (log-normal) sample, like real
// latencies: mostly fast, a long slow tail.
func TestQuantilesMatchExactWithinOnePercent(t *testing.T) {
	r := rand.New(rand.NewPCG(1, 2))
	h := NewHistogram()
	var vals []int64
	for range 200_000 {
		us := int64(math.Exp(r.NormFloat64()*1.2 + 8)) // median ~3ms, tail to seconds
		vals = append(vals, us)
		h.Record(time.Duration(us) * time.Microsecond)
	}
	slices.Sort(vals)
	for _, q := range []float64{0.5, 0.9, 0.95, 0.99, 0.999, 1} {
		idx := int(math.Ceil(q*float64(len(vals)))) - 1
		exact := vals[idx]
		got := h.Quantile(q).Microseconds()
		if got < exact {
			t.Errorf("q%.3f = %dµs, below exact %dµs; must never understate", q, got, exact)
		}
		if rel := float64(got-exact) / float64(exact); rel > 0.01 {
			t.Errorf("q%.3f = %dµs, exact %dµs, off by %.2f%%", q, got, exact, rel*100)
		}
	}
}

func TestMergeAndSummary(t *testing.T) {
	a, b := NewHistogram(), NewHistogram()
	for i := 1; i <= 100; i++ {
		a.Record(time.Duration(i) * time.Millisecond)
	}
	b.Record(10 * time.Second)
	a.Merge(b)
	s := a.Summarize()
	if s.Count != 101 || s.MinMs != 1 || s.MaxMs != 10000 {
		t.Errorf("summary = %+v", s)
	}
	if s.P50Ms < 51 || s.P50Ms > 51.5 {
		t.Errorf("p50 = %v, want about 51ms", s.P50Ms)
	}
	// One slow request in 101 is the p99.9 and the max, not the p99.
	if s.P99Ms > 101 || s.P999 != 10000 {
		t.Errorf("p99 = %v, p99.9 = %v", s.P99Ms, s.P999)
	}
	if (NewHistogram().Summarize() != Summary{}) {
		t.Error("empty histogram should summarize to zeros")
	}
}
