package loadgen

import (
	"errors"
	"fmt"
	"math/rand"
)

// Bugs picks which error each simulated event reports, following a Zipf
// distribution: bug 0 is the most common, bug 1 about half as common (for
// s=1), and so on down a long tail of rare bugs. Real error streams look
// like this: a handful of issues make up most events.
//
// The skew matters for the pipeline. The issues table and its views see
// a few hot keys updated constantly and many cold ones rarely; uniform
// random errors would hide that pattern.
type Bugs struct {
	n    int
	zipf *rand.Zipf
	r    *rand.Rand
}

// NewBugs creates a picker over n bugs with exponent s (> 1; larger is more
// skewed). Not safe for concurrent use: each simulated app has its own.
func NewBugs(n int, s float64, seed int64) (*Bugs, error) {
	if n < 1 {
		return nil, errors.New("loadgen: need at least one bug")
	}
	if s <= 1 {
		return nil, fmt.Errorf("loadgen: zipf exponent must be greater than 1, got %v", s)
	}
	r := rand.New(rand.NewSource(seed))
	return &Bugs{n: n, zipf: rand.NewZipf(r, s, 1, uint64(n-1)), r: r}, nil
}

// Next returns the index of the next bug.
func (b *Bugs) Next() int { return int(b.zipf.Uint64()) }

// operations give the bugs varied, realistic messages.
var operations = []string{
	"checkout", "payment capture", "inventory sync", "login", "search query",
	"image resize", "webhook delivery", "invoice render", "cache refresh", "report export",
}

// BugName is a stable, letters-only name for bug i ("aaa", "aab", ...).
// Letters only, because the grouping normalizer replaces numbers in
// messages: "bug 17" and "bug 18" would become the same issue.
func BugName(i int) string {
	b := []byte{'a', 'a', 'a'}
	for pos := 2; pos >= 0 && i > 0; pos-- {
		b[pos] = byte('a' + i%26)
		i /= 26
	}
	return string(b)
}

// Error builds bug i's error. The order number and duration change on every
// call, as they would in a real app; the grouping normalizer must still put
// every occurrence of bug i into one issue.
func (b *Bugs) Error(i int) error {
	return fmt.Errorf("bug %s: %s failed for order %d after %dms",
		BugName(i), operations[i%len(operations)], b.r.Intn(1_000_000), 5+b.r.Intn(3000))
}
