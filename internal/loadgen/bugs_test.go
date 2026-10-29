package loadgen

import (
	"testing"

	"github.com/erencanyildirim/faultline/event"
	"github.com/erencanyildirim/faultline/internal/grouping"
)

func TestBugNames(t *testing.T) {
	seen := map[string]bool{}
	for i := range 26 * 26 * 26 {
		n := BugName(i)
		if seen[n] {
			t.Fatalf("BugName(%d) = %q repeats", i, n)
		}
		seen[n] = true
	}
	if BugName(0) != "aaa" || BugName(1) != "aab" || BugName(26) != "aba" {
		t.Errorf("names: %s %s %s", BugName(0), BugName(1), BugName(26))
	}
}

// Every occurrence of a bug must land in one issue, and different bugs in
// different issues, or the loss and issue counts of a run mean nothing.
func TestEachBugIsOneIssue(t *testing.T) {
	b, err := NewBugs(300, 1.2, 1)
	if err != nil {
		t.Fatal(err)
	}
	frames := []event.Frame{{Module: "main", Function: "handle", InApp: true}}
	fp := func(i int) string {
		err := b.Error(i)
		e := &event.Event{Exception: &event.Exception{Type: "*errors.errorString", Value: err.Error(), Frames: frames}}
		return grouping.Fingerprint(e).Fingerprint
	}
	owner := map[string]int{}
	for i := range 300 {
		f := fp(i)
		for range 5 {
			if again := fp(i); again != f {
				t.Fatalf("bug %d produced two fingerprints: %q", i, b.Error(i))
			}
		}
		if other, dup := owner[f]; dup {
			t.Fatalf("bugs %d and %d share a fingerprint", other, i)
		}
		owner[f] = i
	}
}

// The distribution is skewed: few bugs carry most events, many are rare.
func TestBugsAreSkewed(t *testing.T) {
	b, _ := NewBugs(200, 1.2, 7)
	counts := make([]int, 200)
	const n = 100_000
	for range n {
		counts[b.Next()]++
	}
	top5 := counts[0] + counts[1] + counts[2] + counts[3] + counts[4]
	if share := float64(top5) / n; share < 0.5 {
		t.Errorf("top 5 bugs have %.0f%% of events, want a majority", share*100)
	}
	if counts[0] <= counts[1] || counts[1] <= counts[10] {
		t.Errorf("not decreasing: %d %d %d", counts[0], counts[1], counts[10])
	}
	seen := 0
	for _, c := range counts {
		if c > 0 {
			seen++
		}
	}
	if seen < 150 {
		t.Errorf("only %d of 200 bugs occurred; the tail should still show up", seen)
	}
}

func TestNewBugsValidates(t *testing.T) {
	if _, err := NewBugs(0, 1.2, 1); err == nil {
		t.Error("0 bugs should fail")
	}
	if _, err := NewBugs(10, 1, 1); err == nil {
		t.Error("exponent 1 should fail")
	}
}
