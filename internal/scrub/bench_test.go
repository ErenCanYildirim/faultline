package scrub

import (
	"fmt"
	"testing"

	"github.com/erencanyildirim/faultline/event"
)

// loadgenEvent looks like what the load generator sends: a message with
// numbers, a few frames with paths, four short tags, a user id and,
// for a third of users, an email.
func loadgenEvent(i int) event.Event {
	e := event.Event{
		ProjectID: "load-1",
		Exception: &event.Exception{
			Type:  "*errors.errorString",
			Value: fmt.Sprintf("bug aab: payment capture failed for order %d after %dms", 100000+i, 5+i%3000),
			Frames: []event.Frame{
				{Module: "github.com/erencanyildirim/faultline/internal/loadgen", Function: "(*app).captureOne", File: "C:/Users/jdoe/src/internal/loadgen/app.go", Line: 161, InApp: true},
				{Module: "github.com/erencanyildirim/faultline/internal/loadgen", Function: "(*app).run", File: "C:/Users/jdoe/src/internal/loadgen/app.go", Line: 137, InApp: true},
				{Module: "runtime", Function: "goexit", File: "C:/Program Files/Go/src/runtime/asm_amd64.s", Line: 1700},
			},
		},
		Tags: map[string]string{"run_id": "run-1029-100658-okfm", "seq": fmt.Sprint(i), "app": "app-007", "bug": "aab"},
		User: &event.User{ID: fmt.Sprintf("app-007-user-%d", i%1000)},
	}
	if i%3 == 0 {
		e.User.Email = fmt.Sprintf("user%d@example.com", i%1000)
	}
	return e
}

// The worker's workload: events as the load generator sends them.
func BenchmarkScrubLoadgenEvent(b *testing.B) {
	events := make([]event.Event, 1000)
	for i := range events {
		events[i] = loadgenEvent(i)
	}
	s := New(nil)
	b.ReportAllocs()
	b.ResetTimer()
	for i := range b.N {
		e := events[i%len(events)]
		// Fresh copies: the scrubber writes in place.
		ex := *e.Exception
		ex.Frames = append([]event.Frame(nil), e.Exception.Frames...)
		e.Exception = &ex
		tags := make(map[string]string, len(e.Tags))
		for k, v := range e.Tags {
			tags[k] = v
		}
		e.Tags = tags
		u := *e.User
		e.User = &u
		s.Event(&e)
	}
}

// A string with nothing to scrub, the common case.
func BenchmarkScrubTextClean(b *testing.B) {
	b.ReportAllocs()
	for range b.N {
		scrubText("bug aab: payment capture failed for order 104233 after 1830ms", false)
	}
}

// A string with something in every category.
func BenchmarkScrubTextDirty(b *testing.B) {
	in := "password=hunter2 Bearer abcdefgh12345 jdoe@example.com 4111 1111 1111 1111 10.0.0.1:443 2001:db8::1 C:/Users/jdoe/x"
	b.ReportAllocs()
	for range b.N {
		scrubText(in, false)
	}
}
