package grouping

import (
	"fmt"
	"strings"
	"testing"

	"github.com/erencanyildirim/faultline/event"
)

// ---- event builders ---------------------------------------------------------

func app(module, function string, line int) event.Frame {
	return event.Frame{Module: module, Function: function, File: "/app/" + module + ".go", Line: line, InApp: true}
}

func lib(module, function string, line int) event.Frame {
	return event.Frame{Module: module, Function: function, File: "/go/pkg/mod/" + module + ".go", Line: line}
}

func exc(typ, value string, frames ...event.Frame) *event.Event {
	return &event.Event{
		Level:     event.LevelError,
		Exception: &event.Exception{Type: typ, Value: value, Frames: frames},
		Release:   "shop@1.0.0",
	}
}

func msg(text string) *event.Event {
	return &event.Event{Level: event.LevelWarning, Message: text}
}

func with(e *event.Event, change func(e *event.Event)) *event.Event {
	change(e)
	return e
}

// A typical stack, innermost first: the failing function, its callers in the
// app, then the HTTP server and the Go runtime.
func checkoutStack() []event.Frame {
	return []event.Frame{
		app("shop/cart", "(*Store).Load", 88),
		app("shop/cart", "(*Cart).Checkout", 142),
		app("shop/api", "(*Handler).PostCheckout", 57),
		app("shop/api", "Router.func1", 20),
		lib("net/http", "HandlerFunc.ServeHTTP", 2220),
		lib("net/http", "(*conn).serve", 2092),
		lib("runtime", "goexit", 1700),
	}
}

func pathErr(frames []event.Frame) *event.Event {
	return exc("*fs.PathError", "open /var/data/cart-8812.json: no such file or directory", frames...)
}

// ---- the pair table ---------------------------------------------------------

type pair struct {
	name string
	why  string
	a, b *event.Event
}

// Pairs that must land in the same issue.
var mustGroup = []pair{
	{
		name: "line numbers differ",
		why:  "a code edit above the failing line moves it; it is still the same bug",
		a:    pathErr(checkoutStack()),
		b: pathErr(func() []event.Frame {
			s := checkoutStack()
			s[0].Line, s[1].Line = 95, 150
			return s
		}()),
	},
	{
		name: "file paths differ",
		why:  "the same code built on Windows and in Docker has different absolute paths",
		a:    pathErr(checkoutStack()),
		b: pathErr(func() []event.Frame {
			s := checkoutStack()
			for i := range s {
				s[i].File = `C:/Users/someone/src/` + s[i].Module + ".go"
			}
			return s
		}()),
	},
	{
		name: "closure renumbered",
		why:  "adding a closure earlier in Router turns func1 into func2 without touching this code path",
		a:    pathErr(checkoutStack()),
		b: pathErr(func() []event.Frame {
			s := checkoutStack()
			s[3].Function = "Router.func2"
			return s
		}()),
	},
	{
		name: "method value wrapper",
		why:  "passing h.PostCheckout as a func value makes the compiler add a -fm wrapper frame name",
		a:    pathErr(checkoutStack()),
		b: pathErr(func() []event.Frame {
			s := checkoutStack()
			s[2].Function = "(*Handler).PostCheckout-fm"
			return s
		}()),
	},
	{
		name: "different library frames",
		why:  "a Go upgrade or a different router changes non-app frames; the app's bug is the same",
		a:    pathErr(checkoutStack()),
		b: pathErr(func() []event.Frame {
			s := checkoutStack()[:4]
			return append(s, lib("github.com/go-chi/chi/v5", "(*Mux).ServeHTTP", 90), lib("runtime", "goexit", 1650))
		}()),
	},
	{
		name: "different frames beyond depth 3",
		why:  "only the 3 innermost app frames count; how the request reached them further up doesn't",
		a:    pathErr(checkoutStack()),
		b: pathErr(func() []event.Frame {
			s := checkoutStack()
			s[3] = app("shop/jobs", "(*Retrier).Run", 33) // 4th app frame
			return s
		}()),
	},
	{
		name: "recursion depth differs",
		why:  "a recursive walk that fails at depth 3 or depth 7 is the same bug",
		a: exc("*json.SyntaxError", "invalid character",
			app("shop/tree", "walk", 10), app("shop/tree", "walk", 12), app("shop/tree", "walk", 12),
			app("shop/tree", "Parse", 40), app("shop/api", "(*Handler).Import", 9)),
		b: exc("*json.SyntaxError", "invalid character",
			app("shop/tree", "walk", 10), app("shop/tree", "walk", 12), app("shop/tree", "walk", 12),
			app("shop/tree", "walk", 12), app("shop/tree", "walk", 12), app("shop/tree", "walk", 12),
			app("shop/tree", "Parse", 40), app("shop/api", "(*Handler).Import", 9)),
	},
	{
		name: "specific type, different value",
		why:  "for a specific error type, the type and stack identify the bug; the path in the message is data",
		a:    exc("*fs.PathError", "open /var/data/cart-1.json: no such file", checkoutStack()...),
		b:    exc("*fs.PathError", "open /tmp/other.json: permission denied", checkoutStack()...),
	},
	{
		name: "generic error, numbers in message differ",
		why:  "errors.New-style text with a different user ID is the same problem",
		a:    exc("*errors.errorString", "user 42 not found", checkoutStack()...),
		b:    exc("*errors.errorString", "user 9001 not found", checkoutStack()...),
	},
	{
		name: "wrapped error, IDs and quoted values differ",
		why:  "fmt.Errorf wrappers put request data into the text; normalization removes it",
		a: exc("*fmt.wrapError", `checkout 0192f3c4-8b2a-7c1d-9e4f-5a6b7c8d9e0f: sku "A-1" out of stock`,
			checkoutStack()...),
		b: exc("*fmt.wrapError", `checkout 3b241101-e2bb-4255-8caf-4136c566a962: sku "B-77" out of stock`,
			checkoutStack()...),
	},
	{
		name: "panic, different addresses",
		why:  "a nil dereference reports a different address every run",
		a:    exc("runtime.errorString", "invalid memory address 0xc000123456", checkoutStack()...),
		b:    exc("runtime.errorString", "invalid memory address 0xc0009abcde", checkoutStack()...),
	},
	{
		name: "message only, numbers differ",
		why:  "a timing warning with different durations and hosts is one recurring problem",
		a:    msg("payment provider slow: 2300ms from 10.0.0.12:443"),
		b:    msg("payment provider slow: 512ms from 10.0.0.7:443"),
	},
	{
		name: "message only, hash in text",
		why:  "a commit hash or trace ID in the message is data, not identity",
		a:    msg("cache miss for key 9f86d081884c7d65"),
		b:    msg("cache miss for key 2c26b46b68ffc68f"),
	},
	{
		name: "context differs",
		why:  "release, environment, tags and user describe where it happened, not what happened",
		a:    pathErr(checkoutStack()),
		b: with(pathErr(checkoutStack()), func(e *event.Event) {
			e.Release, e.Environment = "shop@2.0.0", "staging"
			e.Tags = map[string]string{"region": "us"}
			e.User = &event.User{ID: "u2"}
		}),
	},
	{
		name: "custom fingerprint, different stacks",
		why:  "the app said these belong together; that overrides the stack",
		a:    with(pathErr(checkoutStack()), func(e *event.Event) { e.Fingerprint = []string{"payment-timeout"} }),
		b: with(exc("*net.OpError", "dial tcp: timeout", app("shop/pay", "Charge", 3)),
			func(e *event.Event) { e.Fingerprint = []string{"payment-timeout"} }),
	},
	{
		name: "{{default}} alone equals default",
		why:  `["{{default}}"] means "group normally", so an SDK can always send it`,
		a:    pathErr(checkoutStack()),
		b:    with(pathErr(checkoutStack()), func(e *event.Event) { e.Fingerprint = []string{"{{default}}"} }),
	},
	{
		name: "custom with {{default}}, same tenant",
		why:  "{{default}} keeps normal grouping; the same tenant stays in one issue",
		a:    with(pathErr(checkoutStack()), func(e *event.Event) { e.Fingerprint = []string{"{{default}}", "tenant-a"} }),
		b: with(pathErr(func() []event.Frame { s := checkoutStack(); s[0].Line = 99; return s }()),
			func(e *event.Event) { e.Fingerprint = []string{"{{default}}", "tenant-a"} }),
	},
}

// Pairs that must land in different issues.
var mustSplit = []pair{
	{
		name: "different failing function",
		why:  "the same error type raised in different code is a different bug",
		a:    pathErr(checkoutStack()),
		b: pathErr(func() []event.Frame {
			s := checkoutStack()
			s[0] = app("shop/cart", "(*Store).Save", 120)
			return s
		}()),
	},
	{
		name: "different error type",
		why:  "a timeout and a missing file at the same line are different problems",
		a:    pathErr(checkoutStack()),
		b:    exc("*net.OpError", "dial tcp: i/o timeout", checkoutStack()...),
	},
	{
		name: "shared helper, different caller",
		why:  "with depth 3 the caller counts, so a helper failing for two features is two issues",
		a: exc("*pq.Error", "deadlock detected",
			app("shop/db", "mustExec", 10), app("shop/orders", "Create", 50), app("shop/api", "PostOrder", 5)),
		b: exc("*pq.Error", "deadlock detected",
			app("shop/db", "mustExec", 10), app("shop/stock", "Reserve", 70), app("shop/api", "PostOrder", 5)),
	},
	{
		name: "generic errors at one capture site",
		why: "apps that capture in a central handler give every error the same frames; " +
			"for plain errors the message must tell them apart",
		a: exc("*errors.errorString", "sql: no rows in result set", app("shop/api", "handleError", 12)),
		b: exc("*errors.errorString", "redis: connection refused", app("shop/api", "handleError", 12)),
	},
	{
		name: "panic with string, different text",
		why:  `panic("...") has type string, which says nothing; the text identifies it`,
		a:    exc("string", "invariant violated: negative stock", checkoutStack()...),
		b:    exc("string", "unreachable: unknown order state", checkoutStack()...),
	},
	{
		name: "specific type without frames, different value",
		why:  "with no stack to tell them apart, the message is all that is left",
		a:    exc("*net.OpError", "dial tcp 10.0.0.1:5432: connection refused"),
		b:    exc("*net.OpError", "read udp: i/o timeout"),
	},
	{
		name: "message only, different text",
		why:  "different words are different problems",
		a:    msg("payment provider slow: 2300ms"),
		b:    msg("inventory sync lagging: 2300ms"),
	},
	{
		name: "message vs exception",
		why:  "a log message and an error with the same text come from different code",
		a:    msg("user 42 not found"),
		b:    exc("*errors.errorString", "user 42 not found"),
	},
	{
		name: "custom fingerprint, different parts",
		why:  "the app asked to separate them, even though the stack is identical",
		a:    with(pathErr(checkoutStack()), func(e *event.Event) { e.Fingerprint = []string{"tenant-a"} }),
		b:    with(pathErr(checkoutStack()), func(e *event.Event) { e.Fingerprint = []string{"tenant-b"} }),
	},
	{
		name: "custom with {{default}}, different tenant",
		why:  "{{default}} plus a tenant splits each normal issue per tenant",
		a:    with(pathErr(checkoutStack()), func(e *event.Event) { e.Fingerprint = []string{"{{default}}", "tenant-a"} }),
		b:    with(pathErr(checkoutStack()), func(e *event.Event) { e.Fingerprint = []string{"{{default}}", "tenant-b"} }),
	},
	{
		name: "custom part that looks like a default component",
		why:  "custom parts are prefixed before hashing, so app text can't collide with the default grouping",
		a:    pathErr(checkoutStack()),
		b: with(pathErr(checkoutStack()), func(e *event.Event) {
			e.Fingerprint = []string{"type:*fs.PathError", "frame:shop/cart.(*Store).Load",
				"frame:shop/cart.(*Cart).Checkout", "frame:shop/api.(*Handler).PostCheckout"}
		}),
	},
	{
		name: "no app frames, different library code",
		why:  "when nothing is marked in-app, library frames are better than nothing",
		a:    exc("*json.SyntaxError", "x", lib("encoding/json", "(*decodeState).object", 1), lib("runtime", "goexit", 1)),
		b:    exc("*json.SyntaxError", "x", lib("encoding/xml", "(*Decoder).Token", 1), lib("runtime", "goexit", 1)),
	},
}

func TestEventsThatMustGroupTogether(t *testing.T) {
	for _, p := range mustGroup {
		t.Run(p.name, func(t *testing.T) {
			ra, rb := Fingerprint(p.a), Fingerprint(p.b)
			if ra.Fingerprint != rb.Fingerprint {
				t.Errorf("should group together: %s\n  a: %s\n  b: %s", p.why, ra.Explain(), rb.Explain())
			}
		})
	}
}

func TestEventsThatMustBeSeparate(t *testing.T) {
	for _, p := range mustSplit {
		t.Run(p.name, func(t *testing.T) {
			ra, rb := Fingerprint(p.a), Fingerprint(p.b)
			if ra.Fingerprint == rb.Fingerprint {
				t.Errorf("should be separate issues: %s\n  a: %s\n  b: %s", p.why, ra.Explain(), rb.Explain())
			}
		})
	}
}

// TestGroupingReport prints the whole table with reasons. Run it with
//
//	go test -v -run GroupingReport ./internal/grouping/
//
// to see every rule and why it exists.
func TestGroupingReport(t *testing.T) {
	var b strings.Builder
	fmt.Fprintf(&b, "\nMUST GROUP TOGETHER (%d)\n", len(mustGroup))
	for _, p := range mustGroup {
		fmt.Fprintf(&b, "  %-48s %s\n", p.name, p.why)
	}
	fmt.Fprintf(&b, "\nMUST BE SEPARATE (%d)\n", len(mustSplit))
	for _, p := range mustSplit {
		fmt.Fprintf(&b, "  %-48s %s\n", p.name, p.why)
	}
	t.Log(b.String())
}

func TestMethodAndComponents(t *testing.T) {
	r := Fingerprint(pathErr(checkoutStack()))
	want := []string{
		"type:*fs.PathError",
		"frame:shop/cart.(*Store).Load",
		"frame:shop/cart.(*Cart).Checkout",
		"frame:shop/api.(*Handler).PostCheckout",
	}
	if r.Method != MethodStack || strings.Join(r.Components, "|") != strings.Join(want, "|") {
		t.Errorf("got %s\nwant components %v", r.Explain(), want)
	}
	if len(r.Fingerprint) != 32 {
		t.Errorf("fingerprint %q is not 32 hex characters", r.Fingerprint)
	}

	if r := Fingerprint(msg("hello 42")); r.Method != MethodMessage || r.Components[0] != "message:hello <n>" {
		t.Errorf("message event: %s", r.Explain())
	}
	if r := Fingerprint(exc("*net.OpError", "refused")); r.Method != MethodException {
		t.Errorf("exception without frames: %s", r.Explain())
	}
	custom := with(pathErr(checkoutStack()), func(e *event.Event) { e.Fingerprint = []string{"{{default}}", "t1"} })
	if r := Fingerprint(custom); r.Method != MethodCustom || len(r.Components) != 5 || r.Components[4] != "custom:t1" {
		t.Errorf("custom with default: %s", r.Explain())
	}
}

// The golden fingerprint pins the algorithm. If this fails, every existing
// issue would split in two after deploying the change. Either the change was
// unintended, or it is deliberate and Version must be bumped (and the
// golden values updated) with a note in the decisions log.
func TestGoldenFingerprints(t *testing.T) {
	golden := map[string]struct {
		e    *event.Event
		want string
	}{
		// Independently computed: first 16 bytes of SHA-256 over "v1", then
		// each component behind a zero byte (see finish in grouping.go).
		"stack":   {pathErr(checkoutStack()), "210c36e6aa46c0c43db8d4a543fecb27"},
		"message": {msg("payment provider slow: 2300ms"), "f54da1ea09b625603a0a31c4d45ca798"},
	}
	for name, g := range golden {
		if got := Fingerprint(g.e).Fingerprint; got != g.want {
			t.Errorf("%s: fingerprint changed from %s to %s; see the comment on this test", name, g.want, got)
		}
	}
}
