// Package grouping decides which events are the same problem.
//
// Every event gets a fingerprint; events with the same fingerprint in the
// same project form one issue. The algorithm, in order of precedence:
//
//  1. Custom: the SDK sent fingerprint parts. They are used as given; the
//     part "{{default}}" stands for what the default algorithm would use.
//  2. Stack: the event has an exception with frames. The fingerprint is the
//     exception type plus the innermost StackDepth in-app frames, each
//     reduced to module and normalized function name. For generic error
//     types (plain errors.New text, fmt wrappers, panics with a string) the
//     normalized message is added, because the type says nothing.
//  3. Exception without frames: type plus normalized message.
//  4. Message only: the normalized message.
//
// What is deliberately ignored: line numbers and file paths (they change
// with unrelated edits and differ between build machines), frames outside
// the application, release, environment, tags and user.
//
// The algorithm is versioned. Changing it changes fingerprints, which splits
// every existing issue into an old and a new one; a golden test guards
// against doing that by accident.
package grouping

import (
	"crypto/sha256"
	"encoding/hex"
	"strings"

	"github.com/erencanyildirim/faultline/event"
)

// Version is mixed into every hash. Bump it on any change to the algorithm.
const Version = "v1"

// StackDepth is how many in-app frames, innermost first, identify an issue.
//
// One frame merges unrelated bugs that fail inside the same helper. The
// whole stack splits one bug into many issues when it is reached through
// different routes or when anything far up the stack changes. Three frames,
// the failing function, its caller and one more, distinguish bugs without
// being brittle.
const StackDepth = 3

// DefaultPlaceholder in a custom fingerprint expands to the default
// components, so an SDK can refine grouping instead of replacing it:
// ["{{default}}", tenantID] splits each issue per tenant.
const DefaultPlaceholder = "{{default}}"

// Method says which rule produced a fingerprint.
type Method string

const (
	MethodCustom    Method = "custom"
	MethodStack     Method = "stack"
	MethodException Method = "exception"
	MethodMessage   Method = "message"
)

// Result is a fingerprint and how it was computed.
type Result struct {
	// Fingerprint is 32 hex characters (128 bits of SHA-256).
	Fingerprint string
	Method      Method
	// Components are what was hashed, readable. They explain why two
	// events did or did not group together.
	Components []string
}

// genericTypes carry no information about what went wrong; for them the
// message is the identity of the error.
var genericTypes = map[string]bool{
	"*errors.errorString": true,
	"*fmt.wrapError":      true,
	"*fmt.wrapErrors":     true,
	"*errors.joinError":   true,
	"string":              true, // panic("some text")
}

// Fingerprint computes the grouping fingerprint of e.
func Fingerprint(e *event.Event) Result {
	def := defaultComponents(e)
	if len(e.Fingerprint) == 0 {
		return finish(def.method, def.components)
	}

	var comps []string
	for _, part := range e.Fingerprint {
		if part == DefaultPlaceholder {
			comps = append(comps, def.components...)
			continue
		}
		comps = append(comps, "custom:"+part)
	}
	return finish(MethodCustom, comps)
}

type components struct {
	method     Method
	components []string
}

func defaultComponents(e *event.Event) components {
	ex := e.Exception
	if ex == nil {
		return components{MethodMessage, []string{"message:" + NormalizeMessage(e.Message)}}
	}

	comps := []string{"type:" + ex.Type}
	if genericTypes[ex.Type] {
		comps = append(comps, "value:"+NormalizeMessage(ex.Value))
	}

	frames := significantFrames(ex.Frames)
	if len(frames) == 0 {
		// An exception without a usable stack: the type alone would merge
		// unrelated errors, so the message joins it even for specific types.
		if !genericTypes[ex.Type] {
			comps = append(comps, "value:"+NormalizeMessage(ex.Value))
		}
		return components{MethodException, comps}
	}
	for _, f := range frames {
		comps = append(comps, "frame:"+f)
	}
	return components{MethodStack, comps}
}

// significantFrames returns up to StackDepth normalized frames, innermost
// first: in-app frames if there are any, otherwise all frames except the
// Go runtime's own (runtime.main, runtime.goexit are on every stack).
// Consecutive identical frames are collapsed, so recursion depth doesn't
// split an issue.
func significantFrames(frames []event.Frame) []string {
	pick := func(inAppOnly bool) []string {
		var out []string
		for _, f := range frames {
			if inAppOnly && !f.InApp {
				continue
			}
			if !inAppOnly && f.Module == "runtime" {
				continue
			}
			name := f.Module + "." + NormalizeFunction(f.Function)
			if n := len(out); n > 0 && out[n-1] == name {
				continue // recursion
			}
			out = append(out, name)
			if len(out) == StackDepth {
				break
			}
		}
		return out
	}
	if out := pick(true); len(out) > 0 {
		return out
	}
	return pick(false)
}

func finish(m Method, comps []string) Result {
	h := sha256.New()
	h.Write([]byte(Version))
	for _, c := range comps {
		h.Write([]byte{0}) // separator that can't appear in the components
		h.Write([]byte(c))
	}
	sum := h.Sum(nil)
	return Result{Fingerprint: hex.EncodeToString(sum[:16]), Method: m, Components: comps}
}

// Grouped is an event together with its computed fingerprint, as the worker
// hands it to storage.
type Grouped struct {
	Event       event.Event
	Fingerprint string
}

// Group computes the fingerprint of every event.
func Group(events []event.Event) []Grouped {
	out := make([]Grouped, len(events))
	for i := range events {
		out[i] = Grouped{Event: events[i], Fingerprint: Fingerprint(&events[i]).Fingerprint}
	}
	return out
}

// Explain renders a result for logs and test failures.
func (r Result) Explain() string {
	return string(r.Method) + " " + r.Fingerprint + " [" + strings.Join(r.Components, " | ") + "]"
}
