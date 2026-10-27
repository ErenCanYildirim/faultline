package faultline

import (
	"errors"
	"fmt"
	"io/fs"
	"regexp"
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/erencanyildirim/faultline/event"
)

func TestParseDSN(t *testing.T) {
	tests := []struct {
		dsn      string
		endpoint string
		key      string
		wantErr  string
	}{
		{dsn: "http://abc123@localhost:8080", endpoint: "http://localhost:8080/api/v1/events", key: "abc123"},
		{dsn: "https://k@errors.example.com/proxy/", endpoint: "https://errors.example.com/proxy/api/v1/events", key: "k"},
		{dsn: "", wantErr: "empty"},
		{dsn: "ftp://k@host", wantErr: "scheme"},
		{dsn: "http://localhost:8080", wantErr: "no project key"},
		{dsn: "http://k:secret@host", wantErr: "password"},
		{dsn: "http://k@", wantErr: "no host"},
	}
	for _, tc := range tests {
		t.Run(tc.dsn, func(t *testing.T) {
			got, err := parseDSN(tc.dsn)
			if tc.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("err = %v, want containing %q", err, tc.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got.endpoint != tc.endpoint || got.key != tc.key {
				t.Errorf("got %+v, want endpoint %q key %q", got, tc.endpoint, tc.key)
			}
		})
	}
}

var uuidV7 = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`)

func TestNewEventIDIsUniqueV7(t *testing.T) {
	seen := make(map[string]bool, 10000)
	for range 10000 {
		id := newEventID()
		if !uuidV7.MatchString(id) {
			t.Fatalf("%q is not a UUIDv7", id)
		}
		if seen[id] {
			t.Fatalf("duplicate id %q", id)
		}
		seen[id] = true
	}
}

func TestSplitFunction(t *testing.T) {
	tests := []struct{ in, module, function string }{
		{"github.com/acme/shop/cart.(*Cart).Checkout", "github.com/acme/shop/cart", "(*Cart).Checkout"},
		{"main.main", "main", "main"},
		{"main.main.func1", "main", "main.func1"},
		{"net/http.HandlerFunc.ServeHTTP", "net/http", "HandlerFunc.ServeHTTP"},
		{"github.com/a/b.Map[...]", "github.com/a/b", "Map[...]"},
		{"weird", "", "weird"},
	}
	for _, tc := range tests {
		m, f := splitFunction(tc.in)
		if m != tc.module || f != tc.function {
			t.Errorf("splitFunction(%q) = %q, %q; want %q, %q", tc.in, m, f, tc.module, tc.function)
		}
	}
}

func TestTruncateKeepsValidUTF8(t *testing.T) {
	s := strings.Repeat("ü", 10) // 2 bytes each
	for max := 0; max <= len(s)+1; max++ {
		got := truncate(s, max)
		if len(got) > max {
			t.Fatalf("truncate(%d) returned %d bytes", max, len(got))
		}
		if !utf8.ValidString(got) {
			t.Fatalf("truncate(%d) split a character: %q", max, got)
		}
	}
}

func TestEnforceFieldLimits(t *testing.T) {
	frames := make([]event.Frame, 100)
	for i := range frames {
		frames[i] = event.Frame{Module: "m", Function: fmt.Sprintf("f%d", i), File: strings.Repeat("x", 1000)}
	}
	tags := make(map[string]string, 50)
	for i := range 50 {
		tags[fmt.Sprintf("tag%02d", i)] = strings.Repeat("v", 500)
	}
	e := &event.Event{
		Message:   strings.Repeat("m", 20000),
		Exception: &event.Exception{Type: "T", Value: strings.Repeat("v", 20000), Frames: frames},
		Tags:      tags,
		User:      &event.User{Email: strings.Repeat("e", 1000)},
	}
	enforceFieldLimits(e)

	if len(e.Message) != event.MaxMessageBytes {
		t.Errorf("message %d bytes", len(e.Message))
	}
	if len(e.Exception.Frames) != event.MaxFrames {
		t.Errorf("frames %d", len(e.Exception.Frames))
	}
	if e.Exception.Frames[0].Function != "f0" {
		t.Errorf("innermost frame not kept: %q", e.Exception.Frames[0].Function)
	}
	if len(e.Exception.Frames[0].File) != event.MaxFrameFieldBytes {
		t.Errorf("frame file %d bytes", len(e.Exception.Frames[0].File))
	}
	if len(e.Tags) != event.MaxTags {
		t.Errorf("tags %d", len(e.Tags))
	}
	if _, ok := e.Tags["tag00"]; !ok {
		t.Errorf("tags not chosen by sorted key")
	}
	if len(e.User.Email) != event.MaxUserFieldBytes {
		t.Errorf("email %d bytes", len(e.User.Email))
	}
}

func TestEncodeWithinLimitShrinksFrames(t *testing.T) {
	frames := make([]event.Frame, event.MaxFrames)
	long := strings.Repeat("x", event.MaxFrameFieldBytes)
	for i := range frames {
		frames[i] = event.Frame{Module: long, Function: long, File: long}
	}
	e := &event.Event{EventID: newEventID(), Exception: &event.Exception{Type: "T", Value: "v", Frames: frames}}

	data, ok := encodeWithinLimit(e)
	if !ok {
		t.Fatal("event should fit after shrinking")
	}
	if len(data) > event.MaxEventBytes {
		t.Fatalf("encoded %d bytes", len(data))
	}
	if n := len(e.Exception.Frames); n >= event.MaxFrames || n == 0 {
		t.Errorf("frames after shrink = %d", n)
	}
}

func TestErrorTypeFollowsUnwrapChain(t *testing.T) {
	root := &fs.PathError{Op: "open", Path: "/x", Err: fs.ErrNotExist}
	wrapped := fmt.Errorf("load cart: %w", fmt.Errorf("store: %w", root))
	if got := errorType(wrapped); got != "*fs.PathError" {
		t.Errorf("errorType = %q, want *fs.PathError", got)
	}
	if got := errorType(errors.New("plain")); got != "*errors.errorString" {
		t.Errorf("errorType = %q", got)
	}
	// Only generic types in the chain: fall back to the root.
	if got := errorType(fmt.Errorf("a: %w", errors.New("b"))); got != "*errors.errorString" {
		t.Errorf("errorType(all generic) = %q", got)
	}
	// A specific type wrapping another specific type: the deeper one wins.
	outer := &customErr{err: root}
	if got := errorType(outer); got != "*fs.PathError" {
		t.Errorf("errorType(custom wrapping PathError) = %q", got)
	}
	// A specific type with nothing specific beneath it keeps its own type.
	if got := errorType(&customErr{err: errors.New("x")}); got != "*faultline.customErr" {
		t.Errorf("errorType(custom) = %q", got)
	}
	// errors.Join returns Unwrap() []error, which errors.Unwrap does not follow.
	joined := errors.Join(root, errors.New("b"))
	if got := errorType(joined); got != "*errors.joinError" {
		t.Errorf("errorType(joined) = %q", got)
	}
}

type customErr struct{ err error }

func (c *customErr) Error() string { return "custom: " + c.err.Error() }
func (c *customErr) Unwrap() error { return c.err }

func TestPackageLevelFunctionsAreNoopsBeforeInit(t *testing.T) {
	prev := global.Swap(nil)
	t.Cleanup(func() { global.Store(prev) })

	if id := CaptureError(errors.New("x")); id != "" {
		t.Errorf("CaptureError before Init returned %q", id)
	}
	if id := CaptureMessage("x"); id != "" {
		t.Errorf("CaptureMessage before Init returned %q", id)
	}
	if !Flush(0) || !Close(0) {
		t.Error("Flush/Close before Init should report success")
	}
	if GetStats() != (Stats{}) {
		t.Error("stats before Init should be zero")
	}
}
