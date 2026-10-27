package event

import (
	"strings"
	"testing"
	"time"
)

func validEvent() *Event {
	return &Event{
		EventID:   "0192f3c4-8b2a-7c1d-9e4f-5a6b7c8d9e0f",
		Timestamp: time.Date(2026, 10, 27, 9, 0, 0, 0, time.UTC),
		Level:     LevelError,
		Exception: &Exception{
			Type:   "*fs.PathError",
			Value:  "open x: no such file",
			Frames: []Frame{{Module: "main", Function: "main", InApp: true}},
		},
		SDK: SDKInfo{Name: "faultline-go", Version: "0.1.0"},
	}
}

func TestValidateAcceptsFixtures(t *testing.T) {
	for _, path := range []string{"testdata/event_full.json", "testdata/event_message_only.json"} {
		e := decodeStrict(t, path)
		if err := Validate(&e); err != nil {
			t.Errorf("%s: %v", path, err)
		}
	}
}

func TestValidateRejects(t *testing.T) {
	tests := []struct {
		name    string
		mutate  func(e *Event)
		wantErr string
	}{
		{"missing id", func(e *Event) { e.EventID = "" }, "event_id"},
		{"bad id", func(e *Event) { e.EventID = "0192f3c4-8b2a-7c1d-9e4f-5a6b7c8d9e0g" }, "event_id"},
		{"missing timestamp", func(e *Event) { e.Timestamp = time.Time{} }, "timestamp"},
		{"bad level", func(e *Event) { e.Level = "critical" }, "level"},
		{"no content", func(e *Event) { e.Exception = nil; e.Message = "" }, "message or exception"},
		{"long message", func(e *Event) { e.Message = strings.Repeat("x", MaxMessageBytes+1) }, "message is"},
		{"missing exception type", func(e *Event) { e.Exception.Type = "" }, "exception.type"},
		{"too many frames", func(e *Event) { e.Exception.Frames = make([]Frame, MaxFrames+1) }, "frames has"},
		{"frame without function", func(e *Event) { e.Exception.Frames[0].Function = "" }, "function is required"},
		{"long frame file", func(e *Event) { e.Exception.Frames[0].File = strings.Repeat("x", MaxFrameFieldBytes+1) }, "frames[0]"},
		{"negative line", func(e *Event) { e.Exception.Frames[0].Line = -1 }, "out of range"},
		{"line over uint32", func(e *Event) { e.Exception.Frames[0].Line = 1 << 33 }, "out of range"},
		{"too many tags", func(e *Event) {
			e.Tags = map[string]string{}
			for i := 0; i <= MaxTags; i++ {
				e.Tags[strings.Repeat("k", i+1)] = "v"
			}
		}, "tags has"},
		{"empty tag key", func(e *Event) { e.Tags = map[string]string{"": "v"} }, "tag keys"},
		{"long tag value", func(e *Event) { e.Tags = map[string]string{"k": strings.Repeat("v", MaxTagValueBytes+1)} }, "value"},
		{"long user email", func(e *Event) { e.User = &User{Email: strings.Repeat("e", MaxUserFieldBytes+1)} }, "user field"},
		{"missing sdk", func(e *Event) { e.SDK = SDKInfo{} }, "sdk.name"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			e := validEvent()
			tc.mutate(e)
			err := Validate(e)
			if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Errorf("err = %v, want containing %q", err, tc.wantErr)
			}
		})
	}
}

func TestValidateAcceptsUppercaseAndV4UUIDs(t *testing.T) {
	for _, id := range []string{"0192F3C4-8B2A-7C1D-9E4F-5A6B7C8D9E0F", "3b241101-e2bb-4255-8caf-4136c566a962"} {
		e := validEvent()
		e.EventID = id
		if err := Validate(e); err != nil {
			t.Errorf("%s: %v", id, err)
		}
	}
}
