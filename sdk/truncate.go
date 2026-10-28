package faultline

import (
	"encoding/json"
	"sort"
	"unicode/utf8"

	"github.com/erencanyildirim/faultline/event"
)

// truncate cuts s to at most max bytes without splitting a UTF-8 character.
func truncate(s string, max int) string {
	if len(s) <= max {
		return s
	}
	cut := max
	for cut > 0 && !utf8.RuneStart(s[cut]) {
		cut--
	}
	return s[:cut]
}

// enforceFieldLimits truncates every field to the limits in event/limits.go,
// so ingest never rejects an event this SDK built.
func enforceFieldLimits(e *event.Event) {
	e.Message = truncate(e.Message, event.MaxMessageBytes)
	e.Release = truncate(e.Release, event.MaxReleaseBytes)
	e.Environment = truncate(e.Environment, event.MaxEnvironmentBytes)
	e.SDK.Name = truncate(e.SDK.Name, event.MaxSDKFieldBytes)
	e.SDK.Version = truncate(e.SDK.Version, event.MaxSDKFieldBytes)

	if ex := e.Exception; ex != nil {
		ex.Type = truncate(ex.Type, event.MaxExceptionTypeBytes)
		ex.Value = truncate(ex.Value, event.MaxExceptionValueBytes)
		if len(ex.Frames) > event.MaxFrames {
			ex.Frames = ex.Frames[:event.MaxFrames] // innermost frames are kept
		}
		for i := range ex.Frames {
			f := &ex.Frames[i]
			f.Module = truncate(f.Module, event.MaxFrameFieldBytes)
			f.Function = truncate(f.Function, event.MaxFrameFieldBytes)
			f.File = truncate(f.File, event.MaxFrameFieldBytes)
		}
	}

	if len(e.Tags) > 0 {
		e.Tags = limitTags(e.Tags)
	}

	if len(e.Fingerprint) > 0 {
		parts := e.Fingerprint[:0]
		for _, p := range e.Fingerprint {
			if p != "" && len(parts) < event.MaxFingerprintParts {
				parts = append(parts, truncate(p, event.MaxFingerprintPartBytes))
			}
		}
		e.Fingerprint = parts
	}

	if u := e.User; u != nil {
		u.ID = truncate(u.ID, event.MaxUserFieldBytes)
		u.Username = truncate(u.Username, event.MaxUserFieldBytes)
		u.Email = truncate(u.Email, event.MaxUserFieldBytes)
		u.IPAddress = truncate(u.IPAddress, event.MaxUserFieldBytes)
	}
}

// limitTags keeps at most MaxTags entries, choosing by sorted key so the
// result is the same on every run, and truncates keys and values.
func limitTags(in map[string]string) map[string]string {
	keys := make([]string, 0, len(in))
	for k := range in {
		if k != "" {
			keys = append(keys, k)
		}
	}
	sort.Strings(keys)
	if len(keys) > event.MaxTags {
		keys = keys[:event.MaxTags]
	}
	out := make(map[string]string, len(keys))
	for _, k := range keys {
		out[truncate(k, event.MaxTagKeyBytes)] = truncate(in[k], event.MaxTagValueBytes)
	}
	return out
}

// encodeWithinLimit encodes e and, if the result is over MaxEventBytes,
// shrinks it step by step: fewer frames first, then shorter texts. It returns
// ok=false only if the event still doesn't fit, which takes a pathological
// event; the caller drops it.
func encodeWithinLimit(e *event.Event) (data []byte, ok bool) {
	data, err := json.Marshal(e)
	if err != nil {
		return nil, false
	}
	for len(data) > event.MaxEventBytes && e.Exception != nil && len(e.Exception.Frames) > 1 {
		e.Exception.Frames = e.Exception.Frames[:len(e.Exception.Frames)/2]
		if data, err = json.Marshal(e); err != nil {
			return nil, false
		}
	}
	if len(data) > event.MaxEventBytes {
		e.Message = truncate(e.Message, 1024)
		if e.Exception != nil {
			e.Exception.Value = truncate(e.Exception.Value, 1024)
		}
		if data, err = json.Marshal(e); err != nil {
			return nil, false
		}
	}
	return data, len(data) <= event.MaxEventBytes
}
