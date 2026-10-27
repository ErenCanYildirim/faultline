package event

import (
	"fmt"
	"math"
)

// Validate checks e against the contract and the hard limits. It returns the
// first problem found, worded for the client that sent the event.
//
// It does not check the encoded size (MaxEventBytes) or the timestamp window;
// the caller has the raw bytes and the server clock for that.
func Validate(e *Event) error {
	if !isUUID(e.EventID) {
		return fmt.Errorf("event_id must be a UUID, got %q", truncateForError(e.EventID))
	}
	if e.Timestamp.IsZero() {
		return fmt.Errorf("timestamp is required")
	}
	if !e.Level.Valid() {
		return fmt.Errorf("level %q is not one of debug, info, warning, error, fatal", truncateForError(string(e.Level)))
	}
	if e.Message == "" && e.Exception == nil {
		return fmt.Errorf("message or exception is required")
	}
	if err := maxLen("message", e.Message, MaxMessageBytes); err != nil {
		return err
	}

	if ex := e.Exception; ex != nil {
		if ex.Type == "" {
			return fmt.Errorf("exception.type is required")
		}
		if err := maxLen("exception.type", ex.Type, MaxExceptionTypeBytes); err != nil {
			return err
		}
		if err := maxLen("exception.value", ex.Value, MaxExceptionValueBytes); err != nil {
			return err
		}
		if len(ex.Frames) > MaxFrames {
			return fmt.Errorf("exception.frames has %d entries, limit is %d", len(ex.Frames), MaxFrames)
		}
		for i, f := range ex.Frames {
			if f.Function == "" {
				return fmt.Errorf("exception.frames[%d].function is required", i)
			}
			if len(f.Module) > MaxFrameFieldBytes || len(f.Function) > MaxFrameFieldBytes || len(f.File) > MaxFrameFieldBytes {
				return fmt.Errorf("exception.frames[%d] has a module, function or file over %d bytes", i, MaxFrameFieldBytes)
			}
			// Stored as UInt32. Out-of-range values must be stopped here:
			// past ingest, an unstorable row would block the worker.
			if f.Line < 0 || int64(f.Line) > math.MaxUint32 {
				return fmt.Errorf("exception.frames[%d].line %d is out of range", i, f.Line)
			}
		}
	}

	if len(e.Tags) > MaxTags {
		return fmt.Errorf("tags has %d entries, limit is %d", len(e.Tags), MaxTags)
	}
	for k, v := range e.Tags {
		if k == "" {
			return fmt.Errorf("tag keys must not be empty")
		}
		if err := maxLen("tag key", k, MaxTagKeyBytes); err != nil {
			return err
		}
		if err := maxLen(fmt.Sprintf("tag %q value", truncateForError(k)), v, MaxTagValueBytes); err != nil {
			return err
		}
	}

	if u := e.User; u != nil {
		if len(u.ID) > MaxUserFieldBytes || len(u.Username) > MaxUserFieldBytes ||
			len(u.Email) > MaxUserFieldBytes || len(u.IPAddress) > MaxUserFieldBytes {
			return fmt.Errorf("a user field is over %d bytes", MaxUserFieldBytes)
		}
	}

	if err := maxLen("release", e.Release, MaxReleaseBytes); err != nil {
		return err
	}
	if err := maxLen("environment", e.Environment, MaxEnvironmentBytes); err != nil {
		return err
	}
	if e.SDK.Name == "" || e.SDK.Version == "" {
		return fmt.Errorf("sdk.name and sdk.version are required")
	}
	if err := maxLen("sdk.name", e.SDK.Name, MaxSDKFieldBytes); err != nil {
		return err
	}
	return maxLen("sdk.version", e.SDK.Version, MaxSDKFieldBytes)
}

func maxLen(field, v string, limit int) error {
	if len(v) > limit {
		return fmt.Errorf("%s is %d bytes, limit is %d", field, len(v), limit)
	}
	return nil
}

// isUUID accepts any version in canonical 8-4-4-4-12 hex form. The SDK sends
// v7, but other clients may send v4; deduplication only needs uniqueness.
func isUUID(s string) bool {
	if len(s) != 36 {
		return false
	}
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch i {
		case 8, 13, 18, 23:
			if c != '-' {
				return false
			}
		default:
			if !(c >= '0' && c <= '9' || c >= 'a' && c <= 'f' || c >= 'A' && c <= 'F') {
				return false
			}
		}
	}
	return true
}

// truncateForError keeps client-supplied text in error messages short.
func truncateForError(s string) string {
	if len(s) > 64 {
		return s[:64] + "..."
	}
	return s
}
