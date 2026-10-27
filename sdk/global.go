package faultline

import (
	"sync/atomic"
	"time"
)

// The package-level functions use one shared Client. Before Init they are
// no-ops, so a library can call them without knowing whether the
// application configured the SDK.
var global atomic.Pointer[Client]

// Init creates the shared Client. Calling Init again replaces it and closes
// the old one.
func Init(opts Options) error {
	c, err := New(opts)
	if err != nil {
		return err
	}
	if old := global.Swap(c); old != nil {
		old.Close(2 * time.Second)
	}
	return nil
}

// CurrentClient returns the shared Client, or nil before Init.
func CurrentClient() *Client {
	return global.Load()
}

// CaptureError records err on the shared Client. See Client.CaptureError.
func CaptureError(err error, opts ...EventOption) string {
	if c := global.Load(); c != nil {
		return c.CaptureError(err, opts...)
	}
	return ""
}

// CaptureMessage records msg on the shared Client. See Client.CaptureMessage.
func CaptureMessage(msg string, opts ...EventOption) string {
	if c := global.Load(); c != nil {
		return c.CaptureMessage(msg, opts...)
	}
	return ""
}

// Recover captures a panic on the shared Client, then panics again.
// Use it directly with defer:
//
//	defer faultline.Recover()
//
// It must be the deferred function itself: Go's recover only works when
// called directly by a deferred function, which is why this does not just
// call Client.Recover.
func Recover() {
	if r := recover(); r != nil {
		if c := global.Load(); c != nil {
			c.capturePanic(r)
		}
		panic(r)
	}
}

// Flush waits for buffered events to be sent. See Client.Flush.
func Flush(timeout time.Duration) bool {
	if c := global.Load(); c != nil {
		return c.Flush(timeout)
	}
	return true
}

// Close flushes and stops the shared Client. See Client.Close.
func Close(timeout time.Duration) bool {
	if c := global.Load(); c != nil {
		return c.Close(timeout)
	}
	return true
}

// GetStats returns the shared Client's counters, or zeros before Init.
func GetStats() Stats {
	if c := global.Load(); c != nil {
		return c.Stats()
	}
	return Stats{}
}
