package faultline

import (
	"errors"
	"fmt"
	"runtime"
	"strings"

	"github.com/erencanyildirim/faultline/event"
)

// sdkModule is this package's import path, worked out at runtime so the SDK
// can strip its own frames from captured stacks.
var sdkModule = func() string {
	pc, _, _, _ := runtime.Caller(0)
	module, _ := splitFunction(runtime.FuncForPC(pc).Name())
	return module
}()

// splitFunction splits a runtime function name into package path and
// function name:
//
//	"github.com/acme/shop/cart.(*Cart).Checkout" -> "github.com/acme/shop/cart", "(*Cart).Checkout"
//	"main.main.func1"                            -> "main", "main.func1"
func splitFunction(full string) (module, function string) {
	slash := strings.LastIndex(full, "/")
	dot := strings.Index(full[slash+1:], ".")
	if dot < 0 {
		return "", full
	}
	i := slash + 1 + dot
	return full[:i], full[i+1:]
}

// stacktrace returns the caller's stack, innermost frame first, without the
// SDK's own frames or the runtime frames of a panic in progress.
func (c *Client) stacktrace() []event.Frame {
	pcs := make([]uintptr, event.MaxFrames+32)
	n := runtime.Callers(1, pcs)
	frames := runtime.CallersFrames(pcs[:n])

	out := make([]event.Frame, 0, 16)
	leading := true
	for {
		f, more := frames.Next()
		module, function := splitFunction(f.Function)

		// Skip from the top until the first frame that is neither the SDK nor
		// the runtime. For a panic this removes runtime.gopanic, sigpanic and
		// friends, so frame 0 is the line that panicked.
		skip := leading && (module == sdkModule || module == "runtime")
		if !skip {
			leading = false
		}
		if !skip && f.Function != "runtime.goexit" && f.Function != "" {
			out = append(out, event.Frame{
				Module:   module,
				Function: function,
				File:     f.File,
				Line:     f.Line,
				InApp:    c.isInApp(module),
			})
		}
		if !more || len(out) == event.MaxFrames {
			break
		}
	}
	return out
}

func (c *Client) isInApp(module string) bool {
	if module == "main" {
		return true
	}
	for _, p := range c.opts.InAppPrefixes {
		if module == p || strings.HasPrefix(module, p+"/") {
			return true
		}
	}
	return false
}

// genericErrorTypes say nothing about what went wrong: they are wrappers or
// plain text. Grouping by them would merge unrelated errors.
var genericErrorTypes = map[string]bool{
	"*errors.errorString": true,
	"*fmt.wrapError":      true,
	"*fmt.wrapErrors":     true,
}

// errorType names the error for grouping. It walks the Unwrap chain and
// returns the deepest type that is specific:
//
//	fmt.Errorf("load cart: %w", &fs.PathError{Err: fs.ErrNotExist})
//	  *fmt.wrapError -> *fs.PathError -> *errors.errorString
//	  result: *fs.PathError
//
// Not the outermost type (usually a generic wrapper) and not the root
// (often a plain sentinel like fs.ErrNotExist). If every type in the chain is
// generic, the root's type is used. Joined errors (Unwrap() []error) end the
// walk, because errors.Unwrap does not follow them.
func errorType(err error) string {
	specific := ""
	last := ""
	for e := err; e != nil; e = errors.Unwrap(e) {
		last = fmt.Sprintf("%T", e)
		if !genericErrorTypes[last] {
			specific = last
		}
	}
	if specific != "" {
		return specific
	}
	return last
}
