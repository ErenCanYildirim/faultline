// Command basic shows the SDK end to end: a handled error, a message and a
// recovered panic, sent to a running ingest API.
//
//	go run ./examples/basic
//
// FAULTLINE_DSN overrides the target; the default matches the "demo"
// project in deploy/projects.json and the ingest port in docker-compose.
package main

import (
	"fmt"
	"io/fs"
	"log"
	"log/slog"
	"os"
	"time"

	faultline "github.com/erencanyildirim/faultline/sdk"
)

func main() {
	dsn := os.Getenv("FAULTLINE_DSN")
	if dsn == "" {
		dsn = "http://dev-demo-key-0123456789abcdef@localhost:8080"
	}

	err := faultline.Init(faultline.Options{
		DSN:         dsn,
		Release:     "example@0.1.0",
		Environment: "development",
		// Show the SDK's own warnings, e.g. when ingest is unreachable.
		Logger: slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn})),
	})
	if err != nil {
		log.Fatal(err)
	}

	// 1. A handled error, wrapped twice on its way up.
	if err := checkout("8812"); err != nil {
		id := faultline.CaptureError(err,
			faultline.WithTag("route", "/api/checkout"),
			faultline.WithUser(faultline.User{ID: "u_1029"}),
		)
		fmt.Println("captured error  ", id)
	}

	// 2. A message without a stack trace.
	id := faultline.CaptureMessage("payment provider slow: 2300ms", faultline.WithLevel(faultline.LevelWarning))
	fmt.Println("captured message", id)

	// 3. A panic. faultline.Recover records it and panics again, as a real
	//    crash would. The outer recover exists only to keep this example
	//    running to the end.
	func() {
		defer func() { _ = recover() }()
		defer faultline.Recover()
		applyDiscount(nil)
	}()
	fmt.Println("captured panic")

	// Longer than ingest's 5s produce timeout, so a broker outage comes back
	// as a counted failure instead of an unknown in-flight batch.
	ok := faultline.Close(10 * time.Second)
	s := faultline.GetStats()
	fmt.Printf("\nflushed=%v captured=%d sent=%d failed=%d in_flight=%d dropped=%d\n",
		ok, s.Captured, s.Sent, s.Failed, s.InFlight, s.Dropped)
	if s.Sent != s.Captured {
		fmt.Println("not everything was sent: is the stack up? (.\\tasks.ps1 up)")
		os.Exit(1)
	}
}

func checkout(cartID string) error {
	if err := loadCart(cartID); err != nil {
		return fmt.Errorf("checkout: %w", err)
	}
	return nil
}

func loadCart(cartID string) error {
	return fmt.Errorf("load cart %s: %w", cartID,
		&fs.PathError{Op: "open", Path: "/var/data/cart-" + cartID + ".json", Err: fs.ErrNotExist})
}

type cart struct{ total float64 }

func applyDiscount(c *cart) {
	c.total *= 0.9 // nil pointer dereference
}
