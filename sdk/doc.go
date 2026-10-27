// Package faultline is the Go client SDK. Applications import it to capture
// errors, messages and panics and send them to the ingest API.
//
// Basic use:
//
//	err := faultline.Init(faultline.Options{
//		DSN:         "http://<project-key>@localhost:8080",
//		Release:     "shop-api@1.4.2",
//		Environment: "production",
//	})
//	if err != nil {
//		log.Fatal(err)
//	}
//	defer faultline.Close(2 * time.Second)
//
//	if err := checkout(); err != nil {
//		faultline.CaptureError(err, faultline.WithTag("route", "/checkout"))
//	}
//
// Guarantees the SDK makes to the host application:
//   - Capture calls never block. Events go into a bounded in-memory buffer and
//     a background goroutine sends them in batches.
//   - Memory is bounded. When the buffer is full, new events are dropped and
//     counted in Stats().Dropped; the application keeps running.
//   - The SDK never panics into the application. Before Init, and after
//     Close, every call is a no-op.
//
// Retries, backoff and sampling arrive in step 1.3b. Until then a failed
// send loses that batch and counts it in Stats().Failed.
package faultline
