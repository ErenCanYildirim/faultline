// Package debugsrv serves Go's profiling endpoints (pprof) and a process's
// own debug pages on a separate port from its public API.
//
// A separate port, because profiles expose function names and memory
// contents and a CPU profile costs CPU while it runs: they must never be
// reachable from where the API is. In docker-compose the port is published
// on 127.0.0.1 only.
package debugsrv

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"net/http/pprof"
	"time"
)

// Start serves the debug endpoints on addr until ctx ends. Extra handlers
// are added by path. An empty addr disables the server.
//
//	/debug/pprof/          index of profiles
//	/debug/pprof/profile   CPU profile, ?seconds=30
//	/debug/pprof/heap      memory
func Start(ctx context.Context, addr string, log *slog.Logger, extra map[string]http.Handler) {
	if addr == "" {
		return
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/debug/pprof/", pprof.Index)
	mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	mux.HandleFunc("/debug/pprof/trace", pprof.Trace)
	for path, h := range extra {
		mux.Handle(path, h)
	}
	srv := &http.Server{
		Addr:              addr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
		// No write timeout: a CPU profile streams for as long as asked.
	}
	go func() {
		log.Info("debug server listening", "addr", addr)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Warn("debug server stopped", "err", err)
		}
	}()
	go func() {
		<-ctx.Done()
		sctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = srv.Shutdown(sctx)
	}()
}
