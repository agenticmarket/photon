// Command production shows the parts of running a server that are not the
// handler: lifecycle, health checks, timeouts, and logging that an operator can
// actually read.
//
// Run it:
//
//	go run ./examples/production
//
// Then:
//
//	curl http://localhost:8080/healthz
//	curl http://localhost:8080/readyz
//	curl http://localhost:8080/work
//	# Ctrl-C, then immediately:
//	curl --max-time 1 http://localhost:8080/readyz   # 503, still draining
//
// What this example is for:
//
//   - The shutdown ordering that cannot express the Listen/Shutdown race.
//   - Distinguishing liveness from readiness, which is the difference between
//     "restart me" and "take me out of rotation".
//   - A read/write deadline that does not make a long response impossible.
//   - Logs worth reading.
package main

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/agenticmarket/photon"
)

// address is the listen address. Read from the environment so the same binary
// runs locally and in a container without a rebuild.
var address = envOr("PHOTON_ADDR", ":8080")

// draining is set when shutdown begins.
//
// An atomic rather than a mutex or a plain bool: it is written once by the signal
// goroutine and read by every health check, with no other synchronisation. A
// plain bool here would be a data race that the detector would flag and that
// would, more importantly, be a real bug under load.
var draining atomic.Bool

func main() {
	// Start from the secure defaults and change only what this deployment
	// needs. Starting from an empty photon.Limits{} instead would switch off
	// every limit it does not name - the body limit, the header limits,
	// heartbeats - with nothing to show for it.
	limits := photon.DefaultLimits()
	// Bound the server so one deploy cannot exhaust the machine. Zero means
	// "bounded by the file-descriptor limit", which on most containers is a
	// much larger number than the memory behind it.
	limits.MaxConnections = 2000
	limits.MaxConcurrentRequests = 500

	app := photon.New(
		// JSON logs, because a production log is consumed by something other
		// than a person. This is a one-line change from the TextHandler the other
		// examples use, and it is the single highest-value logging decision.
		photon.Logger(slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
			Level: slog.LevelInfo,
		}))),

		photon.SetLimits(limits),
	)

	// Health and readiness are registered before everything else, and
	// deliberately do not take the full middleware stack. A health check that
	// depends on a database or a downstream service reports the health of *that*
	// dependency, and an orchestrator that restarts the container because a
	// database is briefly down turns a small problem into an outage.
	app.GET("/healthz", photon.HandlerFunc(healthz))
	app.GET("/readyz", photon.HandlerFunc(readyz))

	app.GET("/work", photon.HandlerFunc(work))

	// Listen blocks, so it goes on its own goroutine while main waits for a
	// signal.
	//
	// The alternative - starting a goroutine that calls Shutdown, then calling
	// Listen - looks equivalent and is not. That goroutine can run first, before
	// Listen has bound the socket, and the server exits immediately on startup.
	// It is not a photon bug and the race detector cannot see it; it is the most
	// common way to write this pattern wrong, which is why this ordering is the
	// one shown.
	serveErr := make(chan error, 1)
	go func() {
		slog.Info("server starting",
			"addr", address,
			"max_connections", app.Limits().MaxConnections,
			"pid", os.Getpid())
		if err := app.Listen(address); err != nil {
			serveErr <- err
			return
		}
		serveErr <- nil
	}()

	// Wait for a signal. NotifyContext is used rather than a bare channel
	// because it also cancels a context, which is what makes the deadline below
	// work.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	select {
	case err := <-serveErr:
		// Listen returned on its own: a bind failure, most likely.
		if err != nil {
			slog.Error("server failed to start", "err", err)
			os.Exit(1)
		}
		return
	case <-ctx.Done():
	}

	// Mark the server as draining *before* the timeout, not after. The gap
	// between the signal and this line is exactly when a rolling deploy can
	// still send traffic to a server that is about to refuse it.
	draining.Store(true)
	slog.Info("shutdown requested, draining", "timeout", "15s")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	if err := app.Shutdown(shutdownCtx); err != nil {
		// A deadline that expired has not given in-flight work time to finish.
		// That is the honest outcome and worth logging as an error, not an
		// ordinary return: something took longer than 15 seconds, and an operator
		// needs to know what.
		slog.Error("shutdown did not complete cleanly, forcing exit", "err", err)
		os.Exit(1)
	}

	// Wait for Listen to return, so the process does not exit while connection
	// goroutines are still unwinding.
	if err := <-serveErr; err != nil {
		slog.Error("server stopped with error", "err", err)
		os.Exit(1)
	}
	slog.Info("server stopped cleanly")
}

// healthz reports liveness: is this process functioning?
//
// It checks nothing but itself, and that is deliberate. A liveness probe that
// checks a database will get the container killed during a database blip, which
// turns a recoverable dependency failure into an outage.
func healthz(w http.ResponseWriter, r *http.Request) {
	// Not gated on draining. A draining server is still alive; it is finishing
	// what it accepted. If this returned non-200 while draining, an orchestrator
	// would kill the process mid-request.
	_ = photon.JSON(w, http.StatusOK, map[string]any{
		"status": "ok",
		"pid":    os.Getpid(),
	})
}

// readyz reports readiness: should traffic be sent here?
//
// This is the one that returns 503 while draining, and the distinction is the
// whole point of having two endpoints. A load balancer polling /readyz removes
// this instance from rotation; a liveness probe polling /healthz leaves it
// alone until the orchestrator decides otherwise.
func readyz(w http.ResponseWriter, r *http.Request) {
	if draining.Load() {
		_ = photon.JSON(w, http.StatusServiceUnavailable, map[string]any{
			"status": "draining",
			"reason": "shutting down, not accepting new work",
		})
		return
	}
	_ = photon.JSON(w, http.StatusOK, map[string]any{"status": "ready"})
}

// work is an ordinary handler, to show request logging with a duration and a
// status.
func work(w http.ResponseWriter, r *http.Request) {
	// The deadline comes from the request context, so a client that gives up
	// cancels the work rather than leaving it running to write into a dead
	// socket. This is the difference between a handler that stops and one that
	// finishes work nobody will read.
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()

	start := time.Now()
	select {
	case <-ctx.Done():
		// 499 is nginx's non-standard "client closed request". Using it is a
		// deliberate convention: a 499 is not a server error and should not be
		// counted as one, but it is not a 4xx the client can be blamed for
		// either, because the client is gone.
		w.WriteHeader(499)
		slog.Info("client went away", "path", r.URL.Path, "took", time.Since(start))
		return
	case <-time.After(100 * time.Millisecond):
	}

	slog.Info("request completed",
		"method", r.Method,
		"path", r.URL.Path,
		"peer", photon.ClientIP(r).String(),
		"took", time.Since(start),
		"status", 200)

	_ = photon.JSON(w, http.StatusOK, map[string]any{
		"ok":     true,
		"took":   time.Since(start).String(),
		"stream": "for a long response, see examples/stream",
	})
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
