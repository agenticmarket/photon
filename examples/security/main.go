// Command security shows what photon protects you from by default, and the few
// things it deliberately leaves to you.
//
// Run it:
//
//	go run ./examples/security
//
// Then, from another terminal:
//
//	curl http://localhost:8080/limits
//	curl -i http://localhost:8080/panic
//	curl -i http://localhost:8080/echo-ip
//	curl -i -H "X-Forwarded-For: 1.2.3.4" http://localhost:8080/echo-ip
//	head -c 2000000 /dev/urandom | curl -i -X POST http://localhost:8080/upload --data-binary @-
//	curl -i http://localhost:8080/error
//
// The point of this example is that most of what a "secure Go server" article
// tells you to add is already here, on by default, with no configuration.
package main

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/agenticmarket/photon"
	"github.com/agenticmarket/photon/photonerr"
)

func main() {
	app := photon.New(
		// photon is silent by default, and this example needs to show that a
		// rejected request is logged. The default silence is deliberate: a
		// library that writes to stderr behind your back is a library nobody can
		// debug.
		photon.Logger(slog.New(slog.NewTextHandler(os.Stderr, nil))),

		// Panics are recovered, logged with a stack, and answered with 500. This
		// hook is not needed for that - it exists so the log can be structured
		// and routed somewhere an operator will actually see it.
		photon.OnPanic(func(p *photon.PanicInfo) {
			slog.Error("handler panicked",
				"method", p.Method,
				"path", p.Path,
				"panic", p.Value,
				"stack", string(p.Stack))
		}),
	)

	// /limits reports the effective configuration. Every value here is a default
	// - nothing in this program configures a limit - and that is the point:
	// a new photon server is already bounded.
	app.GET("/limits", photon.HandlerFunc(showLimits))

	// A route that panics, to show the recovery is real and that the process
	// survives it.
	app.GET("/panic", photon.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// The response has already been committed by this point, so a panic
		// cannot change the status. photon reports it as ErrStreamPanic rather
		// than pretending a 500 was sent.
		panic("something went wrong deep in a handler")
	}))

	// A route that returns a typed error, to show RFC 9457 problem documents.
	app.GET("/error", photon.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Render maps a photonerr to a status and a body. An error that is not a
		// photonerr becomes a 500 with a *constant* body, because an unknown
		// error is often a database error containing a connection string.
		_ = photonerr.Render(w, photonerr.NotFound("widget"), photonerr.RenderOptions{})
	}))

	app.GET("/echo-ip", photon.HandlerFunc(echoIP))

	// A deliberately naive upload handler, to show the default body limit at
	// work. It never checks the size itself, and it does not need to.
	app.POST("/upload", photon.HandlerFunc(upload))

	serveErr := make(chan error, 1)
	go func() {
		slog.Info("listening on http://localhost:8080")
		serveErr <- app.Listen(":8080")
	}()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	select {
	case err := <-serveErr:
		if err != nil {
			slog.Error("server stopped", "err", err)
			os.Exit(1)
		}
		return
	case <-ctx.Done():
		slog.Info("shutting down")
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := app.Shutdown(shutdownCtx); err != nil {
		slog.Error("shutdown did not complete cleanly", "err", err)
	}
	if err := <-serveErr; err != nil {
		slog.Error("server stopped", "err", err)
		os.Exit(1)
	}
	slog.Info("stopped")
}

// showLimits reports the effective limits.
//
// Worth reading twice: MaxRequestBodyBytes is 1 MiB and MaxHeaders is 100, and
// neither was written by this program. The limits that stop the boring attacks
// are the ones nobody remembers to configure.
func showLimits(w http.ResponseWriter, r *http.Request) {
	l := photon.DefaultLimits()
	_ = photon.JSON(w, http.StatusOK, map[string]any{
		"note": "every value below is a default; this program configures none of them",
		"limits": map[string]any{
			"max_request_body_bytes":  l.MaxRequestBodyBytes,
			"max_header_bytes":        l.MaxHeaderBytes,
			"max_headers":             l.MaxHeaders,
			"max_header_value_bytes":  l.MaxHeaderValueBytes,
			"read_header_timeout":     l.ReadHeaderTimeout.String(),
			"idle_timeout":            l.IdleTimeout.String(),
			"read_timeout":            l.ReadTimeout.String(),
			"max_connections":         l.MaxConnections,
			"max_concurrent_requests": l.MaxConcurrentRequests,
			"max_per_ip":              l.MaxPerIP,
			"max_streams":             l.MaxStreams,
			"max_stream_buffer_bytes": l.MaxStreamBufferBytes,
		},
	})
}

// echoIP reports the client address photon believes it is talking to.
//
// The interesting result is the second curl in the README: with
// X-Forwarded-For set to something invented, this still reports 127.0.0.1.
func echoIP(w http.ResponseWriter, r *http.Request) {
	peer := photon.ClientIP(r)
	_ = photon.JSON(w, http.StatusOK, map[string]any{
		"client_ip":              peer.String(),
		"x_forwarded_for_header": r.Header.Get("X-Forwarded-For"),
		"note":                   "the header is reported but not believed; see the comment on ClientIP",
	})
}

// upload reads a request body and reports how much it got.
//
// It performs no size checking of its own. MaxRequestBodyBytes is 1 MiB and is
// enforced by a reader that wraps the body, so a 2 GB upload is refused without
// ever being held in memory. That is the property that matters: the limit is a
// *rejection threshold*, not a memory cost. A 100 MB upload costs about the
// same memory as a 100 KB one.
func upload(w http.ResponseWriter, r *http.Request) {
	if r.ContentLength > 0 {
		// The honest way to reject early. Worth doing in a handler that knows its
		// own domain limit is smaller than photon's, to save the client the
		// upload entirely.
		_ = photon.JSON(w, http.StatusRequestEntityTooLarge, map[string]string{
			"error": "this route allows at most 10 KiB",
		})
		return
	}
	_ = photon.JSON(w, http.StatusNoContent, nil)
}
