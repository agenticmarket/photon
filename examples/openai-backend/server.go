package main

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/agenticmarket/photon"
)

// Server is the application: photon's Server, plus the domain state around it.
//
// The composition is the point. photon.Server owns the HTTP concerns - limits,
// streaming, back-pressure, panic recovery, graceful shutdown - and this struct
// owns the tenant, quota, and provider concerns. Neither reaches into the other:
// the handlers talk to photon through its public API, and photon never learns
// what a tenant is.
type Server struct {
	cfg      Config
	logger   *slog.Logger
	provider Provider
	metrics  *metrics

	// draining is set when shutdown begins, so /readyz can report it.
	draining atomic.Bool

	http *photon.Server
}

// maxRequestBody bounds a chat request.
//
// 1 MiB matches photon's default, and is stated here rather than inherited so
// that changing one does not silently change the other. A chat request is a few
// kilobytes; a megabyte is a large conversation, not a large request.
const maxRequestBody = 1 << 20

// New builds the server. It does not listen.
func New(cfg Config, logger *slog.Logger) (*Server, error) {
	if cfg.UpstreamURL != "" {
		// Named rather than defaulted, because the fake provider is not a
		// stand-in that behaves the same way: it does not call a model, and a
		// deployment that believes it is proxying to OpenAI is a deployment that
		// will ship to production having tested nothing.
		return nil, &errNoUpstreamAdapter{url: cfg.UpstreamURL}
	}

	s := &Server{
		cfg:     cfg,
		logger:  logger,
		metrics: &metrics{},
		// The fake provider stands in for a model. The README is explicit that
		// this is a demo, so the boundary is visible from both sides.
		provider: newFakeProvider(15 * time.Millisecond),
	}

	// Stream limits are set from configuration, and MaxStreams is left at 0 when
	// unset so photon derives it from the host's memory budget. That derivation
	// is what makes "a high stream count and a forgotten buffer cap"
	// unrepresentable rather than merely discouraged.
	limits := photon.DefaultLimits()
	limits.MaxStreamBufferBytes = cfg.MaxStreamBufferBytes
	limits.StreamStallTimeout = photon.Duration(cfg.StallTimeout)
	if cfg.MaxStreams > 0 {
		limits.MaxStreams = cfg.MaxStreams
	}

	s.http = photon.New(
		photon.Logger(logger),
		photon.SetLimits(limits),
	)

	s.routes()
	return s, nil
}

// errNoUpstreamAdapter is returned when an upstream URL is configured but no
// adapter is wired.
//
// A distinct error type rather than a log line, because it is a misconfiguration
// that must stop startup. Silently falling back to the fake provider would
// produce a server that accepts money and returns nonsense.
type errNoUpstreamAdapter struct{ url string }

func (e *errNoUpstreamAdapter) Error() string {
	return "PHOTON_UPSTREAM_URL is set to " + e.url +
		" but no upstream adapter is wired: this build only has the fake provider. " +
		"Implement Provider in provider.go, or unset PHOTON_UPSTREAM_URL to run the demo."
}

// Handler returns the server's http.Handler, for tests.
//
// Exposed so a test can drive the whole stack with httptest and no listener,
// which is what makes the streaming and quota tests fast enough to run on every
// change.
func (s *Server) Handler() http.Handler { return s.http }

// routes registers every route.
//
// Operational routes sit outside the API group, so they carry no auth and no
// quota. That is not cosmetic - /healthz must answer while the API is
// saturated, or a load balancer concludes the server is dead and restarts it.
func (s *Server) routes() {
	// The request id is global (Use covers every request, 404s included). It
	// adds a header and a log field and cannot fail a request, so there is no
	// reason for a health check to be untraceable - and an untraceable health
	// check is exactly the one you want to correlate with an incident.
	s.http.Use(s.requestID)

	// Operational endpoints. No auth and no quota: a health check that fails
	// because the rate limiter is busy tells the orchestrator to kill a healthy
	// process, and a metrics scrape that can be rate-limited is a metrics scrape
	// that silently stops working.
	s.http.GET("/healthz", s.healthz)
	s.http.GET("/readyz", s.readyz)
	s.http.GET("/metrics", s.metricsHandler)

	// The API group: authenticated and quota-limited. The policy lives on the
	// group, so a route inside cannot forget it and a route outside cannot pick
	// it up - which is what keeps the probes above unauthenticated.
	api := s.http.Group("/v1", s.authenticate, s.enforceQuota)

	// Streaming() enforces MaxStreams and attaches the Stream to the request.
	// It goes on the streaming route only: every request through it holds a
	// stream slot, and /v1/models has no business taking one.
	api.POST("/chat/completions", s.chatCompletions, photon.Streaming())
	api.GET("/models", s.models)

	// The 404 names no part of the request: a reflected path is a free oracle
	// for route enumeration.
	s.http.NotFound(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		writeError(w, http.StatusNotFound, "invalid_request_error", "unknown_route",
			"unknown route")
	}))
}

// Run listens and serves until a signal, then drains.
//
// The ordering below is load-bearing and is the same pattern as the other
// examples. Listen goes on a goroutine while main waits for a signal, because
// the alternative - a goroutine that calls Shutdown, then calling Listen - lets
// the shutdown win the race and stop the server before the socket is bound.
func (s *Server) Run() error {
	serveErr := make(chan error, 1)
	go func() {
		s.logger.Info("listening",
			"addr", s.cfg.Addr,
			"max_streams", s.http.Limits().MaxStreams,
			"stream_buffer", s.http.Limits().MaxStreamBufferBytes,
			"stall_timeout", s.http.Limits().StreamStallTimeout.String(),
			"config", s.cfg.Redacted())
		serveErr <- s.http.Listen(s.cfg.Addr)
	}()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	select {
	case err := <-serveErr:
		// Listen returned on its own: a bind failure, most likely.
		if err != nil {
			return err
		}
		return nil
	case <-ctx.Done():
	}

	// Mark draining *before* the timeout, not after. The gap between the signal
	// and this line is exactly when a rolling deploy can still send traffic to a
	// server that is about to refuse it.
	s.draining.Store(true)
	s.logger.Info("shutdown requested", "timeout", "20s", "in_flight", s.metrics.requestsInFlight.Load())

	// Twenty seconds, and longer than photon's own defaults, because a streaming
	// server's in-flight work is a model response that can legitimately take a
	// while. Cutting one off is a worse outcome than a slightly slower shutdown.
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	if err := s.http.Shutdown(shutdownCtx); err != nil {
		return err
	}
	if err := <-serveErr; err != nil {
		return err
	}
	return nil
}

// healthz reports liveness: the process is functioning.
//
// It checks nothing external, deliberately. A liveness probe that depends on a
// database will get the container killed during a database blip, turning a
// recoverable dependency failure into an outage.
func (s *Server) healthz(w http.ResponseWriter, r *http.Request) {
	// Not gated on draining. A draining server is still alive and finishing work
	// it already accepted; returning non-200 here would get it killed mid-stream.
	writeJSON(w, http.StatusOK, map[string]any{"status": "ok"})
}

// readyz reports readiness: should traffic be sent here?
//
// This is the one that returns 503 while draining. A load balancer polling
// /readyz takes this instance out of rotation; a liveness probe polling
// /healthz leaves it alone.
func (s *Server) readyz(w http.ResponseWriter, r *http.Request) {
	if s.draining.Load() {
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{
			"status": "draining",
			"reason": "shutting down, not accepting new streams",
		})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"status": "ready"})
}

// metricsHandler serves the Prometheus endpoint.
//
// Not behind auth, and that is a decision rather than an oversight: a scraper
// authenticating is a common source of broken monitoring, and the usual answer is
// to bind this to an internal interface or put it behind a proxy. The
// alternative - exposing traffic shape to anyone who finds the port - is the
// reason the README tells you to do one of those two things.
func (s *Server) metricsHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(s.metrics.render()))
}

// requestID gives each request an identifier for logs.
//
// From an inbound X-Request-ID when present, so a trace started at a proxy
// continues through here, and generated otherwise. It is echoed back so a client
// can quote it in a bug report, which is the entire point.
func (s *Server) requestID(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id := strings.TrimSpace(r.Header.Get("X-Request-ID"))
		if id == "" {
			id = newRequestID()
		}
		// Sanitised: an inbound header is attacker-controlled, and it is about to
		// be written into a log line. Unfiltered, a newline in the value forges
		// log entries - the cheapest log-forging bug there is.
		id = sanitiseLogValue(id, 64)
		w.Header().Set("X-Request-ID", id)
		next.ServeHTTP(w, r.WithContext(withRequestID(r.Context(), id)))
	})
}

func withRequestID(ctx context.Context, id string) context.Context {
	return context.WithValue(ctx, requestIDKey{}, id)
}

func requestIDFrom(ctx context.Context) string {
	id, _ := ctx.Value(requestIDKey{}).(string)
	return id
}

type requestIDKey struct{}

// newRequestID returns a random-enough identifier.
//
// crypto/rand rather than math/rand: this value appears in logs that a client
// then quotes, and a predictable id lets one tenant correlate another tenant's
// requests in the log.
func newRequestID() string {
	var b [12]byte
	if _, err := randRead(b[:]); err != nil {
		// A failure here means the system entropy source is broken, which is not
		// recoverable and not worth pretending around. The timestamp keeps the
		// value unique enough to be useful.
		return "ts-" + itoa(int(time.Now().UnixNano()))
	}
	const hexdigits = "0123456789abcdef"
	out := make([]byte, 0, len(b)*2)
	for _, c := range b {
		out = append(out, hexdigits[c>>4], hexdigits[c&0xF])
	}
	return string(out)
}

// sanitiseLogValue strips anything that could forge or corrupt a log line.
func sanitiseLogValue(s string, max int) string {
	if len(s) > max {
		s = s[:max]
	}
	var b strings.Builder
	b.Grow(len(s))
	for _, r := range s {
		switch {
		case r == '\n' || r == '\r':
			// Replaced rather than dropped, so the two events remain
			// distinguishable in the log.
			b.WriteRune(' ')
		case r < 0x20 || r == 0x7f:
			// Control characters, including the ones that move a terminal cursor.
		default:
			b.WriteRune(r)
		}
	}
	return b.String()
}
