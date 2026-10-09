package photon

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os/signal"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/agenticmarket/photon/photonerr"
)

// Duration is a time.Duration with named constants, so a Limits literal reads
// in the same vocabulary as the rest of the package.
type Duration = time.Duration

// The durations a Limits value uses.
const (
	Second      = time.Second
	Millisecond = time.Millisecond
)

// DefaultShutdownTimeout is how long Run waits for in-flight requests and
// streams after a signal.
//
// 25 seconds sits under Kubernetes' default 30-second termination grace period
// with room for the process to exit, so a rolling deploy drains rather than
// being SIGKILLed mid-answer.
const DefaultShutdownTimeout = 25 * time.Second

// Server is a router plus a listener lifecycle, a resource budget, and
// graceful shutdown.
//
//	app := photon.New()
//	app.GET("/users/:id", getUser)
//	app.Run(":8080")
//
// A Server is an http.Handler, so it can also be mounted, wrapped in
// middleware, or passed to httptest.NewServer.
type Server struct {
	rt              *Router
	limits          Limits
	logger          *slog.Logger
	onPanic         func(*PanicInfo)
	shutdownTimeout time.Duration

	// maxStreams and memoryBudget hold the values of the MaxStreams and
	// MemoryBudget options until every option has run, so they apply whether
	// they come before or after SetLimits, which replaces the whole Limits.
	maxStreams   *int
	memoryBudget *int64

	// base is the http.Server that owns the socket. The HTTP parser is
	// net/http's, deliberately: it is the most heavily attacked code in the Go
	// ecosystem and it has been hardened for fifteen years. photon adds the
	// limits net/http has no concept of.
	base *http.Server

	// baseCtx is the context every request served by Serve descends from. It
	// carries the Server, so a request reaching ServeHTTP through the listener
	// already knows its server and attaching it costs nothing per request.
	baseCtx context.Context

	// drainCtx is cancelled when Shutdown begins. Streams watch it through
	// Stream.Done, so a long-lived stream can finish its answer and return
	// instead of holding shutdown open until the deadline.
	drainCtx  context.Context
	drainStop context.CancelFunc

	// mu guards the lifecycle transition in Serve.
	mu      sync.Mutex
	started bool

	// active tracks in-flight requests, for MaxConcurrentRequests.
	active atomic.Int64

	// streams tracks in-flight streaming responses, for MaxStreams. It is
	// separate from active because a streaming response occupies a connection
	// and a goroutine for its whole life, which is a different resource from a
	// request that completes in microseconds.
	streams atomic.Int64

	// streamBytes tracks stream buffer memory beyond the nominal per-stream
	// buffers, which only the Grow policy can allocate. nominalBytes is the
	// sum of the open streams' own buffer caps. Together they are what
	// MaxTotalStreamBufferBytes bounds.
	streamBytes  atomic.Int64
	nominalBytes atomic.Int64

	// conns counts live connections for MaxConnections, and perIP counts them
	// by peer address for MaxPerIP. Both are guarded by connMu because the
	// listener and the per-connection goroutines run concurrently.
	connMu sync.Mutex
	conns  int
	perIP  map[string]int
	// admitted holds the connections admitted by limitListener, so connState
	// can release each one exactly once.
	admitted sync.Map // net.Conn -> struct{}

	// refusals rate-limits the "connection refused" log line, so a flood that
	// admission control is correctly refusing does not also flood the log.
	refusals logLimiter
	// rejections does the same for requests refused by a limit.
	rejections logLimiter

	// warnedNoDeadline records that the missing-write-deadline warning has
	// been logged, so it appears once rather than once per stream.
	warnedNoDeadline atomic.Bool
}

// PanicInfo describes a panic recovered from a handler.
type PanicInfo struct {
	// Value is the recovered value.
	Value any
	// Stack is the stack trace at the point of recovery.
	Stack []byte
	// Method and Path identify the request that panicked.
	Method, Path string
}

// Option configures a Server. Options are validated in New, so a bad limit
// fails at startup rather than at 3am under attack.
type Option func(*Server)

// SetLimits sets the server's resource budget.
//
//	app := photon.New(photon.SetLimits(photon.DefaultLimits()))
//
// Zero-valued fields keep their meaning from Limits: unlimited for the request
// limits, derived for the stream limits. Start from DefaultLimits and change
// what you need, rather than from an empty Limits, unless you mean to remove
// the defaults.
func SetLimits(l Limits) Option {
	return func(s *Server) { s.limits = l }
}

// Logger sets the logger.
//
// The default logs only errors - handler panics and 5xx responses written with
// Error - through slog.Default. Warnings such as rejected requests are dropped
// unless a logger is configured, so a flood of oversized requests cannot also
// become a flood of log lines.
func Logger(l *slog.Logger) Option {
	return func(s *Server) { s.logger = l }
}

// MemoryBudget sets the memory ceiling used to derive MaxStreams, in bytes.
//
// It exists so a deployment can state its budget rather than hoping detection
// finds the right one. Detection reads GOMEMLIMIT (or a limit set with
// debug.SetMemoryLimit), then the cgroup limit, then the machine's total RAM,
// and falls back to 256 MB with a logged warning.
//
//	app := photon.New(
//		photon.MemoryBudget(512 << 20), // a 512 MB container
//	)
//
// MaxStreams is then clamp(budget/4/256KiB, 64, 8192). Scaling from the memory
// budget rather than from GOMAXPROCS is deliberate: a 2-core edge box and a
// 64-core container have the same problem, and it is memory.
func MemoryBudget(bytes int64) Option {
	return func(s *Server) { s.memoryBudget = &bytes }
}

// MaxStreams sets the concurrent streaming response limit explicitly,
// overriding the value derived from the memory budget.
func MaxStreams(n int) Option {
	return func(s *Server) { s.maxStreams = &n }
}

// OnPanic registers a callback invoked when a handler panics, for example to
// report it to an error tracker. The server still recovers the panic, responds
// 500, and logs it.
func OnPanic(f func(*PanicInfo)) Option {
	return func(s *Server) { s.onPanic = f }
}

// ShutdownTimeout sets how long Run waits for in-flight requests and streams
// to finish after SIGINT or SIGTERM. The default is DefaultShutdownTimeout.
func ShutdownTimeout(d time.Duration) Option {
	return func(s *Server) { s.shutdownTimeout = d }
}

// New returns a Server with secure defaults and no routes.
//
// It takes no required arguments. Configuration exists for deployments that
// need to change something, not as a prerequisite for starting. It panics on
// an invalid configuration, so a bad limit fails at startup.
func New(opts ...Option) *Server {
	s := &Server{
		rt:              NewRouter(),
		limits:          DefaultLimits(),
		logger:          defaultLogger,
		onPanic:         func(*PanicInfo) {},
		shutdownTimeout: DefaultShutdownTimeout,
	}
	for _, opt := range opts {
		opt(s)
	}
	// Applied after every option, so photon.New(MaxStreams(256), SetLimits(l))
	// does not silently lose the 256 when SetLimits replaces the struct.
	if s.maxStreams != nil {
		s.limits.MaxStreams = *s.maxStreams
	}
	if s.memoryBudget != nil {
		s.limits.MemoryBudget = *s.memoryBudget
	}
	if s.logger == nil {
		s.logger = defaultLogger
	}
	if s.onPanic == nil {
		s.onPanic = func(*PanicInfo) {}
	}
	if s.shutdownTimeout <= 0 {
		panic("photon: ShutdownTimeout must be positive")
	}
	// Validation runs BEFORE the derived limits are filled in. A negative value
	// is a caller mistake, and deriving over it would silently replace the
	// mistake with a default: the limit would look configured and would be
	// something the operator never asked for.
	if err := s.limits.validate(); err != nil {
		panic("photon: invalid limits: " + err.Error())
	}
	explicitTotal := s.limits.MaxTotalStreamBufferBytes
	s.applyDerivedLimits()
	// An explicit global stream budget is checked again once MaxStreams is
	// known. Before derivation the check in validate cannot see the count, and
	// a budget below what the streams need is exactly the misconfiguration the
	// derivation exists to prevent.
	implied := int64(s.limits.MaxStreams) * int64(s.limits.MaxStreamBufferBytes)
	if explicitTotal > 0 && explicitTotal < implied {
		panic(fmt.Sprintf("photon: invalid limits: MaxTotalStreamBufferBytes is %d, below MaxStreams x "+
			"MaxStreamBufferBytes (%d x %d = %d) with MaxStreams derived from the memory budget: "+
			"raise it, set MaxStreams explicitly, or leave it 0 to derive it",
			explicitTotal, s.limits.MaxStreams, s.limits.MaxStreamBufferBytes, implied))
	}

	s.baseCtx = context.WithValue(context.Background(), serverKey{}, s)
	s.drainCtx, s.drainStop = context.WithCancel(context.Background())
	s.base = &http.Server{
		Handler: s,
		// net/http owns the parser, so these are set on the underlying server.
		// MaxHeaderBytes is effective from the first byte net/http reads.
		MaxHeaderBytes:    s.limits.MaxHeaderBytes,
		ReadHeaderTimeout: s.limits.ReadHeaderTimeout,
		IdleTimeout:       s.limits.IdleTimeout,
		ReadTimeout:       s.limits.ReadTimeout,
		ErrorLog:          slog.NewLogLogger(s.logger.Handler(), slog.LevelWarn),
		BaseContext:       func(net.Listener) context.Context { return s.baseCtx },
	}
	if s.limits.MaxConnections > 0 || s.limits.MaxPerIP > 0 {
		s.base.ConnState = s.connState
	}
	return s
}

// applyDerivedLimits fills in every limit that is a function of the host rather
// than a choice, and warns when it had to guess.
//
// The point of deriving rather than configuring is that the derived values
// cannot be set inconsistently with each other. MaxStreams follows from the
// memory budget, and MaxTotalStreamBufferBytes follows from MaxStreams, so the
// common misconfiguration - a high stream count paired with a forgotten memory
// cap - is not merely discouraged, it is unrepresentable.
func (s *Server) applyDerivedLimits() {
	if s.limits.MemoryBudget <= 0 {
		budget, source := detectMemoryBudget()
		s.limits.MemoryBudget = budget
		if source == "fallback" {
			// A limit derived from a guess must not look like a measured one.
			s.logger.Warn("no memory limit detected; using the fallback budget",
				"budget", humanBytes(budget),
				"maxStreams", deriveMaxStreams(budget),
				"hint", "set photon.MemoryBudget(n) or GOMEMLIMIT to make this explicit")
		}
	}
	if s.limits.MaxStreams <= 0 {
		s.limits.MaxStreams = deriveMaxStreams(s.limits.MemoryBudget)
	}
	if s.limits.MaxStreamBufferBytes <= 0 {
		s.limits.MaxStreamBufferBytes = defaultStreamBufferBytes
	}
	if s.limits.MaxTotalStreamBufferBytes <= 0 {
		// Derived, never configured beside the count.
		s.limits.MaxTotalStreamBufferBytes =
			int64(s.limits.MaxStreams) * int64(s.limits.MaxStreamBufferBytes)
	}
	if s.limits.StreamStallTimeout <= 0 {
		s.limits.StreamStallTimeout = 30 * Second
	}
	// HeartbeatInterval of 0 means "disabled" and is respected, so only a
	// negative value needs correcting, and negative is rejected by validate.
}

// humanBytes renders a byte count for a log line.
func humanBytes(n int64) string {
	const unit = 1024
	if n < unit {
		return strconv.FormatInt(n, 10) + " B"
	}
	div, exp := int64(unit), 0
	for m := n / unit; m >= unit && exp < 3; m /= unit {
		div *= unit
		exp++
	}
	return strconv.FormatFloat(float64(n)/float64(div), 'f', 1, 64) +
		" " + [...]string{"KiB", "MiB", "GiB", "TiB"}[exp]
}

// validate reports whether the limits are self-consistent.
//
// A limit that is silently ignored is worse than no limit, because it looks
// like protection. Every constraint is therefore checked here, at startup.
func (l Limits) validate() error {
	switch {
	case l.MaxRequestBodyBytes < 0:
		return fmt.Errorf("MaxRequestBodyBytes is %d: use 0 for unlimited, not a negative value",
			l.MaxRequestBodyBytes)
	case l.MaxHeaderBytes < 0:
		// net/http reads a non-positive MaxHeaderBytes as "use the 1 MB
		// default", so a negative value would look like a limit and be 1 MB.
		return fmt.Errorf("MaxHeaderBytes is %d: use 0 for net/http's 1 MB default, not a negative value",
			l.MaxHeaderBytes)
	case l.MaxHeaders < 0:
		return fmt.Errorf("MaxHeaders is %d: use 0 for unlimited, not a negative value", l.MaxHeaders)
	case l.MaxHeaderValueBytes < 0:
		return fmt.Errorf("MaxHeaderValueBytes is %d: use 0 for unlimited, not a negative value",
			l.MaxHeaderValueBytes)
	case l.ReadHeaderTimeout < 0:
		return fmt.Errorf("ReadHeaderTimeout is %s: use 0 for no timeout", l.ReadHeaderTimeout)
	case l.ReadTimeout < 0:
		return fmt.Errorf("ReadTimeout is %s: use 0 for no timeout", l.ReadTimeout)
	case l.IdleTimeout < 0:
		return fmt.Errorf("IdleTimeout is %s: use 0 for no timeout", l.IdleTimeout)
	case l.MaxConnections < 0:
		return fmt.Errorf("MaxConnections is %d: use 0 for unlimited, not a negative value", l.MaxConnections)
	case l.MaxConcurrentRequests < 0:
		return fmt.Errorf("MaxConcurrentRequests is %d: use 0 for unlimited, not a negative value",
			l.MaxConcurrentRequests)
	case l.MaxPerIP < 0:
		return fmt.Errorf("MaxPerIP is %d: use 0 for unlimited, not a negative value", l.MaxPerIP)
	}
	if l.MaxHeaderBytes > 0 && l.MaxHeaderValueBytes > l.MaxHeaderBytes {
		return fmt.Errorf(
			"MaxHeaderValueBytes is %d, larger than MaxHeaderBytes %d: "+
				"the value limit would never apply",
			l.MaxHeaderValueBytes, l.MaxHeaderBytes)
	}
	// The streaming limits are checked for the same reason the others are: a
	// limit that is silently ignored looks like protection and is not.
	if l.MaxStreams < 0 {
		return fmt.Errorf("MaxStreams is %d: use 0 for the derived default, not a negative value", l.MaxStreams)
	}
	if l.MaxStreamBufferBytes < 0 {
		return fmt.Errorf("MaxStreamBufferBytes is %d: use 0 for the default, not a negative value",
			l.MaxStreamBufferBytes)
	}
	if l.StreamStallTimeout < 0 {
		return fmt.Errorf("StreamStallTimeout is %s: use 0 for the default", l.StreamStallTimeout)
	}
	if l.HeartbeatInterval < 0 {
		return fmt.Errorf("HeartbeatInterval is %s: use 0 to disable heartbeats", l.HeartbeatInterval)
	}
	if l.MemoryBudget < 0 {
		return fmt.Errorf("MemoryBudget is %d: use 0 to detect it, not a negative value", l.MemoryBudget)
	}
	// A global cap below the product of the count and the per-stream size is a
	// contradiction, not a stricter policy: the streams the count permits
	// cannot all fit under the cap. The derived default is by construction
	// equal to the product, so only explicit values are checked.
	if l.MaxStreams > 0 && l.MaxStreamBufferBytes > 0 && l.MaxTotalStreamBufferBytes > 0 {
		implied := int64(l.MaxStreams) * int64(l.MaxStreamBufferBytes)
		if l.MaxTotalStreamBufferBytes < implied {
			return fmt.Errorf(
				"MaxTotalStreamBufferBytes is %d, below MaxStreams x MaxStreamBufferBytes (%d x %d = %d): "+
					"the global cap is derived from the count and the buffer size, and setting it lower "+
					"means the cap would silently govern only some streams",
				l.MaxTotalStreamBufferBytes, l.MaxStreams, l.MaxStreamBufferBytes, implied)
		}
	}
	return nil
}

// Routing verbs. They mirror the Router's, and exist so a program can register
// routes without holding a separate reference to the router.

// GET registers a handler for GET requests to path. h may be a plain function.
func (s *Server) GET(path string, h http.HandlerFunc, mw ...Middleware) {
	s.rt.handle(http.MethodGet, path, handlerOf(h), mw)
}

// POST registers a handler for POST requests to path.
func (s *Server) POST(path string, h http.HandlerFunc, mw ...Middleware) {
	s.rt.handle(http.MethodPost, path, handlerOf(h), mw)
}

// PUT registers a handler for PUT requests to path.
func (s *Server) PUT(path string, h http.HandlerFunc, mw ...Middleware) {
	s.rt.handle(http.MethodPut, path, handlerOf(h), mw)
}

// PATCH registers a handler for PATCH requests to path.
func (s *Server) PATCH(path string, h http.HandlerFunc, mw ...Middleware) {
	s.rt.handle(http.MethodPatch, path, handlerOf(h), mw)
}

// DELETE registers a handler for DELETE requests to path.
func (s *Server) DELETE(path string, h http.HandlerFunc, mw ...Middleware) {
	s.rt.handle(http.MethodDelete, path, handlerOf(h), mw)
}

// HEAD registers a handler for HEAD requests to path.
func (s *Server) HEAD(path string, h http.HandlerFunc, mw ...Middleware) {
	s.rt.handle(http.MethodHead, path, handlerOf(h), mw)
}

// OPTIONS registers a handler for OPTIONS requests to path.
func (s *Server) OPTIONS(path string, h http.HandlerFunc, mw ...Middleware) {
	s.rt.handle(http.MethodOptions, path, handlerOf(h), mw)
}

// Handle registers an http.Handler for a method and path. It panics on a
// malformed or conflicting pattern; see Router.Handle.
func (s *Server) Handle(method, path string, h http.Handler, mw ...Middleware) {
	s.rt.handle(method, path, h, mw)
}

// Group returns a group of routes sharing a prefix and middleware. See Group.
func (s *Server) Group(prefix string, mw ...Middleware) *Group { return s.rt.Group(prefix, mw...) }

// Mount serves an existing http.Handler for every method under prefix. See
// Router.Mount.
func (s *Server) Mount(prefix string, h http.Handler) { s.rt.Mount(prefix, h) }

// Use adds middleware that runs for every request, including 404 and 405
// responses, whether routes were registered before or after it. See Router.Use.
func (s *Server) Use(mw ...Middleware) { s.rt.Use(mw...) }

// NotFound sets the handler used when no route matches.
func (s *Server) NotFound(h http.Handler) { s.rt.NotFound(h) }

// MethodNotAllowed sets the handler used when a path matches another method.
func (s *Server) MethodNotAllowed(h http.Handler) { s.rt.MethodNotAllowed(h) }

// Routes returns the registered routes in registration order.
func (s *Server) Routes() []RouteInfo { return s.rt.Routes() }

// Limits returns a copy of the server's resource budget, with derived values
// filled in.
func (s *Server) Limits() Limits { return s.limits }

// Router returns the underlying router.
func (s *Server) Router() *Router { return s.rt }

// ServeHTTP dispatches a request, enforcing the server's limits first.
//
// The order is deliberate and is the whole admission-control design: the
// concurrency budget is checked before the request is touched, the header
// limits before the router runs, and the body limit is installed as a reader so
// a handler cannot read past it by accident.
//
// A static route served through Serve costs no allocations here: the server is
// already in the request context, the panic handler is a plain defer, and a
// request without a body is not wrapped.
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// A request that arrived through Serve already carries this server in its
	// base context. One that did not - httptest, or a Server mounted inside
	// another mux - gets it attached here, so a Stream created from it is
	// still bounded by this server's limits.
	if serverFromContext(r) != s {
		r = withServer(r, s)
	}

	if limit := s.limits.MaxConcurrentRequests; limit > 0 {
		if n := s.active.Add(1); n > int64(limit) {
			s.active.Add(-1)
			s.limitsError(w, r, http.StatusServiceUnavailable,
				"server_at_capacity",
				"the server is at its concurrency limit, retry shortly",
				"MaxConcurrentRequests")
			return
		}
		defer s.active.Add(-1)
	}
	if !s.checkHeaders(w, r) {
		return
	}
	if limit := s.limits.MaxRequestBodyBytes; limit > 0 && r.Body != nil && r.Body != http.NoBody {
		if r.ContentLength > limit {
			// Refused before a byte is read, when the client declared the size.
			w.Header().Set("Connection", "close")
			// The same code DecodeJSON uses when a body overflows mid-read, so a
			// client sees one answer whether or not it declared the length.
			s.limitsError(w, r, http.StatusRequestEntityTooLarge,
				photonerr.ErrLimitExceeded.Code, "the request body is larger than the server accepts",
				"MaxRequestBodyBytes")
			return
		}
		// http.MaxBytesReader rather than a hand-rolled reader: it reports a
		// body of exactly the limit as complete rather than as too large, it
		// returns the standard *http.MaxBytesError that DecodeJSON and every
		// net/http-aware library recognise, and it tells net/http not to reuse
		// the connection once the limit is hit.
		r.Body = http.MaxBytesReader(w, r.Body, limit)
	}

	defer s.recoverPanic(w, r)
	s.rt.ServeHTTP(w, r)
}

// checkHeaders enforces the header limits net/http has no concept of.
//
// MaxHeaders counts header FIELDS, not distinct names. Counting names, as
// len(r.Header) does, lets a request send thousands of copies of one header -
// "X-A: 1" repeated - and pass a limit of 100 while costing exactly the parsing
// work the limit exists to bound.
func (s *Server) checkHeaders(w http.ResponseWriter, r *http.Request) bool {
	maxFields, maxValue := s.limits.MaxHeaders, s.limits.MaxHeaderValueBytes
	if maxFields <= 0 && maxValue <= 0 {
		return true
	}
	fields := 0
	for k, vs := range r.Header {
		fields += len(vs)
		if maxFields > 0 && fields > maxFields {
			s.limitsError(w, r, http.StatusRequestHeaderFieldsTooLarge,
				"too_many_headers",
				"the request has too many header fields",
				"MaxHeaders")
			return false
		}
		if maxValue > 0 {
			for _, v := range vs {
				if len(v) > maxValue {
					// The header name is an RFC token chosen by the client but
					// rendered through the JSON encoder, so it cannot break out
					// of the document; the value is never echoed.
					s.limitsError(w, r, http.StatusRequestHeaderFieldsTooLarge,
						"header_value_too_large",
						"header "+k+" is too large",
						"MaxHeaderValueBytes")
					return false
				}
			}
		}
	}
	return true
}

// recoverPanic turns a panicking handler into a 500 rather than a dead
// process. It is deferred directly by ServeHTTP, so it costs no allocation.
//
// net/http already recovers a panic per connection, but it logs through the
// standard library logger and closes the connection without a response.
// Recovering here reports through the server's own logger and OnPanic hook and
// answers the client.
func (s *Server) recoverPanic(w http.ResponseWriter, r *http.Request) {
	v := recover()
	if v == nil {
		return
	}
	if v == http.ErrAbortHandler {
		// The documented way to abort a response without a stack trace. A
		// stream that panicked mid-answer is reported before it aborts.
		panic(v)
	}
	s.reportPanic(v, r)
	// A 500 with a constant body: the panic value can contain anything the
	// handler put in it, including secrets.
	writeText(w, http.StatusInternalServerError, "500 internal server error\n")
}

// reportPanic records a recovered panic through OnPanic and the logger.
func (s *Server) reportPanic(v any, r *http.Request) {
	info := &PanicInfo{
		Value:  v,
		Stack:  stack(),
		Method: r.Method,
		Path:   r.URL.Path,
	}
	s.onPanic(info)
	s.logger.Error("panic in handler",
		"method", info.Method, "path", info.Path,
		"panic", fmt.Sprint(v), "stack", string(info.Stack))
}

// limitsError writes a limit response as an RFC 9457 problem document. The body
// names the limit, not the request content, so it is safe to send and useful
// to whoever operates the server.
func (s *Server) limitsError(w http.ResponseWriter, r *http.Request, status int, code, msg, limit string) {
	// Rate-limited like refused connections: a flood of oversized requests
	// is the limits doing their job, and must not become a flood of log lines.
	if log, skipped := s.rejections.allow(time.Second); log {
		s.logger.Warn("request rejected",
			"limit", limit, "code", code, "method", r.Method, "path", r.URL.Path, "suppressed", skipped)
	}
	if status == http.StatusServiceUnavailable {
		// Tells a well-behaved client to back off rather than retry in a tight
		// loop against a server that is already at capacity.
		w.Header().Set("Retry-After", "1")
	}
	_ = photonerr.Render(w, &photonerr.Error{Status: status, Code: code, Message: msg, Limit: limit},
		photonerr.RenderOptions{})
}

// Run serves on addr until the process receives SIGINT or SIGTERM, then shuts
// down gracefully and returns.
//
//	if err := app.Run(":8080"); err != nil {
//		log.Fatal(err)
//	}
//
// The address is bound before Run returns control to the accept loop, so a
// port already in use is reported immediately. On a signal, Run stops accepting
// connections, signals active streams through Stream.Done, and waits up to the
// ShutdownTimeout for in-flight work. A second signal during the wait exits the
// process immediately.
//
// It returns nil after a clean shutdown and the context error if the wait ran
// out, in which case remaining connections were closed.
func (s *Server) Run(addr string) error {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("photon: listen on %s: %w", addr, err)
	}
	ctx, stop := signal.NotifyContext(context.Background(), shutdownSignals...)
	defer stop()

	errc := make(chan error, 1)
	go func() { errc <- s.Serve(ln) }()
	s.logger.Info("photon: listening", "addr", ln.Addr().String())

	select {
	case err := <-errc:
		return err
	case <-ctx.Done():
	}
	// Restore default signal handling, so a second Ctrl-C kills the process
	// rather than being swallowed while the drain waits.
	stop()
	s.logger.Info("photon: shutting down", "timeout", s.shutdownTimeout.String())

	shCtx, cancel := context.WithTimeout(context.Background(), s.shutdownTimeout)
	defer cancel()
	shErr := s.Shutdown(shCtx)
	if err := <-errc; err != nil {
		return err
	}
	return shErr
}

// Listen serves on addr and blocks until the server is shut down.
//
// It returns nil after a Shutdown or Close, so the common case needs no error
// handling. Run is the same plus signal handling and graceful shutdown.
func (s *Server) Listen(addr string) error {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("photon: listen on %s: %w", addr, err)
	}
	return s.Serve(ln)
}

// ListenTLS serves HTTPS on addr using the given certificate and key.
//
// TLS 1.2 is the floor and early data is not enabled: 0-RTT is replayable by
// definition (RFC 8470), and a replayable request is worse than a slow one.
// HTTP/2 is negotiated automatically. For certificate reloading or ACME, build
// a tls.Config and call Serve with tls.NewListener.
func (s *Server) ListenTLS(addr, certFile, keyFile string) error {
	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		return fmt.Errorf("photon: load certificate: %w", err)
	}
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("photon: listen on %s: %w", addr, err)
	}
	return s.Serve(tls.NewListener(ln, &tls.Config{
		Certificates: []tls.Certificate{cert},
		MinVersion:   tls.VersionTLS12,
		NextProtos:   []string{"h2", "http/1.1"},
	}))
}

// Serve accepts connections on ln and blocks until the server is shut down.
//
// It returns nil after a Shutdown or Close, and an error if serving failed. It
// can be called once per Server.
func (s *Server) Serve(ln net.Listener) error {
	s.mu.Lock()
	if s.started {
		s.mu.Unlock()
		return errors.New("photon: Serve called more than once on the same server")
	}
	s.started = true
	s.mu.Unlock()

	// The router becomes immutable here. Registering a route after this point
	// would be a data race on a live server, so it panics instead, and lookups
	// stop taking a lock.
	s.rt.Freeze()

	if s.limits.MaxConnections > 0 || s.limits.MaxPerIP > 0 {
		ln = s.limitListener(ln)
	}

	err := s.base.Serve(ln)
	if errors.Is(err, http.ErrServerClosed) {
		return nil
	}
	return err
}

// Shutdown stops accepting new connections and waits for in-flight requests
// and streams to finish, up to ctx's deadline.
//
// Active streams are told through Stream.Done as soon as Shutdown begins, so a
// handler can send a final event and return instead of being cut off.
//
// If the deadline passes first, the remaining connections are closed and
// Shutdown returns ctx.Err. That is the honest outcome: a deadline that expired
// has not given the work time to finish, and leaving those connections open
// would make the deadline advisory.
func (s *Server) Shutdown(ctx context.Context) error {
	s.drainStop()
	err := s.base.Shutdown(ctx)
	if err != nil && ctx.Err() != nil {
		_ = s.base.Close()
	}
	return err
}

// Close stops the server immediately, closing every connection and aborting
// in-flight requests. Use Shutdown for a graceful stop.
func (s *Server) Close() error {
	s.drainStop()
	return s.base.Close()
}

// ---------------------------------------------------------------- logging

// defaultLogger forwards errors to slog.Default and drops everything below.
//
// Errors are the panics and 5xx responses an operator must see; a server that
// swallowed them by default would turn every crash into a silent 500. Warnings
// are dropped because the most common one is a rejected request, and logging
// every rejection by default would hand an attacker a log flood.
var defaultLogger = slog.New(errorsOnlyHandler{})

// errorsOnlyHandler resolves slog.Default on every record, so an application
// that calls slog.SetDefault after New still gets its handler.
type errorsOnlyHandler struct {
	attrs []slog.Attr
	group string
}

func (h errorsOnlyHandler) Enabled(ctx context.Context, l slog.Level) bool {
	return l >= slog.LevelError && slog.Default().Handler().Enabled(ctx, l)
}

func (h errorsOnlyHandler) Handle(ctx context.Context, r slog.Record) error {
	next := slog.Default().Handler()
	if h.group != "" {
		next = next.WithGroup(h.group)
	}
	if len(h.attrs) > 0 {
		next = next.WithAttrs(h.attrs)
	}
	return next.Handle(ctx, r)
}

func (h errorsOnlyHandler) WithAttrs(as []slog.Attr) slog.Handler {
	h.attrs = append(append([]slog.Attr(nil), h.attrs...), as...)
	return h
}

func (h errorsOnlyHandler) WithGroup(name string) slog.Handler {
	if h.group != "" {
		name = h.group + "." + name
	}
	h.group = name
	return h
}

// logLimiter allows one log line per interval and counts the rest, so a
// sustained flood produces a steady trickle of summary lines instead of one
// line per event.
type logLimiter struct {
	last       atomic.Int64
	suppressed atomic.Int64
}

// allow reports whether a line may be logged now, and how many were suppressed
// since the last one that was.
func (l *logLimiter) allow(every time.Duration) (bool, int64) {
	now := time.Now().UnixNano()
	last := l.last.Load()
	if now-last < int64(every) || !l.last.CompareAndSwap(last, now) {
		l.suppressed.Add(1)
		return false, 0
	}
	return true, l.suppressed.Swap(0)
}
