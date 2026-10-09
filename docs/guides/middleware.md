# Middleware

Photon middleware is `func(http.Handler) http.Handler`, the same shape the
whole `net/http` ecosystem uses, so existing middleware works unchanged. This
guide explains where middleware runs and in what order, then gives complete
versions of the middleware most AI backends need: request IDs, access logs,
CORS, bearer-token auth, per-client rate limiting, and request deadlines.

- [The type](#the-type)
- [Where middleware runs](#where-middleware-runs)
- [Global middleware covers every request](#global-middleware-covers-every-request)
- [Group and per-route middleware](#group-and-per-route-middleware)
- [Middleware runs after routing](#middleware-runs-after-routing)
- [Wrapping the ResponseWriter: implement Unwrap](#wrapping-the-responsewriter-implement-unwrap)
- [Request ID](#request-id)
- [Access log](#access-log)
- [CORS](#cors)
- [Bearer-token auth in a group](#bearer-token-auth-in-a-group)
- [Rate limiting per client](#rate-limiting-per-client)
- [Timeouts](#timeouts)
- [Panics and recovery middleware](#panics-and-recovery-middleware)
- [Putting it together](#putting-it-together)

## The type

```go
type Middleware func(http.Handler) http.Handler
```

There is no photon-specific interface. A middleware receives the next handler
and returns a handler that does something before or after calling it, or
instead of calling it:

```go
func timing(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		next.ServeHTTP(w, r)
		log.Printf("%s %s took %s", r.Method, r.URL.Path, time.Since(start))
	})
}
```

A chain `(a, b, c)` runs as `a(b(c(handler)))`: `a` is outermost and sees the
request first.

## Where middleware runs

For a request that matches a route:

```text
photon admission checks and panic recovery
  routing (sets r.Pattern and the path values)
    global middleware, in the order Use was called
      group middleware, parent group before child group
        per-route middleware, in argument order
          handler
```

For a request that matches no route, global middleware runs around the 404 or
405 response instead.

Two things happen before any middleware:

- **Admission checks.** The concurrency limit, the header limits, and a body
  whose declared `Content-Length` is over the limit are answered (503, 431,
  413) before routing. Those requests never reach your middleware, so your
  access log will not see them. Pass `photon.Logger` to see them in photon's
  own log; see [configuration](configuration.md#logging).
- **The body limit is installed.** `r.Body` is already wrapped in
  `http.MaxBytesReader`, so a middleware that reads the body cannot read past
  the limit either.

Photon's panic recovery sits outside everything, so it also recovers panics in
middleware.

## Global middleware covers every request

`app.Use` adds middleware that runs for every request the server answers:

- every route, **whether it was registered before or after the `Use` call**;
- the 404 and 405 responses, including custom `NotFound` and
  `MethodNotAllowed` handlers.

Applying it to misses is deliberate. A rate limiter must count a flood of 404s,
an access log must record them, and a CORS middleware must be able to answer a
preflight `OPTIONS` for a route that registered only `POST`. Because global
middleware runs on the 405, that last case needs no `OPTIONS` routes at all;
see [CORS](#cors).

The order between `Use` calls matters, because it is the order the middleware
runs in. The position of `Use` relative to route registration does not.
Calling `Use` after the server has started panics.

## Group and per-route middleware

Middleware passed to a group applies to every route registered through that
group. It runs inside global middleware and outside per-route middleware.
Nested groups run the parent's middleware first.

```go
api := app.Group("/v1", requireBearer(keys))
api.GET("/models", listModels)

admin := api.Group("/admin", requireRole("admin"))
admin.GET("/usage", usage)          // requireBearer, then requireRole
```

A group is the right place for authentication. A route outside the group
cannot pick it up by accident, and a route inside it cannot forget it.

`Group.Use` is order-dependent, unlike `app.Use`: it applies to routes
registered on the group after the call. That lets a group hold a public route
followed by protected ones:

```go
auth := app.Group("/auth")
auth.POST("/login", login)        // public
auth.Use(requireSession)
auth.GET("/me", me)               // requires a session
```

Per-route middleware is the trailing variadic argument of every registration
method. It runs innermost, in argument order:

```go
app.GET("/search", search, deadline(5*time.Second))
app.POST("/admin/reindex", reindex, requireBearer(keys), requireRole("admin"))
```

## Middleware runs after routing

Routing happens before any of your middleware runs, so middleware can read
`photon.PathParam(r, "id")`, `r.PathValue("id")`, and `r.Pattern`. An access
log or a metrics middleware gets a low-cardinality route label (`/users/:id`)
for free, and an authorization middleware can check that the `:org` in the
path belongs to the caller.

The flip side: middleware cannot change which route runs. Rewriting
`r.URL.Path` or `r.Method` in middleware has no effect on dispatch. Do that
kind of rewriting at your proxy.

## Wrapping the ResponseWriter: implement Unwrap

Middleware that wraps the `http.ResponseWriter` (to capture the status code,
count bytes, compress) must implement:

```go
func (w *myWriter) Unwrap() http.ResponseWriter { return w.ResponseWriter }
```

Photon's streams flush every event and set a write deadline through
`http.ResponseController`. The controller looks for `Flush` and
`SetWriteDeadline` on the writer it is given; when they are not there, it
calls `Unwrap` and tries the writer underneath. A wrapper without `Unwrap`
hides whatever it does not implement itself:

- **If it hides `Flush`**, events stop reaching the client as they are
  produced, the stream fails, and photon logs an error saying a middleware is
  wrapping the writer without an `Unwrap` method.
- **If it implements `Flush` but hides `SetWriteDeadline`** (common in older
  middleware), streaming appears to work, but a client that stops reading can
  no longer be cut off by a socket deadline, and a writer goroutine stays
  blocked until the connection dies. Photon logs a warning about it once per
  server, visible when you pass `photon.Logger`.

`Unwrap` fixes both.

Third-party code that type-asserts `w.(http.Flusher)` or `w.(http.Hijacker)`
instead of using `http.ResponseController` will not see through a wrapper even
with `Unwrap`. If you depend on such code, add the methods it asserts.

A compression middleware has the same problem in a stronger form: most of them
buffer. Do not compress `text/event-stream` responses unless the middleware
flushes on every `Flush`.

## Request ID

Each example below is a complete file in `package main`. Together with the
`main.go` in [Putting it together](#putting-it-together) they form one program.

```go
// requestid.go
package main

import (
	"context"
	"crypto/rand"
	"net/http"
)

type requestIDKey struct{}

// RequestIDFrom returns the id that requestID attached to ctx, or "".
func RequestIDFrom(ctx context.Context) string {
	id, _ := ctx.Value(requestIDKey{}).(string)
	return id
}

// requestID gives every request an id, returns it in X-Request-ID, and stores
// it in the request context for handlers and loggers.
func requestID(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id := r.Header.Get("X-Request-ID")
		if !plainID(id) {
			id = rand.Text() // 26 random base32 characters (Go 1.24)
		}
		w.Header().Set("X-Request-ID", id)
		ctx := context.WithValue(r.Context(), requestIDKey{}, id)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

// plainID accepts an incoming id only if it is short and made of safe
// characters, so a client cannot use it to forge log lines.
func plainID(s string) bool {
	if s == "" || len(s) > 64 {
		return false
	}
	for i := 0; i < len(s); i++ {
		c := s[i]
		ok := c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' ||
			c == '-' || c == '_' || c == '.'
		if !ok {
			return false
		}
	}
	return true
}
```

An incoming `X-Request-ID` is reused so a trace can span your proxy and your
service. It is client-controlled, so it is validated, and it should never be
used for anything but correlation. `r.WithContext` keeps the path values and
`r.Pattern`, so handlers further in still see them.

## Access log

```go
// accesslog.go
package main

import (
	"log/slog"
	"net/http"
	"sync/atomic"
	"time"

	"github.com/agenticmarket/photon"
)

// statusRecorder captures the status code and body size for the access log.
type statusRecorder struct {
	http.ResponseWriter
	status int
	// bytes is atomic because a photon stream writes from its own goroutine.
	bytes atomic.Int64
}

func (w *statusRecorder) WriteHeader(code int) {
	if w.status == 0 && code >= 200 { // 1xx responses are informational
		w.status = code
	}
	w.ResponseWriter.WriteHeader(code)
}

func (w *statusRecorder) Write(p []byte) (int, error) {
	if w.status == 0 {
		w.status = http.StatusOK
	}
	n, err := w.ResponseWriter.Write(p)
	w.bytes.Add(int64(n))
	return n, err
}

// Unwrap lets http.ResponseController reach the real writer's Flush and
// SetWriteDeadline. Without it, a stream behind this middleware cannot flush.
func (w *statusRecorder) Unwrap() http.ResponseWriter { return w.ResponseWriter }

func accessLog(logger *slog.Logger) photon.Middleware {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			start := time.Now()
			rec := &statusRecorder{ResponseWriter: w}
			next.ServeHTTP(rec, r)

			status := rec.status
			if status == 0 {
				status = http.StatusOK // nothing was written: net/http sends 200
			}
			logger.LogAttrs(r.Context(), slog.LevelInfo, "request",
				slog.String("method", r.Method),
				slog.String("route", r.Pattern), // "/users/:id"; empty for a 404
				slog.String("path", r.URL.Path),
				slog.Int("status", status),
				slog.Int64("bytes", rec.bytes.Load()),
				slog.Duration("took", time.Since(start)),
				slog.String("peer", photon.ClientIP(r).String()),
				slog.String("request_id", RequestIDFrom(r.Context())),
			)
		})
	}
}
```

Notes:

- `Unwrap` is the line that keeps streaming working. See
  [above](#wrapping-the-responsewriter-implement-unwrap).
- For a stream, `took` is the length of the whole stream and `bytes` counts
  everything sent, heartbeats included.
- Log `route` rather than `path` as a metrics label. The path is unbounded and
  client-controlled; the route is one of the patterns you registered.
- A request that panics is not logged here: the panic unwinds through this
  middleware before photon writes the 500. Photon logs the panic itself and
  calls your `OnPanic` hook.
- Do not log `Authorization`, cookies, or query strings that may carry
  secrets.

## CORS

```go
// cors.go
package main

import (
	"net/http"

	"github.com/agenticmarket/photon"
)

// cors allows cross-origin requests from an explicit list of origins and
// answers preflights itself.
func cors(origins ...string) photon.Middleware {
	allowed := make(map[string]bool, len(origins))
	for _, o := range origins {
		allowed[o] = true
	}
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			h := w.Header()
			// The response depends on Origin, so shared caches must key on it.
			h.Add("Vary", "Origin")

			origin := r.Header.Get("Origin")
			if origin == "" || !allowed[origin] {
				// Not a cross-origin browser request, or not an allowed one. Add
				// no CORS headers; the browser will keep the response from the page.
				next.ServeHTTP(w, r)
				return
			}
			h.Set("Access-Control-Allow-Origin", origin)
			h.Set("Access-Control-Allow-Credentials", "true") // only if you use cookies

			if r.Method == http.MethodOptions && r.Header.Get("Access-Control-Request-Method") != "" {
				// A preflight. Answer it here, before auth runs: browsers send
				// preflights without credentials.
				h.Add("Vary", "Access-Control-Request-Method")
				h.Add("Vary", "Access-Control-Request-Headers")
				h.Set("Access-Control-Allow-Methods", "GET, POST, PUT, PATCH, DELETE")
				h.Set("Access-Control-Allow-Headers", "Authorization, Content-Type")
				h.Set("Access-Control-Max-Age", "600")
				w.WriteHeader(http.StatusNoContent)
				return
			}
			next.ServeHTTP(w, r)
		})
	}
}
```

Register it with `app.Use`. Two properties of global middleware make this
work:

- **No `OPTIONS` routes are needed.** A preflight to a `POST`-only route is a
  405 as far as the router is concerned, and global middleware runs around the
  405, so `cors` answers the preflight before the 405 is written.
- **It runs before authentication.** Auth lives in a group or on a route, which
  is inside global middleware, so a credential-less preflight never reaches it.
  If auth ran first, the preflight would get a 401 without CORS headers, and
  the browser would report a CORS error instead of the real problem.

Use an allowlist. Reflecting whatever `Origin` arrives, with credentials
allowed, is the same as having no CORS policy. Leave out
`Access-Control-Allow-Credentials` if you authenticate with bearer tokens
rather than cookies.

`EventSource` cannot send an `Authorization` header. A browser client that
streams with `EventSource` authenticates with a cookie (and needs
`withCredentials: true` plus the credentials header above), or uses `fetch` and
reads the stream itself.

## Bearer-token auth in a group

```go
// auth.go
package main

import (
	"context"
	"crypto/sha256"
	"net/http"
	"strings"

	"github.com/agenticmarket/photon"
	"github.com/agenticmarket/photon/photonerr"
)

// Principal is who a request is authenticated as.
type Principal struct {
	ID   string
	Role string
}

type principalKey struct{}

// PrincipalFrom returns the authenticated principal, if any.
func PrincipalFrom(ctx context.Context) (Principal, bool) {
	p, ok := ctx.Value(principalKey{}).(Principal)
	return p, ok
}

// requireBearer authenticates "Authorization: Bearer <key>". keys maps the
// SHA-256 digest of each API key to the principal it identifies. Looking up a
// digest, rather than comparing key strings byte by byte, means response
// timing cannot reveal how much of a guessed key was right.
func requireBearer(keys map[[32]byte]Principal) photon.Middleware {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			scheme, key, ok := strings.Cut(r.Header.Get("Authorization"), " ")
			if !ok || !strings.EqualFold(scheme, "Bearer") || key == "" {
				unauthorized(w, r)
				return
			}
			p, ok := keys[sha256.Sum256([]byte(key))]
			if !ok {
				unauthorized(w, r) // the same answer as a missing key
				return
			}
			ctx := context.WithValue(r.Context(), principalKey{}, p)
			next.ServeHTTP(w, r.WithContext(ctx))
		})
	}
}

// requireRole refuses a principal without the given role. It fails closed if
// requireBearer did not run.
func requireRole(role string) photon.Middleware {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			p, ok := PrincipalFrom(r.Context())
			if !ok {
				unauthorized(w, r)
				return
			}
			if p.Role != role {
				photon.Error(w, r, photonerr.Forbidden(""))
				return
			}
			next.ServeHTTP(w, r)
		})
	}
}

func unauthorized(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("WWW-Authenticate", `Bearer realm="api"`)
	photon.Error(w, r, photonerr.Unauthorized(""))
}
```

`photon.Error` renders a `*photonerr.Error` as an RFC 9457 problem document
with its own status, so a refusal looks like this:

```json
{"type":"https://photon.agenticmarket.dev/errors/unauthorized","title":"unauthorized","status":401,"code":"unauthorized"}
```

Things this gets right that hand-written auth often gets wrong: a missing key
and a wrong key get the same response, so the API does not confirm which keys
exist; the principal travels in the request context, never in a field shared
by concurrent requests; and `requireRole` refuses rather than panics when
authentication did not run.

Authentication is not authorization. A handler for `/v1/orgs/:org/files/:id`
must still check that the principal may see that org and that file.

## Rate limiting per client

A token bucket per client address. Clients get `burst` requests at once and
`perSecond` on average after that.

```go
// ratelimit.go
package main

import (
	"math"
	"net/http"
	"net/netip"
	"strconv"
	"sync"
	"time"

	"github.com/agenticmarket/photon"
	"github.com/agenticmarket/photon/photonerr"
)

// rateLimit allows each client perSecond requests on average, in bursts of up
// to burst requests.
func rateLimit(perSecond float64, burst int) photon.Middleware {
	l := &limiter{
		rate:    perSecond,
		burst:   float64(burst),
		buckets: make(map[netip.Prefix]*bucket),
	}
	// A bucket idle this long has refilled completely, so forgetting it
	// changes nothing.
	l.idle = time.Duration(l.burst / l.rate * float64(time.Second))

	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			ok, wait := l.take(clientKey(r), time.Now())
			if !ok {
				w.Header().Set("Retry-After", strconv.Itoa(int(math.Ceil(wait.Seconds()))))
				photon.Error(w, r, photonerr.TooManyRequests(""))
				return
			}
			next.ServeHTTP(w, r)
		})
	}
}

type bucket struct {
	tokens float64
	last   time.Time
}

type limiter struct {
	mu        sync.Mutex
	rate      float64
	burst     float64
	idle      time.Duration
	buckets   map[netip.Prefix]*bucket
	lastSweep time.Time
}

func (l *limiter) take(key netip.Prefix, now time.Time) (bool, time.Duration) {
	l.mu.Lock()
	defer l.mu.Unlock()

	// Forget idle clients once a minute, so the map only holds active ones.
	if now.Sub(l.lastSweep) > time.Minute {
		for k, b := range l.buckets {
			if now.Sub(b.last) > l.idle {
				delete(l.buckets, k)
			}
		}
		l.lastSweep = now
	}

	b := l.buckets[key]
	if b == nil {
		b = &bucket{tokens: l.burst, last: now}
		l.buckets[key] = b
	}
	b.tokens = min(l.burst, b.tokens+now.Sub(b.last).Seconds()*l.rate)
	b.last = now
	if b.tokens >= 1 {
		b.tokens--
		return true, 0
	}
	return false, time.Duration((1 - b.tokens) / l.rate * float64(time.Second))
}

// clientKey identifies a client by its peer address. IPv6 clients are grouped
// by /64, because one client usually controls a whole /64.
func clientKey(r *http.Request) netip.Prefix {
	ip := photon.ClientIP(r).Unmap()
	bits := 32
	if ip.Is6() {
		bits = 64
	}
	p, _ := ip.Prefix(bits) // an unparseable peer gets the zero prefix
	return p
}
```

Register it with `app.Use`, so it also counts requests for routes that do not
exist.

`photon.ClientIP` returns the socket peer and ignores `X-Forwarded-For`,
because that header is set by the client and keying a limit on it lets anyone
bypass the limit by inventing addresses. Behind a reverse proxy or load
balancer, every request comes from the proxy and all clients share one bucket.
In that deployment, replace `photon.ClientIP` in `clientKey` with a function
that trusts forwarded headers only from your own proxies; the
[security guide](security.md#trusted-proxies-and-the-client-address) has one.

This limiter is per process. With several instances, each enforces its own
limit; use a shared store or rate-limit at the edge if you need one global
budget. For AI APIs a request rate is rarely the right unit on its own: limit
tokens or concurrent streams per principal too.

The connection limits in `photon.Limits` (`MaxConnections`, `MaxPerIP`) are a
different axis. A client can send thousands of requests over one kept-alive
connection without tripping a connection limit.

## Timeouts

Do not use `http.TimeoutHandler` on routes that stream. It buffers the entire
response in memory until the handler returns, and its writer cannot flush, so a
photon stream behind it fails as soon as it tries to send its first event. On
ordinary routes it still holds every response in memory.

For a non-streaming route, bound the request with a context deadline and a
write deadline instead:

```go
// deadline.go
package main

import (
	"context"
	"net/http"
	"time"

	"github.com/agenticmarket/photon"
)

// deadline bounds a non-streaming request: its context is cancelled after d,
// and a write still blocked at d fails instead of waiting for the client.
func deadline(d time.Duration) photon.Middleware {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			ctx, cancel := context.WithTimeout(r.Context(), d)
			defer cancel()

			rc := http.NewResponseController(w)
			_ = rc.SetWriteDeadline(time.Now().Add(d))
			defer rc.SetWriteDeadline(time.Time{}) // do not leave it on a kept-alive connection

			next.ServeHTTP(w, r.WithContext(ctx))
		})
	}
}
```

The context deadline only helps if the handler passes `r.Context()` to the work
it does (database queries, upstream calls). When it expires, return a 504:

```go
if errors.Is(err, context.DeadlineExceeded) {
	photon.Error(w, r, photonerr.Timeout())
	return
}
```

Apply `deadline` per route or through a group, never globally, because a
streaming route must be able to run for as long as the answer takes. A group
with an empty prefix is a convenient way to apply it to a set of routes:

```go
quick := app.Group("", deadline(10*time.Second))
quick.GET("/v1/models", listModels)
quick.GET("/v1/search", search)
```

Streams have their own protection against stuck clients
(`StreamStallTimeout`); see [streaming](../guides/streaming.md#slow-clients-and-back-pressure).
Photon leaves `ReadTimeout` at zero for the same reason; see
[configuration](configuration.md#limits-reference).

## Panics and recovery middleware

You do not need recovery middleware. Photon recovers a panic in any handler or
middleware, answers `500 internal server error` with a constant body (the
panic value can contain anything, including secrets), logs it with a stack
trace, and calls the `OnPanic` hook if you set one. If the panic happens after
a stream has sent its first event, photon aborts the connection instead, so
the client sees a failed stream rather than a clean end that reads like a
complete answer.

If you use a recovery middleware from another library anyway, check that it
re-panics `http.ErrAbortHandler`. Photon uses that value to abort a stream. A
middleware that swallows it and returns normally lets `net/http` end the
response cleanly, and a truncated answer then looks complete.

## Putting it together

```go
// main.go
package main

import (
	"crypto/sha256"
	"log"
	"log/slog"
	"net/http"
	"os"
	"time"

	"github.com/agenticmarket/photon"
)

func main() {
	apiKey := os.Getenv("API_KEY")
	if apiKey == "" {
		log.Fatal("API_KEY is not set")
	}

	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	app := photon.New(photon.Logger(logger))

	// Global: runs for every request, 404s and 405s included, outermost first.
	app.Use(
		requestID,
		accessLog(logger),
		cors("https://app.example.com"),
		rateLimit(10, 20), // 10 requests a second per client, bursts of 20
	)

	app.GET("/healthz", func(w http.ResponseWriter, r *http.Request) {
		_ = photon.Text(w, http.StatusOK, "ok\n")
	})

	keys := map[[32]byte]Principal{
		sha256.Sum256([]byte(apiKey)): {ID: "default", Role: "user"},
	}
	api := app.Group("/v1", requireBearer(keys))
	api.GET("/search", search, deadline(5*time.Second))
	api.POST("/chat", photon.SSE(chat)) // no deadline: a stream runs as long as it needs

	if err := app.Run(":8080"); err != nil {
		log.Fatal(err)
	}
}

func search(w http.ResponseWriter, r *http.Request) {
	_ = photon.JSON(w, http.StatusOK, map[string]string{"q": r.URL.Query().Get("q")})
}

func chat(s *photon.Stream, r *http.Request) error {
	for _, tok := range []string{"Hello", ",", " world"} {
		if err := s.Event("token", []byte(tok)); err != nil {
			return err
		}
	}
	return s.Event("done", nil)
}
```

The order of the `Use` arguments is deliberate: `requestID` first so the
access log has the id; `accessLog` next so it records CORS refusals and 429s;
`cors` before `rateLimit` and long before auth, so preflights are answered
cheaply and without credentials.

The repository's [`examples/middleware`](../../examples/middleware) has
runnable auth, CORS, and rate-limit middleware with tests that make one request
per protected route, which is the reliable way to prove a route's middleware is
attached ([testing middleware wiring](testing.md#testing-middleware-wiring)).

## See also

- [Routing](routing.md): groups, mounts, and how 404/405 are produced
- [Security](security.md): trusted proxies, and what is and is not your job
- [Configuration](configuration.md): `photon.Logger` and the limits that run before middleware
- [Streaming](../guides/streaming.md): what a stream needs from the writer it is given
- [Building AI backends](./ai-backends.md)
- [Testing](testing.md)
