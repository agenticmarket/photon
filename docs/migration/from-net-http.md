# Migrating from net/http

> **What changes:** how routes are registered (`"GET /users/{id}"` becomes `app.GET("/users/:id", h)`), how you read path parameters, and the startup and shutdown code in `main`.
> **What stays the same:** your handlers and your middleware. A Photon handler is `func(w http.ResponseWriter, r *http.Request)` and middleware is `func(http.Handler) http.Handler`.
> **Watch out for:** routes that ServeMux resolved by precedence now panic at startup, `GET` no longer answers `HEAD`, and every request now has a 1 MiB body limit.

This is the shortest migration. Photon is `net/http` with a router, a set of
default limits, streaming, and lifecycle handling on top, so most of your code
moves over untouched.

- [Install](#install)
- [A whole program, before and after](#a-whole-program-before-and-after)
- [Route patterns](#route-patterns)
- [Keeping your handlers as they are](#keeping-your-handlers-as-they-are)
- [Middleware](#middleware)
- [Migrating one piece at a time](#migrating-one-piece-at-a-time)
- [JSON and errors](#json-and-errors)
- [Replacing a hand-written SSE loop](#replacing-a-hand-written-sse-loop)
- [Starting and stopping](#starting-and-stopping)
- [Limits that now apply](#limits-that-now-apply)
- [Testing](#testing)
- [Gotchas](#gotchas)
- [Checklist](#checklist)

## Install

```sh
go get github.com/agenticmarket/photon
```

Photon requires Go 1.24 or later and brings in no other modules.

## A whole program, before and after

**Before**: Go 1.22+ `ServeMux` with hand-written server setup and shutdown.

```go
package main

import (
	"context"
	"errors"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func main() {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", health)
	mux.HandleFunc("GET /users/{id}", getUser)
	mux.HandleFunc("POST /users", createUser)
	mux.Handle("GET /static/", http.StripPrefix("/static", http.FileServer(http.Dir("public"))))

	srv := &http.Server{
		Addr:              ":8080",
		Handler:           logRequests(mux),
		ReadHeaderTimeout: 5 * time.Second,
		IdleTimeout:       60 * time.Second,
		MaxHeaderBytes:    16 << 10,
	}

	go func() {
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatal(err)
		}
	}()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	<-ctx.Done()

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Print(err)
	}
}

func getUser(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	// ...
}
```

**After**:

```go
package main

import (
	"log"
	"net/http"

	"github.com/agenticmarket/photon"
)

func main() {
	app := photon.New() // header timeout, idle timeout, header and body limits are already set

	app.Use(logRequests) // same middleware as before

	app.GET("/healthz", health)
	app.GET("/users/:id", getUser)
	app.POST("/users", createUser)
	app.Mount("/static", http.StripPrefix("/static", http.FileServer(http.Dir("public"))))

	// Listens, handles SIGINT and SIGTERM, and shuts down gracefully.
	if err := app.Run(":8080"); err != nil {
		log.Fatal(err)
	}
}

func getUser(w http.ResponseWriter, r *http.Request) {
	id := photon.PathParam(r, "id")
	// ...
}
```

`health`, `createUser`, and `logRequests` did not change. The `http.Server`
fields, the signal handling, and the shutdown block went away because
`photon.New` and `app.Run` do that work.

## Route patterns

| Go 1.22 ServeMux | Photon | Notes |
|---|---|---|
| `mux.HandleFunc("GET /users/{id}", h)` | `app.GET("/users/:id", h)` | Also `POST`, `PUT`, `PATCH`, `DELETE`, `HEAD`, `OPTIONS`. |
| `mux.Handle("DELETE /users/{id}", h)` | `app.Handle(http.MethodDelete, "/users/:id", h)` | `Handle` takes any `http.Handler`. |
| `"GET /files/{path...}"` | `app.GET("/files/*path", h)` | Photon's catch-all needs at least one character, so `/files/` is not matched. Register `/files` separately if you need it; that is allowed. |
| `"GET /static/"` (subtree) | `app.Mount("/static", h)` | `Mount` serves the prefix and everything below it, for every common method. Or use `app.GET("/static/*path", h)` for GET only. |
| `"GET /{$}"` | `app.GET("/", h)` | Photon's `/` matches only `/`. |
| `"/"` (everything not matched) | `app.NotFound(h)` | |
| `"/webhook"` (no method, any method) | `app.Handle(m, "/webhook", h)` for each method | Or `Mount` if you want the subtree too. |
| `"example.com/x"` (host pattern) | No equivalent | Check `r.Host` in a handler or middleware. |
| `r.PathValue("id")` | `photon.PathParam(r, "id")` | Valid during the handler call only. See [Gotchas](#gotchas). |

### Precedence becomes a startup panic

ServeMux resolves overlapping patterns by picking the most specific one, so this
is legal there:

```go
mux.HandleFunc("GET /users/new", newUserForm)
mux.HandleFunc("GET /users/{id}", getUser)
```

Photon panics at registration, naming both call sites. A static segment beside a
parameter at the same position, or beside a catch-all under the same prefix, is
an ambiguity Photon refuses to resolve for you. Two fixes:

```go
// 1. Move one route under a distinct segment, if you can change the URL.
app.GET("/user-forms/new", newUserForm)
app.GET("/users/:id", getUser)

// 2. Keep the URLs and branch in the handler, if clients depend on them.
app.GET("/users/:id", func(w http.ResponseWriter, r *http.Request) {
	if photon.PathParam(r, "id") == "new" {
		newUserForm(w, r)
		return
	}
	getUser(w, r)
})
```

Routes for different methods never conflict with each other. `GET /users/new`
beside `POST /users/:id` is fine.

## Keeping your handlers as they are

The verb methods take an `http.HandlerFunc`, so any
`func(w http.ResponseWriter, r *http.Request)` passes directly, and so does a
method value such as `mux.ServeHTTP`. `Handle` takes any `http.Handler`.

```go
app.GET("/healthz", health)                           // a plain function
app.GET("/metrics", metricsHandler.ServeHTTP)         // a method value
app.Handle(http.MethodGet, "/version", versionHandler) // an http.Handler
```

The one thing a ServeMux handler might use that Photon does not fill in is
`r.PathValue`. Under Photon it returns `""`. Either switch each call to
`photon.PathParam(r, "id")`, or, while you migrate, add this middleware so the
old calls keep working:

```go
// pathValues copies Photon's path parameters into r.PathValue, so handlers
// written for Go 1.22's ServeMux work unmodified. CopyParams gives the values
// the same lifetime r.PathValue had: safe to keep after the handler returns.
func pathValues(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		for _, p := range photon.CopyParams(r) {
			r.SetPathValue(p.Name, p.Value)
		}
		next.ServeHTTP(w, r)
	})
}

app.Use(pathValues)
```

This works because Photon runs global middleware after routing, so the
parameters are already known. It costs a small allocation per parameterised
request; drop it once the handlers call `photon.PathParam` directly.

## Middleware

Your middleware does not change. Photon's `Middleware` type is
`func(http.Handler) http.Handler`, which is what `net/http` middleware already
is. There are three places to attach it:

```go
app.Use(logRequests, securityHeaders)           // every request, including 404 and 405
api := app.Group("/api", requireAuth)           // every route registered on this group
api.GET("/me", me)
app.GET("/admin/stats", stats, requireAdmin)    // one route
```

They run outermost first: `Use` middleware, then group middleware, then
per-route middleware, then the handler.

Three things differ from wrapping a mux by hand:

- **`app.Use` applies to every route, whether registered before or after the
  call, and to 404 and 405 responses.** A CORS middleware sees preflight
  `OPTIONS` requests for routes that only registered `POST`; a rate limiter
  counts a flood of 404s.
- **Global middleware runs after routing.** `photon.PathParam` works inside it.
  The other side is that rewriting `r.URL.Path` in middleware does not change
  which route runs.
- **`Group.Use` is order-dependent.** It applies to routes registered on the
  group after the call, so a group can hold a public route followed by
  authenticated ones.

If any of your middleware wraps `http.ResponseWriter` (status recorders, access
logs, metrics), give the wrapper an `Unwrap` method. Photon streams through
`http.ResponseController`, which needs it to reach `Flush` and the write
deadline:

```go
type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (s *statusRecorder) WriteHeader(code int) {
	s.status = code
	s.ResponseWriter.WriteHeader(code)
}

// Unwrap lets http.ResponseController reach the real writer.
func (s *statusRecorder) Unwrap() http.ResponseWriter { return s.ResponseWriter }
```

Without `Unwrap`, a stream through that middleware fails with an error saying
the `ResponseWriter` cannot flush. More in [../guides/middleware.md](../guides/middleware.md).

## Migrating one piece at a time

You don't have to move every route at once. There are two ways to run Photon in
front of existing handlers.

### Mount the old code under a prefix

If the legacy routes live under a prefix, mount the old mux there:

```go
legacy := http.NewServeMux()
legacy.HandleFunc("GET /v1/users/{id}", oldGetUser)
legacy.HandleFunc("POST /v1/users", oldCreateUser)

app := photon.New()
app.Mount("/v1", legacy)                   // the path is NOT stripped: legacy sees /v1/users/42
app.POST("/v2/chat", photon.SSE(chat))     // new code
```

`Mount` passes the path through unchanged. If the mounted handler expects paths
relative to the mount point, wrap it: `app.Mount("/v1", http.StripPrefix("/v1", h))`.

Routes take precedence over a mount, so you can register ported routes under
`/v1` while the mount keeps serving the rest.

### Fall back to the old mux for everything Photon doesn't route

If old and new routes are mixed at the same level, mount the old mux whole at
the root. Routes take precedence over mounts, so everything Photon does not
route falls through to it:

```go
legacy := oldRoutes() // your existing *http.ServeMux, unchanged
app.Mount("/", legacy)

// Routes you have moved so far:
app.GET("/healthz", health)
app.POST("/chat", photon.SSE(chat))
```

Move routes across one at a time and delete each from the legacy mux as you go,
so every path has one owner.

Keep in mind:

- **A Photon route wins for every path it matches.** If you add
  `app.GET("/users/:id", h)` while the legacy mux still serves
  `GET /users/export`, Photon's route now answers `/users/export`.
- **Legacy routes go through `app.Use` middleware**, because `Use` covers 404s.
  That is usually what you want for logging. Check it before adding
  authentication with `app.Use`.
- **Legacy routes are under Photon's limits.** The 1 MiB body limit and the
  header limits apply to everything the app serves. If an old endpoint takes
  uploads, raise `MaxRequestBodyBytes` (see [Limits that now apply](#limits-that-now-apply)).

## JSON and errors

Your existing `encoding/json` code keeps working. Photon's helpers are optional;
here is what they replace.

**Before**:

```go
func createUser(w http.ResponseWriter, r *http.Request) {
	var in createUserRequest
	if err := json.NewDecoder(r.Body).Decode(&in); err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	if in.Name == "" {
		http.Error(w, "name is required", http.StatusBadRequest)
		return
	}
	u, err := store.Create(r.Context(), in.Name)
	if err != nil {
		log.Printf("create user: %v", err)
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(u)
}
```

**After**:

```go
import (
	"net/http"

	"github.com/agenticmarket/photon"
	"github.com/agenticmarket/photon/photonerr"
)

func createUser(w http.ResponseWriter, r *http.Request) {
	var in createUserRequest
	if err := photon.DecodeJSON(r, &in); err != nil {
		photon.Error(w, r, err) // 400, 413, or 415 with a message that is safe to send
		return
	}
	if in.Name == "" {
		photon.Error(w, r, photonerr.BadRequest("name is required"))
		return
	}
	u, err := store.Create(r.Context(), in.Name)
	if err != nil {
		photon.Error(w, r, err) // not a *photonerr.Error: 500 with a constant body, and logged
		return
	}
	_ = photon.JSON(w, http.StatusCreated, u)
}
```

What the helpers do:

- **`photon.DecodeJSON`** returns 415 for a non-JSON `Content-Type` (a missing one
  is accepted), 413 when the body is over the limit, and 400 for an empty body,
  malformed JSON, a field of the wrong type, or trailing data after the first
  value. Unknown fields are allowed. Error messages never quote the request body.
- **`photon.JSON`** sets `Content-Type`, `Content-Length`, and
  `X-Content-Type-Options: nosniff`. It returns an error only if the value cannot
  be encoded, and in that case nothing has been written yet, so following it
  with `photon.Error` is safe.
- **`photon.Error`** writes an RFC 9457 problem document. A `*photonerr.Error`
  keeps its status and message. Any other error becomes a 500 with a constant
  body, so a database error with a connection string in it never reaches the
  client. 5xx errors are logged through the server's logger.

The response body changes shape when you adopt `photon.Error`:

```http
HTTP/1.1 400 Bad Request
Content-Type: application/problem+json; charset=utf-8

{"type":"https://…/errors/bad_request","title":"name is required","status":400,"code":"bad_request"}
```

(The `type` URI is shortened here.) If clients parse your old error format, keep
writing it with `photon.JSON` until they move.

## Replacing a hand-written SSE loop

This is where most of the value is. A typical hand-written handler:

**Before**:

```go
func chat(w http.ResponseWriter, r *http.Request) {
	var req chatRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}

	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming unsupported", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")

	for tok := range generate(r.Context(), req.Prompt) {
		fmt.Fprintf(w, "event: token\ndata: %s\n\n", tok)
		flusher.Flush()
	}
	fmt.Fprint(w, "event: done\ndata:\n\n")
	flusher.Flush()
}
```

It works in a demo. What it doesn't do:

- A token containing a newline breaks the SSE framing.
- Write errors are ignored, so the loop keeps going after the client leaves (the
  model call stops only if `generate` watches the context).
- A client that stops reading blocks `Fprintf` with no deadline, holding the
  goroutine and the upstream call.
- Nothing limits how many streams are open at once.
- No heartbeats, so a proxy can drop a quiet connection.
- On shutdown the stream is either waited on with no signal or cut off mid-answer.
- `w.(http.Flusher)` fails if a middleware wrapped the writer.

**After**:

```go
app.POST("/chat", photon.SSE(func(s *photon.Stream, r *http.Request) error {
	var req chatRequest
	if err := photon.DecodeJSON(r, &req); err != nil {
		return err // nothing sent yet: the client gets a normal 400, 413, or 415
	}
	for tok := range generate(r.Context(), req.Prompt) {
		if err := s.Event("token", []byte(tok)); err != nil {
			return err // the client is gone: stop
		}
	}
	return s.Event("done", nil)
}))
```

`generate` stands for your model client. It should stop when its context is
cancelled. `r.Context()` is cancelled when the client disconnects and when the
handler returns, so a producer goroutine that selects on `ctx.Done()` exits
instead of leaking.

| Hand-written | `photon.SSE` |
|---|---|
| Set `Content-Type`, `Cache-Control` | Done for you (and `X-Accel-Buffering: no` for nginx) |
| `fmt.Fprintf(w, "event: %s\ndata: %s\n\n", ...)` | `s.Event(name, data)`, which splits multi-line data correctly |
| `json.Marshal` then write | `s.JSON(name, v)` |
| `flusher.Flush()` | Not needed: each event is sent immediately |
| Ignore write errors | `s.Event` returns an error; return it |
| Hope the client keeps reading | Back-pressure, then disconnect after `StreamStallTimeout` (30s) |
| No concurrency limit | `MaxStreams`, answering 503 with `Retry-After` when full |
| Your own ticker for keep-alives | Comment heartbeats after 15s of silence |
| `http.Error` after streaming started (too late) | Returned error becomes an `event: error` with a problem document |

If you would rather keep the plain handler signature, use the `Streaming`
middleware on the route and fetch the stream with `StreamFrom`. It also enforces
`MaxStreams`:

```go
app.POST("/chat", chat, photon.Streaming())

func chat(w http.ResponseWriter, r *http.Request) {
	s := photon.StreamFrom(r)
	for tok := range generate(r.Context(), "hello") {
		if err := s.Event("token", []byte(tok)); err != nil {
			return
		}
	}
	_ = s.Event("done", nil)
}
```

`photon.NewStream(w, r)` also exists for fully manual use (you must
`defer s.Close()`), but it does not take a `MaxStreams` slot. Prefer `SSE`.

Full details, including resumption with `Last-Event-ID` and NDJSON, are in
[../guides/streaming.md](../guides/streaming.md).

## Starting and stopping

`app.Run(addr)` listens, waits for SIGINT or SIGTERM, stops accepting
connections, tells active streams through `s.Done()`, and waits up to 25 seconds
for in-flight work. A second signal during the wait exits immediately. It returns
`nil` after a clean shutdown.

```go
app := photon.New(photon.ShutdownTimeout(10 * time.Second))
if err := app.Run(":8080"); err != nil {
	log.Fatal(err)
}
```

If you need more control, the pieces are there: `app.Listen(addr)`,
`app.ListenTLS(addr, certFile, keyFile)`, `app.Serve(ln)`, `app.Shutdown(ctx)`,
and `app.Close()`. `Run` covers plain TCP; for TLS with graceful shutdown, call
`ListenTLS` in a goroutine and `Shutdown` from your own signal handling.

## Limits that now apply

`photon.New()` turns these on. ServeMux behind a bare `http.Server` has none of
them unless you set them.

| Limit | Default | What happens over it |
|---|---|---|
| `MaxRequestBodyBytes` | 1 MiB | 413 |
| `MaxHeaderBytes` | 16 KiB | 431 |
| `MaxHeaders` | 100 header fields | 431 |
| `MaxHeaderValueBytes` | 8 KiB | 431 |
| `ReadHeaderTimeout` | 5s | Connection closed |
| `IdleTimeout` | 60s | Idle keep-alive connection closed |
| `ReadTimeout` | 0 (none) | Off, so long streams work |
| `MaxStreams` | Derived from memory, 64 to 8192 | 503 with `Retry-After` |
| `StreamStallTimeout` | 30s | Stuck client disconnected |

To change one, start from the defaults:

```go
l := photon.DefaultLimits()
l.MaxRequestBodyBytes = 10 << 20 // 10 MiB, for an upload endpoint
app := photon.New(photon.SetLimits(l))
```

The body limit is server-wide. If only one route needs large bodies, raise the
server limit to that size and tighten the other routes with
`http.MaxBytesReader` in a middleware. See [../guides/security.md](../guides/security.md).

## Testing

A Photon app is an `http.Handler`, so your existing `httptest` code works:

```go
func TestGetUser(t *testing.T) {
	app := photon.New()
	registerRoutes(app)

	srv := httptest.NewServer(app)
	defer srv.Close()

	res, err := http.Get(srv.URL + "/users/42")
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		t.Fatalf("status = %d", res.StatusCode)
	}
}
```

`app.ServeHTTP(rec, req)` with an `httptest.ResponseRecorder` works too. Building
the app in a test is also how you catch route conflicts: they panic during
`registerRoutes`, before any request.

## Gotchas

- **`GET` does not answer `HEAD`.** ServeMux's `"GET /x"` matches `HEAD` too.
  In Photon a `HEAD` request to a GET-only route gets a 405. If load balancers or
  monitors send `HEAD`, register it: `app.HEAD("/healthz", health)`. `net/http`
  discards the body for `HEAD` responses, so the same handler is fine.
- **Overlapping routes panic instead of resolving by precedence.** See
  [Precedence becomes a startup panic](#precedence-becomes-a-startup-panic).
- **No trailing-slash redirects.** ServeMux redirects `/static` to `/static/` for
  subtree patterns. Photon rejects trailing slashes in patterns at registration
  and does not redirect requests. `/users/` is a 404.
- **A catch-all needs a non-empty remainder.** `/files/*path` matches `/files/a`
  but not `/files/` or `/files`.
- **An unconverted `{id}` panics at startup.** `app.GET("/users/{id}", h)` is
  refused with a message giving the Photon spelling: `{id}` becomes `:id`, and
  `{path...}` becomes `*path`.
- **Patterns are whole segments only.** `/files/:name.json` is rejected. Use
  `/files/:name` and trim the suffix in the handler.
- **`PathParam` aliases request memory.** The string is valid until the handler
  returns. If you store it, log it from a goroutine, or hand it to a worker, use
  `photon.CopyParam(r, "id")`. The race detector does not catch this misuse.
- **Parameters can come back percent-encoded.** Photon routes on the raw path so
  `%2F` cannot act as a separator. When a URL contains an encoded slash,
  parameters are returned in their escaped form. `r.PathValue` always decoded. If
  a parameter can legitimately contain reserved characters, run it through
  `url.PathUnescape`.
- **Routes are fixed once the server starts.** Registering a route after `Run`,
  `Listen`, or `Serve` panics. Register everything in `main` first.
- **`photon.ClientIP(r)` is the socket peer.** It ignores `X-Forwarded-For` on
  purpose. Behind a proxy it returns the proxy's address; trusting forwarded
  headers is your deployment's decision.
- **Limits apply to mounted and fallback handlers.** Old upload endpoints behind
  `Mount` or `NotFound` get 413 above 1 MiB.
- **`http.TimeoutHandler` and streaming don't mix.** It buffers the response and
  cannot flush. Keep it off stream routes, along with any other whole-request
  timeout middleware.
- **Photon is quiet by default.** It logs only errors (panics and 5xx responses)
  through `slog.Default()`. Pass `photon.Logger(...)` to see more. It does not
  write access logs; keep your own middleware for that.

## Checklist

- [ ] `go get github.com/agenticmarket/photon` (Go 1.24+)
- [ ] Translate patterns: `{id}` to `:id`, `{path...}` to `*path`, `"GET /x"` to `app.GET("/x", ...)`
- [ ] Search route registrations for `{` to catch patterns you missed (they register as literals)
- [ ] Add `app.HEAD` for routes that monitors or load balancers probe with `HEAD`
- [ ] Build the app in a test and fix any route-conflict panics
- [ ] Replace `r.PathValue` with `photon.PathParam`, or add the `pathValues` middleware for now
- [ ] Use `photon.CopyParam` wherever a parameter outlives the handler
- [ ] Move middleware to `app.Use`, groups, or per-route arguments
- [ ] Add `Unwrap()` to every `ResponseWriter` wrapper
- [ ] Replace flusher loops with `photon.SSE`, and return the error from `s.Event`
- [ ] Replace server setup, signal handling, and shutdown with `app.Run`
- [ ] Check upload sizes, header sizes, and concurrent stream counts against the defaults
- [ ] Tell client teams if error bodies change to `application/problem+json`
