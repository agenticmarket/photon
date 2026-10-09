# API reference

Every exported symbol in `photon`, and what it is for. Complete by construction:
`doc_coverage_test.go` fails if anything exported is missing from this file or
from the [main README](../README.md), so this cannot silently fall behind the
code. For explanations and examples, see the [guides](README.md).

```go
import (
	"github.com/agenticmarket/photon"
	"github.com/agenticmarket/photon/photonerr"
)
```

## Contents

- [Server](#server) — the type you construct
- [Options](#options) — `photon.New(...)`
- [Routing](#routing) — registering routes, groups, mounts
- [Router](#router) — the router without a server
- [Request helpers](#request-helpers) — reading the request
- [Response helpers](#response-helpers) — writing the response
- [Streaming](#streaming) — server-sent events and other streams
- [Limits](#limits) — the security posture
- [Errors](#errors) — the streaming error vocabulary
- [Errors package](#errors-package-photonerr) — RFC 9457 problem documents
- [Middleware](#middleware)

---

## Server

`Server` is what `photon.New()` returns. It is an `http.Handler`, so it can be
mounted under anything or passed to `httptest.NewServer`, and it owns the
limits, the router, and graceful shutdown.

| Method | What it does |
|---|---|
| `Server.Run(addr)` | Listen, serve, and on SIGINT/SIGTERM shut down gracefully. The usual way to start. |
| `Server.Listen(addr)` | Listen and serve. Blocks; returns nil after `Shutdown` or `Close`. |
| `Server.ListenTLS(addr, cert, key)` | As `Listen`, over TLS 1.2+, with HTTP/2. |
| `Server.Serve(ln)` | Serve on an existing listener - for `tls.NewListener`, systemd sockets, tests. |
| `Server.Shutdown(ctx)` | Stop accepting, tell streams via `Stream.Done`, drain, and close what remains when `ctx` expires. |
| `Server.Close()` | Stop immediately, closing every connection. |
| `Server.ServeHTTP(w, r)` | Serve one request. Makes `Server` an `http.Handler`. |
| `Server.Limits()` | The effective limits, including derived ones. |
| `Server.Router()` | The underlying `*Router`. |
| `Server.Routes()` | The registered routes, for a debug endpoint or a test. |

`DefaultShutdownTimeout` (25 s) is how long `Run` waits after a signal: under
Kubernetes' default 30 s grace period. A second signal exits at once.

`PanicInfo` is what `OnPanic` receives: the recovered `Value`, the `Stack`, and
the request's `Method` and `Path`.

## Options

`photon.New(opts ...Option)` panics on an invalid configuration, so a bad limit
fails at startup. Every option is optional; a server with none is already
secure.

| Option | Purpose |
|---|---|
| `SetLimits(Limits)` | Replace the limit set. Start from `DefaultLimits()`. |
| `Logger(*slog.Logger)` | Structured logging. Default: errors only, to `slog.Default`. |
| `OnPanic(func(*PanicInfo))` | Hook for recovered handler panics, e.g. an error tracker. |
| `MaxStreams(n)` | Concurrent stream cap. 0 = derive from the memory budget. |
| `MemoryBudget(bytes int64)` | The budget `MaxStreams` is derived from. |
| `ShutdownTimeout(d)` | How long `Run` drains after a signal. |

`Option` is the type these are; you rarely name it.

By default only errors are logged - handler panics, and 5xx responses written
with `Error` - through `slog.Default`. Warnings such as rejected requests are
dropped unless you pass a logger, so a flood of bad requests is not also a
flood of log lines.

## Routing

Verbs take an `http.HandlerFunc`, so a plain function works. `Handle` takes
any `http.Handler`. Optional trailing arguments are per-route middleware.

```go
app.GET("/users/:id", getUser)
app.POST("/chat", photon.SSE(chat))
app.Handle("PROPFIND", "/dav/*path", davHandler)
app.GET("/admin", admin, requireAuth, requireRole("admin"))
```

| Method | What it does |
|---|---|
| `GET`, `POST`, `PUT`, `PATCH`, `DELETE`, `HEAD`, `OPTIONS` | Register a route for that method. |
| `Handle(method, path, h, mw...)` | Register any `http.Handler` for any method. |
| `Group(prefix, mw...)` | A `*Group` sharing a prefix and middleware. |
| `Mount(prefix, h)` | Serve an existing `http.Handler` for every method at `prefix` and every path below it that no route matches. Routes take precedence, so `Mount("/", legacy)` is a fallback during a migration. |
| `Use(mw...)` | Middleware for every request: all routes, before or after this call, and 404/405. |
| `NotFound(h)` | Replace the 404 response. |
| `MethodNotAllowed(h)` | Replace the 405 response; `Allow` is set before it runs. |

`HandlerFunc` is an alias for `http.HandlerFunc`, for code that wants one name.
`RouteInfo` is what `Routes()` returns: `Method`, `Path`, and `Params`.

**Registration panics on a conflict**, naming both registration lines.
`/admin/settings` beside `/admin/:page` or `/admin/*rest` is a conflict:
which handler runs would depend on the request, which is how a typo becomes an
authorization bug. Routes cannot be added once the server is serving.

The panic value matches `ErrRouteConflict` (two routes could serve one
request) or `ErrBadPattern` (a malformed pattern, such as `{id}` instead of
`:id`) with `errors.Is`, so a test can assert on the kind of mistake.

### Groups

```go
v1 := app.Group("/v1", requireKey)
v1.POST("/chat/completions", chat)
admin := v1.Group("/admin", requireRole("admin"))
```

`Group` has the same verbs, plus `Group.Handle`, `Group.Use` (middleware for the
group's routes registered after it), `Group.Group` (nest), and `Group.Mount`.
A route path of `"/"` in a group is the group's own path. The prefix may
contain parameters.

### Path syntax

| Pattern | Matches | Captures |
|---|---|---|
| `/users` | exactly `/users` | - |
| `/users/:id` | `/users/42` | `id` = `42` |
| `/files/*path` | `/files/a/b.txt` (not `/files`) | `path` = `a/b.txt` |

`:name` matches one segment. `*name` matches the non-empty rest of the path and
must be last. Trailing slashes are rejected at registration, and so are `{id}`
braces, with a message giving the Photon spelling. Matching uses the raw path,
so `%2F` never acts as a separator; captured values are then decoded, as
`ServeMux` decodes them.

## Router

`NewRouter()` returns a `*Router` without a server: no limits, no lifecycle.
It has the same registration methods as `Server` (`GET`...`OPTIONS`,
`Handle`, `Group`, `Mount`, `Use`, `NotFound`, `MethodNotAllowed`) plus:

| Method | What it does |
|---|---|
| `Router.ServeHTTP(w, r)` | Dispatch a request. |
| `Router.Freeze()` | Make the router immutable; lookups stop taking a lock. `Serve` calls it. |
| `Router.Routes()` | Registered routes in registration order. |
| `Router.Lookup(method, path)` | Resolve without serving: handler, parameters (copied), and the `Allow` list on a 405. For tools and tests. |

## Request helpers

| Function | What it does |
|---|---|
| `PathParam(r, name)` | A path parameter; the same as `r.PathValue(name)`. Valid during the handler only. |
| `CopyParam(r, name)` | A copy safe to keep after the handler returns. |
| `Params(r)` | All parameters, in pattern order, as `[]Param` (`Name`, `Value`). |
| `CopyParams(r)` | A copy of all parameters, safe to keep. |
| `DecodeJSON(r, &v)` | Decode a JSON body: 415 for a non-JSON type, 400 for empty, malformed, trailing data, or a wrong type, 413 past the body limit. |
| `ClientIP(r)` | The socket peer's address. Ignores `X-Forwarded-For` on purpose. |
| `AcceptsGzip(r)` | Whether `Accept-Encoding` allows gzip. |

Parameters are stored with `Request.SetPathValue`, and `r.Pattern` holds the
matched route template (`/users/:id`), so handlers written for `net/http`'s
`ServeMux` and metrics middleware that reads `r.Pattern` work unchanged.

## Response helpers

All set `Content-Type`, `X-Content-Type-Options: nosniff`, and, where the body
is known, `Content-Length`.

| Function | What it does |
|---|---|
| `JSON(w, status, v)` | JSON body. HTML in strings is escaped. |
| `Text(w, status, s)` | `text/plain` body. |
| `HTML(w, status, s)` | `text/html` body. No CSP is set; that is an application decision. |
| `NoContent(w)` | 204. |
| `Redirect(w, r, url)` | 303. |
| `Error(w, r, err)` | An RFC 9457 problem document. A `*photonerr.Error` keeps its status and safe message; any other error becomes a constant 500. 5xx errors are logged with their cause. |

## Streaming

### Getting a stream

```go
app.POST("/chat", photon.SSE(func(s *photon.Stream, r *http.Request) error {
	for tok := range tokens {
		if err := s.Event("token", []byte(tok)); err != nil {
			return err
		}
	}
	return s.Event("done", nil)
}))
```

| Symbol | What it is |
|---|---|
| `SSE(fn, opts...)` | Adapts a `StreamFunc` to an `http.HandlerFunc`: enforces `MaxStreams`, closes the stream, renders a returned error (an HTTP error before the first event, an `error` event after), aborts the connection on a panic mid-stream. |
| `StreamFunc` | `func(s *Stream, r *http.Request) error` |
| `Streaming(opts...)` | Middleware giving a plain handler a stream, reached with `StreamFrom`. |
| `StreamFrom(r)` | The stream attached by `SSE` or `Streaming`, or nil. |
| `NewStream(w, r, opts...)` | A stream with no adapter. The handler must `Close` it. |

### Writing

| Method | What it does |
|---|---|
| `Stream.Event(name, data)` | One SSE event. Empty name = unnamed; data may contain anything. |
| `Stream.JSON(name, v)` | One event with `v` encoded as JSON. |
| `Stream.Comment(text)` | An SSE comment, ignored by clients. |
| `Stream.SetEventID(id)` | The id of the next event, sent in its frame. |
| `Stream.SetRetry(d)` | The client's reconnect delay. |
| `Stream.Write(p)` | Raw bytes (`io.Writer`), for NDJSON or proxying. |
| `Stream.Header()` | Response headers, changeable until the first write. |
| `Stream.Flush()` | Commit headers and wait until everything accepted is written. |
| `Stream.Done()` | Closed when the client leaves, the stream ends, or the server shuts down. |
| `Stream.Err()` | Why the stream ended, or nil. |
| `Stream.StartHeartbeats()` | Open the stream now (commits the 200) and keep it alive with heartbeats while the handler is quiet; returns a stop func. After the first event heartbeats are automatic anyway. |
| `Stream.Overflow(policy)` | Set the overflow policy before the first write. |
| `Stream.Close()` | End the stream, sending what is buffered. Idempotent. |

Events are sent as soon as the connection is free. A `Stream` is safe for
concurrent use; events never interleave mid-frame. An event name or id with a
line break or NUL is refused with `ErrInvalidEvent`.

### Overflow policies

What a producer does when the client is slower and the buffer is full.
`OverflowPolicy` is the type; `OverflowPolicy.String` names it in logs.

| Policy | Behaviour |
|---|---|
| `Disconnect` (default) | Wait for room (back-pressure); end the stream if the client accepts nothing for `StreamStallTimeout`. Never loses data. |
| `DropOldest` | Drop the oldest whole events waiting to be sent instead of waiting for a slow client. For metrics and progress, never for text. |
| `Grow` | Grow the buffer from the server-wide `MaxTotalStreamBufferBytes` budget, then behave like `Disconnect`. |

### Stream options

For `SSE`, `Streaming`, and `NewStream`. `StreamOption` is the type.

| Option | Purpose |
|---|---|
| `WithOverflow(policy)` | The overflow policy. |
| `WithStall(d)` | Override `StreamStallTimeout` for these streams. |
| `WithStreamBuffer(n)` | Override `MaxStreamBufferBytes` for these streams. |

## Limits

`Limits` is the resource budget, and `DefaultLimits()` returns the defaults.
`Duration`, `Second`, and `Millisecond` let a `Limits` literal read naturally.
Change a field by starting from the defaults:

```go
l := photon.DefaultLimits()
l.MaxRequestBodyBytes = 10 << 20
app := photon.New(photon.SetLimits(l))
```

| Field | Default | Bounds |
|---|---|---|
| `MaxRequestBodyBytes` | 1 MiB | Request body size (413) |
| `MaxHeaderBytes` | 16 KiB | Total header size |
| `MaxHeaders` | 100 | Header fields, counting repeats (431) |
| `MaxHeaderValueBytes` | 8 KiB | One header value (431) |
| `ReadHeaderTimeout` | 5 s | Time to send headers (slowloris) |
| `IdleTimeout` | 60 s | Idle keep-alive connections |
| `ReadTimeout` | 0 (none) | Whole-request deadline; off so long streams work |
| `MaxConnections` | 0 (none) | Concurrent connections, refused at accept |
| `MaxConcurrentRequests` | 0 (none) | Handlers running at once (503) |
| `MaxPerIP` | 0 (none) | Connections from one address |
| `MaxStreams` | derived, 64-8192 | Concurrent streams (503) |
| `MaxStreamBufferBytes` | 64 KiB | Unsent bytes per stream |
| `MaxTotalStreamBufferBytes` | derived | Server-wide budget for `Grow` |
| `StreamStallTimeout` | 30 s | Time a stream may make no progress |
| `HeartbeatInterval` | 15 s | Silence before a heartbeat; 0 disables |
| `MemoryBudget` | detected | The budget `MaxStreams` derives from |

See [configuration](guides/configuration.md) for when to change each.

## Errors

The streaming error vocabulary. Test with `errors.Is`.

| Error | Meaning |
|---|---|
| `ErrClientGone` | The client disconnected. A normal end: stop producing. |
| `ErrStreamOverflow` | The client accepted nothing for `StreamStallTimeout`. |
| `ErrStreamClosed` | Write after `Close`. |
| `ErrStreamPanic` | The handler panicked after the first event. |
| `ErrInvalidEvent` | An event name or id contained a line break or NUL. |

`SSE` treats `ErrClientGone`, `ErrStreamOverflow`, and `ErrStreamClosed` as
the stream ending, not as failures to report.

## Errors package (`photonerr`)

`photonerr.Error` carries a `Status`, a stable `Code`, a client-safe `Message`,
an operator-only `Detail` (never sent), and an optional `Limit`. Constructors:

| Constructor | Status |
|---|---|
| `BadRequest(msg)` | 400 |
| `Unauthorized(msg)` | 401 |
| `Forbidden(msg)` | 403 |
| `NotFound(what)` | 404 - message is "`what` not found" |
| `NotAllowed()` | 405 |
| `Conflict(msg)` | 409 |
| `LimitExceeded(limit)` | 413 |
| `UnsupportedMediaType(msg)` | 415 |
| `HeaderTooLarge(limit)` | 431 |
| `UnprocessableEntity(msg)` | 422 |
| `TooManyRequests(msg)` | 429 - set `Retry-After` yourself |
| `Canceled()` | 499 |
| `Internal(cause)` | 500 - cause logged, never sent |
| `Upstream(cause)` | 502 - cause logged, never sent |
| `Unavailable(reason)` | 503 |
| `Timeout()` | 504 |

`WithDetail(format, args...)`, `WithCause(err)`, and `WithLimit(name)` add
context; `errors.Is(err, photonerr.ErrNotFound)` matches by code, and each
constructor has a matching sentinel (`ErrBadRequest`, `ErrUnauthorized`, ...).
`photonerr.Render(w, err, RenderOptions{})` writes the problem document
directly; `photon.Error` is the usual way, because it also logs server errors.

## Middleware

`Middleware` is `func(http.Handler) http.Handler` - plain `net/http`, so any
existing middleware composes. Order: `Use` middleware (outermost, in call
order), then group middleware, then per-route middleware, then the handler.
`Use` runs for every request, including 404 and 405, so CORS preflights need no
`OPTIONS` routes. See [middleware](guides/middleware.md).

## Method index

The registration methods exist on all three types with identical behaviour; the
`Server` versions forward to its `Router`.

| | `Server` | `Router` | `Group` |
|---|---|---|---|
| Verbs | `Server.GET` `Server.POST` `Server.PUT` `Server.PATCH` `Server.DELETE` `Server.HEAD` `Server.OPTIONS` | `Router.GET` `Router.POST` `Router.PUT` `Router.PATCH` `Router.DELETE` `Router.HEAD` `Router.OPTIONS` | `Group.GET` `Group.POST` `Group.PUT` `Group.PATCH` `Group.DELETE` `Group.HEAD` `Group.OPTIONS` |
| Any handler | `Server.Handle` | `Router.Handle` | `Group.Handle` |
| Groups and mounts | `Server.Group` `Server.Mount` | `Router.Group` `Router.Mount` | `Group.Group` `Group.Mount` |
| Middleware | `Server.Use` | `Router.Use` | `Group.Use` |
| Misses | `Server.NotFound` `Server.MethodNotAllowed` | `Router.NotFound` `Router.MethodNotAllowed` | - |
