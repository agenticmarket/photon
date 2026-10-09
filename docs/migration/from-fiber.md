# Migrating from Fiber

> **What changes:** nearly everything at the HTTP layer. Fiber is built on fasthttp; Photon is built on `net/http`. `*fiber.Ctx` becomes `(w http.ResponseWriter, r *http.Request)`, Fiber middleware must be replaced, and streaming moves from a stream-writer callback to `photon.SSE`.
> **What stays the same:** the route syntax for simple cases (`/users/:id`), groups, and your business logic.
> **Watch out for:** Fiber middleware does not port, optional parameters and route constraints have no equivalent, overlapping routes panic, and the default body limit is 1 MiB instead of Fiber's 4 MB.

Of all the migrations, this is the largest. It's worth being clear about why
you'd do it. Photon is not trying to be faster than Fiber; end-to-end its
throughput is on par with `net/http`, and the router is a few percent of CPU. The
reasons to move are the `net/http` ecosystem (every middleware, tracing library,
and test helper written for `http.Handler`), streaming that handles slow clients,
disconnects, and shutdown for you, and secure limits by default. If throughput
is why you chose Fiber, keep Fiber.

- [The shift: fasthttp to net/http](#the-shift-fasthttp-to-nethttp)
- [A whole program, before and after](#a-whole-program-before-and-after)
- [fiber.Ctx, method by method](#fiberctx-method-by-method)
- [Locals to context values](#locals-to-context-values)
- [Memory reuse rules](#memory-reuse-rules)
- [Errors](#errors)
- [Middleware](#middleware)
- [Groups](#groups)
- [Streaming](#streaming)
- [Listen and shutdown](#listen-and-shutdown)
- [Moving a route at a time](#moving-a-route-at-a-time)
- [Testing](#testing)
- [Gotchas](#gotchas)
- [Checklist](#checklist)

## The shift: fasthttp to net/http

Fiber gives each handler a `*fiber.Ctx` that wraps fasthttp's request and
response, both pooled and reused across requests. Handlers return an `error`
that Fiber's `ErrorHandler` renders.

Photon handlers are standard library handlers:

```go
func(w http.ResponseWriter, r *http.Request)
```

- The request is `*http.Request`: headers in `r.Header`, query in
  `r.URL.Query()`, body in `r.Body`, cancellation in `r.Context()`.
- The response is written through `http.ResponseWriter`, or with Photon's
  helpers (`photon.JSON`, `photon.Text`, `photon.Error`).
- Handlers don't return errors. You write the error response and `return`.
- Middleware is `func(http.Handler) http.Handler`, so anything written for
  `net/http` works, and nothing written for Fiber does.

## A whole program, before and after

**Before** (Fiber v2):

```go
package main

import (
	"log"

	"github.com/gofiber/fiber/v2"
)

type createUserRequest struct {
	Name string `json:"name"`
}

func main() {
	app := fiber.New()

	app.Get("/users/:id", func(c *fiber.Ctx) error {
		return c.JSON(fiber.Map{"id": c.Params("id")})
	})

	app.Post("/users", func(c *fiber.Ctx) error {
		var req createUserRequest
		if err := c.BodyParser(&req); err != nil {
			return fiber.NewError(fiber.StatusBadRequest, "invalid body")
		}
		if req.Name == "" {
			return fiber.NewError(fiber.StatusBadRequest, "name is required")
		}
		return c.Status(fiber.StatusCreated).JSON(req)
	})

	log.Fatal(app.Listen(":3000"))
}
```

**After**:

```go
package main

import (
	"log"
	"net/http"

	"github.com/agenticmarket/photon"
	"github.com/agenticmarket/photon/photonerr"
)

type createUserRequest struct {
	Name string `json:"name"`
}

func main() {
	app := photon.New()

	app.GET("/users/:id", func(w http.ResponseWriter, r *http.Request) {
		_ = photon.JSON(w, http.StatusOK, map[string]any{"id": photon.PathParam(r, "id")})
	})

	app.POST("/users", func(w http.ResponseWriter, r *http.Request) {
		var req createUserRequest
		if err := photon.DecodeJSON(r, &req); err != nil {
			photon.Error(w, r, err) // 400, 413, or 415 with a safe message
			return
		}
		if req.Name == "" {
			photon.Error(w, r, photonerr.BadRequest("name is required"))
			return
		}
		_ = photon.JSON(w, http.StatusCreated, req)
	})

	// Run also handles SIGINT/SIGTERM and shuts down gracefully.
	if err := app.Run(":3000"); err != nil {
		log.Fatal(err)
	}
}
```

## fiber.Ctx, method by method

| Fiber v2 | Photon / `net/http` |
|---|---|
| `c.Params("id")` | `photon.PathParam(r, "id")` |
| `c.Params("*")` | Name the catch-all (`/files/*path`), then `photon.PathParam(r, "path")` |
| `c.Query("q")` | `r.URL.Query().Get("q")` |
| `c.Get("X-Key")` (request header) | `r.Header.Get("X-Key")` |
| `c.Set("X-Key", v)` (response header) | `w.Header().Set("X-Key", v)` |
| `c.Body()` | `io.ReadAll(r.Body)` (already limited by `MaxRequestBodyBytes`) |
| `c.BodyParser(&v)` for JSON | `photon.DecodeJSON(r, &v)` |
| `c.BodyParser(&v)` for forms | `r.ParseForm()` then `r.PostForm.Get("name")` |
| `c.FormValue("name")` | `r.FormValue("name")` |
| `c.Cookies("session")` | `r.Cookie("session")` |
| `c.Cookie(&fiber.Cookie{...})` | `http.SetCookie(w, &http.Cookie{...})` |
| `return c.JSON(v)` | `_ = photon.JSON(w, http.StatusOK, v)` |
| `return c.Status(201).JSON(v)` | `_ = photon.JSON(w, http.StatusCreated, v)` |
| `return c.SendString(s)` | `_ = photon.Text(w, http.StatusOK, s)` |
| `return c.SendStatus(204)` | `_ = photon.NoContent(w)` |
| `return c.Redirect(url)` | `http.Redirect(w, r, url, http.StatusFound)` (`photon.Redirect` always sends 303) |
| `fiber.Map{...}` | `map[string]any{...}` |
| `c.Locals("user", u)` | `context.WithValue`, see below |
| `c.UserContext()` | `r.Context()` |
| `c.IP()` | `photon.ClientIP(r)` (a `netip.Addr`) |
| `c.Path()`, `c.Method()` | `r.URL.Path`, `r.Method` |
| `c.Next()` | `next.ServeHTTP(w, r)` |

## Locals to context values

`c.Locals` stores values for the life of a request. In `net/http` that is the
request context. Use an unexported key type so nothing else collides with it:

```go
type ctxKey int

const userKey ctxKey = iota

func withUser(r *http.Request, u *User) *http.Request {
	return r.WithContext(context.WithValue(r.Context(), userKey, u))
}

func userFrom(r *http.Request) (*User, bool) {
	u, ok := r.Context().Value(userKey).(*User)
	return u, ok
}
```

```go
// Before (Fiber middleware)
c.Locals("user", user)
return c.Next()

// After (net/http middleware)
next.ServeHTTP(w, withUser(r, user))
```

`r.Context()` is also how cancellation reaches your code. It is cancelled when
the client disconnects and when the handler returns. Pass it to database calls
and upstream HTTP requests.

## Memory reuse rules

Fiber's docs warn that values from `*fiber.Ctx` (`c.Params`, `c.Query`, `c.Get`,
`c.Body`) are only valid inside the handler, because fasthttp reuses the
underlying buffers. You either copy them (`utils.CopyString`) or turn on
`Immutable`.

Under Photon that rule shrinks to one function family:

| Value | Safe to keep after the handler returns? |
|---|---|
| `photon.PathParam(r, "id")`, `photon.Params(r)` | **No.** They alias request memory. Use `photon.CopyParam` / `photon.CopyParams`. |
| `r.Header.Get(...)`, `r.URL.Query().Get(...)` | Yes. Ordinary Go strings. |
| Bytes you read from `r.Body` | Yes. They're yours. |
| Fields decoded by `photon.DecodeJSON` | Yes. |

So the habit carries over, with a smaller scope. When a path parameter goes into
a goroutine, a channel, a cache, or an async logger, copy it:

```go
app.POST("/jobs/:id/run", func(w http.ResponseWriter, r *http.Request) {
	id := photon.CopyParam(r, "id") // outlives the handler
	go runJob(context.WithoutCancel(r.Context()), id)
	_ = photon.NoContent(w)
})
```

`context.WithoutCancel` keeps context values but drops the cancellation, which
would otherwise fire as soon as the handler returns. The race detector does not
catch a retained `PathParam`, because the access is synchronised and simply
reads the wrong bytes later.

## Errors

**Before**:

```go
return fiber.NewError(fiber.StatusNotFound, "user not found")
```

**After**:

```go
photon.Error(w, r, photonerr.NotFound("user")) // 404, "user not found"
return
```

| Fiber | photonerr |
|---|---|
| `fiber.NewError(400, msg)`, `fiber.ErrBadRequest` | `photonerr.BadRequest(msg)` |
| `fiber.ErrUnauthorized` | `photonerr.Unauthorized(msg)` |
| `fiber.ErrForbidden` | `photonerr.Forbidden(msg)` |
| `fiber.ErrNotFound` | `photonerr.NotFound("thing")` |
| `fiber.ErrConflict` | `photonerr.Conflict(msg)` |
| `fiber.ErrTooManyRequests` | `photonerr.TooManyRequests(msg)` |
| `fiber.ErrInternalServerError` | `photonerr.Internal(cause)` |
| `fiber.ErrBadGateway` | `photonerr.Upstream(cause)` |
| `fiber.ErrServiceUnavailable` | `photonerr.Unavailable(reason)` |
| `fiber.Config{ErrorHandler: ...}` | `photon.Error` renders every error the same way |

Fiber's default error handler sends the message as plain text. `photon.Error`
sends RFC 9457 `application/problem+json`
(`{"type": "…", "title": "user not found", "status": 404, "code": "not_found"}`).
An error that isn't a `*photonerr.Error` becomes a 500 with a constant body, and
5xx errors are logged with their cause. Tell clients about the format change, or
keep the old one by writing it with `photon.Text`.

If you want to keep Fiber's error-returning handler style while porting, an
adapter does it:

```go
func handle(fn func(w http.ResponseWriter, r *http.Request) error) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if err := fn(w, r); err != nil {
			photon.Error(w, r, err) // only valid if fn hasn't written a response yet
		}
	}
}
```

## Middleware

Fiber middleware is a `fiber.Handler` that calls `c.Next()`. It runs on fasthttp
and cannot wrap an `http.Handler`, so none of it ports directly. The good news
is that `net/http` has equivalents for nearly all of it.

**Before**:

```go
app.Use(func(c *fiber.Ctx) error {
	user, err := lookupToken(c.Get("Authorization"))
	if err != nil {
		return fiber.NewError(fiber.StatusUnauthorized, "missing or invalid credentials")
	}
	c.Locals("user", user)
	return c.Next()
})
```

**After**:

```go
func requireUser(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		user, err := lookupToken(r.Header.Get("Authorization"))
		if err != nil {
			photon.Error(w, r, photonerr.Unauthorized("missing or invalid credentials"))
			return // not calling next is how you stop the chain
		}
		next.ServeHTTP(w, withUser(r, user))
	})
}

app.Use(requireUser)                        // every request, including 404 and 405
api := app.Group("/api", requireUser)       // or a group
app.GET("/me", me, requireUser)             // or one route
```

Order is `app.Use` middleware, then group middleware, then per-route middleware,
then the handler. `app.Use` applies to every route regardless of when it was
registered. Fiber middleware, by contrast, runs only for routes registered after
it in the stack.

Replacements for Fiber's bundled middleware:

| Fiber middleware | Under Photon |
|---|---|
| `recover` | Built in. Use `photon.OnPanic` for reporting. |
| `logger` | A short `net/http` middleware with a status recorder (below). |
| `cors` | A `net/http` CORS package, for example `github.com/rs/cors`: `app.Use(cors.New(opts).Handler)`. |
| `limiter` | A middleware around `golang.org/x/time/rate`, or a `net/http` rate-limit package. |
| `requestid` | A few lines setting a header and a context value, or `github.com/go-chi/chi/v5/middleware` `RequestID`, which works without chi's router. |
| `compress` | A `net/http` compression middleware, kept off SSE routes. |
| `timeout` | `http.TimeoutHandler` on non-streaming routes only. |
| `helmet` | A middleware that sets the headers you want. |

Any middleware that wraps `http.ResponseWriter` must implement
`Unwrap() http.ResponseWriter`, or streams through it fail:

```go
type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (s *statusRecorder) WriteHeader(code int) {
	s.status = code
	s.ResponseWriter.WriteHeader(code)
}

// Unwrap lets http.ResponseController reach Flush and the write deadline.
func (s *statusRecorder) Unwrap() http.ResponseWriter { return s.ResponseWriter }

func requestLog(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		rec := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
		next.ServeHTTP(rec, r)
		slog.Info("request", "method", r.Method, "path", r.URL.Path,
			"status", rec.status, "duration", time.Since(start))
	})
}
```

See [../guides/middleware.md](../guides/middleware.md).

## Groups

```go
// Before
api := app.Group("/api", requireUserFiber)
api.Get("/me", me)

// After
api := app.Group("/api", requireUser)
api.GET("/me", me)
```

Group prefixes may contain parameters and groups nest. Photon has no `app.All`;
register each method with `app.Handle`, or use `app.Mount` for a whole subtree.

## Streaming

In Fiber v2, streaming usually goes through fasthttp's body stream writer. (Fiber
v3 wraps the same idea as `c.SendStreamWriter`.)

**Before**:

```go
app.Post("/chat", func(c *fiber.Ctx) error {
	var req chatRequest
	if err := c.BodyParser(&req); err != nil {
		return fiber.NewError(fiber.StatusBadRequest, "invalid body")
	}
	c.Set("Content-Type", "text/event-stream")
	c.Set("Cache-Control", "no-cache")

	ctx, cancel := context.WithCancel(context.Background())
	c.Context().SetBodyStreamWriter(fasthttp.StreamWriter(func(w *bufio.Writer) {
		defer cancel()
		for tok := range generate(ctx, req.Prompt) {
			fmt.Fprintf(w, "event: token\ndata: %s\n\n", tok)
			if err := w.Flush(); err != nil {
				return // the client went away
			}
		}
		fmt.Fprint(w, "event: done\ndata:\n\n")
		w.Flush()
	}))
	return nil
})
```

The stream writer runs after the handler has returned, so it can't touch `c` or
anything borrowed from it, cancellation has to be wired up by hand, and the
only disconnect signal is the error from `w.Flush()`.

**After**:

```go
app.POST("/chat", photon.SSE(func(s *photon.Stream, r *http.Request) error {
	var req chatRequest
	if err := photon.DecodeJSON(r, &req); err != nil {
		return err // nothing sent yet: a normal 400, 413, or 415
	}
	for tok := range generate(r.Context(), req.Prompt) {
		if err := s.Event("token", []byte(tok)); err != nil {
			return err // the client is gone: stop
		}
	}
	return s.Event("done", nil)
}))
```

The function runs for the whole life of the stream, so `r` and its context are
valid throughout, and `r.Context()` is cancelled when the client leaves. Photon
also:

- sends each event immediately and frames multi-line data correctly;
- applies back-pressure to a slow client and disconnects one that is stuck for
  `StreamStallTimeout` (30s);
- caps concurrent streams with `MaxStreams`, answering 503 with `Retry-After`;
- sends comment heartbeats after 15 seconds of silence;
- turns a returned error into an HTTP error before the first event, or an
  `event: error` after it;
- tells the stream about shutdown through `s.Done()` and gives it time to finish.

For NDJSON or other raw formats, set the content type with
`s.Header().Set(...)` before the first write and use `s.Write`. See
[../guides/streaming.md](../guides/streaming.md).

## Listen and shutdown

**Before**:

```go
go func() {
	if err := app.Listen(":3000"); err != nil {
		log.Panic(err)
	}
}()

c := make(chan os.Signal, 1)
signal.Notify(c, os.Interrupt, syscall.SIGTERM)
<-c
_ = app.Shutdown()
```

**After**:

```go
if err := app.Run(":3000"); err != nil {
	log.Fatal(err)
}
```

`Run` stops accepting connections on SIGINT or SIGTERM, tells active streams,
and waits up to 25 seconds for in-flight work (`photon.ShutdownTimeout(d)` to
change). Fiber's `Prefork` has no equivalent; a single Go process already uses
every core.

## Moving a route at a time

Fiber and Photon don't share a handler type, so you can't mount one inside the
other directly. Fiber's adaptor package converts handlers between fasthttp and
`net/http`, but check how it treats streaming responses before you rely on it
for SSE routes. Two approaches that don't depend on it:

1. **Route by path at your load balancer or ingress**, sending migrated paths to
   the Photon service and the rest to Fiber.
2. **Put Photon in front and proxy everything it doesn't handle to Fiber:**

```go
legacy, err := url.Parse("http://127.0.0.1:3000") // the Fiber app, unchanged
if err != nil {
	log.Fatal(err)
}
proxy := httputil.NewSingleHostReverseProxy(legacy)

app := photon.New()
app.Mount("/", proxy) // everything Photon does not route goes to Fiber

app.POST("/chat", photon.SSE(chat)) // migrated: routes take precedence over mounts
```

With the proxy, Fiber sees Photon as its client, so `c.IP()` returns Photon's
address unless you configure Fiber to trust the forwarded header from it.
Requests also pass through Photon's limits first, including the 1 MiB body
limit. Migrate the streaming routes first; they get the most from the move.

## Testing

`app.Test(req)` becomes standard `httptest`:

```go
func TestGetUser(t *testing.T) {
	app := photon.New()
	registerRoutes(app)

	rec := httptest.NewRecorder()
	app.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/users/42", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d", rec.Code)
	}
}
```

For streaming tests, use `httptest.NewServer(app)` and read the response body as
it arrives.

## Gotchas

- **Fiber middleware does not port.** Plan time to replace each one with a
  `net/http` equivalent.
- **Route features Photon lacks.** Optional parameters (`:id?`), `+` wildcards,
  and parameter constraints (`:id<int>`) have no equivalent. Use `:id` or a named
  catch-all, and check the value in the handler.
- **Overlapping routes panic.** Fiber matches routes in registration order, so
  `/users/new` and `/users/:id` can both exist. Photon refuses at registration
  for the same method. Move one under a distinct segment (`/user-forms/new`), or
  keep `/users/:id` and branch on the value.
- **No trailing-slash leniency.** Patterns ending in `/` are rejected, and
  `/users/` is a 404, not `/users`.
- **Catch-alls are named and non-empty.** `/files/*` becomes `/files/*path`,
  which matches `/files/a` but not `/files/`.
- **`PathParam` lifetime.** Same rule as Fiber's buffers, but only for path
  parameters. Use `photon.CopyParam` to keep one.
- **`BodyParser` was format-agnostic; `DecodeJSON` is not.** It returns 415 for a
  non-JSON `Content-Type`. Parse forms with `r.ParseForm`.
- **Smaller default body limit.** Fiber's default `BodyLimit` is 4 MB. Photon's
  `MaxRequestBodyBytes` is 1 MiB. Raise it if you need to:
  `l := photon.DefaultLimits(); l.MaxRequestBodyBytes = 4 << 20; app := photon.New(photon.SetLimits(l))`.
- **`photon.ClientIP` is the socket peer.** It ignores forwarded headers. If you
  used Fiber's `ProxyHeader`, decide how your deployment should establish the
  client address.
- **The route pattern is `r.Pattern`.** For metrics labelled by route, Photon
  sets the standard library's `r.Pattern` to the matched template
  (`/users/:id`), visible to the handler and to all middleware.
- **`GET` does not answer `HEAD`.** Register `app.HEAD` where monitors use it.
- **Routes are fixed once the server starts.** Registering after `Run` panics.
- **No WebSockets in Photon.** If you used Fiber's websocket package, use
  `github.com/coder/websocket` on a Photon route; it works on any `http.Handler`.

## Checklist

- [ ] Replace `fiber.New()` with `photon.New()`
- [ ] Convert handlers from `func(c *fiber.Ctx) error` to `func(w http.ResponseWriter, r *http.Request)` (or use the `handle` adapter)
- [ ] `c.Params` to `photon.PathParam`; `photon.CopyParam` for values that outlive the handler
- [ ] `c.BodyParser` to `photon.DecodeJSON` (JSON) or `r.ParseForm` (forms), plus explicit validation
- [ ] `c.Locals` to context values; `c.UserContext()` to `r.Context()`
- [ ] `fiber.NewError` to `photonerr`, rendered with `photon.Error`
- [ ] Replace every Fiber middleware with a `net/http` one; add `Unwrap()` to `ResponseWriter` wrappers
- [ ] Rewrite optional parameters, constraints, and overlapping routes
- [ ] Replace stream writers with `photon.SSE`
- [ ] Replace `app.Listen` and shutdown code with `app.Run`
- [ ] Raise `MaxRequestBodyBytes` if any endpoint relied on Fiber's 4 MB default
- [ ] Tell client teams about the problem+json error format
