# Migrating from echo

> **What changes:** every handler. `func(c echo.Context) error` becomes `func(w http.ResponseWriter, r *http.Request)`, `echo.HTTPError` becomes `photonerr`, and `e.Start`/`e.Shutdown` become `app.Run`.
> **What stays the same:** the route syntax (`/users/:id`), groups with middleware, and the idea of typed errors rendered in one place.
> **Watch out for:** handlers no longer return errors, `c.Bind` has no equivalent (decode the body, read params explicitly), echo middleware must be rewritten, and static-beside-param routes panic.

echo handlers take an `echo.Context` and return an `error` that a central
`HTTPErrorHandler` renders. Photon handlers are plain `net/http` handlers, and
you render errors where they happen with `photon.Error`. If you like the
error-returning style, a five-line adapter keeps it (see
[Keeping error-returning handlers](#keeping-error-returning-handlers)).

- [A whole program, before and after](#a-whole-program-before-and-after)
- [echo.Context, method by method](#echocontext-method-by-method)
- [Binding](#binding)
- [echo.HTTPError to photonerr](#echohttperror-to-photonerr)
- [Keeping error-returning handlers](#keeping-error-returning-handlers)
- [Middleware](#middleware)
- [Groups](#groups)
- [Streaming](#streaming)
- [Start and shutdown](#start-and-shutdown)
- [Testing](#testing)
- [Gotchas](#gotchas)
- [Checklist](#checklist)

## A whole program, before and after

**Before** (echo v4):

```go
package main

import (
	"net/http"

	"github.com/labstack/echo/v4"
	"github.com/labstack/echo/v4/middleware"
)

type createUserRequest struct {
	Name string `json:"name"`
}

func main() {
	e := echo.New()
	e.Use(middleware.Logger())
	e.Use(middleware.Recover())

	e.GET("/users/:id", func(c echo.Context) error {
		return c.JSON(http.StatusOK, map[string]any{"id": c.Param("id")})
	})

	e.POST("/users", func(c echo.Context) error {
		var req createUserRequest
		if err := c.Bind(&req); err != nil {
			return echo.NewHTTPError(http.StatusBadRequest, "invalid body")
		}
		if req.Name == "" {
			return echo.NewHTTPError(http.StatusBadRequest, "name is required")
		}
		return c.JSON(http.StatusCreated, req)
	})

	e.Logger.Fatal(e.Start(":8080"))
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
	app := photon.New() // panics are recovered by default
	app.Use(requestLog) // your own net/http logging middleware; see below

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

	if err := app.Run(":8080"); err != nil {
		log.Fatal(err)
	}
}
```

## echo.Context, method by method

| echo | Photon / `net/http` |
|---|---|
| `c.Param("id")` | `photon.PathParam(r, "id")` |
| `c.Param("*")` for a `*` route | Name the catch-all: `/files/*path`, then `photon.PathParam(r, "path")` |
| `c.QueryParam("q")` | `r.URL.Query().Get("q")` |
| `c.FormValue("name")` | `r.FormValue("name")` |
| `c.Request()` | `r` |
| `c.Response()` | `w` |
| `c.Request().Header.Get("X")` | `r.Header.Get("X")` |
| `c.Response().Header().Set("X", v)` | `w.Header().Set("X", v)` |
| `c.Bind(&v)` | `photon.DecodeJSON(r, &v)` plus explicit params (see below) |
| `c.Validate(&v)` | Call your validator directly |
| `return c.JSON(200, v)` | `_ = photon.JSON(w, 200, v)` |
| `return c.String(200, s)` | `_ = photon.Text(w, 200, s)` |
| `return c.HTML(200, s)` | `_ = photon.HTML(w, 200, s)` |
| `return c.NoContent(204)` | `_ = photon.NoContent(w)` |
| `return c.Redirect(302, url)` | `http.Redirect(w, r, url, http.StatusFound)` (`photon.Redirect` always sends 303) |
| `return echo.NewHTTPError(400, msg)` | `photon.Error(w, r, photonerr.BadRequest(msg)); return` |
| `c.Set("user", u)` / `c.Get("user")` | `context.WithValue` / `r.Context().Value` |
| `c.RealIP()` | `photon.ClientIP(r)` (different rules, see [Gotchas](#gotchas)) |
| `c.Path()` (route pattern) | `r.Pattern` |
| `e.Static("/static", "public")` | `app.Mount("/static", http.StripPrefix("/static", http.FileServer(http.Dir("public"))))` |

Context values, with an unexported key type:

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

## Binding

`c.Bind` fills one struct from several places: path parameters, query
parameters, and the body, depending on the method, the content type, and the
struct tags (`param`, `query`, `json`, `form`). `photon.DecodeJSON` reads the JSON
body and nothing else. Path and query values are read explicitly.

**Before**:

```go
type updateUserRequest struct {
	ID   string `param:"id"`
	Name string `json:"name"`
}

e.PUT("/users/:id", func(c echo.Context) error {
	var req updateUserRequest
	if err := c.Bind(&req); err != nil {
		return err
	}
	if err := c.Validate(&req); err != nil {
		return echo.NewHTTPError(http.StatusBadRequest, err.Error())
	}
	return c.JSON(http.StatusOK, save(req))
})
```

**After**:

```go
type updateUserRequest struct {
	Name string `json:"name"`
}

app.PUT("/users/:id", func(w http.ResponseWriter, r *http.Request) {
	id := photon.PathParam(r, "id")

	var req updateUserRequest
	if err := photon.DecodeJSON(r, &req); err != nil {
		photon.Error(w, r, err)
		return
	}
	if req.Name == "" {
		photon.Error(w, r, photonerr.BadRequest("name is required"))
		return
	}
	_ = photon.JSON(w, http.StatusOK, save(id, req))
})
```

There are no validation tags. Write checks in Go, or, if you used
go-playground/validator through `e.Validator`, call it directly after
`DecodeJSON` and wrap failures in `photonerr.BadRequest`.

`DecodeJSON` also differs from `c.Bind` in that it:

- returns **415** for a non-JSON `Content-Type` (a missing one is accepted) and
  never parses form or XML bodies;
- returns **413** for bodies over `MaxRequestBodyBytes` (1 MiB by default);
- rejects trailing data after the first JSON value;
- allows unknown fields.

## echo.HTTPError to photonerr

| echo | photonerr | Status |
|---|---|---|
| `echo.NewHTTPError(400, msg)`, `echo.ErrBadRequest` | `photonerr.BadRequest(msg)` | 400 |
| `echo.ErrUnauthorized` | `photonerr.Unauthorized(msg)` | 401 |
| `echo.ErrForbidden` | `photonerr.Forbidden(msg)` | 403 |
| `echo.ErrNotFound` | `photonerr.NotFound("user")` (message: "user not found") | 404 |
| `echo.ErrMethodNotAllowed` | `photonerr.NotAllowed()` | 405 |
| `echo.NewHTTPError(409, msg)` | `photonerr.Conflict(msg)` | 409 |
| `echo.ErrStatusRequestEntityTooLarge` | `photonerr.LimitExceeded("MaxRequestBodyBytes")` | 413 |
| `echo.ErrUnsupportedMediaType` | `photonerr.UnsupportedMediaType(msg)` | 415 |
| `echo.ErrTooManyRequests` | `photonerr.TooManyRequests(msg)` | 429 |
| `echo.ErrInternalServerError` | `photonerr.Internal(cause)` | 500 |
| `echo.ErrBadGateway` | `photonerr.Upstream(cause)` | 502 |
| `echo.ErrServiceUnavailable` | `photonerr.Unavailable(reason)` | 503 |
| `he.SetInternal(err)` | `.WithCause(err)` | |

Two differences in how errors are rendered:

- **The body is RFC 9457 problem+json.** echo's default handler sends
  `{"message": "..."}`. Photon sends
  `{"type": "…", "title": "...", "status": 400, "code": "bad_request"}`. Update
  clients that parse the old shape, or write it yourself with `photon.JSON`.
- **Unknown errors never leak.** A plain `error` passed to `photon.Error` becomes
  a 500 with a constant body, and 5xx errors are logged with the real cause.
  `.WithDetail(...)` and `.WithCause(err)` add operator context that is never
  sent to the client.

## Keeping error-returning handlers

If you have a lot of echo handlers and want to keep the `return err` style while
you port them, a small adapter does it:

```go
// handle adapts an error-returning handler. Return a *photonerr.Error for a
// client-facing error; anything else becomes a 500.
func handle(fn func(w http.ResponseWriter, r *http.Request) error) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if err := fn(w, r); err != nil {
			photon.Error(w, r, err)
		}
	}
}

app.GET("/users/:id", handle(func(w http.ResponseWriter, r *http.Request) error {
	user, err := store.Find(r.Context(), photon.PathParam(r, "id"))
	if errors.Is(err, store.ErrNotFound) {
		return photonerr.NotFound("user")
	}
	if err != nil {
		return err
	}
	return photon.JSON(w, http.StatusOK, user)
}))
```

One rule: only return an error before you have written the response. Once
headers are sent, `photon.Error` cannot change the status. `photon.JSON` is safe
to return here, because it returns an error only when encoding fails, before
anything is written.

## Middleware

echo middleware is `func(next echo.HandlerFunc) echo.HandlerFunc`. Photon's is
`func(next http.Handler) http.Handler`. The shape is the same, so the port is
mostly mechanical.

**Before**:

```go
func requireUser(next echo.HandlerFunc) echo.HandlerFunc {
	return func(c echo.Context) error {
		user, err := lookupToken(c.Request().Header.Get("Authorization"))
		if err != nil {
			return echo.NewHTTPError(http.StatusUnauthorized, "missing or invalid credentials")
		}
		c.Set("user", user)
		return next(c)
	}
}
```

**After**:

```go
func requireUser(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		user, err := lookupToken(r.Header.Get("Authorization"))
		if err != nil {
			photon.Error(w, r, photonerr.Unauthorized("missing or invalid credentials"))
			return
		}
		next.ServeHTTP(w, withUser(r, user))
	})
}
```

Attaching it:

```go
app.Use(requestLog)                         // every request, including 404 and 405
admin := app.Group("/admin", requireUser)   // a group
app.GET("/me", me, requireUser)             // one route
```

Order is `app.Use`, then group, then per-route, then the handler. `app.Use`
covers routes registered before and after the call. `Group.Use` covers routes
registered on the group after it.

echo's `e.Pre(...)` (middleware that runs before routing) has no equivalent.
Photon's middleware runs after the route is chosen, so rewriting `r.URL.Path`
in middleware does not change which route runs. Register the canonical paths
instead.

If a middleware wraps `http.ResponseWriter`, it must have an
`Unwrap() http.ResponseWriter` method or streams through it fail. A status
recorder for an access log:

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

### echo's built-in middleware

| echo middleware | Under Photon |
|---|---|
| `middleware.Recover()` | Built in. Use `photon.OnPanic` to route panic reports. |
| `middleware.Logger()` | Your own, like `requestLog` above. |
| `middleware.BodyLimit("2M")` | `MaxRequestBodyBytes` in `photon.Limits` (1 MiB by default). |
| `middleware.CORS()` | A `net/http` CORS package, for example `github.com/rs/cors`: `app.Use(cors.New(opts).Handler)`. |
| `middleware.RequestID()` | A short middleware that sets a header and a context value. |
| `middleware.Timeout()` | `http.TimeoutHandler` on non-streaming routes only (it buffers and cannot flush). |
| `middleware.Gzip()` | A `net/http` compression middleware, kept off SSE routes. |
| `middleware.RemoveTrailingSlash()` | No equivalent; see [Gotchas](#gotchas). |
| `echo.WrapMiddleware(mw)` | Not needed: `mw` is already Photon middleware. |

## Groups

```go
// Before
admin := e.Group("/admin", requireUser)
admin.GET("/stats", stats)

// After
admin := app.Group("/admin", requireUser)
admin.GET("/stats", stats)
```

Group prefixes can contain parameters (`app.Group("/orgs/:org")`) and groups
nest (`admin.Group("/billing", requireOwner)`).

## Streaming

**Before** (echo, writing and flushing by hand):

```go
e.POST("/chat", func(c echo.Context) error {
	var req chatRequest
	if err := c.Bind(&req); err != nil {
		return echo.NewHTTPError(http.StatusBadRequest, "invalid body")
	}
	w := c.Response()
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.WriteHeader(http.StatusOK)

	for tok := range generate(c.Request().Context(), req.Prompt) {
		if _, err := fmt.Fprintf(w, "event: token\ndata: %s\n\n", tok); err != nil {
			return err
		}
		w.Flush()
	}
	fmt.Fprint(w, "event: done\ndata:\n\n")
	w.Flush()
	return nil
})
```

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

The `SSE` function returns an error, much like an echo handler. Before the first
event, a returned error becomes a normal HTTP error response through
`photon.Error`. After it, the status line is already sent, so the error goes
in-band as an `event: error` with a problem document and the connection is
closed.

Around your function, Photon also frames multi-line data correctly, applies
back-pressure to slow clients and disconnects stuck ones after 30 seconds, caps
concurrent streams (503 with `Retry-After`), sends heartbeats after 15 seconds of
silence, and tells the stream about shutdown through `s.Done()`. See
[../guides/streaming.md](../guides/streaming.md).

## Start and shutdown

**Before** (the pattern from echo's docs):

```go
go func() {
	if err := e.Start(":8080"); err != nil && !errors.Is(err, http.ErrServerClosed) {
		e.Logger.Fatal("shutting down the server")
	}
}()

quit := make(chan os.Signal, 1)
signal.Notify(quit, os.Interrupt, syscall.SIGTERM)
<-quit
ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
defer cancel()
if err := e.Shutdown(ctx); err != nil {
	e.Logger.Fatal(err)
}
```

**After**:

```go
app := photon.New(photon.ShutdownTimeout(10 * time.Second)) // default is 25s
if err := app.Run(":8080"); err != nil {
	log.Fatal(err)
}
```

`e.StartTLS(addr, cert, key)` maps to `app.ListenTLS(addr, cert, key)`, which
blocks like `Listen` and leaves signal handling to you (call `app.Shutdown(ctx)`
on a signal).

## Testing

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

There's no `echo.Context` to construct, so handler unit tests call the handler
directly with a recorder and a request. Note that `photon.PathParam` is filled
in by routing, so tests that need parameters go through `app.ServeHTTP`.

## Gotchas

- **Handlers don't return errors.** Forgetting `return` after `photon.Error` lets
  the handler keep going and write a second response. The `handle` adapter above
  avoids this if you prefer echo's style.
- **No `c.Bind` and no validation tags.** Decode the body, read path and query
  values yourself, validate in Go.
- **415 for non-JSON content types** where `c.Bind` would have parsed a form.
- **Static-beside-param routes panic.** echo lets `/users/new` and `/users/:id`
  coexist and prefers the static one. Photon refuses at registration. Move one
  under a distinct segment (`/user-forms/new`), or keep `/users/:id` and branch on
  the value in the handler.
- **No trailing-slash handling.** `/users/` is a 404, and patterns ending in `/`
  are rejected at registration. If clients send trailing slashes, answer those
  404s with a redirect from `app.NotFound` (the gin guide has
  [a safe version](from-gin.md#trailing-slashes)).
- **`PathParam` is valid only during the handler.** Use `photon.CopyParam` when a
  value goes to a goroutine, a channel, a cache, or an async logger. The race
  detector won't catch this.
- **`photon.ClientIP` ignores `X-Forwarded-For` and `X-Real-IP`.** echo's
  `c.RealIP()` reads them by default. Photon returns the socket peer.
- **The route pattern is `r.Pattern`.** Where echo has `c.Path()`, Photon sets
  the standard library's `r.Pattern` to the matched template (`/users/:id`),
  visible to the handler and to all middleware. It is empty on a 404.
- **`GET` does not answer `HEAD`.** Register `app.HEAD` where monitors use it.
- **Catch-alls must be named and non-empty.** echo's `/files/*` becomes
  `/files/*path`; it matches `/files/a` but not `/files/`.
- **Routes are fixed once the server starts.** Registering after `Run` panics.
- **Default limits.** 1 MiB bodies, 16 KiB of headers, 100 header fields, 8 KiB
  per header value. echo has no body limit unless you add `BodyLimit`.

## Checklist

- [ ] Replace `echo.New()` with `photon.New()`
- [ ] Convert handlers to `func(w http.ResponseWriter, r *http.Request)` (or wrap them with `handle`)
- [ ] `c.Param` to `photon.PathParam`; name every catch-all
- [ ] `c.Bind` to `photon.DecodeJSON` plus explicit path/query reads and validation
- [ ] `echo.NewHTTPError` / `echo.Err*` to `photonerr`, rendered with `photon.Error`
- [ ] `c.Set`/`c.Get` to context values
- [ ] Rewrite middleware as `func(http.Handler) http.Handler`, with `Unwrap()` on `ResponseWriter` wrappers
- [ ] Replace echo's built-in middleware using the table above
- [ ] Fix static-beside-param conflicts; decide on trailing slashes
- [ ] Replace flush loops with `photon.SSE`
- [ ] Replace `e.Start` / `e.Shutdown` with `app.Run`
- [ ] Review default limits and the problem+json error format with client teams
