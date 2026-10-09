# Migrating from gin

> **What changes:** every handler. `func(c *gin.Context)` becomes `func(w http.ResponseWriter, r *http.Request)`, and `c.Param`, `c.JSON`, `c.ShouldBindJSON`, and `c.Next()` each become a plain `net/http` equivalent.
> **What stays the same:** the route syntax (`/users/:id`, `/files/*path`), groups, and the overall shape of the app.
> **Watch out for:** no binding or validation tags, no trailing-slash redirects, static-beside-param routes panic, and catch-all values no longer start with `/`.

gin's API is built around `*gin.Context`. Photon has no context type: handlers
receive the standard `http.ResponseWriter` and `*http.Request`, and Photon's
helpers are free functions that take them. Most of the work is a mechanical
translation, handler by handler.

- [A whole program, before and after](#a-whole-program-before-and-after)
- [gin.Context, method by method](#gincontext-method-by-method)
- [Binding and validation](#binding-and-validation)
- [Errors and aborting](#errors-and-aborting)
- [Passing values between middleware and handlers](#passing-values-between-middleware-and-handlers)
- [Middleware](#middleware)
- [Groups](#groups)
- [Streaming](#streaming)
- [Route conflicts](#route-conflicts)
- [Trailing slashes](#trailing-slashes)
- [Graceful shutdown](#graceful-shutdown)
- [Testing](#testing)
- [Gotchas](#gotchas)
- [Checklist](#checklist)

## A whole program, before and after

**Before** (gin v1):

```go
package main

import (
	"net/http"

	"github.com/gin-gonic/gin"
)

type createUserRequest struct {
	Name  string `json:"name" binding:"required"`
	Email string `json:"email" binding:"required,email"`
}

func main() {
	r := gin.Default()

	r.GET("/users/:id", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{"id": c.Param("id")})
	})

	r.POST("/users", func(c *gin.Context) {
		var req createUserRequest
		if err := c.ShouldBindJSON(&req); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
		}
		c.JSON(http.StatusCreated, gin.H{"name": req.Name, "email": req.Email})
	})

	r.Run(":8080")
}
```

**After**:

```go
package main

import (
	"log"
	"net/http"
	"strings"

	"github.com/agenticmarket/photon"
	"github.com/agenticmarket/photon/photonerr"
)

type createUserRequest struct {
	Name  string `json:"name"`
	Email string `json:"email"`
}

// validate replaces the binding tags.
func (req createUserRequest) validate() error {
	if strings.TrimSpace(req.Name) == "" {
		return photonerr.BadRequest("name is required")
	}
	if !strings.Contains(req.Email, "@") { // or whatever rule you actually need
		return photonerr.BadRequest("email must be an email address")
	}
	return nil
}

func main() {
	app := photon.New() // panics are recovered by default; there is no gin.Default to opt into

	app.GET("/users/:id", func(w http.ResponseWriter, r *http.Request) {
		_ = photon.JSON(w, http.StatusOK, map[string]any{"id": photon.PathParam(r, "id")})
	})

	app.POST("/users", func(w http.ResponseWriter, r *http.Request) {
		var req createUserRequest
		if err := photon.DecodeJSON(r, &req); err != nil {
			photon.Error(w, r, err)
			return
		}
		if err := req.validate(); err != nil {
			photon.Error(w, r, err)
			return
		}
		_ = photon.JSON(w, http.StatusCreated, map[string]any{"name": req.Name, "email": req.Email})
	})

	// Run also handles SIGINT/SIGTERM and shuts down gracefully. gin's Run does not.
	if err := app.Run(":8080"); err != nil {
		log.Fatal(err)
	}
}
```

`gin.Default()` adds a logger and panic recovery. Photon recovers panics on its
own (500, plus an optional `photon.OnPanic` hook). It does not write access
logs; see [Middleware](#middleware) for a short one.

## gin.Context, method by method

| gin | Photon / `net/http` |
|---|---|
| `c.Param("id")` | `photon.PathParam(r, "id")` |
| `c.Param("filepath")` for `/*filepath` | `photon.PathParam(r, "filepath")`, **without** the leading `/` |
| `c.Query("q")` | `r.URL.Query().Get("q")` |
| `c.DefaultQuery("page", "1")` | read with `r.URL.Query().Get`, then apply the default yourself |
| `c.PostForm("name")` | `r.PostFormValue("name")` |
| `c.GetHeader("X-Key")` | `r.Header.Get("X-Key")` |
| `c.Header("X-Key", v)` | `w.Header().Set("X-Key", v)` |
| `c.ShouldBindJSON(&v)` | `photon.DecodeJSON(r, &v)`, then validate |
| `c.JSON(200, v)` | `photon.JSON(w, 200, v)` |
| `gin.H{...}` | `map[string]any{...}` |
| `c.String(200, "hi %s", name)` | `photon.Text(w, 200, fmt.Sprintf("hi %s", name))` |
| `c.Status(204)` | `photon.NoContent(w)` |
| `c.Redirect(302, url)` | `http.Redirect(w, r, url, http.StatusFound)` (`photon.Redirect` always sends 303) |
| `c.AbortWithStatusJSON(401, obj)` | `photon.Error(w, r, photonerr.Unauthorized(msg))` then `return` |
| `c.Set("user", u)` / `c.Get("user")` | `context.WithValue` / `r.Context().Value` (see below) |
| `c.Next()` | `next.ServeHTTP(w, r)` |
| `c.ClientIP()` | `photon.ClientIP(r)` (different rules, see [Gotchas](#gotchas)) |
| `c.Request` | `r` |
| `c.Writer` | `w` |
| `c` used as a `context.Context` | `r.Context()` |
| `c.Stream`, `c.SSEvent` | `photon.SSE` |
| `c.FullPath()` | `r.Pattern` |

## Binding and validation

gin's `ShouldBindJSON` decodes and then runs `binding:"..."` tags through
go-playground/validator. Photon's `DecodeJSON` only decodes. It has no tags and
no validation.

The straightforward replacement is a `validate` method per request type, as in
the program above. It's more code than tags, but each rule is plain Go you can
read and test, and the messages are ones you chose.

If you have many tagged structs and want to keep them, you can call validator
yourself. gin reads the `binding` tag name, so tell validator to do the same and
your existing tags work unchanged:

```go
import "github.com/go-playground/validator/v10"

var validate = func() *validator.Validate {
	v := validator.New()
	v.SetTagName("binding") // reuse the tags you already wrote for gin
	return v
}()

func createUser(w http.ResponseWriter, r *http.Request) {
	var req createUserRequest
	if err := photon.DecodeJSON(r, &req); err != nil {
		photon.Error(w, r, err)
		return
	}
	if err := validate.Struct(&req); err != nil {
		photon.Error(w, r, photonerr.BadRequest("invalid request body").WithCause(err))
		return
	}
	// ...
}
```

That is your dependency, not Photon's. Note that `WithCause` keeps validator's
message for your logs; the client sees only "invalid request body". Build a more
specific safe message from the validation errors if your clients need one.

Differences in decoding to know about:

- `DecodeJSON` returns **415** when the request has a `Content-Type` that isn't
  JSON. `ShouldBindJSON` ignores the content type. A client sending JSON as
  `text/plain` will start failing; a client sending no `Content-Type` still works.
- `DecodeJSON` rejects trailing data after the first JSON value and reports a
  body over `MaxRequestBodyBytes` (1 MiB by default) as 413.
- Unknown fields are allowed, as in gin's default.
- `ShouldBindQuery`, `ShouldBindUri`, `ShouldBindHeader` have no equivalent. Read
  `r.URL.Query()`, `photon.PathParam`, and `r.Header` and convert with `strconv`.

## Errors and aborting

In gin, a middleware or handler writes an error and calls `c.Abort...` so later
handlers don't run. In `net/http` you write the error and `return` without
calling the next handler.

**Before**:

```go
user, err := store.Find(c.Request.Context(), c.Param("id"))
if errors.Is(err, store.ErrNotFound) {
	c.AbortWithStatusJSON(http.StatusNotFound, gin.H{"error": "user not found"})
	return
}
if err != nil {
	c.AbortWithStatusJSON(http.StatusInternalServerError, gin.H{"error": "internal error"})
	return
}
c.JSON(http.StatusOK, user)
```

**After**:

```go
user, err := store.Find(r.Context(), photon.PathParam(r, "id"))
if errors.Is(err, store.ErrNotFound) {
	photon.Error(w, r, photonerr.NotFound("user")) // 404, "user not found"
	return
}
if err != nil {
	photon.Error(w, r, err) // 500, constant body; the real error is logged
	return
}
_ = photon.JSON(w, http.StatusOK, user)
```

The `photonerr` constructors cover the usual statuses: `BadRequest`,
`Unauthorized`, `Forbidden`, `NotFound`, `Conflict`, `UnsupportedMediaType`,
`TooManyRequests`, `Timeout`, `Upstream`, `Internal`, `Unavailable`, and more.
Add operator-only context with `.WithDetail(...)` or `.WithCause(err)`; neither
is sent to the client.

The response is `application/problem+json`, not gin's `{"error": "..."}`:

```json
{"type":"https://…/errors/not_found","title":"user not found","status":404,"code":"not_found"}
```

If clients depend on the old shape, write it with
`photon.JSON(w, http.StatusNotFound, map[string]any{"error": "user not found"})`
until they move.

## Passing values between middleware and handlers

`c.Set` and `c.Get` become request context values. Use an unexported key type so
nothing else can collide with it, and wrap access in two small functions:

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

The middleware passes the new request on with `next.ServeHTTP(w, withUser(r, u))`,
and the handler calls `userFrom(r)`. Path parameters survive `r.WithContext`, so
`photon.PathParam` keeps working downstream.

## Middleware

gin middleware is a handler that calls `c.Next()`. `net/http` middleware is a
function that wraps the next handler. Code before `c.Next()` goes before
`next.ServeHTTP`, code after it goes after.

**Before**:

```go
func AuthRequired() gin.HandlerFunc {
	return func(c *gin.Context) {
		user, err := lookupToken(c.GetHeader("Authorization"))
		if err != nil {
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": "unauthorized"})
			return
		}
		c.Set("user", user)
		c.Next()
	}
}

func Timing() gin.HandlerFunc {
	return func(c *gin.Context) {
		start := time.Now()
		c.Next()
		log.Printf("%s %s %d %s", c.Request.Method, c.Request.URL.Path, c.Writer.Status(), time.Since(start))
	}
}
```

**After**:

```go
func authRequired(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		user, err := lookupToken(r.Header.Get("Authorization"))
		if err != nil {
			photon.Error(w, r, photonerr.Unauthorized("missing or invalid credentials"))
			return // not calling next is the abort
		}
		next.ServeHTTP(w, withUser(r, user))
	})
}

func timing(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		rec := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
		next.ServeHTTP(rec, r)
		log.Printf("%s %s %d %s", r.Method, r.URL.Path, rec.status, time.Since(start))
	})
}

// statusRecorder captures the status code, which gin kept in c.Writer.Status().
type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (s *statusRecorder) WriteHeader(code int) {
	s.status = code
	s.ResponseWriter.WriteHeader(code)
}

// Unwrap is required: Photon reaches Flush and the write deadline through it.
// Without it, streams through this middleware fail.
func (s *statusRecorder) Unwrap() http.ResponseWriter { return s.ResponseWriter }
```

Attaching it:

```go
app.Use(timing)                               // every request, including 404 and 405
admin := app.Group("/admin", authRequired)    // every route on the group
app.GET("/reports", reports, authRequired)    // one route
```

The order is `app.Use` middleware first, then group middleware, then per-route
middleware, then the handler.

`app.Use` applies to every route, including routes registered before the call,
and to 404 and 405 responses. gin's `r.Use` only affects routes registered after
it. `Group.Use` in Photon behaves like gin's: it applies to routes registered on
the group after the call.

### gin-contrib middleware

gin middleware doesn't work under Photon, but most of it has a `net/http`
counterpart. For CORS, `github.com/rs/cors` is a common choice:

```go
import "github.com/rs/cors"

c := cors.New(cors.Options{
	AllowedOrigins: []string{"https://app.example.com"},
	AllowedMethods: []string{http.MethodGet, http.MethodPost},
	AllowedHeaders: []string{"Authorization", "Content-Type"},
})
app.Use(c.Handler)
```

Because `app.Use` covers 405 responses, preflight `OPTIONS` requests reach the
CORS middleware even for routes that only registered `POST`. More in
[../guides/middleware.md](../guides/middleware.md).

## Groups

```go
// Before
v1 := r.Group("/v1")
v1.Use(AuthRequired())
{
	v1.GET("/me", me)
	v1.POST("/chat", chat)
}

// After
v1 := app.Group("/v1", authRequired)
v1.GET("/me", me)
v1.POST("/chat", photon.SSE(chat))
```

Group prefixes may contain parameters (`app.Group("/orgs/:org")`), groups nest
with `v1.Group("/admin", requireAdmin)`, and a group has `Mount` as well as the
verbs. A gin group created with `r.Group("/", mw)` to share middleware without a
prefix becomes `app.Group("", mw)`.

## Streaming

**Before** (gin):

```go
r.POST("/chat", func(c *gin.Context) {
	var req chatRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	tokens := generate(c.Request.Context(), req.Prompt)
	c.Stream(func(w io.Writer) bool {
		tok, ok := <-tokens
		if !ok {
			c.SSEvent("done", "")
			return false
		}
		c.SSEvent("token", tok)
		return true
	})
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

What `photon.SSE` adds over `c.Stream`:

- A returned error before the first event becomes a normal HTTP error response.
  After the first event it is sent as an `event: error` with a problem document.
- A client that stops reading applies back-pressure, and one that stays stuck is
  disconnected after `StreamStallTimeout` (30s), so it can't hold a goroutine
  and an upstream model call indefinitely.
- `MaxStreams` caps concurrent streams, answering 503 with `Retry-After` when
  full.
- Comment heartbeats go out after 15 seconds of silence, so proxies keep the
  connection open.
- On shutdown, the stream is told through `s.Done()` and given up to the
  shutdown timeout to finish, rather than being cut mid-answer.

`s.JSON(name, v)` sends a JSON-encoded event, and `s.Event("", data)` sends an
unnamed one (what OpenAI-style clients expect). See
[../guides/streaming.md](../guides/streaming.md).

## Route conflicts

Current gin releases let a static segment and a parameter share a position:

```go
r.GET("/users/new", newUserForm)
r.GET("/users/:id", getUser)
```

Photon panics at registration for the same method, naming both call sites,
because which handler runs would depend on the request in ways that are easy to
get wrong. The same goes for `/admin/settings` beside `/admin/*rest`. (`/files`
beside `/files/*path` is fine.)

Fix it by moving one route under a distinct segment:

```go
app.GET("/user-forms/new", newUserForm)
app.GET("/users/:id", getUser)
```

Or, if clients depend on the URLs, keep one parameter route and branch:

```go
app.GET("/users/:id", func(w http.ResponseWriter, r *http.Request) {
	if photon.PathParam(r, "id") == "new" {
		newUserForm(w, r)
		return
	}
	getUser(w, r)
})
```

Routes for different methods don't conflict: `GET /users/new` and
`POST /users/:id` can coexist.

## Trailing slashes

gin redirects `/users/` to `/users` (and the reverse) by default through
`RedirectTrailingSlash`. Photon does neither. A pattern ending in `/` is rejected
at registration, and a request for `/users/` is a 404.

Most API clients never send the trailing slash. If yours do, answer those 404s
with a redirect:

```go
// redirectTrailingSlash redirects GET and HEAD requests for "/path/" to "/path",
// as gin's RedirectTrailingSlash did. Install it with app.NotFound.
func redirectTrailingSlash(w http.ResponseWriter, r *http.Request) {
	p := r.URL.EscapedPath()
	if (r.Method != http.MethodGet && r.Method != http.MethodHead) ||
		len(p) < 2 || !strings.HasSuffix(p, "/") ||
		strings.ContainsRune(r.URL.Path, '\\') {
		photon.Error(w, r, photonerr.NotFound(""))
		return
	}
	// "/" + Trim collapses "//evil.example/" to "/evil.example", so the
	// redirect always stays on this host.
	target := "/" + strings.Trim(p, "/")
	if r.URL.RawQuery != "" {
		target += "?" + r.URL.RawQuery
	}
	http.Redirect(w, r, target, http.StatusMovedPermanently)
}

app.NotFound(http.HandlerFunc(redirectTrailingSlash))
```

It only runs for paths no route matched, so a catch-all like `/files/*path`
still receives `/files/a/` unchanged.

## Graceful shutdown

**Before** (the pattern from gin's docs):

```go
srv := &http.Server{Addr: ":8080", Handler: r}
go func() {
	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Fatal(err)
	}
}()
quit := make(chan os.Signal, 1)
signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
<-quit
ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
defer cancel()
if err := srv.Shutdown(ctx); err != nil {
	log.Fatal(err)
}
```

**After**:

```go
if err := app.Run(":8080"); err != nil {
	log.Fatal(err)
}
```

`Run` waits up to 25 seconds by default; change it with
`photon.New(photon.ShutdownTimeout(d))`. Active streams are told through
`s.Done()` when shutdown starts.

## Testing

gin tests that call `router.ServeHTTP(w, req)` translate directly:

```go
func TestGetUser(t *testing.T) {
	app := photon.New()
	registerRoutes(app)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/users/42", nil)
	app.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d", rec.Code)
	}
}
```

`httptest.NewServer(app)` works too, and is what you want for streaming tests.

## Gotchas

- **No binding tags.** See [Binding and validation](#binding-and-validation).
- **415 for non-JSON content types** where `ShouldBindJSON` used to accept them.
- **Catch-all values have no leading slash.** For `/files/*path` and a request to
  `/files/a/b.txt`, gin's `c.Param("path")` was `/a/b.txt`; Photon's
  `PathParam(r, "path")` is `a/b.txt`. The catch-all also needs at least one
  character, so `/files/` is not matched.
- **`PathParam` is valid only during the handler.** gin's `c.Param` returned an
  ordinary string you could keep. Photon's aliases request memory. Use
  `photon.CopyParam` when the value goes into a goroutine, a cache, a channel, or
  an async logger. The race detector does not catch this.
- **`r.Context()` is cancelled when the client leaves.** If your gin code passed
  `c` as a `context.Context`, it likely didn't carry the request's cancellation
  (unless you enabled gin's `ContextWithFallback`). `r.Context()` does, so
  database and upstream calls now return `context.Canceled` when a client
  disconnects. That's usually what you want, but expect it in logs.
- **`photon.ClientIP` ignores `X-Forwarded-For`.** gin's `c.ClientIP()` reads
  forwarded headers unless you configured trusted proxies. Photon returns the
  socket peer, so behind a load balancer it's the balancer's address.
- **The route pattern is `r.Pattern`.** Where gin has `c.FullPath()`, Photon
  sets the standard library's `r.Pattern` to the matched template
  (`/users/:id`), visible to the handler and to all middleware. It is empty on a
  404.
- **`HEAD` is not implied by `GET`.** Register `app.HEAD` where monitors use it.
- **Routes are fixed once the server starts.** Registering after `Run` panics.
- **Default limits.** 1 MiB bodies, 16 KiB of headers, 100 header fields, 8 KiB
  per header value. gin has no body limit by default, so check your upload
  endpoints and anything that sends large cookies or tokens.
- **`gin.WrapH` and `gin.WrapF` go away.** Photon takes `http.Handler` and
  `http.HandlerFunc` directly.

## Checklist

- [ ] Replace `gin.Default()` / `gin.New()` with `photon.New()`
- [ ] Convert each handler to `func(w http.ResponseWriter, r *http.Request)`
- [ ] `c.Param` to `photon.PathParam`; `photon.CopyParam` where values outlive the handler
- [ ] Strip the leading `/` assumption from catch-all values
- [ ] `c.ShouldBindJSON` to `photon.DecodeJSON` plus explicit validation (or validator with `SetTagName("binding")`)
- [ ] `c.JSON` to `photon.JSON`, `gin.H` to `map[string]any`
- [ ] `c.AbortWithStatusJSON` to `photon.Error` plus `return`
- [ ] `c.Set`/`c.Get` to context values
- [ ] Rewrite middleware as `func(http.Handler) http.Handler`, with `Unwrap()` on any `ResponseWriter` wrapper
- [ ] Replace gin-contrib middleware with `net/http` equivalents
- [ ] Fix static-beside-param route conflicts
- [ ] Decide on trailing slashes: 404, or the `NotFound` redirect above
- [ ] Replace `c.Stream` / `c.SSEvent` with `photon.SSE`
- [ ] Replace `r.Run` and shutdown code with `app.Run`
- [ ] Review default limits and the problem+json error format with client teams
