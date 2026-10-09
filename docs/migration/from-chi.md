# Migrating from chi

> **What changes:** route registration (`r.Get("/users/{id}", h)` becomes `app.GET("/users/:id", h)`), `chi.URLParam` becomes `photon.PathParam`, and `r.Route`/`r.Group` become `app.Group`.
> **What stays the same:** handlers and middleware, exactly. Both are plain `net/http`.
> **Watch out for:** `Use` covers routes declared before it, middleware runs after routing, `Mount` does not make paths relative, overlapping routes panic, and regex parameters have no equivalent.

chi is Photon's closest relative. Both are routers for standard `net/http`
handlers, and middleware is `func(http.Handler) http.Handler` in both. Your
handler bodies and middleware don't change; the router calls and a few routing
behaviours do. What Photon adds on top is the server: default limits, streaming,
and graceful shutdown.

- [A whole program, before and after](#a-whole-program-before-and-after)
- [Routes and parameters](#routes-and-parameters)
- [Route, Group, and With](#route-group-and-with)
- [Middleware](#middleware)
- [Mount](#mount)
- [Streaming](#streaming)
- [Serving](#serving)
- [Gotchas](#gotchas)
- [Checklist](#checklist)

## A whole program, before and after

**Before** (chi v5):

```go
package main

import (
	"log"
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
)

func main() {
	r := chi.NewRouter()
	r.Use(middleware.RequestID)
	r.Use(middleware.Logger)
	r.Use(middleware.Recoverer)

	r.Get("/healthz", health)

	r.Route("/users", func(r chi.Router) {
		r.Get("/", listUsers)
		r.Post("/", createUser)
		r.Route("/{id}", func(r chi.Router) {
			r.Get("/", getUser)
			r.Delete("/", deleteUser)
		})
	})

	r.Group(func(r chi.Router) {
		r.Use(requireAdmin)
		r.Get("/admin/stats", stats)
	})

	log.Fatal(http.ListenAndServe(":8080", r))
}

func getUser(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
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
	"github.com/go-chi/chi/v5/middleware" // chi's middleware package works without chi's router
)

func main() {
	app := photon.New()
	app.Use(middleware.RequestID)
	app.Use(middleware.Logger)
	// middleware.Recoverer is optional: Photon recovers panics itself.

	app.GET("/healthz", health)

	users := app.Group("/users")
	users.GET("/", listUsers) // "/" in a group means the group's own path: /users
	users.POST("/", createUser)

	user := users.Group("/:id")
	user.GET("/", getUser) // /users/:id
	user.DELETE("/", deleteUser)

	admin := app.Group("", requireAdmin)
	admin.GET("/admin/stats", stats)

	if err := app.Run(":8080"); err != nil {
		log.Fatal(err)
	}
}

func getUser(w http.ResponseWriter, r *http.Request) {
	id := photon.PathParam(r, "id")
	// ...
}
```

`health`, `listUsers`, `createUser`, `deleteUser`, `stats`, and `requireAdmin`
are untouched.

## Routes and parameters

| chi | Photon |
|---|---|
| `r.Get("/users/{id}", h)` | `app.GET("/users/:id", h)` |
| `r.Post`, `r.Put`, `r.Patch`, `r.Delete`, `r.Head`, `r.Options` | `app.POST`, `app.PUT`, `app.PATCH`, `app.DELETE`, `app.HEAD`, `app.OPTIONS` |
| `r.Method("GET", "/x", h)` | `app.Handle(http.MethodGet, "/x", h)` |
| `r.Handle("/x", h)` (any method) | `app.Handle(m, "/x", h)` for each method, or `app.Mount("/x", h)` for the subtree |
| `r.Get("/files/*", h)` | `app.GET("/files/*path", h)` (catch-alls are named) |
| `chi.URLParam(r, "id")` | `photon.PathParam(r, "id")` |
| `chi.URLParam(r, "*")` | `photon.PathParam(r, "path")` |
| `r.Get("/{id:[0-9]+}", h)` | `app.GET("/:id", h)` and check the value in the handler |
| `r.Get("/{month}-{day}-{year}", h)` | `app.GET("/:date", h)` and split in the handler |
| `r.NotFound(fn)` | `app.NotFound(http.HandlerFunc(fn))` |
| `r.MethodNotAllowed(fn)` | `app.MethodNotAllowed(http.HandlerFunc(fn))` |

Regex parameters are the biggest gap. With chi, a request that fails the regex
doesn't match the route and gets a 404. With Photon, the route matches any
segment and the handler decides:

```go
// Before
r.Get("/orders/{id:[0-9]+}", getOrder)

// After
app.GET("/orders/:id", getOrder)

func getOrder(w http.ResponseWriter, r *http.Request) {
	id, err := strconv.ParseInt(photon.PathParam(r, "id"), 10, 64)
	if err != nil {
		photon.Error(w, r, photonerr.NotFound("order")) // what the regex miss used to produce
		return
	}
	// ...
}
```

A Photon wildcard must be a whole segment, so there is no equivalent of
`/{month}-{day}-{year}`: `/:month-:day` and `/files/:name.json` are rejected at
registration.

Convert every `{...}` as you go. A pattern you forget to convert, such as
`app.GET("/users/{id}", h)`, panics at registration with a message that says
`{id}` becomes `:id`, so nothing slips through to production.

## Route, Group, and With

chi has three ways to scope routes. Photon has one, `Group`, plus per-route
middleware.

```go
// chi: r.Route mounts a sub-router under a prefix
r.Route("/v1", func(r chi.Router) {
	r.Use(requireAPIKey)
	r.Post("/chat", chat)
})

// Photon
v1 := app.Group("/v1", requireAPIKey)
v1.POST("/chat", photon.SSE(chat))
```

```go
// chi: r.Group shares middleware without a prefix
r.Group(func(r chi.Router) {
	r.Use(requireAdmin)
	r.Get("/admin/stats", stats)
})

// Photon: a group with an empty prefix
admin := app.Group("", requireAdmin)
admin.GET("/admin/stats", stats)
```

```go
// chi: r.With adds middleware to one route
r.With(rateLimit).Post("/login", login)

// Photon: per-route middleware is a trailing argument
app.POST("/login", login, rateLimit)
```

Group prefixes may contain parameters (`app.Group("/orgs/:org")`), and
`photon.PathParam(r, "org")` works in every route under it. Groups nest with
`g.Group(prefix, mw...)`.

## Middleware

Middleware is identical in type. Your own middleware, and most of chi's
`middleware` package, work as they are: `RequestID`, `Logger`, `NoCache`,
`Compress`, `Throttle`, `Heartbeat`, and so on are plain `net/http` middleware.

What changes is when it runs.

**`app.Use` covers every route, whenever it was registered.** chi panics if you
call `Use` after the first route on a mux. Photon allows it and applies the
middleware to routes registered before and after, and also to 404 and 405
responses. Your existing code, which calls `Use` first, behaves the same.

`Group.Use` is different: it applies only to routes registered on that group
after the call. That is how one group can hold a public route followed by
authenticated ones.

**Global middleware runs after routing.** chi's `r.Use` middleware runs before
the route is found, which is why `chi.URLParam` doesn't work there. In Photon,
the route is already chosen when `app.Use` middleware runs, so
`photon.PathParam` works in it. The flip side: middleware that changes the path
to influence routing has no effect on routing. That includes chi's
`StripSlashes` and `CleanPath`, which work by changing what chi routes on.

**Order** is `app.Use` middleware, then group middleware, then per-route
middleware, then the handler.

**ResponseWriter wrappers need `Unwrap`.** Photon flushes streams and sets write
deadlines through `http.ResponseController`. A wrapper without an
`Unwrap() http.ResponseWriter` method makes streams through it fail.

Notes on specific chi middleware:

| chi middleware | Under Photon |
|---|---|
| `middleware.Recoverer` | Optional. Photon recovers panics, answers 500, and calls `photon.OnPanic`. If you keep `Recoverer`, it catches panics first and `OnPanic` won't see them. |
| `middleware.RealIP` | Changes what `photon.ClientIP` returns. See [Gotchas](#gotchas). |
| `middleware.Timeout` | Keep it off stream routes; a request deadline ends a long stream. |
| `middleware.StripSlashes`, `CleanPath` | No effect on routing (they change what chi routes on). |
| `middleware.GetHead` | Relies on chi's router. Register `app.HEAD` explicitly instead. |
| `middleware.Compress` | Fine for normal routes. Keep compression off SSE routes unless you've confirmed it flushes per event. |

More in [../guides/middleware.md](../guides/middleware.md).

## Mount

Both routers have `Mount`, and they behave differently.

chi's `Mount` lets a mounted chi router route on the path relative to the mount
point, without changing `r.URL.Path`. Photon's `Mount` passes the request through
unchanged: the mounted handler sees the full path.

```go
// chi
r.Mount("/admin", adminRouter()) // adminRouter's routes are "/", "/users", ...

// Photon, keeping adminRouter as a chi router for now
app.Mount("/admin", http.StripPrefix("/admin", adminRouter()))
```

`http.StripPrefix` changes `r.URL.Path`, so handlers inside `adminRouter` that
read the path will see the shorter one. If you're porting the sub-router anyway,
make it a group instead and drop the `Mount`:

```go
admin := app.Group("/admin", requireAdmin)
admin.GET("/", adminHome)
admin.GET("/users", adminUsers)
```

`Mount` is the right tool for handlers that aren't yours to port: a file server,
an MCP server, a gRPC-gateway mux, an existing `ServeMux`. It serves every
method for the prefix and every path below it. Routes take precedence over a
mount, so `app.Mount("/", chiRouter)` keeps the whole chi app answering while
you port routes to Photon one at a time.

## Streaming

chi leaves streaming to you, so chi apps usually have a hand-written loop:

**Before**:

```go
func chat(w http.ResponseWriter, r *http.Request) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming unsupported", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	for tok := range generate(r.Context(), r.URL.Query().Get("q")) {
		fmt.Fprintf(w, "event: token\ndata: %s\n\n", tok)
		flusher.Flush()
	}
}
```

**After**:

```go
app.GET("/chat", photon.SSE(func(s *photon.Stream, r *http.Request) error {
	for tok := range generate(r.Context(), r.URL.Query().Get("q")) {
		if err := s.Event("token", []byte(tok)); err != nil {
			return err // the client is gone: stop
		}
	}
	return s.Event("done", nil)
}))
```

`photon.SSE` frames multi-line data correctly, sends each event immediately,
applies back-pressure and disconnects a stuck client after 30 seconds, caps
concurrent streams (503 with `Retry-After`), sends heartbeats after 15 seconds of
silence, turns a returned error into either an HTTP error (before the first
event) or an `event: error` (after), and tells the stream about shutdown through
`s.Done()`.

If you'd rather not change the handler's signature yet, attach
`photon.Streaming()` to the route and get the stream with `photon.StreamFrom(r)`.
See [../guides/streaming.md](../guides/streaming.md).

## Serving

chi is a router, so chi apps own their `http.Server`, timeouts, signal handling,
and shutdown. Photon replaces all of that:

```go
// Before
srv := &http.Server{Addr: ":8080", Handler: r, ReadHeaderTimeout: 5 * time.Second}
// ... ListenAndServe in a goroutine, signal.NotifyContext, srv.Shutdown(ctx) ...

// After
if err := app.Run(":8080"); err != nil {
	log.Fatal(err)
}
```

`app.Run` listens, handles SIGINT and SIGTERM, tells active streams through
`s.Done()`, and waits up to 25 seconds (`photon.ShutdownTimeout(d)` to change).
The default limits replace whatever you set on `http.Server`: 5s
`ReadHeaderTimeout`, 60s `IdleTimeout`, 16 KiB of headers, plus a 1 MiB body
limit, a 100-field header count limit, and an 8 KiB per-value limit. To change
one:

```go
l := photon.DefaultLimits()
l.MaxRequestBodyBytes = 10 << 20
app := photon.New(photon.SetLimits(l))
```

The app is an `http.Handler`, so `httptest.NewServer(app)` and
`app.ServeHTTP(rec, req)` work in tests exactly as your chi router did.

## Gotchas

- **Overlapping routes panic.** chi accepts `/users/new` beside `/users/{id}`
  and prefers the static one. Photon refuses for the same method, naming both
  call sites. Move one route under a distinct segment (`/user-forms/new`), or
  keep `/users/:id` and branch on the value. Different methods never conflict.
- **An unconverted `{id}` panics at startup.** `app.GET("/users/{id}", h)` is
  refused with a message giving the Photon spelling, `:id`.
- **No regex parameters, no multi-parameter segments.** Validate in the handler,
  and return the 404 the regex used to produce if you want the same behaviour.
- **`Mount` doesn't make paths relative.** Wrap chi sub-routers in
  `http.StripPrefix`, or port them to groups.
- **Trailing slashes.** `r.Route("/users", ...)` with `r.Get("/", h)` answers
  both `/users` and `/users/` in chi. Photon answers only `/users`. Patterns
  ending in `/` are rejected at registration, and there is no redirect.
- **`chi.URLParam` returns `""` under Photon routes**, because chi's routing
  context isn't there. Search for every call. A chi router mounted inside Photon
  still fills in its own parameters.
- **`PathParam` is valid only during the handler.** `chi.URLParam` returned a
  string you could keep. `photon.PathParam` aliases request memory. Use
  `photon.CopyParam` when the value goes to a goroutine, a channel, a cache, or
  an async logger. The race detector won't flag this.
- **`middleware.RealIP` and `photon.ClientIP`.** `photon.ClientIP` reads
  `r.RemoteAddr` and deliberately ignores forwarded headers. chi's `RealIP`
  overwrites `r.RemoteAddr` with a value from request headers, so after it,
  `ClientIP` no longer reports the socket peer, and it may return an invalid
  `netip.Addr` because `RealIP` stores the address without a port. Only use
  `RealIP` behind a proxy that overwrites those headers, and check
  `photon.ClientIP(r).IsValid()`.
- **The route pattern is `r.Pattern`.** Where chi has
  `chi.RouteContext(ctx).RoutePattern()`, Photon sets the standard library's
  `r.Pattern` to the matched template (`/users/:id`), visible to the handler and
  to all middleware, which runs after routing. Metrics labelled by route read it
  directly; it is empty on a 404.
- **No `chi.Walk`-style introspection of middleware.** To check that a route is
  protected, make a request in a test and assert it isn't answered with 200.
- **Routes are fixed once the server starts.** Registering after `Run` panics.
- **Body limit.** Every request, including mounted handlers, is limited to 1 MiB
  by default. chi applied no limit.

## Checklist

- [ ] Replace `chi.NewRouter()` and your `http.Server` with `photon.New()`
- [ ] `r.Get("/x/{id}", h)` to `app.GET("/x/:id", h)`, and so on for each method
- [ ] Search route registrations for `{` to catch patterns you missed (they register as literals)
- [ ] Name every catch-all (`/*` to `/*path`)
- [ ] Replace regex parameters with handler-side checks
- [ ] `chi.URLParam` to `photon.PathParam`; `photon.CopyParam` where values outlive the handler
- [ ] `r.Route` and `r.Group` to `app.Group`; `r.With(mw)` to a per-route middleware argument
- [ ] Wrap mounted chi sub-routers in `http.StripPrefix`, or port them to groups
- [ ] Review `RealIP`, `Timeout`, `StripSlashes`, `CleanPath`, `GetHead`, and `Recoverer`
- [ ] Add `Unwrap()` to every `ResponseWriter` wrapper
- [ ] Build the app in a test and fix route-conflict panics
- [ ] Replace flusher loops with `photon.SSE`
- [ ] Replace server setup and shutdown with `app.Run`; check the default limits
