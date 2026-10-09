# Migrating to Photon

These guides cover moving an existing HTTP service to Photon. Each one starts
from a specific framework, shows before-and-after code for every concept, and
lists the places where Photon behaves differently enough to bite.

| Coming from | Guide | Size of the job |
|---|---|---|
| `net/http` (including Go 1.22 `ServeMux` patterns) | [from-net-http.md](from-net-http.md) | Small. Handlers and middleware stay as they are. |
| chi v5 | [from-chi.md](from-chi.md) | Small. chi is `net/http`-native too. |
| gin v1 | [from-gin.md](from-gin.md) | Medium. Every handler changes signature. |
| echo v4 | [from-echo.md](from-echo.md) | Medium. Every handler changes signature. |
| Fiber v2 | [from-fiber.md](from-fiber.md) | Larger. You are moving from fasthttp to `net/http`. |
| FastAPI, Express, Fastify | [from-fastapi-and-express.md](from-fastapi-and-express.md) | A rewrite. The guide maps the concepts and walks through one full endpoint. |

## What Photon is

Photon is the streaming-first Go server for AI agents, built on plain
`net/http`. It sends server-sent events token by token, bounds the memory a slow
or malicious client can cost, and shuts down without cutting answers in half.
Secure limits are on by default, and it has no dependencies outside the standard
library.

Handlers are ordinary `net/http` handlers and middleware is
`func(http.Handler) http.Handler`, so existing `net/http` middleware works
unchanged.

Photon is not trying to beat gin, echo, or Fiber on requests per second.
End-to-end throughput is on par with `net/http`, and the router is a few percent
of request CPU. If raw throughput is your reason for choosing a framework, Photon
is the wrong reason to move.

## Should you migrate?

**Probably yes, if:**

- You stream model output over SSE and have written your own flusher loops,
  disconnect checks, heartbeats, and shutdown handling. That code is what
  `photon.SSE` replaces, and it is easy to get subtly wrong.
- You want request limits (body size, header count and size, slowloris timeouts,
  concurrent streams) on by default rather than remembered per service.
- You're on `net/http` or chi already. The move is mostly mechanical and you can
  do it one route at a time.

**Probably not, if:**

- Nothing in your service streams and your current stack works. Most of what
  Photon would give you is its default limits, and you can set many of those on
  an `http.Server` yourself.
- You chose Fiber or another fasthttp-based stack for throughput. Photon is
  `net/http` underneath and will not be faster.
- You depend on things Photon leaves out on purpose:
  - **WebSockets.** Use [`github.com/coder/websocket`](https://github.com/coder/websocket)
    alongside Photon. It works on any `http.Handler`, so it mounts fine, but if
    WebSockets are most of your app, Photon adds little.
  - **Templates.** Use `html/template` directly with `photon.HTML` or `w.Write`.
  - **Regex routes and optional segments.** Photon has `:param` and `*catchall`,
    nothing else.
  - **Request binding with validation tags.** `photon.DecodeJSON` decodes; you
    validate explicitly.
  - **Automatic trailing-slash redirects.** `/users/` is a 404 unless you add a
    redirect yourself.
- You have hundreds of gin or echo handlers that lean on binding and validation
  tags, and none of them stream. The rewrite is real work and the payoff is small.

You don't have to move everything at once. A Photon app is an `http.Handler` and
can mount any `http.Handler`, so a common path is to move the streaming endpoints
first and fall back to the old router for the rest. See
[Migrating one piece at a time](from-net-http.md#migrating-one-piece-at-a-time).

## Concept map

| | `net/http` ServeMux | gin v1 | echo v4 | chi v5 | Fiber v2 | Photon |
|---|---|---|---|---|---|---|
| Built on | `net/http` | `net/http` | `net/http` | `net/http` | fasthttp | `net/http` |
| Register a route | `mux.HandleFunc("GET /u/{id}", h)` | `r.GET("/u/:id", h)` | `e.GET("/u/:id", h)` | `r.Get("/u/{id}", h)` | `app.Get("/u/:id", h)` | `app.GET("/u/:id", h)` |
| Catch-all | `{path...}` | `*path` | `*` | `*` | `*` | `*path` |
| Handler | `func(w, r)` | `func(c *gin.Context)` | `func(c echo.Context) error` | `func(w, r)` | `func(c *fiber.Ctx) error` | `func(w, r)` |
| Middleware | `func(http.Handler) http.Handler` by convention | `gin.HandlerFunc` calling `c.Next()` | `echo.MiddlewareFunc` | `func(http.Handler) http.Handler` | `fiber.Handler` calling `c.Next()` | `func(http.Handler) http.Handler` |
| Path param | `r.PathValue("id")` | `c.Param("id")` | `c.Param("id")` | `chi.URLParam(r, "id")` | `c.Params("id")` | `photon.PathParam(r, "id")` |
| JSON in | `json.NewDecoder(r.Body).Decode(&v)` | `c.ShouldBindJSON(&v)` | `c.Bind(&v)` | `encoding/json` or go-chi/render | `c.BodyParser(&v)` | `photon.DecodeJSON(r, &v)` |
| JSON out | `json.NewEncoder(w).Encode(v)` | `c.JSON(200, v)` | `return c.JSON(200, v)` | `encoding/json` or go-chi/render | `return c.JSON(v)` | `photon.JSON(w, 200, v)` |
| Errors | `http.Error(w, msg, code)` | `c.AbortWithStatusJSON(code, obj)` | `return echo.NewHTTPError(code, msg)` | `http.Error(w, msg, code)` | `return fiber.NewError(code, msg)` | `photon.Error(w, r, photonerr.BadRequest(msg))` |
| Groups | sub-mux plus `http.StripPrefix` | `r.Group("/v1")` | `e.Group("/v1")` | `r.Route("/v1", fn)` | `app.Group("/v1")` | `app.Group("/v1", mw...)` |
| Streaming | your own `http.Flusher` loop | `c.Stream` and `c.SSEvent` | write and `c.Response().Flush()` | your own `http.Flusher` loop | `SetBodyStreamWriter` with a `*bufio.Writer` | `photon.SSE(func(s, r) error)` |
| Graceful shutdown | `srv.Shutdown(ctx)` plus your own signal handling | same as `net/http` | `e.Shutdown(ctx)` plus your own signal handling | same as `net/http` | `app.Shutdown()` plus your own signal handling | `app.Run(addr)` does it |

Photon's pattern rules, briefly: `:name` matches exactly one segment, `*name`
matches the rest of the path (at least one character) and must be last. Two
routes for the same method that could match the same request panic at
registration. That includes a static segment beside a parameter, such as
`/users/new` beside `/users/:id`. A `{id}` left over from ServeMux, chi, or
FastAPI is refused at registration with a message naming the Photon spelling,
so a missed conversion fails at startup rather than never matching. Details in [../guides/routing.md](../guides/routing.md).

## What your clients will notice

Most of a migration is invisible to clients. These parts are not:

- **Error bodies change shape.** `photon.Error` writes RFC 9457
  `application/problem+json`, with `title`, `status`, `code`, and a `type` URI.
  Clients that parse `{"error": "..."}`, `{"message": "..."}`, or FastAPI's
  `{"detail": "..."}` need updating. You can keep your old shape by writing it
  yourself with `photon.JSON`.
- **415 for a JSON endpoint called with a non-JSON `Content-Type`.** A missing
  `Content-Type` is accepted. A client sending JSON as `text/plain` is not.
- **413 for request bodies over 1 MiB**, and **431** for headers over 16 KiB in
  total, over 8 KiB in one value, or more than 100 header fields.
- **404, not a redirect, for a trailing slash.** `/users/` does not match
  `/users`.
- **405 with an `Allow` header** when the path exists for another method.
  `GET` routes do not answer `HEAD` unless you register `HEAD` too.
- **503 with `Retry-After`** when the server is at its concurrent stream limit.
- **Streams:** an error after the first event arrives as an `event: error` with a
  problem document. SSE comment lines (heartbeats) appear after 15 seconds of
  silence. Clients ignore comments, but a hand-written parser might not.

## Generic migration checklist

1. **Routes.** Translate patterns to `:param` and `*catchall`, and search for
   leftover `{`. Build the app in a test (`photon.New()` plus your route
   registration) so conflicts panic in CI, not in production. Decide what to do
   about trailing slashes.
2. **Handlers.** Convert to `func(w http.ResponseWriter, r *http.Request)`. Use
   `photon.PathParam`, `photon.DecodeJSON`, `photon.JSON`, and `photon.Error`.
   Replace binding tags with explicit checks. Use `photon.CopyParam` for any
   parameter that outlives the handler.
3. **Middleware.** Rewrite framework middleware as
   `func(http.Handler) http.Handler`, or swap in an existing `net/http` package.
   Any wrapper around `http.ResponseWriter` needs an `Unwrap() http.ResponseWriter`
   method, or streaming through it fails.
4. **Streaming.** Replace flusher loops, `c.Stream`, and stream writers with
   `photon.SSE`. Check the error from every `s.Event` and stop when it is non-nil.
5. **Server.** Replace startup and shutdown code with `app.Run`. Compare the
   default limits against real traffic (upload sizes, large prompts, big cookies
   or tokens, concurrent streams) and raise the ones you need. Tell client teams
   about the error format.

## Further reading

- [../getting-started.md](../getting-started.md): a first Photon app from scratch
- [../guides/streaming.md](../guides/streaming.md): SSE, back-pressure, heartbeats, shutdown
- [../guides/routing.md](../guides/routing.md): patterns, conflicts, groups, Mount
- [../guides/middleware.md](../guides/middleware.md): ordering, groups, wrapping `ResponseWriter`
- [../guides/security.md](../guides/security.md): the default limits and why they are set where they are
- [../API.md](../API.md): every exported symbol
