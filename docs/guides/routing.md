# Routing

Photon's router matches a method and a path to an ordinary `http.Handler`. It
supports static segments, `:param` segments, and a trailing `*catchall`, and
it refuses at startup any pair of routes that could compete for the same
request. This guide covers the pattern syntax, the conflict rules and why they
exist, groups, mounting existing handlers, and the handful of behaviours that
differ from `net/http`'s `ServeMux`.

- [Registering routes](#registering-routes)
- [Patterns](#patterns)
- [Reading path parameters](#reading-path-parameters)
- [Encoded slashes and raw-path routing](#encoded-slashes-and-raw-path-routing)
- [Conflicting routes panic at startup](#conflicting-routes-panic-at-startup)
- [Trailing slashes](#trailing-slashes)
- [HEAD and OPTIONS](#head-and-options)
- [404 and 405](#404-and-405)
- [Groups](#groups)
- [Mounting an existing handler](#mounting-an-existing-handler)
- [Keeping parameters: PathParam and CopyParam](#keeping-parameters-pathparam-and-copyparam)
- [Listing routes](#listing-routes)
- [The router is frozen once serving starts](#the-router-is-frozen-once-serving-starts)
- [Performance](#performance)

## Registering routes

```go
package main

import (
	"log"
	"net/http"

	"github.com/agenticmarket/photon"
)

func main() {
	app := photon.New()

	app.GET("/healthz", health)          // static
	app.GET("/users/:id", getUser)       // one path segment
	app.GET("/files/*path", getFile)     // the rest of the path
	app.POST("/users", createUser)

	if err := app.Run(":8080"); err != nil {
		log.Fatal(err)
	}
}

func health(w http.ResponseWriter, r *http.Request) {
	_ = photon.Text(w, http.StatusOK, "ok\n")
}

func getUser(w http.ResponseWriter, r *http.Request) {
	_ = photon.JSON(w, http.StatusOK, map[string]string{"id": photon.PathParam(r, "id")})
}

func getFile(w http.ResponseWriter, r *http.Request) {
	_ = photon.JSON(w, http.StatusOK, map[string]string{"path": photon.PathParam(r, "path")})
}

func createUser(w http.ResponseWriter, r *http.Request) {
	_ = photon.NoContent(w)
}
```

There is one method per verb: `GET`, `POST`, `PUT`, `PATCH`, `DELETE`, `HEAD`,
and `OPTIONS`. Each takes an `http.HandlerFunc`, so a plain function or a
function literal works directly. To register an `http.Handler` value (a mux, a
struct with `ServeHTTP`) or a method without a helper, use `Handle`:

```go
app.Handle("PROPFIND", "/dav/*path", davHandler)
```

Every registration method also takes optional per-route middleware as trailing
arguments. See the [middleware guide](middleware.md).

## Patterns

A pattern is a sequence of `/`-separated segments. Each segment is one of:

| Segment | Matches | Captured value |
|---|---|---|
| `users` | exactly the bytes `users` (case-sensitive) | nothing |
| `:id` | exactly one non-empty segment | the segment, without slashes |
| `*path` | one or more characters to the end of the path, slashes included | everything after the preceding `/` |

Some concrete cases:

| Pattern | Matches | Does not match |
|---|---|---|
| `/users/:id` | `/users/42`, `/users/new` | `/users`, `/users/`, `/users/42/posts` |
| `/files/*path` | `/files/a.txt` (`path` = `a.txt`), `/files/a/b.txt` (`path` = `a/b.txt`) | `/files`, `/files/`, `/files/a/` |
| `/files` | `/files` | `/files/` |
| `/` | `/` | anything else |

The rules a pattern must follow. Breaking any of them panics at registration,
with a message that names the problem:

- It starts with `/`. Only the root pattern may be a single `/`.
- It does not end with `/` (see [Trailing slashes](#trailing-slashes)).
- It contains no empty segment (`//`), no `?`, and no `#`.
- It contains no `{` or `}`. A pattern copied from `ServeMux` or chi, such as
  `/users/{id}`, would otherwise register as literal text and never match, so
  it is refused with a message that gives the photon spelling: `{id}` becomes
  `:id`, and `{path...}` becomes `*path`.
- A wildcard occupies a whole segment. `/users/:id.json` is rejected; register
  `/users/:id` and parse the suffix in the handler.
- A wildcard has a name made of letters, digits, and underscores, not starting
  with a digit. Use distinct names within one pattern.
- There is at most one catch-all, and it is the last segment.
- A pattern has at most 16 segments.

Two matching rules are worth knowing because `ServeMux` behaves differently:

- **An empty segment never matches.** A request for `/users/` or `/a//b` is a
  404, even under a catch-all: `/files/a/` does not match `/files/*path`.
- **A request path deeper than 16 segments never matches**, including under a
  catch-all. This bounds the router's per-request state to a fixed-size array.

## Reading path parameters

```go
func getPost(w http.ResponseWriter, r *http.Request) {
	userID := photon.PathParam(r, "id")     // "" if the route has no such parameter
	postID := r.PathValue("postID")         // the same value, through net/http
	_ = photon.JSON(w, http.StatusOK, map[string]string{
		"user": userID, "post": postID, "route": r.Pattern,
	})
}

// registered as: app.GET("/users/:id/posts/:postID", getPost)
```

Photon stores captures with the standard library's `Request.SetPathValue`, so
`r.PathValue` works and a handler written for `ServeMux` runs unchanged. It also
sets `r.Pattern` to the matched route template (`/users/:id/posts/:postID`, or
the path itself for a static route), which gives logs and metrics a
low-cardinality route label.

`photon.Params(r)` returns all captures in pattern order. `CopyParam` and
`CopyParams` return copies you can keep; see
[Keeping parameters](#keeping-parameters-pathparam-and-copyparam).

## Encoded slashes and raw-path routing

Photon routes on the raw, still-encoded path when the request has one. A request
for `/admin%2Fsettings` is one segment, `admin%2Fsettings`. It does not match
`/admin/:page`.

The alternative, routing on the decoded path, would let `%2F` act as a segment
separator. That is a routing bypass: a proxy or an authorization rule that
inspects the raw path sees one thing, and the router dispatches another.

One consequence follows for parameter values. Go keeps a separate raw path only
when the decoded path cannot be re-encoded to the original, which happens for
escapes such as `%2F`. For such a request the captured value is still escaped
(`/files/a%2Fb` gives `path` = `a%2Fb`). For an ordinary request, such as
`/users/caf%C3%A9`, the value is decoded (`café`). If your handler needs the
decoded form, call `url.PathUnescape` on the value and decide what a decoded
`/` means for you.

Photon also does not clean paths. `/files/../secret` reaches `/files/*path`
with `path` = `../secret`. Never join a catch-all value onto a filesystem path
without validating it; `os.Root` (Go 1.24) and `fs.ValidPath` exist for this.

## Conflicting routes panic at startup

If two routes for the same method could match the same request, the second
registration panics. Photon does not pick a winner by registration order or by
"most specific", because either rule turns a typo into a request-dependent
choice. Consider an admin page protected by per-route auth, next to a public
catch-all:

```go
app.GET("/admin/settings", settings, requireAdmin)
app.GET("/admin/*rest", publicAdminAssets)   // panics at startup
```

With a precedence rule, which handler serves `/admin/settings` depends on the
rule and on spelling. Misspell the static route and every request for it goes
to the public handler, with no error anywhere. A panic at startup is a bug you
find in your first test run; a silent choice is an authorization bug you find
in production.

The rules, with examples. Conflicts are checked per method, so
`GET /users/:id` and `POST /users/new` coexist.

| First route | Second route | Result | Why |
|---|---|---|---|
| `/users/new` | `/users/:id` | panic | A literal and a wildcard at the same position. `:id` would also match `new`. |
| `/admin/settings` | `/admin/*rest` | panic | The catch-all also matches the literal. |
| `/a/*rest` | `/a/:id` | panic | The catch-all also matches anything `:id` matches. |
| `/users/:id` | `/users/:name` | panic | Same shape. Which name holds the value would depend on registration order. |
| `/users/:id/posts` | `/users/new/comments` | panic | A literal and a wildcard at position 2 conflict regardless of what follows. |
| `/files` | `/files/*path` | allowed | A catch-all needs at least one more character, so `/files` matches only the first. |
| `/a` | `/a/b` | allowed | Different segment counts, no catch-all. |
| `/a/:x/c` | `/a/:y/d` | allowed | Wildcards in the same position, then different literals. |
| `/` | anything | allowed | The root matches only `/`. |

Registering the identical pattern twice for one method also panics.

The panic message names both registration sites, so you can go straight to the
two lines:

```text
photon: conflicting routes for "GET" "/users/:id"
  new:       api/routes.go:27
  existing:  api/routes.go:26
  reason:    the wildcard in one pattern matches the literal segment "new" in the other, so "/users/new" would be served by whichever was registered first
  existing pattern: "/users/new"
  new pattern:      "/users/:id"
```

The usual fixes for `/users/new` beside `/users/:id`: handle the special value
inside the `:id` handler, use a different method (`POST /users` to create), or
give the special route a different shape (`/users-new`, `/new/user`).

Because conflicts panic during registration, constructing your app in a test is
enough to check the whole route table. See the [testing guide](testing.md#testing-that-routes-do-not-conflict).

## Trailing slashes

`/users/` cannot be registered. The panic suggests the fix:

```text
photon: malformed route pattern: the route pattern "/users/" ends with a slash: register "/users" instead
```

A request with a trailing slash is a 404; photon does not redirect it.
Accepting `/users` and `/users/` as synonyms would make them ambiguous, and a
redirect is a policy decision for your API rather than for the router. If you
have clients that add slashes, fix the clients, or normalise at your proxy.

## HEAD and OPTIONS

`HEAD` is not derived from `GET`. A `HEAD` request to a route that registered
only `GET` gets a 405 with `Allow: GET`. Register `HEAD` explicitly where you
want it, usually with the same handler:

```go
app.GET("/healthz", health)
app.HEAD("/healthz", health) // net/http discards the body of a HEAD response
```

Do not register `HEAD` on a streaming route.

`OPTIONS` is not answered automatically either. You rarely need an `OPTIONS`
route: CORS preflights are best answered by global middleware, which runs on
405 responses too. See [CORS](middleware.md#cors).

## 404 and 405

When nothing matches, photon answers:

- **405 Method Not Allowed** if the path matches a route under another method.
  The `Allow` header lists those methods, sorted and comma-separated
  (`Allow: DELETE, GET`).
- **404 Not Found** otherwise.

The bodies are fixed strings (`404 not found`, `405 method not allowed`) with
`Content-Type: text/plain; charset=utf-8` and `X-Content-Type-Options: nosniff`.
They never include the requested path. A reflected path is a free oracle for
route enumeration, and an XSS vector if a browser can be persuaded to sniff the
response as HTML.

To replace them, set handlers before serving:

```go
app.NotFound(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
	photon.Error(w, r, photonerr.NotFound("route")) // {"title":"route not found",...}
}))

app.MethodNotAllowed(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
	// The Allow header is already set when this runs.
	photon.Error(w, r, photonerr.NotAllowed())
}))
```

(`photonerr` is `github.com/agenticmarket/photon/photonerr`.) Global
middleware registered with `Use` runs around both handlers. Keep the request
path out of custom bodies for the reasons above.

## Groups

A group shares a path prefix and middleware:

```go
api := app.Group("/v1", requireAPIKey)
api.GET("/models", listModels)                    // GET /v1/models
api.POST("/chat/completions", photon.SSE(chat))   // POST /v1/chat/completions

orgs := api.Group("/orgs/:org")                   // nested: /v1/orgs/:org
orgs.GET("/", getOrg)                             // "/" means the prefix itself
orgs.GET("/members/:user", getMember)             // PathParam(r, "org") works here
```

- The prefix must start with `/` and must not end with one. `""` and `"/"` both
  mean no prefix.
- The prefix may contain parameters, and every route under it can read them.
- Route paths inside a group start with `/`. The prefix and path are joined as
  strings, so `api.GET("models", h)` registers `/v1models`.
- Nested groups join their prefixes. The parent's middleware runs outside the
  child's.
- Group middleware runs inside global `Use` middleware and outside per-route
  middleware.

`Group.Use` adds middleware to routes registered on the group **after** the
call. That is deliberate: a group is a registration scope, so a public route
can come before the authenticated ones.

```go
auth := app.Group("/auth")
auth.POST("/login", login)      // public
auth.Use(requireSession)
auth.POST("/logout", logout)    // requires a session
auth.GET("/me", me)             // requires a session
```

A group's middleware is copied into each route and each child group when they
are created, so a later `Use` on a parent does not reach a child that already
exists. This is different from `app.Use`, which applies to every route no
matter when it was registered.

## Mounting an existing handler

`Mount` serves an `http.Handler` for a prefix and everything below it. It is
how an MCP server, a file server, a gRPC-gateway mux, or another photon app
joins your server:

```go
app.Mount("/mcp", mcpHandler)
app.Mount("/static", http.StripPrefix("/static", http.FileServerFS(assets)))
```

What it does:

- Serves **every method** for the prefix and **every path below it**:
  trailing slashes, empty segments, any depth. `http.FileServer` directory
  listings and the `net/http/pprof` index at `/debug/pprof/` work.
- Passes the path through unchanged. The handler sees `/static/app.js`, not
  `/app.js`. Wrap it in `http.StripPrefix` if it expects relative paths.
- Sets `r.Pattern` to the prefix followed by `/*` (`/mcp/*`), so metrics label
  mounted traffic by mount point.
- Runs global middleware. `Group.Mount` also applies the group's middleware.
- Requires a static prefix. A prefix is matched by whole segments: a mount at
  `/mcp` serves `/mcp` and `/mcp/x`, never `/mcpx`.

**Routes take precedence over mounts**, and a longer mount prefix over a
shorter one. That is the one deliberate exception to photon's no-ambiguity
rule, because a mount is a fallback by definition, and which side serves a
request depends on what is registered, never on registration order:

```go
app.Mount("/", legacyMux)       // everything not yet ported
app.GET("/users/:id", getUser)  // ported: photon serves it
app.POST("/chat", photon.SSE(chat))
```

That is the incremental migration path: wrap the old router, then move routes
over one at a time. Requests the new routes do not match - including other
methods on the same path - fall through to the old router.

Serve pprof on a separate, localhost-only listener even though it can be
mounted; it should never be on a public port.

## Keeping parameters: PathParam and CopyParam

`PathParam` returns a string that aliases the request path rather than a copy.
Photon's documented contract is that it is valid for the duration of the
handler call. If a value has to outlive the handler, use `CopyParam` (or
`CopyParams`), which returns an independent copy.

Under that contract this is a bug:

```go
func getUser(w http.ResponseWriter, r *http.Request) {
	id := photon.PathParam(r, "id")
	go audit.Record(id) // runs after the handler returns: the value outlives its request
	...
}
```

and this is the fix:

```go
	id := photon.CopyParam(r, "id")
	go audit.Record(id)
```

The same applies to storing a parameter in a struct, a cache, or a channel, or
handing it to an asynchronous logger. Breaking the contract cannot be detected
for you: the race detector reports unsynchronised access, and nothing about
retaining a string is unsynchronised. Code that relies on today's
implementation keeping the bytes intact is relying on something photon does
not promise, so copy whenever a value leaves the handler.

`CopyParam` allocates. Use it only when the value really escapes; reading a
parameter inside the handler needs no copy.

## Listing routes

`Routes` returns every registered route in registration order:

```go
for _, rt := range app.Routes() {
	fmt.Printf("%-7s %-30s %v\n", rt.Method, rt.Path, rt.Params)
}
```

```text
GET     /healthz                       []
GET     /users/:id                     [id]
POST    /users                         []
```

Each entry is a `RouteInfo{Method, Path, Params}`; the slice is a copy. Use it
to generate documentation, to pre-register metric labels, or to snapshot the
route table in a test. A `Mount` appears as one entry with method `*` and path
`prefix/*`. `RouteInfo` does not describe
middleware, so it cannot tell you whether a route is protected; test that with
a request (see [testing middleware wiring](testing.md#testing-middleware-wiring)).

`Router.Lookup(method, path)` resolves a path without serving it, for tooling.

## The router is frozen once serving starts

`Run`, `Listen`, `ListenTLS`, and `Serve` freeze the router before the first
request. After that, `GET` and the other registration methods, `Use`,
`NotFound`, and `MethodNotAllowed` panic:

```text
photon: the router is frozen because it has started serving, so routes cannot be registered: register every route during setup, before Listen, Run, or Serve
```

A frozen route table is immutable, so lookups take no lock. Registering while
serving would be a data race on a live server, which is why it panics instead.

Calling `app.ServeHTTP` directly, as tests do, does not freeze the router.

The router is also available on its own: `photon.NewRouter()` returns an
`http.Handler` with the same routing, groups, and middleware, without the
server's limits or lifecycle. `app.Router()` returns the one inside a server.

## Performance

- A static route is one map lookup and allocates nothing.
- A parameterised route costs at most two allocations per request, for storing
  the captured values on the request.
- Static routes never enter the matching tree, so registering thousands of them
  does not slow wildcard lookups, and registering N static routes takes time
  linear in N. Each wildcard route is compared against the routes already
  registered for its method.

## See also

- [Middleware](middleware.md): `Use`, groups, and per-route middleware
- [Security](security.md): what raw-path routing and conflict detection defend against
- [Testing](testing.md): checking the route table and 404/405 behaviour in tests
- [Streaming](../guides/streaming.md): `photon.SSE` routes
- [Getting started](../getting-started.md)
- [Migrating from another router](../migration/README.md)
