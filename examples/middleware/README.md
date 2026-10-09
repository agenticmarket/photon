# `middleware` — composing middleware

Middleware in photon is `net/http` middleware. That is the whole point of this
example: every function here would work unchanged under `net/http`, `chi`, `gin`,
or anything else.

```bash
go run ./examples/middleware
```

## The routes

```bash
# Public: no token needed
curl http://localhost:8080/public
# {"route":"public","authenticated":false}

# Private: 401 without a token
curl -i http://localhost:8080/private
# HTTP/1.1 401 Unauthorized
# WWW-Authenticate: Bearer realm="photon"

curl -H "Authorization: Bearer secret" http://localhost:8080/private
# {"route":"private","secret":"you are authenticated"}

curl -H "Authorization: Bearer wrong" http://localhost:8080/private
# 401  (deliberately the same message as a missing token)

# Admin: auth, then a role check
curl -X POST -H "Authorization: Bearer secret" http://localhost:8080/admin
# {"route":"admin","action":"accepted","payload":"would be decoded here"}

# CORS: an allowlist, not a reflection
curl -i -H "Origin: https://app.example" http://localhost:8080/public
# 200, Access-Control-Allow-Origin: https://app.example, Vary: Origin

curl -i -H "Origin: https://evil.example" http://localhost:8080/public
# 403 {"error":"origin not allowed","origin":"https://evil.example"}
```

## The decision this example is actually about

**Authentication is registered per route, not globally.**

```go
app.Use(requestLog, cors, rateLimit(20, time.Minute))

app.GET("/public",  photon.HandlerFunc(public))
app.GET("/private", photon.HandlerFunc(private), requireBearer)
app.POST("/admin",  photon.HandlerFunc(admin), requireBearer, requireRole("admin"))
```

The obvious alternative — global `requireBearer` plus a per-route exemption for
public routes — **is not expressible**, in photon or in `net/http`. Middleware
that has run cannot be un-run. The usual workarounds are both worse:

| Workaround | Why it is worse |
|---|---|
| A context flag `requireBearer` checks | A hidden second source of truth for "which routes are public" |
| A second `Router` for public routes | A policy added to one is silently missing from the other |

Attaching auth per route puts the policy exactly where it applies. A public route
is public because it is conspicuously absent from a list, rather than because a
boolean somewhere says so.

## Order is correctness, not style

```go
app.Use(requestLog)   // 1. outermost
app.Use(cors)         // 2. before auth
app.Use(rateLimit(...))
```

Global middleware wraps per-route middleware, so this order is what runs:

```
requestLog → cors → rateLimit → [ requireBearer → requireRole ] → handler
```

**CORS must come before auth.** A browser sends a preflight *without*
credentials. If auth ran first, the preflight would get a 401 with no
`Access-Control-*` headers, and the browser would report a CORS error instead of
the real problem — which is a genuinely miserable bug to debug, because the
message points at the wrong layer.

`TestCORSOrderingIsLoadBearing` asserts this by building the same routes in both
orders and checking that the order alone changes the answer: **cors-first → 204,
auth-first → 401.** A test that passed under both orderings would not be testing
the thing it names.

Photon's ordering is not accidental: global middleware runs *outside*
per-route middleware, so a route's own middleware always sees a request the
global chain has already decorated, regardless of registration order.

### Preflights need no OPTIONS routes

Global middleware (`Use`) runs for **every** request the server answers,
including a 404, a 405, and an `OPTIONS` preflight to a path that only
registered `GET` or `POST`. So `cors` answers preflights itself, and this
example registers no `OPTIONS` routes.

That was not always true. Photon used to match the route first, so a preflight
to `/private` got a 405 from the router and `cors` never ran - the browser
reported a CORS failure on a server whose CORS code was correct. The example
used to work around it by registering `OPTIONS` routes by hand.
`TestCORSPreflightNeedsNoCredentials` now covers the preflight with no such
route.

### `Use` covers routes registered before it

`Use` applies to every route, whether it was registered before or after the
call:

```go
app.GET("/admin", handler)
app.Use(requireAdmin) // wraps /admin too
```

This is measured, not assumed - `TestLateMiddlewareBehaviour` in
`bench/baseline/middleware_order_test.go` asserts it for three frameworks on
every run:

| framework | middleware added after a route |
|---|---|
| **photon** | **applied** to the route |
| gin | silently ignored |
| chi | panics: `all middlewares must be defined before routes on a mux` |

Photon used to behave like gin here - a security check registered below its
routes quietly did not run - and the guides said so. It was fixed rather than
documented.

Even so, **prove a security boundary with a request, not by reading the
code.** One request per protected route is the check that works, because it
asserts the only property that matters - the middleware actually ran:

```go
func TestAdminRequiresAuth(t *testing.T) {
	app := buildApp()
	rec := httptest.NewRecorder()
	app.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/admin", nil))
	if rec.Code == http.StatusOK {
		t.Fatal("/admin answered 200 without auth: the middleware is not wired to it")
	}
}
```

The tests in [`middleware_order_test.go`](middleware_order_test.go) do exactly
this for every route.

## Things this example gets right that examples usually get wrong

**CORS is an allowlist.** Echoing back whatever `Origin` arrived, with
credentials allowed, is the same as having no CORS policy at all. A denied origin
gets a 403 rather than a silently-missing header, because a browser needs to be
told and a bare missing header produces a confusing console error.

**`Vary: Origin` is set.** Without it a shared cache serves one origin's
response to another.

**Token comparison is constant-time.** `crypto/subtle`, not `==`. A byte-by-byte
comparison returns on the first difference, so its timing reveals how many
leading bytes matched — enough to recover a token one byte at a time.

**Failed auth gives one message.** "Missing token" and "wrong token" get the same
status and body; distinguishing them confirms which tokens exist.

**Rate limits key on the peer address, not a header.** `photon.ClientIP` ignores
`X-Forwarded-For` deliberately — it is set by the client, so keying a rate limit
on it means anyone can bypass the limit by inventing an address.

**Auth state lives in the request context**, never in a field on the server. A
field would be shared by every concurrent request.

**`requireRole` fails closed.** If auth did not run, it returns 401 rather than
panicking. A per-route check that assumed a global one had run is one reordering
away from a security hole.

## Rate limiting vs photon's built-in limits

These are different axes, not competing options:

| Limit | Bounds |
|---|---|
| `Limits.MaxConnections` | Total accepted sockets |
| `Limits.MaxPerIP` | Concurrent connections from one address |
| `rateLimit` (this example) | Requests per unit time |

A client that opens one connection and sends 10,000 sequential requests never
trips a connection limit. A rate limit is a separate concern, not a stricter
version of the same one.

## See also

- `examples/security` — the limits photon enforces for you, and the trusted-proxy
  question this example punts on
- `examples/production` — where middleware fits in a real deployment
