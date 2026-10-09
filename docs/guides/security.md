# Security

Photon handles the HTTP-shaped attacks for you, with limits that are on from
the first `photon.New()`: slow clients, header floods, oversized bodies, route
ambiguity, reflected error content, SSE injection, and unbounded stream
memory. It does not know who your users are, what they may do, or what your
model is allowed to touch. This guide covers both halves: what is on by
default and the attack each default stops, then what remains your job.

- [On by default](#on-by-default)
  - [Slow clients](#slow-clients)
  - [Header floods](#header-floods)
  - [Oversized bodies](#oversized-bodies)
  - [Error responses never reflect the request](#error-responses-never-reflect-the-request)
  - [The client address is the socket peer](#the-client-address-is-the-socket-peer)
  - [Routing cannot be bypassed with encodings or ambiguity](#routing-cannot-be-bypassed-with-encodings-or-ambiguity)
  - [Panics do not leak](#panics-do-not-leak)
  - [TLS defaults in ListenTLS](#tls-defaults-in-listentls)
  - [DecodeJSON refuses non-JSON bodies](#decodejson-refuses-non-json-bodies)
  - [SSE field injection is impossible](#sse-field-injection-is-impossible)
  - [Stream memory is bounded](#stream-memory-is-bounded)
  - [Off by default: connection and concurrency caps](#off-by-default-connection-and-concurrency-caps)
- [What is your job](#what-is-your-job)
  - [Authentication and authorization](#authentication-and-authorization)
  - [CORS, CSRF, and browser-facing responses](#cors-csrf-and-browser-facing-responses)
  - [Rate limits and quotas](#rate-limits-and-quotas)
  - [Trusted proxies and the client address](#trusted-proxies-and-the-client-address)
  - [TLS termination and certificate rotation](#tls-termination-and-certificate-rotation)
  - [Secrets and logs](#secrets-and-logs)
  - [Files and paths](#files-and-paths)
  - [AI-specific risks](#ai-specific-risks)
- [Hardening checklist for production](#hardening-checklist-for-production)
- [Reporting a vulnerability](#reporting-a-vulnerability)

## On by default

Every limit below is a field of `photon.Limits` with a safe default. The
[configuration guide](configuration.md#limits-reference) lists them all with
advice on when to change them.

### Slow clients

**Slowloris** opens connections and sends request headers one byte at a time,
holding a goroutine and a file descriptor per connection for as long as it
likes. `ReadHeaderTimeout` (5 s) closes a connection whose headers have not
arrived in time.

**Idle keep-alive connections** are closed after `IdleTimeout` (60 s), so an
attacker, or a buggy client pool, cannot hold descriptors open indefinitely.

`ReadTimeout`, a deadline on the whole request, is deliberately zero. It is
absolute, so it would cut off a five-minute streamed answer however healthy
the connection is. Streams are protected by `StreamStallTimeout` instead; see
[below](#stream-memory-is-bounded).

### Header floods

- **`MaxHeaderBytes` (16 KiB)** bounds the total header size. `net/http`
  enforces it from the first byte it reads and answers 431 itself. Its own
  default is 1 MiB, which across 10,000 connections is 10 GB of header buffer.
- **`MaxHeaders` (100)** bounds the number of header fields, a limit
  `net/http` has no concept of. Thousands of tiny headers fit under any byte
  limit while costing real CPU to parse, hash, and store. Photon counts
  **fields, not distinct names**: a request with 1,000 copies of `X-A: 1` is
  refused, where a check of `len(r.Header)` would see one name and let it
  through.
- **`MaxHeaderValueBytes` (8 KiB)** bounds any single value, so one enormous
  `Cookie` or `Authorization` header cannot take most of the budget.

Refusals are a 431 problem document naming the limit:

```json
{"type":"https://photon.agenticmarket.dev/errors/too_many_headers","title":"the request has too many header fields","status":431,"code":"too_many_headers","limit":"MaxHeaders"}
```

### Oversized bodies

`MaxRequestBodyBytes` (1 MiB) is enforced in two ways:

- **When `Content-Length` declares a larger body**, photon answers 413 before
  reading a byte of it, and closes the connection.
- **When the length is unknown** (chunked uploads), `r.Body` is wrapped in
  `http.MaxBytesReader`. A read past the limit fails with
  `*http.MaxBytesError`, `net/http` stops reusing the connection, and
  `photon.DecodeJSON` reports it as a 413 rather than as malformed JSON.

The limit is a rejection threshold, not a memory cost. A body is read as a
stream, so it bounds how much work one request can ask for.

### Error responses never reflect the request

A response that echoes a path, header, or body back is a route-enumeration
oracle and, if a browser can be persuaded to sniff it as HTML, an XSS vector.
Photon never does it:

- 404 and 405 bodies are fixed strings.
- Limit refusals are RFC 9457 problem documents that name the limit
  (`MaxHeaders`), which is configuration, not request data. A too-large header
  is named; its value is never echoed.
- `photon.Error` renders a `*photonerr.Error` with its `Message`, which you
  write, and never its `Detail` or wrapped cause, which are for your logs. Any
  other error becomes a 500 with a constant body, because an arbitrary error's
  text can contain a connection string or an internal hostname.
- `photon.DecodeJSON` errors never quote the body. A field name appears only
  when it is a plain identifier.
- `photon.JSON` escapes `<`, `>`, and `&`, so a string containing `<script>`
  is not a reflected-XSS vector.

Every response photon writes itself (`JSON`, `Text`, `HTML`, problem documents,
the 404/405/500 bodies) carries `X-Content-Type-Options: nosniff`, so a browser
will not reinterpret it as another content type.

### The client address is the socket peer

`photon.ClientIP(r)` returns the address of the immediate peer and ignores
`X-Forwarded-For`, `X-Real-IP`, and `Forwarded`. Those headers are written by
the client. Trusting them by default lets anyone forge their address and
defeat rate limiting, access logging, and IP allowlists. `MaxPerIP` is
enforced on the socket peer for the same reason.

Behind a proxy this means every request appears to come from the proxy. See
[Trusted proxies](#trusted-proxies-and-the-client-address) for how to handle
that safely.

### Routing cannot be bypassed with encodings or ambiguity

- **Routing uses the raw path.** `/admin%2Fsettings` is one segment and cannot
  reach `/admin/:page`. If `%2F` were decoded before routing, an encoded slash
  could cross a segment boundary that a proxy rule or an auth check treated as
  fixed.
- **Ambiguous routes are refused at startup.** Two routes that could both match
  a request panic during registration, so which handler serves a request (and
  which middleware protects it) never depends on registration order or
  spelling. See [routing](routing.md#conflicting-routes-panic-at-startup).
- **An encoding cannot walk around a route to a fallback.** A mount, or a
  custom `NotFound`, may hand a request to code that reads the decoded path -
  a legacy router, a file server. So a request whose raw path misses every
  route but whose decoded path would match one (`/admin%2Fsecret`,
  `/%61dmin/secret`) is refused with a 400 instead of reaching the fallback.
- **`HEAD` is not derived from `GET`**, so a handler never answers a method it
  was not written for.

### Panics do not leak

A panicking handler gets a `500 internal server error` with a constant body.
The panic value can contain anything the handler had, including secrets, so it
goes to your logger and your `OnPanic` hook, never to the client. A panic
after a stream has started aborts the connection, so the client sees a failure
instead of a truncated answer that looks complete.

### TLS defaults in ListenTLS

`ListenTLS` sets TLS 1.2 as the minimum version, negotiates HTTP/2, and does
not enable 0-RTT early data: early data is replayable by definition
(RFC 8470), and a replayable request is worse than a slow one. For certificate
reloading or ACME, see [deployment](deployment.md#tls).

### DecodeJSON refuses non-JSON bodies

`photon.DecodeJSON` answers 415 when the `Content-Type` is something other than
`application/json` or a `+json` type. This is a CSRF defence: a cross-site HTML
form can POST `text/plain` or form-encoded bodies without a CORS preflight,
but not `application/json`.

It is not a complete defence. A request with **no** `Content-Type` is
accepted, and a cross-site page can send one (for example, `fetch` with a
`Blob` or `ArrayBuffer` body). If you authenticate with cookies, also use
`SameSite` cookies and check `Origin`, or use CSRF tokens. Bearer tokens in an
`Authorization` header are not sent automatically by browsers, so they are not
exposed to CSRF.

`DecodeJSON` also refuses trailing data, so `{"a":1}{"a":2}` cannot smuggle a
second object past a check that looked only at the first.

### SSE field injection is impossible

SSE is a line-based format. If a newline from user input or model output
reached the wire unescaped, it could end a field early and start a new one,
injecting an event, changing an `id`, or setting `retry`.

- Event names and ids containing CR, LF, or NUL are refused:
  `Event`, `JSON`, and `SetEventID` return `photon.ErrInvalidEvent` and write
  nothing.
- Event data is split on CRLF, LF, and a lone CR exactly as the client splits
  it, and every line becomes its own `data:` field. The client reassembles the
  payload with `\n`, so line breaks survive and nothing in the payload can
  start a new field.
- `Comment` replaces line breaks with spaces.

This is pinned by a fuzz test, `FuzzSSEEventRoundTrip`.

### Stream memory is bounded

A slow or stalled client cannot make a stream grow without limit:

- **`MaxStreams`** bounds concurrent streams. When the server is full, a new
  stream gets a 503 with `Retry-After` before any buffer is allocated. The
  default is derived from the memory budget.
- **`MaxStreamBufferBytes`** (64 KiB) caps the unsent bytes per stream. When it
  is full, the producer waits; a slow client produces a slow producer, not a
  growing buffer.
- **`StreamStallTimeout`** (30 s) ends a stream whose client accepts nothing
  for that long, releasing its goroutine, buffer, and slot.

See [streaming](../guides/streaming.md#slow-clients-and-back-pressure) for the
overflow policies.

### Off by default: connection and concurrency caps

`MaxConnections`, `MaxConcurrentRequests`, and `MaxPerIP` are zero (unlimited)
by default, because the right numbers depend on your machine and your
traffic. When set, connections over the limit are refused before a byte of the
request is read, and requests over `MaxConcurrentRequests` get a 503 with
`Retry-After`. Set them for production; the
[configuration guide](configuration.md#limits-reference) explains how they
interact with streams and proxies.

A connection that is hijacked - a WebSocket upgrade - leaves this accounting
when it is hijacked, because nothing reports when it later closes. Bound
upgraded connections in the upgrade handler, for example with a semaphore.

## What is your job

### Authentication and authorization

Photon has no notion of users. Put authentication in a `Group` so a route
cannot forget it, and test it with one request per protected route. The
[middleware guide](middleware.md#bearer-token-auth-in-a-group) has a complete
bearer-token example.

Authorization is separate and per resource. A route like
`/v1/conversations/:id` must check that the conversation belongs to the
caller; authenticating the caller is not enough. Missing object-level checks
are the most common API vulnerability.

### CORS, CSRF, and browser-facing responses

- **CORS:** use an allowlist of origins. Never reflect an arbitrary `Origin`
  with credentials allowed. See [CORS](middleware.md#cors).
- **CSRF:** relevant if you authenticate with cookies. See
  [DecodeJSON](#decodejson-refuses-non-json-bodies) for what photon covers and
  what it does not.
- **Headers for HTML:** `photon.HTML` sets `nosniff` but no Content Security
  Policy, because a default CSP that is wrong for your app is worse than none.
  Set CSP, `Strict-Transport-Security`, and frame protection yourself, usually
  at the proxy.
- **Redirects:** `photon.Redirect(w, r, url)` redirects wherever you tell it.
  Never pass a URL taken from the request without checking it against an
  allowlist, or it is an open redirect.

### Rate limits and quotas

Photon bounds resources (connections, streams, memory). It does not limit how
often a client may call you, or how many tokens a user may spend. Add a rate
limiter ([example](middleware.md#rate-limiting-per-client)) and, for AI
endpoints, per-principal quotas on tokens and concurrent streams.

### Trusted proxies and the client address

Behind a load balancer, `photon.ClientIP` returns the load balancer's address.
To recover the real client, trust `X-Forwarded-For` **only when the connection
comes from your own proxy**, and read it from the right:

```go
package main

import (
	"net/http"
	"net/netip"
	"strings"

	"github.com/agenticmarket/photon"
)

// trustedProxies are the networks your own load balancers connect from.
var trustedProxies = []netip.Prefix{
	netip.MustParsePrefix("10.0.0.0/8"),
}

func trusted(ip netip.Addr) bool {
	for _, p := range trustedProxies {
		if p.Contains(ip) {
			return true
		}
	}
	return false
}

// realClientIP returns the client address as seen by your outermost trusted
// proxy. X-Forwarded-For is read right to left, through trusted hops only: the
// first address that is not a trusted proxy is the client. Anything to its
// left was written by the client and is ignored.
func realClientIP(r *http.Request) netip.Addr {
	ip := photon.ClientIP(r).Unmap()
	if !trusted(ip) {
		return ip // a direct connection: any forwarded header is the client's invention
	}
	hops := strings.Split(strings.Join(r.Header.Values("X-Forwarded-For"), ","), ",")
	for i := len(hops) - 1; i >= 0; i-- {
		hop, err := netip.ParseAddr(strings.TrimSpace(hops[i]))
		if err != nil {
			return ip // malformed or missing: fall back to the last trusted hop
		}
		hop = hop.Unmap()
		if !trusted(hop) {
			return hop
		}
		ip = hop
	}
	return ip
}
```

Use `realClientIP` wherever you would have used `photon.ClientIP`: rate-limit
keys, audit logs, allowlists. Rules that keep this safe:

- The proxy must **append** to `X-Forwarded-For` (nginx's
  `$proxy_add_x_forwarded_for`, AWS ALB's default), so the rightmost entry is
  the address it actually saw.
- Never take the leftmost entry. The client wrote it.
- List only networks you control in `trustedProxies`. With a CDN in front,
  trust its published ranges and prefer its own header (for example,
  Cloudflare's `CF-Connecting-IP`) when the peer is in those ranges.
- `MaxPerIP` counts the socket peer, which behind a proxy is the proxy. Do not
  set it in that deployment.

### TLS termination and certificate rotation

Terminate TLS at your proxy or load balancer, or in photon with `ListenTLS` or
`Serve` with your own `tls.Config`. Photon does not obtain or renew
certificates. [Deployment](deployment.md#tls) shows certificate reloading and
ACME.

### Secrets and logs

- Load API keys and model-provider credentials from the environment or a
  secret store, never from source.
- Treat your logs as sensitive. Photon logs panic values and stack traces, and
  the full cause of every 5xx written with `photon.Error`, so an operator can
  see what the client cannot. Those can contain whatever your code had in hand.
- Do not log `Authorization` headers, cookies, or prompts that may contain
  personal data unless you have decided to and protect the logs accordingly.
- `photonerr.RenderOptions{IncludeDetail: true}` sends `Detail` to clients. It
  is off by default; leave it off in production.

### Files and paths

Photon does not clean request paths. `/files/../etc/passwd` reaches
`/files/*path` with `path` set to `../etc/passwd`, and an encoded
`%2e%2e` arrives still encoded. Never join a path parameter onto a filesystem
path yourself. Use `os.OpenRoot` (Go 1.24) and open files through the
`*os.Root`, which refuses to escape its directory, or serve an `fs.FS` with
`http.FileServerFS`.

### AI-specific risks

Keep these short rules in mind; each has caused real incidents.

- **Model output is untrusted input.** Render it as text (`textContent`, or a
  Markdown renderer with raw HTML disabled), never as HTML. Never execute it,
  and never interpolate it into SQL, shell commands, or file paths. Check the
  scheme of any URL it produces (`https` only) before linking to or fetching
  it.
- **Tool URLs are an SSRF vector.** A model asked to "fetch this page" can be
  steered to `http://169.254.169.254/` (cloud metadata) or your internal
  services. Give tool calls an HTTP client that refuses non-public addresses,
  checked after DNS resolution:

  ```go
  // publicOnly refuses loopback, private, link-local (which includes cloud
  // metadata), multicast, and unspecified addresses. It runs on the address
  // actually dialled, so a hostname resolving to an internal address is
  // refused too, on every redirect.
  func publicOnly(network, address string, _ syscall.RawConn) error {
  	host, _, err := net.SplitHostPort(address)
  	if err != nil {
  		return err
  	}
  	ip, err := netip.ParseAddr(host)
  	if err != nil {
  		return err
  	}
  	if ip = ip.Unmap(); !ip.IsGlobalUnicast() || ip.IsPrivate() {
  		return fmt.Errorf("refusing to connect to non-public address %s", ip)
  	}
  	return nil
  }

  var toolClient = &http.Client{
  	Timeout: 30 * time.Second,
  	Transport: &http.Transport{
  		// No Proxy: a proxy would do the dialling and bypass the check.
  		DialContext: (&net.Dialer{Timeout: 10 * time.Second, Control: publicOnly}).DialContext,
  	},
  }
  ```

  Add your own VPC ranges or carrier-grade NAT space (`100.64.0.0/10`) if they
  are reachable from the server.
- **Cap agent loops.** Bound the number of tool calls per request, the tokens
  per request and per user, and the wall-clock time with
  `context.WithTimeout(r.Context(), ...)`. Pass `r.Context()` to every model
  and tool call so a client that leaves stops the work.
- **Assume prompt injection.** Retrieved documents, web pages, and tool results
  can contain instructions. Tools should act with the end user's permissions,
  not the server's; side-effecting tools (sending email, spending money,
  deleting data) should require explicit user confirmation; and secrets should
  never be placed in a prompt, because the model can be talked into repeating
  them.

See [Building AI backends](./ai-backends.md) for the surrounding patterns.

## Hardening checklist for production

- [ ] TLS everywhere, terminated at a proxy or with `ListenTLS`; HSTS at the edge.
- [ ] `GOMEMLIMIT` or `photon.MemoryBudget` set explicitly; `app.Limits()` logged at startup.
- [ ] `MaxConcurrentRequests` and `MaxConnections` sized for the machine; `MaxPerIP` only if clients connect directly.
- [ ] `photon.Logger` configured, with alerts on panics and 5xx; 413, 431, 429, and 503 rates on a dashboard.
- [ ] Authentication applied through groups, with one behavioural test per protected route.
- [ ] Object-level authorization on every route that takes a resource id.
- [ ] CORS allowlist; no reflected origins.
- [ ] Rate limits, plus token and stream quotas per principal.
- [ ] Real client IP derived from trusted proxies only.
- [ ] Container runs as non-root, with a read-only root filesystem and no shell.
- [ ] pprof and admin endpoints on a separate, non-public listener.
- [ ] Secrets from the environment or a secret store; logs treated as sensitive.
- [ ] Outbound HTTP for tools goes through an SSRF-safe client with timeouts.
- [ ] `govulncheck ./...` in CI, and Go kept current. Photon has no dependencies, so most fixes that matter (in `net/http` and `crypto/tls`) arrive with Go releases.

## Reporting a vulnerability

Please do not open a public issue for a security problem. Follow the process in
[SECURITY.md](../../SECURITY.md).

## See also

- [Configuration](configuration.md): every limit, its default, and when to change it
- [Middleware](middleware.md): auth, CORS, and rate-limiting examples
- [Deployment](deployment.md): proxies, TLS, and containers
- [Routing](routing.md): conflict rules and raw-path routing
- [Streaming](../guides/streaming.md)
- [Building AI backends](./ai-backends.md)
