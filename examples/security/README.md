# `security` — what photon protects you from by default

No configuration. Every limit in this example is a default, which is the point:
most of what a "secure Go server" article tells you to add is already on.

```bash
go run ./examples/security
```

## `/limits` — the defaults, printed by the server

```bash
curl http://localhost:8080/limits
```

```json
{
  "note": "every value below is a default; this program configures none of them",
  "limits": {
    "max_request_body_bytes": 1048576,
    "max_header_bytes": 16384,
    "max_headers": 100,
    "max_header_value_bytes": 8192,
    "read_header_timeout": "5s",
    "idle_timeout": "1m0s",
    "read_timeout": "0s",
    "max_connections": 0,
    "max_concurrent_requests": 0,
    "max_per_ip": 0,
    "max_streams": 0,
    "max_stream_buffer_bytes": 65536
  }
}
```

Four of those deserve explanation, because they are the ones nobody remembers
to configure:

| Limit | Value | The attack it stops |
|---|---:|---|
| `MaxHeaders` | 100 | **50,000 tiny headers.** Well under any byte limit, but real CPU to parse, hash, and store. This is the limit `net/http` has no concept of. |
| `MaxHeaderValueBytes` | 8192 | One enormous `Cookie` or `Authorization` header. A total-bytes limit alone still permits this. |
| `ReadHeaderTimeout` | 5s | **Slowloris.** A client trickling headers holds a connection and a goroutine open indefinitely. |
| `MaxRequestBodyBytes` | 1 MiB | Uploads that are really memory-exhaustion attacks. |

`max_streams` shows `0` because it is *derived* at startup from the host's memory
budget, not fixed — a 2-core edge box and a 64-core container have the same
problem, and it is memory rather than CPU that bounds it. Read the effective
value with `app.Limits().MaxStreams` after `photon.New`.

## The body limit is a rejection threshold, not a memory cost

```bash
# 2 MB, refused
head -c 2000000 /dev/urandom | curl -i -X POST http://localhost:8080/upload --data-binary @-
# HTTP/1.1 413 Request Entity Too Large
```

`upload` does no size checking of its own, and does not need to. The limit is
enforced by a reader wrapping the body, so **a 2 GB upload costs about the same
memory as a 2 KB one** and is refused at the same rate. The limit bounds how much
work one request can ask for; it does not describe how much memory the server
holds.

## Forwarded headers are not believed

```bash
curl http://127.0.0.1:8080/echo-ip -H "X-Forwarded-For: 1.2.3.4"
```

```json
{
  "client_ip": "127.0.0.1",
  "x_forwarded_for_header": "1.2.3.4",
  "note": "the header is reported but not believed"
}
```

`photon.ClientIP` returns the **immediate peer** and ignores `X-Forwarded-For` and
its relatives. Those headers are set by the client, so trusting them by default
lets anyone forge their apparent address and defeat rate limiting, access
logging, and IP-based authorization.

This is the right default and it has a real cost: **behind a reverse proxy, every
request looks like it comes from the proxy.** A production deployment behind
nginx or a load balancer must configure trusted-proxy handling itself. Photon
documents that the server is responsible for replacing the behaviour; this
example does not do it, because getting it wrong in an example is worse than not
doing it.

## Panics are recovered, logged, and survivable

```bash
curl -i http://localhost:8080/panic
# HTTP/1.1 500 Internal Server Error
```

```
ERROR handler panicked method=GET path=/panic panic="something went wrong deep in a handler"
stack="goroutine 19 [running]:
photon.stack()
	/src/photon/conn.go:213 +0x38
..."
```

The process stays up — `curl /limits` still returns 200 afterwards. One bad
handler cannot take the server down.

`OnPanic` is **optional**. Recovery happens without it; the hook exists so the
log can be structured and routed where an operator will see it.

One caveat worth knowing: if a handler panics *after* the response has been
committed, the status cannot be changed. photon reports that as
`ErrStreamPanic` rather than pretending it sent a 500.

## Typed errors, as RFC 9457 documents

```bash
curl -i http://localhost:8080/error
```

```json
{
  "type": "https://photon.agenticmarket.dev/errors/not_found",
  "title": "widget not found",
  "status": 404,
  "code": "not_found"
}
```

```go
_ = photonerr.Render(w, photonerr.NotFound("widget"), photonerr.RenderOptions{})
```

**An error that is not a `photonerr.Error` becomes a 500 with a constant body.**
That is deliberate: an unknown error is often a database error containing a
connection string, and its text is not safe to send a client.

`IncludeDetail` is off by default for the same reason — the detail is written for
an operator reading a log and usually contains an internal hostname.

## What photon does *not* do

Being explicit about this matters more than the list of things it does:

| Not provided | Why | Use instead |
|---|---|---|
| TLS configuration | Certificates are a deployment concern | `ListenTLS`, or terminate at a proxy |
| Password hashing | Getting it wrong is a vulnerability, not a feature | `golang.org/x/crypto/bcrypt` |
| CSRF protection | Only meaningful for cookie auth, which photon does not assume | Your auth layer's job |
| Trusted-proxy IP resolution | The safe default is "trust nothing" | Configure it per deployment |
| SQL injection defence | photon never touches a database | Parameterised queries |

Photon is an HTTP library. It handles the HTTP-shaped attacks and nothing else.

## Tuning limits

```go
limits := photon.DefaultLimits() // start from the defaults...
limits.MaxRequestBodyBytes = 8 << 20 // ...raise only if you serve uploads
limits.MaxConnections = 10_000       // 0 means "bounded by the fd limit"
limits.MaxPerIP = 100
app := photon.New(photon.SetLimits(limits))
```

Start from `DefaultLimits()`. A `photon.Limits{...}` literal sets every field
it does not name to zero, and zero means *unlimited* for the request limits -
so a literal that only meant to raise the body limit also removes the header
limits.

Raise these **deliberately**. Every one of them is a limit an attacker would
like removed, and a limit removed by reflex is indistinguishable from a limit
that was never there.

Negative values are rejected at startup rather than silently reinterpreted —
a `MaxHeaderBytes: -1` is a bug (net/http would read it as its 1 MB default),
and a library that quietly accepts it hides the bug until it is an incident.

## See also

- [`docs/guides/security.md`](../../docs/guides/security.md) — what Photon protects against, and what it leaves to you
- `examples/middleware` — rate limiting, which complements these limits rather
  than duplicating them
