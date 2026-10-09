# Configuration

`photon.New()` with no arguments gives you a server with safe limits, so
configuration is for changing something, not for getting started. This guide
is the reference for everything you can change: every field of
`photon.Limits` with its default and when to move it, the values photon
derives from your machine's memory, the options `New` accepts, what is logged
by default, and how invalid settings are reported.

- [Changing a limit](#changing-a-limit)
- [Limits reference](#limits-reference)
- [Derived values: memory budget and stream limits](#derived-values-memory-budget-and-stream-limits)
- [Options](#options)
- [Logging](#logging)
- [Validation: New panics on contradictory limits](#validation-new-panics-on-contradictory-limits)
- [A different body limit per route](#a-different-body-limit-per-route)
- [Per-route stream settings](#per-route-stream-settings)
- [Environment variables](#environment-variables)

## Changing a limit

Start from `DefaultLimits()` and change only what you need:

```go
limits := photon.DefaultLimits()
limits.MaxRequestBodyBytes = 8 << 20 // 8 MiB, for document uploads
limits.MaxConcurrentRequests = 2000

app := photon.New(photon.SetLimits(limits))
```

Do not start from an empty `Limits{}`. A zero field means "unlimited" for the
request limits and "derived" for the stream limits, so every field you did not
set loses its protection:

```go
// Wrong: this removes the body limit, the header limits, the header and idle
// timeouts, and heartbeats. Only MaxConnections is what you meant.
app := photon.New(photon.SetLimits(photon.Limits{MaxConnections: 2000}))
```

The `Limits` value is copied into the server by `New`, so changing your copy
afterwards has no effect. `app.Limits()` returns the effective values,
including the derived ones. Logging them at startup is a good habit:

```go
l := app.Limits()
slog.Info("limits",
	"memoryBudget", l.MemoryBudget,
	"maxStreams", l.MaxStreams,
	"maxBody", l.MaxRequestBodyBytes,
	"maxConcurrentRequests", l.MaxConcurrentRequests)
```

## Limits reference

| Field | Default | What it bounds | Zero means | When to change it |
|---|---|---|---|---|
| `MaxRequestBodyBytes` | 1 MiB | Request body size. 413 before reading if `Content-Length` is larger; a read error past the limit otherwise. | Unlimited | Raise for uploads or prompts that carry documents or images. It is server-wide: raise it to the largest body any route accepts and [lower it per route](#a-different-body-limit-per-route). |
| `MaxHeaderBytes` | 16 KiB | Total size of the request headers, enforced by `net/http` (431). | `net/http`'s default, 1 MiB. Not unlimited. | Raise if large cookies or JWTs push requests past 16 KiB. |
| `MaxHeaders` | 100 | Number of header **fields**, repeated names included (431). | Unlimited | Rarely. Raise only if a proxy chain adds many headers. |
| `MaxHeaderValueBytes` | 8 KiB | Size of any single header value (431). | Unlimited | Raise for very large JWTs. Must not exceed `MaxHeaderBytes`. |
| `ReadHeaderTimeout` | 5 s | Time to receive the request headers: the Slowloris defence. | `net/http` falls back to `ReadTimeout`; with both zero, no limit. | Lower if your clients are fast and you want slow ones gone sooner. |
| `IdleTimeout` | 60 s | How long an idle keep-alive connection is kept open. | `net/http` falls back to `ReadTimeout`; with both zero, no limit. | Behind a load balancer, set it **above** the balancer's idle timeout (for example 75 s behind AWS ALB's default 60 s), so the server never closes a connection the balancer is about to reuse. |
| `ReadTimeout` | 0 | The whole request, headers and body together. | No deadline | Leave it at zero on servers that stream. The deadline is absolute, and when it fires mid-stream `net/http` cancels the request. Bound slow uploads per route instead. |
| `MaxConnections` | 0 | Concurrent accepted connections. Excess connections are closed before any request byte is read. | Unlimited (the process file-descriptor limit) | Set it in production. With HTTP/1.1 each open stream holds a connection, so keep it well above `MaxStreams`. |
| `MaxConcurrentRequests` | 0 | Handlers running at once. Excess requests get 503 with `Retry-After: 1`. | Unlimited | Set it to bound work. A stream counts for its whole life, so keep it above `MaxStreams` plus headroom for ordinary requests. |
| `MaxPerIP` | 0 | Concurrent connections from one peer address. | Unlimited | Only when clients connect directly. Behind a proxy or load balancer every connection comes from the proxy, and the limit would throttle all your traffic. Browsers open up to six HTTP/1.1 connections per host. |
| `MaxStreams` | derived | Concurrent streams. Excess streams get 503 with `Retry-After: 1`. | Derived from `MemoryBudget` | Set it when you know your per-stream cost, or want a fixed number regardless of the host. |
| `MaxStreamBufferBytes` | 64 KiB | Bytes accepted from a stream's handler but not yet sent. When full, the producer waits. | 64 KiB | A buffer starts at 4 KiB and grows only as needed, so a token stream rarely reaches the cap. Lower it to reduce what one slow client can pin; raise it for large, bursty events. Events larger than the buffer are still sent, in pieces. |
| `MaxTotalStreamBufferBytes` | derived | The server-wide budget the `Grow` overflow policy draws on when a stream enlarges its buffer past `MaxStreamBufferBytes`. | `MaxStreams × MaxStreamBufferBytes` | Leave it derived. It only matters if you use `photon.Grow`. |
| `StreamStallTimeout` | 30 s | How long a stream may go without the client accepting any data before it ends with `ErrStreamOverflow`. | 30 s (it cannot be disabled) | Lower it to release stuck clients sooner; raise it for clients on unreliable mobile networks. |
| `HeartbeatInterval` | 15 s | Silence on an SSE stream before photon sends a `: ping` comment. | Heartbeats off | Keep it below the idle timeout of every proxy between you and the client. |
| `MemoryBudget` | detected | The memory ceiling `MaxStreams` is derived from. | Detect it | Set it, or `GOMEMLIMIT`, in production, so the derived numbers are not a guess. |

Durations are `time.Duration`. `photon.Second` and `photon.Millisecond` are
provided so a `Limits` literal reads naturally (`5 * photon.Second`).

`MaxConnections` and `MaxPerIP` are enforced by the listener that `Run`,
`Listen`, `ListenTLS`, and `Serve` use. `MaxHeaderBytes`,
`ReadHeaderTimeout`, `IdleTimeout`, and `ReadTimeout` are settings on the
underlying `http.Server`. None of them apply when the photon server is used as
a plain `http.Handler`, as in tests or when mounted inside another server. The
rest are enforced in `ServeHTTP` and apply everywhere.

## Derived values: memory budget and stream limits

Streaming memory is bounded by the number of streams. Rather than hard-code
that number, photon derives it from how much memory the process may use, so a
512 MiB container and a 64 GiB server both get a safe value.

### MemoryBudget detection

When `MemoryBudget` is zero, `New` looks for a memory ceiling in this order and
uses the first it finds:

1. **`GOMEMLIMIT`.** Photon asks the Go runtime for its soft memory limit, so
   any format Go accepts works (`512MiB`, `1GiB`, a byte count), and a limit
   set in code with `debug.SetMemoryLimit` before `photon.New` counts too.
2. **The cgroup v2 limit** (Linux). Photon reads its own cgroup from
   `/proc/self/cgroup` and checks `memory.max` there and in every ancestor up
   to the root, taking the smallest. That finds a container's limit and a
   systemd unit's `MemoryMax=` alike. cgroup v1 is not read.
3. **`MemTotal` from `/proc/meminfo`** (Linux): the machine's RAM.
4. **A 256 MiB fallback**, with a warning in the log.

`GOMEMLIMIT` comes first because a container with a 512 MiB limit on a 64 GiB
host is the common case, and scaling from the host's RAM would advertise 64 GiB
worth of streams.

On macOS and Windows, steps 2 and 3 do not exist, so without `GOMEMLIMIT` or
`MemoryBudget` the server always uses the fallback. The fallback warning is
logged at warning level, which the default logger drops; pass `photon.Logger`
to see it.

### MaxStreams

```text
MaxStreams = clamp(MemoryBudget / 4 / 256 KiB, 64, 8192)
```

A quarter of the budget goes to streams, leaving the rest for the process, the
ordinary request path, and the garbage collector's headroom. Each stream is
costed at 256 KiB: its 64 KiB buffer, its goroutine stack, the stream itself,
and the connection's write buffer. The floor keeps a tiny machine serving a few
streams; the ceiling keeps a large machine from advertising tens of thousands
on the strength of a heuristic. In practice it is one stream per MiB of
budget:

| Memory budget | MaxStreams | MaxTotalStreamBufferBytes |
|---|---|---|
| 32 MiB | 64 (floor) | 4 MiB |
| 256 MiB (fallback) | 256 | 16 MiB |
| 512 MiB | 512 | 32 MiB |
| 1 GiB | 1,024 | 64 MiB |
| 4 GiB | 4,096 | 256 MiB |
| 8 GiB or more | 8,192 (ceiling) | 512 MiB |

### MaxTotalStreamBufferBytes

```text
MaxTotalStreamBufferBytes = MaxStreams × MaxStreamBufferBytes
```

It is derived rather than set beside the count so that a high stream count and
a forgotten memory cap cannot be configured together. Each stream's own buffer
is already accounted for by `MaxStreams`; this budget is what the `Grow`
overflow policy may add on top when a stream enlarges its buffer to absorb a
burst. When the budget is used up, `Grow` behaves like `Disconnect`.

## Options

`New` takes options, applied in order:

```go
var panics = expvar.NewInt("panics")

app := photon.New(
	photon.SetLimits(limits),
	photon.MemoryBudget(512<<20),
	photon.Logger(slog.New(slog.NewJSONHandler(os.Stderr, nil))),
	photon.OnPanic(func(p *photon.PanicInfo) { panics.Add(1) }),
	photon.ShutdownTimeout(20*time.Second),
)
```

**`SetLimits(l Limits)`** sets the whole resource budget. `MaxStreams` and
`MemoryBudget` are applied after it whatever their position, so option order
does not matter:

```go
// Both keep MaxStreams at 256.
photon.New(photon.MaxStreams(256), photon.SetLimits(limits))
photon.New(photon.SetLimits(limits), photon.MaxStreams(256))
```

**`Logger(l *slog.Logger)`** sets the logger. See [Logging](#logging).

**`MemoryBudget(bytes int64)`** states the memory ceiling instead of detecting
it. `MaxStreams` is then derived from it. Use it when you know the budget and
do not want to depend on detection:

```go
app := photon.New(photon.MemoryBudget(512 << 20)) // a 512 MiB container
```

**`MaxStreams(n int)`** sets the concurrent stream limit directly, overriding
the derived value. The 64 and 8,192 bounds apply only to the derived value,
not to one you set.

**`OnPanic(f func(*PanicInfo))`** is called when a handler panics, including
mid-stream, for reporting to an error tracker. `PanicInfo` carries the
recovered `Value`, the `Stack` (up to 8 KiB), and the request's `Method` and
`Path`. Photon still recovers the panic, answers 500 (or aborts a started
stream), and logs it; the hook is in addition to that. It runs on the request's
goroutine, so it must be safe for concurrent use.

**`ShutdownTimeout(d time.Duration)`** sets how long `Run` waits for in-flight
requests and streams after `SIGINT` or `SIGTERM`. The default,
`photon.DefaultShutdownTimeout`, is 25 seconds: under Kubernetes' default
30-second termination grace period, with room for the process to exit. It must
be positive. If you call `Shutdown` yourself, you pass your own deadline and
this option is not used.

## Logging

With no `Logger` option, photon logs **errors only**, through `slog.Default()`:

- handler panics, with the stack trace;
- 5xx responses written with `photon.Error`, with the full error, so you see
  the cause the client does not;
- streams that end with a server error after they started.

The default resolves `slog.Default()` on every record, so calling
`slog.SetDefault` after `photon.New` still takes effect.

Warnings are dropped by default because the most common one is a rejected
request, and logging every rejection by default would hand an attacker a log
flood. Pass a logger to see them:

```go
app := photon.New(photon.Logger(slog.New(slog.NewTextHandler(os.Stderr, nil))))
```

With a logger configured, photon also logs:

| Level | Message | When |
|---|---|---|
| Warn | `request rejected` | A request refused by a limit (431, 413, 503), naming the limit, method, and path. At most one line per second, with a count of the lines suppressed since the last one. |
| Warn | `connection refused` | A connection refused by `MaxConnections` or `MaxPerIP`. At most one line per second, with a count of the lines suppressed since the last one. |
| Warn | `no memory limit detected; using the fallback budget` | At `New`, when detection found nothing. |
| Warn | stream misuse | `Stream.Overflow` called after the first write, or `DropOldest` dropping data from a text stream. |
| Warn | `streams cannot set write deadlines` | Once per server, when a middleware wraps the `ResponseWriter` without an `Unwrap` method; see [middleware](middleware.md#wrapping-the-responsewriter-implement-unwrap). |
| Warn | `net/http` errors | `net/http`'s own error log, such as TLS handshake failures. |
| Info | `photon: listening`, `photon: shutting down` | From `Run`. |

Both rejection lines are rate-limited, so a flood that the limits are refusing
cannot also flood the log. For exact counts, watch your access log or metrics:
the `suppressed` field says how many lines were folded into each one.

## Validation: New panics on contradictory limits

`New` checks the limits before deriving anything and panics on a value that is
a mistake. A bad limit fails at startup, in your first test run, rather than
being silently replaced by a default and discovered under attack. The panic
reads `photon: invalid limits:` followed by the reason:

| Rejected | Meaning |
|---|---|
| A negative `MaxRequestBodyBytes`, `MaxHeaders`, `MaxHeaderValueBytes`, `MaxConnections`, `MaxConcurrentRequests`, or `MaxPerIP` | Use 0 for unlimited. |
| A negative `MaxHeaderBytes` | Use 0 for net/http's 1 MB default. A negative value would otherwise silently become that default. |
| A negative `ReadHeaderTimeout`, `ReadTimeout`, or `IdleTimeout` | Use 0 for no timeout. |
| `MaxHeaderValueBytes` larger than `MaxHeaderBytes` | The value limit could never apply. |
| A negative `MaxStreams`, `MaxStreamBufferBytes`, `StreamStallTimeout`, or `MemoryBudget` | Use 0 for the derived or default value. |
| A negative `HeartbeatInterval` | Use 0 to disable heartbeats. |
| `MaxTotalStreamBufferBytes` below `MaxStreams × MaxStreamBufferBytes` | The cap would govern only some of the streams the count allows; it is a contradiction, not a stricter policy. Checked again after `MaxStreams` is derived, so an explicit cap cannot slip under a derived count. |

`ShutdownTimeout` with a zero or negative duration panics with
`photon: ShutdownTimeout must be positive`.

A zero `MaxHeaderBytes` or `MaxTotalStreamBufferBytes` is not an error: they
mean `net/http`'s 1 MiB default and the derived budget respectively.

## A different body limit per route

`MaxRequestBodyBytes` is applied to every request before routing, so a route
cannot raise it. To accept large bodies on a few routes only, set the
server-wide limit to the largest you accept and lower it everywhere else with
middleware:

```go
// maxBody lowers the body limit for the routes it wraps. It cannot raise it
// above the server's MaxRequestBodyBytes, which applies first.
func maxBody(n int64) photon.Middleware {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.ContentLength > n {
				photon.Error(w, r, photonerr.LimitExceeded("route body limit"))
				return
			}
			r.Body = http.MaxBytesReader(w, r.Body, n)
			next.ServeHTTP(w, r)
		})
	}
}
```

```go
limits := photon.DefaultLimits()
limits.MaxRequestBodyBytes = 20 << 20 // the largest body any route accepts
app := photon.New(photon.SetLimits(limits))

app.POST("/v1/files", uploadFile) // up to 20 MiB

small := app.Group("", maxBody(1<<20)) // everything else: 1 MiB
small.POST("/v1/chat", photon.SSE(chat))
small.POST("/v1/embeddings", embed)
```

`photon.DecodeJSON` reports the lower limit as a 413 too, because
`http.MaxBytesReader` returns the same `*http.MaxBytesError`.

## Per-route stream settings

The stream limits above are server-wide defaults. A streaming route can
override the per-stream ones:

```go
app.GET("/v1/metrics/live", photon.SSE(liveMetrics,
	photon.WithOverflow(photon.DropOldest), // newest value wins
	photon.WithStall(5*time.Second),        // overrides StreamStallTimeout
	photon.WithStreamBuffer(16<<10),        // overrides MaxStreamBufferBytes
))
```

See [streaming](../guides/streaming.md#slow-clients-and-back-pressure) for what
the overflow policies do.

## Environment variables

Photon reads no environment variables. Configuration that changes behaviour
depending on what happens to be set in the environment is hard to audit, so
your program owns its configuration and decides what to read. (The one
exception is indirect: `GOMEMLIMIT` is read by the Go runtime, and photon asks
the runtime for the result.)

Reading `PORT` and a limit yourself:

```go
package main

import (
	"log"
	"net/http"
	"os"
	"strconv"

	"github.com/agenticmarket/photon"
)

func main() {
	limits := photon.DefaultLimits()
	if v := os.Getenv("MAX_BODY_BYTES"); v != "" {
		n, err := strconv.ParseInt(v, 10, 64)
		if err != nil || n <= 0 {
			log.Fatalf("MAX_BODY_BYTES=%q: want a positive number of bytes", v)
		}
		limits.MaxRequestBodyBytes = n
	}

	app := photon.New(photon.SetLimits(limits))
	app.GET("/healthz", func(w http.ResponseWriter, r *http.Request) {
		_ = photon.Text(w, http.StatusOK, "ok\n")
	})

	addr := ":8080"
	if port := os.Getenv("PORT"); port != "" {
		addr = ":" + port
	}
	if err := app.Run(addr); err != nil {
		log.Fatal(err)
	}
}
```

Fail loudly on a value you cannot parse. Falling back to a default would hide
the mistake, which is the same reason `New` panics on invalid limits.

## See also

- [Security](security.md): the attack each limit defends against
- [Deployment](deployment.md): setting `GOMEMLIMIT`, container limits, and proxy timeouts
- [Streaming](../guides/streaming.md): overflow policies and per-stream options
- [Middleware](middleware.md): what runs before and after the limits
- [Getting started](../getting-started.md)
