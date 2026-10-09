# Changelog

All notable changes to Photon. The project follows [Semantic
Versioning](https://semver.org); before 1.0, minor versions may contain breaking
changes, and every one is listed here with what to do about it.

## Unreleased

The first public release, licensed under the Apache License 2.0. Compared
with the pre-release code, this fixes defects that made streaming unusable,
closes two security holes, and reworks the API for everyday use. Measurements
are in [BENCHMARKS.md](BENCHMARKS.md).

### Fixed

- **Events were not delivered until the stream closed.** A single `Event` sat
  in the buffer until 16 KB accumulated, so token streams arrived in one lump
  at the end. Each event is now sent as soon as the connection is free (~17 µs
  on loopback), with natural batching under load.
- **SSE field injection.** A line break in an event name or id, or a lone `\r`
  in data - which model output can contain - could inject SSE fields. Names and
  ids with CR, LF, or NUL are now refused with `ErrInvalidEvent`; data is split
  on CRLF, LF, and CR exactly as clients split it.
- **`MaxHeaders` counted header names, not fields.** A thousand copies of one
  header passed a limit of 100. It now counts every field.
- **A body of exactly `MaxRequestBodyBytes` was rejected** when sent chunked.
  The limit is now enforced with `http.MaxBytesReader`, and the error is the
  standard `*http.MaxBytesError`.
- **`GOMEMLIMIT` in Go's own syntax (`512MiB`) was ignored**, and `512K` was
  read as 51 KiB, so stream limits were derived from a guess. The limit is now
  read from the runtime. Cgroup v2 detection also walks up from the process's
  own cgroup, so systemd's `MemoryMax=` is found.
- **`Shutdown` left connections open after its deadline**, although its
  documentation said they were closed. They now are.
- **TLS with `MaxConnections` or `MaxPerIP` broke HTTP/2.** Admitted
  connections were wrapped, which hid the `*tls.Conn` from net/http: a client
  that negotiated h2 was handed to the HTTP/1 parser, and `r.TLS` was nil.
  Admission slots are now released from net/http's connection-state hook and
  connections are never wrapped.
- **`Mount` could not serve trailing-slash or deep paths**, so the
  `net/http/pprof` index and `http.FileServer` directory listings were 404s,
  and `Group("").Mount("/", h)` panicked.
- **`SetLimits` silently discarded `MaxStreams` and `MemoryBudget`** passed
  before it. Option order no longer matters.
- **A negative `MaxHeaderBytes` became net/http's 1 MB default** instead of an
  error. It, and a negative `MaxRequestBodyBytes`, now panic in `New`. An
  explicit `MaxTotalStreamBufferBytes` below what a derived `MaxStreams` needs
  is now caught too.
- **`MaxTotalStreamBufferBytes` was not a true ceiling**: `Grow` counted only
  growth against it, so buffer memory could reach twice the limit.
- **Path parameters were sometimes escaped** (when the path contained `%2F` or
  another escape net/http keeps in `RawPath`) and decoded otherwise. They are
  now always decoded, as `ServeMux` does.
- **Rejected requests logged one line each** when a logger was configured,
  turning a refused flood into a log flood. They are now rate-limited like
  refused connections.
- **(Review fixes before release)** `Close` could return while the stream's
  writer goroutine was still writing to a slow client, so the writer used a
  `ResponseWriter` net/http had already finished - a data race on HTTP/1 and a
  process crash on HTTP/2. `Close` now waits while the client makes progress,
  and otherwise forces the write to fail before returning. `DropOldest` could
  drop the unsent half of a frame whose first half was on the wire.
  `StartHeartbeats` committed headers from a timer goroutine, racing the
  handler's own header writes. A request whose raw path missed a protected
  route but whose decoded path matched it (`/admin%2Fsecret`) could fall
  through to a mount; it is now refused with a 400.
- An oversized body was `body_too_large` when its length was declared and
  `limit_exceeded` when it overflowed while reading. Both are now
  `limit_exceeded` with `"limit":"MaxRequestBodyBytes"`.
- **`StartHeartbeats` wrote from a second goroutine** to a stream documented
  as single-goroutine. Streams are now safe for concurrent use.
- **`DropOldest` and `Grow` could not take effect** with a real connection: a
  stuck client tripped the stall deadline first. With the new writer the
  producer never blocks on the socket, so both policies work, and `DropOldest`
  drops whole events instead of raw bytes, which would have corrupted the
  stream.
- **A slow client that kept reading could be disconnected** as if it had
  stopped, because a whole 32 KB batch had to finish within one stall period.
  Progress is now measured per 8 KB.
- **The newline tokens a model emits were lost**: data ending in `\n` dropped
  the newline, and `\n\n` arrived as `\n`. Data now round-trips exactly.
- **`Event(name, nil)` sent nothing a browser would dispatch.** Empty data is
  now sent as an empty `data:` line, so `done` events arrive.
- **`Router.Lookup` returned parameters from a buffer it had already returned
  to the pool.** It now returns a copy.
- **Registering routes was quadratic**: 5,000 routes took 7 seconds. Now 31 ms.
- **`ErrStreamPanic` was documented but never produced.** A panic after the
  first event is now recorded, logged, and aborts the connection, so a truncated
  answer cannot look complete.
- **Handler panics were silent by default** because the default logger
  discarded everything. Errors now go to `slog.Default`.
- **`Connection: keep-alive` was sent on HTTP/2 streams**, where RFC 9113
  forbids connection-specific headers.
- `photon init` overwrote existing files and generated a `go.mod` that only
  worked inside the repository. `photon bench` miscounted events when a chunk
  boundary split a line. The OpenAI-compatible demo named its events, which
  OpenAI SDKs do not parse as chunks.

### Added

- `photon.SSE(func(s *Stream, r *http.Request) error)` — the streaming handler
  adapter: limits, closing, error rendering, panic handling.
- `Stream.JSON`, `Stream.Header`, `Stream.Flush`, `Stream.Done` (closes on
  client disconnect and on server shutdown), automatic heartbeats.
- Non-SSE streams (NDJSON, raw bytes) keep their own `Content-Type` and never
  receive heartbeats.
- `Server.Run(addr)` — listen, handle SIGINT/SIGTERM, drain; `ShutdownTimeout`
  option and `DefaultShutdownTimeout` (25 s).
- `Group` (prefix + middleware, nestable) and `Mount` (an `http.Handler` for
  every method under a prefix - for MCP servers, pprof, legacy muxes).
- `DecodeJSON` (415 / 400 / 413 with safe messages) and `Error` (RFC 9457
  problem documents, 5xx logged with cause).
- `photonerr.Unauthorized`, `Forbidden`, `UnsupportedMediaType`,
  `UnprocessableEntity`, `TooManyRequests`.
- `ErrRouteConflict` and `ErrBadPattern`, which a registration panic matches
  with `errors.Is`.
- A once-per-server warning when a middleware hides write deadlines by wrapping
  the `ResponseWriter` without `Unwrap`.
- Path parameters are stored with `Request.SetPathValue`, so `r.PathValue`
  works, and `r.Pattern` holds the matched route template for metrics and
  OpenTelemetry.
- `Retry-After` on every 503.
- Examples: `openai-proxy` (a gateway that cancels the upstream model when the
  client leaves), `agent` (typed agent events with a browser UI). `photon init`
  scaffolds from the examples themselves.
- Documentation: getting started, eight guides, six migration guides, a full
  API reference.

### Changed (breaking)

- **Module path** is `github.com/agenticmarket/photon` (was `photon`).
- **Verbs take `http.HandlerFunc`** (was `http.Handler`). A plain function now
  works without `photon.HandlerFunc(...)`. Existing `photon.HandlerFunc(f)`
  calls still compile. To pass a non-function `http.Handler`, use `Handle` or
  its `ServeHTTP` method value.
- **`Use` applies to every route**, including those registered before it, and
  to 404 and 405 responses (previously only routes registered after it). CORS
  preflights no longer need `OPTIONS` routes. Middleware that should cover only
  some routes belongs on a `Group`.
- **`NotFound` and `MethodNotAllowed` panic after the server starts**, like
  route registration: changing them on a live server was a data race.
- **`NewStream(w, r, opts...)`** takes stream options; `StreamConfig` is no
  longer exported.
- **`SetEventID` attaches the id to the next event** in the same frame (was:
  written immediately as a separate line). `SetRetry` sends a complete frame.
- **`Stream.Overflow` after the first write** is ignored with a warning (was:
  recorded as the stream's error).
- **Streaming writes are asynchronous.** A transport failure is reported by
  the next `Event` call (or by `Flush`), not by the call that queued the data.
- **Overflow policies**: `DropOldest` and `Grow` act when the buffer fills,
  without waiting for the stall deadline. The stall deadline now means "no
  progress for this long" under every policy.
- **`StartHeartbeats` opens the stream immediately** (commits the 200) instead
  of after the first silent interval. Call it after validating the request.
- **Streams from `Streaming()` without a `Server`** (a bare `Router` in a test)
  now get a standalone stream with default limits instead of none.
- **Validation**: negative `MaxConnections`, `MaxConcurrentRequests`, or
  `MaxPerIP` now panic in `New`, like the other limits.
- **Route patterns containing `{` or `}` are refused**, with a message giving
  the Photon spelling, instead of registering a literal path that never matches.
- **`/files` and `/files/*path` may now coexist**: they match disjoint paths.
- **Duplicate parameter names** (`/a/:id/b/:id`) and **group route paths
  without a leading slash** (`api.GET("models")`) now panic at registration.
- **`Mount` is a fallback**: routes take precedence over mounts, and a longer
  prefix over a shorter one, so `Mount("/", legacy)` serves everything not yet
  ported. Registering a route under a mount used to panic. A mount serves every
  method, sets `r.Pattern` to `prefix/*`, and appears in `Routes()` once, with
  method `*`.
- Responses to a rejected request (413, 431, 503) are full RFC 9457 documents
  with `code` and `limit` fields.
- The default logger reports errors through `slog.Default` (was: silent).
