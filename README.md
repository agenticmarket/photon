# Photon

**The streaming-first Go server for AI agents.**

[![CI](https://github.com/agenticmarket/photon/actions/workflows/ci.yml/badge.svg)](https://github.com/agenticmarket/photon/actions/workflows/ci.yml)
[![Go Reference](https://pkg.go.dev/badge/github.com/agenticmarket/photon.svg)](https://pkg.go.dev/github.com/agenticmarket/photon)
[![License: Apache-2.0](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](LICENSE)
![Go 1.24+](https://img.shields.io/badge/go-1.24%2B-00ADD8.svg)
![Dependencies: 0](https://img.shields.io/badge/dependencies-0-brightgreen.svg)

Tokens arrive as they're generated, the model stops when the user leaves, and
no client can run you out of memory. Secure limits on by default. Plain
`net/http` underneath, zero dependencies, Apache-2.0.

**[Website](https://photon.agenticmarket.dev)** ·
**[Getting started](docs/getting-started.md)** ·
**[Docs](docs/README.md)** ·
**[Benchmarks](BENCHMARKS.md)** ·
**[Examples](examples/)** ·
**[For evaluators](#for-teams-evaluating-photon)**

```go
package main

import (
	"net/http"

	"github.com/agenticmarket/photon"
)

func main() {
	app := photon.New()

	app.POST("/chat", photon.SSE(func(s *photon.Stream, r *http.Request) error {
		var req ChatRequest
		if err := photon.DecodeJSON(r, &req); err != nil {
			return err // nothing sent yet, so the client gets a clean 400
		}
		for tok := range model.Stream(r.Context(), req) { // cancelled when the client leaves
			if err := s.Event("token", []byte(tok)); err != nil {
				return err // client gone: stop the model, stop paying for it
			}
		}
		return s.Event("done", nil)
	}))

	app.Run(":8080") // graceful shutdown on SIGINT/SIGTERM, built in
}
```

## Why Photon exists

Every AI backend ends up hand-writing the same streaming endpoint, and the
hand-written version tends to break in the same places:

| What goes wrong | What Photon does |
|---|---|
| Tokens sit in a buffer and arrive in one lump at the end | Each event is sent the moment it is written, ~17 µs to the client |
| The user closes the tab; the model keeps generating and billing | `r.Context()` is cancelled and the next write returns `ErrClientGone` |
| A slow or stalled client makes the server buffer the whole answer | Bounded per-stream buffers with back-pressure; stuck clients are cut after a deadline |
| A proxy kills the connection while the model thinks | Heartbeats every 15 s of silence, automatically |
| An error after the first token has nowhere to go | Errors before streaming are HTTP errors; after, an `error` event; never a fake success |
| A deploy cuts answers in half | `Run` drains; `Stream.Done()` lets endless streams say goodbye and exit |
| Model output injects SSE fields (`\r` in a token) | Data is framed so it cannot start a field; bad names and ids are refused |
| Thousands of connections exhaust memory | `MaxStreams` derived from the container's memory limit; 503 with `Retry-After` when full |

None of this needs configuration. It is what `photon.SSE` does.

## Get started

```bash
go get github.com/agenticmarket/photon
```

Go 1.24 or newer. Nothing else is downloaded: the module has no dependencies.

Or scaffold a working project — a streaming agent with a browser UI — in one
command:

```bash
go install github.com/agenticmarket/photon/cmd/photon@latest
photon init my-agent -template agent   # also: openai-proxy, api, minimal
cd my-agent && go mod tidy && go run .
```

### The basics

```go
app := photon.New()

app.GET("/users/:id", func(w http.ResponseWriter, r *http.Request) {
	photon.JSON(w, http.StatusOK, map[string]string{"id": photon.PathParam(r, "id")})
})

v1 := app.Group("/v1", requireAPIKey)          // middleware for a subtree
v1.POST("/chat/completions", chatCompletions)   // OpenAI-compatible streaming
v1.GET("/models", listModels)

app.Mount("/mcp", mcpHandler)                   // any http.Handler, every method and subpath

log.Fatal(app.Run(":8080"))
```

**[Getting started →](docs/getting-started.md)** — from `go get` to a tested
streaming endpoint in about ten minutes.

## What you get

**Streaming**
- `photon.SSE` — the handler adapter: write events, return errors, done
- `Event`, `JSON`, `Comment`, `SetEventID`/`Last-Event-ID` resume, `SetRetry`
- NDJSON and raw byte streams through the same bounded writer (`io.Writer`)
- Overflow policies: back-pressure (default), drop-oldest for metrics, grow within a budget
- Safe for concurrent writers — parallel tool calls can report into one stream
- Works over HTTP/1.1 and HTTP/2

**Lifecycle**
- `Run(addr)`: listen, handle signals, drain, exit — sized for Kubernetes' 30 s grace period
- `Stream.Done()` fires on client disconnect *and* on shutdown
- Panics recovered and logged; a panic mid-stream aborts the connection instead of faking success

**Security defaults** — all on, all adjustable
- 1 MiB body limit, 16 KiB headers, 100 header fields, 8 KiB per header value
- Slowloris protection (`ReadHeaderTimeout`), idle connection reaping
- Error bodies never reflect request content; `nosniff` on every response helper
- `ClientIP` ignores spoofable `X-Forwarded-For`; routing on the raw path, so `%2F` cannot bypass a route
- Ambiguous routes panic at startup instead of becoming authorization bugs
- `DecodeJSON` refuses non-JSON content types (a CSRF defence) and trailing data

**Plain net/http**
- Handlers are `http.HandlerFunc`; middleware is `func(http.Handler) http.Handler`
- `r.PathValue("id")` and `r.Pattern` work, so `ServeMux` code and OpenTelemetry route labels just work
- Delete Photon and your handlers still compile

## Where it fits

**Use Photon for** AI and agent backends, OpenAI-compatible gateways and
proxies, MCP servers over HTTP, notification and progress feeds, and any JSON
API that wants safe defaults without a framework.

**Look elsewhere if** you need WebSockets in the box (use
[`coder/websocket`](https://github.com/coder/websocket) alongside — it works on
any `http.Handler`), server-rendered templates and sessions, regex or optional
route segments, or the absolute maximum requests per second on a hello-world
benchmark (Photon is `net/http` speed, not `fasthttp` speed).

| | net/http | chi | gin | echo | Fiber | **Photon** |
|---|---|---|---|---|---|---|
| Handler type | `http.Handler` | `http.Handler` | `*gin.Context` | `echo.Context` | `*fiber.Ctx` | **`http.Handler`** |
| Existing net/http middleware | ✓ | ✓ | via adapter | via adapter | via adapter | **✓** |
| HTTP/2 | ✓ | ✓ | ✓ | ✓ | ✗ (fasthttp) | **✓** |
| Third-party dependencies | 0 | 0 | many | several | several | **0** |
| Bounded SSE with back-pressure, heartbeats, drain | – | – | – | – | – | **✓** |
| Limits on by default (body, header count) | – | – | – | – | body only | **✓** |

Coming from one of these? The [migration guides](docs/migration/README.md) map
every concept, with before/after code. You do not have to move everything at
once: `app.Mount("/", existingRouter)` keeps your current app serving every
route Photon does not, so you can move one endpoint at a time.

## For teams evaluating Photon

The questions a platform, security or architecture review asks, answered with
what is in this repository.

| | |
|---|---|
| **License** | [Apache-2.0](LICENSE), including its patent grant. Free for commercial and closed-source use. No CLA. |
| **Supply chain** | The library imports only the Go standard library. CI fails if a dependency is added, and runs `govulncheck` and `staticcheck` on every change. |
| **Lock-in** | Handlers are `http.HandlerFunc`, middleware is `func(http.Handler) http.Handler`, and path values use the standard `r.PathValue`. Remove Photon and your handlers still compile. |
| **Compatibility** | Go 1.24+; HTTP/1.1 and HTTP/2; TLS through `crypto/tls` (`ListenTLS`, or your own `http.Server`); any `net/http` middleware; OpenTelemetry route labels via `r.Pattern`. |
| **Security defaults** | Body, header-count and header-size limits on by default; slowloris protection; encoded-path bypasses refused; SSE injection prevented; errors never echo request content. Details: [security guide](docs/guides/security.md). |
| **Resource safety** | Per-stream buffers are bounded and the number of open streams is derived from the container's memory limit (cgroup v2, `GOMEMLIMIT`). When full, Photon answers 503 with `Retry-After` instead of running out of memory. |
| **Operations** | `Run` handles SIGINT/SIGTERM and drains within a Kubernetes grace period; structured logs through `log/slog`; a panic hook (`OnPanic`) for your error tracker; [RFC 9457](https://www.rfc-editor.org/rfc/rfc9457) error responses with [documented codes](https://photon.agenticmarket.dev/errors/). |
| **Testing** | Full suite under the race detector on Linux, macOS and Windows, on the oldest and newest supported Go; fuzzing of the router and the SSE encoder in CI; regression tests for every fixed defect. |
| **Performance claims** | Every number is reproducible from this repository, with method, raw output and the benchmarks Photon does *not* win: [BENCHMARKS.md](BENCHMARKS.md). |
| **Vulnerability handling** | Private reporting; acknowledgement within 3 working days, a fix or mitigation plan within 14. See [SECURITY.md](SECURITY.md). |
| **Stability** | Pre-1.0: the API may still change, and every change is listed in [CHANGELOG.md](CHANGELOG.md) with what to do about it. Semantic versioning. |
| **Maintainer** | [AgenticMarket](https://agenticmarket.dev). |

## Performance

Photon is **not** faster than `net/http` end to end, and does not claim to be:
the HTTP parser and the kernel dominate a request, and Photon keeps
`net/http`'s parser on purpose. What it does guarantee is that it adds almost
nothing on top:

| Measured (Go 1.27, Windows, Ryzen 7 6800HS) | |
|---|---|
| Static route, full server | 141 ns, **0 allocations** |
| Parametrised route, full server | 409 ns, 2 allocations |
| Router on 16 cores in parallel | 3.5 ns per lookup, lock-free |
| Token latency, `Event` to client over loopback | ~17 µs |
| SSE burst throughput, one stream | 314 MB/s, 0 allocations per event |
| Registering 5,000 routes | 31 ms |

For streaming, Photon delivers about **2.6× the events per second of
`net/http`** at 10 concurrent streams, and 7× Node.js; for ordinary requests it
ties `net/http`, chi, gin and echo. Every number, the method, and the one
benchmark that got slower are in **[BENCHMARKS.md](BENCHMARKS.md)**.

## Documentation

| | |
|---|---|
| [Getting started](docs/getting-started.md) | From `go get` to a streaming endpoint in ten minutes |
| [Guides](docs/README.md) | Routing, middleware, streaming, AI backends, security, configuration, deployment, testing |
| [Migration](docs/migration/README.md) | From net/http, chi, gin, echo, Fiber, FastAPI, Express |
| [API reference](docs/API.md) | Every exported symbol |
| [Examples](examples/) | Runnable programs, including an OpenAI proxy and an agent UI |
| [Benchmarks](BENCHMARKS.md) | Every published number, how it was measured, and how to reproduce it |
| [Error codes](https://photon.agenticmarket.dev/errors/) | What each error response means, for clients and operators |

## Status

**Pre-1.0.** The API is settling and may still change; every change is listed
in [CHANGELOG.md](CHANGELOG.md). Requires Go 1.24 or newer; CI runs the full
test suite with the race detector on Linux, macOS, and Windows, on the minimum
supported Go version and the latest release.

## Contributing and security

Contributions are welcome — see [CONTRIBUTING.md](CONTRIBUTING.md). Report
vulnerabilities privately as described in [SECURITY.md](SECURITY.md).

## License

Photon is open source under the [Apache License 2.0](LICENSE): free to use,
modify, and ship in commercial and closed-source products. Built and maintained
by [AgenticMarket](https://agenticmarket.dev).
