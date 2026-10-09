# `openai-backend` — an OpenAI-compatible SSE backend

A production-shaped streaming backend on photon. No third-party dependencies.

It serves `POST /v1/chat/completions` with `stream: true`, so the official OpenAI
SDK, LangChain, Continue, or a `curl` loop all work against it unchanged.

```bash
# One tenant, no upstream - the demo configuration.
cd examples/openai-backend   # a separate module, so run it from its directory
PHOTON_TENANTS="sk-demo-token-abcdef123456:acme" go run .
```

```bash
curl -N http://localhost:8080/v1/chat/completions \
  -H "Authorization: Bearer sk-demo-token-abcdef123456" \
  -H "Content-Type: application/json" \
  -d '{"model":"photon-demo","messages":[{"role":"user","content":"hello"}],"stream":true}'
```

## What is real and what is not

Stated first, because a demo that blurs this line wastes an afternoon.

| Real | Not implemented |
|---|---|
| SSE framing, back-pressure, stall deadlines, bounded buffers | Calling an actual model |
| Auth, per-tenant rate and concurrency limits | Token counting (estimated, ~4 chars/token) |
| Request validation, OpenAI error shapes | Multiple models, tool calls, function calling |
| Metrics, structured logs, health/ready, graceful drain | Persistence, billing, refunds |
| Dockerfile, non-root, static binary | TLS termination (do it at a proxy) |

**The provider is a fake.** `provider.go` defines the `Provider` interface and
ships a deterministic in-process implementation. Setting `PHOTON_UPSTREAM_URL`
**refuses to start** with a clear error rather than silently serving fake output
from a server that looks like it is proxying a real model. To wire a real one,
implement `Provider` — the interface is the contract, and the doc comment on it
names the three ways implementations go wrong.

## The seam

```
  client ──▶ photon.Server ──▶ auth ──▶ quota ──▶ Streaming() ──▶ handler
             (limits, streams,   (who)   (how much)  (bounded buffer)   │
              back-pressure)                                                 │
                                                                          ▼
                                                            Provider.Stream(ctx, req)
                                                            (token channel)
```

**photon owns** the bounded buffer, `MaxStreams`, the stall deadline,
back-pressure, heartbeats, graceful drain, panic recovery, and IP limits.
**This app owns** auth, quotas, validation, the provider call, logs, and metrics.
Neither reaches into the other.

## Routes

| Route | Auth | Quota | Purpose |
|---|---|---|---|
| `GET /healthz` | no | no | Liveness. 200 even while draining. |
| `GET /readyz` | no | no | Readiness. **503 while draining.** |
| `GET /metrics` | no | no | Prometheus exposition. |
| `POST /v1/chat/completions` | yes | yes | Chat completion, streaming or not. |
| `GET /v1/models` | yes | yes | OpenAI model list. |

Operational endpoints deliberately have no auth and no quota: a health check that
fails because the rate limiter is busy tells the orchestrator to kill a healthy
process.

## Five decisions worth defending

### 1. Quota is checked before the stream starts

Once the first token is written the status line is gone. A quota check after that
point cannot produce a 429 — it can only kill the connection. A caller over quota
would get a `200` followed by a truncated stream, which is worse than a clean
`429`: the client has begun rendering an answer that will never finish, and
cannot distinguish that from a network blip.

### 2. There are two error paths, and they look different

A failure **before** the first token is a real HTTP status. A failure **after** is
an SSE `data:` frame carrying an error object, because the status is already
committed. This is the classic OpenAI-proxy bug: a client receives a truncated
stream with no indication anything went wrong, and renders half an answer as
complete. `TestUpstreamFailureProducesErrorFrame` exists specifically to catch it,
and the terminal chunk reports `finish_reason: "error"` rather than `"stop"` so a
client is never told a failed stream finished normally.

### 3. Cancellation is the most important line in the handler

```go
tokens, upstreamErr, err := s.provider.Stream(r.Context(), req)
```

Passing the request context is what stops the *model call*, not just the write.
Without it the server keeps paying to generate output nobody will read — the most
expensive bug in this file, and the reason the provider contract requires
checking `ctx` before every send.

### 4. Token lookup is a map, and that is a deliberate trade

Constant-time comparison matters for credentials. The honest version of that
argument: a Go map lookup is not constant-time in the key, and the "correct" fix
— scanning every token with `subtle.ConstantTimeCompare` — costs O(tenants) per
request. At tens to hundreds of tenants the map is right. At thousands, or behind
a precisely timing-observable path, the scan is right. `auth.go` says so where the
decision is made.

### 5. Fixed-window rate limiting, with the known flaw stated

A fixed window means a caller can send 2× the limit across a boundary. That is
acceptable for a *fairness* control and **not** acceptable for billing, where a
sliding window or token bucket is required. The comment in `tenant.go` says so
rather than leaving a reader to assume the limit is a hard guarantee.

## Configuration

| Variable | Default | Meaning |
|---|---|---|
| `PHOTON_ADDR` | `:8080` | Listen address |
| `PHOTON_TENANTS` | *(none)* | `token:name` pairs, comma-separated |
| `PHOTON_MODEL` | `photon-demo` | The single model advertised |
| `PHOTON_MAX_STREAMS` | derived | Global stream cap; 0 = derive from memory |
| `PHOTON_TENANT_STREAMS` | `8` | Concurrent streams per tenant |
| `PHOTON_TENANT_RPM` | `60` | Requests per minute per tenant |
| `PHOTON_STREAM_BUFFER` | `65536` | Per-stream buffer bytes |
| `PHOTON_STALL_TIMEOUT` | `30s` | How long a full buffer blocks |

Bad values are **startup errors**, not warnings. A negative `PHOTON_MAX_STREAMS`
that silently meant "unlimited" would turn a typo into an unbounded server, and
the failure would appear under load rather than at boot.

**Tokens in environment variables are visible in `ps` output and in crash dumps.**
This is a demo convenience. A real deployment reads them from a secret store.

## Metrics

```
ssebackend_requests_total{route="POST /v1/chat/completions",status="2xx"} 412
ssebackend_streams_admitted_total 408
ssebackend_streams_closed_total 401
ssebackend_streams_aborted_total 7
ssebackend_tokens_streamed_total 18744
ssebackend_quota_rejected_total 3
ssebackend_upstream_errors_total 1
ssebackend_stream_duration_seconds_bucket{le="+Inf"} 401
```

`route` is the **route pattern**, never a raw path — a path containing an
identifier would otherwise create unbounded label cardinality, which is the
classic way a metrics endpoint brings down the system it is measuring.

`/metrics` is unauthenticated, which is a decision: a scraper that
authenticates is a common source of broken monitoring. **Bind it to an internal
interface or put it behind a proxy** — an exposed metrics endpoint leaks traffic
shape.

## Verifying it

```bash
cd examples/openai-backend
go test ./...              # includes the memory invariant
go test -race ./...        # the concurrency-heavy paths
docker build -f Dockerfile -t photon-sse-backend .   # from the repo root
```

The tests that matter:

| Test | What it protects |
|---|---|
| `TestQuotaReleasesSlotOnClientDisconnect` | A slot leak would lock a tenant out permanently, with no error |
| `TestUpstreamFailureProducesErrorFrame` | Silent truncation rendered as a complete answer |
| `TestSlowConsumerDoesNotGrowMemory` | The design invariant: a slow consumer never grows memory without bound |
| `TestStalledStreamEndsAndReleasesQuota` | A stalled client holding a goroutine and a slot forever |

The Dockerfile is built **from the repository root**, because
`replace photon => ..` points outside the module:

```bash
docker build -f backend/Dockerfile -t photon-sse-backend .
```

Building from `backend/` fails with a confusing `reading /go.mod: no such file`,
which reads like a broken Dockerfile rather than a wrong context.

Image: **15.3 MB**, distroless, non-root, static.

## What a real deployment still needs

Honest list, in rough order of how often it turns out to matter:

1. **A real provider** behind `Provider`.
2. **Secrets from a store**, not the environment.
3. **TLS** at a proxy, with a real certificate.
4. **A sliding-window or token-bucket limiter** if the RPM limit is billable.
5. **Prompt and completion logging**, with PII handled — this server logs no
   request content, which is right for a demo and often wrong for production.
6. **Model routing and fallbacks**; one hard-coded model is not enough.
7. **Rate limiting by IP as well as tenant** — photon offers `Limits.MaxPerIP`,
   which this app deliberately does not set, leaving it to the infrastructure.
