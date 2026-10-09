# Benchmarks

Every performance number Photon publishes, in one place: what was measured, on
what, how, and what it does and does not mean. Raw output is linked for every
table, and every benchmark can be re-run with the commands at the end.

## Summary

- **Streaming is where Photon is ahead.** At 10 concurrent streams it delivers
  about **2.6× the events per second of `net/http`** and of the fastest Go
  framework measured, and 7× Node.js, while each event still leaves within
  ~17 µs of being written.
- **Ordinary requests tie.** For JSON, parameters, uploads and downloads, Photon
  is within measurement noise of `net/http`, chi, gin and echo. Over a real
  socket the HTTP parser and the kernel dominate; Photon keeps `net/http`'s
  parser on purpose and adds almost nothing on top.
- **What Photon adds is small and measured:** 0 allocations for a static route
  through the full server, a lock-free router, and 1 extra allocation over
  `net/http` for a route with parameters.

## The machine, and how to read the numbers

| | |
|---|---|
| CPU | AMD Ryzen 7 6800HS, 8 cores / 16 threads |
| OS | Windows 11, loopback TCP, HTTP/1.1 keep-alive |
| Go | 1.27.1 |
| Node.js | 24.19 (streaming comparison) |
| Date | 2026-10-09, Photon at the 2026-10 overhaul |

This is a laptop: shared, thermally variable, no CPU pinning. **Run-to-run
variation on this host is about 10% at the median, and up to 30% for a single
framework on a single workload**, so differences inside that range are ties,
not rankings. The floor is computed from the committed result files
(`TestMeasurementFloorIsPublished` in `bench/baseline`), not chosen by hand. Every table reports the median of several repetitions, and
the raw files keep every repetition. Client and server share the machine, so
absolute numbers are lower than on a server with a separate load generator;
the comparisons are what carry over.

## 1. Streaming (SSE) across frameworks

Each server streams the same events, byte for byte (an equivalence check
runs before measuring). 100 events per stream, 5 streams per connection, 2
repetitions. Raw: [`bench/stream/results-streaming-2026-10.txt`](bench/stream/results-streaming-2026-10.txt).

| Server | 1 stream, events/s | 10 streams, events/s | Memory (RSS) at 10 streams |
|---|---|---|---|
| **Photon** (default) | **476 k** | **1.84 M** | 18 MB |
| Photon (512 B buffer) | 156 k | 0.95 M | 18 MB |
| chi | 133 k | 0.70 M | 21 MB |
| net/http | 105 k | 0.71 M | 17 MB |
| gin | 118 k | 0.63 M | 18 MB |
| echo | 119 k | 0.63 M | 16 MB |
| fastify (Node) | 79 k | 0.26 M | 71 MB |
| express (Node) | 76 k | 0.24 M | 66 MB |
| node:http (Node) | 64 k | 0.26 M | 55 MB |

**Why Photon is ahead, and why that is not cheating.** The other servers write
and flush once per event. Photon hands each event to a per-stream writer
goroutine: if the connection is free, the event goes out immediately; if a
write is already on the wire, events written meanwhile go out together in the
next one. Nothing is held back waiting for more — that is checked separately
below. The "512 B buffer" row shows how much of the gap is this batching:
with almost no room to batch, Photon is still ahead of the per-event servers,
but by less.

The single-stream medians are noisy (best and median differ by up to 2× for
Photon and Node); the 10-stream figures are much steadier.

## 2. Token latency

| | Photon before the overhaul | Photon now |
|---|---|---|
| `Stream.Event` → bytes readable by the client, loopback | event never delivered (held until 16 KB or close) | **~17 µs** |

This is the number that matters to a user watching an answer appear. It is
measured by `BenchmarkSSETokenLatency` in
[`perf_bench_test.go`](perf_bench_test.go). Before the October 2026 overhaul,
Photon led the streaming table above *because* it held events back — which also
meant a real token stream arrived in one lump at the end. The benchmark now
guards against that regression.

## 3. Ordinary HTTP requests across frameworks

128 connections, 2 s warm-up, 5 s × 3 repetitions per cell; medians in req/s.
Every framework returns identical bytes (checked by
`TestFrameworksRespondIdentically`). Raw: [`bench/results-matrix-2026-10.txt`](bench/results-matrix-2026-10.txt).

| Workload | net/http | **Photon** | chi | gin | echo |
|---|---|---|---|---|---|
| `GET /` plain text | 236 k | **240 k** | 233 k | 243 k | 239 k |
| `GET /health` JSON | 228 k | **254 k** | 251 k | 257 k | 278 k |
| `GET /users/:id` param | 263 k | **292 k** | 254 k | 291 k | 286 k |
| `GET` 1 KB response | 274 k | **266 k** | 264 k | 243 k | 263 k |
| `GET` 10 KB response | 209 k | **209 k** | 197 k | 146 k | 189 k |
| `GET` 100 KB response | 104 k | **105 k** | 106 k | 40 k | 100 k |
| `POST` 1 KB body | 128 k | **132 k** | 125 k | 128 k | 131 k |
| `POST` 100 KB body | 62 k | **66 k** | 64 k | 64 k | 69 k |
| `POST` 1 MB body | 7.1 k | **6.7 k** | 6.8 k | 6.8 k | 6.6 k |
| `GET /` 204 no-op | 227 k | **234 k** | 239 k | 231 k | 247 k |

**Read this as a tie.** Almost every gap is inside the noise floor, and
the order changes between runs. The two real differences are gin on large
responses (its `c.String` path copies the payload: 107 KB allocated per 100 KB
response) and the 1 MB upload row, which shows that at that size every
framework is waiting on the socket, not on itself.

Process-wide allocations per request (client included) are the same as
`net/http` for every workload except the parameter route, where Photon is +1
(33 vs 32): the map that holds path values. chi is +3.

## 4. What Photon adds on top of `net/http`

In-process microbenchmarks of the server and router alone, no socket. Raw:
[`bench/results-2026-10/`](bench/results-2026-10/).

| Measured | Result |
|---|---|
| Static route, full `Server.ServeHTTP` | 141 ns, **0 allocations** |
| Route with two parameters, full server | 409 ns, 2 allocations |
| Parameter route, all 16 threads in parallel | 221 ns |
| Router lookup, 16 threads in parallel | 3.5 ns, lock-free |
| Static lookup with 10 to 10,000 routes | flat, ~40 ns |
| SSE burst, one stream over a real socket | 314 MB/s, 0 allocations per event |
| Registering 5,000 routes at startup | 31 ms |

## 5. Before and after the October 2026 overhaul

The same benchmark code was run against the pre-release version and
the current one, alternating between the two binaries to cancel out thermal
drift. Full write-up: [`bench/RESULTS-2026-10.md`](bench/RESULTS-2026-10.md).

| Benchmark | Before | After | Change |
|---|---|---|---|
| Static request, full server | 312 ns, 5 allocs | 141 ns, 0 allocs | −55% |
| Parameter request, full server | 681 ns, 9 allocs | 409 ns, 2 allocs | −40% |
| Parameter request, 16 threads | 414 ns | 221 ns | −47% |
| Router lookup, 16 threads | 29.9 ns | 3.5 ns | −88% |
| SSE burst, one stream | 200 MB/s | 314 MB/s | +57% |
| 100 concurrent burst streams | 3.96 M events/s | 3.30 M events/s | **−17%** |
| Token latency | never delivered | ~17 µs | fixed |
| Registering 5,000 routes | 7.13 s | 31 ms | 230× |

The −17% is the price of sending events immediately: the old writer won that
benchmark by holding 50-event bursts until the stream closed.

## What these numbers do not say

- **Not "faster than `net/http`."** Photon *is* `net/http`. For ordinary
  requests it can only tie; the tables show that it does.
- **Not production capacity.** Loopback on a laptop, with client and server on
  one machine. Your network, TLS, proxies and handler code will dominate.
- **Not a ranking of the other frameworks.** chi, gin and echo are measured with
  their idiomatic code, and the gaps between them are mostly noise.
- **Not `fasthttp` speed.** Fiber and other `fasthttp`-based servers are not in
  these tables; they trade `net/http` compatibility and HTTP/2 for raw request
  rate, which is a different choice from Photon's.

## Reproduce

```bash
# Micro: router, server, stream writer (this repository)
go test -run '^$' -bench . -benchmem .

# HTTP across frameworks (bench/baseline, its own module)
cd bench/baseline && go run . matrix

# Streaming across Go and Node servers (bench/stream, its own module)
cd bench/stream && ./build.ps1          # or: go build the servers into bin/
cd node && npm ci && cd ..
./bin/streamdriver -only "photon,photon (tight),net/http,gin,echo,chi,fastify,express,node:http" \
  -streams 1,10 -events 100 -per-worker 5 -reps 2
```

To measure your own streaming endpoint — first-token time and gaps between
tokens, against any SSE server — use [`photon bench`](docs/guides/benchmarking.md).

How the harnesses work, and the rules they follow, is in
[`bench/README.md`](bench/README.md).
