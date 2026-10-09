# Streaming audit — SSE across Go, Node, and Python

Measures photon's SSE streaming against the mainstream alternative in each
language, using the same discipline as the rest of the audit: one client, one
machine, identical response bytes, and a failure reported rather than omitted.

**Raw output: [`results-streaming.txt`](results-streaming.txt).** Every number in
any summary of this comes from that file and from no other run.

## Read this before the numbers

**A cross-language benchmark mostly measures the language.** CPython's GIL,
libuv's event loop, and Go's goroutines differ by more than the frameworks do.
The tables below are therefore framed as *photon against the best of each
ecosystem*, not as *photon versus FastAPI*. Node and Python are slower here, and
most of that gap is the runtime rather than the framework — which is exactly what
the floor rows make visible.

**The floors are the point.** `net/http`, `node:http`, and raw ASGI are in every
table with no framework at all. Without them, "photon versus FastAPI" compares two
entire stacks. With them, the cost of the framework layer is separable from the
cost of the runtime, and a framework that cannot beat its own language's floor is
making a statement worth knowing.

## What was measured, and what it does not show

Workload: 500 SSE events per stream, 65 bytes each, 20 streams per connection,
at 1, 10, and 50 concurrent streams. Metrics are events/s (median and best of 3
repetitions, with the spread), median time to first event, and the server
process's own peak RSS.

These are **not** model-provider numbers. A real provider's latency and token
cadence sit between the producer and the server, and any measurement through one
measures mostly the provider, so it is not attempted here.

## Fairness: identical bytes, verified

Every server emits byte-identical frames — the same 325 bytes for a 5-event
stream, the same headers a client can observe. This is not assumed; it is
asserted before every run:

```
.\bin\equivcheck.exe          # or: go run ./driver/equivcheck
```

A mismatch **fails the run** rather than being reported. A throughput table
comparing different responses measures nothing, and a table that quietly
contains one is worse than no table.

The gate has caught real faults in the harness itself, twice: the raw ASGI server
originally parsed the event count from `scope["path"]` when ASGI delivers the
query string separately, so it streamed the right bytes 50 times over; and the
competitor servers originally wrote a literal header block as their first write,
which made `net/http` treat it as body and emit 478 bytes where photon emitted
325. Neither was visible without the gate.

## Layout

```
sse/            the frame every server must emit, and the count/memory helpers
gosrv/          shared handler + listener handshake for the Go servers
go_servers/     photon, chi, gin, echo, net/http
node/server.js  fastify, express, node:http
python/server.py fastapi, starlette, raw ASGI (uvicorn)
driver/         the measurement client
driver/equivcheck/  the byte-equivalence gate
```

### Why one Node file and one Python file

`node/server.js <framework>` and `python/server.py <framework>` each serve every
framework in their language from a single process. Written as separate scripts
they would drift — a different header here, a different flush there — and a
difference in the tables could not be attributed. The floors are selected the
same way, which also makes "no framework" a real control rather than a
description.

## Running it

```powershell
# One-time setup
.\build.ps1                                            # Go servers + driver
cd node; npm install; cd ..
cd python; python -m venv .venv
.venv\Scripts\python -m pip install -r requirements.txt; cd ..

# The fairness gate, then the measurement
.\bin\equivcheck.exe
.\bin\streamdriver.exe -streams 1,10,50 -events 500 -per-worker 20 -reps 3
```

`build.ps1` is required before `go test ./gosrv/` — the equivalence test spawns
the built binaries and skips if they are absent.

## Methodology notes

These are the decisions that changed the numbers, recorded because each one
produced a wrong measurement first.

**Fixed stream count, not a time window.** The driver originally ran for a fixed
*duration*. That couples throughput to connection churn: a faster server finishes
its work sooner and reconnects more often, and every chunked stream ends its
connection, so photon reconnected roughly 900 times a second. Windows holds each
closed port in TIME_WAIT for two minutes against a 16384-port range, so the run
exhausted its own ports and every server after that point failed to accept
connections — a failure that depended on run order, which is the signature of a
harness fault rather than a server property. The driver now runs a fixed number
of streams, so every server does identical work and the comparison is fair by
construction.

**Connections are reused across streams.** HTTP/1.1 keep-alive means a complete
chunked response leaves the connection ready for the next request. Redialing after
every stream — also a bug here, in that same first version — burned one ephemeral
port per stream. Reuse cuts the ports consumed by a 1000-stream run from ~1000 to
~100. Connections that a server has genuinely closed are replaced, so a server
that drops a stream is reported rather than silently retried.

**`SO_LINGER(0)` was tried and removed.** Resetting connections instead of closing
them gracefully was an attempt to sidestep TIME_WAIT. It does avoid TIME_WAIT, but
an RST arriving in a server's accept loop crashed uvicorn on Windows
(`WinError 64` in `proactor_events`), which is not a fair way to measure
something. Connection reuse fixed the underlying problem properly.

**photon is measured in two flush configurations, and both are reported.** The
default 64 KiB stream buffer flushes at 16 KiB, so it coalesces roughly 264 token
frames per write; a 512-byte buffer flushes per frame. A single figure would
either flatter photon or misrepresent it, and the gap between the two rows is
the real cost of coalescing.

**The raw client, not `net/http`.** `net/http`'s client buffers a chunked response
until the body ends, so it cannot observe time-to-first-event at all — the metric
that matters most for streaming. It would also add its own pool and per-request
allocations to every measurement.

**TTFE is pooled across repetitions, not averaged per repetition.** A median of
medians is not the median. At three repetitions the difference is large enough to
change the reading.

**Spread is reported, and it is high.** Node in particular varies by 30–50% between
repetitions on this host. That is large enough that single-run Node numbers should
not be quoted, and it is the reason the table reports median, best, and spread
together rather than a single column.

## Known limitations

- **The client and the server share a machine and a CPU.** Every figure is
  therefore a lower bound on what the server alone could do. The HTTP harness in
  `bench/baseline` has the same limitation.
- **Loopback TCP, HTTP/1.1.** No TLS, no HTTP/2, no reverse proxy. nginx buffering
  is the single most common cause of "my token stream hangs", and this harness
  cannot observe it.
- **One machine, one run.** These are not portable numbers. Re-run before quoting.
- **`asgi` reports 0 for `events` and one Python RSS reading was once 0** — both
  were harness bugs, fixed and described above, and both are the reason the
  equivalence gate and the in-process `/mem` endpoint exist.
