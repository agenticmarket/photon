# Benchmark results: the 2026-10 overhaul

What changed in performance when the router, server, and stream writer were
reworked in October 2026, measured against the pre-release version on the
same machine with the same benchmark code. Raw output is in
[`results-2026-10/`](results-2026-10/).

## Method

- **Machine:** AMD Ryzen 7 6800HS (8 cores, 16 threads), Windows 11, Go 1.27.1.
  A laptop: shared, thermally variable, no CPU pinning. Treat any difference
  under about 15% as noise.
- **Same code on both sides.** The benchmarks live in `perf_bench_test.go` and
  use only public API (plus `rt.Freeze`, present in both versions), so the
  identical file ran against the pre-release tree and the
  new one. A per-tree helper supplies the request context each version receives
  when served through `Serve`.
- **Interleaved.** Both test binaries were compiled once and run alternately,
  old then new: 8 rounds during development, and 6 rounds on the final code
  after the review fixes. The tables are the final run; the earlier one is
  kept as `results-2026-10/ab-round1-*.txt` and agrees within noise. An earlier sequential run (old for 15 minutes,
  then new) was dominated by thermal drift: it showed a 63% regression on the
  parallel benchmark that disappeared once the runs were interleaved. The
  sequential output is kept in `results-2026-10/sequential-*.txt` as a record
  of that mistake.
- **Fresh request per iteration.** Server benchmarks copy the request each
  iteration, as `net/http` hands every request a new `*http.Request`. Reusing
  one request would hide per-request costs. The copy costs one allocation in
  both versions; the allocation counts below subtract it.
- **Medians**, with the full range in the raw files.

## Results

| Benchmark | Before | After | Change |
|---|---|---|---|
| `ServerStatic` — full `Server.ServeHTTP`, static route | 312 ns, 5 allocs | 141 ns, **0 allocs** | **−55%** |
| `ServerParam` — full server, two-parameter route | 681 ns, 9 allocs | 409 ns, **2 allocs** | **−40%** |
| `ServerParallel` — param route from all 16 threads | 414 ns | 221 ns | **−47%** |
| `RouterParallelStatic` — router only, 16 threads | 29.9 ns | 3.5 ns | **−88%** |
| `SSEBurst` — one stream, small events, real socket | 200 MB/s, 1 alloc/event | 314 MB/s, 0 allocs | **+57%** |
| `SSEConcurrentStreams` — 100 streams × 50 events, real sockets | 3.96 M events/s | 3.30 M events/s | **−17%** |
| `SSETokenLatency` — `Event` to bytes at the client, loopback | event never delivered | ~17 µs | fixed |
| `Register5000Routes` — startup with 5,050 routes | 7.13 s | 31 ms | **230×** |

## What explains each change

**Static and parametrised requests.** The old server attached itself to every
request's context (a request copy plus a context node), allocated a closure for
panic recovery and another for the body limit, and stored path parameters in a
fresh context (another request copy, a context node, a holder, a slice). Now
the server rides in the listener's base context, recovery is a plain `defer`,
a body-less request is not wrapped, and parameters go into the request through
`Request.SetPathValue`, whose map is the only allocation left.

**Parallel routing.** Every lookup took a `sync.RWMutex` read lock. A read
lock is a write to a shared cache line, so on 16 threads the lock was the cost.
After `Serve` freezes the router, lookups take no lock at all.

**SSE burst.** The old writer flushed from a new goroutine with a new timer on
every flush. The new one has one writer goroutine per stream, so the producer
fills the buffer while the previous write is on the wire, and nothing is
allocated per event.

**Concurrent streams, −17%.** This is the one regression, and it is the cost of
correctness rather than an inefficiency. The old writer held every event until
16 KB had accumulated or the stream closed, so a 50-event burst left in one
write at close. That is why it measured well — and why a real token stream
arrived all at once at the end (`SSETokenLatency`: never delivered). The new
writer sends as soon as the connection is free, which for a burst means a few
writes per stream instead of one. A model producing tokens at human-visible
speed sends one write per token under either design; only now do they arrive.

**Token latency.** The defect this overhaul was started for. The old benchmark
fails: the first event is not delivered within 3 seconds.

**Registration.** Each registration re-parsed every static route to check for
conflicts, which is quadratic. Static routes are now checked only against
wildcards, and wildcards against pre-parsed statics.

## Cross-framework streaming, after the overhaul

`bench/stream`'s driver (100 events per stream, 5 streams per connection, 2
repetitions), run on the new code against the same servers as before. Raw
output: [`stream/results-streaming-2026-10.txt`](stream/results-streaming-2026-10.txt).

| Server | 1 stream, events/s | 10 streams, events/s | RSS at 10 streams |
|---|---|---|---|
| **Photon** (default) | 476 k | 1.84 M | 18 MB |
| Photon (512 B buffer) | 156 k | 0.95 M | 18 MB |
| chi | 133 k | 0.70 M | 21 MB |
| net/http | 105 k | 0.71 M | 17 MB |
| gin | 118 k | 0.63 M | 18 MB |
| echo | 119 k | 0.63 M | 16 MB |
| fastify / express / node:http | 64–79 k | 0.24–0.26 M | 55–71 MB |

The pre-overhaul run of the same driver showed a similar lead (1.79 M at 10
streams), but it came from holding events until 16 KB accumulated, which is
why tokens never arrived during a stream. The lead now comes from the writer
goroutine: the handler keeps producing while the previous write is on the
wire, and whatever piled up in that time goes out in one write. Each event
still leaves as soon as the connection is free (`SSETokenLatency`, ~17 µs).
The competitors write and flush once per event. The single-stream median is
noisy (best-to-median spread ~100%); the 10-stream figure is stable.

## What these numbers do not say

They do not say Photon is faster than `net/http` end to end. Over a real
socket, the HTTP parser and the kernel dominate, and the router is a few percent
of CPU; see [BENCHMARKS.md](../BENCHMARKS.md) for the cross-framework
measurements, where most workloads tie within this host's noise floor. The improvements above are
in what Photon adds on top of `net/http`, which is now very little.

## Reproducing

The pre-release version is not published, so the "before" column cannot be
rebuilt from this repository. The "after" column and the method can: the
benchmarks are in `perf_bench_test.go`, and an interleaved comparison of any
two versions (say, a release and your branch) looks like this:

```bash
go test -c -o after.test .                      # your branch
(cd ../photon-release && go test -c -o before.test .)
for i in 1 2 3 4 5 6 7 8; do
  ../photon-release/before.test -test.run '^$' -test.bench . -test.benchmem >> before.txt
  ./after.test                  -test.run '^$' -test.bench . -test.benchmem >> after.txt
done
benchstat before.txt after.txt
```
