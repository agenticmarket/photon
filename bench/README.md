# bench/

The benchmark harnesses behind [BENCHMARKS.md](../BENCHMARKS.md). The results
and how to read them are there; this directory is how to reproduce them.

| Path | What it measures |
|---|---|
| `baseline/` | Ordinary HTTP requests across `net/http`, Photon, chi, gin and echo, over loopback TCP with a hand-written client. Its own Go module. |
| `stream/` | Server-sent events across Go and Node.js servers, with a byte-equivalence check before any number is taken. Its own Go module. |
| `RESULTS-2026-10.md` | The before/after comparison for the October 2026 rework, and its method. |
| `results-2026-10/`, `results-matrix-2026-10.txt`, `stream/results-streaming-2026-10.txt` | Raw output behind the published tables. |

In-process microbenchmarks of the router, server and stream writer live next
to the code, in the repository root (`*_bench_test.go`, `perf_bench_test.go`).

## Running

```bash
# Microbenchmarks (repository root)
go test -run '^$' -bench . -benchmem .

# HTTP across frameworks
cd bench/baseline && go run . matrix

# Streaming across frameworks (see stream/README.md for the Node servers)
cd bench/stream && go build -o bin/ ./go_servers/... && go build -o bin/streamdriver ./driver
./bin/streamdriver -streams 1,10 -events 100 -per-worker 5 -reps 2
```

## Rules the harnesses follow

- **In-process numbers are never reported as requests per second.** They
  measure Photon's own cost, not a server.
- **Repetitions, and medians.** Run-to-run spread on a laptop is often larger
  than the difference between frameworks; `baseline` computes that floor from
  its own result files (`TestMeasurementFloorIsPublished`).
- **Identical work.** Every framework returns the same bytes, checked by a test
  before anything is timed, so no server can look fast by doing less.
- **Interleave before/after comparisons**, alternating the two binaries, so
  thermal drift does not masquerade as a regression.
