# Benchmarking streams with `photon bench`

Generic HTTP load tools — `wrk`, `hey`, `ab` — measure whole responses. For a
stream, that number says nothing about what a user sees: how long until the
first token, and whether tokens then arrive steadily or in bursts. `photon
bench` reads a stream event by event and measures exactly that. It works
against any server that speaks SSE, not only Photon.

```bash
go install github.com/agenticmarket/photon/cmd/photon@latest
```

## Usage

```bash
photon bench [flags] <url>
```

| Flag | Default | Meaning |
|---|---|---|
| `-c` | 10 | Streams open at the same time |
| `-n` | `-c` | Total streams to run |
| `-method` | `GET` | HTTP method |
| `-d` | | Request body. Implies `POST` and `Content-Type: application/json` |
| `-H` | | A header, `"Name: value"`; repeatable |
| `-timeout` | 2m | Per-stream timeout |

```bash
# The demo service (photon serve), as fast as it can produce
photon bench -c 50 -n 500 'http://localhost:8484/agent/stream?burst=true'

# An OpenAI-compatible endpoint, with a key
photon bench -c 20 \
  -H 'Authorization: Bearer sk-...' \
  -d '{"model":"m","stream":true,"messages":[{"role":"user","content":"hi"}]}' \
  http://localhost:8080/v1/chat/completions

# Any other SSE server - FastAPI, Express, another Go framework
photon bench -c 25 http://localhost:3000/events
```

## Reading the output

```text
Streams   500 ok, 0 failed in 1.21s
Events    100,500 total, 83,057/s, 4.8 MiB received

                              p50        p90        p99        max
Time to first byte          1.12ms     3.40ms     8.85ms    12.02ms
Time to first event         1.15ms     3.42ms     8.91ms    12.03ms
Gap between events           <1µs      12µs     310µs      4.11ms
Stream duration            98.31ms   140.10ms   201.77ms   230.54ms
```

| Line | What it tells you |
|---|---|
| **Time to first byte** | Connection, routing, and handler start-up, until the response headers arrive |
| **Time to first event** | What a user feels as "it started answering". If this is much larger than first byte, the server is buffering the first event |
| **Gap between events** | The rhythm of the stream. A model streaming at 50 tokens/s should show ~20 ms gaps; gaps of `<1µs` mean events arrived together in one read, which is normal for a fast producer |
| **Stream duration** | First request byte to end of stream |
| **Events per second** | Throughput across all concurrent streams |

Errors are grouped by message at the end, so a server at its `MaxStreams`
limit shows up as a count of `HTTP 503`.

Two patterns worth recognising:

- **First event ≈ stream duration.** The server is holding events until the end.
  That is the bug `photon bench` was written to catch.
- **A large p99 gap with a small p50.** Something periodically stalls the
  stream: GC pauses, a slow upstream, or a proxy buffering in chunks.

## Measuring honestly

- **Run the load generator on a different machine** for numbers you will
  publish. On one machine, client and server compete for the same CPUs.
- **Run several times and look at the spread**, not one result. A laptop's
  numbers drift with temperature by more than most code changes.
- **Compare against a baseline in the same session**, alternating runs.
  [bench/RESULTS-2026-10.md](../../bench/RESULTS-2026-10.md) shows why: a
  15-minute run of the old code followed by the new one measured a regression
  that did not exist.
- **Know what you are measuring.** `?burst=true` measures the server's
  throughput; a paced stream measures latency under realistic token rates.

## See also

- [Streaming](streaming.md) — what makes events arrive promptly
- [bench/](../../bench/) — the repository's own benchmark harness, comparing
  Photon with net/http, chi, gin, echo, Node, and Python
