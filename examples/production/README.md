# `production` — running a server for real

The parts that are not the handler: lifecycle, health checks, timeouts, and logs
worth reading.

```bash
go run ./examples/production
```

```bash
curl http://localhost:8080/healthz
# {"status":"ok","pid":12345}

curl http://localhost:8080/readyz
# {"status":"ready"}

curl http://localhost:8080/work
# {"ok":true,"took":"104.7476ms","stream":"for a long response, see examples/stream"}

# Ctrl-C, then immediately, from another terminal:
curl --max-time 1 http://localhost:8080/readyz
# {"status":"draining","reason":"shutting down, not accepting new streams"}
```

## Liveness and readiness are different questions

This is the most consequential pair of endpoints in the example, and getting it
wrong causes outages that have nothing to do with your code.

| Endpoint | Question | While draining |
|---|---|---|
| `/healthz` | Is this process functioning? | **200** |
| `/readyz` | Should traffic be sent here? | **503** |

A load balancer polling `/readyz` takes a draining instance out of rotation. A
liveness probe polling `/healthz` leaves it alone until the orchestrator decides
otherwise — and that is the point: **a draining server is still alive.** It is
finishing work it already accepted. If `/healthz` returned non-200 while draining,
an orchestrator would kill the process mid-response, and the client would see a
truncated answer for no better reason than impatience.

Both check nothing external. A liveness probe that depends on a database gets the
container killed during a database blip, turning a recoverable dependency failure
into an outage.

## The shutdown ordering that cannot express the bug

```go
serveErr := make(chan error, 1)
go func() { serveErr <- app.Listen(address) }()   // may start first

ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
defer stop()

select {
case err := <-serveErr:
    return err        // Listen returned on its own: a bind failure
case <-ctx.Done():
}
```

The tempting alternative looks equivalent:

```go
go func() { app.Shutdown(ctx) }()   // may run FIRST
app.Listen(":8080")                 // binds, then immediately stops
```

That goroutine can run before `Listen` has bound the socket, and the server exits
on startup. It is not a photon bug and the race detector cannot see it. It is the
most common way to write this pattern wrong, which is why every example in this
repo shows the ordering that cannot express it.

## The drain window is real but short

Between the signal and `Shutdown` completing, `/readyz` returns 503. Measured on
this machine that window is a few milliseconds for a request that takes 100 ms,
which is correct behaviour and almost impossible to observe by hand — a
`curl` in a loop will usually miss it. That is not a defect; a longer window would
mean a slower shutdown.

If you need to see it, hold a request open (a slow client, or a deliberately slow
handler) so `Shutdown` has something to wait for.

## Timeouts, and why `ReadTimeout` is zero

```go
ReadHeaderTimeout: 5 * time.Second,
IdleTimeout:       60 * time.Second,
// ReadTimeout: 0, deliberately
```

`ReadTimeout` covers the whole request, headers and body together. The deadline is
absolute, so on a five-minute AI response it fires mid-stream no matter how
healthy the connection is. **A whole-request deadline is what makes long responses
impossible**, so photon leaves it at zero and offers per-route body deadlines
instead.

`ReadHeaderTimeout` is different and is on: it bounds the Slowloris attack, where
a client trickles headers and holds a connection and a goroutine open.

## Per-request deadlines come from the context

```go
ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
defer cancel()
select {
case <-ctx.Done():
    w.WriteHeader(499)   // nginx's "client closed request"
case <-time.After(100 * time.Millisecond):
}
```

Two things here are deliberate.

**The deadline derives from `r.Context()`**, so a client that gives up cancels the
work rather than leaving it running to write into a dead socket. Without the
parent, a client that disconnects leaves the handler running to completion — the
single most common way a server wastes work nobody will read.

**499 is nginx's non-standard code for "client closed request".** Using it is a
convention worth stating: a 499 is not a server error and should not be counted
as one, but it is not a 4xx the client can be blamed for either, because the
client is gone. Counting it as a 5xx makes your error rate a function of your
users' network.

## JSON logs, because something other than a person reads them

```go
photon.Logger(slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
    Level: slog.LevelInfo,
})))
```

One line, and the highest-value logging decision in the file. Every other example
uses a `TextHandler` because a human reads those.

**Log durations as strings.** slog's JSON handler renders a `time.Duration` as a
bare integer of nanoseconds, so a 337 ms request logs as `"took":337845100`.
Unreadable durations are how latency regressions go unnoticed.

```go
"took", time.Since(start).Round(time.Microsecond).String()
```

`draining` is an `atomic.Bool`, not a plain one. It is written once by the signal
goroutine and read by every health check, and a plain bool there is a data race
the detector would flag and that is a real bug under load.

## What to add before this is production

Honest list:

1. **Real log shipping.** This writes JSON to stdout and relies on the platform.
2. **Tracing.** A request id is not a trace; nothing propagates W3C `traceparent`.
3. **A readiness check that means something.** Here it only knows about draining.
   A real one should also check that the upstream is reachable.
4. **`MaxPerIP`.** photon offers it; this example leaves it to the infrastructure.
5. **TLS**, at a proxy. `ListenTLS` exists and this uses plain HTTP so the example
   runs with no certificate setup.
6. **A load balancer** that actually polls `/readyz`. Without one, the endpoint is
   decoration.

## See also

- `../backend` — these lifecycle concerns plus auth, quotas, metrics, and a
  Dockerfile
- `examples/security` — the limits this example sets and the ones it leaves at zero
