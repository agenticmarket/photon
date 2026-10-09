# `stream` — server-sent events, end to end

```bash
go run ./examples/stream
```

Every route is a `photon.StreamFunc` wrapped in `photon.SSE`: write events,
check the error, return. The guide behind it is
[docs/guides/streaming.md](../../docs/guides/streaming.md).

| Route | Shows |
|---|---|
| `/tokens` | Token-by-token delivery and stopping when the client leaves |
| `/resumable` | Event ids and `Last-Event-ID`, so a reconnect resumes instead of restarting |
| `/metrics?every=200ms` | An endless feed with `DropOldest`, ended cleanly on shutdown with `s.Done()` |
| `/slow-consumer` | Back-pressure, and disconnecting a client that stops reading |
| `/fail` | An error after the stream started, delivered as an `error` event |

## `/tokens`

```bash
curl -N localhost:8080/tokens
```

```text
event: token
data: Photon

event: token
data:  sends
...
event: done
data:
```

Each event arrives as it is written. Close curl halfway: the next `s.Event`
returns `photon.ErrClientGone` and the handler returns. In a real handler,
`r.Context()` passed to the model call is cancelled at the same moment, so the
model stops too.

## `/resumable`

```bash
curl -N localhost:8080/resumable                       # Ctrl-C after a few tokens
curl -N -H 'Last-Event-ID: 5' localhost:8080/resumable # resumes at token 6
```

`s.SetEventID` attaches an id to the next event, in the same frame. A browser's
`EventSource` sends `Last-Event-ID` on reconnect by itself. The header is client
input, so the handler parses and bounds it before trusting it.

## `/metrics`

```bash
curl -N 'localhost:8080/metrics?every=200ms'
```

A feed that never ends on its own. Two things to notice:

- It uses `photon.WithOverflow(photon.DropOldest)`. A client that falls behind
  loses old values and stays current, which is right for metrics and wrong for
  text.
- It selects on `s.Done()`, which closes when the server starts a graceful
  shutdown. Press Ctrl-C on the server: the client receives a final `bye` event
  and the server exits at once instead of waiting out its shutdown timeout.

## `/slow-consumer`

```bash
curl -N --limit-rate 200k localhost:8080/slow-consumer > /dev/null
```

The handler writes far faster than the client reads. Each stream's buffer is
capped, so the handler slows to the client's pace instead of the server's memory
growing. Suspend curl (Ctrl-Z) and it stops reading entirely; three seconds
later - this route's stall deadline - the stream ends with
`photon.ErrStreamOverflow` and the server logs it.

## `/fail`

```bash
curl -N localhost:8080/fail
```

```text
event: token
data: Starting

event: error
data: {"type":"https://photon.agenticmarket.dev/errors/upstream_error","title":"upstream error","status":502,"code":"upstream_error"}
```

The status line was sent with the first event, so the error travels in-band.
The client sees only the safe message; the server log has the cause, including
the internal address the client must not see.
