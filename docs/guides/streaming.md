# Streaming: server-sent events, NDJSON, and long responses

Streaming is the reason Photon exists. This guide covers how to stream, what
Photon does for you while you do, and the handful of rules that keep a stream
correct when the client is slow, gone, or hostile.

- [The short version](#the-short-version)
- [Writing events](#writing-events)
- [Errors: before and after the first event](#errors-before-and-after-the-first-event)
- [When the client goes away](#when-the-client-goes-away)
- [Slow clients and back-pressure](#slow-clients-and-back-pressure)
- [Heartbeats and proxies](#heartbeats-and-proxies)
- [Graceful shutdown](#graceful-shutdown)
- [Resuming with Last-Event-ID](#resuming-with-last-event-id)
- [NDJSON and other non-SSE streams](#ndjson-and-other-non-sse-streams)
- [Concurrency](#concurrency)
- [Lower-level APIs](#lower-level-apis)
- [Limits that apply to streams](#limits-that-apply-to-streams)

## The short version

```go
app.POST("/chat", photon.SSE(func(s *photon.Stream, r *http.Request) error {
	var req ChatRequest
	if err := photon.DecodeJSON(r, &req); err != nil {
		return err // nothing sent yet: the client gets a normal 400
	}
	for tok := range model.Stream(r.Context(), req.Prompt) {
		if err := s.Event("token", []byte(tok)); err != nil {
			return err // the client left: returning cancels r.Context()
		}
	}
	return s.Event("done", nil)
}))
```

`photon.SSE` wraps your function and handles the parts that are easy to get
wrong:

| You write | Photon handles |
|---|---|
| A loop that calls `s.Event` | Headers, SSE framing, flushing each event as it is produced |
| `return err` | The right response: an HTTP error before streaming starts, an `error` event after |
| Nothing | `MaxStreams` admission (503 + `Retry-After` when full) |
| Nothing | Back-pressure and a bounded buffer per stream |
| Nothing | Heartbeats every 15 s of silence, so proxies keep the connection open |
| Nothing | Closing the stream, and aborting the connection if your function panics mid-stream |

## Writing events

```go
s.Event("token", []byte("Hello"))     // event: token / data: Hello
s.Event("", []byte("Hello"))          // an unnamed event: browsers deliver it to onmessage
s.JSON("chunk", map[string]any{"text": "Hello", "index": 0})
s.JSON("", openAIChunk)               // OpenAI-style: unnamed events with JSON data
s.Event("", []byte("[DONE]"))         // OpenAI's end-of-stream sentinel
s.Event("done", nil)                  // empty data is still dispatched by clients
s.Comment("anything")                 // ": anything" — ignored by every client
```

**Each event is sent immediately.** There is no flush call to remember. When
your producer is faster than the network, events that queue up while a write is
in progress go out together in the next write, so a burst does not cost one
system call per token.

**Data can contain anything.** Each line of the data becomes its own `data:`
field, split on `\r\n`, `\n`, and a lone `\r` exactly the way the client splits
them. The client joins the lines back with `\n`, so a payload round-trips
exactly, including the newline tokens a model emits for paragraph breaks. A line
break inside model output can never start a new SSE field.

**Event names and ids cannot contain line breaks.** `Event` and `SetEventID`
return `photon.ErrInvalidEvent` for a name or id containing `\r`, `\n`, or NUL,
rather than letting it end the field early and inject fields of its own. If a
name or id ever comes from user input, check the error.

## Errors: before and after the first event

HTTP sends the status code before the body. Once the first event is written,
the status is `200` and cannot change. `photon.SSE` uses that boundary:

```go
photon.SSE(func(s *photon.Stream, r *http.Request) error {
	if !allowed(r) {
		return photonerr.Forbidden("not your conversation") // -> 403 problem+json
	}
	if err := s.Event("start", nil); err != nil { // the status is now 200
		return err
	}
	if err := callModel(r.Context(), s); err != nil {
		return photonerr.Upstream(err) // -> event: error, then the stream closes
	}
	return nil
})
```

- **Before the first event**, a returned error is written as a normal
  [RFC 9457](https://www.rfc-editor.org/rfc/rfc9457) problem response with its
  own status code: a `*photonerr.Error` keeps its status and message; any other
  error becomes a 500 with a constant body.
- **After the first event**, the error is sent in-band as an event named
  `error`, carrying the same problem document:

  ```text
  event: error
  data: {"type":"https://photon.agenticmarket.dev/errors/upstream_error","title":"upstream error","status":502,"code":"upstream_error"}
  ```

  Only the safe message is sent. The cause (`dial tcp 10.0.0.7:443: connection
  refused`) is logged for you, never written to the client.
- **"The client left" is not an error.** If you return `photon.ErrClientGone`,
  `photon.ErrStreamOverflow`, or a `context.Canceled` caused by the client
  disconnecting, Photon ends the stream quietly. That is why the loop above can
  just `return err`.
- **A panic mid-stream aborts the connection.** The panic is recovered and
  logged, and the connection is closed without the final chunk, so the client
  sees a failed stream. Ending it cleanly would make a truncated answer look
  complete.

On the client, listen for the `error` event:

```js
const es = new EventSource("/events");
es.addEventListener("error", (e) => {
  if (e.data) console.error("server error", JSON.parse(e.data)); // our event
  // without data, it is EventSource's own connection error
});
```

## When the client goes away

A user closes the tab, a phone loses signal, a load balancer times out. Two
things then happen together:

1. `r.Context()` is cancelled.
2. The next `s.Event` returns `photon.ErrClientGone`.

Pass `r.Context()` to whatever produces your events — the model client, the
database query — so the upstream work stops too, instead of generating (and
billing for) tokens nobody will read:

```go
stream, err := llm.ChatStream(r.Context(), req) // cancelled when the client leaves
```

Check the error from every `s.Event`. Ignoring it is the classic streaming bug:
the handler happily writes the rest of the answer into a connection nobody is
reading, and the upstream call keeps running.

## Slow clients and back-pressure

Every stream has a buffer of bytes it has accepted but not yet sent, capped at
`MaxStreamBufferBytes` (64 KiB by default). The buffer starts small and grows
only when needed, so a typical token stream uses a few kilobytes.

When a client reads more slowly than you produce, what happens next is the
stream's **overflow policy**:

| Policy | When the buffer is full | Use it for |
|---|---|---|
| `photon.Disconnect` (default) | The producer waits for room. The stream ends only if the client accepts nothing for `StreamStallTimeout` (30 s). | Text, tokens, anything where every byte matters |
| `photon.DropOldest` | The oldest **whole** events waiting to be sent are discarded, so the producer does not wait for a slow client. It waits only if the room it needs is held by bytes already being written. | Metrics, progress, presence — where the newest value replaces the last |
| `photon.Grow` | The buffer grows past its cap, drawing on the server-wide `MaxTotalStreamBufferBytes` budget; then it behaves like `Disconnect`. | Bursty output you would rather buffer than slow down |

```go
app.GET("/metrics/live", photon.SSE(liveMetrics, photon.WithOverflow(photon.DropOldest)))
```

With every policy, a client that stops reading entirely is disconnected after
`StreamStallTimeout`, and your next `s.Event` returns
`photon.ErrStreamOverflow`. That is what keeps one stuck client from holding a
goroutine and a buffer forever.

"Stopped reading" means the connection accepted nothing for a whole stall
period. A client that keeps reading, however slowly, is served. In practice the
smallest progress a server can observe is set by TCP, which reports freed
receive-window space in steps of up to one segment (often 64 KB), so the
effective floor at the default 30 s stall is a client reading around 2 KB/s.
Lower `StreamStallTimeout` only as far as your slowest legitimate clients
allow.

> **Do not use `DropOldest` for text.** A token stream with events missing from
> the middle reads as a hallucination, and the user cannot tell "the model said
> that" from "the server dropped 400 tokens". Photon logs a warning, which
> cannot be turned off, the first time a stream that has carried text actually
> drops data.

Events are never cut in half: `DropOldest` drops complete events only, and an
event larger than the whole buffer is streamed through in pieces rather than
refused.

## Heartbeats and proxies

Load balancers, CDNs, and corporate proxies close connections that are idle for
30–60 seconds. A model that thinks for a minute before its first token, or a
feed that is quiet overnight, looks idle.

Once a stream has started, Photon sends an SSE comment (`: ping`) after every
`HeartbeatInterval` (15 s) without other output. Every SSE client ignores
comments, so heartbeats change nothing the user sees.

Before the first event there is nothing to keep alive yet, and Photon cannot
send a heartbeat without committing a `200`. If your first event can take
longer than your proxy's idle timeout, start heartbeats explicitly, after
validating the request:

```go
photon.SSE(func(s *photon.Stream, r *http.Request) error {
	req, err := parse(r)
	if err != nil {
		return err // still a normal 400
	}
	stop := s.StartHeartbeats() // opens the stream now: the 200 is committed
	defer stop()
	answer, err := slowThinkingModel(r.Context(), req)
	...
})
```

`StartHeartbeats` opens the stream immediately, on your goroutine, and
heartbeats then follow every 15 s of silence. Errors after it are delivered as
`error` events.

Photon also sends `X-Accel-Buffering: no`, which tells nginx not to buffer the
response. For other proxies, see [deployment](deployment.md#reverse-proxies-for-sse).

## Graceful shutdown

`app.Run` handles `SIGINT` and `SIGTERM`. On a signal it stops accepting
connections and waits up to `ShutdownTimeout` (25 s) for in-flight work. A
stream that never ends — a notification feed — would hold shutdown open for the
whole 25 s and then be cut off.

`s.Done()` fixes that. It is closed when the client leaves, when the stream
ends, **or when the server starts shutting down**, and the stream is still
writable afterwards:

```go
photon.SSE(func(s *photon.Stream, r *http.Request) error {
	for {
		select {
		case <-s.Done():
			// Shutting down (or the client left). Say goodbye and return;
			// EventSource reconnects, to another instance in a rolling deploy.
			return s.Event("reconnect", nil)
		case n := <-notifications:
			if err := s.JSON("notification", n); err != nil {
				return err
			}
		}
	}
})
```

A finite stream — one answer — does not need `Done`: it finishes within the
shutdown window on its own.

## Resuming with Last-Event-ID

Give events ids, and a reconnecting `EventSource` sends the last one it saw in
the `Last-Event-ID` header:

```go
photon.SSE(func(s *photon.Stream, r *http.Request) error {
	start := 0
	if id, err := strconv.Atoi(r.Header.Get("Last-Event-ID")); err == nil && id >= 0 {
		start = id + 1 // the id of the last event received, so resume after it
	}
	for i := start; i < len(tokens); i++ {
		if err := s.SetEventID(strconv.Itoa(i)); err != nil {
			return err
		}
		if err := s.Event("token", []byte(tokens[i])); err != nil {
			return err
		}
	}
	return nil
})
```

`SetEventID` attaches the id to the **next** event, in the same frame, so an
event and its id cannot be separated. `s.SetRetry(2*time.Second)` tells the
client how long to wait before reconnecting.

`Last-Event-ID` comes from the client, so treat it as untrusted input: parse
it, bound it, and fall back to starting over. A real backend usually streams
from a durable log and uses the log offset as the id, so resuming works across
server restarts too.

## NDJSON and other non-SSE streams

Some model servers (Ollama, for one) stream newline-delimited JSON instead of
SSE. A `Stream` is an `io.Writer`; set the content type before the first write
and write lines:

```go
photon.SSE(func(s *photon.Stream, r *http.Request) error {
	s.Header().Set("Content-Type", "application/x-ndjson")
	enc := json.NewEncoder(s)
	for chunk := range chunks {
		if err := enc.Encode(chunk); err != nil {
			return err
		}
	}
	return nil
})
```

Heartbeats are only sent on `text/event-stream` responses, so they never
corrupt another format. A non-SSE stream has no in-band error format, so an
error after the first write aborts the connection instead of sending an
`error` event.

`io.Copy(s, upstreamBody)` proxies a byte stream with the same bounds and
back-pressure.

## Concurrency

A `Stream` is safe for concurrent use. Each `Event`, `JSON`, `Comment`, or
`Write` call is written as a unit, so events from several goroutines never
interleave mid-frame — useful for an agent that runs tools in parallel and
reports progress from each. Order across goroutines is the order the calls
acquired the stream; order within one goroutine is always preserved.

Your `StreamFunc` must not return while other goroutines are still writing to
the stream: wait for them (`sync.WaitGroup`, `errgroup`) first. After it
returns, the stream is closed and further writes return
`photon.ErrStreamClosed`.

## Lower-level APIs

You will rarely need these, but they exist:

- **`photon.Streaming()` + `photon.StreamFrom(r)`** — middleware that gives a
  plain `http.HandlerFunc` a stream and enforces `MaxStreams`. Use it to keep an
  existing handler signature. Apply it per route
  (`app.GET("/events", h, photon.Streaming())`), not with `Use`: every request
  through it holds a `MaxStreams` slot.
- **`photon.NewStream(w, r)`** — a stream with no adapter. You must
  `defer s.Close()`, and nothing enforces `MaxStreams`.
- **`s.Flush()`** — commits the headers if needed and blocks until everything
  accepted so far is written. Useful in tests to make timing deterministic.
- **`s.Err()`** — why the stream ended: `nil`, `ErrClientGone`,
  `ErrStreamOverflow`, `ErrStreamPanic`, or a transport error.
- **`s.Header()`** — response headers, changeable until the first write.

## Limits that apply to streams

| Limit | Default | What it bounds |
|---|---|---|
| `MaxStreams` | derived from memory, 64–8192 | Concurrent streams; the 503 threshold |
| `MaxStreamBufferBytes` | 64 KiB | Unsent bytes per stream |
| `MaxTotalStreamBufferBytes` | `MaxStreams × MaxStreamBufferBytes` | Server-wide budget for `Grow` |
| `StreamStallTimeout` | 30 s | How long a stream may make no progress |
| `HeartbeatInterval` | 15 s | Silence before a heartbeat (0 disables) |
| `ReadTimeout` | 0 (none) | Deliberately off: a whole-request deadline would kill a long stream |

`MaxStreams` is derived from the memory budget (`GOMEMLIMIT`, the cgroup limit,
or physical memory) so that a 512 MB container and a 64 GB server both get a
safe number without configuration. Per-route overrides:
`photon.WithStall(d)`, `photon.WithStreamBuffer(n)`,
`photon.WithOverflow(p)`. See [configuration](configuration.md).

## See also

- [Building AI backends](ai-backends.md) — OpenAI-compatible endpoints, proxying a model, agent events
- [`examples/stream`](../../examples/stream) — every technique on this page, runnable
- [Security](security.md) — what the limits defend against
