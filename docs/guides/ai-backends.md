# Building AI backends with Photon

Photon does not talk to models. It has no provider SDK, no prompt templates,
and no agent framework, and that is deliberate: those change every month, and
the code that serves them should not. What it does is solve the server problems
every AI backend has, whichever model and library you use:

1. **Streaming tokens** to the client as they are generated.
2. **Stopping the model** when the client leaves, so you stop paying for it.
3. **Surviving slow and stuck clients** without running out of memory.
4. **Serving long requests** — minutes, not milliseconds — without timeouts
   killing them, and without shutdowns cutting them in half.

This guide shows how those pieces fit into the endpoints AI backends actually
expose.

- [An OpenAI-compatible endpoint](#an-openai-compatible-endpoint)
- [Proxying an upstream model](#proxying-an-upstream-model)
- [Agent event streams](#agent-event-streams)
- [Long-running jobs and progress](#long-running-jobs-and-progress)
- [Serving MCP](#serving-mcp)
- [Authentication, quotas, and limits](#authentication-quotas-and-limits)
- [Security for model-facing code](#security-for-model-facing-code)
- [Testing](#testing)

## An OpenAI-compatible endpoint

Most AI clients - the OpenAI SDKs, LangChain, LlamaIndex, Continue, Open WebUI -
can be pointed at any server that speaks the Chat Completions format. Streaming
in that format is unnamed SSE events carrying JSON chunks, ended by a literal
`[DONE]`:

```text
data: {"id":"c1","object":"chat.completion.chunk","choices":[{"index":0,"delta":{"content":"Hel"}}]}

data: {"id":"c1","object":"chat.completion.chunk","choices":[{"index":0,"delta":{"content":"lo"}}]}

data: [DONE]
```

A request can ask for a stream (`"stream": true`) or a single JSON response, so
the handler decides first and only then hands over to `photon.SSE`:

```go
type ChatRequest struct {
	Model    string    `json:"model"`
	Messages []Message `json:"messages"`
	Stream   bool      `json:"stream"`
}

type Message struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

func chatCompletions(w http.ResponseWriter, r *http.Request) {
	var req ChatRequest
	if err := photon.DecodeJSON(r, &req); err != nil {
		openAIError(w, http.StatusBadRequest, "invalid_request_error", "the request body is not valid JSON")
		return
	}
	if len(req.Messages) == 0 {
		openAIError(w, http.StatusBadRequest, "invalid_request_error", "messages must not be empty")
		return
	}
	if !req.Stream {
		answer, err := model.Complete(r.Context(), req)
		if err != nil {
			openAIError(w, http.StatusBadGateway, "api_error", "the model is unavailable")
			return
		}
		_ = photon.JSON(w, http.StatusOK, answer)
		return
	}

	photon.SSE(func(s *photon.Stream, r *http.Request) error {
		for chunk, err := range model.Stream(r.Context(), req) {
			if err != nil {
				return photonerr.Upstream(err) // sent as an error event; the cause is logged
			}
			if err := s.JSON("", chunk); err != nil {
				return err // the client left: r.Context() is cancelled, so is the model call
			}
		}
		return s.Event("", []byte("[DONE]"))
	})(w, r)
}

// openAIError writes the error shape OpenAI SDKs parse into a typed exception.
func openAIError(w http.ResponseWriter, status int, typ, msg string) {
	_ = photon.JSON(w, status, map[string]any{
		"error": map[string]any{"message": msg, "type": typ},
	})
}
```

Three details matter here:

- **Unnamed events.** `s.JSON("", chunk)` writes `data:` with no `event:`
  line. The OpenAI SDKs treat a named event differently, so naming these
  `message` or `chunk` breaks them.
- **Validate before streaming.** Everything that can fail with a clear status
  code — bad JSON, empty messages, an unknown model, a quota — happens before
  the first event, while the status can still be `400` or `429`.
- **`photon.SSE(...)(w, r)`** — `SSE` returns an ordinary `http.HandlerFunc`,
  so it composes inside another handler like any other.

A complete, runnable version that works without an API key is in
[`examples/openai-proxy`](../../examples/openai-proxy); a fuller backend with
tenants, quotas, and metrics is in
[`examples/openai-backend`](../../examples/openai-backend).

## Proxying an upstream model

A gateway in front of OpenAI, Anthropic, vLLM, Ollama, or your own model server
forwards the request and relays the stream. The important part is the context:

```go
func proxy(s *photon.Stream, r *http.Request) error {
	body, _ := json.Marshal(upstreamRequestFrom(r))

	// r.Context() is cancelled when OUR client disconnects. Passing it to the
	// upstream request closes the upstream connection too, which is what tells
	// the provider to stop generating.
	req, err := http.NewRequestWithContext(r.Context(), http.MethodPost,
		upstreamURL+"/v1/chat/completions", bytes.NewReader(body))
	if err != nil {
		return photonerr.Internal(err)
	}
	req.Header.Set("Authorization", "Bearer "+apiKey)
	req.Header.Set("Content-Type", "application/json")

	resp, err := upstreamClient.Do(req)
	if err != nil {
		return photonerr.Upstream(err) // nothing sent yet: the client gets a 502
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return photonerr.Upstream(fmt.Errorf("upstream status %d", resp.StatusCode))
	}

	sc := bufio.NewScanner(resp.Body)
	sc.Buffer(make([]byte, 0, 64<<10), 1<<20) // one chunk can exceed bufio's 64 KB default
	for sc.Scan() {
		data, ok := bytes.CutPrefix(sc.Bytes(), []byte("data: "))
		if !ok {
			continue // blank lines, comments, and other fields
		}
		if err := s.Event("", data); err != nil {
			return err // our client left; returning closes the upstream body
		}
	}
	if err := sc.Err(); err != nil {
		return photonerr.Upstream(err)
	}
	return nil
}
```

And the client the gateway uses for the upstream:

```go
var upstreamClient = &http.Client{
	Transport: &http.Transport{
		ResponseHeaderTimeout: 30 * time.Second, // a model that never starts answering
		MaxIdleConnsPerHost:   64,               // reuse connections to the provider
		IdleConnTimeout:       90 * time.Second,
	},
	// No Client.Timeout: it is a deadline on the WHOLE response, and a long
	// answer is supposed to take minutes.
}
```

Back-pressure flows end to end here. If your client reads slowly, `s.Event`
waits, so the loop stops reading from the upstream, so the upstream's TCP
window fills and the provider slows down. Nothing piles up in memory in
between.

If you would rather forward bytes untouched — including the provider's own
event names — skip the parsing: set
`s.Header().Set("Content-Type", "text/event-stream")` and `io.Copy(s, resp.Body)`.
You give up the ability to inspect or rewrite events.

## Agent event streams

An agent produces more than tokens: it thinks, calls tools, gets results, and
answers. Give each kind of step its own event name and a JSON body, and the
frontend becomes a `switch` on the event type:

```go
func runAgent(s *photon.Stream, r *http.Request) error {
	task, err := decodeTask(r)
	if err != nil {
		return err // 400, before the stream opens
	}
	const maxSteps = 8 // a hard cap on tool loops; models do get stuck
	for step := 0; step < maxSteps; step++ {
		plan, err := llm.Plan(r.Context(), task)
		if err != nil {
			return photonerr.Upstream(err)
		}
		if err := s.JSON("thinking", map[string]any{"step": step, "text": plan.Reasoning}); err != nil {
			return err
		}
		if plan.Final != "" {
			for tok := range llm.StreamAnswer(r.Context(), task) {
				if err := s.JSON("token", map[string]string{"text": tok}); err != nil {
					return err
				}
			}
			return s.JSON("done", map[string]int{"steps": step + 1})
		}
		if err := s.JSON("tool_call", plan.Call); err != nil {
			return err
		}
		result := tools.Run(r.Context(), plan.Call) // see "Security" below
		if err := s.JSON("tool_result", result); err != nil {
			return err
		}
		task = task.With(result)
	}
	return s.JSON("error", map[string]string{"message": "the agent did not finish within the step limit"})
}
```

```js
const es = new EventSource("/agent?task=" + encodeURIComponent(q));
for (const type of ["thinking", "tool_call", "tool_result", "token", "done"]) {
  es.addEventListener(type, (e) => render(type, JSON.parse(e.data)));
}
es.addEventListener("done", () => es.close());
```

**Parallel tools.** A `Stream` is safe for concurrent use, so tools running in
parallel can each report progress directly. Wait for them before returning:

```go
var wg sync.WaitGroup
for _, call := range plan.Calls {
	wg.Add(1)
	go func() {
		defer wg.Done()
		_ = s.JSON("tool_call", call)
		_ = s.JSON("tool_result", tools.Run(r.Context(), call))
	}()
}
wg.Wait()
```

**Resuming.** Long agent runs outlive flaky connections. Number the events
with `s.SetEventID` and replay from `Last-Event-ID` on reconnect — see
[Resuming with Last-Event-ID](streaming.md#resuming-with-last-event-id). If the
run continues while the client is away, write events to a log (a database
table, Redis stream, NATS) and stream from the log instead of from the agent
directly.

## Long-running jobs and progress

A job that takes minutes - document ingestion, a batch embedding run, a long
agent task - does not have to hold one request open. Two patterns work well:

1. **Start, then watch.** `POST /jobs` returns `202` with a job id; `GET
   /jobs/:id/events` streams progress. If the client drops, it reconnects to
   the events route without restarting the job.
2. **Progress with `DropOldest`.** Progress is the one place where losing an
   event is fine: the next one supersedes it. With `DropOldest` a slow client
   never slows the job down — it simply sees fewer updates:

   ```go
   app.GET("/jobs/:id/events", photon.SSE(jobEvents, photon.WithOverflow(photon.DropOldest)))
   ```

   Use the default `Disconnect` policy for anything that must arrive intact,
   such as the final result.

Watch `s.Done()` in long loops so a deploy can drain them — see
[Graceful shutdown](streaming.md#graceful-shutdown).

## Serving MCP

The Model Context Protocol's Streamable HTTP transport is plain HTTP: a single
endpoint that answers `POST` with JSON or an SSE stream, `GET` with an SSE
stream, and `DELETE` to end a session. MCP server libraries for Go give you an
`http.Handler` for it — the official SDK
(`github.com/modelcontextprotocol/go-sdk`) has a streamable HTTP handler, for
example — and `Mount` serves it under a prefix for every method:

```go
var mcpHandler http.Handler = newMCPHandler() // from your MCP library

api := app.Group("", requireAPIKey) // MCP endpoints need auth like any other
api.Mount("/mcp", mcpHandler)
```

Photon's limits apply to the mounted handler: the 1 MiB body limit, the header
limits, panic recovery, and graceful shutdown. Raise `MaxRequestBodyBytes` if
your tools take large inputs. Photon does not implement MCP itself: the protocol changes quickly, and a
dedicated MCP SDK mounted under Photon keeps up with it better than the core
could.

## Authentication, quotas, and limits

Put authentication on a group, so a route cannot forget it:

```go
func requireAPIKey(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		key, ok := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer ")
		tenant, found := tenants.Lookup(key) // constant-time comparison inside
		if !ok || !found {
			photon.Error(w, r, photonerr.Unauthorized("missing or invalid API key"))
			return
		}
		next.ServeHTTP(w, r.WithContext(withTenant(r.Context(), tenant)))
	})
}

v1 := app.Group("/v1", requireAPIKey)
v1.POST("/chat/completions", chatCompletions)
v1.GET("/models", listModels)
```

Quotas — requests per minute, concurrent streams per key, tokens per day — are
application logic, but report them the standard way:

```go
w.Header().Set("Retry-After", "20")
photon.Error(w, r, photonerr.TooManyRequests("rate limit reached for this key"))
```

Server-wide limits are on by default; the ones worth revisiting for an AI
backend:

| Limit | Default | Consider |
|---|---|---|
| `MaxRequestBodyBytes` | 1 MiB | Raise for long contexts or file uploads; prompts with images can be large |
| `MaxStreams` | derived from memory | Set explicitly if you know your capacity |
| `StreamStallTimeout` | 30 s | Lower for internal clients, raise for mobile networks |
| `HeartbeatInterval` | 15 s | Lower if a proxy in front has an idle timeout under 30 s |
| `MaxPerIP` / `MaxConnections` | off | Turn on for a public endpoint |

See [configuration](configuration.md) for all of them.

## Security for model-facing code

Photon secures the HTTP layer. Everything between the request and the model is
yours, and a few rules carry most of the weight:

- **Model output is untrusted input.** Never execute it, never render it as
  HTML without escaping, never follow URLs from it without an allow-list. A
  prompt injection in a web page the model read becomes the model's output.
- **Tools run with your credentials, not the user's intent.** Give each tool the
  least access it needs, validate tool arguments as strictly as you would a
  public API's input, and cap how many tool calls one request can make.
- **Server-side request forgery.** A "fetch this URL" tool can be pointed at
  `http://169.254.169.254/` or your internal services. Resolve and check the
  address before connecting, and block private ranges.
- **Do not log prompts and completions by default.** They contain whatever
  users paste. Log ids, sizes, and timings; sample content only with a reason.
- **SSE field injection is handled.** Model output inside `s.Event` data cannot
  create new SSE fields, and event names and ids with line breaks are refused —
  see [Writing events](streaming.md#writing-events).

## Testing

Put the model behind an interface and test against a fake. Photon's server is
an `http.Handler`, so a streaming endpoint tests like any other:

```go
func TestChatStreams(t *testing.T) {
	app := newApp(fakeModel{tokens: []string{"Hel", "lo"}})
	rec := httptest.NewRecorder()
	req := httptest.NewRequest("POST", "/v1/chat/completions",
		strings.NewReader(`{"model":"m","messages":[{"role":"user","content":"hi"}],"stream":true}`))
	req.Header.Set("Content-Type", "application/json")

	app.ServeHTTP(rec, req)

	body := rec.Body.String()
	if !strings.Contains(body, `"content":"Hel"`) || !strings.HasSuffix(body, "data: [DONE]\n\n") {
		t.Fatalf("unexpected stream:\n%s", body)
	}
}
```

Use `httptest.NewServer(app)` when the test needs a real connection — to check
that a disconnect cancels the upstream call, for instance. More in
[testing](testing.md).

## See also

- [Streaming](streaming.md) — the full `Stream` API and its guarantees
- [`examples/openai-proxy`](../../examples/openai-proxy) — a gateway that relays an upstream model, runnable without an API key
- [`examples/agent`](../../examples/agent) — an agent event stream with tools, resume, and shutdown handling
- [`examples/openai-backend`](../../examples/openai-backend) — tenants, quotas, metrics, Dockerfile
