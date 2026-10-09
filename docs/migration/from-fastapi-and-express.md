# Migrating from FastAPI or Express

> **What changes:** the language and the concurrency model. Async functions and generators become ordinary Go functions on goroutines, Pydantic models and JSON schemas become structs plus explicit checks, and `HTTPException` becomes `photonerr`.
> **What stays the same:** the HTTP contract. Same routes, same SSE wire format, same OpenAI-compatible request and response bodies, so streaming clients don't need to change.
> **Watch out for:** no generated OpenAPI docs, no type coercion and no 422 validation errors, a 1 MiB default body limit that large prompts or base64 images can exceed, and different error bodies.

This guide is for teams moving an AI backend from Python (FastAPI, Starlette)
or Node (Express, Fastify) to Go. It maps the concepts, then walks through one
complete OpenAI-compatible streaming endpoint in all three.

A note on expectations: Go won't make your model faster. For an AI backend, the
model call dominates latency, and that doesn't change. What changes is the
server around it: one binary, requests that run in parallel on every core
without workers, and a streaming layer that handles slow clients, disconnects,
and shutdown for you.

- [The mental model](#the-mental-model)
- [Concept map](#concept-map)
- [Routes and parameters](#routes-and-parameters)
- [Request bodies: models to structs](#request-bodies-models-to-structs)
- [Responses and errors](#responses-and-errors)
- [Dependencies and middleware](#dependencies-and-middleware)
- [Streaming](#streaming)
- [A complete OpenAI-compatible streaming endpoint](#a-complete-openai-compatible-streaming-endpoint)
- [Background work](#background-work)
- [Startup, shutdown, and deployment](#startup-shutdown-and-deployment)
- [Testing](#testing)
- [Gotchas](#gotchas)
- [Checklist](#checklist)

## The mental model

Four things are different from an async Python or Node server:

1. **Every request runs on its own goroutine.** There's no event loop to block.
   A blocking call (a database query, an HTTP request to a model provider) is
   fine; it parks that goroutine and the rest of the server keeps going. There
   is no `async` or `await`.
2. **Cancellation is a value you pass down.** `r.Context()` is cancelled when the
   client disconnects or the handler returns. Pass it to every call that does
   I/O, and those calls stop when the client leaves.
3. **Errors are return values.** There's no `raise` or `throw` for control flow.
   A function returns an `error`; you check it and decide.
4. **Nothing is injected.** Dependencies are function arguments, struct fields,
   or values that middleware puts in the request context.

## Concept map

| FastAPI | Express / Fastify | Photon |
|---|---|---|
| `app = FastAPI()` | `const app = express()` / `Fastify()` | `app := photon.New()` |
| `@app.get("/users/{id}")` | `app.get('/users/:id', fn)` | `app.GET("/users/:id", h)` |
| `def f(id: int)` (path param) | `req.params.id` | `photon.PathParam(r, "id")`, then `strconv` |
| `q: str \| None = None` (query) | `req.query.q` | `r.URL.Query().Get("q")` |
| `x_key: str = Header()` | `req.get('x-key')` | `r.Header.Get("X-Key")` |
| Pydantic model parameter | `express.json()` + manual checks / JSON schema in Fastify | struct + `photon.DecodeJSON` + a `validate` method |
| `return {...}` | `res.json({...})` | `photon.JSON(w, http.StatusOK, v)` |
| `raise HTTPException(404, ...)` | `res.status(404).json(...)` | `photon.Error(w, r, photonerr.NotFound("user"))` |
| `Depends(get_user)` | middleware setting `req.user` | middleware setting a context value |
| `@app.middleware("http")` | `app.use(fn)` | `app.Use(mw)` |
| `APIRouter(prefix="/v1")` | `express.Router()` mounted at `/v1` | `app.Group("/v1")` |
| `StreamingResponse` / `EventSourceResponse` | `res.write(...)` | `photon.SSE(fn)` |
| async generator, `yield` | async generator, `for await` | `for ... range` with `s.Event` |
| `await request.is_disconnected()` | `req.on('close')` / `res.on('close')` | the error from `s.Event`, `r.Context().Done()`, `s.Done()` |
| `BackgroundTasks` | un-awaited promise | a goroutine (see [Background work](#background-work)) |
| `lifespan` startup code | code before `app.listen` | code in `main` before `app.Run` |
| uvicorn with workers | node, cluster | the compiled binary |
| `TestClient` | supertest | `net/http/httptest` |
| `/docs`, `openapi.json` | swagger plugins | **none** |

## Routes and parameters

**FastAPI**:

```python
@app.get("/users/{user_id}")
async def get_user(user_id: int, verbose: bool = False):
    user = await db.find_user(user_id)
    if user is None:
        raise HTTPException(status_code=404, detail="user not found")
    return user
```

**Express**:

```js
app.get('/users/:userId', async (req, res) => {
  const userId = Number(req.params.userId);
  const verbose = req.query.verbose === 'true';
  const user = await db.findUser(userId);
  if (!user) return res.status(404).json({ error: 'user not found' });
  res.json(user);
});
```

**Photon**:

```go
app.GET("/users/:user_id", func(w http.ResponseWriter, r *http.Request) {
	userID, err := strconv.Atoi(photon.PathParam(r, "user_id"))
	if err != nil {
		photon.Error(w, r, photonerr.BadRequest("user_id must be an integer"))
		return
	}
	verbose := r.URL.Query().Get("verbose") == "true" // absent means false, as in the Python default
	_ = verbose                                       // use it as your handler did

	user, err := db.FindUser(r.Context(), userID)
	if errors.Is(err, sql.ErrNoRows) {
		photon.Error(w, r, photonerr.NotFound("user"))
		return
	}
	if err != nil {
		photon.Error(w, r, err) // 500 with a constant body; the cause is logged
		return
	}
	_ = photon.JSON(w, http.StatusOK, user)
})
```

Photon's patterns are `:name` for one segment and `*name` for the rest of the
path. Two routes for the same method that could match the same request panic at
startup, so `/users/me` beside `/users/:id` is an error; see
[../guides/routing.md](../guides/routing.md). Trailing slashes are not
redirected: `/users/` is a 404.

## Request bodies: models to structs

**FastAPI** (Pydantic v2):

```python
class Message(BaseModel):
    role: Literal["system", "user", "assistant"]
    content: str

class ChatRequest(BaseModel):
    model: str
    messages: list[Message] = Field(min_length=1)
    temperature: float = Field(default=1.0, ge=0, le=2)
    max_tokens: int | None = None
    stream: bool = False
```

**Photon**:

```go
type message struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

type chatRequest struct {
	Model       string    `json:"model"`
	Messages    []message `json:"messages"`
	Temperature float64   `json:"temperature"`
	MaxTokens   *int      `json:"max_tokens"` // nil when absent, like None
	Stream      bool      `json:"stream"`
}

// validate does what the Pydantic types and Field constraints did.
func (req *chatRequest) validate() error {
	if req.Model == "" {
		return photonerr.BadRequest("model is required")
	}
	if len(req.Messages) == 0 {
		return photonerr.BadRequest("messages must not be empty")
	}
	for _, m := range req.Messages {
		switch m.Role {
		case "system", "user", "assistant":
		default:
			return photonerr.BadRequest("each message role must be system, user, or assistant")
		}
	}
	if req.Temperature < 0 || req.Temperature > 2 {
		return photonerr.BadRequest("temperature must be between 0 and 2")
	}
	return nil
}
```

And in the handler:

```go
req := chatRequest{Temperature: 1.0} // defaults: fields missing from the JSON keep these
if err := photon.DecodeJSON(r, &req); err != nil {
	photon.Error(w, r, err) // 400, 413, or 415
	return
}
if err := req.validate(); err != nil {
	photon.Error(w, r, err)
	return
}
```

How the two behave differently:

- **Defaults** come from the struct value you decode into. Fields absent from
  the JSON keep whatever you set first.
- **Optional fields** that must distinguish "absent" from "zero" are pointers.
- **Unknown fields are ignored**, as with Pydantic's default. If you used
  `extra="forbid"`, you'll need your own check.
- **No coercion.** Pydantic accepts `"max_tokens": "100"` for an `int` field.
  Go's JSON decoder doesn't, and `DecodeJSON` answers 400 ("field max_tokens has
  the wrong type"). If clients send numbers as strings, fix them or decode into a
  `json.Number` and convert yourself.
- **400, not 422.** FastAPI reports validation failures as 422 with a list of
  errors. Photon's decoder returns 400 and your checks return whatever you build.
  If clients depend on 422, return
  `photonerr.UnprocessableEntity("messages must not be empty")`.
- **415 for non-JSON content types.** A request with `Content-Type: text/plain`
  is refused; one with no `Content-Type` is accepted. Express's `express.json()`
  silently skipped such bodies, leaving `req.body` empty.
- **Body size.** `DecodeJSON` reads through the server's 1 MiB limit (Express's
  `express.json()` defaults to 100 KB; FastAPI has none). Long contexts and base64
  images can exceed 1 MiB; see [Gotchas](#gotchas).

## Responses and errors

| FastAPI / Express | Photon |
|---|---|
| `return {"ok": True}` / `res.json({ok: true})` | `photon.JSON(w, http.StatusOK, map[string]any{"ok": true})` |
| `status_code=201` / `res.status(201).json(v)` | `photon.JSON(w, http.StatusCreated, v)` |
| `Response(status_code=204)` / `res.sendStatus(204)` | `photon.NoContent(w)` |
| `PlainTextResponse(s)` / `res.send(s)` | `photon.Text(w, http.StatusOK, s)` |
| `RedirectResponse(url)` / `res.redirect(url)` | `photon.Redirect(w, r, url)` (303) or `http.Redirect` for other codes |
| `raise HTTPException(400, detail=msg)` | `photon.Error(w, r, photonerr.BadRequest(msg)); return` |
| `raise HTTPException(401, ...)` | `photonerr.Unauthorized(msg)` |
| `raise HTTPException(403, ...)` | `photonerr.Forbidden(msg)` |
| `raise HTTPException(404, ...)` | `photonerr.NotFound("thing")` |
| `raise HTTPException(409, ...)` | `photonerr.Conflict(msg)` |
| `raise HTTPException(429, ...)` | `photonerr.TooManyRequests(msg)` |
| upstream model call failed | `photonerr.Upstream(err)` (502) |
| unhandled exception | any other `error`, or a panic: 500 with a constant body, logged |

The error body changes shape. FastAPI sends `{"detail": "..."}`; Express sends
whatever you wrote. `photon.Error` sends RFC 9457 `application/problem+json`:

```json
{"type":"https://…/errors/not_found","title":"user not found","status":404,"code":"not_found"}
```

(`type` shortened.) Messages you pass to `photonerr` constructors are sent to
the client, so they must not contain request content or internals. Add
operator-only context with `.WithDetail(...)` or `.WithCause(err)`; neither is
sent. If a client depends on the old shape, write it yourself with `photon.JSON`.

## Dependencies and middleware

A FastAPI dependency that authenticates becomes middleware that puts the user in
the request context.

**FastAPI**:

```python
async def current_user(authorization: str = Header(default="")) -> User:
    user = await lookup_token(authorization)
    if user is None:
        raise HTTPException(status_code=401, detail="missing or invalid credentials")
    return user

@app.get("/me")
async def me(user: User = Depends(current_user)):
    return user
```

**Express**:

```js
async function currentUser(req, res, next) {
  const user = await lookupToken(req.get('authorization'));
  if (!user) return res.status(401).json({ error: 'missing or invalid credentials' });
  req.user = user;
  next();
}

app.get('/me', currentUser, (req, res) => res.json(req.user));
```

**Photon**:

```go
type ctxKey int

const userKey ctxKey = iota

func currentUser(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		user, err := lookupToken(r.Context(), r.Header.Get("Authorization"))
		if err != nil {
			photon.Error(w, r, photonerr.Unauthorized("missing or invalid credentials"))
			return
		}
		ctx := context.WithValue(r.Context(), userKey, user)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

func userFrom(r *http.Request) *User {
	u, _ := r.Context().Value(userKey).(*User)
	return u
}

app.GET("/me", func(w http.ResponseWriter, r *http.Request) {
	_ = photon.JSON(w, http.StatusOK, userFrom(r))
}, currentUser)
```

Middleware attaches in three places: `app.Use(mw)` for every request (including
404 and 405), `app.Group("/v1", mw)` for a group, or as trailing arguments to a
route as above. Any `net/http` middleware works. For CORS, a common choice is
`github.com/rs/cors`: `app.Use(cors.New(cors.Options{...}).Handler)`.

Dependencies that provide resources (a database pool, a model client) become
fields on a struct whose methods are your handlers:

```go
type api struct {
	db *sql.DB
}

func (a *api) getUser(w http.ResponseWriter, r *http.Request) {
	// a.db is available here
}

a := &api{db: db}
app.GET("/users/:id", a.getUser) // a method value is an http.HandlerFunc
```

See [../guides/middleware.md](../guides/middleware.md).

## Streaming

### Generators become loops

**FastAPI** with `StreamingResponse`:

```python
@app.post("/chat")
async def chat(req: ChatRequest, request: Request):
    async def events():
        async for token in generate(req):
            if await request.is_disconnected():
                return
            yield f"event: token\ndata: {token}\n\n"
        yield "event: done\ndata: \n\n"
    return StreamingResponse(events(), media_type="text/event-stream")
```

With sse-starlette's `EventSourceResponse` you yield dicts such as
`{"event": "token", "data": token}` instead of formatting lines yourself.

**Express**:

```js
app.post('/chat', async (req, res) => {
  res.writeHead(200, { 'Content-Type': 'text/event-stream', 'Cache-Control': 'no-cache' });
  let gone = false;
  res.on('close', () => { gone = true; });
  for await (const token of generate(req.body)) {
    if (gone) return;
    res.write(`event: token\ndata: ${token}\n\n`);
  }
  res.write('event: done\ndata: \n\n');
  res.end();
});
```

**Photon**:

```go
app.POST("/chat", photon.SSE(func(s *photon.Stream, r *http.Request) error {
	var req chatRequest
	if err := photon.DecodeJSON(r, &req); err != nil {
		return err // nothing sent yet: the client gets a normal 400
	}
	for token, err := range generate(r.Context(), req) {
		if err != nil {
			return photonerr.Upstream(err)
		}
		if err := s.Event("token", []byte(token)); err != nil {
			return err // the client is gone: stop
		}
	}
	return s.Event("done", nil)
}))
```

`s.Event` handles the framing, including tokens that contain newlines, which the
f-string and template-literal versions above would break on. `s.JSON(name, v)`
sends a JSON-encoded event, and `s.Event("", data)` sends an unnamed one, which is
what OpenAI-style clients read.

### Async generators become iterators

A Python async generator or a JavaScript `async function*` maps naturally to a
Go iterator function (Go 1.23+). The caller ranges over it; returning from the
loop stops the producer.

```go
// generate stands in for your model client. It yields tokens one at a time
// and stops when ctx is cancelled.
func generate(ctx context.Context, req chatRequest) iter.Seq2[string, error] {
	return func(yield func(string, error) bool) {
		for _, word := range strings.Fields("This text stands in for a model's output.") {
			select {
			case <-ctx.Done():
				yield("", ctx.Err())
				return
			case <-time.After(50 * time.Millisecond):
			}
			if !yield(word+" ", nil) {
				return // the caller stopped ranging
			}
		}
	}
}
```

A real client would make an HTTP request to the provider with
`http.NewRequestWithContext(ctx, ...)` and read its stream inside the iterator.
A channel works too; if you use one, make sure the producer goroutine selects on
`ctx.Done()` so it exits when the handler returns.

### Noticing that the client left

| FastAPI / Express | Photon |
|---|---|
| `await request.is_disconnected()` in the loop | `s.Event` returns an error. Return it. |
| Generator cancelled by the server | `r.Context()` is cancelled, so the model call stops if you passed the context. |
| `req.on('close')` / `res.on('close')` | `<-r.Context().Done()` or `<-s.Done()` in a `select` |

For a token stream, the error from `s.Event` is all you need. Don't stop the
loop on `s.Done()` in that case: `Done` also fires when the server starts a
graceful shutdown, and during shutdown Photon lets in-flight answers finish (up
to the shutdown timeout) rather than cutting them off. `s.Done()` is for
open-ended streams, such as notifications or progress feeds, that would otherwise
never return:

```go
for {
	select {
	case <-s.Done():
		return nil // client gone, or the server is shutting down
	case ev := <-updates:
		if err := s.JSON("update", ev); err != nil {
			return err
		}
	}
}
```

### Errors in the middle of a stream

Before the first event, a returned error becomes a normal HTTP response (400,
401, 502, ...) through `photon.Error`. After the first event the status is
already 200, so Photon sends the error in-band as `event: error` with a problem
document, then closes the connection:

```
event: error
data: {"type":"https://…/errors/upstream_error","title":"upstream error","status":502,"code":"upstream_error"}
```

In FastAPI, an exception raised inside the generator after the first chunk
usually just ends the response, and the client sees the stream stop. Not every
OpenAI-compatible client looks for an event named `error`. If yours expects a
specific error format, send that yourself with `s.JSON` and then `return nil`.

### What you no longer write

- **Back-pressure.** In Node, `res.write` returns `false` when the socket buffer
  is full, and most hand-written SSE code ignores it, so a slow reader grows the
  process's memory. In Photon, each stream has a bounded buffer (64 KiB);
  `s.Event` waits when it is full, and a client that stays stuck for 30 seconds
  is disconnected.
- **Heartbeats.** Photon sends an SSE comment after 15 seconds of silence, so
  proxies and load balancers keep the connection open. Before the first event
  (while a model is still thinking), call `stop := s.StartHeartbeats()` and
  `defer stop()`.
- **A concurrency cap.** `MaxStreams` is derived from available memory (64 to
  8192). When it's reached, new streams get 503 with `Retry-After`. Set it with
  `photon.MaxStreams(n)`.
- **Headers.** `Content-Type: text/event-stream`, `Cache-Control`, and
  `X-Accel-Buffering: no` (for nginx) are set for you.

Full details: [../guides/streaming.md](../guides/streaming.md).

## A complete OpenAI-compatible streaming endpoint

The same endpoint three times: `POST /v1/chat/completions`, bearer-token auth,
request validation, OpenAI `chat.completion.chunk` events, and a final
`data: [DONE]`. A stand-in generator plays the model.

### FastAPI

```python
import asyncio
import json
import os
import secrets
import time
from typing import Literal

from fastapi import Depends, FastAPI, Header, HTTPException, Request
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field

API_KEY = os.environ["API_KEY"]
app = FastAPI()


class Message(BaseModel):
    role: Literal["system", "user", "assistant"]
    content: str


class ChatRequest(BaseModel):
    model: str = Field(min_length=1)
    messages: list[Message] = Field(min_length=1)
    stream: bool = False


def require_api_key(authorization: str = Header(default="")):
    if not secrets.compare_digest(authorization.encode(), f"Bearer {API_KEY}".encode()):
        raise HTTPException(status_code=401, detail="missing or invalid API key")


async def generate(req: ChatRequest):
    for word in "This text stands in for a model's output.".split():
        await asyncio.sleep(0.05)
        yield word + " "


@app.post("/v1/chat/completions", dependencies=[Depends(require_api_key)])
async def chat_completions(req: ChatRequest, request: Request):
    if not req.stream:
        raise HTTPException(status_code=400, detail="this endpoint only streams; set stream to true")

    cid = f"chatcmpl-{time.time_ns()}"
    created = int(time.time())

    def chunk(delta, finish_reason=None):
        return {
            "id": cid,
            "object": "chat.completion.chunk",
            "created": created,
            "model": req.model,
            "choices": [{"index": 0, "delta": delta, "finish_reason": finish_reason}],
        }

    async def events():
        yield f"data: {json.dumps(chunk({'role': 'assistant'}))}\n\n"
        async for token in generate(req):
            if await request.is_disconnected():
                return
            yield f"data: {json.dumps(chunk({'content': token}))}\n\n"
        yield f"data: {json.dumps(chunk({}, 'stop'))}\n\n"
        yield "data: [DONE]\n\n"

    return StreamingResponse(events(), media_type="text/event-stream")
```

### Express

```js
import crypto from 'node:crypto';
import express from 'express';

const API_KEY = process.env.API_KEY;
const app = express();
app.use(express.json());

function requireApiKey(req, res, next) {
  const got = Buffer.from(req.get('authorization') ?? '');
  const want = Buffer.from(`Bearer ${API_KEY}`);
  if (got.length !== want.length || !crypto.timingSafeEqual(got, want)) {
    return res.status(401).json({ error: 'missing or invalid API key' });
  }
  next();
}

async function* generate(body) {
  for (const word of "This text stands in for a model's output.".split(' ')) {
    await new Promise((resolve) => setTimeout(resolve, 50));
    yield word + ' ';
  }
}

app.post('/v1/chat/completions', requireApiKey, async (req, res) => {
  const { model, messages, stream } = req.body ?? {};
  if (typeof model !== 'string' || model === '') {
    return res.status(400).json({ error: 'model is required' });
  }
  if (!Array.isArray(messages) || messages.length === 0) {
    return res.status(400).json({ error: 'messages must not be empty' });
  }
  if (messages.some((m) => !['system', 'user', 'assistant'].includes(m?.role))) {
    return res.status(400).json({ error: 'each message role must be system, user, or assistant' });
  }
  if (stream !== true) {
    return res.status(400).json({ error: 'this endpoint only streams; set stream to true' });
  }

  const id = `chatcmpl-${Date.now()}`;
  const created = Math.floor(Date.now() / 1000);
  const chunk = (delta, finish_reason = null) => ({
    id, object: 'chat.completion.chunk', created, model,
    choices: [{ index: 0, delta, finish_reason }],
  });

  res.writeHead(200, { 'Content-Type': 'text/event-stream', 'Cache-Control': 'no-cache' });
  let gone = false;
  res.on('close', () => { gone = true; });
  const send = (obj) => res.write(`data: ${JSON.stringify(obj)}\n\n`); // return value (back-pressure) ignored

  send(chunk({ role: 'assistant' }));
  for await (const token of generate(req.body)) {
    if (gone) return;
    send(chunk({ content: token }));
  }
  send(chunk({}, 'stop'));
  res.write('data: [DONE]\n\n');
  res.end();
});

app.listen(8080);
```

### Photon

```go
package main

import (
	"context"
	"crypto/subtle"
	"iter"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/agenticmarket/photon"
	"github.com/agenticmarket/photon/photonerr"
)

type message struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

type chatRequest struct {
	Model    string    `json:"model"`
	Messages []message `json:"messages"`
	Stream   bool      `json:"stream"`
}

func (req *chatRequest) validate() error {
	if req.Model == "" {
		return photonerr.BadRequest("model is required")
	}
	if len(req.Messages) == 0 {
		return photonerr.BadRequest("messages must not be empty")
	}
	for _, m := range req.Messages {
		switch m.Role {
		case "system", "user", "assistant":
		default:
			return photonerr.BadRequest("each message role must be system, user, or assistant")
		}
	}
	if !req.Stream {
		return photonerr.BadRequest("this endpoint only streams; set stream to true")
	}
	return nil
}

// The OpenAI chat.completion.chunk shape.
type delta struct {
	Role    string `json:"role,omitempty"`
	Content string `json:"content,omitempty"`
}

type choice struct {
	Index        int     `json:"index"`
	Delta        delta   `json:"delta"`
	FinishReason *string `json:"finish_reason"` // null until the last chunk
}

type chunk struct {
	ID      string   `json:"id"`
	Object  string   `json:"object"`
	Created int64    `json:"created"`
	Model   string   `json:"model"`
	Choices []choice `json:"choices"`
}

func main() {
	apiKey := os.Getenv("API_KEY")
	if apiKey == "" {
		log.Fatal("API_KEY is not set")
	}
	if err := newApp(apiKey).Run(":8080"); err != nil {
		log.Fatal(err)
	}
}

// newApp builds the server. It is separate from main so tests can build it too.
func newApp(apiKey string) *photon.Server {
	app := photon.New()
	v1 := app.Group("/v1", requireAPIKey(apiKey))
	v1.POST("/chat/completions", photon.SSE(chatCompletions))
	return app
}

func requireAPIKey(key string) photon.Middleware {
	want := []byte("Bearer " + key)
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			got := []byte(r.Header.Get("Authorization"))
			if subtle.ConstantTimeCompare(got, want) != 1 {
				photon.Error(w, r, photonerr.Unauthorized("missing or invalid API key"))
				return
			}
			next.ServeHTTP(w, r)
		})
	}
}

func chatCompletions(s *photon.Stream, r *http.Request) error {
	var req chatRequest
	if err := photon.DecodeJSON(r, &req); err != nil {
		return err // nothing sent yet: the client gets a 400, 413, or 415
	}
	if err := req.validate(); err != nil {
		return err // still a normal 400
	}

	id := "chatcmpl-" + strconv.FormatInt(time.Now().UnixNano(), 36)
	created := time.Now().Unix()
	send := func(d delta, finish *string) error {
		return s.JSON("", chunk{ // "" sends an unnamed event, as OpenAI does
			ID:      id,
			Object:  "chat.completion.chunk",
			Created: created,
			Model:   req.Model,
			Choices: []choice{{Index: 0, Delta: d, FinishReason: finish}},
		})
	}

	if err := send(delta{Role: "assistant"}, nil); err != nil {
		return err
	}
	for token, err := range generate(r.Context(), req) {
		if err != nil {
			return photonerr.Upstream(err) // after the first event: sent as "event: error"
		}
		if err := send(delta{Content: token}, nil); err != nil {
			return err // the client left; r.Context() is cancelled, so generate stops too
		}
	}
	stop := "stop"
	if err := send(delta{}, &stop); err != nil {
		return err
	}
	return s.Event("", []byte("[DONE]"))
}

// generate stands in for your model client.
func generate(ctx context.Context, req chatRequest) iter.Seq2[string, error] {
	return func(yield func(string, error) bool) {
		for _, word := range strings.Fields("This text stands in for a model's output.") {
			select {
			case <-ctx.Done():
				yield("", ctx.Err())
				return
			case <-time.After(50 * time.Millisecond):
			}
			if !yield(word+" ", nil) {
				return
			}
		}
	}
}
```

Try it:

```sh
API_KEY=dev go run .

curl -N http://localhost:8080/v1/chat/completions \
  -H "Authorization: Bearer dev" \
  -H "Content-Type: application/json" \
  -d '{"model":"demo","stream":true,"messages":[{"role":"user","content":"hi"}]}'
```

### What maps to what

- **`Depends(require_api_key)` / `requireApiKey`** became `requireAPIKey`, attached
  to the `/v1` group, so every route added to the group later is protected too.
- **The Pydantic model and the Express checks** became a struct plus
  `validate()`. A validation error returned before the first event is a normal
  400, the same moment FastAPI would have answered 422.
- **The async generator** became `generate`, an iterator. `async for` became
  `for token, err := range`.
- **`request.is_disconnected()` / `res.on('close')`** became the error from
  `send`. When the client leaves, `r.Context()` is cancelled and `generate`
  stops on its next step, so you stop paying for tokens nobody reads.
- **`json.dumps` / `JSON.stringify` plus `data: ...\n\n`** became `s.JSON("", v)`.
- **Error handling mid-stream**, which neither original has, is the
  `photonerr.Upstream(err)` return.
- **Back-pressure, heartbeats, the stream cap, and graceful shutdown** have no
  lines in the Go version because Photon does them.

### Supporting `stream: false`

The endpoint above only streams. To serve both modes from one route, decode in a
plain handler and hand the decoded request to `photon.SSE` only when streaming:

```go
v1.POST("/chat/completions", func(w http.ResponseWriter, r *http.Request) {
	var req chatRequest
	if err := photon.DecodeJSON(r, &req); err != nil {
		photon.Error(w, r, err)
		return
	}
	if err := req.validate(); err != nil { // with the Stream check removed
		photon.Error(w, r, err)
		return
	}
	if !req.Stream {
		completeOnce(w, r, req) // collect the tokens, then photon.JSON one chat.completion
		return
	}
	photon.SSE(func(s *photon.Stream, r *http.Request) error {
		return streamChat(s, r, req) // the loop from chatCompletions, minus the decoding
	})(w, r)
})
```

## Background work

FastAPI's `BackgroundTasks` and Node's un-awaited promises become goroutines.
Two rules:

```go
app.POST("/documents/:id/index", func(w http.ResponseWriter, r *http.Request) {
	id := photon.CopyParam(r, "id")           // 1. copy path params that outlive the handler
	ctx := context.WithoutCancel(r.Context()) // 2. keep context values, drop the cancellation
	go indexDocument(ctx, id)
	_ = photon.NoContent(w)
})
```

1. `photon.PathParam` returns a string that aliases request memory and is valid
   only until the handler returns. `photon.CopyParam` makes a copy you can keep.
   The race detector won't catch a mistake here.
2. `r.Context()` is cancelled the moment the handler returns, which would cancel
   the background work. `context.WithoutCancel` keeps the values without the
   cancellation.

Don't use `w` or `r` from the goroutine. Also note that graceful shutdown waits
for requests and streams, not for goroutines you start yourself; if those must
finish, track them (a `sync.WaitGroup`) or use a real queue.

## Startup, shutdown, and deployment

- **Startup.** FastAPI's `lifespan` and code before `app.listen` become ordinary
  code in `main` before `app.Run`: open the database, build the model client,
  register routes.
- **Workers.** There is no worker count to tune. One Go process runs requests in
  parallel on all cores. Drop uvicorn's `--workers` and Node's cluster setup.
- **Shutdown.** `app.Run` handles SIGINT and SIGTERM: it stops accepting
  connections, tells active streams through `s.Done()`, lets in-flight answers
  finish for up to 25 seconds (which fits inside Kubernetes' default 30-second
  grace period), then closes what's left. Change it with
  `photon.New(photon.ShutdownTimeout(d))`.
- **Building.** `go build -o server .` produces one binary with no runtime to
  install. Copy it into a minimal container image.
- **Proxies.** `photon.ClientIP(r)` returns the socket peer and ignores
  `X-Forwarded-For`, unlike uvicorn's `--proxy-headers` or Express's
  `trust proxy`. Behind a load balancer, decide explicitly how you establish the
  client address.

## Testing

`TestClient` and supertest become `net/http/httptest`. Because the app is built
in `newApp`, a test can run it for real and read the stream:

```go
func TestChatCompletionsStreams(t *testing.T) {
	srv := httptest.NewServer(newApp("test-key"))
	defer srv.Close()

	body := `{"model":"demo","stream":true,"messages":[{"role":"user","content":"hi"}]}`
	req, err := http.NewRequest(http.MethodPost, srv.URL+"/v1/chat/completions", strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Authorization", "Bearer test-key")
	req.Header.Set("Content-Type", "application/json")

	res, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		t.Fatalf("status = %d", res.StatusCode)
	}

	var last string
	sc := bufio.NewScanner(res.Body)
	for sc.Scan() {
		if data, ok := strings.CutPrefix(sc.Text(), "data: "); ok {
			last = data
		}
	}
	if last != "[DONE]" {
		t.Fatalf("last data line = %q, want [DONE]", last)
	}
}
```

Add a test that the endpoint answers 401 without the header. That is the
reliable way to check that authentication covers a route.

## Gotchas

- **No OpenAPI generation.** FastAPI's `/docs` and `openapi.json` have no
  equivalent. If clients, SDK generators, or internal tools depend on them, keep
  a hand-written spec or generate one separately.
- **No validation framework.** Pydantic constraints become Go checks. There is
  no type coercion, and validation failures are 400 unless you choose 422.
- **The 1 MiB body limit.** Long conversation histories and base64-encoded
  images in vision requests can pass 1 MiB. Raise it deliberately:

  ```go
  l := photon.DefaultLimits()
  l.MaxRequestBodyBytes = 8 << 20 // 8 MiB
  app := photon.New(photon.SetLimits(l))
  ```

- **Header limits.** 16 KiB of headers in total, 8 KiB per value, 100 fields.
  Very large JWTs or cookies get 431.
- **Error bodies change shape** to `application/problem+json`. Clients reading
  `detail` or `error` need updating, or keep writing the old shape yourself.
- **Path parameters are short-lived.** `photon.PathParam` is valid only during
  the handler. Use `photon.CopyParam` for anything you keep.
- **FastAPI's `{id}` becomes `:id`.** A route copied as `/users/{user_id}` is
  refused at registration with a message giving the Photon spelling,
  `/users/:user_id`.
- **Route conflicts panic at startup.** `/users/me` beside `/users/:id` is an
  error. Move one, or branch on the value inside the `:id` handler.
- **No trailing-slash redirects.** `/v1/models/` is a 404.
- **`GET` does not answer `HEAD`.** Register `app.HEAD` for routes that health
  checkers probe with `HEAD`.
- **No WebSockets in Photon.** If you used FastAPI's `@app.websocket` or Node's
  `ws`, use `github.com/coder/websocket` on a Photon route; it works on any
  `http.Handler`.
- **Heartbeat comments.** After 15 seconds of silence the stream carries SSE
  comment lines (lines starting with `:`). Standard SSE clients ignore them;
  check any hand-written parser.
- **Routes are fixed once the server starts.** Register everything in `newApp`
  or `main`.

## Checklist

- [ ] List every route, parameter, and response shape in the current service
- [ ] Translate routes (`{id}` to `:id`) and build the app in a test to catch conflicts
- [ ] Turn each Pydantic model or schema into a struct plus a `validate` method
- [ ] Decide on 400 versus 422 for validation errors
- [ ] Map `HTTPException` / `res.status(...)` to `photonerr` and `photon.Error`
- [ ] Turn auth dependencies into middleware on a group
- [ ] Turn resource dependencies into struct fields with method handlers
- [ ] Turn each async generator into an iterator or channel that respects `ctx`
- [ ] Replace `StreamingResponse` / `EventSourceResponse` / `res.write` with `photon.SSE`, returning the error from every `s.Event`
- [ ] Raise `MaxRequestBodyBytes` if prompts or images can exceed 1 MiB
- [ ] Copy path parameters and detach contexts for background goroutines
- [ ] Replace your OpenAPI docs if anything depends on them
- [ ] Tell client teams about the error format
- [ ] Load-test with realistic concurrent stream counts and check `MaxStreams`
