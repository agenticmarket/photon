# Getting started

From an empty directory to a streaming endpoint, with tests, in about ten
minutes. You need Go 1.24 or newer and nothing else.

- [1. See it work](#1-see-it-work)
- [2. Your first server](#2-your-first-server)
- [3. Routes and JSON](#3-routes-and-json)
- [4. Errors](#4-errors)
- [5. Your first stream](#5-your-first-stream)
- [6. Middleware and groups](#6-middleware-and-groups)
- [7. Limits](#7-limits)
- [8. A test](#8-a-test)
- [9. Running it for real](#9-running-it-for-real)
- [Where next](#where-next)

## 1. See it work

The `photon` CLI includes a demo service. It is the fastest way to see what
"streaming done right" looks like before writing any code:

```bash
go install github.com/agenticmarket/photon/cmd/photon@latest
photon serve
```

Open <http://localhost:8484> and press **Stream**: tokens appear one at a time,
and the dashboard shows the time to the first one. From a terminal:

```bash
curl -N localhost:8484/agent/stream                       # a token stream
curl -N localhost:8484/v1/chat/completions \
  -H 'Content-Type: application/json' \
  -d '{"model":"photon-demo","stream":true,"messages":[{"role":"user","content":"hi"}]}'
photon bench -c 20 'http://localhost:8484/agent/stream?burst=true'
```

(`-N` stops curl from buffering. Without it a stream looks like it arrives all
at once — which is exactly the bug Photon exists to prevent, so it is a
confusing thing to see.)

## 2. Your first server

```bash
mkdir hello && cd hello
go mod init hello
go get github.com/agenticmarket/photon
```

`main.go`:

```go
package main

import (
	"log"
	"net/http"

	"github.com/agenticmarket/photon"
)

func main() {
	app := photon.New()

	app.GET("/", func(w http.ResponseWriter, r *http.Request) {
		photon.Text(w, http.StatusOK, "hello\n")
	})

	log.Println("listening on http://localhost:8080")
	log.Fatal(app.Run(":8080"))
}
```

```bash
go run .
curl localhost:8080/          # hello
```

Three things are already true that you did not write:

- **Limits are on.** Bodies over 1 MiB, more than 100 header fields, and
  clients that take more than 5 seconds to send their headers are refused.
- **Shutdown is graceful.** Ctrl-C stops accepting connections and waits for
  in-flight requests (up to 25 s) before exiting. Press it twice to exit at once.
- **Handlers are plain `net/http`.** `func(w http.ResponseWriter, r
  *http.Request)` — the same signature as the standard library, so any
  existing handler or middleware works.

## 3. Routes and JSON

```go
type Todo struct {
	ID    int    `json:"id"`
	Title string `json:"title"`
}

app.GET("/todos/:id", func(w http.ResponseWriter, r *http.Request) {
	id := photon.PathParam(r, "id") // also available as r.PathValue("id")
	photon.JSON(w, http.StatusOK, Todo{ID: 1, Title: "todo " + id})
})

app.POST("/todos", func(w http.ResponseWriter, r *http.Request) {
	var t Todo
	if err := photon.DecodeJSON(r, &t); err != nil {
		photon.Error(w, r, err) // 400, 413, or 415 - already the right one
		return
	}
	photon.JSON(w, http.StatusCreated, t)
})
```

Patterns are `:name` for one segment and `*name` for the rest of the path:
`/files/*path` matches `/files/a/b.txt` with `path` = `a/b.txt`.

Photon refuses ambiguous routes when you register them:

```go
app.GET("/todos/:id", getTodo)
app.GET("/todos/new", newTodoForm) // panics at startup: which one serves /todos/new?
```

It feels strict the first time. It is the same strictness that stops a typo in
a route from silently deciding who can see the admin page. Fix it by giving the
two routes distinct shapes, for example `/todos/:id` and `/todo-forms/new`. More
in [routing](guides/routing.md).

## 4. Errors

Return errors from the [`photonerr`](API.md#errors-package-photonerr) package
and let `photon.Error` write them:

```go
import "github.com/agenticmarket/photon/photonerr"

app.GET("/todos/:id", func(w http.ResponseWriter, r *http.Request) {
	todo, err := store.Get(r.Context(), photon.PathParam(r, "id"))
	switch {
	case errors.Is(err, sql.ErrNoRows):
		photon.Error(w, r, photonerr.NotFound("todo")) // 404 "todo not found"
		return
	case err != nil:
		photon.Error(w, r, err) // 500, constant body; the real error is logged
		return
	}
	photon.JSON(w, http.StatusOK, todo)
})
```

Every error response is an [RFC 9457](https://www.rfc-editor.org/rfc/rfc9457)
problem document:

```json
{"type":"https://photon.agenticmarket.dev/errors/not_found","title":"todo not found","status":404,"code":"not_found"}
```

The message comes from your code, never from the request, so error bodies
cannot leak internals or reflect attacker input. An unknown error — a database
error with a connection string in it — becomes a 500 with a constant body, and
its full text goes to the log instead.

## 5. Your first stream

```go
app.GET("/count", photon.SSE(func(s *photon.Stream, r *http.Request) error {
	for i := 1; i <= 10; i++ {
		if err := s.Event("count", []byte(strconv.Itoa(i))); err != nil {
			return err // the client left
		}
		time.Sleep(500 * time.Millisecond)
	}
	return s.Event("done", nil)
}))
```

```bash
curl -N localhost:8080/count
```

```text
event: count
data: 1

event: count
data: 2
...
```

Close curl halfway. `s.Event` returns `photon.ErrClientGone`, the function
returns, and — if you passed `r.Context()` to whatever produces the events — the
upstream work stops too.

In a browser:

```js
const es = new EventSource("/count");
es.addEventListener("count", (e) => console.log(e.data));
es.addEventListener("done", () => es.close());
```

That is most of what an AI endpoint needs. Replace the loop with a model call:

```go
app.POST("/chat", photon.SSE(func(s *photon.Stream, r *http.Request) error {
	var req struct{ Prompt string `json:"prompt"` }
	if err := photon.DecodeJSON(r, &req); err != nil {
		return err // before the first event: the client gets a 400
	}
	for tok := range llm.Stream(r.Context(), req.Prompt) {
		if err := s.JSON("token", map[string]string{"text": tok}); err != nil {
			return err
		}
	}
	return s.Event("done", nil)
}))
```

Errors returned **before** the first event become normal HTTP errors; errors
returned **after** it are sent as an `error` event. Slow clients, heartbeats,
the stream limit, and shutdown are handled for you — the
[streaming guide](guides/streaming.md) explains each.

## 6. Middleware and groups

Middleware is `func(http.Handler) http.Handler`, the standard shape:

```go
func logRequests(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		next.ServeHTTP(w, r)
		slog.Info("request", "method", r.Method, "route", r.Pattern, "took", time.Since(start))
	})
}

app.Use(logRequests) // every request, including 404s
```

A group shares a prefix and middleware, which makes it the right home for
authentication — a route inside cannot forget it, and a route outside cannot
pick it up by accident:

```go
api := app.Group("/api", requireToken)
api.GET("/todos", listTodos)
api.POST("/todos", createTodo)
```

Existing `http.Handler`s mount under a prefix for every method:

```go
app.Mount("/debug/pprof", http.DefaultServeMux) // with net/http/pprof imported
```

More in [middleware](guides/middleware.md).

## 7. Limits

The defaults suit a small internet-facing service. Change one by starting from
them:

```go
limits := photon.DefaultLimits()
limits.MaxRequestBodyBytes = 10 << 20 // 10 MiB, for file uploads
limits.MaxConcurrentRequests = 2000

app := photon.New(
	photon.SetLimits(limits),
	photon.Logger(slog.Default()), // also log rejected requests
)
```

Every limit, its default, and when to change it:
[configuration](guides/configuration.md).

## 8. A test

The server is an `http.Handler`, so the standard library tests it:

```go
func TestGetTodo(t *testing.T) {
	app := newApp() // build routes in a function you can call from tests
	rec := httptest.NewRecorder()
	app.ServeHTTP(rec, httptest.NewRequest("GET", "/todos/7", nil))

	if rec.Code != http.StatusOK {
		t.Fatalf("status %d, body %s", rec.Code, rec.Body)
	}
}
```

Building the app in a test also proves your routes do not conflict, because a
conflict panics during construction. More in [testing](guides/testing.md).

## 9. Running it for real

```bash
CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o server .
GOMEMLIMIT=450MiB ./server        # in a 512 MB container
```

Photon reads the memory limit — `GOMEMLIMIT`, or the container's cgroup limit —
to decide how many concurrent streams it can afford. Behind nginx, turn off
response buffering for streams. Dockerfiles, systemd units, Kubernetes probes,
and proxy settings are in [deployment](guides/deployment.md).

## Where next

- **Building an AI backend?** [AI backends](guides/ai-backends.md): an
  OpenAI-compatible endpoint, proxying a model, agent events, MCP.
- **Moving an existing service?** [Migration guides](migration/README.md) for
  net/http, chi, gin, echo, Fiber, FastAPI, and Express.
- **Want a working project?** `photon init my-app -template agent`, or browse
  [the examples](../examples/).
- **Everything else:** [the documentation index](README.md).
