# Testing

A photon server is an `http.Handler`, so the standard library's `httptest`
package is all you need: build the app, call `app.ServeHTTP` with a recorded
response, and assert on the result. This guide shows how to structure an app
for testing, then how to test routes, conflicts, middleware wiring, limits, and
streams, and ends with the commands the photon repository itself runs.

- [Build the app in one function](#build-the-app-in-one-function)
- [Testing handlers with httptest](#testing-handlers-with-httptest)
- [Table-driven route tests](#table-driven-route-tests)
- [Testing a handler on its own](#testing-a-handler-on-its-own)
- [Testing that routes do not conflict](#testing-that-routes-do-not-conflict)
- [Testing middleware wiring](#testing-middleware-wiring)
- [Testing limits](#testing-limits)
- [Testing streams](#testing-streams)
- [The race detector](#the-race-detector)
- [The repository's own tests](#the-repositorys-own-tests)

## Build the app in one function

Put every route, middleware, and option in one function that `main` calls and
your tests call too. Tests then exercise the real registration, in the real
order, rather than a copy of it.

```go
// app.go
package main

import (
	"net/http"
	"strings"

	"github.com/agenticmarket/photon"
	"github.com/agenticmarket/photon/photonerr"
)

// newApp builds the whole server. main calls newApp(); tests pass options
// such as photon.MemoryBudget to make the server independent of the host.
func newApp(opts ...photon.Option) *photon.Server {
	app := photon.New(opts...)
	app.GET("/healthz", func(w http.ResponseWriter, r *http.Request) {
		_ = photon.Text(w, http.StatusOK, "ok\n")
	})
	app.GET("/users/:id", getUser)
	app.DELETE("/users/:id", deleteUser)
	app.POST("/v1/chat", photon.SSE(chat))
	return app
}

func getUser(w http.ResponseWriter, r *http.Request) {
	id := photon.PathParam(r, "id")
	if id == "0" {
		photon.Error(w, r, photonerr.NotFound("user"))
		return
	}
	_ = photon.JSON(w, http.StatusOK, map[string]string{"id": id})
}

func deleteUser(w http.ResponseWriter, r *http.Request) {
	_ = photon.NoContent(w)
}

type chatRequest struct {
	Prompt string `json:"prompt"`
}

func chat(s *photon.Stream, r *http.Request) error {
	var req chatRequest
	if err := photon.DecodeJSON(r, &req); err != nil {
		return err // nothing sent yet: the client gets a 400, 413, or 415
	}
	if req.Prompt == "" {
		return photonerr.BadRequest("prompt is required")
	}
	for _, word := range strings.Fields(req.Prompt) {
		if err := s.Event("token", []byte(word)); err != nil {
			return err
		}
	}
	return s.Event("done", nil)
}
```

Tests build the app with `photon.MemoryBudget(256 << 20)`. Without it, `New`
derives `MaxStreams` from the memory of whatever machine runs the test, so a
test that depends on the stream limit could pass on your laptop and fail in CI.

Build a fresh app in each test. It is cheap, and state such as a rate limiter's
buckets then cannot leak from one test into another.

## Testing handlers with httptest

The tests below live in `app_test.go`, with these imports:

```go
package main

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/agenticmarket/photon"
)
```

The basic shape:

```go
func TestHealthz(t *testing.T) {
	app := newApp(photon.MemoryBudget(256 << 20))

	rec := httptest.NewRecorder()
	app.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/healthz", nil))

	if rec.Code != http.StatusOK || rec.Body.String() != "ok\n" {
		t.Fatalf("got %d %q", rec.Code, rec.Body.String())
	}
}
```

Calling `ServeHTTP` directly runs everything photon does per request: the
header, body, and concurrency limits, routing, middleware, panic recovery, and
the stream limits. It does not freeze the router, so a test can register extra
routes after building the app.

What it does not run are the settings that belong to the listener and the
underlying `http.Server`: `MaxConnections`, `MaxPerIP`, `MaxHeaderBytes`,
`ReadHeaderTimeout`, and `IdleTimeout`. `httptest.NewServer(app)` does not run
them either, because it uses its own `http.Server`. To test those, serve the
app on a real listener:

```go
ln, err := net.Listen("tcp", "127.0.0.1:0")
if err != nil {
	t.Fatal(err)
}
go app.Serve(ln) // freezes the router; Serve can be called once per server
t.Cleanup(func() {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = app.Shutdown(ctx)
})
base := "http://" + ln.Addr().String()
```

## Table-driven route tests

One table covers the success paths and the router's own answers: 404, 405 with
its `Allow` header, and the cases where photon deliberately differs from
`ServeMux`.

```go
func TestRoutes(t *testing.T) {
	app := newApp(photon.MemoryBudget(256 << 20))

	tests := []struct {
		method, target string
		want           int
		allow          string // the Allow header expected on a 405
	}{
		{"GET", "/healthz", 200, ""},
		{"GET", "/users/42", 200, ""},
		{"GET", "/users/0", 404, ""},
		{"DELETE", "/users/42", 204, ""},
		{"PUT", "/users/42", 405, "DELETE, GET"},
		{"HEAD", "/healthz", 405, "GET"}, // HEAD is not derived from GET
		{"GET", "/users/", 404, ""},      // a trailing slash is not redirected
		{"GET", "/users%2F42", 404, ""},  // an encoded slash is not a separator
		{"GET", "/nope", 404, ""},
	}
	for _, tc := range tests {
		t.Run(tc.method+" "+tc.target, func(t *testing.T) {
			rec := httptest.NewRecorder()
			app.ServeHTTP(rec, httptest.NewRequest(tc.method, tc.target, nil))
			if rec.Code != tc.want {
				t.Fatalf("status = %d, want %d; body %q", rec.Code, tc.want, rec.Body.String())
			}
			if got := rec.Header().Get("Allow"); got != tc.allow {
				t.Errorf("Allow = %q, want %q", got, tc.allow)
			}
		})
	}
}
```

## Testing a handler on its own

Photon stores path parameters with the standard `Request.SetPathValue`, so a
handler can be tested without the router by setting the value yourself:

```go
func TestGetUser(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/users/42", nil)
	req.SetPathValue("id", "42") // what the router does on a match

	rec := httptest.NewRecorder()
	getUser(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
}
```

Prefer going through `app.ServeHTTP` when middleware matters, which is most of
the time.

## Testing that routes do not conflict

Two routes that could match the same request, a malformed pattern, or a nil
handler all panic during registration. So the cheapest test of your entire
route table is to build the app:

```go
// TestAppBuilds fails if any two routes conflict, a pattern is malformed, or
// a handler is nil, and prints photon's message naming both registration lines.
func TestAppBuilds(t *testing.T) {
	defer func() {
		if v := recover(); v != nil {
			t.Fatalf("building the app panicked:\n%v", v)
		}
	}()
	newApp(photon.MemoryBudget(256 << 20))
}
```

Every other test that calls `newApp` would fail too, but this one says why in
its name. Recovering keeps the panic from aborting the rest of the test binary.

To assert that a conflict **is** detected, for example that nothing can be
registered over an existing route, check the panic message. The panic value is
not an exported type, so match on its text:

```go
func mustPanic(t *testing.T, want string, f func()) {
	t.Helper()
	defer func() {
		v := recover()
		if v == nil {
			t.Fatalf("expected a panic containing %q", want)
		}
		if msg := fmt.Sprint(v); !strings.Contains(msg, want) {
			t.Fatalf("panic %q does not contain %q", msg, want)
		}
	}()
	f()
}

func TestUsersNewWouldConflict(t *testing.T) {
	app := newApp(photon.MemoryBudget(256 << 20))
	mustPanic(t, "conflicting routes", func() {
		app.GET("/users/new", func(w http.ResponseWriter, r *http.Request) {})
	})
}
```

A snapshot of the route table catches routes that were added or removed by
accident, which matters when a route is an endpoint someone depends on:

```go
func TestRouteTable(t *testing.T) {
	app := newApp(photon.MemoryBudget(256 << 20))
	var got []string
	for _, rt := range app.Routes() {
		got = append(got, rt.Method+" "+rt.Path)
	}
	want := []string{
		"GET /healthz",
		"GET /users/:id",
		"DELETE /users/:id",
		"POST /v1/chat",
	}
	if !slices.Equal(got, want) {
		t.Errorf("routes changed:\n got %q\nwant %q", got, want)
	}
}
```

## Testing middleware wiring

`RouteInfo` does not say which middleware a route has, so the only reliable
proof that a route is protected is a request. For an app whose `/v1` group
uses the bearer-token middleware from the
[middleware guide](middleware.md#bearer-token-auth-in-a-group), this test sends
an unauthenticated request to **every** route under `/v1`, so a route added
outside the group later fails it:

```go
func TestEveryV1RouteRequiresAuth(t *testing.T) {
	app := newApp(photon.MemoryBudget(256 << 20))
	for _, rt := range app.Routes() {
		if !strings.HasPrefix(rt.Path, "/v1/") {
			continue
		}
		rec := httptest.NewRecorder()
		app.ServeHTTP(rec, httptest.NewRequest(rt.Method, concrete(rt.Path), nil))
		if rec.Code != http.StatusUnauthorized {
			t.Errorf("%s %s without credentials = %d, want 401", rt.Method, rt.Path, rec.Code)
		}
	}
}

// concrete turns a route pattern into a path that matches it.
func concrete(pattern string) string {
	segs := strings.Split(pattern, "/")
	for i, s := range segs {
		if strings.HasPrefix(s, ":") || strings.HasPrefix(s, "*") {
			segs[i] = "x"
		}
	}
	return strings.Join(segs, "/")
}
```

Pair it with a test that a valid credential gets through. A middleware that
refuses everything passes the test above.

## Testing limits

Limits are enforced in `ServeHTTP`, so they are testable with a recorder. A
request with 101 header fields exceeds the default `MaxHeaders` of 100. Photon
counts fields, not names, so 101 values of one header count as 101:

```go
func TestTooManyHeaders(t *testing.T) {
	app := newApp(photon.MemoryBudget(256 << 20))
	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	for i := range 101 {
		req.Header.Add("X-Test", strconv.Itoa(i))
	}
	rec := httptest.NewRecorder()
	app.ServeHTTP(rec, req)

	if rec.Code != http.StatusRequestHeaderFieldsTooLarge {
		t.Fatalf("status = %d, want 431", rec.Code)
	}
	var problem struct {
		Code  string `json:"code"`
		Limit string `json:"limit"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &problem); err != nil {
		t.Fatal(err)
	}
	if problem.Code != "too_many_headers" || problem.Limit != "MaxHeaders" {
		t.Errorf("problem = %+v", problem)
	}
}
```

A body over `MaxRequestBodyBytes` (1 MiB) is refused with 413. `httptest`
sets `Content-Length` for a `strings.Reader`, so this exercises the path where
photon refuses before reading; set `req.ContentLength = -1` to exercise the
streaming-upload path, where the limit is enforced while the handler reads:

```go
func TestBodyTooLarge(t *testing.T) {
	app := newApp(photon.MemoryBudget(256 << 20))
	for _, length := range []string{"declared", "unknown"} {
		body := `{"prompt":"` + strings.Repeat("a", 1<<20) + `"}`
		req := httptest.NewRequest(http.MethodPost, "/v1/chat", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		if length == "unknown" {
			req.ContentLength = -1
		}
		rec := httptest.NewRecorder()
		app.ServeHTTP(rec, req)
		if rec.Code != http.StatusRequestEntityTooLarge {
			t.Errorf("%s length: status = %d, want 413", length, rec.Code)
		}
	}
}
```

The stream limit needs two requests in flight at once, so it uses a real
server. With `MaxStreams(1)`, a second stream is refused with 503 while the
first is open:

```go
func TestStreamLimit(t *testing.T) {
	hold := make(chan struct{})
	release := sync.OnceFunc(func() { close(hold) })

	app := photon.New(photon.MemoryBudget(256<<20), photon.MaxStreams(1))
	app.GET("/s", photon.SSE(func(s *photon.Stream, r *http.Request) error {
		if err := s.Event("open", nil); err != nil {
			return err
		}
		<-hold
		return nil
	}))
	srv := httptest.NewServer(app)
	defer srv.Close()
	defer release() // runs before srv.Close, which waits for the handler

	first, err := http.Get(srv.URL + "/s")
	if err != nil {
		t.Fatal(err)
	}
	defer first.Body.Close()

	second, err := http.Get(srv.URL + "/s")
	if err != nil {
		t.Fatal(err)
	}
	second.Body.Close()
	if second.StatusCode != http.StatusServiceUnavailable || second.Header.Get("Retry-After") == "" {
		t.Errorf("second stream: %d, Retry-After %q; want 503 with Retry-After",
			second.StatusCode, second.Header.Get("Retry-After"))
	}
}
```

`http.Get` returns once the response headers arrive, and photon sends them with
the first event, so by then the first stream holds the only slot.

## Testing streams

### Reading events

A small parser turns a response body into the events a client would see. It
skips comments (photon's heartbeats) and frames without data (a `retry` hint):

```go
// sse_test.go
package main

import (
	"bufio"
	"io"
	"strings"
	"testing"
)

// event is one server-sent event as a client receives it. ID is the id sent
// with this event, if any.
type event struct {
	ID, Name, Data string
}

// readEvent reads the next event from br. It returns io.EOF when the stream
// ends between events, and io.ErrUnexpectedEOF when it ends inside one.
func readEvent(br *bufio.Reader) (event, error) {
	var ev event
	var data []string
	started := false
	for {
		line, err := br.ReadString('\n')
		if err == io.EOF && line == "" && !started {
			return event{}, io.EOF
		}
		if err == io.EOF {
			return event{}, io.ErrUnexpectedEOF
		}
		if err != nil {
			return event{}, err
		}
		line = strings.TrimRight(line, "\r\n")
		if line == "" {
			if data != nil {
				ev.Data = strings.Join(data, "\n")
				return ev, nil
			}
			ev, started = event{}, false // a frame with no data is not an event
			continue
		}
		started = true
		if strings.HasPrefix(line, ":") {
			continue // a comment
		}
		field, value, _ := strings.Cut(line, ":")
		value = strings.TrimPrefix(value, " ")
		switch field {
		case "event":
			ev.Name = value
		case "id":
			ev.ID = value
		case "data":
			data = append(data, value)
		}
	}
}

// readEvents reads every event until the stream ends.
func readEvents(t *testing.T, r io.Reader) []event {
	t.Helper()
	br := bufio.NewReader(r)
	var events []event
	for {
		ev, err := readEvent(br)
		if err == io.EOF {
			return events
		}
		if err != nil {
			t.Fatalf("reading the stream: %v", err)
		}
		events = append(events, ev)
	}
}
```

`bufio.Reader` has no line-length limit, so it also handles large events.

### Streams with a recorder

With `httptest.NewRecorder`, `ServeHTTP` returns after the stream has ended
and everything has been written, so the whole stream is in the body:

```go
func TestChatStreamsTokens(t *testing.T) {
	app := newApp(photon.MemoryBudget(256 << 20))
	req := httptest.NewRequest(http.MethodPost, "/v1/chat",
		strings.NewReader(`{"prompt":"hello there"}`))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	app.ServeHTTP(rec, req)

	if ct := rec.Header().Get("Content-Type"); ct != "text/event-stream; charset=utf-8" {
		t.Fatalf("Content-Type = %q", ct)
	}
	want := []event{
		{Name: "token", Data: "hello"},
		{Name: "token", Data: "there"},
		{Name: "done"},
	}
	if got := readEvents(t, rec.Body); !slices.Equal(got, want) {
		t.Errorf("events = %+v, want %+v", got, want)
	}
}
```

An error returned before the first event is an ordinary HTTP error, so the
same recorder tests it:

```go
func TestChatRejectsBadInput(t *testing.T) {
	app := newApp(photon.MemoryBudget(256 << 20))
	for _, tc := range []struct {
		contentType, body string
		want              int
	}{
		{"application/json", `{"prompt":""}`, 400},
		{"application/json", `{"prompt":`, 400},
		{"text/plain", `{"prompt":"hi"}`, 415},
	} {
		req := httptest.NewRequest(http.MethodPost, "/v1/chat", strings.NewReader(tc.body))
		req.Header.Set("Content-Type", tc.contentType)
		rec := httptest.NewRecorder()
		app.ServeHTTP(rec, req)
		if rec.Code != tc.want {
			t.Errorf("%s %s: status = %d, want %d", tc.contentType, tc.body, rec.Code, tc.want)
		}
	}
}
```

### Streams over a real socket

A recorder shows the final body, not when each event arrived. To test that a
token reaches the client while the stream is still open, use
`httptest.NewServer` and read the body as it comes. `s.Flush()` blocks until
everything sent so far has been written to the connection, which makes the
ordering deterministic: the test cannot pass by an accident of timing.

```go
func TestFirstTokenArrivesWhileStreaming(t *testing.T) {
	hold := make(chan struct{})
	release := sync.OnceFunc(func() { close(hold) })

	app := photon.New(photon.MemoryBudget(256 << 20))
	app.GET("/slow", photon.SSE(func(s *photon.Stream, r *http.Request) error {
		if err := s.Event("token", []byte("first")); err != nil {
			return err
		}
		if err := s.Flush(); err != nil { // "first" is on the wire before we block
			return err
		}
		select {
		case <-hold:
		case <-r.Context().Done():
			return r.Context().Err()
		}
		return s.Event("token", []byte("second"))
	}))
	srv := httptest.NewServer(app)
	defer srv.Close()
	defer release()

	resp, err := http.Get(srv.URL + "/slow")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	br := bufio.NewReader(resp.Body)

	if ev, err := readEvent(br); err != nil || ev.Data != "first" {
		t.Fatalf("first event = %+v, %v", ev, err)
	}
	release() // the handler was still blocked when "first" arrived
	if ev, err := readEvent(br); err != nil || ev.Data != "second" {
		t.Fatalf("second event = %+v, %v", ev, err)
	}
}
```

### The client leaving stops the work

The property that saves money in an AI backend is that a handler stops when
its client disconnects. Test it by cancelling the client's request and checking
that the handler's next write fails with `photon.ErrClientGone`:

```go
func TestClientDisconnectStopsTheHandler(t *testing.T) {
	stopped := make(chan error, 1)
	app := photon.New(photon.MemoryBudget(256 << 20))
	app.GET("/feed", photon.SSE(func(s *photon.Stream, r *http.Request) error {
		for i := 0; ; i++ {
			if err := s.Event("tick", []byte(strconv.Itoa(i))); err != nil {
				stopped <- err
				return err
			}
			time.Sleep(10 * time.Millisecond)
		}
	}))
	srv := httptest.NewServer(app)
	defer srv.Close()

	ctx, cancel := context.WithCancel(context.Background())
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, srv.URL+"/feed", nil)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := readEvent(bufio.NewReader(resp.Body)); err != nil {
		t.Fatal(err)
	}
	cancel() // the client goes away
	resp.Body.Close()

	select {
	case err := <-stopped:
		if !errors.Is(err, photon.ErrClientGone) {
			t.Errorf("handler stopped with %v, want ErrClientGone", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the handler kept producing after the client left")
	}
}
```

In a real handler the same cancellation reaches your model call through
`r.Context()`; see [streaming](../guides/streaming.md#when-the-client-goes-away).

## The race detector

```sh
go test -race ./...
```

Run it in CI. It needs cgo, so set `CGO_ENABLED=1` and have a C compiler
available (gcc or clang; on Windows, MinGW-w64 or LLVM-MinGW). Tests run
several times slower under it.

The detector reports only races on code paths your tests actually execute, so
it is most useful with tests that make concurrent requests, such as several
goroutines calling an `httptest.NewServer`. It cannot find mistakes that are
not unsynchronised memory access. Keeping a `PathParam` value past the end of
its handler is one of those: see
[routing](routing.md#keeping-parameters-pathparam-and-copyparam).

## The repository's own tests

From the root of the photon repository:

```sh
go test ./...                     # unit, regression, and allocation tests
go test -race ./...               # the same under the race detector

# Fuzz the router's matcher: any path, against a fixed set of routes, must
# match or miss without panicking, and every captured value must come from the path.
go test ./internal/tree -fuzz=FuzzFind -fuzztime=60s

# Fuzz SSE framing: every event is either refused or decoded by a
# spec-compliant parser to exactly the name, id, and data that were sent.
go test -run '^$' -fuzz FuzzSSEEventRoundTrip -fuzztime=60s .
```

`-run '^$'` skips the ordinary tests so only the fuzz target runs. A fuzz run
targets one package and one fuzz function at a time. Failing inputs are saved
under `testdata/fuzz/` and replayed by every later `go test`, so a bug the
fuzzer finds stays fixed.

## See also

- [Routing](routing.md): conflict rules, 404 and 405
- [Middleware](middleware.md): the auth and CORS middleware these tests exercise
- [Configuration](configuration.md): the limits under test and their defaults
- [Streaming](../guides/streaming.md)
- [Getting started](../getting-started.md)
