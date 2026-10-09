package photon

import (
	"bufio"
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math"
	"net"
	"net/http"
	"net/http/httptest"
	"runtime/debug"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/agenticmarket/photon/photonerr"
)

// This file pins every defect found in the 2026-10 review, one test per
// defect, plus the features added to remove the workarounds those defects
// forced. Each test states the failure it guards against, because a
// regression test whose purpose is forgotten is the first one deleted.

// ---------------------------------------------------------------- SSE parser

// sseEvent is one dispatched event, as an EventSource would deliver it.
type sseEvent struct {
	ID, Event, Data string
}

// sseWire is a parsed stream: the events a spec-compliant client dispatches,
// plus everything else it saw.
type sseWire struct {
	Events   []sseEvent
	Comments int
	Retries  []string
	Unknown  []string // fields a client would ignore; an injection shows up here or as an extra event
}

// parseSSE parses a stream the way the HTML living standard says a client
// must: lines end at CRLF, LF, or a lone CR; a blank line dispatches; data
// fields accumulate with "\n" between them; an event with no data is not
// dispatched. Writing the parser from the specification rather than from the
// encoder is the point - a test that parsed the way the encoder writes would
// agree with any bug the encoder has.
func parseSSE(wire string) sseWire {
	var out sseWire
	var data strings.Builder
	var hasData bool
	event, lastID := "", ""
	lines := splitSSELines(wire)
	for _, line := range lines {
		if line == "" {
			if hasData {
				d := data.String()
				d = strings.TrimSuffix(d, "\n")
				ev := event
				if ev == "" {
					ev = "message"
				}
				out.Events = append(out.Events, sseEvent{ID: lastID, Event: ev, Data: d})
			}
			data.Reset()
			hasData, event = false, ""
			continue
		}
		if strings.HasPrefix(line, ":") {
			out.Comments++
			continue
		}
		field, value, found := strings.Cut(line, ":")
		if found {
			value = strings.TrimPrefix(value, " ")
		}
		switch field {
		case "event":
			event = value
		case "data":
			data.WriteString(value)
			data.WriteByte('\n')
			hasData = true
		case "id":
			if !strings.Contains(value, "\x00") {
				lastID = value
			}
		case "retry":
			out.Retries = append(out.Retries, value)
		default:
			out.Unknown = append(out.Unknown, line)
		}
	}
	return out
}

// splitSSELines splits on CRLF, LF, and lone CR, dropping a trailing partial
// line, which a client holds until it is completed.
func splitSSELines(s string) []string {
	var lines []string
	start := 0
	for i := 0; i < len(s); i++ {
		switch s[i] {
		case '\n':
			lines = append(lines, s[start:i])
			start = i + 1
		case '\r':
			lines = append(lines, s[start:i])
			if i+1 < len(s) && s[i+1] == '\n' {
				i++
			}
			start = i + 1
		}
	}
	return lines
}

// normalizeLineBreaks is what SSE can carry: every line break arrives as LF.
func normalizeLineBreaks(s string) string {
	s = strings.ReplaceAll(s, "\r\n", "\n")
	return strings.ReplaceAll(s, "\r", "\n")
}

// recordStream runs fn against a fresh standalone stream and returns the wire.
func recordStream(t *testing.T, fn func(s *Stream)) string {
	t.Helper()
	rec := httptest.NewRecorder()
	s := newStream(rec, httptest.NewRequest(http.MethodGet, "/x", nil), newFlusher(rec),
		streamConfig{Stall: 2 * time.Second})
	fn(s)
	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	return rec.Body.String()
}

// ---------------------------------------------------------------- defect 1

// TestTokenIsDeliveredImmediately is the regression test for the defect that
// made photon unusable for its own purpose: a single small Event was held in
// the buffer until 16 KB accumulated or the stream closed, so a token stream
// arrived in one lump at the end.
func TestTokenIsDeliveredImmediately(t *testing.T) {
	release := make(chan struct{})
	app := New(MemoryBudget(256 << 20))
	app.GET("/s", SSE(func(s *Stream, r *http.Request) error {
		if err := s.Event("token", []byte("hello")); err != nil {
			return err
		}
		<-release // the handler is still running: nothing has been closed
		return nil
	}))
	addr := startServer(t, app)
	defer close(release)

	c, err := net.Dial("tcp", strings.TrimPrefix(addr, "http://"))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	_, _ = io.WriteString(c, "GET /s HTTP/1.1\r\nHost: x\r\n\r\n")
	_ = c.SetReadDeadline(time.Now().Add(2 * time.Second))
	br := bufio.NewReader(c)
	for {
		line, err := br.ReadString('\n')
		if strings.Contains(line, "data: hello") {
			return
		}
		if err != nil {
			t.Fatalf("the event was not delivered while the handler was still running: %v", err)
		}
	}
}

// ---------------------------------------------------------------- defect 2

// TestSSEFieldInjectionIsRefused is the regression test for SSE injection. A
// line break in an event name or id ended the field early and let the rest be
// read as new fields, and a lone CR in data - which model output can contain -
// did the same, because only LF was treated as a line break.
func TestSSEFieldInjectionIsRefused(t *testing.T) {
	wire := recordStream(t, func(s *Stream) {
		if err := s.Event("x\ndata: injected-by-name", []byte("ok")); !errors.Is(err, ErrInvalidEvent) {
			t.Errorf("Event with a newline in its name = %v, want ErrInvalidEvent", err)
		}
		if err := s.SetEventID("1\rdata: injected-by-id"); !errors.Is(err, ErrInvalidEvent) {
			t.Errorf("SetEventID with a CR = %v, want ErrInvalidEvent", err)
		}
		if err := s.SetEventID("a\x00b"); !errors.Is(err, ErrInvalidEvent) {
			t.Errorf("SetEventID with a NUL = %v, want ErrInvalidEvent", err)
		}
		if err := s.Event("", []byte("a\revent: evil\rdata: injected-by-cr")); err != nil {
			t.Fatal(err)
		}
	})
	got := parseSSE(wire)
	if len(got.Events) != 1 {
		t.Fatalf("parsed %d events, want exactly 1; wire:\n%q", len(got.Events), wire)
	}
	ev := got.Events[0]
	if ev.Event != "message" {
		t.Errorf("event type = %q, want the default; a CR in data changed it", ev.Event)
	}
	if want := "a\nevent: evil\ndata: injected-by-cr"; ev.Data != want {
		t.Errorf("data = %q, want %q (the CRs carried as line breaks inside the data)", ev.Data, want)
	}
}

// FuzzSSEEventRoundTrip states the framing contract for every input: an Event
// call either refuses its input, or produces exactly one event that a
// spec-compliant client decodes to the same name, id, and data, with no extra
// fields. Data line breaks arrive as LF, which is all SSE can carry.
func FuzzSSEEventRoundTrip(f *testing.F) {
	f.Add("token", "1", []byte("hello"))
	f.Add("", "", []byte(""))
	f.Add("x", "id", []byte("\n"))
	f.Add("x", "", []byte("a\r\nb\rc\n"))
	f.Add("a\nb", "", []byte("data: evil"))
	f.Add("e", "\r", []byte("\r\r\n\n"))
	f.Add(":comment", "", []byte(": not a comment"))
	f.Fuzz(func(t *testing.T, name, id string, data []byte) {
		var nameErr, idErr error
		wire := recordStream(t, func(s *Stream) {
			idErr = s.SetEventID(id)
			nameErr = s.Event(name, data)
		})
		if !validField(name) {
			if !errors.Is(nameErr, ErrInvalidEvent) {
				t.Fatalf("invalid name %q accepted (err %v)", name, nameErr)
			}
			if wire != "" {
				t.Fatalf("a refused event still wrote %q", wire)
			}
			return
		}
		if nameErr != nil {
			t.Fatalf("Event(%q) = %v", name, nameErr)
		}
		got := parseSSE(wire)
		if len(got.Events) != 1 || len(got.Unknown) != 0 || got.Comments != 0 {
			t.Fatalf("wire %q parsed to %+v; want exactly one event and nothing else", wire, got)
		}
		ev := got.Events[0]
		wantName := name
		if wantName == "" {
			wantName = "message"
		}
		if ev.Event != wantName {
			t.Errorf("event = %q, want %q", ev.Event, wantName)
		}
		if want := normalizeLineBreaks(string(data)); ev.Data != want {
			t.Errorf("data = %q, want %q", ev.Data, want)
		}
		if validField(id) {
			if idErr != nil || ev.ID != id {
				t.Errorf("id = %q (err %v), want %q", ev.ID, idErr, id)
			}
		} else if !errors.Is(idErr, ErrInvalidEvent) {
			t.Errorf("invalid id %q accepted", id)
		}
	})
}

// TestLineBreakTokensSurvive pins the lossless encoding. The original encoder
// dropped a trailing newline from data, so the newline tokens a model emits
// for paragraph breaks - "\n", "\n\n" - arrived as "" and "\n", and markdown
// output lost its structure.
func TestLineBreakTokensSurvive(t *testing.T) {
	tokens := []string{"Hello", "\n", "\n\n", " world", "a\n", "", "x\r\ny"}
	wire := recordStream(t, func(s *Stream) {
		for _, tok := range tokens {
			if err := s.Event("token", []byte(tok)); err != nil {
				t.Fatal(err)
			}
		}
	})
	got := parseSSE(wire)
	if len(got.Events) != len(tokens) {
		t.Fatalf("got %d events, want %d; wire %q", len(got.Events), len(tokens), wire)
	}
	for i, tok := range tokens {
		if want := normalizeLineBreaks(tok); got.Events[i].Data != want {
			t.Errorf("token %d: got %q, want %q", i, got.Events[i].Data, want)
		}
	}
}

// ---------------------------------------------------------------- defect 3

// TestStreamIsSafeForConcurrentWriters is the regression test for heartbeats
// racing the handler: the heartbeat goroutine wrote to a Stream documented as
// single-goroutine. Now every frame is written as a unit, so heartbeats and
// several producers can share a stream and every event arrives intact.
func TestStreamIsSafeForConcurrentWriters(t *testing.T) {
	const writers, perWriter = 8, 200
	l := DefaultLimits()
	l.HeartbeatInterval = time.Millisecond // as many heartbeats as possible
	app := New(SetLimits(l), MemoryBudget(256<<20))
	app.GET("/s", SSE(func(s *Stream, r *http.Request) error {
		stop := s.StartHeartbeats()
		defer stop()
		var wg sync.WaitGroup
		for w := 0; w < writers; w++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for i := 0; i < perWriter; i++ {
					payload := fmt.Sprintf("writer=%d seq=%d %s", w, i, strings.Repeat("x", i%50))
					if err := s.Event("t", []byte(payload)); err != nil {
						return
					}
					if i%37 == 0 {
						time.Sleep(2 * time.Millisecond) // let heartbeats in
					}
				}
			}()
		}
		wg.Wait()
		return nil
	}))
	srv := httptest.NewServer(app)
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/s")
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()

	got := parseSSE(string(body))
	if len(got.Unknown) != 0 {
		t.Fatalf("frames were spliced together: %d unknown fields, first %q", len(got.Unknown), got.Unknown[0])
	}
	if len(got.Events) != writers*perWriter {
		t.Fatalf("got %d events, want %d", len(got.Events), writers*perWriter)
	}
	next := make([]int, writers)
	for _, ev := range got.Events {
		var w, seq int
		if _, err := fmt.Sscanf(ev.Data, "writer=%d seq=%d", &w, &seq); err != nil {
			t.Fatalf("corrupt event %q", ev.Data)
		}
		if seq != next[w] {
			t.Fatalf("writer %d: got seq %d, want %d; events from one goroutine were reordered", w, seq, next[w])
		}
		next[w]++
	}
	t.Logf("%d events and %d heartbeats, all intact", len(got.Events), got.Comments)
}

// ---------------------------------------------------------------- defect 4

// TestStaticRouteAllocatesNothing pins the claim the README makes. It was
// false: a static request through Server.ServeHTTP cost 5 allocations - the
// server attached to the context per request, a panic-recovery closure, and a
// body-limit closure. A request through Serve now carries the server in its
// base context, recovery is a plain defer, and a body-less request is not
// wrapped.
func TestStaticRouteAllocatesNothing(t *testing.T) {
	app, w := allocApp()
	r := httptest.NewRequest(http.MethodGet, "/healthz", nil).WithContext(app.baseCtx)
	if n := allocsPerFreshRequest(app, w, r); n != 0 {
		t.Errorf("static route: %.0f allocations per request, want 0", n)
	}
}

// TestParamRouteAllocations pins the parametrised cost: the map net/http
// allocates inside Request.SetPathValue, and nothing else. It was 9 through
// the server - a request copy for the context, a context node, a holder, a
// capture slice, plus the closures the static path also paid.
func TestParamRouteAllocations(t *testing.T) {
	app, w := allocApp()
	r := httptest.NewRequest(http.MethodGet, "/users/42/posts/7", nil).WithContext(app.baseCtx)
	if n := allocsPerFreshRequest(app, w, r); n > 2 {
		t.Errorf("param route: %.0f allocations per request, want at most 2", n)
	}
	t.Logf("param route: %.0f allocations per request", allocsPerFreshRequest(app, w, r))
}

// allocsPerFreshRequest measures what photon allocates for one request, on a
// fresh copy of tmpl each time - as net/http hands every request a new
// *http.Request. Reusing one request would hide per-request costs such as the
// path-value map, which is only allocated the first time. The cost of making
// the copy itself is measured separately and subtracted.
func allocsPerFreshRequest(app *Server, w http.ResponseWriter, tmpl *http.Request) float64 {
	copyOnly := testing.AllocsPerRun(1000, func() {
		req := *tmpl
		sinkRequest = &req
	})
	total := testing.AllocsPerRun(1000, func() {
		req := *tmpl
		app.ServeHTTP(w, &req)
	})
	return total - copyOnly
}

var sinkRequest *http.Request

func allocApp() (*Server, *benchWriter) {
	app := New(MemoryBudget(256 << 20))
	h := func(w http.ResponseWriter, r *http.Request) {}
	app.GET("/healthz", h)
	app.GET("/users/:id/posts/:post", h)
	app.rt.Freeze()
	return app, &benchWriter{h: http.Header{}}
}

// TestPathValueInteroperatesWithNetHTTP checks parameters are visible through
// the standard library's own accessors: r.PathValue for handlers written for
// ServeMux, and r.Pattern for the route label that otelhttp and metrics
// middleware read.
func TestPathValueInteroperatesWithNetHTTP(t *testing.T) {
	app := New(MemoryBudget(256 << 20))
	app.GET("/users/:id/files/*path", func(w http.ResponseWriter, r *http.Request) {
		_, _ = fmt.Fprintf(w, "%s|%s|%s|%v", r.PathValue("id"), r.PathValue("path"), r.Pattern, Params(r))
	})
	app.GET("/health", func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, r.Pattern)
	})
	rec := httptest.NewRecorder()
	app.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/users/7/files/a/b.txt", nil))
	if want := "7|a/b.txt|/users/:id/files/*path|[{id 7} {path a/b.txt}]"; rec.Body.String() != want {
		t.Errorf("got %q, want %q", rec.Body.String(), want)
	}
	rec = httptest.NewRecorder()
	app.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/health", nil))
	if rec.Body.String() != "/health" {
		t.Errorf("static route r.Pattern = %q, want /health", rec.Body.String())
	}
}

// ---------------------------------------------------------------- defect 5

// TestMaxHeadersCountsFieldsNotNames is the regression test for a header-count
// bypass. MaxHeaders compared len(r.Header), the number of distinct NAMES, so
// a thousand copies of one header passed a limit of 100 - exactly the "many
// tiny headers" request the limit exists to refuse.
func TestMaxHeadersCountsFieldsNotNames(t *testing.T) {
	app := New(MemoryBudget(256 << 20))
	app.GET("/", func(w http.ResponseWriter, r *http.Request) {})
	r := httptest.NewRequest(http.MethodGet, "/", nil)
	for i := 0; i < 1000; i++ {
		r.Header.Add("X-A", "1")
	}
	rec := httptest.NewRecorder()
	app.ServeHTTP(rec, r)
	if rec.Code != http.StatusRequestHeaderFieldsTooLarge {
		t.Errorf("1000 copies of one header: status %d, want 431", rec.Code)
	}
}

// ---------------------------------------------------------------- defect 6

// TestGOMEMLIMITIsReadInGoSyntax is the regression test for GOMEMLIMIT being
// ignored. The parser stripped two characters from "512MiB" and failed on
// "512M", so the standard spelling fell through to the fallback budget, and
// "512K" was read as 51 KiB. The limit is now read from the runtime, which has
// already parsed it.
func TestGOMEMLIMITIsReadInGoSyntax(t *testing.T) {
	prev := debug.SetMemoryLimit(-1)
	defer debug.SetMemoryLimit(prev)

	debug.SetMemoryLimit(512 << 20) // what GOMEMLIMIT=512MiB sets at startup
	if got := readGOMEMLIMIT(); got != 512<<20 {
		t.Errorf("readGOMEMLIMIT = %d, want %d", got, 512<<20)
	}
	debug.SetMemoryLimit(math.MaxInt64) // unset
	if got := readGOMEMLIMIT(); got != 0 {
		t.Errorf("readGOMEMLIMIT with no limit = %d, want 0", got)
	}
}

// ---------------------------------------------------------------- defect 7

// TestShutdownDeadlineClosesConnections is the regression test for a
// documented guarantee that did not hold: Shutdown said connections were
// closed when its deadline expired, but net/http's Shutdown leaves them open,
// so a handler that ignored shutdown kept running indefinitely.
func TestShutdownDeadlineClosesConnections(t *testing.T) {
	app := New(MemoryBudget(256 << 20))
	app.GET("/hang", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_ = http.NewResponseController(w).Flush()
		<-r.Context().Done() // ignores everything except its own connection closing
	})
	ln, _ := net.Listen("tcp", "127.0.0.1:0")
	go func() { _ = app.Serve(ln) }()

	resp, err := http.Get("http://" + ln.Addr().String() + "/hang")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	if err := app.Shutdown(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Shutdown = %v, want DeadlineExceeded", err)
	}
	done := make(chan error, 1)
	go func() { _, err := io.ReadAll(resp.Body); done <- err }()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("the connection was still open after Shutdown's deadline expired")
	}
}

// ---------------------------------------------------------------- features

// TestStreamDoneLetsShutdownFinishEarly checks the graceful path: a stream that
// watches Done sends a final event when Shutdown begins and returns, so
// Shutdown completes in milliseconds rather than waiting out its deadline -
// and the client receives the goodbye rather than a cut connection.
func TestStreamDoneLetsShutdownFinishEarly(t *testing.T) {
	app := New(MemoryBudget(256 << 20))
	started := make(chan struct{})
	app.GET("/feed", SSE(func(s *Stream, r *http.Request) error {
		if err := s.Event("hello", nil); err != nil {
			return err
		}
		close(started)
		<-s.Done()
		return s.Event("bye", []byte("server restarting"))
	}))
	ln, _ := net.Listen("tcp", "127.0.0.1:0")
	go func() { _ = app.Serve(ln) }()

	resp, err := http.Get("http://" + ln.Addr().String() + "/feed")
	if err != nil {
		t.Fatal(err)
	}
	<-started
	body := make(chan string, 1)
	go func() { b, _ := io.ReadAll(resp.Body); body <- string(b) }()

	begin := time.Now()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := app.Shutdown(ctx); err != nil {
		t.Fatalf("Shutdown = %v", err)
	}
	if d := time.Since(begin); d > 2*time.Second {
		t.Errorf("Shutdown took %s; a stream watching Done should let it finish at once", d)
	}
	got := parseSSE(<-body)
	if n := len(got.Events); n != 2 || got.Events[1].Event != "bye" {
		t.Errorf("client saw %+v, want hello then bye", got.Events)
	}
}

// TestSSEErrorBeforeFirstEventIsAnHTTPError checks the adapter turns an error
// returned before anything was streamed into an ordinary response with the
// right status - the window in which the status code can still be chosen.
func TestSSEErrorBeforeFirstEventIsAnHTTPError(t *testing.T) {
	app := New(MemoryBudget(256 << 20))
	app.POST("/chat", SSE(func(s *Stream, r *http.Request) error {
		var req struct {
			Prompt string `json:"prompt"`
		}
		if err := DecodeJSON(r, &req); err != nil {
			return err
		}
		if req.Prompt == "" {
			return photonerr.BadRequest("prompt is required")
		}
		return s.Event("token", []byte(req.Prompt))
	}))
	cases := []struct {
		body, ct string
		status   int
	}{
		{`{"prompt":""}`, "application/json", 400},
		{`{"prompt":`, "application/json", 400},
		{`{"prompt":"hi"}`, "text/plain", 415},
		{`{"prompt":"hi"}`, "application/json", 200},
	}
	for _, tc := range cases {
		r := httptest.NewRequest(http.MethodPost, "/chat", strings.NewReader(tc.body))
		r.Header.Set("Content-Type", tc.ct)
		rec := httptest.NewRecorder()
		app.ServeHTTP(rec, r)
		if rec.Code != tc.status {
			t.Errorf("%s %s: status %d, want %d (body %q)", tc.ct, tc.body, rec.Code, tc.status, rec.Body.String())
		}
		if tc.status != 200 && rec.Header().Get("Content-Type") != "application/problem+json; charset=utf-8" {
			t.Errorf("error response content type = %q", rec.Header().Get("Content-Type"))
		}
	}
}

// TestSSEErrorMidStreamIsAnErrorEvent checks an error after the first event is
// delivered in-band, with the safe message only, and that the cause is logged.
func TestSSEErrorMidStreamIsAnErrorEvent(t *testing.T) {
	var logs lockedBuffer
	app := New(MemoryBudget(256<<20), Logger(slog.New(slog.NewTextHandler(&logs, nil))))
	app.GET("/s", SSE(func(s *Stream, r *http.Request) error {
		if err := s.Event("token", []byte("partial")); err != nil {
			return err
		}
		return photonerr.Upstream(errors.New("dial tcp 10.0.0.7:443: connection refused"))
	}))
	rec := httptest.NewRecorder()
	app.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/s", nil))

	got := parseSSE(rec.Body.String())
	if len(got.Events) != 2 || got.Events[1].Event != "error" {
		t.Fatalf("events = %+v, want a token then an error event", got.Events)
	}
	var problem struct {
		Status int    `json:"status"`
		Code   string `json:"code"`
	}
	if err := json.Unmarshal([]byte(got.Events[1].Data), &problem); err != nil {
		t.Fatal(err)
	}
	if problem.Status != 502 || problem.Code != "upstream_error" {
		t.Errorf("error event = %+v, want 502 upstream_error", problem)
	}
	if strings.Contains(rec.Body.String(), "10.0.0.7") {
		t.Error("the internal cause leaked into the stream")
	}
	if !logs.contains("10.0.0.7") {
		t.Error("the internal cause was not logged for the operator")
	}
}

// TestSSEPanicAfterFirstEventAbortsTheConnection checks a panic mid-stream is
// visible to the client as a failure. A clean end would make a truncated
// answer indistinguishable from a complete one.
func TestSSEPanicAfterFirstEventAbortsTheConnection(t *testing.T) {
	var panics int
	var mu sync.Mutex
	app := New(MemoryBudget(256<<20), OnPanic(func(*PanicInfo) { mu.Lock(); panics++; mu.Unlock() }))
	app.GET("/s", SSE(func(s *Stream, r *http.Request) error {
		_ = s.Event("token", []byte("partial"))
		_ = s.Flush()
		panic("boom")
	}))
	srv := httptest.NewServer(app)
	defer srv.Close()
	resp, err := http.Get(srv.URL + "/s")
	if err != nil {
		t.Fatal(err)
	}
	_, err = io.ReadAll(resp.Body)
	resp.Body.Close()
	if err == nil {
		t.Error("the client read a clean end of stream after a panic; it must see the connection fail")
	}
	mu.Lock()
	defer mu.Unlock()
	if panics != 1 {
		t.Errorf("OnPanic called %d times, want 1", panics)
	}
}

// TestDefaultLoggerReportsPanics checks a server with no logger configured
// does not swallow a panic: it reaches slog.Default.
func TestDefaultLoggerReportsPanics(t *testing.T) {
	var buf lockedBuffer
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&buf, nil)))
	defer slog.SetDefault(prev)

	app := New(MemoryBudget(256 << 20))
	app.GET("/p", func(w http.ResponseWriter, r *http.Request) { panic("kaboom") })
	rec := httptest.NewRecorder()
	app.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/p", nil))
	if rec.Code != 500 {
		t.Fatalf("status %d, want 500", rec.Code)
	}
	if !buf.contains("kaboom") {
		t.Errorf("the panic was not logged through slog.Default; log was %q", buf.String())
	}
}

// TestUseRunsOnMissesAndEarlierRoutes checks global middleware covers every
// request: routes registered before Use, 404s, and 405s - which is what lets a
// CORS middleware answer a preflight for a POST-only route without an
// OPTIONS route of its own.
func TestUseRunsOnMissesAndEarlierRoutes(t *testing.T) {
	app := New(MemoryBudget(256 << 20))
	app.POST("/early", func(w http.ResponseWriter, r *http.Request) {})
	app.Use(func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("X-Middleware", "ran")
			if r.Method == http.MethodOptions {
				w.WriteHeader(http.StatusNoContent) // a CORS preflight answer
				return
			}
			next.ServeHTTP(w, r)
		})
	})
	app.GET("/late", func(w http.ResponseWriter, r *http.Request) {})

	for _, tc := range []struct {
		method, path string
		status       int
	}{
		{"POST", "/early", 200},
		{"GET", "/late", 200},
		{"GET", "/missing", 404},
		{"GET", "/early", 405},
		{"OPTIONS", "/early", 204},
	} {
		rec := httptest.NewRecorder()
		app.ServeHTTP(rec, httptest.NewRequest(tc.method, tc.path, nil))
		if rec.Code != tc.status || rec.Header().Get("X-Middleware") != "ran" {
			t.Errorf("%s %s: status %d, middleware %q; want %d and the middleware to run",
				tc.method, tc.path, rec.Code, rec.Header().Get("X-Middleware"), tc.status)
		}
	}
}

// TestGroupPrefixAndMiddlewareOrder checks prefixes join, parameters in a
// prefix are captured, and middleware runs global, then group, then route.
func TestGroupPrefixAndMiddlewareOrder(t *testing.T) {
	var order []string
	mark := func(name string) Middleware {
		return func(next http.Handler) http.Handler {
			return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				order = append(order, name)
				next.ServeHTTP(w, r)
			})
		}
	}
	app := New(MemoryBudget(256 << 20))
	app.Use(mark("global"))
	org := app.Group("/orgs/:org", mark("group"))
	org.GET("/", func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, "org "+PathParam(r, "org"))
	})
	repos := org.Group("/repos", mark("nested"))
	repos.GET("/:repo", func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, PathParam(r, "org")+"/"+PathParam(r, "repo"))
	}, mark("route"))

	rec := httptest.NewRecorder()
	app.ServeHTTP(rec, httptest.NewRequest("GET", "/orgs/acme/repos/photon", nil))
	if rec.Body.String() != "acme/photon" {
		t.Errorf("body %q, want acme/photon", rec.Body.String())
	}
	if got := strings.Join(order, ">"); got != "global>group>nested>route" {
		t.Errorf("middleware order %s, want global>group>nested>route", got)
	}
	rec = httptest.NewRecorder()
	app.ServeHTTP(rec, httptest.NewRequest("GET", "/orgs/acme", nil))
	if rec.Body.String() != "org acme" {
		t.Errorf("group root: body %q, want %q", rec.Body.String(), "org acme")
	}
}

// TestMountServesTheSubtree checks a mounted handler receives every method for
// the prefix and everything below it - trailing slashes and deep paths
// included, which an index page such as pprof's or a file server's directory
// listing needs - and that routes take precedence over it.
func TestMountServesTheSubtree(t *testing.T) {
	echo := func(name string) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			_, _ = io.WriteString(w, name+" "+r.Method+" "+r.URL.Path+" "+r.Pattern)
		})
	}
	app := New(MemoryBudget(256 << 20))
	app.Mount("/mcp", echo("mcp"))
	app.Mount("/", echo("legacy")) // everything not yet ported
	app.GET("/mcp/health", func(w http.ResponseWriter, r *http.Request) { _, _ = io.WriteString(w, "route") })

	deep := "/mcp" + strings.Repeat("/x", 20)
	for _, tc := range []struct{ method, path, want string }{
		{"POST", "/mcp", "mcp POST /mcp /mcp/*"},
		{"GET", "/mcp/", "mcp GET /mcp/ /mcp/*"},
		{"DELETE", "/mcp/session/1", "mcp DELETE /mcp/session/1 /mcp/*"},
		{"GET", deep, "mcp GET " + deep + " /mcp/*"},
		{"GET", "/mcp/health", "route"},                             // a route wins over its mount
		{"GET", "/mcpx", "legacy GET /mcpx /*"},                     // a prefix is whole segments
		{"PUT", "/anything/else/", "legacy PUT /anything/else/ /*"}, // the root mount is the fallback
	} {
		rec := httptest.NewRecorder()
		app.ServeHTTP(rec, httptest.NewRequest(tc.method, tc.path, nil))
		if rec.Body.String() != tc.want {
			t.Errorf("%s %s: body %q, want %q", tc.method, tc.path, rec.Body.String(), tc.want)
		}
	}

	// A group with no prefix can mount at its root. This used to build the
	// pattern "//*..." and panic.
	g := New(MemoryBudget(256 << 20)).Group("")
	g.Mount("/", echo("root"))
}

// TestParamValuesAreDecoded checks parameters reach the handler decoded,
// whether or not the request path contained an escape that net/http keeps in
// RawPath. Routing still happens on the raw path, so %2F cannot split a
// segment; only the captured value is decoded, as ServeMux does.
func TestParamValuesAreDecoded(t *testing.T) {
	app := New(MemoryBudget(256 << 20))
	app.GET("/files/:name", func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, photon_PathParam(r, "name"))
	})
	for path, want := range map[string]string{
		"/files/a%20b":      "a b", // no RawPath: decoded by net/http
		"/files/a%2Fb":      "a/b", // RawPath set: decoded by photon, still one segment
		"/files/report%7E1": "report~1",
	} {
		rec := httptest.NewRecorder()
		app.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, path, nil))
		if rec.Body.String() != want {
			t.Errorf("%s: param %q, want %q", path, rec.Body.String(), want)
		}
	}
}

// photon_PathParam keeps the call above readable as the public API.
var photon_PathParam = PathParam

// TestGroupRoutePathMustStartWithSlash checks api.GET("models") on a "/v1"
// group is refused rather than registered as "/v1models".
func TestGroupRoutePathMustStartWithSlash(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Error("a group route without a leading slash was accepted")
		}
	}()
	New(MemoryBudget(256<<20)).Group("/v1").GET("models", func(w http.ResponseWriter, r *http.Request) {})
}

// TestOptionsSurviveSetLimits checks MaxStreams and MemoryBudget apply whether
// they come before or after SetLimits, which replaces the whole Limits value.
func TestOptionsSurviveSetLimits(t *testing.T) {
	app := New(MaxStreams(123), MemoryBudget(64<<20), SetLimits(DefaultLimits()))
	if got := app.Limits().MaxStreams; got != 123 {
		t.Errorf("MaxStreams = %d after SetLimits, want 123", got)
	}
	if got := app.Limits().MemoryBudget; got != 64<<20 {
		t.Errorf("MemoryBudget = %d after SetLimits, want %d", got, 64<<20)
	}
}

// TestNegativeByteLimitsAreRefused checks a negative MaxHeaderBytes or
// MaxRequestBodyBytes panics in New. net/http reads a non-positive
// MaxHeaderBytes as its 1 MB default, so -1 would look like a limit and be the
// most permissive setting there is.
func TestNegativeByteLimitsAreRefused(t *testing.T) {
	for name, mutate := range map[string]func(*Limits){
		"MaxHeaderBytes":      func(l *Limits) { l.MaxHeaderBytes = -1 },
		"MaxRequestBodyBytes": func(l *Limits) { l.MaxRequestBodyBytes = -1 },
	} {
		func() {
			defer func() {
				if msg := fmt.Sprint(recover()); !strings.Contains(msg, name) {
					t.Errorf("%s = -1: panic %q, want one naming %s", name, msg, name)
				}
			}()
			l := DefaultLimits()
			mutate(&l)
			New(SetLimits(l))
		}()
	}
}

// TestConflictPanicMatchesErrRouteConflict checks a test can assert on the kind
// of registration mistake without parsing the message.
func TestConflictPanicMatchesErrRouteConflict(t *testing.T) {
	app := New(MemoryBudget(256 << 20))
	app.GET("/a/:id", func(w http.ResponseWriter, r *http.Request) {})
	defer func() {
		err, _ := recover().(error)
		if !errors.Is(err, ErrRouteConflict) {
			t.Errorf("conflict panic %v does not match ErrRouteConflict", err)
		}
	}()
	app.GET("/a/new", func(w http.ResponseWriter, r *http.Request) {})
}

// TestConflictPanicNamesTheCallersLine checks the diagnostic points at the
// user's registration, not at photon's own Group.GET or Server.GET.
func TestConflictPanicNamesTheCallersLine(t *testing.T) {
	app := New(MemoryBudget(256 << 20))
	g := app.Group("/admin")
	g.GET("/settings", func(w http.ResponseWriter, r *http.Request) {})
	defer func() {
		msg := fmt.Sprint(recover())
		if strings.Count(msg, "regression_test.go:") != 2 {
			t.Errorf("conflict message does not name both registration lines:\n%s", msg)
		}
		if strings.Contains(msg, "group.go") || strings.Contains(msg, "router.go") {
			t.Errorf("conflict message points into photon itself:\n%s", msg)
		}
	}()
	g.GET("/*rest", func(w http.ResponseWriter, r *http.Request) {})
}

// TestRegisteringManyRoutesIsLinear guards startup time. Each registration
// re-parsed every static route to check for conflicts, so registering N routes
// was quadratic: 10,000 routes took long enough to time out a benchmark run.
func TestRegisteringManyRoutesIsLinear(t *testing.T) {
	rt := NewRouter()
	h := func(w http.ResponseWriter, r *http.Request) {}
	start := time.Now()
	for i := 0; i < 20000; i++ {
		rt.GET("/r/"+strconv.Itoa(i)+"/x", h)
	}
	for i := 0; i < 50; i++ {
		rt.GET("/p"+strconv.Itoa(i)+"/:id", h)
	}
	if d := time.Since(start); d > 3*time.Second {
		t.Errorf("registering 20,050 routes took %s", d)
	}
}

// TestDecodeJSON covers each refusal and the statuses they carry.
func TestDecodeJSON(t *testing.T) {
	type req struct {
		N int `json:"n"`
	}
	l := DefaultLimits()
	l.MaxRequestBodyBytes = 64
	app := New(SetLimits(l), MemoryBudget(256<<20))
	app.POST("/", func(w http.ResponseWriter, r *http.Request) {
		var v req
		if err := DecodeJSON(r, &v); err != nil {
			Error(w, r, err)
			return
		}
		_ = JSON(w, 200, v)
	})
	for _, tc := range []struct {
		name, ct, body string
		status         int
		mustNotContain string
	}{
		{"ok", "application/json", `{"n":1}`, 200, ""},
		{"ok with charset", "application/json; charset=utf-8", `{"n":1}`, 200, ""},
		{"vendor json", "application/vnd.api+json", `{"n":1}`, 200, ""},
		{"no content type", "", `{"n":1}`, 200, ""},
		{"unknown fields are fine", "application/json", `{"n":1,"extra":true}`, 200, ""},
		{"form post", "application/x-www-form-urlencoded", `{"n":1}`, 415, ""},
		{"empty", "application/json", ``, 400, ""},
		{"malformed", "application/json", `{"n":`, 400, ""},
		{"trailing value", "application/json", `{"n":1}{"n":2}`, 400, ""},
		{"wrong type", "application/json", `{"n":"<script>"}`, 400, "<script>"},
		{"too large", "application/json", `{"n":1,"pad":"` + strings.Repeat("x", 100) + `"}`, 413, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(tc.body))
			r.ContentLength = -1 // as a streaming client would send it
			if tc.ct != "" {
				r.Header.Set("Content-Type", tc.ct)
			}
			rec := httptest.NewRecorder()
			app.ServeHTTP(rec, r)
			if rec.Code != tc.status {
				t.Errorf("status %d, want %d; body %s", rec.Code, tc.status, rec.Body.String())
			}
			if tc.mustNotContain != "" && strings.Contains(rec.Body.String(), tc.mustNotContain) {
				t.Errorf("the response reflected request content: %s", rec.Body.String())
			}
		})
	}
}

// TestDropOldestDropsWholeEvents checks the policy keeps the newest events and
// never cuts one. The original dropped raw bytes from the front of the buffer,
// which would leave half an SSE frame on the wire and corrupt every event
// after it - and, in practice, could not be reached at all with a real
// transport.
func TestDropOldestDropsWholeEvents(t *testing.T) {
	w := newBlockingWriter()
	s := newStream(w, httptest.NewRequest(http.MethodGet, "/x", nil), newFlusher(w),
		streamConfig{MaxBuffer: 1024, Stall: 5 * time.Second}).Overflow(DropOldest)

	// The first event is taken by the writer and blocks in Write, so every
	// event after it queues in the 1 KB buffer and the oldest must be dropped.
	for i := 0; i < 200; i++ {
		if err := s.Event("metric", []byte(fmt.Sprintf(`{"seq":%d}`, i))); err != nil {
			t.Fatalf("event %d: %v (DropOldest must never block the producer)", i, err)
		}
	}
	w.release()
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	_, body := w.snapshot()
	got := parseSSE(string(body))
	if len(got.Unknown) != 0 {
		t.Fatalf("a dropped event left a partial frame: %q", got.Unknown)
	}
	if n := len(got.Events); n == 0 || n >= 200 {
		t.Fatalf("delivered %d of 200 events; want some dropped and some kept", n)
	}
	if last := got.Events[len(got.Events)-1].Data; last != `{"seq":199}` {
		t.Errorf("last event %q, want the newest", last)
	}
	prev := -1
	for _, ev := range got.Events {
		var m struct{ Seq int }
		if err := json.Unmarshal([]byte(ev.Data), &m); err != nil || m.Seq <= prev {
			t.Fatalf("event %q out of order or corrupt after %d", ev.Data, prev)
		}
		prev = m.Seq
	}
	t.Logf("delivered %d of 200 events, newest kept, all intact", len(got.Events))
}

// TestNonSSEStreamKeepsItsContentTypeAndGetsNoHeartbeats checks an NDJSON
// stream - what Ollama and several model servers speak - can be written
// through a Stream without SSE comments corrupting it.
func TestNonSSEStreamKeepsItsContentTypeAndGetsNoHeartbeats(t *testing.T) {
	l := DefaultLimits()
	l.HeartbeatInterval = time.Millisecond
	app := New(SetLimits(l), MemoryBudget(256<<20))
	app.GET("/ndjson", SSE(func(s *Stream, r *http.Request) error {
		s.Header().Set("Content-Type", "application/x-ndjson")
		for i := 0; i < 5; i++ {
			if _, err := fmt.Fprintf(s, "{\"i\":%d}\n", i); err != nil {
				return err
			}
			time.Sleep(10 * time.Millisecond) // long enough for heartbeats to fire
		}
		return nil
	}))
	srv := httptest.NewServer(app)
	defer srv.Close()
	resp, err := http.Get(srv.URL + "/ndjson")
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if ct := resp.Header.Get("Content-Type"); ct != "application/x-ndjson" {
		t.Errorf("Content-Type %q, want application/x-ndjson", ct)
	}
	if bytes.Contains(body, []byte(": ping")) {
		t.Errorf("an SSE heartbeat was written into an NDJSON stream: %q", body)
	}
	if lines := strings.Count(string(body), "\n"); lines != 5 {
		t.Errorf("got %d lines, want 5: %q", lines, body)
	}
}

// TestHeartbeatsAreAutomaticOnceStreaming checks a quiet SSE stream sends
// heartbeats without the handler asking, once it has started.
func TestHeartbeatsAreAutomaticOnceStreaming(t *testing.T) {
	l := DefaultLimits()
	l.HeartbeatInterval = 20 * time.Millisecond
	app := New(SetLimits(l), MemoryBudget(256<<20))
	app.GET("/s", SSE(func(s *Stream, r *http.Request) error {
		if err := s.Event("start", nil); err != nil {
			return err
		}
		time.Sleep(150 * time.Millisecond) // a model thinking
		return s.Event("end", nil)
	}))
	srv := httptest.NewServer(app)
	defer srv.Close()
	resp, err := http.Get(srv.URL + "/s")
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	got := parseSSE(string(body))
	if got.Comments < 2 {
		t.Errorf("%d heartbeats during a 150ms silence at a 20ms interval, want several", got.Comments)
	}
	if len(got.Events) != 2 {
		t.Errorf("events %+v, want start and end", got.Events)
	}
}

// TestHTTP2StreamHasNoConnectionHeader checks the stream works over HTTP/2 and
// does not send Connection, a connection-specific header RFC 9113 forbids in
// an HTTP/2 response.
func TestHTTP2StreamHasNoConnectionHeader(t *testing.T) {
	app := New(MemoryBudget(256 << 20))
	app.GET("/s", SSE(func(s *Stream, r *http.Request) error {
		return s.Event("token", []byte("over h2"))
	}))
	srv := httptest.NewUnstartedServer(app)
	srv.EnableHTTP2 = true
	srv.StartTLS()
	defer srv.Close()

	resp, err := srv.Client().Get(srv.URL + "/s")
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.ProtoMajor != 2 {
		t.Fatalf("negotiated %s, want HTTP/2", resp.Proto)
	}
	if v := resp.Header.Get("Connection"); v != "" {
		t.Errorf("Connection: %q sent over HTTP/2", v)
	}
	if got := parseSSE(string(body)); len(got.Events) != 1 || got.Events[0].Data != "over h2" {
		t.Errorf("events %+v", got.Events)
	}
}

// TestRegistrationSettersRefuseAfterServe checks NotFound and MethodNotAllowed
// follow the same rule as routes. ServeHTTP reads them without a lock once the
// router is frozen, so changing them on a live server would be a data race.
func TestRegistrationSettersRefuseAfterServe(t *testing.T) {
	rt := NewRouter()
	rt.Freeze()
	for name, fn := range map[string]func(){
		"NotFound":         func() { rt.NotFound(http.NotFoundHandler()) },
		"MethodNotAllowed": func() { rt.MethodNotAllowed(http.NotFoundHandler()) },
	} {
		func() {
			defer func() {
				if recover() == nil {
					t.Errorf("%s after Freeze did not panic", name)
				}
			}()
			fn()
		}()
	}
}

// TestRunReportsABadAddress checks Run fails fast on an address it cannot bind
// instead of starting a goroutine that fails later.
func TestRunReportsABadAddress(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	app := New(MemoryBudget(256 << 20))
	if err := app.Run(ln.Addr().String()); err == nil {
		t.Fatal("Run on an address already in use returned nil")
	}
}

// TestSteadySlowReaderIsNotDisconnected pins the meaning of the stall timeout:
// a client that keeps reading, however slowly, is served; only one that stops
// is disconnected.
//
// The first version wrote a whole 32 KB batch under one write deadline, so a
// client reading steadily at a few KB/s could not finish a single write within
// the stall period and was disconnected as if it had stopped. Writes are now
// renewed every progressChunk bytes.
func TestSteadySlowReaderIsNotDisconnected(t *testing.T) {
	if testing.Short() {
		t.Skip("takes several seconds by design")
	}
	ended := make(chan error, 1)
	app := New(MemoryBudget(256 << 20))
	app.GET("/s", SSE(func(s *Stream, r *http.Request) error {
		chunk := bytes.Repeat([]byte("x"), 1024)
		for {
			if err := s.Event("c", chunk); err != nil {
				ended <- err
				return err
			}
		}
	}, WithStall(2*time.Second)))
	addr := strings.TrimPrefix(startServer(t, app), "http://")

	c, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	_, _ = io.WriteString(c, "GET /s HTTP/1.1\r\nHost: x\r\n\r\n")

	// Read 64 KB every 50 ms - about 1.3 MB/s, far below what the producer
	// offers - for four seconds, long after every socket buffer between the two
	// ends has filled.
	//
	// The rate is set by how coarsely TCP reports progress, not by what Photon
	// needs. Once a receive window has closed, Linux does not reopen it until
	// a sixteenth of the receive buffer is free (receiver-side silly-window
	// avoidance), and loopback buffers autotune to 6 MB - so the sender sees
	// progress only every ~384 KB read. At 160 KB/s that is 2.4 s, longer than
	// the 2 s stall used here, and the reader looks stopped although it never
	// is. Windows reopens the window in much smaller steps. With the default
	// 30 s stall the same reader is served on every platform.
	buf := make([]byte, 64<<10)
	deadline := time.Now().Add(4 * time.Second)
	for time.Now().Before(deadline) {
		if _, err := io.ReadFull(c, buf); err != nil {
			t.Fatalf("read while reading steadily: %v", err)
		}
		select {
		case err := <-ended:
			t.Fatalf("a steadily reading client was disconnected: %v", err)
		default:
		}
		time.Sleep(50 * time.Millisecond)
	}

	// Now stop reading. The stream must end on its own, with ErrStreamOverflow.
	select {
	case err := <-ended:
		if !errors.Is(err, ErrStreamOverflow) {
			t.Errorf("stream ended with %v, want ErrStreamOverflow", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("a client that stopped reading was never disconnected")
	}
}

// TestTLSWithConnectionLimitsKeepsHTTP2 is the regression test for admission
// control breaking TLS. Connections were wrapped so the slot could be released
// on Close, which hid the *tls.Conn from net/http: a client that negotiated h2
// was handed to the HTTP/1 parser, and r.TLS was nil. Admission now releases
// through net/http's connection-state hook and never wraps the connection.
func TestTLSWithConnectionLimitsKeepsHTTP2(t *testing.T) {
	// Borrow httptest's self-signed certificate and a client that trusts it.
	ts := httptest.NewUnstartedServer(nil)
	ts.StartTLS()
	cert := ts.TLS.Certificates[0]
	clientTLS := ts.Client().Transport.(*http.Transport).TLSClientConfig.Clone()
	ts.Close()

	l := DefaultLimits()
	l.MaxConnections = 10
	l.MaxPerIP = 5
	app := New(SetLimits(l), MemoryBudget(256<<20))
	app.GET("/tls", func(w http.ResponseWriter, r *http.Request) {
		_, _ = fmt.Fprintf(w, "%s tls=%v", r.Proto, r.TLS != nil)
	})
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go func() {
		_ = app.Serve(tls.NewListener(ln, &tls.Config{
			Certificates: []tls.Certificate{cert},
			NextProtos:   []string{"h2", "http/1.1"},
			MinVersion:   tls.VersionTLS12,
		}))
	}()
	defer app.Close()

	client := &http.Client{Transport: &http.Transport{TLSClientConfig: clientTLS, ForceAttemptHTTP2: true}}
	defer client.CloseIdleConnections()
	resp, err := client.Get("https://" + ln.Addr().String() + "/tls")
	if err != nil {
		t.Fatalf("request over TLS with connection limits: %v", err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if string(body) != "HTTP/2.0 tls=true" {
		t.Errorf("handler saw %q, want HTTP/2.0 with r.TLS set", body)
	}
}
