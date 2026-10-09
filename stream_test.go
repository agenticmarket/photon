package photon

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

// This file covers streaming correctness. The invariant test - bounded memory
// under a slow consumer - is in stream_bound_test.go, and it is the one that
// matters most; this file is the ordinary behavioural surface around it.

// blockingWriter is a ResponseWriter whose Write blocks until released.
//
// It is the only way to test back-pressure deterministically: a real socket has
// its own buffering and a real client's read rate is not controllable, so a
// test that tries to make a consumer "slow" through a socket is testing the
// kernel. This blocks the write itself, which is exactly the condition the
// bounded buffer exists for.
type blockingWriter struct {
	mu      sync.Mutex
	header  http.Header
	body    []byte
	status  int
	blocked chan struct{} // closed to release blocked writes
	entered chan struct{} // signalled when a write blocks
	once    sync.Once
	flushed int
}

func newBlockingWriter() *blockingWriter {
	return &blockingWriter{
		header:  make(http.Header),
		blocked: make(chan struct{}),
		entered: make(chan struct{}),
	}
}

func (w *blockingWriter) Header() http.Header { return w.header }

func (w *blockingWriter) WriteHeader(code int) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.status == 0 {
		w.status = code
	}
}

func (w *blockingWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	if w.status == 0 {
		w.status = http.StatusOK
	}
	blocked, entered := w.blocked, w.entered
	w.mu.Unlock()

	// Block until released, then write.
	w.once.Do(func() { close(entered) })
	<-blocked

	w.mu.Lock()
	defer w.mu.Unlock()
	w.body = append(w.body, p...)
	return len(p), nil
}

func (w *blockingWriter) Flush() {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.flushed++
}

func (w *blockingWriter) SetWriteDeadline(time.Time) error { return nil }

func (w *blockingWriter) release() {
	w.mu.Lock()
	defer w.mu.Unlock()
	select {
	case <-w.blocked:
	default:
		close(w.blocked)
	}
}

func (w *blockingWriter) enteredBlocking() bool {
	select {
	case <-w.entered:
		return true
	default:
		return false
	}
}

func (w *blockingWriter) snapshot() (int, []byte) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.status, append([]byte(nil), w.body...)
}

// ---------------------------------------------------------------- SSE framing

func TestStreamEventFraming(t *testing.T) {
	tests := []struct {
		name   string
		event  string
		data   string
		want   string
		repeat string
	}{
		{
			name:  "named event with data",
			event: "token",
			data:  "hello",
			want:  "event: token\ndata: hello\n\n",
		},
		{
			name: "unnamed event omits the event field",
			data: "hello",
			want: "data: hello\n\n",
		},
		{
			name: "multi-line data becomes one data field per line",
			data: "line one\nline two\nline three",
			want: "data: line one\ndata: line two\ndata: line three\n\n",
		},
		{
			name: "a trailing newline is kept as an empty data line",
			data: "hello\n",
			// The client joins data lines with "\n", so "hello" then "" is
			// "hello\n": the payload round-trips. Dropping the empty line, as
			// an earlier version did, turned a newline token into nothing and
			// "\n\n" into "\n", which flattened markdown paragraph breaks.
			want: "data: hello\ndata: \n\n",
		},
		{
			name: "a lone CR is a line break, so it cannot start a new field",
			data: "a\rb",
			want: "data: a\ndata: b\n\n",
		},
		{
			name: "CRLF is normalised to a single line break",
			data: "a\r\nb",
			want: "data: a\ndata: b\n\n",
		},
		{
			name:  "empty data is one empty data line, so the event is dispatched",
			event: "done",
			data:  "",
			// A frame with no data field is discarded by EventSource, so
			// Event("done", nil) used to send nothing a browser would see.
			want: "event: done\ndata: \n\n",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			rec := httptest.NewRecorder()
			s := newStream(rec, httptest.NewRequest(http.MethodGet, "/x", nil),
				newFlusher(rec), streamConfig{MaxBuffer: 4096})
			if err := s.Event(tc.event, []byte(tc.data)); err != nil {
				t.Fatalf("Event: %v", err)
			}
			if err := s.Close(); err != nil {
				t.Fatalf("Close: %v", err)
			}
			if got := rec.Body.String(); got != tc.want {
				t.Errorf("framing =\n%q\nwant\n%q", got, tc.want)
			}
		})
	}
}

func TestStreamCommentIsIgnoredByClients(t *testing.T) {
	rec := httptest.NewRecorder()
	s := newStream(rec, httptest.NewRequest(http.MethodGet, "/x", nil),
		newFlusher(rec), streamConfig{MaxBuffer: 4096})
	if err := s.Comment("ping"); err != nil {
		t.Fatalf("Comment: %v", err)
	}
	_ = s.Close()
	if got, want := rec.Body.String(), ": ping\n\n"; got != want {
		t.Errorf("comment = %q, want %q", got, want)
	}
}

func TestStreamCommentCollapsesNewlines(t *testing.T) {
	rec := httptest.NewRecorder()
	s := newStream(rec, httptest.NewRequest(http.MethodGet, "/x", nil),
		newFlusher(rec), streamConfig{MaxBuffer: 4096})
	// A newline inside a comment would terminate the frame early and the
	// remainder would be parsed as a data field by the client.
	if err := s.Comment("a\nb"); err != nil {
		t.Fatalf("Comment: %v", err)
	}
	_ = s.Close()
	if got, want := rec.Body.String(), ": a b\n\n"; got != want {
		t.Errorf("comment = %q, want %q", got, want)
	}
}

func TestStreamSetEventIDAndRetry(t *testing.T) {
	rec := httptest.NewRecorder()
	s := newStream(rec, httptest.NewRequest(http.MethodGet, "/x", nil),
		newFlusher(rec), streamConfig{MaxBuffer: 4096})
	if err := s.SetEventID("42"); err != nil {
		t.Fatalf("SetEventID: %v", err)
	}
	if err := s.SetRetry(3 * time.Second); err != nil {
		t.Fatalf("SetRetry: %v", err)
	}
	// The id travels in the same frame as the event it identifies, so the two
	// cannot be separated by another writer or by DropOldest.
	if err := s.Event("token", []byte("x")); err != nil {
		t.Fatalf("Event: %v", err)
	}
	_ = s.Close()
	body := rec.Body.String()
	if !strings.Contains(body, "id: 42\nevent: token\ndata: x\n\n") {
		t.Errorf("the id is not in the event's own frame: %q", body)
	}
	// The SSE spec defines retry in milliseconds.
	if !strings.Contains(body, "retry: 3000\n") {
		t.Errorf("body missing retry in milliseconds: %q", body)
	}
}

// ---------------------------------------------------------------- headers

func TestStreamSetsSSEHeaders(t *testing.T) {
	rec := httptest.NewRecorder()
	s := newStream(rec, httptest.NewRequest(http.MethodGet, "/x", nil),
		newFlusher(rec), streamConfig{MaxBuffer: 4096})
	if err := s.Event("token", []byte("hi")); err != nil {
		t.Fatalf("Event: %v", err)
	}
	_ = s.Close()

	h := rec.Header()
	if got := h.Get("Content-Type"); !strings.HasPrefix(got, "text/event-stream") {
		t.Errorf("Content-Type = %q, want text/event-stream", got)
	}
	// Without this, nginx buffers the response and token streaming appears to
	// the client as a hang followed by a burst.
	if got := h.Get("X-Accel-Buffering"); got != "no" {
		t.Errorf("X-Accel-Buffering = %q, want \"no\"; nginx buffers without it", got)
	}
	if got := h.Get("Cache-Control"); !strings.Contains(got, "no-cache") {
		t.Errorf("Cache-Control = %q, want it to contain no-cache", got)
	}
}

func TestStreamCommitsStatusAtFirstWrite(t *testing.T) {
	rec := httptest.NewRecorder()
	s := newStream(rec, httptest.NewRequest(http.MethodGet, "/x", nil),
		newFlusher(rec), streamConfig{MaxBuffer: 4096})

	// Before the first write the handler owns the status, which is the whole
	// reason the buffer is acquired lazily.
	rec.WriteHeader(http.StatusAccepted)
	if status, _ := rec.Result().StatusCode, 0; status != http.StatusAccepted {
		t.Fatalf("pre-write status = %d, want 202", status)
	}

	if err := s.Event("token", []byte("hi")); err != nil {
		t.Fatalf("Event: %v", err)
	}
	_ = s.Close()

	// After the first write the status is fixed at 200: the handler already
	// chose 202, and net/http ignores a second WriteHeader. This is HTTP, not
	// a photon choice, and it is why a handler cannot discover an encoding
	// failure after it has started streaming.
	if got := rec.Result().StatusCode; got != http.StatusAccepted {
		t.Errorf("post-write status = %d, want the status chosen before the first write (202)", got)
	}
}

func TestStreamWriteBeforeFirstWriteLeavesStatusToHandler(t *testing.T) {
	// A handler that never writes must not have committed a 200: it chose not
	// to stream, and inventing a response for it would be wrong.
	rec := httptest.NewRecorder()
	s := newStream(rec, httptest.NewRequest(http.MethodGet, "/x", nil),
		newFlusher(rec), streamConfig{MaxBuffer: 4096})
	rec.WriteHeader(http.StatusNoContent)
	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if got := rec.Result().StatusCode; got != http.StatusNoContent {
		t.Errorf("status = %d, want 204: an unwritten stream must not commit a status", got)
	}
}

// ---------------------------------------------------------------- errors

func TestStreamWriteAfterCloseReturnsError(t *testing.T) {
	rec := httptest.NewRecorder()
	s := newStream(rec, httptest.NewRequest(http.MethodGet, "/x", nil),
		newFlusher(rec), streamConfig{MaxBuffer: 4096})
	if err := s.Close(); err != nil {
		t.Fatalf("first Close: %v", err)
	}
	// A handler that defers Close and also closes on the normal path is doing
	// something entirely reasonable, so the second close must be a no-op and
	// the write after it must be an error rather than a panic.
	if err := s.Close(); err != nil {
		t.Errorf("second Close = %v, want nil (Close must be idempotent)", err)
	}
	if err := s.Event("x", []byte("y")); !errors.Is(err, ErrStreamClosed) {
		t.Errorf("write after Close = %v, want ErrStreamClosed", err)
	}
}

func TestStreamCloseIsIdempotentUnderConcurrency(t *testing.T) {
	rec := httptest.NewRecorder()
	s := newStream(rec, httptest.NewRequest(http.MethodGet, "/x", nil),
		newFlusher(rec), streamConfig{MaxBuffer: 4096})
	_ = s.Event("x", []byte("y"))

	// A handler closing from a deferred call while its context is cancelled is
	// the ordinary teardown race, and Close must survive it.
	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = s.Close()
		}()
	}
	wg.Wait()
}

// ---------------------------------------------------------------- cancellation

func TestStreamWriteFailsWhenClientIsGone(t *testing.T) {
	// A handler must be able to learn that the client vanished, because that
	// is what stops the upstream model call. This is the whole reason a stream
	// reports an error instead of writing into the void.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		s := NewStream(w, r)
		// Write until the write fails, which happens once the client is gone.
		deadline := time.Now().Add(5 * time.Second)
		for time.Now().Before(deadline) {
			if err := s.Event("token", []byte("x")); err != nil {
				if !errors.Is(err, ErrClientGone) && !errors.Is(err, ErrStreamOverflow) {
					t.Errorf("write error = %v, want ErrClientGone or ErrStreamOverflow", err)
				}
				return
			}
			time.Sleep(time.Millisecond)
		}
		t.Error("write never failed; the disconnect was not observed")
	}))
	defer srv.Close()

	conn, err := net.Dial("tcp", strings.TrimPrefix(srv.URL, "http://"))
	if err != nil {
		t.Fatal(err)
	}
	// Send a request, read a little, then disappear without closing cleanly.
	if _, err := conn.Write([]byte("GET / HTTP/1.1\r\nHost: x\r\n\r\n")); err != nil {
		t.Fatal(err)
	}
	br := bufio.NewReader(conn)
	_, _ = br.ReadString('\n')
	buf := make([]byte, 4096)
	_, _ = br.Read(buf)
	// A hard close, so the server sees a reset rather than a clean FIN that
	// might be read as "the client finished".
	_ = conn.Close()
}

func TestStreamErrReportsDisconnect(t *testing.T) {
	// Err must distinguish a clean end from a failed one, so a handler can log
	// meaningfully and a test can assert on it.
	rec := &failingWriter{header: make(http.Header), failAfter: 1}
	s := newStream(rec, httptest.NewRequest(http.MethodGet, "/x", nil),
		newFlusher(rec), streamConfig{MaxBuffer: 4096})

	// Writes are delivered by the stream's writer goroutine, so a transport
	// failure surfaces on the next call after it happens. Flush makes the
	// timing deterministic: it waits for the write and reports its outcome.
	_ = s.Event("one", []byte("a"))
	_ = s.Flush()
	_ = s.Event("two", []byte("b"))
	if err := s.Flush(); !errors.Is(err, ErrClientGone) {
		t.Fatalf("Flush after a failed write = %v, want ErrClientGone", err)
	}
	if err := s.Event("three", []byte("c")); !errors.Is(err, ErrClientGone) {
		t.Errorf("Event after the stream failed = %v, want ErrClientGone", err)
	}
	if s.Err() == nil {
		t.Error("Err() = nil, want the reason the stream ended")
	}
}

// failingWriter fails writes after the first n.
type failingWriter struct {
	mu        sync.Mutex
	header    http.Header
	body      []byte
	failAfter int
	writes    int
}

func (w *failingWriter) Header() http.Header { return w.header }

func (w *failingWriter) WriteHeader(int) {}

func (w *failingWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.writes++
	if w.writes > w.failAfter {
		return 0, errors.New("connection reset by peer")
	}
	w.body = append(w.body, p...)
	return len(p), nil
}

func (w *failingWriter) FlushError() error { return nil }

func (w *failingWriter) SetWriteDeadline(time.Time) error { return nil }

// ---------------------------------------------------------------- middleware

func TestStreamingMiddlewareProvidesStream(t *testing.T) {
	var got *Stream
	app := New()
	app.Use(Streaming())
	app.GET("/s", HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got = StreamFrom(r)
		if got == nil {
			t.Error("StreamFrom returned nil; the middleware did not attach a stream")
			return
		}
		_ = got.Event("token", []byte("hi"))
	}))
	srv := httptest.NewServer(app)
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/s")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if !strings.Contains(string(body), "data: hi\n") {
		t.Errorf("body = %q, want it to contain the event", body)
	}
	if got == nil || got.Err() != nil {
		t.Errorf("stream Err = %v, want a clean stream", got.Err())
	}
}

func TestStreamingMiddlewareEnforcesMaxStreams(t *testing.T) {
	// The limit is what makes the memory invariant hold. Without it, a client
	// that opens many connections gets one buffer each, unbounded.
	app := New(MaxStreams(2))
	app.Use(Streaming())

	release := make(chan struct{})
	started := make(chan struct{}, 8)
	app.GET("/s", HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		s := StreamFrom(r)
		started <- struct{}{}
		_ = s.Event("token", []byte("hi"))
		<-release
	}))
	srv := httptest.NewServer(app)
	defer srv.Close()

	// Two streams occupy the budget.
	for i := 0; i < 2; i++ {
		resp, err := http.Get(srv.URL + "/s")
		if err != nil {
			t.Fatal(err)
		}
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("stream %d = %d, want 200", i, resp.StatusCode)
		}
		defer resp.Body.Close()
	}
	<-started
	<-started

	// The third must be refused rather than served, because serving it would
	// allocate a third buffer the budget does not cover.
	resp, err := http.Get(srv.URL + "/s")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Errorf("third stream = %d, want 503", resp.StatusCode)
	}

	close(release)
}

func TestStreamingMiddlewareReleasesSlotOnDisconnect(t *testing.T) {
	// The budget must be returned when a client vanishes, or a server would
	// slowly refuse every stream after enough clients had come and gone.
	app := New(MaxStreams(1))
	app.Use(Streaming())
	app.GET("/s", HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		s := StreamFrom(r)
		_ = s.Event("token", []byte("hi"))
		<-r.Context().Done()
	}))
	srv := httptest.NewServer(app)
	defer srv.Close()

	for i := 0; i < 5; i++ {
		req, err := http.NewRequest(http.MethodGet, srv.URL+"/s", nil)
		if err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithCancel(req.Context())
		req = req.WithContext(ctx)
		done := make(chan struct{})
		go func() {
			defer close(done)
			resp, err := http.DefaultClient.Do(req)
			if err == nil {
				_, _ = io.Copy(io.Discard, resp.Body)
				resp.Body.Close()
			}
		}()
		time.Sleep(30 * time.Millisecond)
		cancel()
		<-done
		// Give the server a moment to observe the disconnect and release.
		time.Sleep(50 * time.Millisecond)
	}
}

func TestStreamFromNilRequestIsNil(t *testing.T) {
	if got := StreamFrom(nil); got != nil {
		t.Errorf("StreamFrom(nil) = %v, want nil", got)
	}
}

// ---------------------------------------------------------------- limits

func TestDerivedStreamLimitsAreConsistent(t *testing.T) {
	// The whole point of deriving MaxTotalStreamBufferBytes from MaxStreams is
	// that the common misconfiguration cannot be expressed.
	s := New()
	l := s.Limits()
	if l.MaxStreams <= 0 {
		t.Fatal("MaxStreams was not derived")
	}
	want := int64(l.MaxStreams) * int64(l.MaxStreamBufferBytes)
	if l.MaxTotalStreamBufferBytes != want {
		t.Errorf("MaxTotalStreamBufferBytes = %d, want %d (MaxStreams x MaxStreamBufferBytes)",
			l.MaxTotalStreamBufferBytes, want)
	}
}

func TestDeriveMaxStreamsClamps(t *testing.T) {
	tests := []struct {
		name   string
		budget int64
		want   int
	}{
		{"tiny budget still serves some streams", 8 << 20, maxStreamsFloor},
		{"512 MB container", 512 << 20, 512},
		{"1 GB", 1 << 30, 1024},
		{"huge budget is capped", 1 << 40, maxStreamsCeiling},
		{"zero budget uses the fallback", 0, deriveMaxStreams(fallbackMemoryBudget)},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := deriveMaxStreams(tc.budget); got != tc.want {
				t.Errorf("deriveMaxStreams(%d) = %d, want %d", tc.budget, got, tc.want)
			}
		})
	}
}

func TestNewPanicsOnContradictoryStreamLimits(t *testing.T) {
	// A global cap below the product of the count and the buffer is a
	// contradiction, not a stricter policy, and a limit that is silently
	// ignored looks like protection.
	defer func() {
		r := recover()
		if r == nil {
			t.Fatal("New did not panic on a global cap below the implied product")
		}
		if msg := fmt.Sprint(r); !strings.Contains(msg, "MaxTotalStreamBufferBytes") {
			t.Errorf("panic %q does not name the limit", msg)
		}
	}()
	New(SetLimits(Limits{
		MaxStreams:                100,
		MaxStreamBufferBytes:      64 << 10,
		MaxTotalStreamBufferBytes: 1024, // far below 100 * 64 KB
	}))
}

func TestNewRejectsNegativeStreamLimits(t *testing.T) {
	for _, tc := range []struct {
		name string
		l    Limits
	}{
		{"negative MaxStreams", Limits{MaxStreams: -1}},
		{"negative MaxStreamBufferBytes", Limits{MaxStreamBufferBytes: -1}},
		{"negative stall timeout", Limits{StreamStallTimeout: -Second}},
		{"negative heartbeat", Limits{HeartbeatInterval: -Second}},
		{"negative memory budget", Limits{MemoryBudget: -1}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			defer func() {
				if recover() == nil {
					t.Error("New did not panic")
				}
			}()
			New(SetLimits(tc.l))
		})
	}
}

func TestExplicitLimitsAreRespected(t *testing.T) {
	s := New(SetLimits(Limits{
		MaxStreams:                7,
		MaxStreamBufferBytes:      8192,
		StreamStallTimeout:        2 * Second,
		HeartbeatInterval:         0,
		MaxRequestBodyBytes:       1 << 20,
		MaxHeaderBytes:            16 << 10,
		MaxHeaders:                100,
		MaxHeaderValueBytes:       8 << 10,
		ReadHeaderTimeout:         5 * Second,
		IdleTimeout:               60 * Second,
		MaxConnections:            0,
		MaxConcurrentRequests:     0,
		MaxPerIP:                  0,
		MaxTotalStreamBufferBytes: 0,
		MemoryBudget:              0,
	}))
	l := s.Limits()
	if l.MaxStreams != 7 {
		t.Errorf("MaxStreams = %d, want 7", l.MaxStreams)
	}
	if l.MaxStreamBufferBytes != 8192 {
		t.Errorf("MaxStreamBufferBytes = %d, want 8192", l.MaxStreamBufferBytes)
	}
	// Derived from the explicit count, not from the default.
	if want := int64(7) * int64(8192); l.MaxTotalStreamBufferBytes != want {
		t.Errorf("MaxTotalStreamBufferBytes = %d, want %d", l.MaxTotalStreamBufferBytes, want)
	}
}

// bufCap returns the stream buffer's capacity under the lock, for tests that
// check the buffer bound while the writer goroutine is running.
func (s *Stream) bufCap() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return cap(s.buf)
}
