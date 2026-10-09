package photon

import (
	"bytes"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// Regression tests for the defects found by the independent review of the
// rewritten stream writer, admission control, and mounts (2026-10-09).

// slowWriter accepts every write, but each one takes delay - a client reading
// steadily and slowly. It records when the last byte was accepted.
type slowWriter struct {
	mu       sync.Mutex
	header   http.Header
	delay    time.Duration
	total    int
	lastDone time.Time
}

func (w *slowWriter) Header() http.Header { return w.header }
func (w *slowWriter) WriteHeader(int)     {}
func (w *slowWriter) Write(p []byte) (int, error) {
	time.Sleep(w.delay)
	w.mu.Lock()
	defer w.mu.Unlock()
	w.total += len(p)
	w.lastDone = time.Now()
	return len(p), nil
}
func (w *slowWriter) Flush()                           {}
func (w *slowWriter) SetWriteDeadline(time.Time) error { return nil }

// TestCloseWaitsForASlowReaderInsteadOfAbandoningTheWriter is the regression
// test for the most serious defect the review found. Close waited a fixed two
// stall periods and then returned while the writer goroutine was still
// writing; the handler returned, and the writer went on using a ResponseWriter
// net/http had already finished - a data race on HTTP/1, a panic that crashes
// the process on HTTP/2. Close now waits for as long as the client makes
// progress, so every byte is delivered before it returns.
func TestCloseWaitsForASlowReaderInsteadOfAbandoningTheWriter(t *testing.T) {
	w := &slowWriter{header: http.Header{}, delay: 100 * time.Millisecond}
	s := newStream(w, httptest.NewRequest(http.MethodGet, "/x", nil), newFlusher(w),
		streamConfig{MaxBuffer: 64 << 10, Stall: 250 * time.Millisecond})
	payload := bytes.Repeat([]byte("x"), 60<<10) // ~8 pieces of 100 ms: well over two stall periods
	if _, err := s.Write(payload); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("Close = %v, want a clean end for a client that kept reading", err)
	}
	closedAt := time.Now()
	w.mu.Lock()
	total, last := w.total, w.lastDone
	w.mu.Unlock()
	if total != len(payload) {
		t.Errorf("delivered %d of %d bytes before Close returned", total, len(payload))
	}
	if last.After(closedAt) {
		t.Error("the writer was still writing after Close returned")
	}
}

// forcedDeadlineWriter blocks every Write until a write deadline that has
// already passed is set - the forced deadline Close uses - and then fails it.
// Deadlines in the future are ignored, standing in for a socket whose own
// deadline is far away, so only Close's patience can end the write.
type forcedDeadlineWriter struct {
	header  http.Header
	forced  chan struct{}
	once    sync.Once
	writing atomic.Bool
}

func (w *forcedDeadlineWriter) Header() http.Header { return w.header }
func (w *forcedDeadlineWriter) WriteHeader(int)     {}
func (w *forcedDeadlineWriter) Flush()              {}

func (w *forcedDeadlineWriter) SetWriteDeadline(t time.Time) error {
	if !t.IsZero() && !t.After(time.Now()) {
		w.once.Do(func() { close(w.forced) })
	}
	return nil
}

func (w *forcedDeadlineWriter) Write(p []byte) (int, error) {
	w.writing.Store(true)
	defer w.writing.Store(false)
	<-w.forced
	return 0, timeoutError{}
}

type timeoutError struct{}

func (timeoutError) Error() string { return "i/o timeout" }
func (timeoutError) Timeout() bool { return true }

// TestCloseForcesAStuckWriteToFail checks the other half: a client that has
// stopped reading entirely. Close must end the write - by forcing the deadline
// - and wait for the writer to exit, rather than return with it still inside
// Write.
func TestCloseForcesAStuckWriteToFail(t *testing.T) {
	w := &forcedDeadlineWriter{header: http.Header{}, forced: make(chan struct{})}
	s := newStream(w, httptest.NewRequest(http.MethodGet, "/x", nil), newFlusher(w),
		streamConfig{MaxBuffer: 4 << 10, Stall: 200 * time.Millisecond})
	_ = s.Event("x", []byte("hello"))
	time.Sleep(50 * time.Millisecond) // let the writer enter Write
	start := time.Now()
	err := s.Close()
	if !errors.Is(err, ErrStreamOverflow) {
		t.Errorf("Close = %v, want ErrStreamOverflow", err)
	}
	if w.writing.Load() {
		t.Error("Close returned while the writer was still inside Write")
	}
	if d := time.Since(start); d > 2*time.Second {
		t.Errorf("Close took %s to give up on a stuck client", d)
	}
}

// TestPartlySentFrameIsNotDroppable pins the DropOldest fix: a write that ends
// inside a frame leaves that frame non-droppable, because part of it is
// already on the wire and dropping the rest would cut an event in half.
func TestPartlySentFrameIsNotDroppable(t *testing.T) {
	s := &Stream{policy: DropOldest}
	s.frames = []frameRec{{end: 600, droppable: true}, {end: 900, droppable: true}}
	s.shiftFramesLocked(512) // the batch ended 512 bytes into the first frame
	if len(s.frames) != 2 || s.frames[0].end != 88 || s.frames[0].droppable {
		t.Errorf("frames after a mid-frame write = %+v, want the cut frame kept and not droppable", s.frames)
	}
	if !s.frames[1].droppable {
		t.Error("the untouched frame after it lost its droppable flag")
	}
}

// TestStartHeartbeatsCommitsOnTheCallersGoroutine checks StartHeartbeats opens
// the stream immediately. It used to schedule the first heartbeat on a timer
// goroutine, which could set response headers concurrently with the handler -
// a fatal concurrent map write - and wrote SSE comments into NDJSON streams.
func TestStartHeartbeatsCommitsOnTheCallersGoroutine(t *testing.T) {
	l := DefaultLimits()
	l.HeartbeatInterval = 10 * time.Millisecond
	app := New(SetLimits(l), MemoryBudget(256<<20))
	app.GET("/ndjson", SSE(func(s *Stream, r *http.Request) error {
		s.Header().Set("Content-Type", "application/x-ndjson")
		stop := s.StartHeartbeats()
		defer stop()
		time.Sleep(60 * time.Millisecond) // several heartbeat intervals
		_, err := io.WriteString(s, `{"done":true}`+"\n")
		return err
	}))
	srv := httptest.NewServer(app)
	defer srv.Close()
	resp, err := http.Get(srv.URL + "/ndjson")
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if string(body) != `{"done":true}`+"\n" {
		t.Errorf("body %q: heartbeats leaked into an NDJSON stream", body)
	}
}

// TestStreamAdmissionCountsBorrowedBytes checks MaxTotalStreamBufferBytes is a
// true ceiling: a stream may not be admitted into buffer memory another stream
// has borrowed with Grow.
func TestStreamAdmissionCountsBorrowedBytes(t *testing.T) {
	l := DefaultLimits()
	l.MaxStreams = 4
	l.MaxStreamBufferBytes = 1 << 10
	app := New(SetLimits(l))
	if !app.acquireStream(1<<10) || !app.reserveStreamBytes(2<<10) {
		t.Fatal("setup: the first stream could not be admitted or grow")
	}
	if !app.acquireStream(1 << 10) {
		t.Fatal("a second stream should fit beside 1 KiB + 2 KiB of a 4 KiB budget")
	}
	if app.acquireStream(1 << 10) {
		t.Error("a third stream was admitted although the budget was spent: 2 KiB nominal + 2 KiB borrowed")
	}
}

// TestEncodedPathCannotEscapeToAFallback is the regression test for an
// authorization bypass through a mount. /admin%2Fsecret matched no route on
// the raw path (the escaped slash is not a separator), so it fell through to
// the root mount - whose legacy router read the decoded path /admin/secret and
// served it without the group's authentication. Such a request is now refused.
func TestEncodedPathCannotEscapeToAFallback(t *testing.T) {
	app := New(MemoryBudget(256 << 20))
	app.Mount("/", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, "legacy served "+r.URL.Path)
	}))
	deny := func(http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
		})
	}
	app.Group("/admin", deny).GET("/:page", func(w http.ResponseWriter, r *http.Request) {})
	app.GET("/files/:name", func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, "file "+PathParam(r, "name"))
	})

	for _, tc := range []struct {
		path   string
		status int
		body   string
	}{
		{"/admin%2Fsecret", 400, ""},            // would have reached legacy as /admin/secret
		{"/%61dmin/secret", 400, ""},            // a needless escape, same trick
		{"/admin/secret", 401, ""},              // the route, with its middleware
		{"/files/a%2Fb", 200, "file a/b"},       // %2F inside a route parameter is fine
		{"/other", 200, "legacy served /other"}, // ordinary fallback traffic
	} {
		rec := httptest.NewRecorder()
		app.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, tc.path, nil))
		if rec.Code != tc.status || (tc.body != "" && rec.Body.String() != tc.body) {
			t.Errorf("%s: %d %q, want %d %q", tc.path, rec.Code, rec.Body.String(), tc.status, tc.body)
		}
	}
}
