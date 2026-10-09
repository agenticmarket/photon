package photon

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// Leaks are the failure mode a streaming server cannot afford. A request
// handler that leaked a goroutine would be caught by a load test; a stream that
// leaked one per connection would be an outage, because streams are long-lived
// by definition and the leak accumulates for as long as the server runs.

// TestNoGoroutineLeakAcrossStreamChurn opens and abandons many streams and
// requires the goroutine count to return to where it started.
//
// Each stream is a goroutine held for the stream's whole life, so a leak here
// is not a slow drift - it is one goroutine per connection ever accepted.
func TestNoGoroutineLeakAcrossStreamChurn(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping the goroutine leak test in -short")
	}

	app := New(MaxStreams(64))
	app.Use(Streaming())
	app.GET("/s", HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		s := StreamFrom(r)
		// Write a few events, then block until the client goes away, which is
		// what a real stream does for its entire useful life.
		for i := 0; i < 3; i++ {
			if err := s.Event("token", []byte("x")); err != nil {
				return
			}
		}
		<-r.Context().Done()
	}))
	srv := httptest.NewServer(app)
	defer srv.Close()

	// Warm up so one-time goroutines are not counted as a leak.
	churnStreams(t, srv.URL, 20)
	settle()
	before := runtime.NumGoroutine()

	for round := 0; round < 4; round++ {
		churnStreams(t, srv.URL, 40)
		settle()
	}

	after := runtime.NumGoroutine()
	// A margin rather than an exact count: a connection close in flight can
	// still be unwinding. The point is that the count does not grow with the
	// number of connections served.
	if after > before+15 {
		buf := make([]byte, 1<<16)
		n := runtime.Stack(buf, true)
		t.Errorf("goroutines grew from %d to %d across stream churn (+%d) over 160 streams\n%s",
			before, after, after-before, buf[:n])
	}
}

// TestNoStreamSlotLeak verifies the concurrent stream budget is fully returned,
// so a long-lived server does not slowly refuse every stream.
func TestNoStreamSlotLeak(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping the stream slot leak test in -short")
	}

	app := New(MaxStreams(8))
	app.Use(Streaming())
	app.GET("/s", HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		s := StreamFrom(r)
		_ = s.Event("token", []byte("x"))
		<-r.Context().Done()
	}))
	srv := httptest.NewServer(app)
	defer srv.Close()

	for round := 0; round < 20; round++ {
		churnStreams(t, srv.URL, 8)
		settle()
		if n := app.streams.Load(); n != 0 {
			t.Fatalf("after round %d, %d stream slots still held; want 0. "+
				"A leaked slot means the server will refuse streams after enough clients have come and gone.",
				round, n)
		}
	}
}

// TestStreamBufferStartsSmallAndIsReleased checks a stream's memory follows
// what it actually holds. A token stream has a few hundred bytes in flight at a
// time, so it must not pin the full MaxStreamBufferBytes for its whole life,
// and Close must let the buffer go.
func TestStreamBufferStartsSmallAndIsReleased(t *testing.T) {
	const size = 64 << 10
	rec := httptest.NewRecorder()
	s := newStream(rec, httptest.NewRequest(http.MethodGet, "/x", nil),
		newFlusher(rec), streamConfig{MaxBuffer: size})
	if err := s.Event("token", []byte("hello")); err != nil {
		t.Fatal(err)
	}
	if c := s.bufCap(); c > initialStreamBuffer {
		t.Errorf("buffer capacity after one small event = %d, want at most %d", c, initialStreamBuffer)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if c := s.bufCap(); c != 0 {
		t.Errorf("buffer capacity after Close = %d, want it released", c)
	}
}

// TestStuckWriterIsWaitedForNotAbandoned pins what happens when a write never
// returns and ignores deadlines - a ResponseWriter wrapper that blocks, which
// no deadline reaches.
//
// The producer still gets ErrStreamOverflow within the stall period, so the
// handler stops producing. But Close does not return while the writer is
// inside Write: a writer that outlives the handler would touch a response
// net/http has already finished, which on HTTP/2 crashes the process. An
// earlier version returned after two stall periods with the writer still
// running; that was the defect, not a feature. A writer that honours deadlines
// is forced out instead - see TestCloseForcesAStuckWriteToFail.
func TestStuckWriterIsWaitedForNotAbandoned(t *testing.T) {
	const size = 4 << 10
	stuck := newBlockingWriter()
	s := newStream(stuck, httptest.NewRequest(http.MethodGet, "/x", nil),
		newFlusher(stuck), streamConfig{MaxBuffer: size})
	s.stall = 100 * time.Millisecond

	payload := make([]byte, 512)
	start := time.Now()
	var err error
	for i := 0; i < 1000 && err == nil; i++ {
		_, err = s.Write(payload)
	}
	if !errors.Is(err, ErrStreamOverflow) {
		t.Fatalf("writing into a stuck consumer = %v, want ErrStreamOverflow", err)
	}
	if waited := time.Since(start); waited > 2*time.Second {
		t.Errorf("producer waited %s for a 100ms stall deadline", waited)
	}

	closed := make(chan struct{})
	go func() { _ = s.Close(); close(closed) }()
	select {
	case <-closed:
		t.Fatal("Close returned while the writer was still blocked inside Write")
	case <-time.After(500 * time.Millisecond):
	}
	stuck.release()
	select {
	case <-closed:
	case <-time.After(2 * time.Second):
		t.Fatal("Close did not return after the write finished")
	}
	if c := s.bufCap(); c != 0 {
		t.Error("the buffer was retained after Close")
	}
}

// TestGracefulShutdownDrainsActiveStreams checks that a stream in flight is
// allowed to finish when Shutdown is called, rather than being cut off.
//
// The design requires streams to drain: an AI response that is cut in half when
// the server is asked to stop is a worse outcome than one that takes a moment
// longer to stop.
func TestGracefulShutdownDrainsActiveStreams(t *testing.T) {
	app := New()
	app.Use(Streaming())

	finished := make(chan struct{})
	writing := make(chan struct{})
	app.GET("/s", HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		s := StreamFrom(r)
		for i := 0; i < 5; i++ {
			if err := s.Event("token", []byte("tok")); err != nil {
				return
			}
			time.Sleep(30 * time.Millisecond)
		}
		_ = s.Close()
		close(finished)
	}))
	_ = writing

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	serveErr := make(chan error, 1)
	go func() { serveErr <- app.Serve(ln) }()

	base := "http://" + ln.Addr().String()
	resp, err := http.Get(base + "/s")
	if err != nil {
		t.Fatal(err)
	}

	// Shutdown while the stream is still writing. The stream must be allowed to
	// complete within the grace period.
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	shutdownDone := make(chan error, 1)
	go func() { shutdownDone <- app.Shutdown(ctx) }()

	select {
	case <-finished:
	case <-time.After(10 * time.Second):
		t.Fatal("the stream did not finish; Shutdown cut an active stream short")
	}

	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if got := strings.Count(string(body), "data: tok"); got != 5 {
		t.Errorf("received %d events, want 5; a drained stream must deliver everything it accepted", got)
	}

	select {
	case err := <-shutdownDone:
		if err != nil {
			t.Errorf("Shutdown = %v, want nil after a clean drain", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Shutdown did not return")
	}
	if err := <-serveErr; err != nil {
		t.Errorf("Serve = %v, want nil after a clean shutdown", err)
	}
}

// TestShutdownDeadlineCutsStreamsHonestly checks the other half: when the
// deadline passes, streams are cut off and the caller is told.
//
// A deadline that expired has not given the work time to finish. Reporting
// success there would be the dishonest outcome, and an operator would never
// learn their streams were being truncated.
func TestShutdownDeadlineCutsStreamsHonestly(t *testing.T) {
	app := New()
	app.Use(Streaming())
	app.GET("/s", HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		s := StreamFrom(r)
		_ = s.Event("token", []byte("x"))
		// Outlive the shutdown deadline.
		time.Sleep(5 * time.Second)
		_ = s.Close()
	}))

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go func() { _ = app.Serve(ln) }()
	base := "http://" + ln.Addr().String()

	resp, err := http.Get(base + "/s")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	time.Sleep(100 * time.Millisecond)

	// A short deadline the stream cannot meet.
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	err = app.Shutdown(ctx)
	if err == nil {
		t.Error("Shutdown = nil, want the deadline error; a cut-off stream must be reported")
	}
}

// TestStreamSlotReleasedOnPanic checks a panicking handler does not strand its
// slot.
//
// The panic must not leave resources permanently retained: a stream budget that
// leaks one slot per panic is a server that stops accepting streams after
// enough bad requests, which is a denial of service reachable by any client
// that can trigger a handler bug.
func TestStreamSlotReleasedOnPanic(t *testing.T) {
	app := New(MaxStreams(2))
	app.Use(Streaming())
	app.GET("/boom", HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		s := StreamFrom(r)
		_ = s.Event("token", []byte("x"))
		panic("handler exploded mid-stream")
	}))
	srv := httptest.NewServer(app)
	defer srv.Close()

	for i := 0; i < 5; i++ {
		resp, err := http.Get(srv.URL + "/boom")
		if err == nil {
			resp.Body.Close()
		}
		settle()
		if n := app.streams.Load(); n != 0 {
			t.Fatalf("after %d panicking requests, %d stream slots still held; want 0", i+1, n)
		}
	}
}

// TestConcurrentStreamsShareOneRouter runs many streams concurrently through a
// single router, which is the ordinary production shape.
func TestConcurrentStreamsShareOneRouter(t *testing.T) {
	app := New(MaxStreams(64))
	app.Use(Streaming())

	// A parameterised route, so routing and streaming are exercised together.
	var served atomic.Int64
	app.GET("/agent/:id/stream", HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		s := StreamFrom(r)
		id := PathParam(r, "id")
		for i := 0; i < 5; i++ {
			if err := s.Event("token", []byte(id)); err != nil {
				return
			}
		}
		_ = s.Close()
		served.Add(1)
	}))
	srv := httptest.NewServer(app)
	defer srv.Close()

	const n = 30
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			id := strconv.Itoa(i)
			resp, err := http.Get(srv.URL + "/agent/" + id + "/stream")
			if err != nil {
				t.Errorf("get: %v", err)
				return
			}
			defer resp.Body.Close()
			body, _ := io.ReadAll(resp.Body)
			// Every event must carry this request's own id: a cross-talk bug
			// here would send one agent another agent's output.
			if got := strings.Count(string(body), "data: "+id); got != 5 {
				t.Errorf("agent %s received %d of its own events; body:\n%s", id, got, body)
			}
		}(i)
	}
	wg.Wait()

	if got := served.Load(); got != n {
		t.Errorf("served %d streams, want %d", got, n)
	}
}

// ---------------------------------------------------------------- helpers

// churnStreams opens n connections, reads a little of each stream, then
// disconnects abruptly.
func churnStreams(t *testing.T, base string, n int) {
	t.Helper()
	addr := strings.TrimPrefix(base, "http://")
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			c, err := net.DialTimeout("tcp", addr, 5*time.Second)
			if err != nil {
				return
			}
			// Half are abandoned mid-request, the harshest case for the
			// server's connection and slot accounting.
			if i%2 == 0 {
				_, _ = c.Write([]byte("GET /s HTTP/1.1\r\nHost: x\r\n\r\n"))
				// Read a little so the stream actually starts.
				buf := make([]byte, 256)
				_, _ = c.Read(buf)
			}
			_ = c.Close()
		}()
	}
	wg.Wait()
}

// settle gives finished goroutines a chance to be reaped.
func settle() {
	for i := 0; i < 20; i++ {
		runtime.GC()
		time.Sleep(25 * time.Millisecond)
	}
}
