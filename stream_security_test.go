package photon

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// These are the attacks a streaming endpoint actually faces. A streaming
// handler is a long-lived, write-heavy, client-controlled resource, which makes
// it a different attack surface from an ordinary request: an attacker is not
// trying to make one request expensive, they are trying to make one *connection*
// expensive and hold it.
//
// The assertion throughout is the same: memory stays bounded and the server
// stays serving. A streaming server that survives these and then serves another
// request correctly is doing its job.

// TestSecurityManyConnectionsNeverRead opens the maximum number of streams this
// host allows and never reads a byte from any of them.
//
// This is the worst case for a naive implementation: every client holds a
// connection open, drains nothing, and the server has no signal that anything is
// wrong.
func TestSecurityManyConnectionsNeverRead(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping the many-connection test in -short")
	}

	// Kept below this host's ~200 accept-backlog ceiling, which is a property of
	// Windows rather than of photon. Exceeding it produces connection refusals
	// from the loopback stack before any server code runs.
	const clients = 100
	const bufferBytes = 8 << 10

	app := New(SetLimits(Limits{
		MaxStreams:                clients,
		MaxStreamBufferBytes:      bufferBytes,
		StreamStallTimeout:        300 * time.Millisecond,
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
	app.Use(Streaming())
	app.GET("/s", HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		s := StreamFrom(r)
		payload := make([]byte, 512)
		deadline := time.Now().Add(3 * time.Second)
		for time.Now().Before(deadline) {
			if err := s.Event("token", payload); err != nil {
				return
			}
		}
	}))
	// A cheap endpoint to prove the server is still serving after the attack.
	app.GET("/ping", HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = Text(w, http.StatusOK, "pong")
	}))
	srv := httptest.NewServer(app)
	defer srv.Close()

	addr := strings.TrimPrefix(srv.URL, "http://")
	var wg sync.WaitGroup
	for i := 0; i < clients; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			c, err := net.DialTimeout("tcp", addr, 10*time.Second)
			if err != nil {
				return
			}
			defer c.Close()
			// Ask for a stream, then never read it.
			_, _ = c.Write([]byte("GET /s HTTP/1.1\r\nHost: x\r\n\r\n"))
			time.Sleep(3 * time.Second)
		}()
	}

	// While every client is holding a stream open and reading nothing, the heap
	// must stay near the product of the stream count and the buffer size - and
	// crucially, must not grow past it.
	runtime.GC()
	base := heapInUseMB()

	var peak float64
	for i := 0; i < 12; i++ {
		time.Sleep(200 * time.Millisecond)
		if h := heapInUseMB(); h > peak {
			peak = h
		}
		// The server must still serve an ordinary request while every stream is
		// stalled. A server that cannot answer /ping is already an outage, and
		// that is the real assertion here.
		resp, err := http.Get(srv.URL + "/ping")
		if err != nil {
			t.Fatalf("round %d: the server stopped serving while streams were stalled: %v", i, err)
		}
		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
	}
	wg.Wait()

	// The theoretical maximum: every stream holding a full buffer.
	maxExpectedMB := float64(clients*bufferBytes) / (1 << 20)
	t.Logf("base %.1f MB, peak %.1f MB with %d unread streams (buffers alone = %.1f MB)",
		base, peak, clients, maxExpectedMB)

	// A generous multiple of the buffers themselves, because goroutine stacks,
	// response overhead and the client side all count. An unbounded
	// implementation would be orders of magnitude past this.
	allowance := maxExpectedMB*4 + 40
	if peak-base > allowance {
		t.Errorf("heap grew %.1f MB with %d clients that never read (allowance %.1f MB); "+
			"a slow consumer must not be able to grow the server", peak-base, clients, allowance)
	}
}

// TestSecurityOversizedEvent checks a single enormous event is streamed rather
// than buffered whole.
//
// An event larger than the buffer must be split across flushes, not refused and
// not allocated. Refusing it would make a legitimately large payload impossible;
// allocating it would hand the client a way to request arbitrary memory.
func TestSecurityOversizedEvent(t *testing.T) {
	const cap_ = 4 << 10

	rec := httptest.NewRecorder()
	s := newStream(rec, httptest.NewRequest(http.MethodGet, "/x", nil),
		newFlusher(rec), streamConfig{MaxBuffer: cap_})
	s.stall = 200 * time.Millisecond

	// 10x the buffer, which must be delivered in pieces.
	huge := make([]byte, cap_*10)
	for i := range huge {
		huge[i] = 'z'
	}
	if err := s.Event("big", huge); err != nil {
		t.Fatalf("an oversized event was refused: %v; it should be split across flushes", err)
	}
	_ = s.Close()

	// Every byte must arrive, and the buffer must never have exceeded its cap.
	if got := strings.Count(rec.Body.String(), "z"); got != len(huge) {
		t.Errorf("delivered %d of %d bytes; an oversized event must be split, not truncated", got, len(huge))
	}
	if s.grewPast {
		t.Error("the buffer grew; an oversized event must be streamed through the bound, not by growing it")
	}
}

// TestSecurityCancellationStorm cancels many requests at once, which is what an
// attacker does to force goroutine and slot churn.
//
// The danger is a slot or a goroutine released twice, or not at all, under a
// burst of concurrent teardowns.
func TestSecurityCancellationStorm(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping the cancellation storm test in -short")
	}

	app := New(MaxStreams(32))
	app.Use(Streaming())
	app.GET("/s", HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		s := StreamFrom(r)
		_ = s.Event("token", []byte("x"))
		<-r.Context().Done()
	}))
	srv := httptest.NewServer(app)
	defer srv.Close()

	for round := 0; round < 6; round++ {
		var wg sync.WaitGroup
		for i := 0; i < 32; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				req, err := http.NewRequestWithContext(ctx, http.MethodGet, srv.URL+"/s", nil)
				if err != nil {
					return
				}
				done := make(chan struct{})
				go func() {
					defer close(done)
					resp, err := http.DefaultClient.Do(req)
					if err == nil {
						_, _ = io.Copy(io.Discard, resp.Body)
						resp.Body.Close()
					}
				}()
				// Cancel at a slightly different moment per goroutine, so the
				// teardowns do not all land together.
				time.Sleep(time.Duration(i%8) * time.Millisecond)
				cancel()
				<-done
			}()
		}
		wg.Wait()
		settle()

		if n := app.streams.Load(); n != 0 {
			t.Fatalf("round %d: %d stream slots still held after a cancellation storm; want 0", round, n)
		}
	}
}

// TestSecurityImmediateDisconnect opens and closes streams with no reading at
// all, which is the cheapest way to make a server allocate.
//
// The connection is closed before the handler can write, so the server does the
// work of setting up a stream for a client that is already gone.
func TestSecurityImmediateDisconnect(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping the immediate-disconnect test in -short")
	}

	app := New(MaxStreams(16))
	app.Use(Streaming())
	app.GET("/s", HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		s := StreamFrom(r)
		for i := 0; i < 100; i++ {
			if err := s.Event("token", []byte("x")); err != nil {
				return
			}
		}
	}))
	srv := httptest.NewServer(app)
	defer srv.Close()

	addr := strings.TrimPrefix(srv.URL, "http://")
	for round := 0; round < 5; round++ {
		var wg sync.WaitGroup
		for i := 0; i < 60; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				c, err := net.DialTimeout("tcp", addr, 5*time.Second)
				if err != nil {
					return
				}
				// Disconnect immediately, before writing a full request.
				_ = c.Close()
			}()
		}
		wg.Wait()
		settle()
		if n := app.streams.Load(); n != 0 {
			t.Fatalf("round %d: %d stream slots held after immediate disconnects", round, n)
		}
	}
}

// TestSecurityProducerFailureStorm fails the producer repeatedly, because a
// failing upstream model is the ordinary case in production, not an edge case.
//
// The response must terminate predictably and the server must keep serving. A
// stream that cannot be closed cleanly after an error is a connection leak.
func TestSecurityProducerFailureStorm(t *testing.T) {
	app := New(MaxStreams(32))
	app.Use(Streaming())
	app.GET("/fail", HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		s := StreamFrom(r)
		_ = s.Event("token", []byte("partial"))
		// The upstream failed after sending some output.
		_ = s.Close()
	}))
	app.GET("/ping", HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = Text(w, http.StatusOK, "pong")
	}))
	srv := httptest.NewServer(app)
	defer srv.Close()

	for round := 0; round < 4; round++ {
		var wg sync.WaitGroup
		for i := 0; i < 25; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				resp, err := http.Get(srv.URL + "/fail")
				if err != nil {
					return
				}
				// The partial output must still be delivered, even though the
				// producer failed: the client already paid for those tokens.
				body, _ := io.ReadAll(resp.Body)
				resp.Body.Close()
				if !strings.Contains(string(body), "partial") {
					t.Errorf("partial output was lost; got %q", body)
				}
			}()
		}
		wg.Wait()
		settle()
	}

	// The server must still be healthy.
	resp, err := http.Get(srv.URL + "/ping")
	if err != nil {
		t.Fatalf("the server stopped serving after a producer failure storm: %v", err)
	}
	resp.Body.Close()
}

// TestSecurityMalformedStreamRequests checks a streaming endpoint does not
// become a place where ordinary request validation is skipped.
func TestSecurityMalformedStreamRequests(t *testing.T) {
	app := New()
	app.Use(Streaming())
	app.POST("/s", HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		s := StreamFrom(r)
		_ = s.Event("token", []byte("x"))
	}))
	srv := httptest.NewServer(app)
	defer srv.Close()

	tests := []struct {
		name    string
		method  string
		path    string
		body    string
		wantMin int
	}{
		// GET on a POST-only path is a 405, not a 404: the path exists, just not
		// for that method. Asserting 404 here would have been my error, and the
		// router is right.
		{"wrong method", http.MethodGet, "/s", "", http.StatusMethodNotAllowed},
		{"unknown path", http.MethodPost, "/nope", "", http.StatusNotFound},
		{"oversized body", http.MethodPost, "/s", strings.Repeat("x", 2<<20), http.StatusRequestEntityTooLarge},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			req, err := http.NewRequest(tc.method, srv.URL+tc.path, strings.NewReader(tc.body))
			if err != nil {
				t.Fatal(err)
			}
			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				t.Fatal(err)
			}
			defer resp.Body.Close()
			io.Copy(io.Discard, resp.Body)
			if resp.StatusCode != tc.wantMin {
				t.Errorf("status = %d, want %d", resp.StatusCode, tc.wantMin)
			}
		})
	}
}

// TestSecurityExtremeStreamCount pushes the stream limit far past what this
// host can hold, which is what a real attack looks like: the limit should be
// enforced by the server, not by the client's ability to open sockets.
func TestSecurityExtremeStreamCount(t *testing.T) {
	// A deliberately tiny budget: the point is that the limit is honoured, not
	// that it is large.
	const maxStreams = 4

	app := New(MaxStreams(maxStreams))
	app.Use(Streaming())
	release := make(chan struct{})
	var admitted atomic.Int64
	app.GET("/s", HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		admitted.Add(1)
		s := StreamFrom(r)
		_ = s.Event("token", []byte("x"))
		<-release
	}))
	srv := httptest.NewServer(app)
	defer srv.Close()

	// More concurrent stream requests than the budget allows.
	const attempts = 40
	var wg sync.WaitGroup
	var refused, served atomic.Int64
	for i := 0; i < attempts; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			resp, err := http.Get(srv.URL + "/s")
			if err != nil {
				return
			}
			defer resp.Body.Close()
			if resp.StatusCode == http.StatusServiceUnavailable {
				refused.Add(1)
			} else {
				served.Add(1)
			}
			// Hold the stream open so the budget stays occupied.
			_, _ = io.Copy(io.Discard, resp.Body)
		}()
	}

	time.Sleep(500 * time.Millisecond)
	if got := app.streams.Load(); got > maxStreams {
		t.Errorf("%d concurrent streams admitted, over the limit of %d", got, maxStreams)
	}
	close(release)
	wg.Wait()

	t.Logf("%d served, %d refused, %d handlers entered", served.Load(), refused.Load(), admitted.Load())
	if refused.Load() == 0 {
		t.Errorf("no request was refused despite %d attempts against a budget of %d", attempts, maxStreams)
	}
}

// TestSecurityNoPanicOnHostileInput feeds the framing code input designed to
// break it, since a panic in a stream after headers are committed cannot be
// reported to the client and must not take down the process.
func TestSecurityNoPanicOnHostileInput(t *testing.T) {
	hostile := [][]byte{
		nil,
		{},
		{0x00},
		[]byte("\n\n\n"),
		[]byte("\r"),
		[]byte("\r\n\r\n"),
		[]byte(strings.Repeat("\n", 1000)),
		[]byte(strings.Repeat("x", 1<<20)),
		{0xff, 0xfe, 0xfd, 0xfc},
		[]byte("event: \x00\x01\x02"),
		[]byte("data: \r\n\r\n\r\n"),
	}
	for i, payload := range hostile {
		t.Run(fmt.Sprintf("payload%d", i), func(t *testing.T) {
			rec := httptest.NewRecorder()
			s := newStream(rec, httptest.NewRequest(http.MethodGet, "/x", nil),
				newFlusher(rec), streamConfig{MaxBuffer: 4096})
			s.stall = 200 * time.Millisecond
			// Whatever happens, it must not panic.
			_ = s.Event("hostile", payload)
			_ = s.Comment(string(payload[:min(len(payload), 64)]))
			_ = s.SetEventID(string(payload[:min(len(payload), 32)]))
			_ = s.Close()
		})
	}
}
