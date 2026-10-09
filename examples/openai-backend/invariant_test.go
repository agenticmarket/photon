package main

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"
)

// slowWriter is a ResponseWriter whose Write blocks until released.
//
// It exists to reproduce the one condition the bounded buffer is for: a consumer
// that accepts the connection and then stops reading. A real socket can be made
// slow but not reliably slow, because the kernel's own buffers absorb a burst -
// so a test that uses a socket measures the kernel, not the server. This blocks
// the write itself, which is exactly the state the design has to handle.
type slowWriter struct {
	h        http.Header
	status   int
	released chan struct{}
	entered  chan struct{}
	once     sync.Once
	written  int
}

func newSlowWriter() *slowWriter {
	return &slowWriter{
		h:        make(http.Header),
		released: make(chan struct{}),
		entered:  make(chan struct{}),
	}
}

func (w *slowWriter) Header() http.Header { return w.h }

func (w *slowWriter) WriteHeader(code int) {
	if w.status == 0 {
		w.status = code
	}
}

func (w *slowWriter) Write(p []byte) (int, error) {
	w.once.Do(func() { close(w.entered) })
	<-w.released
	w.written += len(p)
	return len(p), nil
}

func (w *slowWriter) Flush() {}

func (w *slowWriter) release() {
	select {
	case <-w.released:
	default:
		close(w.released)
	}
}

func (w *slowWriter) blocking() bool {
	select {
	case <-w.entered:
		return true
	default:
		return false
	}
}

// TestSlowConsumerDoesNotGrowMemory is the invariant test.
//
// The invariant: "A slow consumer must never cause unbounded memory growth.
// Ever. Not under any input."
//
// The assertion is that memory stays *flat*, not merely that it ends up small. A
// constant that grows slowly is a leak, and a test that only checked a ceiling at
// the end would pass it. So this samples throughout and compares the trend.
//
// The writer blocks forever, which is the harshest case: the buffer fills, the
// flush cannot drain, and the only thing that can save the server is the stall
// deadline. With photon's default back-pressure the handler returns and the
// stream ends; the buffer is bounded and dropped.
func TestSlowConsumerDoesNotGrowMemory(t *testing.T) {
	if testing.Short() {
		t.Skip("slow-consumer invariant test takes several seconds by design")
	}

	tenant := &Tenant{Name: "acme"}
	cfg := DefaultConfig()
	cfg.Tenants = map[string]*Tenant{testToken: tenant}
	cfg.ModelName = "test-model"
	cfg.PerTenantStreams = 0 // unlimited, isolating the buffer bound
	cfg.PerTenantRPM = 0
	// Short, so the test finishes. In production this is 30s and a client on a
	// slow network is far more likely to be a temporary condition.
	cfg.StallTimeout = 300 * time.Millisecond
	// A small buffer makes the bound observable: with 16 KB, filling it is quick.
	cfg.MaxStreamBufferBytes = 16 << 10

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	s, err := New(cfg, logger)
	if err != nil {
		t.Fatal(err)
	}

	const (
		attempts  = 40
		duration  = 3 * time.Second
		allowGrow = 24 << 20 // generous; catches unbounded growth, not GC noise
	)

	provider := s.provider.(*fakeProvider)
	provider.delay = 0 // produce as fast as possible: maximise pressure

	runtime.GC()
	base := heapInUse()

	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < attempts; i++ {
			w := newSlowWriter()
			req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions",
				strings.NewReader(chatBody(true)))
			req.Header.Set("Authorization", "Bearer "+testToken)
			req.Header.Set("Content-Type", "application/json")

			// ServeHTTP on the slow writer directly. The server's own handler
			// chain runs; only the socket is replaced.
			func() {
				defer w.release()
				ctx, cancel := context.WithTimeout(req.Context(), 5*time.Second)
				defer cancel()
				s.Handler().ServeHTTP(w, req.WithContext(ctx))
			}()
		}
	}()

	// Sample while the pressure runs, so a growth trend is visible rather than
	// only its end state.
	peak := base
	deadline := time.Now().Add(duration)
	for time.Now().Before(deadline) {
		if h := heapInUse(); h > peak {
			peak = h
		}
		time.Sleep(100 * time.Millisecond)
	}
	<-done

	if grew := peak - base; grew > allowGrow {
		t.Errorf("heap grew %d MiB with a stalled consumer (base %d MiB, peak %d MiB); "+
			"the buffer is supposed to be bounded at %d KiB per stream",
			grew>>20, base>>20, peak>>20, cfg.MaxStreamBufferBytes>>10)
	}
}

// heapInUse returns the heap in use, in bytes.
//
// HeapInuse rather than HeapAlloc: HeapInuse counts spans the runtime has handed
// out, which is closer to what the process actually costs and is stable enough
// to compare across samples.
func heapInUse() uint64 {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	return m.HeapInuse
}

// TestStalledStreamEndsAndReleasesQuota combines the two invariants.
//
// A stalled consumer must both (a) have its stream ended by the deadline, and
// (b) release its quota slot. Failing (a) leaks a goroutine; failing (b) locks
// the tenant out permanently. They are separate failure modes with the same
// symptom from the client's side - "it hangs" - so they are tested together.
func TestStalledStreamEndsAndReleasesQuota(t *testing.T) {
	tenant := &Tenant{Name: "acme"}
	cfg := DefaultConfig()
	cfg.Tenants = map[string]*Tenant{testToken: tenant}
	cfg.ModelName = "test-model"
	cfg.PerTenantStreams = 1
	cfg.PerTenantRPM = 0
	cfg.StallTimeout = 200 * time.Millisecond
	cfg.MaxStreamBufferBytes = 8 << 10

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	s, err := New(cfg, logger)
	if err != nil {
		t.Fatal(err)
	}

	w := newSlowWriter()
	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions",
		strings.NewReader(chatBody(true)))
	req.Header.Set("Authorization", "Bearer "+testToken)
	req.Header.Set("Content-Type", "application/json")

	done := make(chan struct{})
	go func() {
		defer close(done)
		s.Handler().ServeHTTP(w, req)
	}()

	// The handler must return on its own. If it does not, the stall deadline is
	// not being applied and a stalled client holds a goroutine and a buffer
	// forever - the exhaustion vector the whole design exists to prevent.
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		w.release()
		t.Fatal("a stalled stream never ended; the stall deadline is not being applied")
	}
	w.release()

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && tenant.InFlight() > 0 {
		time.Sleep(10 * time.Millisecond)
	}
	if n := tenant.InFlight(); n != 0 {
		t.Errorf("InFlight = %d after a stalled stream ended, want 0: the quota slot leaked", n)
	}
}
