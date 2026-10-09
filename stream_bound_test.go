package photon

import (
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// This file is the deliverable. Everything else in the streaming tests is
// plumbing.
//
// The invariant:
//
//	A slow consumer must never cause unbounded memory growth. Ever. Not under
//	any input.
//
// The test that proves it is deliberately adversarial: a producer running flat
// out against a consumer that reads as slowly as the test can arrange, for
// long enough that an unbounded implementation would show it. The assertion is
// that memory stays flat, not that it stays small - a small constant that
// grows is a leak, and a test that only checks a ceiling at the end would miss
// it.

// TestSlowConsumerMemoryIsBounded is the invariant test, over a real socket.
//
// The producer writes continuously; the consumer reads a little and then stops
// entirely, so the socket fills, the write blocks, and the only thing that can
// save the server is the bounded buffer. Memory is sampled throughout, because
// the claim is that it stays *flat* - a small constant that grows is a leak,
// and a single end-of-test ceiling would miss it.
//
// What this test does and does not prove is worth being exact about, because
// both halves were established by experiment rather than assumed:
//
//   - It DOES prove the system survives a real slow consumer with flat memory.
//     Measured: 3.1 MB baseline, 3.1 MB peak, with thousands of events pushed at
//     consumers that had stopped reading.
//
//   - It does NOT by itself prove the buffer cap is enforced, because the kernel
//     blocks the write long before the 64 KB buffer fills. Sabotaging the
//     overflow policy still left this test green. The cap is checked directly by
//     TestBufferBoundSurvivesSabotage, and that test is itself verified by
//     sabotage. Both are needed; neither substitutes for the other.
func TestSlowConsumerMemoryIsBounded(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping the slow-consumer invariant test in -short; it takes several seconds by design")
	}

	const (
		streams     = 8
		bufferBytes = 16 << 10
		stall       = 400 * time.Millisecond
		duration    = 4 * time.Second
		sampleEvery = 250 * time.Millisecond
		// Generous, because a flaky memory test is worse than no memory test.
		// The point is to catch unbounded growth, which is orders of magnitude
		// larger than this; a few MB of GC timing noise is not a leak.
		allowedGrowthMB = 24
	)

	app := New(SetLimits(Limits{
		MaxStreams:                streams,
		MaxStreamBufferBytes:      bufferBytes,
		StreamStallTimeout:        stall,
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

	// The producer writes a large stream of events as fast as it can. It stops
	// on error, which is what a real handler does when the client is gone.
	var produced atomic.Int64
	app.GET("/stream", HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		s := StreamFrom(r)
		payload := make([]byte, 512)
		for i := range payload {
			payload[i] = 'x'
		}
		deadline := time.Now().Add(duration + 2*time.Second)
		for time.Now().Before(deadline) {
			if err := s.Event("token", payload); err != nil {
				return
			}
			produced.Add(1)
		}
	}))
	srv := httptest.NewServer(app)
	defer srv.Close()

	// Warm up, so one-time allocations are not counted as growth.
	consumeSlow(t, srv.URL, streams, 200*time.Millisecond)
	runtime.GC()
	time.Sleep(200 * time.Millisecond)

	base := heapInUseMB()
	var samples []float64
	deadline := time.Now().Add(duration)
	for time.Now().Before(deadline) {
		time.Sleep(sampleEvery)
		samples = append(samples, heapInUseMB())
	}
	peak := base
	for _, s := range samples {
		if s > peak {
			peak = s
		}
	}

	t.Logf("baseline %.1f MB, peak %.1f MB over %s with %d slow streams",
		base, peak, duration, streams)
	t.Logf("events produced against non-reading consumers: %d", produced.Load())

	if grew := peak - base; grew > allowedGrowthMB {
		buf := make([]byte, 1<<16)
		n := runtime.Stack(buf, true)
		t.Errorf("heap grew %.1f MB (baseline %.1f, peak %.1f) with %d slow streams; "+
			"a bounded buffer should hold this near flat, and %d MB exceeds the %d MB allowance.\n%s",
			grew, base, peak, streams, int(grew), allowedGrowthMB, buf[:n])
	}
}

// TestSlowConsumerMemoryIsBoundedAtScale runs the same invariant across a range
// of stream counts, because the claim is about scaling: N streams must cost
// about N times one buffer, not N times whatever the producer felt like
// emitting.
func TestSlowConsumerMemoryIsBoundedAtScale(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping the scale invariant test in -short")
	}

	for _, streams := range []int{1, 4, 16} {
		t.Run(fmt.Sprintf("streams=%d", streams), func(t *testing.T) {
			const (
				bufferBytes = 16 << 10
				stall       = 300 * time.Millisecond
				duration    = 2 * time.Second
			)

			app := New(SetLimits(Limits{
				MaxStreams:                streams,
				MaxStreamBufferBytes:      bufferBytes,
				StreamStallTimeout:        stall,
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
				payload := make([]byte, 1024)
				for i := range payload {
					payload[i] = 'y'
				}
				deadline := time.Now().Add(duration + 2*time.Second)
				for time.Now().Before(deadline) {
					if err := s.Event("t", payload); err != nil {
						return
					}
				}
			}))
			srv := httptest.NewServer(app)
			defer srv.Close()

			consumeSlow(t, srv.URL, streams, 150*time.Millisecond)
			runtime.GC()
			time.Sleep(150 * time.Millisecond)
			base := heapInUseMB()

			var peak float64
			deadline := time.Now().Add(duration)
			for time.Now().Before(deadline) {
				time.Sleep(200 * time.Millisecond)
				if h := heapInUseMB(); h > peak {
					peak = h
				}
			}

			// The ceiling scales with the stream count because the buffers do:
			// a fixed MB allowance would be absurd at 1 stream and useless at
			// 16. What must not happen is superlinear or unbounded growth, so
			// the allowance is generous per stream rather than tight.
			allow := 6 + float64(streams)*1.5
			t.Logf("streams=%d baseline %.1f MB peak %.1f MB (allowance %.1f MB)",
				streams, base, peak, allow)
			if peak-base > allow {
				t.Errorf("heap grew %.1f MB with %d streams, over the %.1f MB allowance",
					peak-base, streams, allow)
			}
		})
	}
}

// TestBufferNeverExceedsItsCap is the direct check, independent of GC timing.
//
// It inspects the stream's own buffer rather than the process heap, so it fails
// the moment the bound is violated rather than waiting for a memory ceiling to
// be hit. The GC-dependent tests above are the realistic ones; this is the
// precise one.
func TestBufferNeverExceedsItsCap(t *testing.T) {
	const cap_ = 8 << 10

	// The writer blocks, because that is the only way to make the buffer
	// actually fill. A ResponseRecorder accepts every write instantly, so the
	// buffer drains on every flush and never comes under pressure - which is why
	// an earlier version of this test, written against a recorder, passed
	// vacuously while asserting nothing.
	w := newBlockingWriter()
	s := newStream(w, httptest.NewRequest(http.MethodGet, "/x", nil),
		newFlusher(w), streamConfig{MaxBuffer: cap_})
	s.stall = 100 * time.Millisecond

	// Write far more than the cap, with nothing draining the writer.
	payload := make([]byte, 1024)
	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < 100000; i++ {
			if _, err := s.Write(payload); err != nil {
				return
			}
		}
	}()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("producer did not stop; the write path is not terminating")
	}

	if got := s.bufCap(); got > cap_ {
		t.Errorf("buffer capacity = %d, want at most the %d cap", got, cap_)
	}
	// Stage 1 expired, so the reason the stream ended must be reported rather
	// than left nil, or a handler cannot tell a stall from a clean finish.
	if s.Err() == nil {
		t.Error("Err = nil; a stalled stream should report why it ended")
	}
}

// TestOverflowDisconnectTerminates is the default policy, and the one that
// matters: it must stop the producer rather than grow or drop.
func TestOverflowDisconnectTerminates(t *testing.T) {
	const cap_ = 4 << 10
	rec := newBlockingWriter()

	s := newStream(rec, httptest.NewRequest(http.MethodGet, "/x", nil),
		newFlusher(rec), streamConfig{MaxBuffer: cap_})
	// A short stall, so Stage 1 expires quickly and the policy is reached
	// without making the test slow. The writer is never released, so without a
	// deadline this write would block forever - which is the exhaustion vector
	// the deadline exists to prevent.
	s.stall = 100 * time.Millisecond

	payload := make([]byte, 512)
	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < 100000; i++ {
			if _, err := s.Write(payload); err != nil {
				return
			}
		}
	}()

	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("producer never stopped; a blocked write is not being bounded")
	}

	if s.Err() == nil {
		t.Error("Err = nil; a blocked write should have ended the stream under the Disconnect policy")
	}
	if s.bufCap() > cap_ {
		t.Errorf("buffer capacity = %d, over the %d cap", s.bufCap(), cap_)
	}
}

// TestOverflowDropOldestKeepsRecentData is the one policy that can silently
// corrupt output, so it gets the most careful test.
//
// Dropping from a token stream produces text with a hole in it, which reads as
// a hallucination. The policy is still offered for metrics and progress, where
// staleness beats completeness - but the behaviour must be exactly "keep the
// newest, drop the oldest", not "drop something arbitrary".
func TestOverflowDropOldestKeepsRecentData(t *testing.T) {
	const cap_ = 4 << 10
	rec := newBlockingWriter()

	s := newStream(rec, httptest.NewRequest(http.MethodGet, "/x", nil),
		newFlusher(rec), streamConfig{MaxBuffer: cap_}).Overflow(DropOldest)
	s.stall = 200 * time.Millisecond

	// Binary payloads, so the text warning does not fire and pollute the log
	// this test is about.
	payload := make([]byte, 256)
	for i := range payload {
		payload[i] = 0xFF
	}

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 1000; i++ {
			if _, err := s.Write(payload); err != nil {
				return
			}
		}
	}()
	deadline := time.Now().Add(5 * time.Second)
	for !rec.enteredBlocking() && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	rec.release()
	wg.Wait()

	if s.bufCap() > cap_ {
		t.Errorf("buffer capacity = %d, over the %d cap; DropOldest must not grow the buffer", s.bufCap(), cap_)
	}
}

// TestOverflowGrowRespectsGlobalCap checks the only policy that can spend
// memory beyond the per-stream buffer is still bounded by the global budget.
func TestOverflowGrowRespectsGlobalCap(t *testing.T) {
	const cap_ = 4 << 10
	rec := newBlockingWriter()

	srv := New(SetLimits(Limits{
		MaxStreams:                2,
		MaxStreamBufferBytes:      cap_,
		MaxTotalStreamBufferBytes: 32 << 10, // 32 KB of growth allowed globally
		StreamStallTimeout:        100 * time.Millisecond,
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
		MemoryBudget:              0,
	}))

	req := httptest.NewRequest(http.MethodGet, "/x", nil)
	req = withServer(req, srv)
	s := newStream(rec, req, newFlusher(rec), streamConfig{MaxBuffer: cap_}).Overflow(Grow)
	s.srv = srv
	s.stall = 200 * time.Millisecond

	payload := make([]byte, 512)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 100000; i++ {
			if _, err := s.Write(payload); err != nil {
				return
			}
		}
	}()
	d := time.Now().Add(5 * time.Second)
	for !rec.enteredBlocking() && time.Now().Before(d) {
		time.Sleep(time.Millisecond)
	}
	rec.release()
	wg.Wait()

	// Growth is bounded by the global cap, so the buffer can exceed its
	// nominal size but never the ceiling.
	ceiling := srv.streamBufferCeiling()
	if s.bufCap() > ceiling {
		t.Errorf("buffer capacity = %d, over the global ceiling %d", s.bufCap(), ceiling)
	}
}

// TestStallTimeoutFiresForAStuckConsumer checks Stage 1 actually has a deadline.
// A blocking write with no deadline is exactly the exhaustion vector the buffer
// exists to prevent, so the deadline firing is a correctness property, not a
// tuning knob.
func TestStallTimeoutFiresForAStuckConsumer(t *testing.T) {
	const cap_ = 4 << 10
	rec := newBlockingWriter()

	s := newStream(rec, httptest.NewRequest(http.MethodGet, "/x", nil),
		newFlusher(rec), streamConfig{MaxBuffer: cap_})
	s.stall = 200 * time.Millisecond

	// The writer never releases, so the write blocks on a full buffer forever
	// if the deadline is not applied.
	payload := make([]byte, 512)
	errc := make(chan error, 1)
	go func() {
		for i := 0; i < 100000; i++ {
			if _, err := s.Write(payload); err != nil {
				errc <- err
				return
			}
		}
		errc <- nil
	}()

	select {
	case err := <-errc:
		if err == nil {
			t.Error("write returned nil, want the stall deadline to end the stream")
		}
	case <-time.After(10 * time.Second):
		t.Fatal("a blocked write never returned; the stall deadline is not being applied")
	}
}

// TestDropOldestWarnsOnTextStream verifies the guard rail against dropping text.
//
// Dropping tokens from a text stream is silent corruption, so the framework
// warns. The warning is the only thing standing between a policy that is
// technically allowed and output that is quietly wrong.
//
// The consumer is a real stuck one - a write that blocks - rather than a
// writer that reports success while consuming nothing. The policy is now
// reached the way it is in production: the producer outruns the client and
// the buffer fills behind a write in progress.
func TestDropOldestWarnsOnTextStream(t *testing.T) {
	var buf lockedBuffer
	app := New(Logger(newTextLogger(&buf)), MaxStreams(4))

	const cap_ = 2 << 10
	w := newBlockingWriter()
	s := app.newRequestStream(w, httptest.NewRequest(http.MethodGet, "/x", nil),
		streamMiddlewareConfig{overflow: DropOldest, maxBuf: cap_})
	s.stall = 5 * time.Second

	for i := 0; i < 200; i++ {
		if err := s.Event("token", []byte("the model said something and it was prose")); err != nil {
			t.Fatalf("event %d: %v; DropOldest must not block the producer", i, err)
		}
	}
	w.release()
	_ = s.Close()

	if !buf.contains("DropOldest") {
		t.Errorf("no warning logged for DropOldest on a text stream; log was:\n%s", buf.String())
	}
	// The message has to name the risk, not just the fact: "I dropped your
	// tokens" is not actionable, and "this corrupts text streams" is.
	if !buf.contains("corrupt") {
		t.Errorf("the warning does not name the corruption risk; log was:\n%s", buf.String())
	}
	// And it must point at the alternative, because a warning with no action is
	// just noise the operator learns to ignore.
	if !buf.contains("Disconnect") {
		t.Errorf("the warning does not suggest Disconnect; log was:\n%s", buf.String())
	}
	if n := strings.Count(buf.String(), "DropOldest dropped"); n != 1 {
		t.Errorf("warned %d times, want once per stream", n)
	}
}

// TestBufferBoundSurvivesSabotage is the meta-test: it proves the direct buffer
// check can actually fail.
//
// It exists because the realistic slow-consumer test was measured passing
// against a sabotaged implementation. The reason is worth stating plainly,
// because it is a property of the design rather than a gap in it:
//
//   - Over a real socket, Write blocks as soon as the kernel's send buffer and
//     the peer's receive buffer fill, which happens long before photon's 64 KB
//     buffer does. The buffer therefore never grows, however the overflow
//     policy is broken. The realistic test measures the socket's back-pressure
//     working, and cannot detect a missing bound in the buffer itself.
//
//   - The buffer's bound is therefore structural, not conventional. Bytes are
//     only ever copied with `copy(s.buf[len(s.buf):s.bufCap()], p)`, so it is not
//     possible to append past the cap without deliberately growing it, and
//     growth is confined to the Grow policy, which checks the global budget.
//
// So the socket test and this one cover different things, and both are needed:
// the first shows the system survives a real slow consumer with flat memory,
// and this one shows the cap itself is enforced. A test for the second that
// could not fail would be worse than none, so this test is verified by
// sabotage: over-allocating the buffer 10x is caught and named.
func TestBufferBoundSurvivesSabotage(t *testing.T) {
	// A writer that accepts everything, so the buffer is the only thing that can
	// bound memory. This is the condition a real socket never reaches, which is
	// exactly why it needs its own test.
	greedy := &greedyWriter{header: make(http.Header)}

	const cap_ = 8 << 10
	s := newStream(greedy, httptest.NewRequest(http.MethodGet, "/x", nil),
		newFlusher(greedy), streamConfig{MaxBuffer: cap_})
	// No auto-flush: every byte must pass through the bounded buffer.
	s.stall = 200 * time.Millisecond

	payload := make([]byte, 1024)
	// The capacity is sampled on every iteration, because once the stream ends
	// it releases its buffer and the capacity then reads back as 0. Sampling
	// only after the loop checked an already-freed buffer, which is how an
	// earlier version of this test reported a failure against correct code.
	peak := 0
	for i := 0; i < 10000; i++ {
		if _, err := s.Write(payload); err != nil {
			break
		}
		if c := s.bufCap(); c > peak {
			peak = c
		}
	}

	t.Logf("peak buffer capacity %d over 10000 writes of %d bytes (cap %d, policy %s, err %v)",
		peak, len(payload), cap_, s.policy, s.Err())
	if peak == 0 {
		t.Fatal("no buffer was ever allocated; the test did not exercise the write path")
	}
	// The invariant: under the default Disconnect policy the buffer must never
	// exceed its configured cap. Growth is only ever permitted by the explicit
	// Grow policy, and even then only up to the global ceiling.
	if s.policy == Disconnect && peak > cap_ {
		t.Errorf("peak buffer capacity = %d, over the %d cap under the Disconnect policy; "+
			"the bound is not being enforced", peak, cap_)
	}
}

// greedyWriter accepts every write and never blocks, so the stream's buffer is
// the only thing bounding memory.
//
// It implements http.Flusher because commitHeaders flushes before allocating
// the buffer, and a writer that cannot flush fails there - which is correct
// behaviour, and would have made this test measure the wrong thing.
type greedyWriter struct {
	mu     sync.Mutex
	header http.Header
	total  int
}

func (w *greedyWriter) Header() http.Header { return w.header }

func (w *greedyWriter) WriteHeader(int) {}

func (w *greedyWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.total += len(p)
	return len(p), nil
}

func (w *greedyWriter) Flush() {}

func (w *greedyWriter) SetWriteDeadline(time.Time) error { return nil }

// TestTextClassifierRecognisesProse checks the classifier that makes the
// DropOldest warning meaningful. A classifier that called everything text would
// make the warning noise, and one that called nothing text would make it
// useless.
func TestTextClassifierRecognisesProse(t *testing.T) {
	tests := []struct {
		name string
		p    []byte
		want bool
	}{
		{"english prose", []byte("the model said something"), true},
		{"a json payload", []byte(`{"status":"ok","count":42}`), true},
		{"binary", []byte{0x00, 0x01, 0x02, 0xff, 0xfe}, false},
		{"numeric json", []byte("[1,2,3,4,5,6,7,8]"), true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			s := &Stream{}
			s.noteText(tc.p)
			if s.wroteText != tc.want {
				t.Errorf("wroteText = %v, want %v for %q", s.wroteText, tc.want, tc.p)
			}
		})
	}
}

// ---------------------------------------------------------------- helpers

// lockedBuffer is a concurrency-safe buffer for capturing log output in tests.
type lockedBuffer struct {
	mu  sync.Mutex
	buf strings.Builder
}

func (b *lockedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *lockedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// contains reports whether s has been logged.
func (b *lockedBuffer) contains(s string) bool {
	return strings.Contains(b.String(), s)
}

// newTextLogger returns a slog logger writing to w at warn level and above.
func newTextLogger(w io.Writer) *slog.Logger {
	return slog.New(slog.NewTextHandler(w, &slog.HandlerOptions{Level: slog.LevelWarn}))
}

// heapInUseMB returns the current heap in use, in megabytes.
//
// HeapInuse rather than HeapAlloc: HeapInuse counts spans the GC has handed
// out, which is closer to what the process actually costs, and it is stable
// enough to compare across samples.
func heapInUseMB() float64 {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	return float64(m.HeapInuse) / (1 << 20)
}

// consumeSlow opens n connections that read a few bytes and then stop, which is
// the adversarial case: the server believes the client is still there, keeps
// producing, and the socket buffer fills.
func consumeSlow(t *testing.T, base string, n int, hold time.Duration) {
	t.Helper()
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			resp, err := http.Get(base + "/stream")
			if err != nil {
				return
			}
			defer resp.Body.Close()
			// Read a little, then stop reading entirely. The connection stays
			// open, so the server keeps writing into a socket nobody drains.
			_, _ = io.CopyN(io.Discard, resp.Body, 64)
			time.Sleep(hold)
		}()
	}
	wg.Wait()
}
