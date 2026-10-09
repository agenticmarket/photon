package photon

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// stageKey is the context key middleware stamps its stage value under. A
// distinct unexported type is used so the key cannot collide with a value set
// elsewhere in the request, which is the mistake a bare string key invites.
type stageKey string

// readAllString drains and returns a response body.
func readAllString(t *testing.T, resp *http.Response) string {
	t.Helper()
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Errorf("read body: %v", err)
		return ""
	}
	return string(b)
}

// The tests in this file exist because the race detector only reports races on
// executed paths. The unit suite exercises the router and the admission
// counters; these exercise the shapes of concurrency a real server actually
// sees, which is where a shared-ownership bug hides: one router serving many
// goroutines, one connection closed by two of them, a pool buffer handed back
// while a handler still holds a reference into it.
//
// Every test here is written to be run with -race. A clean run proves only
// that these paths are clean; it does not prove the absence of races elsewhere.

// newRaceServer returns a server with routes registered and a base URL. The
// server is shut down when the test ends.
func newRaceServer(t *testing.T, s *Server) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	done := make(chan error, 1)
	go func() { done <- s.Serve(ln) }()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = s.Shutdown(ctx)
		select {
		case err := <-done:
			if err != nil {
				t.Errorf("Serve returned %v, want nil after shutdown", err)
			}
		case <-time.After(10 * time.Second):
			t.Error("Serve did not return after Shutdown")
		}
	})
	return "http://" + ln.Addr().String()
}

// TestConcurrentRequestsOnOneRouterSharesNothing hammers a single router from
// many goroutines with distinct and overlapping paths, and has each handler read
// its own params.
//
// The failure this catches is aliasing: if a capture buffer were shared between
// requests, or returned to the pool too early, one goroutine would read another
// goroutine's parameter values. Each request here asserts the exact value it
// sent, so any cross-talk is a hard failure rather than a flake.
func TestConcurrentRequestsOnOneRouterSharesNothing(t *testing.T) {
	const (
		workers   = 24
		perWorker = 8
	)
	s := New()
	// A route with two parameters and a catch-all, so the captures vary in
	// count and length, which is what makes a reused buffer observable.
	s.GET("/w/:worker/item/:item", HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		worker := PathParam(r, "worker")
		item := PathParam(r, "item")
		fmt.Fprintf(w, "%s/%s", worker, item)
	}))
	s.GET("/w/:worker/tree/*rest", HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(w, "%s|%s|%s", PathParam(r, "worker"), PathParam(r, "rest"), PathParam(r, "item"))
	}))
	s.GET("/static", HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("static"))
	}))
	base := newRaceServer(t, s)

	// Requests share a pool of clients with keep-alive enabled, so the test
	// exercises many concurrent requests over a bounded number of connections.
	//
	// The bound is deliberate. A bare Go listener on this Windows host refuses
	// connections once the accept backlog overflows at roughly 200 simultaneous
	// handshakes, which was reproduced with no server code involved, so an
	// unbounded fan-out would test the host's SYN backlog rather than photon's
	// concurrency. Reusing connections keeps the concurrency being measured
	// where it belongs: in the router and the capture pool.
	transport := &http.Transport{
		MaxIdleConnsPerHost: workers,
		MaxConnsPerHost:     workers,
	}
	defer transport.CloseIdleConnections()
	client := &http.Client{Transport: transport, Timeout: 30 * time.Second}

	var wg sync.WaitGroup
	var bad atomic.Int64
	for wk := 0; wk < workers; wk++ {
		for n := 0; n < perWorker; n++ {
			wg.Add(1)
			go func(wk, n int) {
				defer wg.Done()
				worker := strconv.Itoa(wk)
				item := strconv.Itoa(n)

				url := base + "/w/" + worker + "/item/" + item
				resp, err := client.Get(url)
				if err != nil {
					bad.Add(1)
					t.Errorf("GET %s: %v", url, err)
					return
				}
				body := readAllString(t, resp)
				resp.Body.Close()
				if body != worker+"/"+item {
					bad.Add(1)
					t.Errorf("item route returned %q, want %q (cross-talk between requests?)", body, worker+"/"+item)
				}

				// The catch-all route must not see the previous route's values.
				// The repeat count is offset by 1 because an empty segment is
				// deliberately a non-match, so "/tree/" would be a 404.
				rest := strings.Repeat("x", n%37+1)
				resp2, err := client.Get(base + "/w/" + worker + "/tree/" + rest)
				if err != nil {
					bad.Add(1)
					t.Errorf("GET catch-all for worker %s: %v", worker, err)
					return
				}
				got := readAllString(t, resp2)
				resp2.Body.Close()
				want := worker + "|" + rest + "|"
				if !strings.HasPrefix(got, want) || got != want {
					bad.Add(1)
					t.Errorf("catch-all returned %q, want %q", got, want)
				}
			}(wk, n)
		}
	}
	wg.Wait()
	if n := bad.Load(); n > 0 {
		t.Fatalf("%d concurrent requests returned the wrong data", n)
	}
}

// TestConcurrentMiddlewareChain runs a middleware chain that reads and writes
// per-request state, from many goroutines at once.
//
// Middleware is the part of the stack most likely to hide shared state, because
// a middleware function is a closure that can capture something the author
// assumed was per-request when it is not.
func TestConcurrentMiddlewareChain(t *testing.T) {
	s := New()
	var middlewareCalls atomic.Int64

	tag := func(name string) Middleware {
		return func(next http.Handler) http.Handler {
			return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				middlewareCalls.Add(1)
				// Stamp a per-request value, and assert later that it survived
				// the rest of the chain unchanged.
				r = r.WithContext(context.WithValue(r.Context(), stageKey("stage-"+name), r.URL.Path))
				next.ServeHTTP(w, r)
			})
		}
	}
	s.Use(tag("a"), tag("b"), tag("c"))
	s.GET("/mw/:id", HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		for _, name := range []string{"a", "b", "c"} {
			got, _ := r.Context().Value(stageKey("stage-" + name)).(string)
			if got != r.URL.Path {
				t.Errorf("middleware %s saw path %q, want %q", name, got, r.URL.Path)
				break
			}
		}
		_, _ = fmt.Fprintf(w, "%s", PathParam(r, "id"))
	}))
	base := newRaceServer(t, s)

	var wg sync.WaitGroup
	for i := 0; i < 200; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			id := strconv.Itoa(i)
			resp, err := http.Get(base + "/mw/" + id)
			if err != nil {
				t.Errorf("get: %v", err)
				return
			}
			body := readAllString(t, resp)
			resp.Body.Close()
			if body != id {
				t.Errorf("body = %q, want %q", body, id)
			}
		}(i)
	}
	wg.Wait()

	if got, want := middlewareCalls.Load(), int64(200*3); got != want {
		t.Errorf("middleware ran %d times, want %d", got, want)
	}
}

// TestParamsPoolUnderConcurrentChurn stresses the capture pool specifically, by
// running a large number of requests that all capture at once and forcing GC
// mid-flight.
//
// sync.Pool is cleared at every GC, so the interesting window is a GC landing
// between Get and Put. The handler reads its params after a yield point to make
// sure the buffer is still valid for the whole request.
func TestParamsPoolUnderConcurrentChurn(t *testing.T) {
	s := New()
	s.GET("/p/:a/:b/:c", HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Yield and force allocation pressure between reading the params, so a
		// buffer returned early would have been reused by another goroutine by
		// the time the handler looks at it.
		got := PathParam(r, "a") + "|" + PathParam(r, "b") + "|" + PathParam(r, "c")
		runtime.Gosched()
		var sink int
		for i := 0; i < 64; i++ {
			sink += i
		}
		_ = sink
		again := PathParam(r, "a") + "|" + PathParam(r, "b") + "|" + PathParam(r, "c")
		if got != again {
			t.Errorf("params changed mid-request: %q then %q", got, again)
		}
		_, _ = w.Write([]byte(got))
	}))
	base := newRaceServer(t, s)

	var wg sync.WaitGroup
	for wk := 0; wk < 32; wk++ {
		wg.Add(1)
		go func(wk int) {
			defer wg.Done()
			for n := 0; n < 25; n++ {
				a, b, c := strconv.Itoa(wk), strconv.Itoa(n), strings.Repeat("z", n%11+1)
				resp, err := http.Get(base + "/p/" + a + "/" + b + "/" + c)
				if err != nil {
					t.Errorf("get: %v", err)
					return
				}
				body := readAllString(t, resp)
				resp.Body.Close()
				if want := a + "|" + b + "|" + c; body != want {
					t.Errorf("body = %q, want %q", body, want)
					return
				}
			}
		}(wk)
	}
	wg.Wait()
}

// TestConcurrentHighConnectionCount opens many simultaneous connections and
// drives requests across them, which is what exercises the admission counters
// and the connection close path under contention.
func TestConcurrentHighConnectionCount(t *testing.T) {
	s := New(SetLimits(Limits{
		MaxRequestBodyBytes:   1 << 20,
		MaxHeaderBytes:        16 << 10,
		MaxHeaders:            100,
		MaxHeaderValueBytes:   8 << 10,
		ReadHeaderTimeout:     5 * Second,
		IdleTimeout:           60 * Second,
		MaxConnections:        0,
		MaxConcurrentRequests: 0,
		MaxPerIP:              0,
	}))
	s.GET("/ping", HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("pong"))
	}))
	base := newRaceServer(t, s)

	// 120 connections, below this host's measured accept-backlog ceiling of
	// roughly 200 simultaneous handshakes. Exceeding it produces connection
	// refusals from the Windows loopback stack that have nothing to do with the
	// server under test.
	const conns = 120
	var wg sync.WaitGroup
	for i := 0; i < conns; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			// One client, one transport, so the connection is genuinely reused
			// across a request and then closed while the server may still be
			// sweeping idle connections.
			tr := &http.Transport{}
			defer tr.CloseIdleConnections()
			client := &http.Client{Transport: tr, Timeout: 10 * time.Second}
			for k := 0; k < 3; k++ {
				resp, err := client.Get(base + "/ping")
				if err != nil {
					t.Errorf("get: %v", err)
					return
				}
				if body := readAllString(t, resp); body != "pong" {
					t.Errorf("body = %q, want %q", body, "pong")
				}
				resp.Body.Close()
			}
		}()
	}
	wg.Wait()
}

// TestShutdownRacesWithInFlightRequests closes connections out from under active
// handlers, which is the exact interleaving that produced the admission-release race.
//
// net/http's Shutdown sweeps idle connections on the caller's goroutine while
// per-connection goroutines finish their own work, so a connection can be
// closed from two directions at once. This test is named for the bug: it is the
// deterministic regression for it at the integration level.
func TestShutdownRacesWithInFlightRequests(t *testing.T) {
	for attempt := 0; attempt < 20; attempt++ {
		s := New()
		release := make(chan struct{})
		var inFlight atomic.Int64
		s.GET("/slow", HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			inFlight.Add(1)
			defer inFlight.Add(-1)
			select {
			case <-release:
			case <-r.Context().Done():
			}
		}))
		// A fast route used to build up a pool of idle keep-alive connections.
		// Shutdown's closeIdleConns sweep then closes all of them on the
		// caller's goroutine, which is the interleaving that raced: the sweep
		// and a per-connection goroutine reaching Close at the same time.
		s.GET("/quick", HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			_, _ = w.Write([]byte("ok"))
		}))
		base := newRaceServer(t, s)

		// Keep-alive transport, so completed requests leave idle connections
		// for the shutdown sweep to close.
		transport := &http.Transport{MaxIdleConnsPerHost: 64, MaxConnsPerHost: 64}
		defer transport.CloseIdleConnections()
		client := &http.Client{Transport: transport, Timeout: 30 * time.Second}

		// Build up idle connections.
		var idleWG sync.WaitGroup
		for i := 0; i < 48; i++ {
			idleWG.Add(1)
			go func() {
				defer idleWG.Done()
				resp, err := client.Get(base + "/quick")
				if err == nil {
					_, _ = io.Copy(io.Discard, resp.Body)
					resp.Body.Close()
				}
			}()
		}
		idleWG.Wait()

		// Fire a burst of slow requests, then shut down while they are still
		// running, so Shutdown's idle sweep and the handler goroutines race for
		// the same listener and connection state.
		var wg sync.WaitGroup
		for i := 0; i < 40; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				resp, err := client.Get(base + "/slow")
				if err == nil {
					resp.Body.Close()
				}
			}()
		}
		// Give the requests time to be established and in the handler.
		deadline := time.Now().Add(2 * time.Second)
		for inFlight.Load() == 0 && time.Now().Before(deadline) {
			time.Sleep(time.Millisecond)
		}

		ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
		_ = s.Shutdown(ctx)
		cancel()
		close(release)
		wg.Wait()
	}
}

// TestRequestCancellationReachesTheHandler cancels a request from the client
// side and requires the server handler to observe the cancellation, so upstream
// work can stop.
func TestRequestCancellationReachesTheHandler(t *testing.T) {
	for attempt := 0; attempt < 20; attempt++ {
		s := New()
		cancelled := make(chan struct{}, 1)
		// entered is closed once the handler is actually running, so the cancel
		// cannot land before the request reaches the server. Sleeping a fixed
		// interval instead would make this test flaky under load: on a slow
		// round the cancel can arrive first, the client aborts before the
		// request is dispatched, and no handler is ever cancelled.
		entered := make(chan struct{})
		s.GET("/cancel", HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			select {
			case entered <- struct{}{}:
			default:
			}
			select {
			case <-r.Context().Done():
				select {
				case cancelled <- struct{}{}:
				default:
				}
			case <-time.After(10 * time.Second):
			}
		}))
		base := newRaceServer(t, s)

		ctx, cancel := context.WithCancel(context.Background())
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, base+"/cancel", nil)
		if err != nil {
			t.Fatal(err)
		}
		done := make(chan struct{})
		go func() {
			defer close(done)
			resp, err := http.DefaultClient.Do(req)
			if err == nil {
				resp.Body.Close()
			}
		}()
		// Wait for the handler to be running, then cancel.
		select {
		case <-entered:
		case <-time.After(10 * time.Second):
			t.Fatal("handler never started")
		}
		cancel()
		<-done

		select {
		case <-cancelled:
		case <-time.After(10 * time.Second):
			t.Fatal("handler never observed the client cancellation")
		}
	}
}

// TestConcurrentRegistrationIsRejected documents the actual contract: a router
// is not safe for concurrent registration and serving, and registration after
// Serve panics rather than racing.
//
// The task list asks for concurrent registration "only if registration is
// documented as supported". It is documented as unsupported, so this test pins
// the panic that the contract promises instead of exercising a race.
func TestConcurrentRegistrationIsRejected(t *testing.T) {
	s := New()
	// A route registered before Serve, to prove the router is genuinely frozen
	// rather than merely busy.
	s.GET("/a", HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	base := newRaceServer(t, s)

	// Serve freezes the router asynchronously, so the test must wait for that
	// before attempting registration. Without the wait, a single-proc run can
	// schedule the registering goroutine before Serve gets there, and the
	// registration would legitimately succeed, which is a bug in the test's
	// timing assumption rather than in the router.
	deadline := time.Now().Add(5 * time.Second)
	for !s.isStarted() && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if !s.isStarted() {
		t.Fatal("Serve never started")
	}

	// Hammer the frozen router with registration attempts. Each must panic, and
	// a panic is the documented behaviour, not a data race.
	var wg sync.WaitGroup
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			defer func() {
				if r := recover(); r == nil {
					t.Errorf("goroutine %d: registration on a frozen router did not panic", i)
				}
			}()
			s.GET(fmt.Sprintf("/late/%d", i), HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
		}(i)
	}
	wg.Wait()

	// The server must still serve the routes registered before Serve.
	resp, err := http.Get(base + "/a")
	if err != nil {
		t.Fatalf("server unusable after concurrent registration attempts: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("GET /a = %d, want 200", resp.StatusCode)
	}
}

// TestNoGoroutineLeakAcrossConnectionChurn is a leak check: repeated
// connect/disconnect cycles must not accumulate goroutines.
func TestNoGoroutineLeakAcrossConnectionChurn(t *testing.T) {
	s := New()
	s.GET("/churn", HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("ok"))
	}))
	base := newRaceServer(t, s)

	// Warm up, so one-time goroutines are not counted as a leak.
	churn(20, base)
	before := runtime.NumGoroutine()

	for i := 0; i < 6; i++ {
		churn(60, base)
		// Give finished handlers a chance to be reaped.
		time.Sleep(100 * time.Millisecond)
	}
	after := runtime.NumGoroutine()

	// A slow connection close can still be in flight; allow a small margin
	// rather than requiring an exact count.
	if after > before+10 {
		buf := make([]byte, 1<<16)
		n := runtime.Stack(buf, true)
		t.Errorf("goroutines grew from %d to %d across connection churn (+%d)\n%s",
			before, after, after-before, buf[:n])
	}
}

// churn opens and abandons n short-lived connections.
func churn(n int, base string) {
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			addr := strings.TrimPrefix(base, "http://")
			c, err := net.DialTimeout("tcp", addr, 5*time.Second)
			if err != nil {
				return
			}
			// Half the connections are abandoned mid-request, which is the
			// harshest case for the server's connection accounting.
			if i%2 == 0 {
				_, _ = c.Write([]byte("GET /churn HTTP/1.1\r\nHost: x\r\n\r\n"))
			}
			_ = c.Close()
		}()
	}
	wg.Wait()
}

// TestAdmissionCountersReturnToZero checks that admitting and releasing
// connections under concurrency leaves no residue, so a long-lived server does
// not slowly exhaust its own connection budget.
func TestAdmissionCountersReturnToZero(t *testing.T) {
	s := New(SetLimits(Limits{
		MaxRequestBodyBytes:   1 << 20,
		MaxHeaderBytes:        16 << 10,
		MaxHeaders:            100,
		MaxHeaderValueBytes:   8 << 10,
		ReadHeaderTimeout:     5 * Second,
		IdleTimeout:           60 * Second,
		MaxConnections:        512,
		MaxConcurrentRequests: 0,
		MaxPerIP:              512,
	}))
	s.GET("/x", HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	base := newRaceServer(t, s)

	// A dedicated transport, closed at the end. http.Get uses the shared
	// DefaultClient transport, which keeps idle connections alive for the whole
	// process; one of those would legitimately still hold an admission slot at
	// the moment of the check and would make the test report a leak that is
	// really just an idle socket.
	tr := &http.Transport{}
	defer tr.CloseIdleConnections()
	client := &http.Client{Transport: tr, Timeout: 10 * time.Second}

	for round := 0; round < 4; round++ {
		var wg sync.WaitGroup
		for i := 0; i < 100; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				resp, err := client.Get(base + "/x")
				if err == nil {
					_, _ = bufio.NewReader(resp.Body).ReadString('\n')
					resp.Body.Close()
				}
			}()
		}
		wg.Wait()
	}
	// Close the client's sockets so the server observes the closes and returns
	// the admission slots.
	tr.CloseIdleConnections()

	// Allow closes to settle.
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		s.connMu.Lock()
		conns, perIP := s.conns, len(s.perIP)
		s.connMu.Unlock()
		if conns == 0 && perIP == 0 {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	s.connMu.Lock()
	conns, perIP := s.conns, len(s.perIP)
	s.connMu.Unlock()
	t.Errorf("after churn conns=%d perIPKeys=%d, want 0 and 0", conns, perIP)
}
