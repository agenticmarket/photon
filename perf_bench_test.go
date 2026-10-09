package photon

import (
	"bufio"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

// These benchmarks measure the server as a whole rather than the router alone,
// because the router is a small share of a request and a router benchmark can
// improve while the request the user actually sends gets slower.

// benchApp is a server with a realistic mix of static and parametrised routes.
func benchApp(b *testing.B) *Server {
	b.Helper()
	app := New(MemoryBudget(256 << 20))
	h := HandlerFunc(func(w http.ResponseWriter, r *http.Request) {})
	app.GET("/", h)
	app.GET("/healthz", h)
	app.GET("/v1/models", h)
	app.POST("/v1/chat/completions", h)
	app.GET("/users/:id", h)
	app.GET("/users/:id/posts/:post", h)
	app.GET("/static/*path", h)
	return app
}

type benchWriter struct{ h http.Header }

func (w *benchWriter) Header() http.Header         { return w.h }
func (w *benchWriter) Write(p []byte) (int, error) { return len(p), nil }
func (w *benchWriter) WriteHeader(int)             {}

// BenchmarkServerStatic is a full Server.ServeHTTP for a static route, on a
// fresh copy of the request each time - net/http hands every request a new
// *http.Request, so reusing one would hide per-request costs. The copy is
// part of what is measured, identically for every version compared.
func BenchmarkServerStatic(b *testing.B) {
	app := benchApp(b)
	app.rt.Freeze()
	r := servedRequest(app, httptest.NewRequest(http.MethodGet, "/healthz", nil))
	w := &benchWriter{h: http.Header{}}
	b.ReportAllocs()
	for b.Loop() {
		req := *r
		app.ServeHTTP(w, &req)
	}
}

// BenchmarkServerParam is a full Server.ServeHTTP for a two-parameter route.
func BenchmarkServerParam(b *testing.B) {
	app := benchApp(b)
	app.rt.Freeze()
	r := servedRequest(app, httptest.NewRequest(http.MethodGet, "/users/42/posts/7", nil))
	w := &benchWriter{h: http.Header{}}
	b.ReportAllocs()
	for b.Loop() {
		req := *r
		app.ServeHTTP(w, &req)
	}
}

// BenchmarkServerParallel runs the same dispatch from every P at once, which is
// what exposes shared-state contention: a lock or a shared counter that costs
// nothing on one core can dominate on sixteen.
func BenchmarkServerParallel(b *testing.B) {
	app := benchApp(b)
	app.rt.Freeze()
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		r := servedRequest(app, httptest.NewRequest(http.MethodGet, "/users/42", nil))
		w := &benchWriter{h: http.Header{}}
		for pb.Next() {
			req := *r
			app.ServeHTTP(w, &req)
		}
	})
}

// BenchmarkRouterParallelStatic isolates the router's own synchronisation.
func BenchmarkRouterParallelStatic(b *testing.B) {
	app := benchApp(b)
	app.rt.Freeze()
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		r := httptest.NewRequest(http.MethodGet, "/healthz", nil)
		w := &benchWriter{h: http.Header{}}
		for pb.Next() {
			app.rt.ServeHTTP(w, r)
		}
	})
}

// BenchmarkSSETokenLatency measures the time from Event to the bytes arriving
// at a real client over loopback, which is the number an AI user feels as
// "typing speed". It is the benchmark the original streaming implementation
// could not pass: a single small event sat in the buffer until close.
func BenchmarkSSETokenLatency(b *testing.B) {
	next := make(chan struct{})
	app := New(MemoryBudget(256 << 20))
	app.Use(Streaming())
	app.GET("/s", HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		s := StreamFrom(r)
		for range next {
			if err := s.Event("token", []byte("hello")); err != nil {
				return
			}
		}
		_ = s.Close()
	}))
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		b.Fatal(err)
	}
	go func() { _ = app.Serve(ln) }()
	defer app.Close()

	c, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		b.Fatal(err)
	}
	defer c.Close()
	_, _ = io.WriteString(c, "GET /s HTTP/1.1\r\nHost: x\r\n\r\n")
	br := bufio.NewReader(c)
	readEvent := func() bool {
		_ = c.SetReadDeadline(time.Now().Add(3 * time.Second))
		for {
			line, err := br.ReadString('\n')
			if err != nil {
				return false
			}
			if strings.HasPrefix(line, "data: hello") {
				return true
			}
		}
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		next <- struct{}{}
		if !readEvent() {
			b.Fatalf("event %d was not delivered within 3s", i)
		}
	}
	b.StopTimer()
	close(next)
}

// BenchmarkSSEBurst measures raw event throughput over a real socket: one
// stream, a producer writing small events as fast as it can, a client reading
// everything. It rewards batching, so it is the benchmark where a design that
// holds events back looks best - which is why it is reported beside the
// latency benchmark rather than instead of it.
func BenchmarkSSEBurst(b *testing.B) {
	payload := []byte(`{"token":"hello world","index":12345}`)
	app := New(MemoryBudget(256 << 20))
	app.Use(Streaming())
	n := 0
	app.GET("/s", HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		s := StreamFrom(r)
		for i := 0; i < n; i++ {
			if err := s.Event("token", payload); err != nil {
				return
			}
		}
		_ = s.Close()
	}))
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		b.Fatal(err)
	}
	go func() { _ = app.Serve(ln) }()
	defer app.Close()

	n = b.N
	b.SetBytes(int64(len(payload)))
	b.ResetTimer()
	resp, err := http.Get("http://" + ln.Addr().String() + "/s")
	if err != nil {
		b.Fatal(err)
	}
	got, _ := io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	b.StopTimer()
	if got == 0 {
		b.Fatal("nothing delivered")
	}
}

// BenchmarkSSEConcurrentStreams runs 100 concurrent streams of 50 events each
// over keep-alive connections, the shape of a busy AI gateway.
func BenchmarkSSEConcurrentStreams(b *testing.B) {
	const streams, events = 100, 50
	payload := []byte(`{"token":"hello world"}`)
	app := New(MemoryBudget(1 << 30))
	app.Use(Streaming())
	app.GET("/s", HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		s := StreamFrom(r)
		for i := 0; i < events; i++ {
			if err := s.Event("token", payload); err != nil {
				return
			}
		}
		_ = s.Close()
	}))
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		b.Fatal(err)
	}
	go func() { _ = app.Serve(ln) }()
	defer app.Close()
	url := "http://" + ln.Addr().String() + "/s"
	client := &http.Client{Transport: &http.Transport{MaxIdleConns: streams, MaxIdleConnsPerHost: streams}}
	defer client.CloseIdleConnections()

	b.ReportAllocs()
	b.ResetTimer()
	var wg sync.WaitGroup
	work := make(chan struct{})
	for i := 0; i < streams; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range work {
				resp, err := client.Get(url)
				if err != nil {
					b.Error(err)
					return
				}
				_, _ = io.Copy(io.Discard, resp.Body)
				resp.Body.Close()
			}
		}()
	}
	for i := 0; i < b.N; i++ {
		work <- struct{}{}
	}
	close(work)
	wg.Wait()
	b.StopTimer()
	b.ReportMetric(float64(b.N*events)/b.Elapsed().Seconds(), "events/s")
}

// BenchmarkRegister5000Routes measures startup: registering 5,000 static
// routes and 50 parametrised ones, the size of a large generated API.
func BenchmarkRegister5000Routes(b *testing.B) {
	h := HandlerFunc(func(w http.ResponseWriter, r *http.Request) {})
	for i := 0; i < b.N; i++ {
		app := New(MemoryBudget(256 << 20))
		for j := 0; j < 5000; j++ {
			app.GET("/api/v1/resource"+strconv.Itoa(j)+"/item", h)
		}
		for j := 0; j < 50; j++ {
			app.GET("/api/v2/thing"+strconv.Itoa(j)+"/:id", h)
		}
	}
}
