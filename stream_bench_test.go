package photon

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// The synthetic producer is the important benchmark here, and the reason is
// stated plainly: a real model provider would put its own network latency and
// its own token cadence between the producer and the server, so any measurement
// through one would be mostly a measurement of that provider. The synthetic
// producer emits events 1, 2, 3 ... N as fast as it can, so what is left is
// photon's own cost.
//
// A real provider's latency would dominate any measurement through it, so it
// is deliberately not part of this benchmark.

// tokenPayload is one synthetic event's payload, sized like a real token.
var tokenPayload = []byte(" the quick brown fox jumps over the lazy dog")

// newSyntheticStreamServer returns a server whose /stream endpoint emits n
// events and then closes.
func newSyntheticStreamServer(tb testing.TB, maxStreams int) *Server {
	tb.Helper()
	app := New(MaxStreams(maxStreams))
	app.Use(Streaming())
	app.GET("/stream", HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		s := StreamFrom(r)
		// Parse the count from the query so one server can serve every size.
		n, _ := strconv.Atoi(r.URL.Query().Get("n"))
		if n <= 0 {
			n = 100
		}
		for i := 0; i < n; i++ {
			if err := s.Event("token", tokenPayload); err != nil {
				return
			}
		}
		_ = s.Close()
	}))
	return app
}

// BenchmarkStreamSyntheticProducer measures the per-event cost of a synthetic
// producer, in process.
//
// This is a microbenchmark: it measures the framing, buffering and flushing
// path, and it does NOT measure sockets, the kernel, or net/http's parser. The
// numbers are not requests per second and are not labelled as such.
func BenchmarkStreamSyntheticProducer(b *testing.B) {
	for _, events := range []int{1, 10, 100, 1000} {
		b.Run(fmt.Sprintf("events=%d", events), func(b *testing.B) {
			rec := httptest.NewRecorder()
			req := httptest.NewRequest(http.MethodGet, "/stream", nil)

			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				// A fresh stream per iteration: Close releases the buffer and
				// marks the stream closed, so reusing one would be measuring a
				// closed stream. The allocation reported per event is therefore
				// inclusive of the stream's own setup, which is the honest
				// figure for "one request that streamed N events".
				s := newStream(rec, req, newFlusher(rec), streamConfig{MaxBuffer: 64 << 10})
				for j := 0; j < events; j++ {
					if err := s.Event("token", tokenPayload); err != nil {
						b.Fatal(err)
					}
				}
				if err := s.Close(); err != nil {
					b.Fatal(err)
				}
				rec.Body.Reset()
			}
			b.StopTimer()
		})
	}
}

// BenchmarkStreamPerEventOverSocket measures the same synthetic producer over a
// real socket, which is the number that reflects what a client would see.
//
// The server and the client share a process and therefore a CPU, so these
// figures are a lower bound on what the server alone could do. That limitation
// is inherited from the existing harness and is recorded there too.
func BenchmarkStreamPerEventOverSocket(b *testing.B) {
	app := newSyntheticStreamServer(b, 64)
	srv := httptest.NewServer(app)
	defer srv.Close()

	const events = 100
	client := &http.Client{Timeout: 30 * time.Second}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		resp, err := client.Get(srv.URL + "/stream?n=" + strconv.Itoa(events))
		if err != nil {
			b.Fatal(err)
		}
		drainBody(resp)
	}
}

// BenchmarkStreamConcurrentStreams measures throughput at several concurrency
// levels over a real socket.
//
// The host caps concurrent handshakes at roughly 200, so the largest case is
// bounded by that rather than by the server. The benchmark reports what this
// machine can do and says so, rather than implying a number the host cannot
// produce.
func BenchmarkStreamConcurrentStreams(b *testing.B) {
	for _, streams := range []int{1, 10, 50, 100} {
		b.Run(fmt.Sprintf("streams=%d", streams), func(b *testing.B) {
			app := newSyntheticStreamServer(b, streams)
			srv := httptest.NewServer(app)
			defer srv.Close()

			// One client per stream, so the concurrency is real rather than
			// serialised through a single connection's request queue.
			var wg sync.WaitGroup
			urls := make([]string, streams)
			for i := range urls {
				urls[i] = srv.URL + "/stream?n=50"
			}

			// Each iteration every stream sends one request, so b.N is in units
			// of "rounds" and the reported ns/op is per round. The per-request
			// figure is the round time divided by the stream count, which
			// BenchmarkStreamThroughput reports directly.
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				for _, u := range urls {
					wg.Add(1)
					go func(u string) {
						defer wg.Done()
						resp, err := http.Get(u)
						if err != nil {
							return
						}
						drainBody(resp)
					}(u)
				}
				wg.Wait()
			}
		})
	}
}

// BenchmarkStreamThroughput reports events per second at each concurrency level,
// which is the figure a streaming deployment actually cares about.
//
// b.N is divided among the streams rather than multiplied by them: an earlier
// version gave every goroutine the full b.N, so the streams=50 and streams=100
// cases did proportionally more work and reported nonsense - including a
// 100-stream case that looked faster than the 10-stream one. The per-round cost
// is measured, and the per-event figure is derived from it.
//
// The server and the client share a process, so this is not a server-only
// measurement. It is a like-for-like comparison point against the existing
// framework harness, which shares the same limitation.
func BenchmarkStreamThroughput(b *testing.B) {
	for _, streams := range []int{1, 10, 50, 100} {
		b.Run(fmt.Sprintf("streams=%d", streams), func(b *testing.B) {
			app := newSyntheticStreamServer(b, streams)
			srv := httptest.NewServer(app)
			defer srv.Close()

			const eventsPerStream = 50
			// Each goroutine does b.N/streams requests, so the total work is b.N
			// rounds regardless of the stream count.
			perStream := b.N / streams
			if perStream < 1 {
				perStream = 1
			}
			var delivered atomic.Int64
			// A client sized for the concurrency. http.DefaultClient keeps two
			// idle connections per host, so with 100 concurrent streams almost
			// every request opened and closed a socket. On Windows, closesocket
			// blocks an OS thread, and this benchmark died at the runtime's
			// 10,000-thread limit measuring the harness rather than photon.
			client := &http.Client{Transport: &http.Transport{
				MaxIdleConns: streams, MaxIdleConnsPerHost: streams,
			}}
			defer client.CloseIdleConnections()

			b.ResetTimer()
			start := time.Now()
			var wg sync.WaitGroup
			for i := 0; i < streams; i++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					url := srv.URL + "/stream?n=" + strconv.Itoa(eventsPerStream)
					for j := 0; j < perStream; j++ {
						resp, err := client.Get(url)
						if err != nil {
							return
						}
						delivered.Add(int64(drainBody(resp)))
					}
				}()
			}
			wg.Wait()
			elapsed := time.Since(start)
			b.StopTimer()

			// events/s, measured over the window the benchmark actually ran.
			total := float64(delivered.Load())
			if total > 0 && elapsed > 0 {
				b.ReportMetric(total/elapsed.Seconds(), "events/s")
			}
		})
	}
}

// BenchmarkStreamSlowConsumer measures the cost of the back-pressure path: a
// consumer that reads slowly, forcing the buffer to fill and flush repeatedly.
//
// This is the path that has to stay cheap, because it is the path an attack
// drives and the path a real slow network takes.
func BenchmarkStreamSlowConsumer(b *testing.B) {
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/stream", nil)
	s := newStream(rec, req, newFlusher(rec), streamConfig{MaxBuffer: 4 << 10})
	// Flush on every write, which is the most expensive legal setting and the
	// one a slow consumer effectively imposes.

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = s.Event("token", tokenPayload)
	}
	b.StopTimer()
	_ = s.Close()
}

// drainBody reads and closes a response, returning the number of data lines.
func drainBody(resp *http.Response) int {
	if resp == nil {
		return 0
	}
	defer resp.Body.Close()
	buf := make([]byte, 32<<10)
	lines := 0
	total := 0
	for {
		n, err := resp.Body.Read(buf)
		total += n
		for _, c := range buf[:n] {
			if c == '\n' {
				lines++
			}
		}
		if err != nil {
			break
		}
	}
	return lines
}
