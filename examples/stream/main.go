// Command stream is a tour of photon's server-sent events.
//
// Run it:
//
//	go run ./examples/stream
//
// Then, in another terminal (-N turns off curl's buffering):
//
//	curl -N localhost:8080/tokens                         # a token stream
//	curl -N localhost:8080/resumable                      # Ctrl-C halfway, then:
//	curl -N -H 'Last-Event-ID: 5' localhost:8080/resumable
//	curl -N 'localhost:8080/metrics?every=200ms'          # DropOldest, a feed that never ends
//	curl -N localhost:8080/slow-consumer --limit-rate 200k # back-pressure; Ctrl-Z to stop reading
//	curl -N localhost:8080/fail                           # an error after streaming starts
//
// Every handler is a photon.StreamFunc: write events, check the error, return.
// photon.SSE does the rest - headers, flushing, limits, heartbeats, closing,
// and turning a returned error into the right response.
package main

import (
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/agenticmarket/photon"
	"github.com/agenticmarket/photon/photonerr"
)

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	app := photon.New(photon.Logger(logger))

	app.GET("/tokens", photon.SSE(tokens))
	app.GET("/resumable", photon.SSE(resumable))
	// Metrics are the one stream where losing an old value is fine: the next
	// one supersedes it. DropOldest keeps a slow client current instead of
	// making it fall behind. Never use it for text.
	app.GET("/metrics", photon.SSE(metrics, photon.WithOverflow(photon.DropOldest)))
	// A short stall deadline, so the disconnect is quick enough to watch.
	app.GET("/slow-consumer", photon.SSE(slowConsumer, photon.WithStall(3*time.Second)))
	app.GET("/fail", photon.SSE(fail))
	app.GET("/", index)

	logger.Info("listening on http://localhost:8080")
	if err := app.Run(":8080"); err != nil {
		logger.Error("server stopped", "err", err)
		os.Exit(1)
	}
}

var answer = strings.Fields("Photon sends each token the moment it is produced, " +
	"stops the work when the client leaves, and keeps a slow client from costing the server memory.")

// tokens streams an answer a word at a time, the way a model does.
func tokens(s *photon.Stream, r *http.Request) error {
	for i, word := range answer {
		if i > 0 {
			word = " " + word
		}
		// The error is the whole cancellation story: if the client has gone,
		// Event says so and the handler stops. Pass r.Context() to a real
		// model call and the model stops too.
		if err := s.Event("token", []byte(word)); err != nil {
			return err
		}
		time.Sleep(100 * time.Millisecond) // a model's token cadence
	}
	return s.Event("done", nil) // empty data is still delivered
}

// resumable streams the same answer with event ids, so a client that drops
// can reconnect with Last-Event-ID and receive only what it missed. A browser's
// EventSource sends the header automatically.
func resumable(s *photon.Stream, r *http.Request) error {
	_ = s.SetRetry(2 * time.Second) // how long the client waits before reconnecting

	// Last-Event-ID is client input: parse it, bound it, and on anything odd
	// start over rather than guess.
	start := 0
	if n, err := strconv.Atoi(r.Header.Get("Last-Event-ID")); err == nil && n >= 0 {
		start = min(n+1, len(answer)) // n is the last event received
	}
	for i := start; i < len(answer); i++ {
		// The id travels with the event in one frame, so the two cannot be
		// separated.
		if err := s.SetEventID(strconv.Itoa(i)); err != nil {
			return err
		}
		if err := s.JSON("token", map[string]any{"index": i, "text": answer[i]}); err != nil {
			return err
		}
		time.Sleep(150 * time.Millisecond)
	}
	return s.Event("done", nil)
}

// metrics is a feed that never ends on its own. It watches s.Done(), which
// closes when the client leaves OR the server begins a graceful shutdown, so a
// deploy does not wait for this stream to time out.
func metrics(s *photon.Stream, r *http.Request) error {
	every := 500 * time.Millisecond
	if d, err := time.ParseDuration(r.URL.Query().Get("every")); err == nil && d >= 10*time.Millisecond {
		every = d
	}
	tick := time.NewTicker(every)
	defer tick.Stop()
	for {
		select {
		case <-s.Done():
			// Still writable during shutdown: say why the stream is ending.
			return s.Event("bye", []byte("server shutting down"))
		case t := <-tick.C:
			if err := s.JSON("metric", map[string]any{
				"at": t.UnixMilli(), "rps": 100 + rand.IntN(400),
			}); err != nil {
				return err
			}
		}
	}
}

// slowConsumer writes far faster than a throttled client reads. The buffer is
// bounded, so the producer slows to the client's pace instead of the server's
// memory growing. Suspend curl (Ctrl-Z) and the client stops reading entirely:
// three seconds later - the stall deadline set for this route - the stream ends
// with ErrStreamOverflow and the handler's goroutine and buffer are released.
func slowConsumer(s *photon.Stream, r *http.Request) error {
	chunk := strings.Repeat("x", 1024)
	start := time.Now()
	for i := 0; i < 100_000; i++ {
		if err := s.Event("chunk", []byte(chunk)); err != nil {
			slog.Info("slow consumer ended", "events", i, "after", time.Since(start).Round(time.Millisecond),
				"reason", err, "overflow", errors.Is(err, photon.ErrStreamOverflow))
			return err
		}
	}
	return nil
}

// fail shows an error after the first event. The status line is already sent,
// so the error goes in-band as an "error" event with a safe message, and the
// cause is logged for the operator.
func fail(s *photon.Stream, r *http.Request) error {
	if err := s.Event("token", []byte("Starting")); err != nil {
		return err
	}
	time.Sleep(200 * time.Millisecond)
	return photonerr.Upstream(fmt.Errorf("model server at 10.0.0.7 reset the connection"))
}

func index(w http.ResponseWriter, r *http.Request) {
	_ = photon.HTML(w, http.StatusOK, `<!doctype html><meta charset="utf-8"><title>photon streaming</title>
<h1>photon streaming</h1>
<ul>
  <li><a href="/tokens">/tokens</a> - an answer, one token at a time</li>
  <li><a href="/resumable">/resumable</a> - the same, resumable with Last-Event-ID</li>
  <li><a href="/metrics?every=200ms">/metrics</a> - an endless feed using DropOldest</li>
  <li><a href="/slow-consumer">/slow-consumer</a> - back-pressure (try curl --limit-rate 200k)</li>
  <li><a href="/fail">/fail</a> - an error after the stream has started</li>
</ul>
<pre id="out"></pre>
<script>
const es = new EventSource("/tokens"), out = document.getElementById("out");
es.addEventListener("token", (e) => out.textContent += e.data);
es.addEventListener("done", () => es.close());
</script>`)
}
