// Command go_photon serves the benchmark stream with photon.
//
// Two configurations, because photon's write policy and a bare ResponseWriter's
// differ, and comparing them head to head without saying so would measure the
// policy rather than the framework:
//
//	-flush=coalesced  photon's shipped default (64 KiB buffer). Each event is
//	                 handed to a per-stream writer goroutine and sent as soon
//	                 as the connection is free; events written while a send is
//	                 in flight go out together in the next one. Nothing is held
//	                 back waiting for more (see SSETokenLatency).
//	-flush=tight      a 512-byte buffer, so the producer waits whenever 512
//	                 bytes are pending and each write carries only a few
//	                 events. This is the closest the public API comes to the
//	                 competitors' write-and-flush-per-event.
//
// Both are reported. A table showing only the default figure would hide how
// much of the gap is batching, and one showing only the tight figure would
// misrepresent what photon does out of the box.
package main

import (
	"flag"
	"fmt"
	"net/http"
	"os"

	"github.com/agenticmarket/photon"

	"streambench/gosrv"
	"streambench/sse"
)

func main() {
	mode := flag.String("flush", "coalesced", "coalesced|tight")
	flag.Parse()

	app := photon.New()

	switch *mode {
	case "coalesced":
		// Left at photon's default rather than configured, so the number is the
		// shipped behaviour and not a tuned one.
		app.Use(photon.Streaming())
	case "tight":
		// The smallest useful buffer: a few frames per write, not one. The
		// report says so rather than calling this per-event.
		app.Use(photon.Streaming(photon.WithStreamBuffer(512)))
	default:
		fmt.Fprintf(os.Stderr, "go_photon: unknown -flush=%q, want coalesced or tight\n", *mode)
		os.Exit(2)
	}

	var delivered sse.Events

	app.GET("/stream", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		s := photon.StreamFrom(r)
		if s == nil {
			// Streaming() did not attach a stream. Answering 500 rather than
			// hanging means the driver's equivalence gate reports the failure,
			// rather than the run silently measuring a server that streams
			// nothing.
			http.Error(w, "no stream attached", http.StatusInternalServerError)
			return
		}
		n := sse.Count(r.URL.Query().Get("n"))
		for i := 0; i < n; i++ {
			// Event rather than Write: the SSE framing is the protocol, and
			// hand-formatting the frame here would measure photon's raw write
			// path instead of its streaming path.
			if err := s.Event(sse.EventName, sse.Payload); err != nil {
				return
			}
		}
		delivered.Add(int64(n))
		_ = s.Close()
	}))

	app.GET("/mem", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Peak RSS and delivered events, read in-process: the driver is a
		// separate process, so this is photon's own memory rather than an upper
		// bound that includes the harness.
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		fmt.Fprintf(w, "rss=%d events=%d", sse.Memory(), delivered.Load())
	}))

	gosrv.ListenReady("photon/"+*mode, app)
}
