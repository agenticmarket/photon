// Package gosrv holds the handler bodies shared by the non-photon Go servers.
//
// chi, Gin, and Echo all wrap a plain http.Handler, so one implementation can
// serve all three. That is deliberate: if each framework's handler were written
// separately, a difference in the tables could come from a difference in the
// handlers rather than from the frameworks, and nothing would reveal which.
package gosrv

import (
	"fmt"
	"net/http"
	"sync/atomic"

	"streambench/sse"
)

// delivered counts events across the process, for the in-process memory report.
var delivered atomic.Int64

// writeFlusher is what a streaming handler needs.
//
// Asserted rather than assumed: a writer that cannot stream would otherwise
// buffer the whole response and still report a throughput figure, and the
// driver's equivalence gate would then pass against a server that never streamed
// a single event.
type writeFlusher interface {
	http.ResponseWriter
	Flush()
}

// NewStreamHandler returns the /stream handler.
//
// The frame is precomputed and the header block written verbatim, so every server
// in the harness emits identical bytes. Letting each framework set headers its
// own way would make header handling part of what is measured, and for SSE the
// header block is identical work everywhere.
//
// Flushing per event is forced, and is what a hand-written SSE handler does: the
// client sees each token as it is produced. photon is measured both this way and
// coalesced, and the report shows both rather than the flattering one.
func NewStreamHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		bw, ok := w.(writeFlusher)
		if !ok {
			http.Error(w, "streaming unsupported", http.StatusInternalServerError)
			return
		}
		// Headers go through the header map, and the status is committed by the
		// first flush. Writing a literal header block as the first Write would
		// make net/http treat it as body and add its own framing around it, which
		// is a difference in response bytes that has nothing to do with
		// streaming.
		sse.ApplyHeaders(bw.Header())

		n := sse.Count(r.URL.Query().Get("n"))
		for i := 0; i < n; i++ {
			if _, err := bw.Write(sse.Frame); err != nil {
				return
			}
			// Flush per event: the client sees each token as it is produced.
			// http.Flusher.Flush has no error to return, so a failed flush is
			// reported by the next Write instead.
			bw.Flush()
		}
		delivered.Add(int64(n))
	})
}

// NewMemHandler returns the /mem handler, reporting peak RSS and delivered
// events.
//
// Read in-process because the driver is a separate process. A process-wide
// figure taken by the harness would include the harness, and so would only be an
// upper bound on the server's cost rather than the server's own.
func NewMemHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		fmt.Fprintf(w, "rss=%d events=%d", sse.Memory(), delivered.Load())
	})
}
