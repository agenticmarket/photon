package main

import (
	"net/http"

	"github.com/gin-gonic/gin"

	"streambench/sse"
)

// ginNativeStream streams using Gin's own ResponseWriter, through a Gin handler.
//
// It exists to measure what using Gin *properly* for SSE costs, rather than
// sidestepping Gin's pipeline with gin.WrapH. Every event here goes through
// c.Writer, so Gin's per-request bookkeeping is included - which is the honest
// figure for a team that has adopted Gin and needs to stream inside it.
//
// Both variants are reported: /stream-gin (this one) and /stream (WrapH). The
// gap between them is the cost of Gin's context, and hiding it would flatter
// Gin.
func ginNativeStream() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		// Asserted rather than assumed: a writer that cannot stream would
		// otherwise buffer the whole response and still report a throughput
		// figure, and the driver's equivalence gate would pass against a server
		// that never streamed an event.
		c, ok := w.(gin.ResponseWriter)
		if !ok {
			http.Error(w, "streaming unsupported", http.StatusInternalServerError)
			return
		}
		sse.ApplyHeaders(c.Header())
		c.WriteHeader(http.StatusOK)
		c.Flush()

		n := sse.Count(r.URL.Query().Get("n"))
		for i := 0; i < n; i++ {
			if _, err := c.Write(sse.Frame); err != nil {
				return
			}
			c.Flush()
		}
	}
}
