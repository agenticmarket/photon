// Package sse defines the one stream every server in this benchmark emits.
//
// The fairness rule of the audit is that two servers are only comparable if they
// produce identical bytes. That has to be enforced in a single place, or each
// server's handler drifts and the tables quietly start measuring different
// responses. Everything here is therefore specified, not chosen: the frame below
// is what photon's Event produces, and every competitor must match it exactly.
package sse

import (
	"net/http"
	"strconv"
	"strings"
	"sync/atomic"
)

// EventName is the single event name every server uses. Varying it per server
// would make the payloads differ for no useful reason.
const EventName = "token"

// Payload is the per-event data, sized like a real token. It contains no
// newline, so the whole payload is one "data:" line and every server can produce
// the same frame with a single write.
var Payload = []byte(" the quick brown fox jumps over the lazy dog")

// ApplyHeaders sets the streaming headers on h and commits a 200.
//
// Set through the header map rather than written as a literal block. Writing the
// block as the first Write makes net/http treat it as body content: it then adds
// its own status line, a Date, and chunked framing around it, and the server
// emits 478 bytes where photon emits 325 - a difference that has nothing to do
// with streaming and would invalidate every table.
//
// The header values are photon's, so that the framework layer is what is being
// compared rather than each server's idea of a streaming response. net/http adds
// the same Date and Transfer-Encoding to all of them.
func ApplyHeaders(h http.Header) {
	h.Set("Content-Type", "text/event-stream; charset=utf-8")
	h.Set("Cache-Control", "no-cache, no-transform")
	h.Set("Connection", "keep-alive")
	h.Set("X-Accel-Buffering", "no")
}

// Frame is one complete SSE event, including its terminating blank line.
//
// It is precomputed so that every server formats the identical bytes with a
// single append, and so the benchmark measures streaming rather than string
// concatenation. A server that formats the frame per event would be measuring
// formatting, which is the one thing all five Go servers would otherwise do
// differently.
var Frame = []byte("event: " + EventName + "\n" +
	"data: " + string(Payload) + "\n" +
	"\n")

// FrameLen is the length of one frame, used to verify a delivered byte count
// against an expected one.
func FrameLen() int { return len(Frame) }

// TotalBytes returns the exact response body length for a stream of n events.
func TotalBytes(n int) int { return n * len(Frame) }

// Count parses n from a query string, defaulting to 50.
//
// The default matches the benchmark's per-stream event count, so a malformed
// request still produces a stream rather than an error the driver would have to
// special-case.
func Count(s string) int {
	if s == "" {
		return 50
	}
	n, err := strconv.Atoi(s)
	if err != nil || n <= 0 {
		return 50
	}
	// Bounded so a hostile or accidental value cannot make one stream allocate
	// for hours. The benchmark never asks for more than this.
	const maxCount = 1_000_000
	if n > maxCount {
		return maxCount
	}
	return n
}

// Memory returns the process's peak resident set in bytes, as this runtime can
// report it.
//
// Measured inside the server process on purpose. The existing harness measures
// allocations process-wide, which includes its own client and is therefore only
// an upper bound on the server's cost. Here the client is a separate process, so
// this figure is the server's own.
func Memory() int64 { return readMem() }

// Events tracks delivered events for the in-process memory report.
type Events struct{ n atomic.Int64 }

func (e *Events) Add(delta int64) { e.n.Add(delta) }
func (e *Events) Load() int64     { return e.n.Load() }

// CountEvents counts the "event: " occurrences in a body, used by the driver's
// equivalence gate. It is exported so the gate is written once.
func CountEvents(body []byte) int {
	return strings.Count(string(body), "event: ")
}
