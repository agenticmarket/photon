package photon

import (
	"context"
	"errors"
	"net/http"
	"time"

	"github.com/agenticmarket/photon/photonerr"
)

// StreamFunc is a streaming handler. It writes events to s and returns when
// the stream is finished. Returning an error ends the stream with that error;
// see SSE for how it reaches the client.
type StreamFunc func(s *Stream, r *http.Request) error

// SSE adapts a StreamFunc into an http.HandlerFunc. It is the recommended way
// to stream.
//
//	app.POST("/v1/chat", photon.SSE(func(s *photon.Stream, r *http.Request) error {
//		var req ChatRequest
//		if err := photon.DecodeJSON(r, &req); err != nil {
//			return err // nothing sent yet: the client gets a 400, not a stream
//		}
//		for chunk := range llm.Stream(r.Context(), req) {
//			if err := s.JSON("", chunk); err != nil {
//				return err // client gone: returning stops the upstream call
//			}
//		}
//		return s.Event("", []byte("[DONE]"))
//	}))
//
// Around the function it:
//
//   - enforces the server's MaxStreams limit, answering 503 with Retry-After
//     when the server is full, before any buffer is allocated;
//   - closes the stream when the function returns, flushing what is buffered;
//   - turns a returned error into the right response. Before the first event
//     the status code is still open, so the error is rendered as a normal
//     problem+json response with its own status. After it, the error is sent
//     in-band as an "error" event carrying the same problem document, and
//     server errors are logged. ErrClientGone and other end-of-stream errors
//     are not errors to report and are dropped quietly;
//   - recovers a panic after the first event by aborting the connection, so
//     the client sees a failed stream rather than a clean end that reads like
//     a complete answer.
//
// Options set the overflow policy, stall deadline, and buffer size for the
// streams this handler creates.
func SSE(fn StreamFunc, opts ...StreamOption) http.HandlerFunc {
	cfg := newStreamOptions(opts)
	return func(w http.ResponseWriter, r *http.Request) {
		serveStream(w, r, cfg, fn)
	}
}

// Streaming is middleware that gives every request it wraps a Stream, reached
// through StreamFrom, and enforces the server's MaxStreams limit.
//
//	app.GET("/events", events, photon.Streaming())
//
//	func events(w http.ResponseWriter, r *http.Request) {
//		s := photon.StreamFrom(r)
//		...
//	}
//
// It exists for handlers that must keep the plain http.Handler signature. SSE
// does the same job with less ceremony and also handles returned errors, so
// prefer it for new code. Apply Streaming to stream routes rather than with
// Use: every request through it occupies a MaxStreams slot for its duration.
func Streaming(opts ...StreamOption) Middleware {
	cfg := newStreamOptions(opts)
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			serveStream(w, r, cfg, func(_ *Stream, r *http.Request) error {
				next.ServeHTTP(w, r)
				return nil
			})
		})
	}
}

// serveStream runs fn with a stream for the request: the one shared machinery
// behind SSE and Streaming.
func serveStream(w http.ResponseWriter, r *http.Request, cfg streamMiddlewareConfig, fn StreamFunc) {
	// Already inside a stream - Streaming() and SSE on the same route. Reuse
	// it: a second stream would take a second MaxStreams slot and fight the
	// first for the ResponseWriter. The outer one owns the close.
	if st := StreamFrom(r); st != nil {
		st.finish(w, r, fn(st, r), false)
		return
	}

	srv := serverFromContext(r)
	var st *Stream
	if srv != nil {
		bufBytes := srv.limits.MaxStreamBufferBytes
		if cfg.maxBuf > 0 {
			bufBytes = cfg.maxBuf
		}
		if !srv.acquireStream(bufBytes) {
			srv.limitsError(w, r, http.StatusServiceUnavailable,
				"too_many_streams",
				"the server is at its concurrent stream limit, retry shortly",
				"MaxStreams")
			return
		}
		defer srv.releaseStream(bufBytes)
		st = srv.newRequestStream(w, r, cfg)
	} else {
		st = newStandaloneStream(w, r, cfg)
	}
	r = r.WithContext(context.WithValue(r.Context(), streamKey{}, st))
	st.r = r

	defer func() {
		v := recover()
		if v == nil {
			return
		}
		st.mu.Lock()
		started := st.started
		if started {
			st.failLocked(ErrStreamPanic)
		}
		st.mu.Unlock()
		if !started || v == http.ErrAbortHandler {
			// Nothing has been sent, so the server's own recovery can still
			// answer with a 500. Close first, so the stream releases its slot
			// and touches nothing after the handler unwinds.
			_ = st.Close()
			panic(v)
		}
		// The headers are committed. Report the panic here, because it is
		// about to be replaced by ErrAbortHandler, then abort the connection.
		if srv != nil {
			srv.reportPanic(v, r)
		} else {
			defaultLogger.Error("panic in streaming handler", "path", r.URL.Path, "panic", v)
		}
		_ = st.Close()
		panic(http.ErrAbortHandler)
	}()

	st.finish(w, r, fn(st, r), true)
}

// finish delivers the handler's result and, when owned, closes the stream.
func (s *Stream) finish(w http.ResponseWriter, r *http.Request, err error, owned bool) {
	if err == nil || isEndOfStream(err, r) {
		if owned {
			_ = s.Close()
		}
		return
	}
	s.mu.Lock()
	started, sse := s.started, s.sse
	s.mu.Unlock()

	if !started {
		// Nothing was written, so the status code is still ours to choose.
		if owned {
			_ = s.Close()
		}
		Error(w, r, err)
		return
	}

	var pe *photonerr.Error
	if !errors.As(err, &pe) {
		pe = photonerr.Internal(err)
	}
	if pe.Status >= 500 {
		logFor(r).Error("stream failed",
			"method", r.Method, "path", r.URL.Path, "status", pe.Status, "err", err)
	}
	if sse {
		// The status line is long gone, so the error travels in-band as an
		// "error" event with the same problem document a non-stream error would
		// have had. Only the safe message is sent.
		_ = s.sendProblem(pe)
		if owned {
			_ = s.Close()
		}
		return
	}
	// Not SSE, so there is no in-band error format. Ending the response cleanly
	// would make a truncated body look complete; abort the connection instead.
	s.mu.Lock()
	s.failLocked(err)
	s.mu.Unlock()
	if owned {
		_ = s.Close()
	}
	panic(http.ErrAbortHandler)
}

// sendProblem writes pe as an "error" event.
func (s *Stream) sendProblem(pe *photonerr.Error) error {
	title := pe.Message
	if title == "" {
		title = http.StatusText(pe.Status)
	}
	return s.JSON("error", struct {
		Type   string `json:"type"`
		Title  string `json:"title"`
		Status int    `json:"status"`
		Code   string `json:"code,omitempty"`
	}{"https://photon.agenticmarket.dev/errors/" + pe.Code, title, pe.Status, pe.Code})
}

// isEndOfStream reports whether err only says the stream ended - the client
// left, stopped reading, or the stream was closed - rather than that the
// handler failed.
func isEndOfStream(err error, r *http.Request) bool {
	switch {
	case errors.Is(err, ErrClientGone), errors.Is(err, ErrStreamOverflow), errors.Is(err, ErrStreamClosed):
		return true
	case errors.Is(err, context.Canceled) && r.Context().Err() != nil:
		// An upstream call cancelled because the client left.
		return true
	}
	return false
}

// streamMiddlewareConfig holds the options for streams created by SSE,
// Streaming, and NewStream.
type streamMiddlewareConfig struct {
	overflow OverflowPolicy
	stall    time.Duration
	maxBuf   int
}

// StreamOption configures the streams created by SSE, Streaming, or
// NewStream.
type StreamOption func(*streamMiddlewareConfig)

func newStreamOptions(opts []StreamOption) streamMiddlewareConfig {
	var cfg streamMiddlewareConfig
	for _, o := range opts {
		o(&cfg)
	}
	return cfg
}

// WithOverflow sets the overflow policy. A handler can still change it per
// stream with Stream.Overflow before the first write.
func WithOverflow(p OverflowPolicy) StreamOption {
	return func(c *streamMiddlewareConfig) { c.overflow = p }
}

// WithStall sets how long a stream may make no progress against a full buffer
// before it ends with ErrStreamOverflow, overriding StreamStallTimeout.
func WithStall(d time.Duration) StreamOption {
	return func(c *streamMiddlewareConfig) { c.stall = d }
}

// WithStreamBuffer sets the per-stream buffer cap in bytes, overriding
// MaxStreamBufferBytes.
func WithStreamBuffer(n int) StreamOption {
	return func(c *streamMiddlewareConfig) { c.maxBuf = n }
}

// streamKey is the context key for the request's Stream.
type streamKey struct{}

// StreamFrom returns the Stream attached to r by SSE or Streaming, or nil.
func StreamFrom(r *http.Request) *Stream {
	if r == nil {
		return nil
	}
	s, _ := r.Context().Value(streamKey{}).(*Stream)
	return s
}

// acquireStream reserves one slot in the concurrent stream budget, and the
// stream's buffer in the global buffer budget.
//
// Both are checked because either can run out. The count bounds goroutines and
// connections; the bytes bound memory, and they diverge when some streams use
// a larger buffer (WithStreamBuffer) or have grown into idle capacity (Grow) -
// a new stream must not be admitted into memory another stream has borrowed.
func (s *Server) acquireStream(bufBytes int) bool {
	if s.limits.MaxStreams > 0 {
		if n := s.streams.Add(1); n > int64(s.limits.MaxStreams) {
			s.streams.Add(-1)
			return false
		}
	}
	if ceiling := s.limits.MaxTotalStreamBufferBytes; ceiling > 0 {
		if s.nominalBytes.Add(int64(bufBytes))+s.streamBytes.Load() > ceiling {
			s.nominalBytes.Add(-int64(bufBytes))
			if s.limits.MaxStreams > 0 {
				s.streams.Add(-1)
			}
			return false
		}
	}
	return true
}

// releaseStream returns a slot and its buffer bytes to the budgets.
func (s *Server) releaseStream(bufBytes int) {
	if s.limits.MaxStreams > 0 {
		s.streams.Add(-1)
	}
	if s.limits.MaxTotalStreamBufferBytes > 0 {
		s.nominalBytes.Add(-int64(bufBytes))
	}
}

// streamBufferCeiling returns the largest a single stream's buffer may grow.
func (s *Server) streamBufferCeiling() int {
	if s.limits.MaxTotalStreamBufferBytes <= 0 {
		return s.limits.MaxStreamBufferBytes
	}
	return int(s.limits.MaxTotalStreamBufferBytes)
}

// reserveStreamBytes accounts for a Grow beyond the per-stream cap.
//
// MaxTotalStreamBufferBytes is a true ceiling on stream buffer memory: the
// buffers of the streams currently open plus all growth must fit under it,
// checked both here and when a stream is admitted. With the derived default
// (MaxStreams x MaxStreamBufferBytes) a stream can only grow into room left by
// stream slots not in use - Grow borrows idle capacity, and a full server has
// none to lend. An earlier version counted growth alone against the ceiling,
// so buffer memory could reach twice the stated limit.
func (s *Server) reserveStreamBytes(n int64) bool {
	ceiling := s.limits.MaxTotalStreamBufferBytes
	if ceiling <= 0 {
		return false
	}
	if s.streamBytes.Add(n)+s.nominalBytes.Load() > ceiling {
		s.streamBytes.Add(-n)
		return false
	}
	return true
}

// releaseStreamBytes returns grown bytes to the global budget.
func (s *Server) releaseStreamBytes(n int64) {
	if s.streamBytes.Add(-n) < 0 {
		s.streamBytes.Store(0)
	}
}
