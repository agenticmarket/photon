package photon

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode"
	"unicode/utf8"
)

// Streaming errors. A streaming handler returns an error; it does not panic, and
// it does not try to write a status code after the headers are committed.
var (
	// ErrClientGone means the client disconnected before the stream finished.
	//
	// This is the normal end of a stream whose client went away, not a failure.
	// A handler should treat it as a reason to stop work, not to report an
	// error: the whole point of propagating the request context is that the
	// upstream model call can be cancelled instead of billing for output
	// nobody will read.
	ErrClientGone = errors.New("photon: stream client disconnected")

	// ErrStreamClosed means the stream was already closed. Writes after Close
	// return it rather than panicking, because a deferred Close racing a
	// normal end-of-stream is a normal thing for a handler to do.
	ErrStreamClosed = errors.New("photon: stream is closed")

	// ErrStreamOverflow means the consumer accepted nothing for a whole
	// StreamStallTimeout while the buffer was full, so the stream was ended
	// rather than allowed to hold memory and a goroutine indefinitely.
	ErrStreamOverflow = errors.New("photon: stream overflowed: consumer too slow")

	// ErrStreamPanic means a handler panicked after the response headers were
	// committed.
	//
	// The client sees the connection abort, not a 500: the status line is long
	// gone. The connection is aborted rather than closed cleanly so that a
	// truncated answer cannot be mistaken for a complete one.
	ErrStreamPanic = errors.New("photon: handler panicked during stream")

	// ErrInvalidEvent means an SSE event name or id contained a line break or
	// a NUL. Either would end the field early and let the rest of the value be
	// read as a new field - an injection, when the value came from a user or a
	// model. Data may contain any bytes; it is split into lines safely.
	ErrInvalidEvent = errors.New("photon: SSE event name or id contains a line break or NUL")
)

// OverflowPolicy decides what a producer does when it writes faster than the
// client reads and the stream's buffer is full.
//
// The buffer holds bytes accepted from the handler but not yet written to the
// client. While the client keeps reading, however slowly, the policy shapes
// the stream; if the client stops reading entirely for StreamStallTimeout, the
// stream ends with ErrStreamOverflow under every policy, because at that point
// the only alternative is holding the resources forever.
type OverflowPolicy int

const (
	// Disconnect applies back-pressure: the producer waits for the client to
	// make room, and the stream ends only if no room appears within
	// StreamStallTimeout. It is the default and the only policy that never
	// loses or reorders data, which makes it the right one for text.
	Disconnect OverflowPolicy = iota

	// DropOldest discards the oldest whole events still waiting to be sent, so
	// the producer does not wait for a slow client and the client always
	// receives the newest data. The producer waits only when the room it needs
	// is held by bytes already being written, which cannot be recalled.
	//
	// Use it only where staleness is worth less than completeness: metrics,
	// progress, presence, and change feeds where the newest value supersedes
	// the last. Do NOT use it for text. A token stream with events dropped from
	// the middle reads as a hallucination, and the client cannot tell "the
	// model said that" from "the server dropped 400 tokens". The server logs a
	// warning, which cannot be turned off, when a stream that has carried text
	// actually drops data.
	//
	// Events are dropped whole, never cut: a partial SSE frame would corrupt
	// every event after it.
	DropOldest

	// Grow enlarges the stream's buffer beyond MaxStreamBufferBytes to absorb
	// a burst, drawing on the server-wide MaxTotalStreamBufferBytes budget.
	// When the budget is exhausted it behaves like Disconnect.
	Grow
)

// String makes the policy readable in logs.
func (p OverflowPolicy) String() string {
	switch p {
	case Disconnect:
		return "Disconnect"
	case DropOldest:
		return "DropOldest"
	case Grow:
		return "Grow"
	default:
		return "Unknown"
	}
}

// defaultStreamBufferBytes is the per-stream buffer cap when nothing else is
// configured. It matches Limits.MaxStreamBufferBytes.
const defaultStreamBufferBytes = 64 << 10

// initialStreamBuffer is the buffer a stream starts with. It grows on demand
// up to the cap, so a token stream - a few hundred bytes in flight at a time -
// never pays for the full 64 KB.
const initialStreamBuffer = 4 << 10

// heartbeatFrame is the SSE comment a heartbeat sends. Every SSE client
// ignores comments, so it keeps a proxy's idle timer reset and nothing else.
var heartbeatFrame = []byte(": ping\n\n")

// Stream is a bounded, cancellable streaming response, usually server-sent
// events.
//
// The easiest way to get one is SSE, which also enforces the server's stream
// limit, handles errors, and closes the stream for you:
//
//	app.POST("/chat", photon.SSE(func(s *photon.Stream, r *http.Request) error {
//		for tok := range model.Stream(r.Context(), prompt) {
//			if err := s.Event("token", []byte(tok)); err != nil {
//				return err // the client left; stop the upstream work
//			}
//		}
//		return s.Event("done", nil)
//	}))
//
// # Delivery
//
// Each Event is sent as soon as the connection is free, so a token reaches the
// client the moment it is produced. When the producer is faster than the
// network, events that queue up while a write is in progress are sent together
// in the next write, which is what keeps a fast producer from costing one
// system call per token.
//
// # Memory is bounded
//
// Bytes accepted but not yet sent live in a buffer capped at
// MaxStreamBufferBytes. A slow client therefore slows the producer (or loses
// old events, under DropOldest) instead of growing the server's memory, and a
// client that stops reading entirely is disconnected after
// StreamStallTimeout.
//
// # The status code is committed at the first write
//
// The first write of any kind - Event, JSON, Comment, SetRetry, Write, Flush,
// or StartHeartbeats - commits a 200 with the streaming headers.
// Until then the handler can still answer with any status - which is why SSE
// renders an error returned before the first event as a normal error response.
//
// # Concurrency
//
// A Stream is safe for concurrent use. Each event is written as a unit, so
// events from different goroutines never interleave mid-frame; their order is
// the order in which the calls acquired the stream. Event order from a single
// goroutine is always preserved.
type Stream struct {
	w   http.ResponseWriter
	rc  streamFlusher
	r   *http.Request
	srv *Server

	// Configuration, fixed at the first write.
	policy    OverflowPolicy
	stall     time.Duration
	maxBuffer int
	heartbeat time.Duration

	// ctxDone is the request's Done channel, nil for a stream with no request.
	ctxDone <-chan struct{}

	// emitMu serialises producers. One frame is assembled and handed to the
	// buffer at a time, which is what keeps a heartbeat or a second goroutine
	// from splicing bytes into the middle of an event.
	emitMu  sync.Mutex
	scratch []byte        // frame under construction; guarded by emitMu
	encBuf  bytes.Buffer  // JSON encoding scratch; guarded by emitMu
	enc     *json.Encoder // writes into encBuf; guarded by emitMu
	nextID  string        // id for the next event; guarded by emitMu
	hasID   bool          // whether nextID is set; guarded by emitMu

	// mu guards everything below, which the writer goroutine shares with the
	// producers.
	mu sync.Mutex
	// buf holds accepted bytes not yet written. buf[:inflight] is being
	// written by the writer goroutine right now and must not be modified;
	// everything after it may be appended to or, under DropOldest, dropped.
	buf      []byte
	inflight int
	// limit is the cap on len(buf): maxBuffer, or more after Grow.
	limit int
	// frames records where each complete frame in buf ends, for DropOldest.
	// It is only kept under that policy.
	frames []frameRec
	// midFrame is set while an oversized event is being fed through the
	// buffer in pieces. A heartbeat must not be spliced into it.
	midFrame bool
	// needFlush asks the writer to flush even with nothing buffered, so Flush
	// can commit the headers on their own.
	needFlush bool
	// reserved is the growth this stream holds from the server's global
	// stream budget.
	reserved int64

	started     bool // headers committed and the writer running
	closed      bool // no further writes accepted
	closeCalled bool // Close has run
	sse         bool // the response is text/event-stream, so heartbeats are safe
	hbOff       bool // heartbeats stopped by the caller
	err         error
	wroteText   bool // a text-like payload was written, for the DropOldest warning
	warnedDrop  bool // the DropOldest warning has been logged
	grewPast    bool // Grow enlarged the buffer beyond its nominal cap
	abandoned   bool // a write never returned, so the writer was given up on

	// accepted, written, and dropped count bytes, so Flush can tell when
	// everything accepted before it has left the buffer.
	accepted, written, dropped int64

	wake       chan struct{} // producer -> writer: there is work
	space      chan struct{} // writer -> producer: room was freed
	writerDone chan struct{} // closed when the writer goroutine exits

	// doneCh and doneCancel back Done, created on first use.
	doneCh     <-chan struct{}
	doneCancel context.CancelFunc
}

// frameRec is one complete frame in a stream's buffer.
type frameRec struct {
	end int // offset in buf just past the frame
	// droppable is false for a frame too large for the buffer, which was fed
	// through in pieces and may already be partly on the wire.
	droppable bool
}

// frame kinds for appendLocked.
const (
	frameWhole = iota // a complete frame, droppable
	framePart         // a piece of a frame larger than the buffer
	frameLast         // the final piece of such a frame
)

// streamFlusher is the subset of http.ResponseController photon needs, kept as
// an interface so tests can substitute a writer that blocks.
type streamFlusher interface {
	Flush() error
	SetWriteDeadline(t time.Time) error
}

// newFlusher returns a streamFlusher for w.
//
// http.ResponseController is the supported way to reach Flush and
// SetWriteDeadline through a wrapped ResponseWriter. A writer that supports
// neither returns http.ErrNotSupported, which ends the stream with an error
// naming the problem rather than buffering the whole response silently.
func newFlusher(w http.ResponseWriter) streamFlusher {
	return &responseControllerFlusher{rc: http.NewResponseController(w)}
}

// responseControllerFlusher adapts net/http's ResponseController.
type responseControllerFlusher struct {
	rc *http.ResponseController
}

func (f *responseControllerFlusher) Flush() error { return f.rc.Flush() }

func (f *responseControllerFlusher) SetWriteDeadline(t time.Time) error {
	return f.rc.SetWriteDeadline(t)
}

// streamConfig carries per-stream settings. The zero value of each field means
// "none" here; NewStream and the server fill in defaults before calling
// newStream.
type streamConfig struct {
	Overflow  OverflowPolicy
	Stall     time.Duration
	MaxBuffer int
	Heartbeat time.Duration
}

// NewStream creates a stream writing to w for the request r.
//
// Prefer SSE, which also enforces the server's MaxStreams limit, closes the
// stream, and turns a returned error into the right response. A stream made
// with NewStream must be closed by the handler before it returns:
//
//	s := photon.NewStream(w, r)
//	defer s.Close()
//
// A stream created for a request served by a photon Server inherits that
// server's limits. One created outside a Server uses DefaultLimits.
func NewStream(w http.ResponseWriter, r *http.Request, opts ...StreamOption) *Stream {
	cfg := newStreamOptions(opts)
	if srv := serverFromContext(r); srv != nil {
		return srv.newRequestStream(w, r, cfg)
	}
	return newStandaloneStream(w, r, cfg)
}

// newStandaloneStream builds a stream with DefaultLimits, for a request that
// did not come through a Server. Having no server means no MaxStreams slot and
// no Grow budget, but the per-stream bounds - buffer cap and stall deadline -
// still hold.
func newStandaloneStream(w http.ResponseWriter, r *http.Request, cfg streamMiddlewareConfig) *Stream {
	d := DefaultLimits()
	c := streamConfig{
		Overflow:  cfg.overflow,
		Stall:     d.StreamStallTimeout,
		MaxBuffer: d.MaxStreamBufferBytes,
		Heartbeat: d.HeartbeatInterval,
	}
	if cfg.stall > 0 {
		c.Stall = cfg.stall
	}
	if cfg.maxBuf > 0 {
		c.MaxBuffer = cfg.maxBuf
	}
	return newStream(w, r, newFlusher(w), c)
}

// newStream builds a stream with an explicit flusher. Tests use this to inject
// a writer whose writes block, which is the only way to test back-pressure
// deterministically.
func newStream(w http.ResponseWriter, r *http.Request, f streamFlusher, cfg streamConfig) *Stream {
	s := &Stream{
		w: w, rc: f, r: r,
		policy:    cfg.Overflow,
		stall:     cfg.Stall,
		maxBuffer: cfg.MaxBuffer,
		heartbeat: cfg.Heartbeat,
		wake:      make(chan struct{}, 1),
		space:     make(chan struct{}, 1),
	}
	if s.maxBuffer <= 0 {
		s.maxBuffer = defaultStreamBufferBytes
	}
	if r != nil {
		s.srv = serverFromContext(r)
		s.ctxDone = r.Context().Done()
	}
	return s
}

// newRequestStream builds a stream for a request under the server's limits.
func (s *Server) newRequestStream(w http.ResponseWriter, r *http.Request, cfg streamMiddlewareConfig) *Stream {
	c := streamConfig{
		Overflow:  cfg.overflow,
		Stall:     s.limits.StreamStallTimeout,
		MaxBuffer: s.limits.MaxStreamBufferBytes,
		Heartbeat: s.limits.HeartbeatInterval,
	}
	if cfg.stall > 0 {
		c.Stall = cfg.stall
	}
	if cfg.maxBuf > 0 {
		c.MaxBuffer = cfg.maxBuf
	}
	st := newStream(w, r, newFlusher(w), c)
	st.srv = s
	return st
}

// serverKey is the context key for the server that dispatched a request, so
// NewStream can find the limits it must respect without the caller passing
// them.
type serverKey struct{}

// serverFromContext returns the server handling r, or nil.
func serverFromContext(r *http.Request) *Server {
	if r == nil {
		return nil
	}
	s, _ := r.Context().Value(serverKey{}).(*Server)
	return s
}

// withServer attaches s to r so a stream created from r inherits its limits.
// Requests that arrive through Serve already carry the server in their base
// context, so this only runs for direct ServeHTTP calls.
func withServer(r *http.Request, s *Server) *http.Request {
	if s == nil {
		return r
	}
	return r.WithContext(context.WithValue(r.Context(), serverKey{}, s))
}

// Overflow sets the policy for a full buffer. It must be called before the
// first write; after that the policy is fixed, and a later call is ignored and
// logged rather than changing behaviour mid-stream.
func (s *Stream) Overflow(p OverflowPolicy) *Stream {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.started {
		if s.srv != nil {
			s.srv.logger.Warn("Stream.Overflow called after the first write; ignored",
				"path", requestPathOf(s.r), "policy", p.String())
		}
		return s
	}
	s.policy = p
	return s
}

// Header returns the response headers, which can be changed until the first
// write commits them. A StreamFunc has no ResponseWriter, so this is where it
// sets a Content-Type for a non-SSE stream, a request id, or CORS headers.
func (s *Stream) Header() http.Header { return s.w.Header() }

// ---------------------------------------------------------------- SSE API

// Event writes one server-sent event with the given name and data.
//
// An empty name sends an unnamed event, which browsers deliver to onmessage
// and which is what OpenAI-style streaming APIs use. Data may contain anything:
// each line becomes its own "data:" field, splitting on CRLF, LF, and a lone CR
// exactly as the client will, so a line break inside model output cannot start
// a new field. The client receives the data with line breaks as "\n". Empty or
// nil data sends an event with empty data, which clients do dispatch.
//
// It returns ErrInvalidEvent if name contains a line break or NUL, and
// otherwise the error that ended the stream, if any: ErrClientGone,
// ErrStreamOverflow, or ErrStreamClosed. A non-nil error means stop producing.
func (s *Stream) Event(name string, data []byte) error {
	if !validField(name) {
		return ErrInvalidEvent
	}
	s.emitMu.Lock()
	defer s.emitMu.Unlock()
	s.scratch = appendEvent(s.scratch[:0], s.nextID, s.hasID, name, data)
	s.nextID, s.hasID = "", false
	_, err := s.emit(s.scratch, data)
	s.trimScratch()
	return err
}

// JSON writes one event whose data is v encoded as JSON.
//
//	s.JSON("token", map[string]string{"text": tok})
//	s.JSON("", chunk) // an OpenAI-style unnamed chunk
//
// The encoding never contains a raw line break, so the event is always a single
// data line. An encoding error is returned without ending the stream.
func (s *Stream) JSON(name string, v any) error {
	if !validField(name) {
		return ErrInvalidEvent
	}
	s.emitMu.Lock()
	defer s.emitMu.Unlock()
	s.encBuf.Reset()
	if s.enc == nil {
		s.enc = json.NewEncoder(&s.encBuf)
	}
	if err := s.enc.Encode(v); err != nil {
		return err
	}
	data := bytes.TrimSuffix(s.encBuf.Bytes(), []byte{'\n'})
	s.scratch = appendEvent(s.scratch[:0], s.nextID, s.hasID, name, data)
	s.nextID, s.hasID = "", false
	_, err := s.emit(s.scratch, data)
	s.trimScratch()
	if s.encBuf.Cap() > maxRetainedScratch {
		s.encBuf = bytes.Buffer{}
		s.enc = nil
	}
	return err
}

// Comment writes an SSE comment frame, ": text\n\n".
//
// Comments are ignored by EventSource and by every AI SDK, so a comment is the
// cheapest way to keep a connection alive through a proxy without changing
// what the client sees. Line breaks in text are replaced with spaces.
func (s *Stream) Comment(text string) error {
	text = strings.NewReplacer("\r", " ", "\n", " ").Replace(text)
	s.emitMu.Lock()
	defer s.emitMu.Unlock()
	s.scratch = append(append(append(s.scratch[:0], ": "...), text...), "\n\n"...)
	_, err := s.emit(s.scratch, nil)
	return err
}

// SetEventID sets the id of the next event, which is what lets a client
// resume with Last-Event-ID after a reconnect.
//
// The id is sent as part of the next Event or JSON call, in the same frame, so
// an event and its id cannot be separated - by a concurrent writer, or by
// DropOldest dropping one but not the other. It returns ErrInvalidEvent if id
// contains a line break or NUL.
func (s *Stream) SetEventID(id string) error {
	if !validField(id) {
		return ErrInvalidEvent
	}
	s.emitMu.Lock()
	defer s.emitMu.Unlock()
	s.mu.Lock()
	err := s.writableErrLocked()
	s.mu.Unlock()
	if err != nil {
		return err
	}
	s.nextID, s.hasID = id, true
	return nil
}

// SetRetry tells the client how long to wait before reconnecting if the
// connection drops. It is sent immediately as its own frame.
func (s *Stream) SetRetry(d time.Duration) error {
	if d < 0 {
		d = 0
	}
	s.emitMu.Lock()
	defer s.emitMu.Unlock()
	s.scratch = append(strconv.AppendInt(append(s.scratch[:0], "retry: "...), d.Milliseconds(), 10), "\n\n"...)
	_, err := s.emit(s.scratch, nil)
	return err
}

// Write implements io.Writer, so a Stream can be handed to anything that
// writes bytes: io.Copy from an upstream response, an NDJSON encoder, a
// template.
//
// Bytes written this way are sent as-is, not SSE-framed. Set the Content-Type
// before the first write for a non-SSE stream, for example
// w.Header().Set("Content-Type", "application/x-ndjson"); heartbeats are only
// sent on text/event-stream responses, so they cannot corrupt another format.
func (s *Stream) Write(p []byte) (int, error) {
	s.emitMu.Lock()
	defer s.emitMu.Unlock()
	if len(p) == 0 {
		s.mu.Lock()
		err := s.writableErrLocked()
		s.mu.Unlock()
		return 0, err
	}
	return s.emit(p, p)
}

// Flush commits the response headers if they are not yet sent and blocks until
// everything accepted so far has been written to the connection, or the
// stream fails.
//
// Events are sent promptly without it. It is useful to open a stream before a
// slow first event, so the client sees the response start and proxies see
// traffic, and in tests.
func (s *Stream) Flush() error {
	s.emitMu.Lock()
	defer s.emitMu.Unlock()
	if err := s.start(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	target := s.accepted
	s.needFlush = true
	s.signalWriter()
	var timer *time.Timer
	defer stopTimer(&timer)
	for {
		if s.err != nil {
			return s.err
		}
		if s.written+s.dropped >= target && !s.needFlush && s.inflight == 0 {
			return nil
		}
		if err := s.waitLocked(&timer); err != nil {
			return err
		}
	}
}

// Err returns the reason the stream ended, or nil if it has not ended or ended
// cleanly.
//
// ErrClientGone and ErrStreamOverflow are normal outcomes for a consumer that
// left or stopped reading, and should not be logged as server errors.
func (s *Stream) Err() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.err
}

// Done returns a channel that is closed when the handler should stop
// producing: the client disconnected, the stream failed or was closed, or the
// server began a graceful shutdown.
//
//	for {
//		select {
//		case <-s.Done():
//			return s.Event("bye", nil) // still writable during shutdown
//		case ev := <-feed:
//			if err := s.JSON("update", ev); err != nil {
//				return err
//			}
//		}
//	}
//
// During a shutdown the stream is still writable, so a handler can send a
// final event - a resume id, a "reconnect" hint - and return. Long-lived
// streams that watch Done let Shutdown finish early instead of waiting out its
// whole deadline.
func (s *Stream) Done() <-chan struct{} {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.doneCh != nil {
		return s.doneCh
	}
	parent := context.Background()
	if s.r != nil {
		parent = s.r.Context()
	}
	ctx, cancel := context.WithCancel(parent)
	s.doneCh, s.doneCancel = ctx.Done(), cancel
	if s.err != nil || s.closed {
		cancel()
	}
	if s.srv != nil {
		// Cancel on drain, and stop listening for drain once the stream's own
		// context ends, so a finished stream leaves nothing registered on the
		// server.
		stopDrain := context.AfterFunc(s.srv.drainCtx, cancel)
		context.AfterFunc(ctx, func() { stopDrain() })
	}
	return s.doneCh
}

// StartHeartbeats opens the stream now and keeps it alive while the handler is
// quiet, and returns a function that stops the heartbeats.
//
// Heartbeats are SSE comments, sent after HeartbeatInterval with nothing else
// written. Once a stream has started they are automatic on text/event-stream
// responses, so StartHeartbeats is only needed for one thing: keeping a
// connection alive BEFORE the first event, while a model is still thinking.
// Load balancers and CDNs commonly drop a connection idle for 30-60 seconds,
// and without this a slow first token looks to the client like a dead server.
//
// It commits the 200 status immediately, so call it after validating the
// request - once an error would no longer be an HTTP error response. The
// returned stop function is idempotent.
func (s *Stream) StartHeartbeats() (stop func()) {
	s.mu.Lock()
	s.hbOff = false
	s.mu.Unlock()
	// Open the stream now, on the handler's goroutine. Committing it later
	// from a timer goroutine would race the handler's own Header().Set calls
	// - a concurrent map write, which crashes the process - and could commit
	// a 200 in the instant the handler was about to answer with an error.
	_ = s.Flush()
	var once sync.Once
	return func() {
		once.Do(func() {
			s.mu.Lock()
			s.hbOff = true
			s.mu.Unlock()
		})
	}
}

// Close ends the stream, sending anything still buffered first.
//
// It is idempotent and safe to call from any goroutine. It returns the error
// that ended the stream, or nil for a clean end. A stream that never wrote
// anything sends nothing, so the handler can still write an error response.
func (s *Stream) Close() error {
	s.emitMu.Lock()
	defer s.emitMu.Unlock()

	s.mu.Lock()
	if s.closeCalled {
		err := s.err
		s.mu.Unlock()
		return err
	}
	s.closeCalled = true
	s.closed = true
	started := s.started
	writerDone := s.writerDone
	s.signalWriter()
	s.mu.Unlock()

	if started {
		s.awaitWriter(writerDone)
		s.mu.Lock()
		abandoned := s.abandoned
		s.mu.Unlock()
		// Clear the write deadline so a keep-alive connection is not left
		// holding one that would break the next request. After a forced
		// timeout the connection is broken anyway and is not reused.
		if !abandoned {
			_ = s.rc.SetWriteDeadline(time.Time{})
		}
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	// The buffer is dropped, never pooled: an abandoned writer may still be
	// reading it, and leaving it to the garbage collector is what makes that
	// safe.
	s.buf, s.frames = nil, nil
	if s.reserved > 0 && s.srv != nil {
		s.srv.releaseStreamBytes(s.reserved)
		s.reserved = 0
	}
	if s.doneCancel != nil {
		s.doneCancel()
	}
	return s.err
}

// awaitWriter waits for the writer goroutine to send what is buffered and exit.
//
// The wait is bounded by two stall periods - one for a write already in
// progress and one for the final drain - because a ResponseWriter whose Write
// never returns cannot be interrupted in Go. Past that the writer is abandoned:
// one parked goroutine, which MaxStreams already bounds.
func (s *Stream) awaitWriter(done <-chan struct{}) {
	select {
	case <-done:
		return // the common case: nothing was left to send
	default:
	}
	if s.stall <= 0 {
		<-done
		return
	}
	// Wait for as long as the client keeps making progress. A slow client
	// draining a full buffer at Close is still being served, and the handler
	// must stay alive while it is, because a writer that outlives the handler
	// writes into a response net/http has already finished: a data race on
	// HTTP/1, and a panic - a crashed process - on HTTP/2.
	t := time.NewTimer(s.stall)
	defer t.Stop()
	for {
		select {
		case <-done:
			return
		case <-s.space: // the writer completed a piece: progress
			t.Reset(s.stall)
		case <-t.C:
			select {
			case <-done: // finished just as the timer fired
				return
			default:
			}
			// No progress for a whole stall period. Make the write fail
			// instead of abandoning it: an expired deadline returns the
			// blocked Write with an error on both HTTP/1 and HTTP/2, and the
			// writer then exits. Only a ResponseWriter that supports no
			// deadline at all (a middleware without Unwrap, which is warned
			// about) can keep this waiting, and waiting is the safe failure.
			s.mu.Lock()
			s.abandoned = true
			s.failLocked(ErrStreamOverflow)
			s.mu.Unlock()
			if err := s.rc.SetWriteDeadline(time.Now()); errors.Is(err, http.ErrNotSupported) {
				// Nothing can interrupt this write: the ResponseWriter is a
				// wrapper that hides deadlines (warned about once per
				// server). Waiting forever would hang the handler, so give
				// it one more stall period and then leave it, loudly.
				t.Reset(s.stall)
				select {
				case <-done:
				case <-t.C:
					if s.srv != nil {
						s.srv.logger.Error("stream writer abandoned: a write never returned and the "+
							"ResponseWriter supports no deadline; add Unwrap to the middleware wrapping it",
							"path", requestPathOf(s.r))
					}
				}
				return
			}
			// With a deadline the blocked write returns promptly with an
			// error, so waiting for the writer cannot hang.
			<-done
			return
		}
	}
}

// ---------------------------------------------------------------- internals

// maxRetainedScratch is the largest frame buffer a stream keeps between
// events. One unusually large event should not pin its size for the rest of a
// long stream.
const maxRetainedScratch = 64 << 10

func (s *Stream) trimScratch() {
	if cap(s.scratch) > maxRetainedScratch {
		s.scratch = nil
	}
}

// start commits the response headers and starts the writer goroutine, once.
// emitMu must be held.
//
// The headers are set here, on the handler's goroutine, and the writer only
// exists afterwards, so the ResponseWriter is never touched by two goroutines
// at once. They reach the wire with the first batch of data, in one write.
func (s *Stream) start() error {
	s.mu.Lock()
	if s.started || s.closed {
		err := s.writableErrLocked()
		s.mu.Unlock()
		return err
	}
	s.mu.Unlock()

	h := s.w.Header()
	ct := h.Get("Content-Type")
	if ct == "" {
		ct = "text/event-stream; charset=utf-8"
		h.Set("Content-Type", ct)
	}
	if h.Get("Cache-Control") == "" {
		h.Set("Cache-Control", "no-cache, no-transform")
	}
	// nginx buffers responses by default, and buffered token streaming looks
	// to the client like a hang followed by a burst.
	h.Set("X-Accel-Buffering", "no")
	// A streamed response has no length, and one set by a middleware would
	// make the client stop reading early.
	h.Del("Content-Length")
	// Connection is meaningful only in HTTP/1.x. In HTTP/2 and HTTP/3 it is a
	// connection-specific header that RFC 9113 forbids in a response.
	if s.r == nil || s.r.ProtoMajor < 2 {
		h.Set("Connection", "keep-alive")
	}
	s.w.WriteHeader(http.StatusOK)

	s.mu.Lock()
	s.started = true
	s.sse = strings.HasPrefix(strings.ToLower(ct), "text/event-stream")
	s.limit = s.maxBuffer
	s.buf = make([]byte, 0, min(s.maxBuffer, initialStreamBuffer))
	s.writerDone = make(chan struct{})
	s.mu.Unlock()
	go s.writeLoop()
	return nil
}

// emit hands one frame to the writer and returns how many bytes of it were
// accepted. emitMu must be held. sample is the payload, for the text classifier
// behind the DropOldest warning.
//
// A frame that fits is appended whole. A frame larger than the entire buffer
// is fed through in pieces, waiting for the writer between them; it stays
// contiguous on the wire because emitMu keeps every other producer out until
// it is done.
func (s *Stream) emit(frame, sample []byte) (int, error) {
	if err := s.start(); err != nil {
		return 0, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.policy == DropOldest && sample != nil {
		s.noteText(sample)
	}
	var timer *time.Timer
	defer stopTimer(&timer)

	p, sent := frame, 0
	for {
		if err := s.writableErrLocked(); err != nil {
			s.midFrame = false
			return sent, err
		}
		free := s.limit - len(s.buf)
		if len(p) <= free {
			kind := frameWhole
			if s.midFrame {
				kind = frameLast
			}
			s.appendLocked(p, kind)
			s.midFrame = false
			s.signalWriter()
			return len(frame), nil
		}
		switch {
		case len(p) > s.limit:
			// Larger than the whole buffer: it can never fit at once, so send
			// what fits and wait for the writer to make room for the rest.
			if free > 0 {
				s.appendLocked(p[:free], framePart)
				p, sent = p[free:], sent+free
				s.midFrame = true
				s.signalWriter()
			}
		case s.policy == DropOldest && !s.midFrame:
			if s.dropOldestLocked(len(p) - free) {
				continue
			}
		case s.policy == Grow:
			if s.growLocked(len(p)) {
				continue
			}
		}
		if err := s.waitLocked(&timer); err != nil {
			s.midFrame = false
			return sent, err
		}
	}
}

// appendLocked appends p to the buffer, growing its capacity toward the limit
// as needed, and records frame boundaries for DropOldest.
func (s *Stream) appendLocked(p []byte, kind int) {
	if need := len(s.buf) + len(p); need > cap(s.buf) {
		newCap := max(2*cap(s.buf), need, initialStreamBuffer)
		newCap = min(newCap, max(s.limit, need))
		grown := make([]byte, len(s.buf), newCap)
		// The writer may be reading buf[:inflight] from the old array; that
		// is safe, because the old array is never written again and the copy
		// carries the same prefix.
		copy(grown, s.buf)
		s.buf = grown
	}
	s.buf = append(s.buf, p...)
	s.accepted += int64(len(p))
	if s.policy == DropOldest && kind != framePart {
		s.frames = append(s.frames, frameRec{end: len(s.buf), droppable: kind == frameWhole})
	}
}

// waitLocked waits for the writer to free space, with s.mu held on entry and
// exit. timer bounds the time without progress: it starts on the first wait
// and restarts whenever the writer completes a write, so a slow consumer that
// keeps reading is never mistaken for a stuck one.
func (s *Stream) waitLocked(timer **time.Timer) error {
	var tc <-chan time.Time
	if s.stall > 0 {
		if *timer == nil {
			*timer = time.NewTimer(s.stall)
		}
		tc = (*timer).C
	}
	writerDone := s.writerDone
	s.mu.Unlock()
	select {
	case <-s.space:
		s.mu.Lock()
		if *timer != nil {
			(*timer).Reset(s.stall)
		}
		return nil
	case <-tc:
		s.mu.Lock()
		// The consumer accepted nothing for a whole stall period. If a write
		// is still in progress it is stuck; the writer is abandoned rather
		// than waited for, because a goroutine blocked in Write cannot be
		// interrupted.
		if s.inflight > 0 {
			s.abandoned = true
		}
		s.failLocked(ErrStreamOverflow)
		return s.err
	case <-writerDone:
		s.mu.Lock()
		return s.writableErrLocked()
	case <-s.ctxDone:
		s.mu.Lock()
		s.contextEndedLocked()
		return s.err
	}
}

// dropOldestLocked frees at least need bytes by discarding the oldest whole
// events that are waiting to be sent. It reports whether it succeeded; it
// drops nothing if it cannot free enough, because losing data and still having
// to wait would be the worst of both policies.
//
// Only frames entirely past the in-flight prefix are candidates: a frame the
// writer has started sending cannot be recalled, and cutting its tail would
// corrupt every event after it. Frames fed through in pieces are never dropped
// for the same reason.
func (s *Stream) dropOldestLocked(need int) bool {
	frameStart := func(k int) int {
		if k == 0 {
			return 0
		}
		return s.frames[k-1].end
	}
	first := 0
	for first < len(s.frames) && frameStart(first) < s.inflight {
		first++
	}
	// Decide what to drop before touching anything.
	freed, last := 0, -1
	for k := first; k < len(s.frames) && freed < need; k++ {
		if s.frames[k].droppable {
			freed += s.frames[k].end - frameStart(k)
			last = k
		}
	}
	if freed < need {
		return false
	}
	// Compact in one pass: keep every frame from first on except the
	// droppable ones up to last, sliding the kept ones down.
	dst := frameStart(first)
	kept := s.frames[:first]
	for k := first; k < len(s.frames); k++ {
		from, to := frameStart(k), s.frames[k].end
		if k <= last && s.frames[k].droppable {
			continue
		}
		dst += copy(s.buf[dst:], s.buf[from:to])
		kept = append(kept, frameRec{end: dst, droppable: s.frames[k].droppable})
	}
	// Bytes after the last recorded frame - a piece of an oversized frame in
	// progress - move down with everything else.
	tail := 0
	if n := len(s.frames); n > 0 {
		tail = s.frames[n-1].end
	}
	dst += copy(s.buf[dst:], s.buf[tail:])
	s.dropped += int64(len(s.buf) - dst)
	s.buf = s.buf[:dst]
	s.frames = kept

	if s.wroteText && !s.warnedDrop {
		s.warnedDrop = true
		// The warning names the risk rather than the fact, because "I dropped
		// your tokens" is not actionable and "this corrupts text streams" is.
		// It is logged once per stream, not per drop, so a diagnostic does not
		// become a log flood.
		if s.srv != nil {
			s.srv.logger.Warn("DropOldest dropped data from a text stream; this can corrupt output",
				"path", requestPathOf(s.r),
				"risk", "dropped tokens read as a hallucination; the client cannot tell the difference",
				"hint", "use Disconnect for text, and DropOldest only for metrics and progress")
		}
	}
	return true
}

// growLocked raises the buffer cap for a frame of size need, drawing the extra
// from the server's global stream budget. It reports whether it grew.
func (s *Stream) growLocked(need int) bool {
	if s.srv == nil {
		// No server means no global budget, and growing without one is the
		// unbounded growth the invariant forbids.
		return false
	}
	ceiling := s.srv.streamBufferCeiling()
	newLimit := max(2*s.limit, len(s.buf)+need)
	newLimit = min(newLimit, ceiling)
	if newLimit <= s.limit || newLimit < len(s.buf)+need {
		return false
	}
	extra := int64(newLimit - s.limit)
	if !s.srv.reserveStreamBytes(extra) {
		return false
	}
	s.reserved += extra
	s.limit = newLimit
	s.grewPast = true
	return true
}

// writableErrLocked returns the reason the stream cannot be written, or nil.
func (s *Stream) writableErrLocked() error {
	if s.err != nil {
		return s.err
	}
	if s.closed {
		return ErrStreamClosed
	}
	select {
	case <-s.ctxDone:
		s.contextEndedLocked()
		return s.err
	default:
	}
	return nil
}

// contextEndedLocked records that the request's context ended, with s.mu held
// on entry and exit.
//
// The context ends when the client disconnects, but also when one of our own
// writes fails: net/http cancels it from inside the failing Write, before the
// writer goroutine has recorded why the write failed. If a write is in flight,
// its outcome is the better explanation - a write deadline expiring means the
// client stopped reading (ErrStreamOverflow), not that it left - so the writer
// is given a moment to report it first.
func (s *Stream) contextEndedLocked() {
	if s.err == nil && s.inflight > 0 && s.writerDone != nil {
		done := s.writerDone
		s.mu.Unlock()
		t := time.NewTimer(time.Second)
		select {
		case <-done:
		case <-t.C:
		}
		t.Stop()
		s.mu.Lock()
	}
	s.failLocked(ErrClientGone)
}

// failLocked ends the stream with err. The first error is kept: it is the
// cause, and anything after it is a consequence.
func (s *Stream) failLocked(err error) {
	if s.err == nil {
		s.err = err
	}
	s.closed = true
	s.signalWriter()
	if s.doneCancel != nil {
		s.doneCancel()
	}
}

// signalWriter wakes the writer goroutine without blocking.
func (s *Stream) signalWriter() {
	select {
	case s.wake <- struct{}{}:
	default:
	}
}

// writeLoop is the stream's writer goroutine. It sends whatever is buffered in
// one write and flush, then waits for more. Coalescing falls out of this
// naturally: events that arrive while a write is in progress go out together
// in the next one, and an event that arrives while the writer is idle goes out
// immediately.
func (s *Stream) writeLoop() {
	defer close(s.writerDone)
	var hbC <-chan time.Time
	var hb *time.Timer
	if s.heartbeat > 0 {
		hb = time.NewTimer(s.heartbeat)
		hbC = hb.C
		defer hb.Stop()
	}
	for {
		s.mu.Lock()
		for len(s.buf) == 0 && !s.needFlush {
			if s.err != nil || s.closed {
				s.mu.Unlock()
				return
			}
			s.mu.Unlock()
			select {
			case <-s.wake:
			case <-hbC:
				s.mu.Lock()
				if s.sse && !s.hbOff && !s.midFrame && len(s.buf) == 0 && s.err == nil && !s.closed {
					s.appendLocked(heartbeatFrame, frameWhole)
				}
				s.mu.Unlock()
				hb.Reset(s.heartbeat)
			case <-s.ctxDone:
				s.mu.Lock()
				s.failLocked(ErrClientGone)
				s.mu.Unlock()
				return
			}
			s.mu.Lock()
		}
		if s.err != nil {
			s.mu.Unlock()
			return
		}
		// Take at most half the buffer per write. A client that stops
		// reading then holds at most half of it in a stuck write, and the
		// other half stays available to the producer - which is what lets
		// DropOldest keep accepting new events instead of blocking behind
		// bytes it can no longer recall.
		n := min(len(s.buf), max(s.limit/2, 1))
		out := s.buf[:n:n]
		s.inflight = n
		s.needFlush = false
		s.mu.Unlock()

		err := s.writeOut(out)

		s.mu.Lock()
		s.inflight = 0
		if s.err != nil {
			// The stream was failed while this write was in progress - most
			// likely abandoned on a stall. Its buffer may already be gone.
			s.mu.Unlock()
			return
		}
		if err != nil {
			s.failLocked(translateWriteError(err))
			s.mu.Unlock()
			return
		}
		rest := copy(s.buf, s.buf[n:])
		s.buf = s.buf[:rest]
		s.shiftFramesLocked(n)
		s.written += int64(n)
		s.mu.Unlock()

		select {
		case s.space <- struct{}{}:
		default:
		}
		if hb != nil {
			hb.Reset(s.heartbeat)
		}
	}
}

// shiftFramesLocked discards frame records for the n bytes just written and
// rebases the rest.
func (s *Stream) shiftFramesLocked(n int) {
	if len(s.frames) == 0 {
		return
	}
	i := 0
	for i < len(s.frames) && s.frames[i].end <= n {
		i++
	}
	// The write may have ended inside a frame - batches are capped at half
	// the buffer, not cut at frame boundaries. Part of that frame is now on
	// the wire, so dropping the rest would leave half an event that corrupts
	// every event after it.
	if i < len(s.frames) {
		start := 0
		if i > 0 {
			start = s.frames[i-1].end
		}
		if start < n {
			s.frames[i].droppable = false
		}
	}
	rest := s.frames[i:]
	for k := range rest {
		rest[k].end -= n
	}
	s.frames = append(s.frames[:0], rest...)
}

// writeOut writes p and flushes it to the client. The write deadline is set
// first, so a client that stops reading cannot hold the writer longer than one
// stall period: a socket write with a deadline returns an error when it
// expires, which is what turns "the client stopped reading" into a bounded
// event rather than a parked goroutine.
//
// The batch is written in pieces of progressChunk, with the deadline renewed
// before each one. A single deadline over a 32 KB write would disconnect a
// client that is reading steadily but slowly - 2 KB/s needs 16 s for 32 KB -
// which is exactly the client back-pressure exists to serve. Renewing it per
// piece makes the rule what the documentation says: the stream ends when the
// client accepts nothing for a whole StreamStallTimeout, not when one large
// write takes a while.
func (s *Stream) writeOut(p []byte) error {
	for {
		if s.stall > 0 {
			if err := s.rc.SetWriteDeadline(time.Now().Add(s.stall)); errors.Is(err, http.ErrNotSupported) {
				s.warnNoDeadline()
			}
		}
		piece := p[:min(len(p), progressChunk)]
		if len(piece) > 0 {
			n, err := s.w.Write(piece)
			if err != nil {
				return err
			}
			if n < len(piece) {
				return io.ErrShortWrite
			}
		}
		p = p[len(piece):]
		if len(p) == 0 {
			return s.rc.Flush()
		}
		// Stop early if the stream was failed meanwhile - by Close giving up
		// on a stalled client, or by the producer's stall timer.
		s.mu.Lock()
		failed := s.err != nil
		s.mu.Unlock()
		if failed {
			return ErrStreamOverflow
		}
		// Tell a producer waiting for space that the client is making
		// progress, so its own stall clock restarts too. No space is freed
		// until the batch completes, so the producer simply checks and waits
		// again - but it no longer mistakes a long write for a stuck client.
		select {
		case s.space <- struct{}{}:
		default:
		}
	}
}

// warnNoDeadline logs, once per server, that streams cannot set a socket write
// deadline. That happens when a middleware wraps the ResponseWriter without an
// Unwrap method: Flush may still work, but a client that stops reading is then
// only detected by the producer's stall timer, and the writer goroutine stays
// blocked until the connection dies. The fix is one method on the wrapper.
func (s *Stream) warnNoDeadline() {
	if s.srv != nil && s.srv.warnedNoDeadline.CompareAndSwap(false, true) {
		s.srv.logger.Warn("streams cannot set write deadlines: a middleware wraps the ResponseWriter "+
			"without an Unwrap() http.ResponseWriter method",
			"path", requestPathOf(s.r),
			"fix", "add Unwrap to the wrapper so http.ResponseController can reach the connection")
	}
}

// progressChunk is how many bytes must be accepted within one stall period
// for a client to count as making progress: 8 KB per StreamStallTimeout, about
// 270 bytes a second at the 30 s default. It also matches the size net/http's
// own connection buffer writes through in, so splitting costs no extra system
// calls.
const progressChunk = 8 << 10

// translateWriteError maps a transport error onto photon's vocabulary.
//
// A write to a disconnected client surfaces as a broken pipe, a reset, or a
// "use of closed network connection", depending on the OS. All of them mean
// the consumer is gone and the upstream work should stop.
func translateWriteError(err error) error {
	switch {
	case err == nil:
		return nil
	case errors.Is(err, ErrClientGone), errors.Is(err, ErrStreamOverflow):
		return err
	case errors.Is(err, http.ErrNotSupported):
		// Not a client problem: a middleware wrapped the ResponseWriter in a
		// type that hides http.Flusher, so nothing can be streamed.
		return fmt.Errorf("photon: the ResponseWriter cannot flush; a middleware is wrapping it "+
			"without an Unwrap method, so the stream cannot be delivered incrementally: %w", err)
	case errors.Is(err, context.DeadlineExceeded), isTimeout(err):
		return ErrStreamOverflow
	default:
		return fmt.Errorf("%w: %v", ErrClientGone, err)
	}
}

// isTimeout reports whether err is a network timeout.
func isTimeout(err error) bool {
	var t interface{ Timeout() bool }
	if errors.As(err, &t) {
		return t.Timeout()
	}
	return false
}

// stopTimer stops *t if it was started.
func stopTimer(t **time.Timer) {
	if *t != nil {
		(*t).Stop()
	}
}

// noteText records whether the stream has carried text-like data, which is
// what makes DropOldest's corruption risk concrete. A dropped metric is a stale
// metric; a dropped sentence is a lie.
func (s *Stream) noteText(p []byte) {
	if s.wroteText {
		return
	}
	// Sample the head only: the first bytes decide the payload type, and
	// classifying a whole token stream would cost more than the check is worth.
	head := p
	if len(head) > 64 {
		head = head[:64]
	}
	if !utf8.Valid(head) {
		return
	}
	printable, total := 0, 0
	for _, r := range string(head) {
		total++
		if unicode.IsPrint(r) || unicode.IsSpace(r) {
			printable++
		}
	}
	// Mostly printable means text. Erring toward "text" makes the warning fire
	// more often, and a spurious warning is cheap next to a silent corruption.
	if total > 0 && printable*2 >= total {
		s.wroteText = true
	}
}

// ---------------------------------------------------------------- SSE framing

// validField reports whether v can be sent as an SSE field value. A CR or LF
// would end the field early and let the rest be parsed as a new field; a NUL
// makes clients ignore an id.
func validField(v string) bool {
	return !strings.ContainsAny(v, "\r\n\x00")
}

// appendEvent appends one SSE frame to dst.
//
// Data is split into lines exactly as the client splits them - on CRLF, LF,
// and a lone CR - and every line, including an empty one, becomes a "data:"
// field. That is what makes the encoding lossless: the client joins the lines
// with "\n" and gets the payload back, a trailing newline included, and no
// byte of the payload can begin a new field.
func appendEvent(dst []byte, id string, hasID bool, name string, data []byte) []byte {
	if hasID {
		dst = append(append(append(dst, "id: "...), id...), '\n')
	}
	if name != "" {
		dst = append(append(append(dst, "event: "...), name...), '\n')
	}
	for {
		i := bytes.IndexAny(data, "\r\n")
		if i < 0 {
			dst = append(append(append(dst, "data: "...), data...), '\n')
			break
		}
		dst = append(append(append(dst, "data: "...), data[:i]...), '\n')
		if data[i] == '\r' && i+1 < len(data) && data[i+1] == '\n' {
			i++
		}
		data = data[i+1:]
	}
	return append(dst, '\n')
}

// requestPathOf returns r's raw path, or "" for a nil request.
func requestPathOf(r *http.Request) string {
	if r == nil {
		return ""
	}
	return requestPath(r)
}

var _ io.Writer = (*Stream)(nil)
