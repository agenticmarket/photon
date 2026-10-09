package main

import "sync/atomic"

// Tenant is one customer of this server.
//
// It holds identity and live counters, never a secret. A token belongs in the
// configuration map that resolves *to* a tenant, not on the tenant itself, so
// there is no path by which a struct representing a caller can be logged and
// leak the credential.
type Tenant struct {
	// Name identifies the tenant in logs and metrics. It is the only thing about
	// a tenant that is ever written down.
	Name string

	// inFlight is the number of streams this tenant currently has open.
	//
	// Atomic rather than a mutex because it is read and written on the hot path
	// of every request, and contention on a single counter would serialise
	// exactly the requests that are supposed to run concurrently.
	inFlight atomic.Int64

	// rpm is the request-rate window state for this tenant.
	rpm rpmWindow
}

// tenantID is the context key under which a resolved tenant is stored.
type tenantKey struct{}

// rpmWindow is a fixed-window rate counter.
//
// Fixed rather than sliding: a sliding window needs per-request timestamps and a
// heap, which on a hot path is a real cost for a difference that only shows up at
// the boundary. The known consequence of a fixed window is that a caller can
// send 2x the limit across a window boundary, and that is stated in the README
// rather than hidden. A sliding window is the right answer for a plan with a
// hard billing ceiling; this is a fairness control, not a billing meter.
type rpmWindow struct {
	count atomic.Int64
	// resetUnixNano is when the window rolls over. Written rarely, read on every
	// request, so it is atomic rather than a plain field.
	resetUnixNano atomic.Int64
}

// allow reports whether a request fits in the current window, consuming a slot
// when it does.
func (w *rpmWindow) allow(nowNS, limit int64) bool {
	if limit <= 0 {
		return true // no limit configured
	}
	reset := w.resetUnixNano.Load()
	if nowNS > reset {
		// Start a new window. Two requests can race here and both reset, which
		// costs at most one extra request in the new window. The alternative is a
		// mutex, held on every request, to make a bound that is already
		// approximate.
		w.count.Store(0)
		w.resetUnixNano.Store(nowNS + int64(windowNS))
		return w.count.Add(1) <= limit
	}
	return w.count.Add(1) <= limit
}

// remaining reports how many requests are left in the current window.
//
// Reported in the RateLimit headers so a client can back off deliberately rather
// than by guessing from 429s.
func (w *rpmWindow) remaining(nowNS, limit int64) int64 {
	if limit <= 0 {
		return -1 // unlimited
	}
	if nowNS > w.resetUnixNano.Load() {
		return limit
	}
	if left := limit - w.count.Load(); left > 0 {
		return left
	}
	return 0
}

// windowNS is the rate-limit window, one minute.
const windowNS = int64(60 * 1e9)

// AcquireStream reserves a concurrency slot for this tenant.
//
// It returns false when the tenant is already at its limit. The check and the
// increment are not atomic together, so two concurrent requests can both pass the
// check and both increment - which is a limit exceeded by one. Making it exact
// would need a compare-and-swap loop, and an exact answer is not worth a
// contended CAS on every stream. The overshoot is one request per tenant, and the
// comment belongs here so nobody later "fixes" it into something slower believing
// it is a bug.
func (t *Tenant) AcquireStream(limit int64) bool {
	if limit <= 0 {
		t.inFlight.Add(1)
		return true
	}
	if t.inFlight.Add(1) > limit {
		// Roll the increment back. Without this a client that keeps retrying
		// would drive the counter arbitrarily high and, once the limit was raised,
		// no request would be admitted for as long as the counter stayed above it.
		t.inFlight.Add(-1)
		return false
	}
	return true
}

// ReleaseStream returns a concurrency slot.
//
// Always paired with AcquireStream by a defer at the call site. That pairing is
// the whole reason this counter cannot leak: a panic, an early return, or a
// client disconnect all unwind through the defer, and the tests check it.
func (t *Tenant) ReleaseStream() {
	if n := t.inFlight.Add(-1); n < 0 {
		// A negative count means ReleaseStream ran more times than AcquireStream.
		// That is a bug in this program, not a client problem, and it is clamped
		// rather than left to drift: a counter that goes negative and stays there
		// silently disables the limit for that tenant.
		t.inFlight.Store(0)
	}
}

// InFlight reports the tenant's current stream count, for metrics and logs.
func (t *Tenant) InFlight() int64 { return t.inFlight.Load() }
