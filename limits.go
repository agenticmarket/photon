package photon

import (
	"math"
	"runtime/debug"
)

// Limits is the server's resource budget. Every field bounds something an
// attacker can grow, and every default is safe.
//
// A single struct is used rather than a long list of options because these
// limits are one cohesive object a deployment has to reason about as a whole.
// Setting them independently is how a server ends up configured to accept more
// work than it can hold.
//
// A Limits value is copied into the Server at construction and is read-only
// afterwards, so it can be shared and mutated by the caller without racing the
// running server.
type Limits struct {
	// MaxRequestBodyBytes bounds a request body. Zero means unlimited, which is
	// almost never what an internet-facing server wants.
	//
	// It applies to every request before routing, so a route can lower it
	// (wrap r.Body in http.MaxBytesReader) but not raise it: set it to the
	// largest body any route accepts.
	//
	// This is a rejection threshold, not a memory cost. A body is read as a
	// stream, so a 10 MB upload costs about the same memory as a 10 KB one;
	// the limit bounds how much work a single request can ask for, not how much
	// memory the server holds.
	MaxRequestBodyBytes int64

	// MaxHeaderBytes bounds the size of the request headers. net/http enforces
	// this from the first byte it reads, and its own default is 1 MB, which
	// across 10,000 connections is 10 GB of header buffer.
	MaxHeaderBytes int

	// MaxHeaders bounds the number of header fields.
	//
	// This is the limit net/http has no concept of, and it is the one that
	// closes the "50,000 tiny headers" attack: a request with thousands of
	// small headers is well under any byte limit while still costing real CPU
	// to parse, hash, and store.
	MaxHeaders int

	// MaxHeaderValueBytes bounds a single header value. A limit on total bytes
	// alone still permits one 8 KB cookie or Authorization header.
	MaxHeaderValueBytes int

	// ReadHeaderTimeout bounds how long a client may take to send its request
	// headers. This is the Slowloris defence: without it, a client can hold a
	// connection and a goroutine open indefinitely by trickling bytes.
	ReadHeaderTimeout Duration

	// IdleTimeout bounds how long an idle keep-alive connection is held open.
	// It bounds file-descriptor retention, which is the most common slow leak
	// in a long-running server.
	IdleTimeout Duration

	// ReadTimeout bounds the whole request, headers and body together.
	//
	// It is zero by default, and that is deliberate. A whole-request deadline
	// is what makes a five-minute AI response impossible: the deadline is
	// absolute, so it fires mid-stream no matter how healthy the connection
	// is. Bounding the body read is the opt-in per-route answer.
	ReadTimeout Duration

	// MaxConnections bounds concurrent accepted connections. Zero means
	// unlimited, which is bounded in practice by the process file-descriptor
	// limit rather than by us.
	//
	// Admission is checked before a request is read, so a flood is refused
	// before it can consume a read buffer.
	MaxConnections int

	// MaxConcurrentRequests bounds handlers running at once. Unlike
	// MaxConnections, which counts idle sockets too, this counts work.
	MaxConcurrentRequests int

	// MaxPerIP bounds concurrent connections from one address, 0 for no limit.
	//
	// It is enforced against the connection's peer address, never against a
	// forwarded header: a header is set by the client, so trusting it would let
	// anyone bypass the limit by inventing an address.
	MaxPerIP int

	// MaxStreams bounds concurrent streaming responses, 0 for the derived
	// default.
	//
	// This is the limit that makes streaming memory bounded. Each stream costs
	// a goroutine, a buffer, and a connection, so a client that opens many
	// streams would otherwise allocate one buffer per stream, unbounded. Photon
	// derives the default from the detected memory budget rather than from
	// GOMAXPROCS, because a 2-core edge box and a 64-core container have the
	// same problem and it is memory, not CPU.
	//
	// The derived default is used when this is 0. MemoryBudget overrides the
	// detection.
	MaxStreams int

	// MaxStreamBufferBytes is the per-stream buffer cap.
	//
	// When full, a write blocks applying TCP back-pressure to the producer,
	// up to StreamStallTimeout, and only then does the overflow policy apply.
	// The natural consequence of a slow client is therefore a slow producer,
	// not a growing buffer.
	//
	// The default is 64 KB. The 1 MiB request body limit is not the right
	// reference here: a stream's buffer is per concurrent stream and every one
	// of them is resident at once, so the number that matters is the product.
	MaxStreamBufferBytes int

	// MaxTotalStreamBufferBytes is the **global** ceiling on stream buffer
	// memory, 0 for the derived default.
	//
	// It is DERIVED from MaxStreams times MaxStreamBufferBytes rather than
	// configured independently, which is what makes the common
	// misconfiguration - a high stream count and a forgotten cap - structurally
	// impossible rather than merely discouraged.
	//
	// Setting it below the product of the count and the per-stream size is
	// rejected by validate, because a cap the count already exceeds is a
	// contradiction rather than a stricter policy.
	MaxTotalStreamBufferBytes int64

	// StreamStallTimeout bounds how long a write blocks on a full buffer
	// before the overflow policy applies.
	//
	// The first stage of the overflow policy always blocks; this is the deadline on
	// that block. It is what makes blocking safe: a blocking write with a
	// deadline cannot be an unbounded resource hold, whereas the same write
	// without one is exactly the exhaustion vector the buffer exists to
	// prevent.
	//
	// The default is 30 s, which covers a network hiccup, a phone changing
	// network, or a client pausing, with room to spare.
	StreamStallTimeout Duration

	// HeartbeatInterval is how often a stream may go without writing before
	// the server sends an SSE comment frame.
	//
	// Zero disables heartbeats. The default is 15 s, below the 30-60 s idle
	// timeouts typical of load balancers and CDNs. Comment frames are ignored
	// by EventSource and by every AI SDK, so a heartbeat costs one small
	// write and changes nothing the client sees.
	HeartbeatInterval Duration

	// MemoryBudget is the memory ceiling used to derive MaxStreams, 0 to
	// detect it.
	//
	// Detection order is GOMEMLIMIT, then the cgroup limit, then MemTotal, and
	// a 256 MB fallback with a logged warning. It exists so a deployment can
	// state its budget explicitly rather than hoping the heuristic is right.
	MemoryBudget int64
}

// DefaultLimits returns the limits a server uses when nothing is configured.
//
// They are chosen for a small internet-facing deployment and are secure without
// being hostile to legitimate traffic. Raise them deliberately rather than by
// reflex: every one of them is a limit an attacker would like removed.
func DefaultLimits() Limits {
	return Limits{
		// 1 MiB. AI prompts and API JSON fit comfortably; a service that takes
		// uploads raises it, and routes that should accept less lower it with
		// http.MaxBytesReader.
		MaxRequestBodyBytes: 1 << 20,

		// 16 KB, against net/http's 1 MB.
		MaxHeaderBytes: 16 << 10,

		// net/http has no count limit at all.
		MaxHeaders: 100,

		// 8 KB, so no single header can consume a quarter of the budget.
		MaxHeaderValueBytes: 8 << 10,

		ReadHeaderTimeout: 5 * Second,
		IdleTimeout:       60 * Second,
		// ReadTimeout stays zero: a whole-request deadline breaks long-lived
		// streaming responses, which is the workload this server exists for.
		ReadTimeout: 0,

		// Both are zero, meaning unbounded by us. A deployment sets them from
		// its own memory budget; a 64 MB box and a 64-core machine do not have
		// the same capacity, and the process rlimit is the real backstop.
		MaxConnections:        0,
		MaxConcurrentRequests: 0,
		MaxPerIP:              0,

		// MaxStreams is 0 here and derived by New. It is a function of the
		// host's memory, so baking a number into DefaultLimits would be a lie
		// on any machine but this one. See deriveMaxStreams.
		MaxStreams:                0,
		MaxStreamBufferBytes:      64 << 10,
		MaxTotalStreamBufferBytes: 0, // derived
		StreamStallTimeout:        30 * Second,
		HeartbeatInterval:         15 * Second,
		MemoryBudget:              0, // detected
	}
}

// streamResidentBytes is the memory one active stream is assumed to cost.
//
// Photon budgets 256 KB per stream: the 64 KB buffer, the goroutine stack,
// the Stream itself, and the connection's write buffer. Using the buffer size
// alone would understate the real cost by roughly 4x and advertise four times
// as many streams as the host can hold.
const streamResidentBytes = 256 << 10

// streamBudgetFraction is the share of the memory budget a deployment is
// willing to give to streams.
//
// A quarter, leaving the rest for the process, the ordinary request path, and
// the GC's own headroom. A streaming server that allocates its whole budget to
// streams dies under normal load.
const streamBudgetFraction = 0.25

// maxStreamsFloor and maxStreamsCeiling bound the derived value.
//
// The floor exists so a tiny machine still serves a few streams rather than
// none. The ceiling exists so a large machine does not advertise tens of
// thousands of streams on the strength of a detection heuristic: 8192 is high
// enough for any realistic deployment, and a host that needs more should raise
// it deliberately rather than inherit it from a guess.
const (
	maxStreamsFloor   = 64
	maxStreamsCeiling = 8192
)

// deriveMaxStreams computes MaxStreams from a memory budget.
//
// The budget is a quarter of the total, divided by the per-stream resident
// cost, clamped. This is the whole of the scaling rule, and the reason
// the global buffer cap can be derived rather than configured: a stream count
// that follows from the budget cannot be set independently of it.
func deriveMaxStreams(budget int64) int {
	if budget <= 0 {
		budget = fallbackMemoryBudget
	}
	streams := int(float64(budget) * streamBudgetFraction / streamResidentBytes)
	if streams < maxStreamsFloor {
		return maxStreamsFloor
	}
	if streams > maxStreamsCeiling {
		return maxStreamsCeiling
	}
	return streams
}

// fallbackMemoryBudget is used when detection finds nothing.
//
// A quarter of 256 MB, which on a host with no cgroup limit and no GOMEMLIMIT
// is a guess. It is logged as a warning by New rather than used silently,
// because a limit derived from a guess should not look like a measured one.
const fallbackMemoryBudget = 256 << 20

// detectMemoryBudget finds the memory ceiling to scale from.
//
// The order is deliberate: an explicit limit first, then GOMEMLIMIT, then the
// cgroup limit, then the machine's total. GOMEMLIMIT comes before physical
// memory because a container with a 512 MB limit on a 64 GB host is the
// common case, and scaling from the host's RAM would advertise 64 GB worth of
// streams into 512 MB.
func detectMemoryBudget() (int64, string) {
	if v := readGOMEMLIMIT(); v > 0 {
		return v, "GOMEMLIMIT"
	}
	if v := readCgroupLimit(); v > 0 {
		return v, "cgroup"
	}
	if v := readMemTotal(); v > 0 {
		return v, "MemTotal"
	}
	return fallbackMemoryBudget, "fallback"
}

// readGOMEMLIMIT returns the Go runtime's soft memory limit, 0 if none is set.
//
// The runtime is asked rather than the environment variable parsed again: it
// has already parsed GOMEMLIMIT in every format Go accepts ("512MiB", "1GiB",
// a bare byte count), and it also reflects a limit the program set itself with
// debug.SetMemoryLimit. SetMemoryLimit(-1) reads the limit without changing it.
func readGOMEMLIMIT() int64 {
	v := debug.SetMemoryLimit(-1)
	if v <= 0 || v == math.MaxInt64 {
		return 0
	}
	return v
}
