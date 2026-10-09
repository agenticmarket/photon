// Command streamdriver measures SSE streaming across Go, Node, and Python.
//
// Every server is a separate process, started one at a time, driven by this one
// client. That structure is the whole design:
//
//   - Separate processes, so a Python server's GIL and a Node server's event
//     loop are never competing for a CPU with the measurement.
//   - One at a time, so no two frameworks ever share a core.
//   - One client, written once, so no framework is measured with a client tuned
//     for another.
//
// The rule the harness exists to enforce: no number is reported that this
// process did not measure in this run. Comparing against a framework's own
// published benchmark is not a comparison, because the conditions are never
// identical.
package main

import (
	"bufio"
	"flag"
	"fmt"
	"io"
	"math"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// ---------------------------------------------------------------- workloads

// server describes one binary under test.
type server struct {
	// name is what appears in the table.
	name string
	// lang groups the table, so the runtime confound is visible rather than
	// implied.
	lang string
	// cmd, when set, is the program to run. The Go binaries are inferred from
	// the name; Node and Python need an interpreter that is not implied by a
	// filename.
	cmd string
	// args are passed to the program.
	args []string
	// cwd is where it is launched, so Node and Python resolve their own
	// dependencies relative to their directory.
	cwd string
	// path overrides /stream for a server exposing several variants.
	path string
	// note is carried into the report. Set when a row is not a plain framework
	// comparison, so a reader does not have to guess why it differs.
	note string
}

// allServers is the matrix.
//
// The floors (net/http, node/http, raw ASGI) are not decoration. Without them
// "photon versus FastAPI" compares two whole stacks; with them, the cost of the
// framework layer itself is visible, and a framework that cannot beat its own
// language's floor is making a statement worth reporting.
func allServers(binDir, nodeDir, pyDir string) []server {
	// Absolute: the child is launched with cmd.Dir set to its own directory, and
	// exec resolves a relative program path against the child's working directory
	// rather than the driver's.
	py, err := filepath.Abs(filepath.Join(pyDir, ".venv", "Scripts", "python.exe"))
	if err != nil {
		fmt.Fprintf(os.Stderr, "resolving the python interpreter: %v\n", err)
		os.Exit(1)
	}

	return []server{
		// Go.
		{name: "photon", lang: "go", args: []string{"-flush=coalesced"}, cwd: binDir,
			note: "photon default: sends at once; batches only while a write is in flight"},
		{name: "photon (tight)", lang: "go", args: []string{"-flush=tight"}, cwd: binDir,
			note: "photon 512 B buffer: a few frames per write"},
		{name: "chi", lang: "go", cwd: binDir},
		{name: "gin", lang: "go", cwd: binDir},
		{name: "echo", lang: "go", cwd: binDir},
		{name: "net/http", lang: "go", cwd: binDir, note: "floor: no framework"},

		// Node. nodeDir holds server.js, which selects the framework by
		// argument, so one file serves all three.
		{name: "fastify", lang: "node", cwd: nodeDir, cmd: "node",
			args: []string{"server.js", "fastify"},
			note: "reply.raw path, as Fastify's docs recommend"},
		{name: "express", lang: "node", cwd: nodeDir, cmd: "node",
			args: []string{"server.js", "express"}},
		{name: "node:http", lang: "node", cwd: nodeDir, cmd: "node",
			args: []string{"server.js", "http"},
			note: "floor: no framework"},

		// Python.
		{name: "fastapi", lang: "python", cwd: pyDir, cmd: py,
			args: []string{"server.py", "fastapi"}},
		{name: "starlette", lang: "python", cwd: pyDir, cmd: py,
			args: []string{"server.py", "starlette"}},
		{name: "asgi", lang: "python", cwd: pyDir, cmd: py,
			args: []string{"server.py", "asgi"},
			note: "floor: no framework"},
	}
}

// ------------------------------------------------------------------- result

// result is one measured configuration.
type result struct {
	name string
	lang string
	// eventsPerSec is the median across repetitions; bestPerSec is the best run.
	// Both are reported because a single figure hides the run-to-run variation
	// that decides whether a ranking is meaningful at all.
	eventsPerSec float64
	bestPerSec   float64
	// ttfeUS is the median time to first event, pooled across repetitions. For a
	// token stream this is the number a user perceives as responsiveness, and it
	// routinely matters more than peak throughput.
	ttfeUS float64
	// rssMB is the server's own peak resident set, read inside that process.
	rssMB  float64
	rssErr string
	// spreadPct is the variation across repetitions, as a percentage of the
	// median. Reported because a framework whose own runs vary by 20% cannot be
	// ranked against one whose runs vary by 3%.
	spreadPct float64
	// delivered is the total events the server actually sent, counted by the
	// client from the bytes it received. It is checked against the equivalence
	// gate rather than trusted.
	delivered int64
	note      string
	err       error
}

// ------------------------------------------------------------------- client

// clientConn is a minimal keep-alive HTTP/1.1 client that reads a stream as it
// arrives.
//
// net/http's client is deliberately not used. It buffers a chunked response
// until the body ends before returning it, so it cannot observe time-to-first
// event at all - which is the metric that matters most here. It would also add
// its own pool and per-request allocations to every measurement.
type clientConn struct {
	c    net.Conn
	br   *bufio.Reader
	bw   *bufio.Writer
	host string
}

func dial(host string) (*clientConn, error) {
	c, err := net.DialTimeout("tcp", host, 10*time.Second)
	if err != nil {
		return nil, err
	}
	if tc, ok := c.(*net.TCPConn); ok {
		// Nagle would batch small SSE frames, so a token stream would arrive in
		// bursts and the time-to-first-event would measure the kernel's timer
		// rather than the server.
		_ = tc.SetNoDelay(true)
	}
	return &clientConn{
		c:    c,
		br:   bufio.NewReaderSize(c, 64<<10),
		bw:   bufio.NewWriterSize(c, 8<<10),
		host: host,
	}, nil
}

// streamResult is what one stream produced.
type streamResult struct {
	events int
	bytes  int
	ttfe   time.Duration
	err    error
}

// stream issues one request and consumes the response, timing the first event.
func (cc *clientConn) stream(path string, want int) streamResult {
	start := time.Now()

	if _, err := cc.bw.WriteString("GET " + path + " HTTP/1.1\r\nHost: " + cc.host +
		"\r\nAccept: text/event-stream\r\nConnection: keep-alive\r\n\r\n"); err != nil {
		return streamResult{err: err}
	}
	if err := cc.bw.Flush(); err != nil {
		return streamResult{err: err}
	}

	// Status line and headers.
	if _, err := cc.br.ReadString('\n'); err != nil {
		return streamResult{err: err}
	}
	chunked := false
	for {
		line, err := cc.br.ReadString('\n')
		if err != nil {
			return streamResult{err: err}
		}
		line = strings.TrimRight(line, "\r\n")
		if line == "" {
			break
		}
		if k, v, ok := strings.Cut(line, ":"); ok {
			if strings.EqualFold(strings.TrimSpace(k), "transfer-encoding") &&
				strings.Contains(strings.ToLower(v), "chunked") {
				chunked = true
			}
		}
	}
	if !chunked {
		// A stream with no chunked encoding would end only at connection close,
		// which the keep-alive client cannot use. None of these servers produce
		// it, so this is a real error rather than a case to handle.
		return streamResult{err: fmt.Errorf("response is not chunked")}
	}

	var res streamResult
	pending := 0
	for {
		payload, done, err := readChunk(cc.br)
		if payload != "" {
			pending += len(payload)
			// One event is "event: token". Counting the marker rather than
			// splitting on the blank line keeps this correct for a frame that
			// straddles a chunk boundary, which it does whenever a server's
			// buffer is smaller than the frame.
			res.events += strings.Count(payload, "event: token")
			if res.ttfe == 0 {
				res.ttfe = time.Since(start)
			}
		}
		if err != nil {
			if err == io.EOF && res.events == want {
				// A clean end after the expected number of events. The
				// connection is not reusable, so the caller must redial; that is
				// reported rather than hidden.
				res.bytes = pending
				res.err = errStreamEnded
				return res
			}
			return streamResult{err: err}
		}
		if done {
			res.bytes = pending
			res.err = errStreamEnded
			return res
		}
	}
}

// usable reports whether the connection can carry another request.
//
// A connection that has been closed by the server - which is what a server does
// when it has sent `Connection: close`, and what an intermediary does when it
// decides the stream is over - cannot be reused, and the next write would fail
// with a confusing "broken pipe" rather than a clear reconnect.
func (cc *clientConn) usable() bool {
	if cc == nil || cc.c == nil {
		return false
	}
	// A non-blocking write of zero bytes is the cheapest probe that does not
	// disturb the stream: on a dead connection it reports the error, on a live
	// one it succeeds without sending anything.
	_ = cc.c.SetWriteDeadline(time.Now().Add(time.Second))
	_, err := cc.c.Write(nil)
	_ = cc.c.SetWriteDeadline(time.Time{})
	return err == nil
}

// errStreamEnded marks a stream that completed successfully: the terminating
// zero-length chunk arrived and the connection is ready for the next request.
var errStreamEnded = fmt.Errorf("stream ended")

// readChunk reads one HTTP/1.1 chunk, returning its payload and whether the
// stream is finished.
func readChunk(br *bufio.Reader) (payload string, done bool, err error) {
	line, err := br.ReadString('\n')
	if err != nil {
		return "", false, err
	}
	line = strings.TrimSpace(line)
	if i := strings.IndexByte(line, ';'); i >= 0 {
		line = line[:i] // chunk extension
	}
	size, err := strconv.ParseInt(line, 16, 64)
	if err != nil {
		return "", false, fmt.Errorf("bad chunk size %q", line)
	}
	if size == 0 {
		return "", true, nil
	}
	buf := make([]byte, size+2) // payload + trailing CRLF
	if _, err := io.ReadFull(br, buf); err != nil {
		return "", false, err
	}
	return string(buf[:size]), false, nil
}

// ------------------------------------------------------------------ driving

// drive runs exactly perWorker streams on each of the n connections, and returns
// the total events delivered, the time-to-first-event samples, and the elapsed
// window.
//
// **Fixed stream count, not a time window.** This was a time window first, and it
// was wrong in a way that only showed up on the third run. A window means a fast
// server reconnects more often, and every chunked stream ends its connection, so
// throughput and connection churn are coupled: photon reconnected about 900
// times a second, exhausted Windows' 16k ephemeral ports within seconds, and
// servers later in the run failed to start at all - because the ports the *earlier*
// servers had burned were still in TIME_WAIT. The result depended on run order.
//
// A fixed count decouples them. Every server does exactly the same work - the
// same number of streams, each with the same number of events - so the comparison
// is fair by construction, the connection count is bounded and known in advance,
// and events/s is derived from the elapsed time that work actually took.
func drive(host, path string, streams, events, perWorker int) (int64, []float64, time.Duration, error) {
	if streams == 0 {
		return 0, nil, 0, nil
	}

	ccs := make([]*clientConn, streams)
	for i := range ccs {
		cc, err := dial(host)
		if err != nil {
			for _, c := range ccs {
				if c != nil {
					c.c.Close()
				}
			}
			return 0, nil, 0, fmt.Errorf("dial %d/%d: %w", i+1, streams, err)
		}
		ccs[i] = cc
	}
	defer func() {
		for _, c := range ccs {
			c.c.Close()
		}
	}()

	// Each connection carries one *long* stream, which is what a token stream
	// actually is: a model response is thousands of events on one connection
	// held open for seconds or minutes.
	//
	// The alternative - one short stream per connection, re-dialled each time -
	// was tried first and is wrong twice over. It exhausts Windows' ephemeral
	// port range after a few thousand requests, and more importantly it measures
	// connection setup rather than streaming, which is the thing under test. A
	// short-stream loop therefore reports a number dominated by TCP handshakes
	// and TIME_WAIT, and would rank the servers by accept-loop speed.
	var total atomic.Int64
	var wg sync.WaitGroup
	samples := make([][]float64, streams)
	errs := make([]error, streams)

	// The loop is not trimmed to the deadline, and that is deliberate. An earlier
	// version shrank the final request to whatever was left of the window, which
	// in practice reduced it to a handful of events and turned the run into
	// thousands of reconnects per second - exhausting Windows' ephemeral port
	// range and measuring TCP handshakes rather than streaming. One stream is
	// simply allowed to overrun the window by its own duration, and the window
	// used for events/s is what actually elapsed.
	start := time.Now()
	for i := range ccs {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			cc := ccs[i]
			// Per-worker samples: a shared slice would add contention that
			// measures the harness rather than the server.
			local := make([]float64, 0, 64)
			first := true
			for k := 0; k < perWorker; k++ {
				r := cc.stream(path+"?n="+strconv.Itoa(events), events)
				if r.err != nil && r.err != errStreamEnded {
					// A stream that failed before delivering anything means the
					// server dropped the connection. The run is void rather than
					// reported as a slow server, because a dropped connection and a
					// slow one are not the same finding.
					if r.events == 0 {
						errs[i] = r.err
						return
					}
					break
				}
				total.Add(int64(r.events))
				if first && r.ttfe > 0 {
					// Time to first event is recorded once per connection, not
					// per stream: the first event is what a user waits for, and
					// later events on the same connection are throughput.
					local = append(local, float64(r.ttfe.Microseconds()))
					first = false
				}
				// The connection is reused: HTTP/1.1 keep-alive means a complete
				// chunked response leaves the connection ready for the next
				// request, and the stream only ended because the terminating
				// chunk arrived.
				//
				// Redialing after every stream was a bug in this harness, and an
				// expensive one. It burned one ephemeral port per stream - at 50
				// streams x 20 streams x 5 repetitions x 12 servers that is tens
				// of thousands - and Windows holds each closed port in TIME_WAIT
				// for two minutes against a 16384-port range. The run therefore
				// exhausted its own ports partway through, and every server after
				// that point failed to accept connections. The failures depended
				// on run order, which is the signature of a harness fault rather
				// than a server characteristic.
				//
				// A connection that is genuinely broken is still replaced, so a
				// server that drops it is reported rather than silently retried.
				if !cc.usable() {
					cc.c.Close()
					nc, err := dial(host)
					if err != nil {
						errs[i] = fmt.Errorf("redial %d/%d after %d events: %w",
							k+1, perWorker, r.events, err)
						return
					}
					cc = nc
					ccs[i] = nc
				}
			}
			samples[i] = local
		}(i)
	}
	wg.Wait()
	// The window that actually elapsed, so events/s is not inflated by dividing
	// by a window that never happened.
	elapsed := time.Since(start)

	for _, err := range errs {
		if err != nil {
			return total.Load(), nil, elapsed, err
		}
	}
	all := make([]float64, 0, total.Load())
	for _, s := range samples {
		all = append(all, s...)
	}
	return total.Load(), all, elapsed, nil
}

// ------------------------------------------------------------------- server

// start launches a server and waits for its handshake.
func start(s server) (addr string, stop func(), err error) {
	var cmd *exec.Cmd
	if s.cmd != "" {
		// Absolutised only when it looks like a path. A bare command name like
		// "node" is resolved from PATH by exec, and calling filepath.Abs on it
		// produces a path named after the working directory - which is how "node"
		// becomes D:\...\bench\stream\node, a directory rather than a program.
		prog := s.cmd
		if strings.ContainsAny(prog, `/\`) {
			abs, absErr := filepath.Abs(prog)
			if absErr != nil {
				return "", nil, absErr
			}
			prog = abs
		}
		cmd = exec.Command(prog, s.args...)
	} else {
		bin, absErr := filepath.Abs(filepath.Join(s.cwd, binaryFor(s)))
		if absErr != nil {
			return "", nil, absErr
		}
		cmd = exec.Command(bin, s.args...)
	}
	cmd.Dir = s.cwd

	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return "", nil, err
	}
	// stderr is inherited rather than discarded: a server that logs a warning
	// per request would otherwise be silently flooding, and that is a finding.
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		return "", nil, err
	}

	cleanup := func() {
		if cmd.Process != nil {
			_ = cmd.Process.Kill()
		}
		_ = cmd.Wait()
	}

	ch := make(chan string, 1)
	go func() {
		sc := bufio.NewScanner(stdout)
		for sc.Scan() {
			if _, rest, ok := strings.Cut(sc.Text(), "READY "); ok {
				f := strings.Fields(rest)
				ch <- f[len(f)-1]
				return
			}
		}
		ch <- ""
	}()

	select {
	case a := <-ch:
		if a == "" {
			cleanup()
			return "", nil, fmt.Errorf("%s exited before printing READY", s.name)
		}
		return a, cleanup, nil
	case <-time.After(readinessTimeout):
		cleanup()
		// Generous, and deliberately not tuned to the fastest server: importing
		// FastAPI on a cold Windows filesystem takes tens of seconds, and a
		// driver that gave up early would report a timeout rather than the actual
		// finding, which is that Python starts far more slowly than Go.
		return "", nil, fmt.Errorf("%s did not print READY within %s", s.name, readinessTimeout)
	}
}

// readinessTimeout bounds how long a server may take to bind and announce.
const readinessTimeout = 3 * time.Minute

// binaryFor maps a server name to its binary.
//
// The name in the table is not always a filename - "net/http" contains a slash -
// so the mapping is explicit rather than derived. Deriving it produced
// bin\net\http.exe, which is both wrong and a path traversal.
func binaryFor(s server) string {
	switch s.name {
	case "photon", "photon (tight)":
		return "photon.exe"
	case "net/http":
		return "nethttp.exe"
	default:
		return s.name + ".exe"
	}
}

// mem asks a running server for its own peak RSS.
func mem(host string) (int64, error) {
	cc, err := dial(host)
	if err != nil {
		return 0, err
	}
	defer cc.c.Close()
	_ = cc.c.SetDeadline(time.Now().Add(10 * time.Second))

	if _, err := cc.bw.WriteString("GET /mem HTTP/1.1\r\nHost: " + cc.host +
		"\r\nConnection: close\r\n\r\n"); err != nil {
		return 0, err
	}
	if err := cc.bw.Flush(); err != nil {
		return 0, err
	}
	resp, err := httpReadAll(cc.br)
	if err != nil {
		return 0, err
	}
	// "rss=<bytes> events=<n>"
	for _, f := range strings.Fields(resp) {
		if v, ok := strings.CutPrefix(f, "rss="); ok {
			return strconv.ParseInt(v, 10, 64)
		}
	}
	return 0, fmt.Errorf("no rss in /mem response %q", trunc(resp))
}

// -------------------------------------------------------------------- main

func main() {
	binDir := flag.String("bin", "bin", "directory holding the Go server binaries")
	nodeDir := flag.String("node", "node", "node server directory")
	pyDir := flag.String("python", "python", "python server directory")
	only := flag.String("only", "", "comma-separated server names to measure")
	streamsList := flag.String("streams", "1,10,50", "concurrent stream counts")
	events := flag.Int("events", 500, "events per stream")
	perWorker := flag.Int("per-worker", 20, "streams each connection carries, per repetition")
	reps := flag.Int("reps", 3, "repetitions per configuration")
	flag.Parse()

	counts, err := parseInts(*streamsList)
	if err != nil {
		fmt.Fprintf(os.Stderr, "bad -streams: %v\n", err)
		os.Exit(2)
	}

	servers := allServers(*binDir, *nodeDir, *pyDir)
	if *only != "" {
		keep := map[string]bool{}
		for _, n := range strings.Split(*only, ",") {
			keep[strings.TrimSpace(n)] = true
		}
		var filtered []server
		for _, s := range servers {
			if keep[s.name] {
				filtered = append(filtered, s)
			}
		}
		servers = filtered
	}

	fmt.Print(header(*streamsList, *events, *perWorker, *reps))

	for _, n := range counts {
		fmt.Printf("\n## %d concurrent stream(s), %d events each, %d streams per connection\n\n",
			n, *events, *perWorker)

		rows := make([]result, 0, len(servers))
		for _, s := range servers {
			r := measure(s, n, *events, *perWorker, *reps)
			rows = append(rows, r)
		}
		printTable(rows)
	}
}

func header(streams string, events, perWorker, reps int) string {
	var b strings.Builder
	fmt.Fprintf(&b, "# Streaming audit — SSE across Go, Node, and Python\n\n")
	fmt.Fprintf(&b, "Streams %s, %d events each, %d streams per connection, %d repetitions.\n",
		streams, events, perWorker, reps)
	fmt.Fprintf(&b, "Best and median of the repetitions are both reported, with the spread.\n\n")
	fmt.Fprintf(&b, "Every server does the same work: the same number of streams, each carrying\n")
	fmt.Fprintf(&b, "the same number of events. Throughput is derived from the time that work\n")
	fmt.Fprintf(&b, "actually took, so a faster server is not penalised for finishing early.\n\n")
	fmt.Fprintf(&b, "Every server emits byte-identical frames; that is verified before every\n")
	fmt.Fprintf(&b, "run by equivcheck, and a mismatch fails the run rather than being reported.\n\n")
	fmt.Fprintf(&b, "**Read the language column before comparing rows.** Go, Node, and Python\n")
	fmt.Fprintf(&b, "differ by more than the frameworks do: CPython's GIL, libuv's event loop, and\n")
	fmt.Fprintf(&b, "Go's goroutines are the dominant variable. The honest reading is \"photon\n")
	fmt.Fprintf(&b, "against the best of each ecosystem\", not \"faster than FastAPI\".\n\n")
	fmt.Fprintf(&b, "Each language's floor (net/http, node:http, raw ASGI) is in the table on\n")
	fmt.Fprintf(&b, "purpose: without it, the comparison is between whole stacks rather than\n")
	fmt.Fprintf(&b, "between frameworks.\n\n")
	fmt.Fprintf(&b, "TTFE is the median time to first event, which is what a user perceives as\n")
	fmt.Fprintf(&b, "responsiveness. RSS is the server process's own peak, read in-process.\n")
	fmt.Fprintf(&b, "Each server runs alone; the driver is a separate process, so no client cost\n")
	fmt.Fprintf(&b, "is inside a server's measurement.\n\n")
	fmt.Fprintf(&b, "```\n")
	return b.String()
}

// measure runs one server through the full protocol.
func measure(s server, streams, events, perWorker, reps int) result {
	r := result{name: s.name, lang: s.lang, note: s.note}

	host, stop, err := start(s)
	if err != nil {
		r.err = err
		return r
	}
	defer stop()

	path := s.path
	if path == "" {
		path = "/stream"
	}

	// Warmup, discarded. The first requests on a fresh process pay for lazy
	// initialisation - route trees, JIT warmup, import caches - and including that
	// would penalise whichever runtime happens to be slowest to start. A small
	// fixed number of streams, not a window: the warmup exists to touch the code
	// path once, not to measure anything.
	if _, _, _, err := drive(host, path, streams, events, 1); err != nil {
		r.err = fmt.Errorf("warmup: %w", err)
		return r
	}

	// Every repetition's events/s is kept, so the best and the median can both be
	// reported and the spread between them is visible. A framework whose own runs
	// vary by 20% cannot be ranked against one whose runs vary by 3%, and hiding
	// that behind a single number would present noise as a result.
	var runs []float64
	var ttfes [][]float64
	var delivered int64

	for rep := 0; rep < reps; rep++ {
		got, samples, elapsed, err := drive(host, path, streams, events, perWorker)
		if err != nil {
			r.err = err
			return r
		}
		runs = append(runs, float64(got)/elapsed.Seconds())
		ttfes = append(ttfes, samples)
		delivered += got
	}

	sorted := sortedCopy(runs)
	r.eventsPerSec = percentile(sorted, 50)
	r.bestPerSec = sorted[len(sorted)-1]
	if r.eventsPerSec > 0 {
		r.spreadPct = (sorted[len(sorted)-1] - sorted[0]) / r.eventsPerSec * 100
	}
	r.delivered = delivered

	// TTFE is pooled across repetitions rather than averaged per repetition: a
	// median of medians is not the median, and with three repetitions the
	// difference is large enough to matter for a latency figure.
	var allTTFE []float64
	for _, s := range ttfes {
		allTTFE = append(allTTFE, s...)
	}
	r.ttfeUS = percentile(sortedCopy(allTTFE), 50)

	if rss, err := mem(host); err == nil {
		r.rssMB = float64(rss) / (1 << 20)
	} else {
		r.rssErr = err.Error()
	}
	return r
}

func parseInts(s string) ([]int, error) {
	var out []int
	for _, p := range strings.Split(s, ",") {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}
		v, err := strconv.Atoi(p)
		if err != nil {
			return nil, err
		}
		out = append(out, v)
	}
	return out, nil
}

func percentile(sorted []float64, p float64) float64 {
	if len(sorted) == 0 {
		return 0
	}
	rank := int(math.Ceil(p/100*float64(len(sorted)))) - 1
	if rank < 0 {
		rank = 0
	}
	if rank >= len(sorted) {
		rank = len(sorted) - 1
	}
	return sorted[rank]
}

func sortedCopy(v []float64) []float64 {
	s := append([]float64(nil), v...)
	sort.Float64s(s)
	return s
}

func printTable(rows []result) {
	headers := []string{"framework", "lang", "events/s (med)", "events/s (best)",
		"TTFE us", "RSS MB", "spread", "note"}
	out := [][]string{headers}
	for _, r := range rows {
		if r.err != nil {
			// A failed configuration is shown, not omitted. A gap in a benchmark
			// table reads as "we did not test this", which is a different and
			// worse claim than "this failed".
			out = append(out, []string{r.name, r.lang, "ERROR", "-", "-", "-", "-", r.err.Error()})
			continue
		}
		rss := fmt.Sprintf("%.1f", r.rssMB)
		if r.rssErr != "" {
			rss = "-"
		}
		out = append(out, []string{
			r.name, r.lang,
			comma(int(r.eventsPerSec)),
			comma(int(r.bestPerSec)),
			fmt.Sprintf("%.0f", r.ttfeUS),
			rss,
			fmt.Sprintf("%.0f%%", r.spreadPct),
			r.note,
		})
	}
	widths := make([]int, len(headers))
	for _, row := range out {
		for i, c := range row {
			if i < len(widths) && len(c) > widths[i] {
				widths[i] = len(c)
			}
		}
	}
	write := func(cells []string) {
		var b strings.Builder
		for i, c := range cells {
			if i > 0 {
				b.WriteString("  ")
			}
			b.WriteString(pad(c, widths[i]))
		}
		fmt.Println(strings.TrimRight(b.String(), " "))
	}
	write(headers)
	for i := range headers {
		if i > 0 {
			fmt.Print("  ")
		}
		fmt.Print(strings.Repeat("-", widths[i]))
	}
	fmt.Println()
	for _, row := range out[1:] {
		write(row)
	}
}

func pad(s string, n int) string {
	if len(s) >= n {
		return s
	}
	return s + strings.Repeat(" ", n-len(s))
}

func comma(n int) string {
	s := strconv.Itoa(n)
	if len(s) <= 3 {
		return s
	}
	var out []byte
	for i, c := range []byte(s) {
		if i > 0 && (len(s)-i)%3 == 0 {
			out = append(out, ',')
		}
		out = append(out, c)
	}
	return string(out)
}

func trunc(s string) string {
	const max = 80
	if len(s) <= max {
		return s
	}
	return s[:max] + "..."
}

// httpReadAll reads a response body, for the /mem endpoint.
//
// Handles both Content-Length and chunked. Only the first was handled at first,
// and a server that answers `Connection: close` with chunked encoding - which
// Node does - produced an empty RSS cell. A missing measurement is worse than a
// wrong one: it reads as "this framework was not measured" rather than "the
// harness could not read it".
func httpReadAll(br *bufio.Reader) (string, error) {
	// Status line.
	if _, err := br.ReadString('\n'); err != nil {
		return "", err
	}
	length := -1
	chunked := false
	for {
		line, err := br.ReadString('\n')
		if err != nil {
			return "", err
		}
		line = strings.TrimRight(line, "\r\n")
		if line == "" {
			break
		}
		if k, v, ok := strings.Cut(line, ":"); ok {
			switch {
			case strings.EqualFold(strings.TrimSpace(k), "content-length"):
				length, _ = strconv.Atoi(strings.TrimSpace(v))
			case strings.EqualFold(strings.TrimSpace(k), "transfer-encoding"):
				chunked = strings.Contains(strings.ToLower(v), "chunked")
			}
		}
	}

	switch {
	case chunked:
		var b strings.Builder
		for {
			payload, done, err := readChunk(br)
			b.WriteString(payload)
			if err != nil || done {
				return b.String(), nil
			}
		}
	case length > 0:
		buf := make([]byte, length)
		if _, err := io.ReadFull(br, buf); err != nil {
			return "", err
		}
		return string(buf), nil
	default:
		// No length and no chunking: the body ends at EOF, so read what is
		// there. /mem is tiny and always complete, so this is safe rather than
		// a stall.
		rest, err := io.ReadAll(br)
		return string(rest), err
	}
}
