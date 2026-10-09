// Command photonbench is the Photon performance audit harness.
//
// Every framework under test is served by this one process, on this one
// machine, against the same listener configuration, driven by the same client
// with the same connection count. Servers run one at a time, so no two
// frameworks ever compete for the same CPU.
//
// The rule this file exists to enforce: no number is reported that was not
// produced by this harness in this run. Comparing against a framework's own
// published benchmark is not a comparison, because the conditions are never
// identical.
package main

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"net"
	"net/http"
	"os"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// ---------------------------------------------------------------- workloads

// workload is one named request pattern. Every framework serves the identical
// set, so the only difference measured is the framework itself.
type workload struct {
	name   string
	method string
	target string
	body   []byte
}

// payload builds n bytes of a JSON object, so the response size is exact and
// the payload is realistic rather than a run of one character.
func payload(n int) []byte {
	if n <= 0 {
		return nil
	}
	filler := strings.Repeat("x", max(n-9, 0))
	return []byte(`{"d":"` + filler + `"}`)
}

// jsonBody builds a JSON object of exactly n bytes for request bodies.
func jsonBody(n int) []byte { return payload(n) }

// bytesReader returns a reader over b, or nil when there is no body, so
// httptest.NewRequest produces a bodyless request where the workload has none.
func bytesReader(b []byte) io.Reader {
	if b == nil {
		return nil
	}
	return bytes.NewReader(b)
}

// ------------------------------------------------------------ measurement

// result is one measured run.
type result struct {
	framework string
	workload  string
	conns     int
	rps       float64
	lat       latency
	allocs    float64
	bytesOp   float64
	requests  int
}

type latency struct {
	p50, p90, p95, p99, p999, max, min float64 // microseconds
}

func (l latency) String() string {
	return fmt.Sprintf("%.1f/%.1f/%.1f/%.1f/%.1f/%.1f",
		l.p50, l.p90, l.p95, l.p99, l.p999, l.max)
}

// percentile returns the p-th percentile of a sorted slice by nearest rank. It
// never interpolates a value that was not observed.
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

func summarise(samples []float64) latency {
	if len(samples) == 0 {
		return latency{}
	}
	sort.Float64s(samples)
	return latency{
		min:  samples[0],
		max:  samples[len(samples)-1],
		p50:  percentile(samples, 50),
		p90:  percentile(samples, 90),
		p95:  percentile(samples, 95),
		p99:  percentile(samples, 99),
		p999: percentile(samples, 99.9),
	}
}

// ------------------------------------------------------------------- client

// clientConn is a minimal keep-alive HTTP/1.1 client.
//
// net/http's client is deliberately not used: its connection pool, transport
// settings, and per-request allocations would be measured alongside the
// server. A hand-written client keeps the measurement about the server and
// removes a variable that is not under test.
type clientConn struct {
	c    net.Conn
	br   *bufio.Reader
	bw   *bufio.Writer
	host string
}

func dial(host string) (*clientConn, error) {
	c, err := net.Dial("tcp", host)
	if err != nil {
		return nil, err
	}
	if tc, ok := c.(*net.TCPConn); ok {
		// Nagle would batch small writes, adding latency that belongs to the
		// client's kernel settings rather than to the server.
		_ = tc.SetNoDelay(true)
	}
	return &clientConn{
		c:    c,
		br:   bufio.NewReaderSize(c, 32<<10),
		bw:   bufio.NewWriterSize(c, 4<<10),
		host: host,
	}, nil
}

// do issues one request on a reused connection and drains the response,
// returning the latency in microseconds.
func (cc *clientConn) do(method, target string, body []byte) (float64, error) {
	start := time.Now()

	if _, err := cc.bw.WriteString(method + " " + target + " HTTP/1.1\r\nHost: " + cc.host +
		"\r\nConnection: keep-alive\r\n"); err != nil {
		return 0, err
	}
	if body != nil {
		if _, err := cc.bw.WriteString("Content-Length: " +
			strconv.Itoa(len(body)) + "\r\n\r\n"); err != nil {
			return 0, err
		}
		if _, err := cc.bw.Write(body); err != nil {
			return 0, err
		}
	} else if _, err := cc.bw.WriteString("\r\n"); err != nil {
		return 0, err
	}
	if err := cc.bw.Flush(); err != nil {
		return 0, err
	}
	if err := cc.readResponse(); err != nil {
		return 0, err
	}
	return float64(time.Since(start).Microseconds()), nil
}

// readResponse parses the status line and headers, then discards the body.
func (cc *clientConn) readResponse() error {
	line, err := cc.br.ReadString('\n')
	if err != nil {
		return err
	}
	// "HTTP/1.1 200 OK" — the version length is not fixed in principle, so the
	// status code is located after the first space rather than at a constant
	// index. A fixed index silently misparses "HTTP/2 200".
	proto, rest, ok := strings.Cut(line, " ")
	if !ok || !strings.HasPrefix(proto, "HTTP/") {
		return fmt.Errorf("photonbench: malformed status line %q", strings.TrimSpace(line))
	}
	status, tail, ok := strings.Cut(rest, " ")
	if !ok {
		status = rest
		tail = ""
	}
	code, err := strconv.Atoi(strings.TrimSpace(status))
	if err != nil {
		return fmt.Errorf("photonbench: malformed status %q in %q", status, strings.TrimSpace(line))
	}
	_ = tail

	contentLength := -1
	chunked := false
	for {
		h, err := cc.br.ReadString('\n')
		if err != nil {
			return err
		}
		if h == "\r\n" || h == "\n" {
			break
		}
		k, v, ok := strings.Cut(h, ":")
		if !ok {
			continue
		}
		switch strings.ToLower(strings.TrimSpace(k)) {
		case "content-length":
			contentLength, _ = strconv.Atoi(strings.TrimSpace(v))
		case "transfer-encoding":
			if strings.Contains(strings.ToLower(v), "chunked") {
				chunked = true
			}
		}
	}

	switch {
	case code == 204 || code == 304 || (code >= 100 && code < 200):
		return nil
	case chunked:
		return cc.discardChunked()
	case contentLength > 0:
		_, err := io.CopyN(io.Discard, cc.br, int64(contentLength))
		return err
	default:
		// Neither Content-Length nor chunked: the body would end only when the
		// connection closes. None of the workloads produce this, so it is an
		// error rather than a silent stall.
		return errors.New("photonbench: response has no Content-Length and no chunked encoding")
	}
}

func (cc *clientConn) discardChunked() error {
	for {
		line, err := cc.br.ReadString('\n')
		if err != nil {
			return err
		}
		size, err := strconv.ParseInt(strings.TrimSpace(line), 16, 64)
		if err != nil {
			return err
		}
		if size == 0 {
			for { // trailers, then a blank line
				t, err := cc.br.ReadString('\n')
				if err != nil {
					return err
				}
				if t == "\r\n" || t == "\n" {
					return nil
				}
			}
		}
		if _, err := io.CopyN(io.Discard, cc.br, size+2); err != nil {
			return err
		}
	}
}

// openConns establishes conns connections, in batches.
//
// The batching is a workaround for this host, not a property of any server.
// Windows resolves the default listen backlog to somaxconn (200), and the
// kernel refuses connections beyond that depth even while the server's accept
// loop is draining: measured at ~205 against a bare net.Listener with no server
// code, and 202–207 for every framework in this audit. A simultaneous burst
// larger than the backlog therefore measures the environment rather than the
// server. Each batch is fully connected before the next starts, which keeps
// the queue below its depth, and the treatment is identical for every
// framework.
func openConns(host string, n int) ([]*clientConn, error) {
	const batch = 32
	out := make([]*clientConn, 0, n)

	for start := 0; start < n; start += batch {
		end := min(start+batch, n)
		batchConns := make([]*clientConn, end-start)

		var wg sync.WaitGroup
		errs := make([]error, end-start)
		for i := start; i < end; i++ {
			wg.Add(1)
			go func(k int) {
				defer wg.Done()
				cc, err := dial(host)
				batchConns[k] = cc
				errs[k] = err
			}(i - start)
		}
		wg.Wait()

		for _, err := range errs {
			if err != nil {
				for _, cc := range out {
					cc.c.Close()
				}
				return nil, err
			}
		}
		out = append(out, batchConns...)
		// Let the accept loop drain the batch before adding to the queue.
		time.Sleep(2 * time.Millisecond)
	}
	return out, nil
}

// drive runs the workload on conns connections for dur, returning throughput,
// latency percentiles, and the request count.
func drive(host string, w workload, conns int, dur time.Duration) (float64, latency, int, error) {
	ccs, err := openConns(host, conns)
	if err != nil {
		return 0, latency{}, 0, err
	}
	defer func() {
		for _, cc := range ccs {
			cc.c.Close()
		}
	}()

	deadline := time.Now().Add(dur)
	var total int64
	var wg sync.WaitGroup

	// Each worker keeps its own samples: a shared slice would add contention
	// that measures the harness rather than the server.
	samples := make([][]float64, len(ccs))
	errs := make([]error, len(ccs))

	for i := range ccs {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			cc := ccs[i]
			local := make([]float64, 0, 8192)
			for time.Now().Before(deadline) {
				us, err := cc.do(w.method, w.target, w.body)
				if err != nil {
					errs[i] = err
					return
				}
				local = append(local, float64(us))
				atomic.AddInt64(&total, 1)
			}
			samples[i] = local
		}(i)
	}
	wg.Wait()

	for _, err := range errs {
		if err != nil {
			return 0, latency{}, 0, err
		}
	}
	all := make([]float64, 0, total)
	for _, s := range samples {
		all = append(all, s...)
	}
	return float64(total) / dur.Seconds(), summarise(all), int(total), nil
}

// ------------------------------------------------------------------ server

// target is one framework's HTTP handler.
type target struct {
	name    string
	handler http.Handler
}

// runOnce starts a target, warms it, measures throughput, then measures
// allocations in a separate run.
func runOnce(t target, w workload, conns int, warm, dur time.Duration) (result, error) {
	// Throughput and latency.
	host, stop, err := serve(t.handler)
	if err != nil {
		return result{}, err
	}
	if _, _, _, err := drive(host, w, conns, warm); err != nil {
		stop()
		return result{}, fmt.Errorf("warmup: %w", err)
	}
	rps, lat, count, err := drive(host, w, conns, dur)
	if err != nil {
		stop()
		return result{}, err
	}
	stop()

	// Allocations, in a separate run. Reading memory stats mid-flight would
	// stop the world often enough to distort the throughput number.
	allocs, bytesOp := measureAllocs(t.handler, w, conns)

	return result{
		framework: t.name, workload: w.name, conns: conns,
		rps: rps, lat: lat, allocs: allocs, bytesOp: bytesOp, requests: count,
	}, nil
}

// serve starts h on a loopback port and returns the address and a stop func.
//
// Connections are accepted with a stagger. Windows resolves the default listen
// backlog to somaxconn (200), so a client that opens connections faster than
// the server can accept them is refused by the kernel — measured at 205
// connections against a bare net.Listener with no server code at all, and at
// 202–205 for every framework in this audit. Staggering the dial removes the
// harness's own limit so the server's behaviour, not the accept queue's depth,
// is what is being measured.
func serve(h http.Handler) (string, func(), error) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return "", nil, err
	}
	srv := &http.Server{Handler: h}
	go func() { _ = srv.Serve(ln) }()
	host := ln.Addr().String()
	return host, func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(ctx)
	}, nil
}

type memStat struct{ Mallocs, TotalAlloc uint64 }

func memStats() memStat {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	return memStat{m.Mallocs, m.TotalAlloc}
}

// measureAllocs returns server-side allocations per request.
//
// The client shares this process, so the figure includes the harness's own
// allocations and is an upper bound on the server's. It is reported as such
// rather than as a server-only number; the server-only measurement is the
// router benchmark in bench/README.md.
func measureAllocs(h http.Handler, w workload, conns int) (allocs, bytes float64) {
	host, stop, err := serve(h)
	if err != nil {
		return 0, 0
	}
	defer stop()

	if _, _, _, err := drive(host, w, conns, 300*time.Millisecond); err != nil {
		return 0, 0
	}
	before := memStats()
	_, _, count, err := drive(host, w, conns, 2*time.Second)
	after := memStats()
	if err != nil || count == 0 {
		return 0, 0
	}
	return float64(after.Mallocs-before.Mallocs) / float64(count),
		float64(after.TotalAlloc-before.TotalAlloc) / float64(count)
}

// ------------------------------------------------------------------ output

func printTable(headers []string, rows [][]string) {
	widths := make([]int, len(headers))
	for i, h := range headers {
		widths[i] = len(h)
	}
	for _, r := range rows {
		for i, c := range r {
			if i < len(widths) && len(c) > widths[i] {
				widths[i] = len(c)
			}
		}
	}
	var b strings.Builder
	writeRow := func(cells []string) {
		for i, c := range cells {
			if i > 0 {
				b.WriteString("  ")
			}
			b.WriteString(pad(c, widths[i]))
		}
		b.WriteString("\n")
	}
	writeRow(headers)
	for i := range headers {
		if i > 0 {
			b.WriteString("  ")
		}
		b.WriteString(strings.Repeat("-", widths[i]))
	}
	b.WriteString("\n")
	for _, r := range rows {
		writeRow(r)
	}
	fmt.Print(b.String())
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

func humanBytes(n float64) string {
	switch {
	case n > 1<<20:
		return fmt.Sprintf("%.1f MB", n/(1<<20))
	case n > 1<<10:
		return fmt.Sprintf("%.1f KB", n/(1<<10))
	default:
		return fmt.Sprintf("%.0f B", n)
	}
}

// ------------------------------------------------------------------ matrix

// allWorkloads is the matrix. Each entry is served identically by every
// framework.
var allWorkloads = []workload{
	{name: "plain", method: "GET", target: "/"},
	{name: "json", method: "GET", target: "/health"},
	{name: "param", method: "GET", target: "/users/123456"},
	{name: "resp-1kb", method: "GET", target: "/resp-1kb"},
	{name: "resp-10kb", method: "GET", target: "/resp-10kb"},
	{name: "resp-100kb", method: "GET", target: "/resp-100kb"},
	{name: "req-1kb", method: "POST", target: "/req-1kb", body: jsonBody(1 << 10)},
	{name: "req-100kb", method: "POST", target: "/req-100kb", body: jsonBody(100 << 10)},
	{name: "req-1mb", method: "POST", target: "/req-1mb", body: jsonBody(1 << 20)},
	{name: "noop", method: "GET", target: "/"},
}

// frameworks returns the targets under test for one workload.
//
// Fiber is deliberately absent. It is built on fasthttp, which does not use
// net/http's server, so a comparison would not be like-for-like: a different
// parser, a different request type, and a handler signature that is not
// http.Handler. A number next to the others would look comparable and would not
// be. See the report.
func frameworks(w workload, mw int) []target {
	out := []target{
		{name: "net/http", handler: newStd(w, mw)},
		{name: "photon", handler: newPhoton(w, mw)},
		{name: "chi", handler: newChi(w, mw)},
	}
	if h, err := newGin(w, mw); err == nil {
		out = append(out, target{name: "gin", handler: h})
	}
	if h, err := newEcho(w, mw); err == nil {
		out = append(out, target{name: "echo", handler: h})
	}
	return out
}

func main() {
	mode := "matrix"
	if len(os.Args) > 1 {
		mode = os.Args[1]
	}
	const conns = 128
	warm := 2 * time.Second
	dur := 5 * time.Second

	switch mode {
	case "matrix":
		runMatrix(conns, warm, dur)
	case "middleware":
		runMiddleware(conns, warm, dur)
	case "conns":
		runConns(warm, dur)
	case "profile":
		out := "../../bench/profiles"
		if len(os.Args) > 2 {
			out = os.Args[2]
		}
		if err := os.MkdirAll(out, 0o755); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		if err := profiling(allWorkloads[0], conns, dur, out); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
	default:
		fmt.Fprintf(os.Stderr, "unknown mode %q: want matrix, middleware, or conns\n", mode)
		os.Exit(2)
	}
}

// runMatrix measures every workload on every framework at one concurrency.
func runMatrix(conns int, warm, dur time.Duration) {
	reps := 3
	if v := os.Getenv("PHOTONBENCH_REPS"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			reps = n
		}
	}

	fmt.Printf("# Photon performance audit — workload matrix\n\n")
	fmt.Printf("%d connections, %s warmup, %s measurement x %d repetitions (best and median\n",
		conns, warm, dur, reps)
	fmt.Printf("reported), loopback TCP, HTTP/1.1 keep-alive.\n\n")
	fmt.Printf("Repetitions matter: single-run throughput on this host varies by up to\n")
	fmt.Printf("30%% between runs, which is larger than the gap between frameworks. A\n")
	fmt.Printf("single-run table would report noise as a ranking.\n\n")
	fmt.Printf("Latency columns are microseconds: p50/p90/p95/p99/p99.9/max.\n")
	fmt.Printf("allocs and B/op are process-wide, so they include the client; they are an\n")
	fmt.Printf("upper bound on the server's cost, not a server-only figure.\n\n")

	for _, w := range allWorkloads {
		fmt.Printf("## %s — %s %s\n\n", w.name, w.method, w.target)

		// Collect every repetition per framework, so the spread is visible
		// rather than hidden behind a single figure.
		byFramework := map[string][]result{}
		var order []string
		for _, t := range frameworks(w, 0) {
			if !serves(t.handler, w) {
				fmt.Printf("  %-9s skipped: does not serve this path\n", t.name)
				continue
			}
			byFramework[t.name] = nil
			order = append(order, t.name)
		}

		for rep := 0; rep < reps; rep++ {
			for _, t := range frameworks(w, 0) {
				if byFramework[t.name] == nil && len(byFramework[t.name]) == 0 &&
					!contains(order, t.name) {
					continue
				}
				r, err := runOnce(t, w, conns, warm, dur)
				if err != nil {
					fmt.Printf("  %-9s ERROR %v\n", t.name, err)
					continue
				}
				byFramework[t.name] = append(byFramework[t.name], r)
			}
			fmt.Printf("  repetition %d/%d complete\n", rep+1, reps)
		}

		headers := []string{"framework", "req/s (best)", "req/s (median)", "spread",
			"p50", "p99", "p99.9", "allocs", "B/op"}
		rows := [][]string{headers}
		for _, name := range order {
			rs := byFramework[name]
			if len(rs) == 0 {
				continue
			}
			rps := make([]float64, len(rs))
			for i, r := range rs {
				rps[i] = r.rps
			}
			best, med, spread := stats(rps)
			// Latency is taken from the median-throughput run, so it pairs
			// with the rps figure rather than coming from a different run.
			bestRun := rs[0]
			for _, r := range rs {
				if r.rps == best {
					bestRun = r
				}
			}
			rows = append(rows, []string{
				name, comma(int(best)), comma(int(med)),
				fmt.Sprintf("%.1f%%", spread),
				fmt.Sprintf("%.0f", bestRun.lat.p50),
				fmt.Sprintf("%.0f", bestRun.lat.p99),
				fmt.Sprintf("%.0f", bestRun.lat.p999),
				fmt.Sprintf("%.1f", bestRun.allocs),
				humanBytes(bestRun.bytesOp),
			})
		}
		printTable(rows[0], rows[1:])
		fmt.Println()
	}
}

func contains(s []string, v string) bool {
	for _, x := range s {
		if x == v {
			return true
		}
	}
	return false
}

// stats returns the best value, the median, and the spread as a percentage of
// the median. The spread is reported because a framework whose own repetitions
// vary by 20% cannot be ranked against one whose repetitions vary by 3%.
func stats(v []float64) (best, median, spreadPct float64) {
	if len(v) == 0 {
		return 0, 0, 0
	}
	s := append([]float64(nil), v...)
	sort.Float64s(s)
	best = s[len(s)-1]
	median = percentile(s, 50)
	if median > 0 {
		spreadPct = (best - s[0]) / median * 100
	}
	return best, median, spreadPct
}

// serves reports whether h answers w, so a framework that could not register a
// workload is skipped rather than measured on a 404.
func serves(h http.Handler, w workload) bool {
	host, stop, err := serve(h)
	if err != nil {
		return false
	}
	defer stop()
	cc, err := dial(host)
	if err != nil {
		return false
	}
	defer cc.c.Close()
	_, err = cc.do(w.method, w.target, w.body)
	return err == nil
}

// runMiddleware measures the incremental cost of 0, 1, 5, and 10 no-op
// middlewares on each framework.
func runMiddleware(conns int, warm, dur time.Duration) {
	w := allWorkloads[0] // static plaintext
	counts := []int{0, 1, 5, 10}
	names := []string{"photon", "net/http", "gin", "echo", "chi"}

	fmt.Printf("# Middleware scaling — static plaintext, %d connections\n\n", conns)
	fmt.Printf("Each middleware wraps and calls through, so the delta is dispatch cost,\n")
	fmt.Printf("not the work a middleware would do.\n\n")
	fmt.Printf("The last column is the marginal cost of one middleware, derived from the\n")
	fmt.Printf("0-to-10 comparison: (1/rps10 - 1/rps0) / 10, in microseconds per request.\n\n")

	headers := []string{"framework", "0 mw", "1 mw", "5 mw", "10 mw", "us/mw"}
	rows := [][]string{headers}
	for _, name := range names {
		row := []string{name}
		var rps []float64
		ok := true
		for _, n := range counts {
			var h http.Handler
			for _, t := range frameworks(w, n) {
				if t.name == name {
					h = t.handler
				}
			}
			if h == nil {
				ok = false
				break
			}
			r, err := runOnce(target{name: name, handler: h}, w, conns, warm, dur)
			if err != nil {
				// The reason is reported. A framework silently dropping out of
				// the table is indistinguishable from one that was slow, and
				// that ambiguity would hide a real difference.
				fmt.Printf("  %-9s (%d mw): %v\n", name, n, err)
				ok = false
				break
			}
			rps = append(rps, r.rps)
			row = append(row, comma(int(r.rps)))
		}
		if !ok {
			fmt.Printf("  %-9s skipped\n", name)
			continue
		}
		if len(rps) == len(counts) && rps[0] > 0 {
			perMW := (1/rps[len(rps)-1] - 1/rps[0]) * 1e6 / 10
			row = append(row, fmt.Sprintf("%.3f", perMW))
		} else {
			row = append(row, "-")
		}
		rows = append(rows, row)
	}
	printTable(rows[0], rows[1:])
}

// runConns measures one workload across connection counts, to separate
// throughput saturation from latency growth.
func runConns(warm, dur time.Duration) {
	w := allWorkloads[0]
	counts := []int{1, 10, 100, 500, 1000, 2000}
	fmt.Printf("# Connection scaling — static plaintext\n\n")
	fmt.Printf("Latency in microseconds. Throughput should rise then plateau and latency\n")
	fmt.Printf("should stay flat until the plateau, then degrade. A server that gains\n")
	fmt.Printf("throughput by destroying tail latency is not fast.\n\n")
	headers := []string{"conns", "req/s", "p50", "p90", "p99", "p99.9", "max"}
	rows := [][]string{headers}
	for _, c := range counts {
		r, err := runOnce(target{name: "photon", handler: newPhoton(w, 0)}, w, c, warm, dur)
		if err != nil {
			// The error is shown rather than swallowed: a failed connection
			// count is a finding, and hiding it would leave a gap in the table
			// that reads like "we did not test it".
			fmt.Printf("  %d connections: ERROR %v\n", c, err)
			rows = append(rows, []string{strconv.Itoa(c), "err", "-", "-", "-", "-", "-"})
			continue
		}
		rows = append(rows, []string{
			strconv.Itoa(c), comma(int(r.rps)),
			fmt.Sprintf("%.0f", r.lat.p50), fmt.Sprintf("%.0f", r.lat.p90),
			fmt.Sprintf("%.0f", r.lat.p99), fmt.Sprintf("%.0f", r.lat.p999),
			fmt.Sprintf("%.0f", r.lat.max),
		})
	}
	printTable(rows[0], rows[1:])
}
