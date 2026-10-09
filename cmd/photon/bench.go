package main

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"math"
	"math/bits"
	"net/http"
	"os"
	"sort"
	"strings"
	"sync"
	"time"
)

// headerFlags collects repeated -H flags.
type headerFlags []string

func (h *headerFlags) String() string     { return strings.Join(*h, ", ") }
func (h *headerFlags) Set(v string) error { *h = append(*h, v); return nil }

// runBench measures an SSE endpoint the way a user experiences it: how long
// until the first event arrives, how long the gaps between events are, and how
// many events per second the server sustains across concurrent streams.
//
// Generic HTTP load tools (wrk, hey, ab) measure whole responses, which says
// nothing about a stream that is supposed to arrive a token at a time.
func runBench(args []string) error {
	flags := flag.NewFlagSet("bench", flag.ContinueOnError)
	conc := flags.Int("c", 10, "concurrent streams")
	total := flags.Int("n", 0, "total streams to run (default: the -c value)")
	method := flags.String("method", "GET", "HTTP method")
	data := flags.String("d", "", "request body; implies POST and Content-Type: application/json unless set with -H")
	timeout := flags.Duration("timeout", 2*time.Minute, "per-stream timeout")
	var headers headerFlags
	flags.Var(&headers, "H", `request header, "Name: value" (repeatable)`)
	flags.Usage = func() {
		fmt.Fprintf(flags.Output(), "Usage: photon bench [flags] <url>\n\n")
		flags.PrintDefaults()
		fmt.Fprintf(flags.Output(), "\nExamples:\n"+
			"  photon bench -c 50 -n 500 'http://localhost:8484/agent/stream?burst=true'\n"+
			"  photon bench -c 20 -d '{\"model\":\"m\",\"stream\":true,\"messages\":[{\"role\":\"user\",\"content\":\"hi\"}]}' \\\n"+
			"      -H 'Authorization: Bearer sk-...' http://localhost:8080/v1/chat/completions\n")
	}
	var target string
	if len(args) > 0 && !strings.HasPrefix(args[0], "-") {
		target, args = args[0], args[1:]
	}
	if err := flags.Parse(args); err != nil {
		return err
	}
	if target == "" {
		target = flags.Arg(0)
	}
	if target == "" || (!strings.HasPrefix(target, "http://") && !strings.HasPrefix(target, "https://")) {
		flags.Usage()
		return errors.New("bench needs an http:// or https:// URL")
	}
	if *conc < 1 {
		return errors.New("-c must be at least 1")
	}
	if *total < *conc {
		*total = *conc
	}
	if *data != "" && *method == "GET" {
		*method = "POST"
	}
	hdr := http.Header{"Accept": {"text/event-stream"}}
	if *data != "" {
		hdr.Set("Content-Type", "application/json")
	}
	for _, h := range headers {
		k, v, ok := strings.Cut(h, ":")
		if !ok {
			return fmt.Errorf("bad header %q, want \"Name: value\"", h)
		}
		hdr.Set(strings.TrimSpace(k), strings.TrimSpace(v))
	}

	client := &http.Client{Transport: &http.Transport{
		MaxIdleConns:        *conc,
		MaxIdleConnsPerHost: *conc,
		DisableCompression:  true, // measure the stream as sent, not after a gzip layer
		ForceAttemptHTTP2:   true,
	}}
	defer client.CloseIdleConnections()

	fmt.Printf("Benchmarking %s %s with %d streams, %d at a time...\n", *method, target, *total, *conc)
	var (
		mu      sync.Mutex
		results []streamResult
		gaps    histogram
	)
	work := make(chan struct{}, *total)
	for i := 0; i < *total; i++ {
		work <- struct{}{}
	}
	close(work)
	start := time.Now()
	var wg sync.WaitGroup
	for i := 0; i < *conc; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			var local histogram
			for range work {
				res := runOneStream(client, *method, target, *data, hdr, *timeout, &local)
				mu.Lock()
				results = append(results, res)
				mu.Unlock()
			}
			mu.Lock()
			gaps.merge(&local)
			mu.Unlock()
		}()
	}
	wg.Wait()
	report(os.Stdout, results, &gaps, time.Since(start))
	return nil
}

// streamResult is one stream's measurements.
type streamResult struct {
	ttfb, ttfe, duration time.Duration
	events               int
	bytes                int64
	err                  error
}

func runOneStream(client *http.Client, method, target, body string, hdr http.Header,
	timeout time.Duration, gaps *histogram) streamResult {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	var rb io.Reader
	if body != "" {
		rb = strings.NewReader(body)
	}
	req, err := http.NewRequestWithContext(ctx, method, target, rb)
	if err != nil {
		return streamResult{err: err}
	}
	req.Header = hdr.Clone()

	start := time.Now()
	resp, err := client.Do(req)
	if err != nil {
		return streamResult{err: err}
	}
	defer resp.Body.Close()
	res := streamResult{ttfb: time.Since(start)}
	if resp.StatusCode != http.StatusOK {
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 64<<10))
		res.err = fmt.Errorf("HTTP %d", resp.StatusCode)
		return res
	}

	// A minimal SSE reader: an event is dispatched at a blank line after at
	// least one data field, exactly as EventSource does it. Lines are read
	// with ReadSlice so a long event does not allocate per line.
	br := bufio.NewReaderSize(resp.Body, 64<<10)
	var last time.Time
	sawData, midLine := false, false
	for {
		line, err := br.ReadSlice('\n')
		res.bytes += int64(len(line))
		if err == bufio.ErrBufferFull {
			if !midLine && bytes.HasPrefix(line, []byte("data")) {
				sawData = true
			}
			midLine = true
			continue
		}
		if len(line) > 0 && !midLine {
			trimmed := bytes.TrimRight(line, "\r\n")
			switch {
			case len(trimmed) == 0 && sawData:
				now := time.Now()
				if res.events == 0 {
					res.ttfe = now.Sub(start)
				} else {
					gaps.add(now.Sub(last))
				}
				last = now
				res.events++
				sawData = false
			case bytes.HasPrefix(trimmed, []byte("data")):
				sawData = true
			}
		}
		midLine = false
		if err != nil {
			if !errors.Is(err, io.EOF) {
				res.err = err
			}
			break
		}
	}
	res.duration = time.Since(start)
	return res
}

// report prints the summary.
func report(w io.Writer, results []streamResult, gaps *histogram, elapsed time.Duration) {
	var ok []streamResult
	errs := map[string]int{}
	var events int
	var bytesIn int64
	for _, r := range results {
		events += r.events
		bytesIn += r.bytes
		if r.err != nil {
			errs[r.err.Error()]++
			continue
		}
		ok = append(ok, r)
	}
	fmt.Fprintf(w, "\nStreams   %d ok, %d failed in %s\n", len(ok), len(results)-len(ok), elapsed.Round(time.Millisecond))
	fmt.Fprintf(w, "Events    %s total, %s/s, %s received\n",
		thousands(events), thousands(int(float64(events)/elapsed.Seconds())), humanSize(bytesIn))
	if len(ok) > 0 {
		pick := func(f func(streamResult) time.Duration) []time.Duration {
			out := make([]time.Duration, 0, len(ok))
			for _, r := range ok {
				if d := f(r); d > 0 {
					out = append(out, d)
				}
			}
			sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
			return out
		}
		fmt.Fprintf(w, "\n%-22s %10s %10s %10s %10s\n", "", "p50", "p90", "p99", "max")
		row(w, "Time to first byte", pick(func(r streamResult) time.Duration { return r.ttfb }))
		row(w, "Time to first event", pick(func(r streamResult) time.Duration { return r.ttfe }))
		if gaps.n > 0 {
			fmt.Fprintf(w, "%-22s %10s %10s %10s %10s\n", "Gap between events",
				dur(gaps.quantile(.50)), dur(gaps.quantile(.90)), dur(gaps.quantile(.99)), dur(gaps.max))
		}
		row(w, "Stream duration", pick(func(r streamResult) time.Duration { return r.duration }))
	}
	if len(errs) > 0 {
		fmt.Fprintln(w, "\nErrors")
		for msg, n := range errs {
			fmt.Fprintf(w, "  %6d  %s\n", n, msg)
		}
	}
	fmt.Fprintln(w)
}

func row(w io.Writer, name string, sorted []time.Duration) {
	if len(sorted) == 0 {
		fmt.Fprintf(w, "%-22s %10s\n", name, "-")
		return
	}
	q := func(p float64) time.Duration {
		return sorted[min(len(sorted)-1, int(math.Ceil(p*float64(len(sorted))))-1)]
	}
	fmt.Fprintf(w, "%-22s %10s %10s %10s %10s\n", name, dur(q(.50)), dur(q(.90)), dur(q(.99)), dur(sorted[len(sorted)-1]))
}

// histogram is a log-linear latency histogram: 16 buckets per power of two of
// nanoseconds, so any quantile is within about 6% of the true value, in a fixed
// 1 KB however many samples arrive. Storing every gap would cost memory in
// proportion to the events received, which is the thing being benchmarked.
type histogram struct {
	counts [64 * 16]uint64
	n      uint64
	max    time.Duration
}

func (h *histogram) add(d time.Duration) {
	if d <= 0 {
		d = 1
	}
	h.counts[bucketOf(uint64(d))]++
	h.n++
	h.max = max(h.max, d)
}

func (h *histogram) merge(o *histogram) {
	for i, c := range o.counts {
		h.counts[i] += c
	}
	h.n += o.n
	h.max = max(h.max, o.max)
}

func (h *histogram) quantile(q float64) time.Duration {
	target := uint64(math.Ceil(q * float64(h.n)))
	var seen uint64
	for i, c := range h.counts {
		seen += c
		if seen >= target && c > 0 {
			return time.Duration(bucketLow(i))
		}
	}
	return h.max
}

func bucketOf(v uint64) int {
	if v < 16 {
		return int(v)
	}
	exp := bits.Len64(v) - 1 // v is in [2^exp, 2^(exp+1))
	sub := (v >> (exp - 4)) & 15
	return exp*16 + int(sub)
}

func bucketLow(i int) uint64 {
	exp, sub := i/16, uint64(i%16)
	if exp < 4 {
		return uint64(i)
	}
	return (1 << exp) | sub<<(exp-4)
}

func dur(d time.Duration) string {
	switch {
	case d < time.Microsecond:
		return "<1µs" // events that arrived in the same read
	case d < time.Millisecond:
		return fmt.Sprintf("%.0fµs", float64(d)/float64(time.Microsecond))
	case d < time.Second:
		return fmt.Sprintf("%.2fms", float64(d)/float64(time.Millisecond))
	default:
		return fmt.Sprintf("%.2fs", d.Seconds())
	}
}

func thousands(n int) string {
	s := fmt.Sprint(n)
	for i := len(s) - 3; i > 0; i -= 3 {
		s = s[:i] + "," + s[i:]
	}
	return s
}

func humanSize(n int64) string {
	switch {
	case n >= 1<<30:
		return fmt.Sprintf("%.1f GiB", float64(n)/(1<<30))
	case n >= 1<<20:
		return fmt.Sprintf("%.1f MiB", float64(n)/(1<<20))
	case n >= 1<<10:
		return fmt.Sprintf("%.1f KiB", float64(n)/(1<<10))
	default:
		return fmt.Sprintf("%d B", n)
	}
}
