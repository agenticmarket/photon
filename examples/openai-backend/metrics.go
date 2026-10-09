package main

import (
	"fmt"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// streamBuckets are the histogram's upper bounds, in seconds.
//
// Declared before metrics because the array size is derived from it: Go requires
// an array length to be a constant expression, and a package-level var is one.
var streamBuckets = [...]float64{0.1, 0.5, 1, 2.5, 5, 10, 30, 60, 300}

// metrics is the in-process counter set.
//
// Prometheus text format, hand-rendered, because the counters are simple and a
// client library for them would be the largest dependency in a server whose
// whole argument is being small. The exposition format is a line-oriented text
// format and this is the whole of it.
//
// Everything is atomic or mutex-guarded so that scraping, which happens on a
// random request, never contends with the streaming path. A metric that blocks
// request handling during a scrape is a self-inflicted outage.
type metrics struct {
	// requestsTotal counts every request, keyed by route and status class.
	//
	// Keyed by *route* rather than raw path so that a path containing an
	// identifier - /v1/threads/abc123 - does not create an unbounded number of
	// label values. That is the classic way a metrics endpoint brings down the
	// system it is measuring: cardinality explodes with client-supplied data.
	requestsTotal    sync.Map // route -> *statusCounter
	requestsInFlight atomic.Int64

	streamsAdmitted atomic.Int64
	streamsClosed   atomic.Int64
	streamsAborted  atomic.Int64 // client went away or the provider failed
	tokensStreamed  atomic.Int64
	quotaRejected   atomic.Int64
	upstreamErrors  atomic.Int64
	totalStreamTime atomic.Int64 // nanoseconds, summed

	// streamDuration is a fixed-bucket histogram.
	//
	// Fixed buckets rather than an exact distribution because a streaming server
	// wants a coarse shape - was it fast, or did it stall - and a client that
	// wants precision can read the upstream provider's own metrics. Buckets are
	// seconds, which is the unit a reader thinks in.
	//
	// One extra slot beyond the bounds holds the +Inf overflow, because the
	// Prometheus histogram format requires a +Inf bucket and a distribution
	// without one is not a valid distribution.
	streamDuration [len(streamBuckets) + 1]atomic.Int64
}

// statusCounter counts statuses for one route.
type statusCounter struct {
	byClass [6]atomic.Int64 // 1xx..5xx, index by class-1
}

func (c *statusCounter) add(status int) {
	class := status / 100
	if class < 1 || class > 5 {
		return
	}
	c.byClass[class-1].Add(1)
}

func (c *statusCounter) snapshot() [6]int64 {
	var out [6]int64
	for i := range c.byClass {
		out[i] = c.byClass[i].Load()
	}
	return out
}

// observeStream records one completed stream.
func (m *metrics) observeStream(d time.Duration) {
	m.streamsClosed.Add(1)
	m.totalStreamTime.Add(int64(d))

	secs := d.Seconds()
	for i, upper := range streamBuckets {
		if secs <= upper {
			m.streamDuration[i].Add(1)
			return
		}
	}
	// Past the last bucket, so it lands in +Inf. Prometheus requires the +Inf
	// bucket to exist, and a distribution without it is not a distribution.
	m.streamDuration[len(streamBuckets)].Add(1)
}

// render writes the Prometheus exposition format.
func (m *metrics) render() string {
	var b strings.Builder

	// HELP and TYPE lines come first for each family, as the format requires.
	writeCounter(&b, "ssebackend_requests_total",
		"Total HTTP requests, by route and status class.",
		func(w *strings.Builder) {
			type row struct {
				route  string
				status string
				value  int64
			}
			var rows []row
			m.requestsTotal.Range(func(k, v any) bool {
				route := k.(string)
				sc := v.(*statusCounter)
				snap := sc.snapshot()
				for class := 1; class <= 5; class++ {
					if snap[class-1] > 0 {
						rows = append(rows, row{
							route:  route,
							status: fmt.Sprintf("%dxx", class),
							value:  snap[class-1],
						})
					}
				}
				return true
			})
			// Sorted so a scrape is byte-identical between calls. A metrics
			// endpoint whose output reorders itself defeats diffing and alerting.
			sort.Slice(rows, func(i, j int) bool {
				if rows[i].route != rows[j].route {
					return rows[i].route < rows[j].route
				}
				return rows[i].status < rows[j].status
			})
			for _, r := range rows {
				fmt.Fprintf(w, "ssebackend_requests_total{route=%q,status=%q} %d\n",
					r.route, r.status, r.value)
			}
		})

	writeCounter(&b, "ssebackend_streams_admitted_total",
		"Streams admitted after passing quota.",
		func(w *strings.Builder) {
			fmt.Fprintf(w, "ssebackend_streams_admitted_total %d\n", m.streamsAdmitted.Load())
		})

	writeCounter(&b, "ssebackend_streams_closed_total",
		"Streams that finished normally.",
		func(w *strings.Builder) {
			fmt.Fprintf(w, "ssebackend_streams_closed_total %d\n", m.streamsClosed.Load())
		})

	writeCounter(&b, "ssebackend_streams_aborted_total",
		"Streams ended early: client disconnected or the provider failed.",
		func(w *strings.Builder) {
			fmt.Fprintf(w, "ssebackend_streams_aborted_total %d\n", m.streamsAborted.Load())
		})

	writeCounter(&b, "ssebackend_tokens_streamed_total",
		"Tokens written to clients.",
		func(w *strings.Builder) {
			fmt.Fprintf(w, "ssebackend_tokens_streamed_total %d\n", m.tokensStreamed.Load())
		})

	writeCounter(&b, "ssebackend_quota_rejected_total",
		"Requests rejected by a quota limit.",
		func(w *strings.Builder) {
			fmt.Fprintf(w, "ssebackend_quota_rejected_total %d\n", m.quotaRejected.Load())
		})

	writeCounter(&b, "ssebackend_upstream_errors_total",
		"Upstream provider failures.",
		func(w *strings.Builder) {
			fmt.Fprintf(w, "ssebackend_upstream_errors_total %d\n", m.upstreamErrors.Load())
		})

	writeCounter(&b, "ssebackend_stream_duration_seconds_sum",
		"Total time spent streaming.",
		func(w *strings.Builder) {
			fmt.Fprintf(w, "ssebackend_stream_duration_seconds_sum %.3f\n",
				float64(m.totalStreamTime.Load())/float64(time.Second))
		})

	// The histogram: one _bucket line per bound, cumulative, then _sum and _count.
	fmt.Fprintf(&b, "# HELP ssebackend_stream_duration_seconds Stream lifetime in seconds.\n")
	fmt.Fprintf(&b, "# TYPE ssebackend_stream_duration_seconds histogram\n")
	var cumulative int64
	for i, upper := range streamBuckets {
		cumulative += m.streamDuration[i].Load()
		fmt.Fprintf(&b, "ssebackend_stream_duration_seconds_bucket{le=\"%g\"} %d\n",
			upper, cumulative)
	}
	cumulative += m.streamDuration[len(streamBuckets)].Load()
	fmt.Fprintf(&b, "ssebackend_stream_duration_seconds_bucket{le=\"+Inf\"} %d\n", cumulative)
	fmt.Fprintf(&b, "ssebackend_stream_duration_seconds_sum %.3f\n",
		float64(m.totalStreamTime.Load())/float64(time.Second))
	fmt.Fprintf(&b, "ssebackend_stream_duration_seconds_count %d\n", cumulative)

	writeCounter(&b, "ssebackend_requests_in_flight",
		"Requests currently being served.",
		func(w *strings.Builder) {
			fmt.Fprintf(w, "ssebackend_requests_in_flight %d\n", m.requestsInFlight.Load())
		})

	return b.String()
}

// writeCounter writes a HELP line, a TYPE line, and the body.
func writeCounter(b *strings.Builder, name, help string, body func(*strings.Builder)) {
	fmt.Fprintf(b, "# HELP %s %s\n", name, help)
	fmt.Fprintf(b, "# TYPE %s counter\n", name)
	body(b)
}

// recordRequest counts one request under a route label.
func (m *metrics) recordRequest(route string, status int) {
	v, _ := m.requestsTotal.LoadOrStore(route, &statusCounter{})
	v.(*statusCounter).add(status)
}
