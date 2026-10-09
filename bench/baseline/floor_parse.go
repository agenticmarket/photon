package main

import (
	"bufio"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
)

// The results files are the source of truth for the floor. Parsing them rather
// than transcribing the numbers by hand means the floor cannot drift from the
// data that was actually recorded, and a transcription error cannot silently
// make the floor wrong.
var resultFiles = []string{
	"../results-matrix-2026-10.txt",
}

// parseSpreads reads spread percentages out of the harness's result files.
//
// The format is a columnar table: a header naming the columns, a dashed rule,
// then one row per framework with the spread as a percentage. The parser
// anchors on the header rather than on column position, because the column
// count varies between files (some tables have no p99.9, some have no allocs)
// and a fixed offset would read the wrong number from a table that merely
// happens to be a different shape.
func parseSpreads(path string) ([]spreadSample, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	var out []spreadSample
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 0, 64*1024), 1<<20)

	workload := ""
	spreadCol := -1

	for sc.Scan() {
		line := strings.TrimRight(sc.Text(), " \t\r")

		// A "## ..." or "# ..." line names the section for the table beneath it.
		// Both are handled because a results file may open with a top-level "# "
		// heading and no "## " at all, so a parser that only resets on "## "
		// would carry a stale column offset into that whole file.
		//
		// The column is reset because the next table's shape is not guaranteed to
		// match - a connection-scaling table has a p50 column and no spread column, and
		// carrying a stale offset into it is what turns a latency in microseconds
		// into a percentage.
		if strings.HasPrefix(line, "## ") {
			workload = strings.TrimSpace(strings.TrimPrefix(line, "## "))
			spreadCol = -1
			continue
		}
		if strings.HasPrefix(line, "# ") {
			workload = strings.TrimSpace(strings.TrimPrefix(line, "# "))
			spreadCol = -1
			continue
		}

		// The header row: locate the spread column.
		//
		// Located by ordinal position among *values*, not by raw field index. The
		// header reads "req/s (best)  req/s (median)  spread", which is three
		// whitespace-separated tokens where a data row has two values - "photon
		// 210,957 166,892 32.6%" - because the parenthesised labels are split by
		// spaces and the data has no parentheses. A raw field index of 5 lands on
		// "32.6%"; an index of 3 lands on the median. Both produce a plausible
		// number, which is why the mismatch is handled explicitly rather than
		// left as a comment.
		if strings.Contains(line, "spread") {
			spreadCol = spreadOrdinal(line)
			continue
		}
		if spreadCol < 0 {
			continue
		}

		// A dashed rule ends the header; rows follow.
		if strings.TrimSpace(line) == "" || strings.HasPrefix(strings.TrimSpace(line), "---") {
			continue
		}

		fields := strings.Fields(line)
		if len(fields) <= spreadCol {
			// Not a data row, or a row from a different table shape. Skipping
			// rather than failing, because these files are the harness's
			// human-readable output and one odd line should not make the floor
			// uncomputable.
			continue
		}

		pct, ok := parsePercent(fields[spreadCol])
		if !ok {
			continue
		}
		out = append(out, spreadSample{
			source:   filepath.Base(path),
			workload: workload,
			subject:  fields[0],
			spread:   pct,
		})
	}
	return out, sc.Err()
}

// parsePercent accepts "21.3%" and returns 21.3.
func parsePercent(s string) (float64, bool) {
	s = strings.TrimSuffix(strings.TrimSpace(s), "%")
	if s == "" {
		return 0, false
	}
	v, err := strconv.ParseFloat(s, 64)
	if err != nil || math.IsNaN(v) || v < 0 {
		return 0, false
	}
	return v, true
}

// spreadOrdinal returns which *value* column the spread occupies, or -1.
//
// It walks the header's fields counting one column per label. A parenthesised
// qualifier is not a column of its own: "req/s (best)" is one column named
// "best", so the "(best)" token belongs to the label before it.
//
// "req/s (best) req/s (median) spread p50" therefore yields 2, which is the
// third field of "photon 210,957 166,892 32.6%" - the spread.
func spreadOrdinal(header string) int {
	col := -1 // column index of the label most recently seen
	for _, f := range strings.Fields(header) {
		if strings.HasPrefix(f, "(") {
			// A qualifier: it qualifies the label already counted, so it does
			// not advance the column.
			continue
		}
		if f == "spread" {
			return col + 1
		}
		col++
	}
	return -1
}

// columnIndex returns the field index of a named column in a header row.
func columnIndex(header, name string) int {
	for i, f := range strings.Fields(header) {
		if f == name {
			return i
		}
	}
	return -1
}

// buildFloor aggregates samples into a report.
func buildFloor(samples []spreadSample) floorReport {
	r := floorReport{
		Samples:    samples,
		PerSubject: map[string]float64{},
	}
	if len(samples) == 0 {
		return r
	}

	values := make([]float64, 0, len(samples))
	sum := 0.0
	for _, s := range samples {
		values = append(values, s.spread)
		sum += s.spread
		if s.spread > r.PerSubject[s.subject] {
			r.PerSubject[s.subject] = s.spread
		}
	}
	sort.Float64s(values)

	r.Min = values[0]
	r.Max = values[len(values)-1]
	r.Mean = sum / float64(len(values))
	r.Median = medianOf(values)
	return r
}

// medianOf is the middle value, averaging the two central values on an even
// count.
//
// Hand-written rather than taken from a statistics package: the harness exists
// to compare frameworks without pulling in dependencies, and a median is four
// lines.
func medianOf(sorted []float64) float64 {
	n := len(sorted)
	if n == 0 {
		return 0
	}
	if n%2 == 1 {
		return sorted[n/2]
	}
	return (sorted[n/2-1] + sorted[n/2]) / 2
}

type kv struct {
	k string
	v float64
}

func sortedPairs(m map[string]float64) []kv {
	out := make([]kv, 0, len(m))
	for k, v := range m {
		out = append(out, kv{k, v})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].k < out[j].k })
	return out
}

func sprintf(format string, args ...any) string {
	return fmt.Sprintf(format, args...)
}
