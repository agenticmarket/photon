package main

import (
	"os"
	"strings"
	"testing"
)

// TestMeasurementFloorIsPublished derives the floor BENCHMARKS.md quotes.
//
// The failure it prevents is quiet and expensive: a reader takes two numbers from
// the tables, sees a 12% gap, and concludes one framework is faster. When the
// floor is 20%, that conclusion is unsupported, and nothing in the document says
// so.
//
// So this does not check a constant. It derives the floor from the committed
// result files and asserts that the derivation works and produces a plausible
// percentage; the value is logged so BENCHMARKS.md can quote it.
func TestMeasurementFloorIsPublished(t *testing.T) {
	var all []spreadSample
	for _, path := range resultFiles {
		samples, err := parseSpreads(path)
		if err != nil {
			t.Fatalf("reading %s: %v", path, err)
		}
		all = append(all, samples...)
	}

	if len(all) == 0 {
		t.Fatalf("no spread values parsed from %v; the floor cannot be derived and the "+
			"audit's numbers are unrankable against each other", resultFiles)
	}

	report := buildFloor(all)
	t.Log("\n" + report.interpret())

	worst := 0.0
	for _, v := range report.PerSubject {
		if v > worst {
			worst = v
		}
	}

	// A floor of zero means every repetition was identical, which on a shared
	// machine means the parser read the wrong column; above 100% it read a
	// latency. Either way the published floor would be wrong.
	if report.Median <= 0 || report.Median > 100 {
		t.Errorf("derived median floor is %.1f%%, which is not a plausible run-to-run "+
			"spread; the parser or the results file is wrong", report.Median)
	}

	t.Logf("floor: median %.1f%%, worst per-subject %.1f%%", report.Median, worst)
}

// TestResultFilesExist keeps the floor honest about its inputs.
//
// A floor derived from files that are not there would silently become a floor
// derived from nothing, and the failure would appear as an empty report rather
// than as a wrong number.
func TestResultFilesExist(t *testing.T) {
	for _, path := range resultFiles {
		st, err := os.Stat(path)
		if err != nil {
			t.Errorf("%s: %v", path, err)
			continue
		}
		if st.Size() == 0 {
			t.Errorf("%s is empty; the floor would be derived from no data", path)
		}
	}
}

// TestParserReadsPercentagesInEveryShapeTheHarnessPrints guards the parser
// against the harness's own output changing shape.
//
// The column count varies between tables - some have p99.9, some do not; some
// have allocs, some do not. A parser anchored on a fixed column offset would
// read the wrong number the first time a table's shape changed, and the result
// would be a plausible, wrong floor.
func TestParserReadsPercentagesInEveryShapeTheHarnessPrints(t *testing.T) {
	tests := []struct {
		name   string
		input  string
		want   []float64
		reason string
	}{
		{
			name: "full table with p99.9 and allocs",
			input: `framework  req/s (best)  req/s (median)  spread  p50  p99   p99.9  allocs  B/op
---------  ------------  --------------  ------  ---  ----  -----  ------  ------
photon     210,957       166,892         32.6%   530  3713  8167   30.0    2.5 KB`,
			want:   []float64{32.6},
			reason: "the shape in the committed results file",
		},
		{
			name: "table without p99.9",
			input: `framework  req/s (best)  req/s (median)  spread  p50  p99   allocs  B/op
---------  ------------  --------------  ------  ---  ----  ------  ------
photon     210,957       166,892         32.6%   530  3713  30.0    2.5 KB`,
			want:   []float64{32.6},
			reason: "spread is not the last column, so a fixed offset would miss it",
		},
		{
			name: "whole percent with no decimal",
			input: `framework  req/s (best)  req/s (median)  spread  p50
---------  ------------  --------------  ------  ---
echo       168,186       164,230         6.2%    557`,
			want:   []float64{6.2},
			reason: "a bare integer percent",
		},
		{
			name: "row before any header is ignored",
			input: `some other line 12.3%
framework  req/s (best)  req/s (median)  spread  p50
---------  ------------  --------------  ------  ---
photon     210,957       166,892         30.0%   530`,
			want:   []float64{30},
			reason: "spreadCol is only set by a real header row",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			path := writeTemp(t, tc.input)
			samples, err := parseSpreads(path)
			if err != nil {
				t.Fatal(err)
			}
			if len(samples) != len(tc.want) {
				t.Fatalf("parsed %d rows, want %d: %+v", len(samples), len(tc.want), samples)
			}
			for i, w := range tc.want {
				if samples[i].spread != w {
					t.Errorf("row %d spread = %.1f, want %.1f (%s)",
						i, samples[i].spread, w, tc.reason)
				}
			}
		})
	}
}

func TestMedianOf(t *testing.T) {
	tests := []struct {
		in   []float64
		want float64
	}{
		{[]float64{5}, 5},
		{[]float64{1, 2, 3}, 2},
		{[]float64{1, 2, 3, 4}, 2.5},
		{[]float64{3.9, 12, 12.7, 14.9, 16.5, 17.5, 19.9, 20.8, 22.3, 25.2, 25.8, 26.3, 29, 33.8, 34.1}, 20.8},
		{nil, 0},
	}
	for _, tc := range tests {
		if got := medianOf(tc.in); got != tc.want {
			t.Errorf("medianOf(%v) = %.2f, want %.2f", tc.in, got, tc.want)
		}
	}
}

// writeTemp writes s to a file in the test's temp dir and returns its path.
// TestParserMatchesTheCommittedResults pins the parser to the real file.
//
// The unit test above proves the parser handles the shapes in isolation. This
// one proves it handles *this* file, whose header writes "req/s (best)" as
// three whitespace-separated tokens where the data rows have two values. A
// parser that indexed the header's raw field positions read the median instead
// of the spread and reported a floor of 4758% - a number wrong enough to be
// obviously wrong, which is the lucky case. The dangerous version is a 10%
// error that still looks like a percentage.
func TestParserMatchesTheCommittedResults(t *testing.T) {
	samples, err := parseSpreads("../results-matrix-2026-10.txt")
	if err != nil {
		t.Fatal(err)
	}
	if len(samples) == 0 {
		t.Fatal("no samples parsed from results-matrix-2026-10.txt")
	}

	// The first table in the file only, read by hand. The
	// file holds ten tables and every subject appears in each, so matching on
	// subject alone would compare all ten against the first table's numbers.
	//
	// Matched as a prefix because the headings carry an arrow between the
	// workload and the route ("## plain -> GET /") and the arrow is not
	// relied on.
	firstTable := "plain"
	want := map[string]float64{
		"net/http": 10.4,
		"photon":   9.5,
		"chi":      19.4,
		"gin":      10.5,
		"echo":     11.9,
	}

	checked := 0
	for _, s := range samples {
		if !strings.HasPrefix(s.workload, firstTable) {
			continue
		}
		w, ok := want[s.subject]
		if !ok {
			t.Errorf("unexpected subject %q in the first table", s.subject)
			continue
		}
		if s.spread != w {
			t.Errorf("first table, %s: parsed spread %.1f%%, want %.1f%%",
				s.subject, s.spread, w)
		}
		checked++
	}
	if checked != len(want) {
		t.Errorf("checked %d rows in the first table, want %d; the workload heading is "+
			"not being captured correctly", checked, len(want))
	}

	// Every parsed value must be a plausible percentage. A value above 200%
	// means the parser has read a latency or a byte count, and the floor derived
	// from it is meaningless - which is exactly the failure this catches.
	for _, s := range samples {
		if s.spread > 200 {
			t.Errorf("%s/%s: spread %.1f%% is not a plausible percentage; the parser "+
				"has almost certainly read a latency column as a spread", s.workload, s.subject, s.spread)
		}
	}
	t.Logf("parsed %d samples from results-matrix-2026-10.txt, all plausible percentages", len(samples))
}

// TestSpreadOrdinal pins the column arithmetic, which is the part that was wrong.
//
// The header writes "req/s (best)" as two tokens - "req/s" and "(best)" - where
// a data row has one value for the pair. Counting raw tokens therefore puts
// spread at index 5; counting *columns* puts it at index 3, which is where the
// value actually is. Reading index 5 lands on a latency; reading index 3 by
// accident is the near miss that returns a plausible percentage.
func TestSpreadOrdinal(t *testing.T) {
	tests := []struct {
		header string
		want   int
	}{
		{
			"framework  req/s (best)  req/s (median)  spread  p50  p99   p99.9  allocs  B/op",
			3, // fourth data field: framework, best, median, then spread
		},
		{
			"framework  req/s (best)  req/s (median)  spread  p50  p99   allocs  B/op",
			3, // same position without p99.9
		},
		{
			"conns  req/s    p50    p90    p99    p99.9  max",
			-1, // no spread column at all
		},
		{
			"framework  spread  p50",
			1, // "spread" is the second column
		},
	}
	for _, tc := range tests {
		if got := spreadOrdinal(tc.header); got != tc.want {
			t.Errorf("spreadOrdinal(%q) = %d, want %d", tc.header, got, tc.want)
		}
	}
}

func writeTemp(t *testing.T, s string) string {
	t.Helper()
	path := t.TempDir() + "/results.txt"
	if err := os.WriteFile(path, []byte(s), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}
