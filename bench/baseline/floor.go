package main

// Measurement floor: the smallest performance difference this host can resolve.
//
// It is published (BENCHMARKS.md) so that a regression is distinguishable from
// noise. The point is not to produce a number, it is to stop every published
// number from being read as if it were exact.
//
// # Why the floor is computed and not chosen
//
// A floor written down by hand drifts. Someone runs the benchmark on a quieter
// day, gets a 4% spread, and "updates" the floor to 5% - and the floor now
// certifies that a 6% regression is real, when the honest floor is 21%. A floor
// that can be relaxed by a good run is not a floor.
//
// So this reads the spread column out of the committed results files and
// reports the distribution. It is derived from the numbers that were actually
// published, which is the only definition of a floor that cannot be argued with.
//
// # Why the median and not the mean or the max
//
// The median is used as the headline because it is the value below which half
// the measurements fall, and it is not moved by a single pathological cell -
// which on this host is echo, whose spreads run 3.9% to 6.2% while everything
// else sits above 30%. A mean would be dragged by that outlier; a max would
// simply report the worst afternoon.
//
// The max is still reported. It answers the question a reader actually has
// ("how wrong could this be?") and it is the number to quote when defending a
// claim against someone who measured a different number.

// spreadSample is one measured spread, with enough provenance to be traced
// back to the file and cell it came from.
type spreadSample struct {
	source   string // results file
	workload string // the section heading
	subject  string // framework name
	spread   float64
}

// floorReport is the published distribution.
type floorReport struct {
	Samples []spreadSample

	Min    float64
	Median float64
	Mean   float64
	Max    float64

	// PerSubject is the worst spread each framework produced, which is the
	// number that matters for a claim about that framework specifically. A
	// framework that is usually quiet and occasionally 38% is not a quiet
	// framework.
	PerSubject map[string]float64
}

// interpret renders the rule the floor implies, in the form a reader applies to
// two numbers in the audit.
//
// This is the deliverable. "The floor is 21%" is trivia; "a difference below
// 21% is not a result, and here is how to tell which side of that a pair of
// numbers falls on" is usable.
func (r floorReport) interpret() string {
	var b []byte
	add := func(format string, args ...any) {
		b = append(b, []byte(sprintf(format, args...))...)
		b = append(b, '\n')
	}

	add("measurement floor on this host")
	add("  samples        %d", len(r.Samples))
	add("  min            %.1f%%", r.Min)
	add("  median         %.1f%%  <- the floor", r.Median)
	add("  mean           %.1f%%", r.Mean)
	add("  max            %.1f%%", r.Max)
	add("")
	add("rule: two runs of the same workload differing by less than the median")
	add("spread are not distinguishable from noise. A 10%% difference on a")
	add("workload whose own repetitions vary by %.0f%% is not a result; it is a", r.Median)
	add("different afternoon.")
	add("")
	add("per-subject worst spread, which is what to quote for a claim about one:")
	for _, s := range sortedPairs(r.PerSubject) {
		add("  %-12s %.1f%%", s.k, s.v)
	}
	return string(b)
}
