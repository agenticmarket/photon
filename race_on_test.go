//go:build race

package photon

// raceEnabled reports whether the binary was built with -race.
//
// The race detector instruments every memory access, which adds allocations
// that a normal build does not make. Allocation-budget tests therefore measure
// the detector's overhead rather than the code's, and would fail spuriously.
// The budget tests check this flag and skip only the numeric assertion; the
// code under test still runs, and the race detector still checks it for races.
const raceEnabled = true
