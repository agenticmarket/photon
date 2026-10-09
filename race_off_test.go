//go:build !race

package photon

// raceEnabled reports whether the binary was built with -race. See
// race_on_test.go for why the allocation budgets need to know.
const raceEnabled = false
