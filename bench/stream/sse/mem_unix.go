//go:build !windows

package sse

import "runtime"

// readMem reports the process's peak resident set.
//
// MemStats.Sys rather than HeapAlloc: HeapAlloc is live Go heap, which would
// make a server that ships a large runtime library look smaller than one that
// does not, even though both process the same bytes. Sys is what the OS
// committed to the process, which is the comparable quantity across runtimes.
func readMem() int64 {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	return int64(m.Sys)
}
