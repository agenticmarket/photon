package sse

import "runtime"

// readMem reports the process's peak resident set on Windows.
//
// MemStats.Sys rather than HeapAlloc, for the reason given in mem_unix.go: Sys
// is what the OS committed to the process, which is comparable across runtimes,
// whereas HeapAlloc would reward a small runtime library rather than a fast one.
func readMem() int64 {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	return int64(m.Sys)
}
