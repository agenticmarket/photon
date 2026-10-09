// Command go_nethttp serves the benchmark stream with net/http's ServeMux and
// no framework at all.
//
// This is the floor, and it is the most important row in the table. Without it,
// "photon vs chi" is a comparison of two stacks; with it, the cost of the
// framework layer itself is visible. A framework that cannot beat ServeMux on
// this workload is not slow - it has nothing to add, which is a legitimate and
// worth-reporting finding for SSE.
package main

import (
	"net/http"

	"streambench/gosrv"
)

func main() {
	mux := http.NewServeMux()
	mux.Handle("/stream", gosrv.NewStreamHandler())
	mux.Handle("/mem", gosrv.NewMemHandler())
	gosrv.ListenReady("net/http", mux)
}
