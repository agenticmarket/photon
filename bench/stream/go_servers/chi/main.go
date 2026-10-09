// Command go_chi serves the benchmark stream with chi.
//
// chi has no streaming helper, so the handler writes the shared frame directly
// and flushes per event - which is the only way any of the non-photon servers
// can stream, and therefore the behaviour every competitor is measured at.
package main

import (
	"github.com/go-chi/chi/v5"

	"streambench/gosrv"
)

func main() {
	r := chi.NewRouter()
	r.Handle("/stream", gosrv.NewStreamHandler())
	r.Handle("/mem", gosrv.NewMemHandler())
	gosrv.ListenReady("chi", r)
}
