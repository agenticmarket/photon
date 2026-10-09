// Command hello is the smallest useful photon server.
//
// Run it:
//
//	go run ./examples/hello
//
// Then:
//
//	curl http://localhost:8080/greet/Ada
//	curl http://localhost:8080/health
//	curl -N http://localhost:8080/count
//
// It is deliberately small enough to read in one sitting: a path parameter, a
// JSON response, a token stream, and graceful shutdown - on a server that is
// already secure without configuration.
package main

import (
	"log"
	"net/http"
	"strconv"
	"time"

	"github.com/agenticmarket/photon"
)

func main() {
	app := photon.New() // secure defaults: body, header, and stream limits are already on

	// :name matches one path segment. Handlers are ordinary net/http
	// functions, so any existing handler or middleware works here.
	app.GET("/greet/:name", func(w http.ResponseWriter, r *http.Request) {
		name := photon.PathParam(r, "name") // or r.PathValue("name"), the net/http way
		_ = photon.JSON(w, http.StatusOK, map[string]string{"message": "Hello, " + name + "!"})
	})

	// A static route costs zero allocations inside photon.
	app.GET("/health", func(w http.ResponseWriter, r *http.Request) {
		_ = photon.JSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})

	// A server-sent event stream. Each event reaches the client as soon as it
	// is written; if the client disconnects, Event returns an error and the
	// handler stops.
	app.GET("/count", photon.SSE(func(s *photon.Stream, r *http.Request) error {
		for i := 1; i <= 5; i++ {
			if err := s.Event("count", []byte(strconv.Itoa(i))); err != nil {
				return err
			}
			time.Sleep(300 * time.Millisecond)
		}
		return s.Event("done", nil)
	}))

	// Run listens, and on Ctrl-C or SIGTERM stops accepting connections and
	// lets in-flight requests and streams finish before returning.
	log.Println("listening on http://localhost:8080")
	if err := app.Run(":8080"); err != nil {
		log.Fatal(err)
	}
}
