// Command api is a small JSON API: grouped routes with middleware, JSON in and
// out, structured errors, and authentication on part of the tree.
//
// Run it:
//
//	go run ./examples/api
//
// Then:
//
//	curl localhost:8080/v1/notes
//	curl -X POST localhost:8080/v1/notes -H 'Authorization: Bearer demo' \
//	     -H 'Content-Type: application/json' -d '{"text":"buy milk"}'
//	curl localhost:8080/v1/notes/1
//	curl -X DELETE localhost:8080/v1/notes/1 -H 'Authorization: Bearer demo'
//
// What this example is for:
//
//   - Middleware is plain net/http, so existing middleware drops in unchanged.
//     Use applies to every request, 404s included.
//   - A Group shares a prefix and middleware; authentication lives on a group
//     so a route cannot forget it.
//   - DecodeJSON and Error turn every bad input into the right status - 400,
//     401, 404, 413, 415 - with a body that never echoes the request.
package main

import (
	"crypto/subtle"
	"log/slog"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/agenticmarket/photon"
	"github.com/agenticmarket/photon/photonerr"
)

type note struct {
	ID      int       `json:"id"`
	Text    string    `json:"text"`
	Created time.Time `json:"created"`
}

// store is an in-memory note store. photon has no models and no database: it is
// an HTTP library, not a framework.
type store struct {
	mu    sync.Mutex
	next  int
	notes map[int]note
}

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	app := photon.New(photon.Logger(logger))
	s := &store{notes: map[int]note{}}

	app.Use(requestLog(logger)) // every request, including 404 and 405

	v1 := app.Group("/v1")
	v1.GET("/notes", s.list)
	v1.GET("/notes/:id", s.get)

	// Writes need a key. The group's middleware runs only for its routes.
	writes := v1.Group("/notes", bearer("demo"))
	writes.POST("/", s.create) // "/" in a group is the group's own path: POST /v1/notes
	writes.DELETE("/:id", s.remove)

	logger.Info("listening on http://localhost:8080")
	if err := app.Run(":8080"); err != nil {
		logger.Error("server stopped", "err", err)
		os.Exit(1)
	}
}

func (s *store) list(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	out := make([]note, 0, len(s.notes))
	for _, n := range s.notes {
		out = append(out, n)
	}
	s.mu.Unlock()
	_ = photon.JSON(w, http.StatusOK, out)
}

func (s *store) get(w http.ResponseWriter, r *http.Request) {
	n, err := s.lookup(r)
	if err != nil {
		photon.Error(w, r, err)
		return
	}
	_ = photon.JSON(w, http.StatusOK, n)
}

func (s *store) create(w http.ResponseWriter, r *http.Request) {
	var in struct {
		Text string `json:"text"`
	}
	// 415 for a non-JSON body, 400 for malformed JSON, 413 past the 1 MiB
	// default limit - each with a safe message.
	if err := photon.DecodeJSON(r, &in); err != nil {
		photon.Error(w, r, err)
		return
	}
	in.Text = strings.TrimSpace(in.Text)
	if in.Text == "" || len(in.Text) > 1000 {
		photon.Error(w, r, photonerr.BadRequest("text must be 1 to 1000 characters"))
		return
	}
	s.mu.Lock()
	s.next++
	n := note{ID: s.next, Text: in.Text, Created: time.Now().UTC()}
	s.notes[n.ID] = n
	s.mu.Unlock()

	w.Header().Set("Location", "/v1/notes/"+strconv.Itoa(n.ID))
	_ = photon.JSON(w, http.StatusCreated, n)
}

func (s *store) remove(w http.ResponseWriter, r *http.Request) {
	n, err := s.lookup(r)
	if err != nil {
		photon.Error(w, r, err)
		return
	}
	s.mu.Lock()
	delete(s.notes, n.ID)
	s.mu.Unlock()
	_ = photon.NoContent(w)
}

// lookup finds the note named by the :id path parameter.
func (s *store) lookup(r *http.Request) (note, error) {
	id, err := strconv.Atoi(photon.PathParam(r, "id"))
	if err != nil || id < 1 {
		return note{}, photonerr.BadRequest("id must be a positive integer")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	n, ok := s.notes[id]
	if !ok {
		// "note not found" - the message names the kind of thing, never the
		// id the client sent, so error bodies cannot reflect request content.
		return note{}, photonerr.NotFound("note")
	}
	return n, nil
}

// bearer requires an exact bearer token. A real service would look the token
// up; the constant-time comparison is the part worth copying.
func bearer(token string) photon.Middleware {
	want := []byte(token)
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			got, ok := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer ")
			if !ok || subtle.ConstantTimeCompare([]byte(got), want) != 1 {
				w.Header().Set("WWW-Authenticate", "Bearer")
				photon.Error(w, r, photonerr.Unauthorized("a valid bearer token is required"))
				return
			}
			next.ServeHTTP(w, r)
		})
	}
}

// requestLog is ordinary net/http middleware. statusWriter records the status
// code and implements Unwrap, so http.ResponseController - and therefore
// streaming - still reaches the underlying writer.
func requestLog(logger *slog.Logger) photon.Middleware {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			start := time.Now()
			sw := &statusWriter{ResponseWriter: w, status: http.StatusOK}
			next.ServeHTTP(sw, r)
			logger.Info("request",
				"method", r.Method,
				"route", r.Pattern, // the matched template, e.g. /v1/notes/:id
				"status", sw.status,
				"peer", photon.ClientIP(r).String(),
				"took", time.Since(start).Round(time.Microsecond))
		})
	}
}

type statusWriter struct {
	http.ResponseWriter
	status int
}

func (w *statusWriter) WriteHeader(code int) {
	w.status = code
	w.ResponseWriter.WriteHeader(code)
}

func (w *statusWriter) Unwrap() http.ResponseWriter { return w.ResponseWriter }
