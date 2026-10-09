package main

import (
	"io"
	"net/http"
	"strconv"
	"strings"

	"github.com/gin-gonic/gin"
	"github.com/go-chi/chi/v5"
	"github.com/labstack/echo/v4"

	"github.com/agenticmarket/photon"
)

// This file defines one handler per framework for each workload. The rule is
// that every framework produces byte-identical responses on identical paths.
// A framework that cannot serve a workload identically is excluded from that
// workload's table rather than measured against a different one, because
// comparing different responses measures nothing.

// responseSpec describes what a workload's response should be.
type responseSpec struct {
	contentType string
	body        []byte
	// readBody is set for request-body workloads, so every framework reads
	// exactly the same number of bytes.
	readBody bool
	status   int
}

const (
	ctPlain = "text/plain; charset=utf-8"
	ctJSON  = "application/json; charset=utf-8"
)

func (s responseSpec) write(w http.ResponseWriter) {
	w.Header().Set("Content-Type", s.contentType)
	w.Header().Set("Content-Length", strconv.Itoa(len(s.body)))
	w.WriteHeader(s.status)
	_, _ = w.Write(s.body)
}

var (
	specPlain = responseSpec{contentType: ctPlain, body: []byte("Hello World"), status: 200}
	specJSON  = responseSpec{contentType: ctJSON, body: []byte(`{"status":"ok"}`), status: 200}
	specNoop  = responseSpec{status: 204}
)

// specsByWorkload is the single source of truth for what each workload
// returns. Every framework is built from this, so no framework can be
// accidentally given an easier response.
var specsByWorkload = map[string]responseSpec{
	"plain":         specPlain,
	"json":          specJSON,
	"param":         {contentType: ctJSON, status: 200},
	"resp-1kb":      {contentType: ctJSON, body: payload(1 << 10), status: 200},
	"resp-10kb":     {contentType: ctJSON, body: payload(10 << 10), status: 200},
	"resp-100kb":    {contentType: ctJSON, body: payload(100 << 10), status: 200},
	"noop":          specNoop,
	"req-1kb":       {readBody: true, status: 204},
	"req-10kb":      {readBody: true, status: 204},
	"req-100kb":     {readBody: true, status: 204},
	"req-1mb":       {readBody: true, status: 204},
	"param-missing": specNoop,
}

// workloadTarget is the path every framework registers for a workload.
func workloadTarget(w workload) string {
	switch w.name {
	case "plain", "noop":
		return "/"
	case "json":
		return "/health"
	case "param", "param-missing":
		return "/users/:id"
	default:
		return "/" + w.name
	}
}

// concretePath turns a workload into the request path a client sends.
func concretePath(w workload) string {
	switch w.name {
	case "plain", "noop":
		return "/"
	case "json":
		return "/health"
	case "param":
		return "/users/123456"
	case "param-missing":
		return "/users/123456/extra"
	default:
		return "/" + w.name
	}
}

// buildRoutes registers a workload on a framework-agnostic registrar.
//
// Each framework gets its own adapter below; this shared shape keeps them
// from drifting apart.
type registrar interface {
	handle(method, pattern string, h http.Handler)
	middleware(n int)
}

// ------------------------------------------------------------------- Photon

// workloadPattern converts a workload to each framework's own pattern syntax.
//
// chi uses "{id}" while photon, Gin, and Echo all use ":id", and net/http's
// ServeMux uses "{id}" with a stricter grammar. Getting this wrong yields a
// 404, which is why the equivalence test exists.
func workloadPattern(w workload, syntax int) string {
	const (
		colon = iota // photon, gin, echo
		brace        // chi, net/http
	)
	p := workloadTarget(w)
	if syntax == brace {
		return strings.ReplaceAll(p, ":id", "{id}")
	}
	return p
}

func newPhoton(w workload, mw int) http.Handler {
	rt := photon.NewRouter()
	spec := specsByWorkload[w.name]

	h := http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		if spec.readBody {
			_, _ = drain(r.Body)
			rw.WriteHeader(204)
			return
		}
		if w.name == "param" {
			body := []byte(`{"id":"` + paramID(r) + `"}`)
			rw.Header().Set("Content-Type", ctJSON)
			rw.Header().Set("Content-Length", strconv.Itoa(len(body)))
			_, _ = rw.Write(body)
			return
		}
		spec.write(rw)
	})
	// The method must match the workload, or every POST workload 405s on
	// photon and 404s on the frameworks that do not fall back to GET.
	rt.Handle(w.method, workloadPattern(w, 0), h, noopMw(mw)...)
	return rt
}

// paramID reads the id from a photon request.
func paramID(r *http.Request) string { return photon.PathParam(r, "id") }

// ------------------------------------------------------------ middleware

// noopMw returns n no-op middlewares. Each one wraps and calls through, so
// the cost measured is the dispatch itself rather than any work a middleware
// might do.
func noopMw(n int) []photon.Middleware {
	out := make([]photon.Middleware, n)
	for i := range out {
		out[i] = func(next http.Handler) http.Handler {
			return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				next.ServeHTTP(w, r)
			})
		}
	}
	return out
}

// ------------------------------------------------------------------- net/http

type stdTarget struct{ mux *http.ServeMux }

func (t *stdTarget) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	t.mux.ServeHTTP(w, r)
}

func newStd(w workload, mw int) *stdTarget {
	t := &stdTarget{mux: http.NewServeMux()}
	spec := specsByWorkload[w.name]

	var h http.Handler = http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		if spec.readBody {
			_, _ = drain(r.Body)
			rw.WriteHeader(204)
			return
		}
		if w.name == "param" {
			id := r.PathValue("id")
			body := []byte(`{"id":"` + id + `"}`)
			rw.Header().Set("Content-Type", ctJSON)
			rw.Header().Set("Content-Length", strconv.Itoa(len(body)))
			_, _ = rw.Write(body)
			return
		}
		spec.write(rw)
	})
	for i := 0; i < mw; i++ {
		h = noopStd(h)
	}
	t.mux.Handle(w.method+" "+workloadPattern(w, 1), h)
	return t
}

func noopStd(next http.Handler) http.Handler { return next }

// ---------------------------------------------------------------------- Gin

func newGin(w workload, mw int) (http.Handler, error) {
	gin.SetMode(gin.ReleaseMode)
	g := gin.New()
	spec := specsByWorkload[w.name]

	// Gin and chi both require middleware to be registered before any route.
	// Gin silently ignores middleware added after a route; chi panics. That
	// difference is a behavioural finding of this harness, independent of
	// performance, and the ordering is enforced here for every framework so the
	// comparison is not accidentally unfair.
	for i := 0; i < mw; i++ {
		g.Use(func(c *gin.Context) { c.Next() })
	}

	// Gin takes a handler function rather than an http.Handler, so the body
	// is wrapped here rather than changing the shared response spec.
	g.Handle(w.method, workloadPattern(w, 0), func(c *gin.Context) {
		if spec.readBody {
			_, _ = drain(c.Request.Body)
			c.Status(204)
			return
		}
		if w.name == "param" {
			c.Header("Content-Type", ctJSON)
			c.String(200, `{"id":"`+c.Param("id")+`"}`)
			return
		}
		c.Header("Content-Type", spec.contentType)
		c.String(spec.status, string(spec.body))
	})
	return g, nil
}

// -------------------------------------------------------------------- Echo

func newEcho(w workload, mw int) (http.Handler, error) {
	e := echo.New()
	e.HideBanner = true
	e.HidePort = true
	spec := specsByWorkload[w.name]

	for i := 0; i < mw; i++ {
		e.Use(func(next echo.HandlerFunc) echo.HandlerFunc {
			return func(c echo.Context) error { return next(c) }
		})
	}

	// Echo uses the same ":id" parameter syntax as photon.
	e.Add(w.method, workloadPattern(w, 0), func(c echo.Context) error {
		if spec.readBody {
			_, _ = drain(c.Request().Body)
			return c.NoContent(204)
		}
		if w.name == "param" {
			c.Response().Header().Set("Content-Type", ctJSON)
			return c.String(200, `{"id":"`+c.Param("id")+`"}`)
		}
		c.Response().Header().Set("Content-Type", spec.contentType)
		return c.Blob(spec.status, spec.contentType, spec.body)
	})
	return e, nil
}

// -------------------------------------------------------------------- chi

func newChi(w workload, mw int) http.Handler {
	r := chi.NewRouter()
	spec := specsByWorkload[w.name]

	// chi requires all middleware to be registered before any route: Use after
	// a route panics by design. This is a real API constraint, discovered by
	// the harness rather than by documentation, and it is why middleware is
	// registered first here for every framework.
	for i := 0; i < mw; i++ {
		r.Use(func(next http.Handler) http.Handler { return next })
	}

	r.Method(w.method, workloadPattern(w, 1), http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		if spec.readBody {
			_, _ = drain(r.Body)
			rw.WriteHeader(204)
			return
		}
		if w.name == "param" {
			id := chi.URLParam(r, "id")
			body := []byte(`{"id":"` + id + `"}`)
			rw.Header().Set("Content-Type", ctJSON)
			rw.Header().Set("Content-Length", strconv.Itoa(len(body)))
			_, _ = rw.Write(body)
			return
		}
		spec.write(rw)
	}))
	return r
}

// ------------------------------------------------------------------ helpers

// drain reads r to completion, so every framework reads the same number of
// body bytes and the comparison is about the framework rather than about who
// skipped the work.
func drain(r io.Reader) (int64, error) {
	buf := make([]byte, 32<<10)
	var total int64
	for {
		n, err := r.Read(buf)
		total += int64(n)
		if err != nil {
			return total, nil
		}
	}
}
