// Package photon is a small, secure HTTP server for AI agents and streaming
// APIs.
//
// It is built on net/http, so every handler is an ordinary http.Handler and
// every existing net/http middleware composes unchanged. What it adds is the
// part a streaming AI backend otherwise writes by hand: bounded, cancellable
// server-sent events; resource limits that are on by default; strict routing
// that refuses ambiguous routes at startup; and graceful shutdown that lets a
// stream finish its answer.
//
//	app := photon.New()
//
//	app.GET("/users/:id", getUser)
//	app.POST("/chat", photon.SSE(chat))
//
//	app.Run(":8080")
//
// Removing photon leaves code that still compiles against net/http.
package photon

import (
	"io"
	"net/http"
	"net/url"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/agenticmarket/photon/internal/tree"
	"github.com/agenticmarket/photon/photonerr"
)

// Registration errors. Route registration panics rather than returning an
// error, because a bad route is a programming mistake that should stop the
// program at startup; the panic value matches one of these with errors.Is, so
// a test can assert on the kind of mistake without parsing the message.
var (
	// ErrRouteConflict matches a panic from registering a route that could
	// serve the same request as one already registered.
	ErrRouteConflict = tree.ErrConflict
	// ErrBadPattern matches a panic from registering a malformed pattern.
	ErrBadPattern = tree.ErrMalformed
)

// Middleware wraps a handler with additional behaviour. Middleware is plain
// net/http: no photon-specific interface, so any existing middleware composes.
//
// A chain of (a, b, c) runs as a(b(c(handler))), outermost first.
type Middleware func(http.Handler) http.Handler

// RouteInfo describes one registered route. It is returned by Routes for
// documentation, tests, and metrics.
type RouteInfo struct {
	// Method is the HTTP method this route handles.
	Method string
	// Path is the pattern as registered, for example "/users/:id".
	Path string
	// Params names the route's path parameters, in capture order.
	Params []string
}

// staticEntry is a fully-static route, held in a flat map so the common case
// costs one hash lookup and never enters the tree.
type staticEntry struct {
	handler http.Handler
}

// methodRoutes holds the routes for one HTTP method.
//
// Wildcard routes live in the tree. Fully-static routes live only in the flat
// map, because inserting 10,000 of them into the tree made every parametrised
// request scan 10,000 siblings: 16 us per request, against 1.5 us with them
// excluded. The tree's children are kept sorted, so the residual cost of the
// wildcards that remain is logarithmic.
//
// The two structures are not independent sources of truth: registration
// compares a static pattern against the tree's wildcards, and the tree's
// conflict rules cover a wildcard against a literal. That is what keeps
// "/admin/settings" from coexisting with "/admin/*rest".
type methodRoutes struct {
	// static indexes fully-static routes by path. It is nil when the method
	// has none, so a parametrised-only method skips the map lookup entirely.
	static map[string]*staticEntry

	// tree holds the wildcard routes for this method, and serves wildcard
	// lookups.
	tree *tree.Tree

	// staticRoutes holds the static routes with their patterns parsed once,
	// for comparing a newly registered wildcard against them.
	staticRoutes []*tree.Route
}

// routeRecord remembers a route's handler before global middleware is applied,
// so Use can rebuild every route's chain whenever it is called.
type routeRecord struct {
	base http.Handler
	set  func(http.Handler)
}

// Router dispatches requests to handlers by method and path.
//
// Register every route before serving. Serve freezes the router, after which
// registration panics and lookups take no lock at all.
type Router struct {
	mu      sync.RWMutex
	methods map[string]*methodRoutes
	// order preserves registration order for Routes, so generated
	// documentation lists routes the way the program declares them.
	order []RouteInfo
	// records holds every route's pre-middleware handler, so global middleware
	// can be applied regardless of whether it was added before or after the
	// route.
	records []routeRecord
	// global is the middleware added with Use, applied to every route and to
	// the 404 and 405 responses.
	global []Middleware

	// frozen is set by the first Serve. After it, the routing tables are
	// immutable, which is what lets ServeHTTP read them without a lock: an
	// RWMutex read lock is a write to a shared cache line, and on a many-core
	// machine every request contending for that line is the bottleneck.
	frozen atomic.Bool

	// notFound and methodNotAllowed replace the default 404 and 405 responses.
	notFound         http.Handler
	methodNotAllowed http.Handler

	// miss is the compiled 404/405 handler, global middleware included.
	miss http.Handler

	// mounts are handlers serving a path prefix, longest prefix first,
	// consulted when no route matches.
	mounts []*mountEntry
}

// NewRouter returns an empty Router.
func NewRouter() *Router {
	rt := &Router{methods: make(map[string]*methodRoutes)}
	rt.miss = http.HandlerFunc(rt.serveMiss)
	return rt
}

// GET registers a handler for GET requests to path.
//
// h is an http.HandlerFunc, so a plain function, a method value such as
// mux.ServeHTTP, and photon.HandlerFunc(f) are all accepted. Use Handle to
// register an http.Handler value directly.
func (rt *Router) GET(path string, h http.HandlerFunc, mw ...Middleware) {
	rt.handle(http.MethodGet, path, handlerOf(h), mw)
}

// POST registers a handler for POST requests to path.
func (rt *Router) POST(path string, h http.HandlerFunc, mw ...Middleware) {
	rt.handle(http.MethodPost, path, handlerOf(h), mw)
}

// PUT registers a handler for PUT requests to path.
func (rt *Router) PUT(path string, h http.HandlerFunc, mw ...Middleware) {
	rt.handle(http.MethodPut, path, handlerOf(h), mw)
}

// PATCH registers a handler for PATCH requests to path.
func (rt *Router) PATCH(path string, h http.HandlerFunc, mw ...Middleware) {
	rt.handle(http.MethodPatch, path, handlerOf(h), mw)
}

// DELETE registers a handler for DELETE requests to path.
func (rt *Router) DELETE(path string, h http.HandlerFunc, mw ...Middleware) {
	rt.handle(http.MethodDelete, path, handlerOf(h), mw)
}

// HEAD registers a handler for HEAD requests to path. It is not derived from
// GET: a HEAD handler that writes a body produces a response that disagrees
// with its own headers, so the choice stays explicit.
func (rt *Router) HEAD(path string, h http.HandlerFunc, mw ...Middleware) {
	rt.handle(http.MethodHead, path, handlerOf(h), mw)
}

// OPTIONS registers a handler for OPTIONS requests to path.
func (rt *Router) OPTIONS(path string, h http.HandlerFunc, mw ...Middleware) {
	rt.handle(http.MethodOptions, path, handlerOf(h), mw)
}

// handlerOf converts a possibly-nil HandlerFunc to a possibly-nil Handler.
//
// A nil func converted directly to http.Handler is a non-nil interface holding
// a nil func, which would pass the nil check at registration and panic on the
// first request instead.
func handlerOf(f http.HandlerFunc) http.Handler {
	if f == nil {
		return nil
	}
	return f
}

// Handle registers a handler for the given method and path.
//
// It panics if the pattern is malformed or conflicts with an existing route.
// A conflicting route is a programming mistake that must surface in a test
// rather than a request-dependent choice made at runtime, where a typo becomes
// an authorization bug.
//
// Per-route middleware is composed around h and stored with the route, so the
// hot path serves an already-built chain.
func (rt *Router) Handle(method, path string, h http.Handler, mw ...Middleware) {
	rt.handle(method, path, h, mw)
}

// handle registers a route. Every public entry point funnels through here; the
// registration site for diagnostics is found by walking the stack to the first
// frame outside this package, so the depth of the call chain does not matter.
func (rt *Router) handle(method, path string, h http.Handler, mw []Middleware) {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	rt.handleLocked(method, path, h, mw)
}

// handleLocked performs registration with rt.mu held.
func (rt *Router) handleLocked(method, path string, h http.Handler, mw []Middleware) {
	if rt.frozen.Load() {
		panic("photon: the router is frozen because it has started serving, so routes cannot be " +
			"registered: register every route during setup, before Listen, Run, or Serve")
	}
	if method == "" {
		panic("photon: empty method for " + strconv.Quote(path))
	}
	if h == nil {
		panic("photon: nil handler for " + strconv.Quote(method) + " " + strconv.Quote(path))
	}

	base := h
	if len(mw) > 0 {
		base = chain(mw, h)
	}
	final := base
	if len(rt.global) > 0 {
		// Global middleware runs outside per-route middleware, so a route's
		// own middleware sees a request the global chain has already
		// decorated.
		final = chain(rt.global, base)
	}

	mr := rt.methods[method]
	if mr == nil {
		mr = &methodRoutes{}
		rt.methods[method] = mr
	}

	// A fully-static pattern is served from the flat map and never enters the
	// tree, because putting 10,000 static routes into the tree would make
	// every parametrised request scan 10,000 siblings: measured at 16 us per
	// request, versus 1.5 us with them excluded. The tree is for wildcards
	// only, and the map is O(1) for everything else.
	//
	// The static route is still compared against the tree's wildcards, or a
	// conflict like "/admin/settings" against "/admin/*rest" would go
	// unnoticed.
	pat, err := tree.ParsePattern(path)
	if err != nil {
		panic(err)
	}

	if mr.tree == nil {
		mr.tree = tree.New()
	}
	site := registrationSite()
	r := &tree.Route{Method: method, Path: path, Handler: final, Caller: site}

	if pat.Static() {
		if _, dup := mr.static[path]; dup {
			panic(&tree.ConflictError{
				New:      &tree.Route{Method: method, Path: path, Caller: site},
				Existing: &tree.Route{Method: method, Path: path},
				Reason:   "the same path is already registered for this method",
			})
		}
		// A static route can only conflict with a wildcard: two different
		// literal paths never match the same request, and an identical one
		// was refused above. So it is checked against the tree alone, which
		// keeps registering N static routes linear rather than quadratic.
		if err := mr.tree.CheckConflictWith(r, nil); err != nil {
			panic(err)
		}
		if mr.static == nil {
			mr.static = make(map[string]*staticEntry)
		}
		e := &staticEntry{handler: final}
		mr.static[path] = e
		mr.staticRoutes = append(mr.staticRoutes, r)
		rt.order = append(rt.order, RouteInfo{Method: method, Path: path})
		rt.records = append(rt.records, routeRecord{base: base, set: func(h http.Handler) { e.handler = h }})
		return
	}
	// A wildcard is compared against the wildcards in the tree and against
	// every static route, because a pattern in the map is invisible to the
	// tree. Without this, "/admin/settings" and "/admin/*rest" would coexist
	// depending on which was registered first.
	if err := mr.tree.CheckConflictWith(r, mr.staticRoutes); err != nil {
		panic(err)
	}
	if err := mr.tree.Add(r); err != nil {
		panic(err)
	}
	rememberPattern(path, pat.ParamNames())
	rt.order = append(rt.order, RouteInfo{
		Method: method, Path: path, Params: pat.ParamNames(),
	})
	rt.records = append(rt.records, routeRecord{base: base, set: func(h http.Handler) { r.Handler = h }})
}

// chain composes middleware around h, outermost first.
func chain(mw []Middleware, h http.Handler) http.Handler {
	for i := len(mw) - 1; i >= 0; i-- {
		h = mw[i](h)
	}
	return h
}

// Use adds middleware that runs for every request the router answers: every
// route, whether registered before or after this call, and the 404 and 405
// responses.
//
// Applying it to misses is deliberate. CORS must answer a preflight OPTIONS
// for a route that only registered POST, a rate limiter must count a flood of
// 404s, and an access log must record them; middleware that silently skipped
// those requests would be a security check that quietly does not run.
//
// Global middleware runs outside group and per-route middleware, in the order
// Use was called. It runs after routing, so PathParam works inside it.
func (rt *Router) Use(mw ...Middleware) {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	if rt.frozen.Load() {
		panic("photon: the router is frozen because it has started serving, so middleware cannot be added")
	}
	rt.global = append(rt.global, mw...)
	// Recompile every route and the miss handler. This is registration-time
	// work proportional to the number of routes, never per request.
	for _, rec := range rt.records {
		rec.set(chain(rt.global, rec.base))
	}
	rt.miss = chain(rt.global, http.HandlerFunc(rt.serveMiss))
}

// NotFound sets the handler used when no route matches the path. Global
// middleware still runs around it.
func (rt *Router) NotFound(h http.Handler) {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	rt.mustNotBeFrozen("NotFound")
	rt.notFound = h
}

// MethodNotAllowed sets the handler used when the path matches a route under a
// different method. The Allow header is set before it runs.
func (rt *Router) MethodNotAllowed(h http.Handler) {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	rt.mustNotBeFrozen("MethodNotAllowed")
	rt.methodNotAllowed = h
}

// mustNotBeFrozen panics if the router is serving. Swapping a handler that
// ServeHTTP reads without a lock would be a data race on a live server.
func (rt *Router) mustNotBeFrozen(what string) {
	if rt.frozen.Load() {
		panic("photon: the router is frozen because it has started serving, so " + what + " cannot be changed")
	}
}

// Freeze makes the router immutable. Serve calls it before the first request.
// Registering afterwards panics, and lookups stop taking a lock.
func (rt *Router) Freeze() {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	rt.frozen.Store(true)
}

// Routes returns the registered routes in registration order. The returned
// slice is a copy; modifying it does not affect the router.
func (rt *Router) Routes() []RouteInfo {
	rt.mu.RLock()
	defer rt.mu.RUnlock()
	out := make([]RouteInfo, len(rt.order))
	copy(out, rt.order)
	return out
}

// Lookup resolves method and path to a handler, returning the Allow header
// value when the path matches only under other methods.
//
// It is exported for tests and for tooling that generates documentation; the
// server does not use it. The returned parameters are a copy and safe to keep.
func (rt *Router) Lookup(method, path string) (http.Handler, []Param, []string) {
	if !rt.frozen.Load() {
		rt.mu.RLock()
		defer rt.mu.RUnlock()
	}
	scratch := paramsPool.Get().(*tree.Params)
	defer paramsPool.Put(scratch)
	h, _, params := rt.find(method, path, scratch)
	if h == nil {
		return nil, nil, rt.allow(path)
	}
	if len(params) > 0 {
		params = append([]Param(nil), params...)
	}
	return h, params, nil
}

// find resolves a request. The caller holds rt.mu or the router is frozen.
//
// path is the raw request path: routing on the raw form is what stops an
// encoded slash from acting as a segment separator.
//
// scratch must belong to the calling goroutine. The captures it holds alias
// the request path, so a buffer shared between concurrent requests would let
// one request observe another request's path. The returned captures are only
// valid until scratch is reused.
//
// A miss returns a nil handler and no Allow list: the 405 path computes that
// itself, so a successful dispatch never pays for it.
func (rt *Router) find(method, path string, scratch *tree.Params) (h http.Handler, pattern string, params []Param) {
	if mr := rt.methods[method]; mr != nil {
		// Hot path: a fully-static route is one map lookup and no tree walk.
		if mr.static != nil {
			if e, ok := mr.static[path]; ok {
				return e.handler, path, nil
			}
		}
		if mr.tree != nil {
			if r := mr.tree.Find(path, scratch); r != nil {
				return r.Handler, r.Path, scratch.All()
			}
		}
	}
	// Routes take precedence; a mount serves what they leave.
	if m := rt.mountFor(path); m != nil {
		return m.handler, m.pattern, nil
	}
	return nil, "", nil
}

// sortStrings sorts s in place. It is an insertion sort because the candidate
// list is the set of HTTP methods, which is at most a handful of entries.
func sortStrings(s []string) {
	for i := 1; i < len(s); i++ {
		for j := i; j > 0 && s[j] < s[j-1]; j-- {
			s[j], s[j-1] = s[j-1], s[j]
		}
	}
}

// allow returns the methods under which path matches, sorted, for the Allow
// header of a 405.
//
// This is the most expensive path in the router, because it consults every
// method's tree. It runs only when the requested method did not match, which
// is a 404 or a 405, and never on a successful dispatch.
func (rt *Router) allow(path string) []string {
	var out []string
	scratch := paramsPool.Get().(*tree.Params)
	defer paramsPool.Put(scratch)
	for method, mr := range rt.methods {
		if mr.static != nil {
			if _, ok := mr.static[path]; ok {
				out = append(out, method)
				continue
			}
		}
		if mr.tree != nil && mr.tree.Find(path, scratch) != nil {
			out = append(out, method)
		}
	}
	sortStrings(out)
	return out
}

// ServeHTTP dispatches a request. It implements http.Handler, so a Router can
// be mounted inside another server, wrapped in http.HandlerFunc, or tested
// directly with httptest.
//
// Path parameters captured by a match are available to the handler through
// PathParam, for the duration of the handler call.
func (rt *Router) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	path := requestPath(r)

	// Static routes are resolved without touching the capture pool at all.
	// This is the path that must not allocate.
	if rt.frozen.Load() {
		if mr := rt.methods[r.Method]; mr != nil && mr.static != nil {
			if e, ok := mr.static[path]; ok {
				r.Pattern = path
				e.handler.ServeHTTP(w, r)
				return
			}
		}
		scratch := paramsPool.Get().(*tree.Params)
		h, pattern, params := rt.find(r.Method, path, scratch)
		if rt.ambiguousEncoding(r, h, pattern) {
			paramsPool.Put(scratch)
			rejectAmbiguous(w, r)
			return
		}
		rt.dispatch(w, r, h, pattern, params, scratch)
		return
	}

	// Not frozen: a standalone router used before Serve, typically in a test.
	// Registration can still run concurrently, so the lookup takes the lock.
	scratch := paramsPool.Get().(*tree.Params)
	rt.mu.RLock()
	h, pattern, params := rt.find(r.Method, path, scratch)
	miss := rt.miss
	ambiguous := rt.ambiguousEncoding(r, h, pattern)
	rt.mu.RUnlock()
	if ambiguous {
		paramsPool.Put(scratch)
		rejectAmbiguous(w, r)
		return
	}
	if h == nil {
		paramsPool.Put(scratch)
		miss.ServeHTTP(w, r)
		return
	}
	rt.dispatch(w, r, h, pattern, params, scratch)
}

// isFallback reports whether a lookup result is one an encoded path must not
// reach by escaping a route: a mount, or a miss answered by a custom NotFound
// or MethodNotAllowed handler, either of which may hand the request to code
// that reads the decoded path. The built-in 404 is not a fallback; it already
// fails closed.
func (rt *Router) isFallback(h http.Handler, pattern string) bool {
	if h == nil {
		return rt.notFound != nil || rt.methodNotAllowed != nil
	}
	return strings.HasSuffix(pattern, "/*") && rt.mountFor(strings.TrimSuffix(pattern, "/*")) != nil
}

// ambiguousEncoding reports whether a request whose path contains escapes
// that net/http keeps in RawPath - %2F, or a needless escape such as %61 for
// "a" - would be served by a route if its path were decoded, while matching on
// the raw path sent it to a mount or a 404.
//
// Matching on the raw path is what stops %2F from splitting a segment. But a
// mount, or a NotFound handler that delegates, passes the request to code that
// may read the decoded r.URL.Path - a legacy router, a file server - and to
// that code /admin%2Fsecret is /admin/secret. Serving it there would let an
// encoding walk around a protected route and the middleware on it. Refusing
// the request fails closed; no well-formed client sends such a path to a route
// it means to reach.
func (rt *Router) ambiguousEncoding(r *http.Request, h http.Handler, pattern string) bool {
	if r.URL.RawPath == "" || !rt.isFallback(h, pattern) {
		return false
	}
	scratch := paramsPool.Get().(*tree.Params)
	defer paramsPool.Put(scratch)
	h2, pattern2, _ := rt.find(r.Method, r.URL.Path, scratch)
	return pattern2 != pattern || (h2 == nil) != (h == nil)
}

// rejectAmbiguous writes the 400 for ambiguousEncoding.
func rejectAmbiguous(w http.ResponseWriter, r *http.Request) {
	Error(w, r, photonerr.BadRequest("the request path uses an encoding that changes which route it reaches"))
}

// dispatch runs a matched handler, or the miss handler when h is nil.
func (rt *Router) dispatch(w http.ResponseWriter, r *http.Request, h http.Handler, pattern string, params []Param, scratch *tree.Params) {
	if h == nil {
		paramsPool.Put(scratch)
		rt.miss.ServeHTTP(w, r)
		return
	}
	// The captures live in scratch, so they are stored in the request before
	// the scratch buffer goes back to the pool. The values themselves are
	// substrings of the request path.
	//
	// Matching runs on the raw path when it differs from the decoded one - a
	// request with %2F in it - so that an encoded slash cannot split a
	// segment. The values are then decoded here, so a handler always receives
	// decoded parameters, exactly as net/http's ServeMux gives them.
	decode := r.URL.RawPath != ""
	for i := range params {
		v := params[i].Value
		if decode && strings.IndexByte(v, '%') >= 0 {
			if u, err := url.PathUnescape(v); err == nil {
				v = u
			}
		}
		r.SetPathValue(params[i].Name, v)
	}
	r.Pattern = pattern
	paramsPool.Put(scratch)
	h.ServeHTTP(w, r)
}

// serveMiss writes the 405 or 404 response. It is the innermost handler of the
// compiled miss chain, so global middleware has already run.
func (rt *Router) serveMiss(w http.ResponseWriter, r *http.Request) {
	var allow []string
	var notFound, notAllowed http.Handler
	if rt.frozen.Load() {
		allow = rt.allow(requestPath(r))
		notFound, notAllowed = rt.notFound, rt.methodNotAllowed
	} else {
		rt.mu.RLock()
		allow = rt.allow(requestPath(r))
		notFound, notAllowed = rt.notFound, rt.methodNotAllowed
		rt.mu.RUnlock()
	}
	if len(allow) > 0 {
		w.Header().Set("Allow", strings.Join(allow, ", "))
		if notAllowed != nil {
			notAllowed.ServeHTTP(w, r)
			return
		}
		writeText(w, http.StatusMethodNotAllowed, "405 method not allowed\n")
		return
	}
	if notFound != nil {
		notFound.ServeHTTP(w, r)
		return
	}
	// The body is a fixed string and never reflects the requested path: a
	// reflected path is a free oracle for route enumeration and an XSS vector
	// if the content type is mis-sniffed.
	writeText(w, http.StatusNotFound, "404 not found\n")
}

// requestPath returns the path to route on.
//
// RawPath is set only when the escaped form differs from the decoded Path, so
// a request for "/users%2Fadmin" is routed on "/users%2Fadmin" and the encoded
// slash is not a segment separator. Routing on Path instead would let "%2F"
// cross a segment boundary, which is a routing bypass.
func requestPath(r *http.Request) string {
	if r.URL.RawPath != "" {
		return r.URL.RawPath
	}
	return r.URL.Path
}

// libraryDir is the directory holding this package's source, used to find the
// first stack frame that belongs to the caller rather than to photon.
var libraryDir = func() string {
	_, file, _, _ := runtime.Caller(0)
	return filepath.Dir(file)
}()

// registrationSite returns "dir/file:line" for the user code that registered
// a route, skipping every frame inside photon itself. Conflict panics name both
// registration sites, so pointing at photon's own Group.GET instead of the
// caller's line would make the diagnostic useless.
func registrationSite() string {
	var pcs [32]uintptr
	n := runtime.Callers(2, pcs[:])
	frames := runtime.CallersFrames(pcs[:n])
	for {
		f, more := frames.Next()
		if filepath.Dir(f.File) != libraryDir || strings.HasSuffix(f.File, "_test.go") {
			return tree.Site(f.File, f.Line)
		}
		if !more {
			return "unknown"
		}
	}
}

// paramsPool recycles capture buffers across requests.
//
// A buffer cannot be stored on the Router, because the captures alias the
// request path and a shared buffer would let one request observe another
// request's path. sync.Pool gives each goroutine its own value without a
// per-request allocation, and Go clears it at every GC, so an idle server
// retains nothing.
var paramsPool = sync.Pool{
	New: func() any { return tree.NewParams() },
}

// writeText writes a small plain-text response with a fixed body.
func writeText(w http.ResponseWriter, status int, body string) {
	h := w.Header()
	h.Set("Content-Type", "text/plain; charset=utf-8")
	h.Set("X-Content-Type-Options", "nosniff")
	w.WriteHeader(status)
	_, _ = io.WriteString(w, body)
}
