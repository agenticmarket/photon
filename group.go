package photon

import (
	"net/http"
	"strings"
)

// Group is a set of routes that share a path prefix and middleware.
//
//	api := app.Group("/v1", requireAPIKey)
//	api.POST("/chat/completions", photon.SSE(chat))
//	api.GET("/models", listModels)
//
// Group middleware runs inside global middleware (Use) and outside per-route
// middleware. It applies only to routes registered through the group, which is
// what makes it the right place for authentication: a route outside the group
// cannot pick it up by accident, and a route inside it cannot forget it.
type Group struct {
	rt     *Router
	prefix string
	mw     []Middleware
}

// Group returns a group of routes under prefix, wrapped in mw.
//
// The prefix must start with "/" and must not end with one. It may contain
// parameters: app.Group("/orgs/:org") is valid, and PathParam(r, "org") works
// in every route under it.
func (rt *Router) Group(prefix string, mw ...Middleware) *Group {
	return &Group{rt: rt, prefix: cleanPrefix(prefix), mw: append([]Middleware(nil), mw...)}
}

// Group returns a nested group. The prefixes are joined and the parent's
// middleware runs outside the child's.
func (g *Group) Group(prefix string, mw ...Middleware) *Group {
	all := make([]Middleware, 0, len(g.mw)+len(mw))
	all = append(all, g.mw...)
	all = append(all, mw...)
	return &Group{rt: g.rt, prefix: g.prefix + cleanPrefix(prefix), mw: all}
}

// Use adds middleware to routes registered on this group after the call.
//
// Unlike Router.Use it is order-dependent, because a group is a registration
// scope rather than the whole server: middleware added halfway through a group
// covers the routes that follow it, which is how a group can hold a public
// login route followed by authenticated ones.
func (g *Group) Use(mw ...Middleware) { g.mw = append(g.mw, mw...) }

// Handle registers h for method at prefix+path.
func (g *Group) Handle(method, path string, h http.Handler, mw ...Middleware) {
	g.rt.handle(method, g.join(path), h, g.middleware(mw))
}

// GET registers a GET route under the group's prefix.
func (g *Group) GET(path string, h http.HandlerFunc, mw ...Middleware) {
	g.rt.handle(http.MethodGet, g.join(path), handlerOf(h), g.middleware(mw))
}

// POST registers a POST route under the group's prefix.
func (g *Group) POST(path string, h http.HandlerFunc, mw ...Middleware) {
	g.rt.handle(http.MethodPost, g.join(path), handlerOf(h), g.middleware(mw))
}

// PUT registers a PUT route under the group's prefix.
func (g *Group) PUT(path string, h http.HandlerFunc, mw ...Middleware) {
	g.rt.handle(http.MethodPut, g.join(path), handlerOf(h), g.middleware(mw))
}

// PATCH registers a PATCH route under the group's prefix.
func (g *Group) PATCH(path string, h http.HandlerFunc, mw ...Middleware) {
	g.rt.handle(http.MethodPatch, g.join(path), handlerOf(h), g.middleware(mw))
}

// DELETE registers a DELETE route under the group's prefix.
func (g *Group) DELETE(path string, h http.HandlerFunc, mw ...Middleware) {
	g.rt.handle(http.MethodDelete, g.join(path), handlerOf(h), g.middleware(mw))
}

// HEAD registers a HEAD route under the group's prefix.
func (g *Group) HEAD(path string, h http.HandlerFunc, mw ...Middleware) {
	g.rt.handle(http.MethodHead, g.join(path), handlerOf(h), g.middleware(mw))
}

// OPTIONS registers an OPTIONS route under the group's prefix.
func (g *Group) OPTIONS(path string, h http.HandlerFunc, mw ...Middleware) {
	g.rt.handle(http.MethodOptions, g.join(path), handlerOf(h), g.middleware(mw))
}

// Mount serves h for every request at prefix+path or below that no route
// matches. See Router.Mount.
func (g *Group) Mount(path string, h http.Handler) {
	g.rt.mount(cleanPrefix(g.join(path)), h, g.middleware(nil))
}

// join appends a route path to the group's prefix. A route path of "/" means
// the prefix itself, because trailing slashes are not valid routes.
func (g *Group) join(path string) string {
	if path == "" || path[0] != '/' {
		// Without this, api.GET("models") on a "/v1" group would register
		// "/v1models" - a valid pattern, so nothing else would notice.
		panic("photon: route path " + quote(path) + " in group " + quote(displayPrefix(g.prefix)) +
			" must start with /")
	}
	if path == "/" && g.prefix != "" {
		return g.prefix
	}
	return g.prefix + path
}

// middleware returns the group's middleware followed by a route's own, as a
// fresh slice so a later Use on the group cannot change a registered route.
func (g *Group) middleware(route []Middleware) []Middleware {
	if len(g.mw) == 0 {
		return route
	}
	out := make([]Middleware, 0, len(g.mw)+len(route))
	out = append(out, g.mw...)
	return append(out, route...)
}

// cleanPrefix validates a group prefix. "" and "/" both mean no prefix.
func cleanPrefix(p string) string {
	if p == "" || p == "/" {
		return ""
	}
	if !strings.HasPrefix(p, "/") {
		panic("photon: group prefix " + quote(p) + " must start with /")
	}
	if strings.HasSuffix(p, "/") {
		panic("photon: group prefix " + quote(p) + " must not end with /")
	}
	return p
}

// mountEntry is one mounted handler: everything at or below prefix that no
// route matched.
type mountEntry struct {
	prefix  string // "" for a mount at the root
	pattern string // what r.Pattern is set to, e.g. "/mcp/*"
	handler http.Handler
}

// matches reports whether the raw request path falls under the mount.
func (m *mountEntry) matches(path string) bool {
	if m.prefix == "" {
		return true
	}
	return strings.HasPrefix(path, m.prefix) &&
		(len(path) == len(m.prefix) || path[len(m.prefix)] == '/')
}

// Mount serves h for every request at prefix or below it that no route
// matches, whatever the method.
//
// It is how an existing http.Handler joins a photon server - an MCP server, a
// gRPC-gateway mux, pprof, a file server, or the old router during a
// migration:
//
//	app.Mount("/mcp", mcpHandler)
//	app.Mount("/static", http.StripPrefix("/static", http.FileServer(dir)))
//	app.Mount("/", legacyMux) // everything not yet ported
//
// Routes take precedence over mounts, and a longer prefix over a shorter one,
// so a service can be migrated route by route: mount the old handler at "/"
// and register new routes as they are ported. That precedence is the one
// exception to photon's rule against ambiguous routes, and it is a deliberate
// one: a mount is a fallback by definition, and which side serves a request is
// decided by what is registered, never by registration order.
//
// The mounted handler sees every path under the prefix, including trailing
// slashes and paths of any depth, and r.Pattern is the prefix followed by
// "/*". The path is passed through unchanged; wrap h in http.StripPrefix if it
// expects paths relative to the mount point. The prefix must be static.
func (rt *Router) Mount(prefix string, h http.Handler) {
	rt.mount(cleanPrefix(prefix), h, nil)
}

func (rt *Router) mount(prefix string, h http.Handler, mw []Middleware) {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	if rt.frozen.Load() {
		panic("photon: the router is frozen because it has started serving, so handlers cannot be mounted")
	}
	if h == nil {
		panic("photon: nil handler mounted at " + quote(prefix))
	}
	if strings.ContainsAny(prefix, ":*{}") {
		panic("photon: mount prefix " + quote(prefix) + " must be static: a mount matches by prefix, not by pattern")
	}
	for _, m := range rt.mounts {
		if m.prefix == prefix {
			panic("photon: a handler is already mounted at " + quote(displayPrefix(prefix)))
		}
	}
	base := h
	if len(mw) > 0 {
		base = chain(mw, h)
	}
	m := &mountEntry{prefix: prefix, pattern: prefix + "/*", handler: base}
	if len(rt.global) > 0 {
		m.handler = chain(rt.global, base)
	}
	// Longest prefix first, so the most specific mount wins.
	i := 0
	for i < len(rt.mounts) && len(rt.mounts[i].prefix) >= len(prefix) {
		i++
	}
	rt.mounts = append(rt.mounts, nil)
	copy(rt.mounts[i+1:], rt.mounts[i:])
	rt.mounts[i] = m
	rt.order = append(rt.order, RouteInfo{Method: "*", Path: m.pattern})
	rt.records = append(rt.records, routeRecord{base: base, set: func(h http.Handler) { m.handler = h }})
}

// mountFor returns the mount serving path, or nil. The caller holds rt.mu or
// the router is frozen.
func (rt *Router) mountFor(path string) *mountEntry {
	for _, m := range rt.mounts {
		if m.matches(path) {
			return m
		}
	}
	return nil
}

func displayPrefix(p string) string {
	if p == "" {
		return "/"
	}
	return p
}

// quote renders s for a panic message.
func quote(s string) string { return "\"" + s + "\"" }
