// Package tree implements the segment-based routing tree used by the router.
//
// The tree holds only routes that contain at least one wildcard. Fully static
// routes are served by the router's flat map, so this package never sees the
// common case.
package tree

import (
	"net/http"
	"strings"
)

// MaxSegments bounds how many path segments a single route pattern may
// contain. It bounds the matcher's per-request state to a fixed-size stack
// array, which is what makes matching allocation-free.
//
// A pattern deeper than this is rejected at registration rather than matched,
// so the bound is enforced on the only path that can produce an unbounded walk.
const MaxSegments = 16

// Segment kinds, in structural priority order. A match attempt tries a node's
// static children first, then its param child, then its catch-all child. The
// order is a property of the data structure, not a heuristic that drifts as
// routes are added.
const (
	kindStatic uint8 = iota
	kindParam
	kindCatchAll
)

// node is one segment position in the tree.
//
// A node with a non-nil route terminates a match. A node may hold a route and
// also have children: "/api" and "/api/users" differ in segment count, so they
// are distinguishable and both are representable.
type node struct {
	// seg is the literal segment this node matches. Empty for the root, and
	// unused for param and catch-all nodes, whose values are captured from the
	// path rather than compared.
	seg string

	// route is the route registered at exactly this node, or nil.
	route *Route

	// statics holds literal children, one per distinct segment string, kept
	// sorted by segment.
	//
	// Sorting is what keeps a wildcard match from scanning every literal
	// sibling: with 10,000 static routes stored as siblings, the linear scan
	// cost 16 us per parametrised request. A binary search over
	// the same 10,000 entries costs about 14 comparisons.
	statics []*node

	// param is the single `:name` child, or nil. Only one param child can
	// exist per node: two params at the same position match the same set of
	// paths, so they are a conflict rather than a choice.
	param *node

	// catchAll is the single `*name` child, or nil. Always terminal.
	catchAll *node

	// paramNames lists this route's param names in pattern order. Set on the
	// node that terminates the route, nil elsewhere.
	paramNames []string
}

// Route is one registered route.
type Route struct {
	// Method is the HTTP method this route was registered for.
	Method string
	// Path is the pattern as registered, kept for diagnostics.
	Path string
	// Pattern is the parsed pattern.
	Pattern Pattern
	// Handler is served on a match.
	Handler http.Handler
	// Caller is the registration site, for conflict diagnostics.
	Caller string
}

// Tree holds every route for a single HTTP method.
type Tree struct {
	root *node

	// maxDepth is the greatest segment count registered, used to report
	// expected match depth and to size diagnostics.
	maxDepth int

	// routes records every registered pattern, in registration order, for
	// conflict detection and for Routes.
	routes []*Route
}

// New returns an empty tree.
func New() *Tree {
	return &Tree{root: &node{}}
}

// Add registers r, returning an error if its pattern is malformed or it
// conflicts with an existing route. The router turns the error into a panic;
// see the routing design's stance on making ambiguity unrepresentable.
func (t *Tree) Add(r *Route) error {
	p, err := parsePattern(r.Path)
	if err != nil {
		return err
	}
	r.Pattern = p
	return t.insert(r)
}

// CheckConflict reports whether r would conflict with any of statics or with a
// registered wildcard route, without inserting anything.
//
// The router needs this in both directions. A static pattern is compared
// against the wildcards already registered, and a wildcard pattern is compared
// against the static routes already indexed, because the two live in different
// structures and neither would otherwise see the other.
func (t *Tree) CheckConflict(r *Route, statics []string) error {
	p, err := parsePattern(r.Path)
	if err != nil {
		return err
	}
	r.Pattern = p
	for _, existing := range t.routes {
		if err := checkConflict(r, existing); err != nil {
			return err
		}
	}
	for _, s := range statics {
		sp, err := parsePattern(s)
		if err != nil {
			return err
		}
		existing := &Route{Method: r.Method, Path: s, Pattern: sp}
		if err := checkConflict(r, existing); err != nil {
			return err
		}
	}
	return nil
}

// CheckConflictWith is CheckConflict for static routes whose patterns are
// already parsed. The router keeps its static routes parsed, so registering a
// wildcard compares against them without parsing every one of them again -
// which, done on every registration, made startup quadratic in the number of
// routes.
func (t *Tree) CheckConflictWith(r *Route, statics []*Route) error {
	p, err := parsePattern(r.Path)
	if err != nil {
		return err
	}
	r.Pattern = p
	for _, existing := range t.routes {
		if err := checkConflict(r, existing); err != nil {
			return err
		}
	}
	for _, existing := range statics {
		if err := checkConflict(r, existing); err != nil {
			return err
		}
	}
	return nil
}

// insert places r into the tree.
//
// A static route is accepted for the tree's own sake, so the tree's conflict
// rules can be tested directly. The router does not do this: it keeps static
// routes in a flat map and calls CheckConflict for them, because a large static
// route set in the tree would slow every wildcard match.
func (t *Tree) insert(r *Route) error {
	p := &r.Pattern
	for _, existing := range t.routes {
		if err := checkConflict(r, existing); err != nil {
			return err
		}
	}

	n := t.root
	for i := 0; i < len(p.kinds); i++ {
		switch p.kinds[i] {
		case kindStatic:
			n = n.addStatic(p.segments[i])
		case kindParam:
			if n.param == nil {
				n.param = &node{}
			}
			n = n.param
		case kindCatchAll:
			if n.catchAll == nil {
				n.catchAll = &node{}
			}
			n = n.catchAll
		}
	}
	if n.route != nil {
		return &ConflictError{
			New: r, Existing: n.route,
			Reason: "the same path is already registered for this method",
		}
	}
	n.route = r
	n.paramNames = p.names
	t.routes = append(t.routes, r)
	if d := len(p.kinds); d > t.maxDepth {
		t.maxDepth = d
	}
	return nil
}

// addStatic returns the literal child of n matching seg, creating it if
// needed. The children slice is kept sorted so a lookup can binary search it.
func (n *node) addStatic(seg string) *node {
	if c := n.staticChild(seg); c != nil {
		return c
	}
	c := &node{seg: seg}
	// Insert in sorted order. Registration is not on the hot path, so a linear
	// shift is the right trade for a lookup that binary searches.
	i := sortSearch(n.statics, seg)
	n.statics = append(n.statics, nil)
	copy(n.statics[i+1:], n.statics[i:])
	n.statics[i] = c
	return c
}

// staticChild returns the literal child of n matching seg, or nil if there is
// none. The children are sorted by segment, so this is a binary search.
func (n *node) staticChild(seg string) *node {
	i := sortSearch(n.statics, seg)
	if i < len(n.statics) && n.statics[i].seg == seg {
		return n.statics[i]
	}
	return nil
}

// sortSearch returns the index at which seg belongs in a slice of children
// sorted by segment.
func sortSearch(children []*node, seg string) int {
	lo, hi := 0, len(children)
	for lo < hi {
		mid := int(uint(lo+hi) >> 1)
		if children[mid].seg < seg {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	return lo
}

// Routes returns every registered route in registration order.
func (t *Tree) Routes() []*Route { return t.routes }

// MaxDepth returns the greatest segment count among registered patterns.
func (t *Tree) MaxDepth() int { return t.maxDepth }

// Find resolves path against the tree, writing captures into p and returning
// the matched route, or nil if nothing matches.
//
// It exists so the router can hold one reusable Params across requests instead
// of allocating per request; see the matching cost note in find.
func (t *Tree) Find(path string, p *Params) *Route { return t.find(path, p) }

// Param is one captured path parameter.
type Param struct {
	// Name is the name written in the route pattern, without its ':' or '*'.
	Name string
	// Value is the matched text, aliasing the request path.
	Value string
}

// Params holds the values captured by a match, in pattern order.
//
// Values alias the request path. They are valid for the duration of the
// request that produced them and must be copied with Copy to outlive it.
type Params struct {
	pairs []Param
}

// NewParams returns an empty Params with room for the maximum number of
// captures any pattern can produce, so matching never grows a slice and
// therefore never allocates.
func NewParams() *Params {
	return &Params{pairs: make([]Param, 0, MaxSegments)}
}

// Reset prepares p to receive a new set of captures, keeping its backing
// array. The router calls this once per request.
func (p *Params) Reset() { p.pairs = p.pairs[:0] }

// Len returns the number of captured parameters.
func (p *Params) Len() int { return len(p.pairs) }

// Get returns the value captured for name, or "" if this pattern had no such
// parameter. Names are unique within a pattern, so this is unambiguous.
func (p *Params) Get(name string) string {
	for i := range p.pairs {
		if p.pairs[i].Name == name {
			return p.pairs[i].Value
		}
	}
	return ""
}

// At returns the i-th capture, and whether i was in range.
func (p *Params) At(i int) (Param, bool) {
	if i < 0 || i >= len(p.pairs) {
		return Param{}, false
	}
	return p.pairs[i], true
}

// All returns the captures in pattern order. The slice aliases p's backing
// array and is invalidated by the next Reset.
func (p *Params) All() []Param { return p.pairs }

// Copy returns an independent copy of p's captures, safe to retain past the
// end of the request. Use it when a capture must outlive the handler call, for
// example when it is stored in a struct or handed to an async logger.
func (p *Params) Copy() []Param {
	out := make([]Param, len(p.pairs))
	copy(out, p.pairs)
	return out
}

// find walks the tree for path and, on a match, writes the captured values
// into p. It returns the matched route, or nil if no route matches.
//
// path must be the raw request path as received, before any decoding, and must
// begin with a slash. An empty result leaves p's captures in an undefined
// state; the router discards them when find returns nil.
func (t *Tree) find(path string, p *Params) *Route {
	var (
		segs [MaxSegments]string
		offs [MaxSegments]int
	)
	n := 0

	if len(path) == 0 || path[0] != '/' {
		return nil
	}
	pos := 1
	truncated := false
	for pos <= len(path) {
		seg := path[pos:]
		offs[n] = pos
		if i := strings.IndexByte(seg, '/'); i >= 0 {
			seg, pos = seg[:i], pos+i+1
		} else {
			pos = len(path) + 1
		}
		segs[n] = seg
		n++
		if n == MaxSegments {
			// A path deeper than the state array cannot be matched. It must
			// report no match rather than matching on the prefix it did fit,
			// because a truncated path is not the path that was registered.
			truncated = pos <= len(path)
			break
		}
	}
	if truncated {
		return nil
	}
	if n == 1 && segs[0] == "" {
		// The root path "/" has no segments; only a route registered at "/"
		// matches it.
		p.pairs = p.pairs[:0]
		return t.root.route
	}
	for i := 0; i < n; i++ {
		// A trailing slash or a doubled slash produces an empty segment. It
		// must not match a param, because that would let "/users/" and
		// "/users/me" reach the same handler with different semantics, and
		// registration rejects trailing slashes precisely to avoid it.
		if segs[i] == "" {
			return nil
		}
	}

	cur := t.root
	pairs := p.pairs[:0]
	for i := 0; i < n; i++ {
		seg := segs[i]
		next := cur.staticChild(seg)
		switch {
		case next != nil:
			// Literal match, nothing to capture.
		case cur.param != nil:
			next = cur.param
			pairs = append(pairs, Param{Value: seg})
		case cur.catchAll != nil && path[offs[i]:] != "":
			// A catch-all consumes the entire remainder of the path, slashes
			// included, so it ends the walk here rather than descending one
			// segment and continuing. It requires a non-empty remainder, so
			// "/files" and "/files/" do not match while "/files/a" and
			// "/files/a/b" both do.
			next = cur.catchAll
			if next.route == nil {
				return nil
			}
			pairs = append(pairs, Param{Value: path[offs[i]:]})
			for k := range pairs {
				pairs[k].Name = next.paramNames[k]
			}
			p.pairs = pairs
			return next.route
		default:
			return nil
		}

		if i == n-1 {
			if next.route == nil {
				return nil
			}
			// paramNames is in wildcard order, which is the order the values
			// were captured in, so pairing is positional.
			for k := range pairs {
				pairs[k].Name = next.paramNames[k]
			}
			p.pairs = pairs
			return next.route
		}
		cur = next
	}
	return nil
}
