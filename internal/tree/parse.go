package tree

import (
	"fmt"
	"runtime"
	"strings"
)

// Pattern is a parsed route pattern.
//
// A pattern is a sequence of segments. Each segment is one of:
//
//	literal   a fixed string compared byte-for-byte
//	:param    captures exactly one non-empty segment
//	*catch    captures the remaining path, including slashes
//
// Anything else is rejected at registration.
type Pattern struct {
	// Path is the pattern as written.
	Path string
	// segments holds the literal text of each segment. It is empty at
	// positions held by param and catch-all nodes, which is why a literal
	// read must check the kind at that index first.
	segments []string
	// kinds holds one kindStatic/kindParam/kindCatchAll per segment position.
	kinds []uint8
	// names holds the param names in wildcard order, which is the order the
	// matcher captures values in.
	names []string
}

// Static reports whether p contains no wildcards.
func (p *Pattern) Static() bool {
	for _, k := range p.kinds {
		if k != kindStatic {
			return false
		}
	}
	return true
}

// Depth returns the number of segments in p.
func (p *Pattern) Depth() int { return len(p.kinds) }

// ParamNames returns a copy of p's wildcard names, in capture order.
func (p *Pattern) ParamNames() []string {
	out := make([]string, len(p.names))
	copy(out, p.names)
	return out
}

// ParsePattern parses a route pattern without registering it. It is used by
// the router to classify a pattern before deciding where to store it.
func ParsePattern(path string) (Pattern, error) { return parsePattern(path) }

// parsePattern validates and parses a route pattern.
//
// Errors name the offending construct and match ErrMalformed, because a router
// that fails with "bad pattern" has its errors suppressed by users.
func parsePattern(path string) (Pattern, error) {
	switch {
	case path == "":
		return Pattern{}, malformed("the route pattern must not be empty")
	case path[0] != '/':
		return Pattern{}, malformed("the route pattern %q must start with %q", path, "/")
	case strings.ContainsAny(path, "?#"):
		return Pattern{}, malformed(
			"the route pattern %q must not contain %q or %q: "+
				"a query or fragment is not part of a route path",
			path, "?", "#")
	case strings.ContainsAny(path, "{}"):
		// net/http's ServeMux, chi, and FastAPI write parameters as {name}.
		// Accepted as literal text, such a pattern registers without complaint
		// and then never matches a real request - the most likely mistake when
		// porting routes, and a silent one. Name the fix instead.
		return Pattern{}, malformed(
			"the route pattern %q uses {braces}: photon writes a parameter as "+
				":name for one segment and *name for the rest of the path, "+
				"so {id} becomes :id and {path...} becomes *path",
			path)
	case strings.Contains(path, "//"):
		return Pattern{}, malformed(
			"the route pattern %q contains an empty segment %q: "+
				"only the root may be a single slash",
			path, "//")
	}

	// A trailing slash is a distinct path, and accepting it as a synonym makes
	// "/foo" and "/foo/" ambiguous. Reject it at registration rather than
	// choosing a winner at request time.
	if len(path) > 1 && path[len(path)-1] == '/' {
		return Pattern{}, malformed(
			"the route pattern %q ends with a slash: register %q instead",
			path, path[:len(path)-1])
	}

	var p Pattern
	p.Path = path
	rest := path[1:]
	for rest != "" {
		var seg string
		if i := strings.IndexByte(rest, '/'); i >= 0 {
			seg, rest = rest[:i], rest[i+1:]
		} else {
			seg, rest = rest, ""
		}

		switch {
		case seg == "":
			return Pattern{}, malformed(
				"the route pattern %q contains an empty segment", path)

		case seg[0] == ':':
			name := seg[1:]
			if err := checkWildcardName(name, path); err != nil {
				return Pattern{}, err
			}
			if p.hasCatchAll() {
				return Pattern{}, malformed(
					"the route pattern %q has a segment after its catch-all: "+
						"a catch-all must be the final segment", path)
			}
			if p.hasName(name) {
				return Pattern{}, malformed(
					"the route pattern %q uses the parameter name %q twice", path, name)
			}
			p.kinds = append(p.kinds, kindParam)
			p.segments = append(p.segments, "")
			p.names = append(p.names, name)

		case seg[0] == '*':
			name := seg[1:]
			if err := checkWildcardName(name, path); err != nil {
				return Pattern{}, err
			}
			if p.hasCatchAll() {
				return Pattern{}, malformed(
					"the route pattern %q has more than one catch-all: "+
						"a route may contain at most one", path)
			}
			if p.hasName(name) {
				return Pattern{}, malformed(
					"the route pattern %q uses the parameter name %q twice", path, name)
			}
			p.kinds = append(p.kinds, kindCatchAll)
			p.segments = append(p.segments, "")
			p.names = append(p.names, name)

		default:
			if p.hasCatchAll() {
				return Pattern{}, malformed(
					"the route pattern %q has a segment after its catch-all: "+
						"a catch-all must be the final segment", path)
			}
			// A wildcard must occupy a whole segment. ":id-suffix" is
			// rejected because partial-segment matching makes conflicts hard
			// to reason about and error messages unreadable; register ":id"
			// and parse in the handler, or use a catch-all.
			if strings.ContainsAny(seg, ":*") {
				return Pattern{}, malformed(
					"the route pattern %q has the mixed segment %q: "+
						"a wildcard must occupy a whole segment "+
						"(write %q or %q, not %q)",
					path, seg, ":name", "*name", seg)
			}
			p.kinds = append(p.kinds, kindStatic)
			p.segments = append(p.segments, seg)
		}

		if len(p.kinds) > MaxSegments {
			return Pattern{}, malformed(
				"the route pattern %q has more than %d segments: "+
					"the limit bounds the router's per-request matching state",
				path, MaxSegments)
		}
	}
	return p, nil
}

// malformed builds a parse error that satisfies errors.Is(err, ErrMalformed).
func malformed(format string, args ...any) error {
	return fmt.Errorf("%w: %s", ErrMalformed, fmt.Sprintf(format, args...))
}

// hasCatchAll reports whether a catch-all has already been parsed.
// hasName reports whether a wildcard named name is already in the pattern.
// Two parameters with one name would make the second silently overwrite the
// first in the request's path values.
func (p *Pattern) hasName(name string) bool {
	for _, n := range p.names {
		if n == name {
			return true
		}
	}
	return false
}

func (p *Pattern) hasCatchAll() bool {
	for _, k := range p.kinds {
		if k == kindCatchAll {
			return true
		}
	}
	return false
}

// checkWildcardName validates the name following ':' or '*'.
func checkWildcardName(name, path string) error {
	if name == "" {
		return malformed(
			"the route pattern %q has an unnamed wildcard: "+
				"give it a name, for example \":name\" or \"*name\"", path)
	}
	for i, r := range name {
		if i == 0 {
			if r >= '0' && r <= '9' {
				return malformed(
					"the route pattern %q has the wildcard name %q starting with a digit: "+
						"a name may not start with a digit", path, name)
			}
		}
		if badRune(r) {
			return malformed(
				"the route pattern %q has the invalid wildcard name %q: "+
					"names may use letters, digits and underscore, "+
					"and may not start with a digit",
				path, name)
		}
	}
	return nil
}

// badRune reports whether r is not allowed in a wildcard name.
func badRune(r rune) bool {
	switch {
	case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r == '_', r >= '0' && r <= '9':
		return false
	case r > 0x7f:
		// A non-ASCII rune counts as a letter, except surrogates, which
		// cannot appear in a decoded name and indicate a malformed pattern.
		return r >= 0xD800 && r <= 0xDFFF
	}
	return true
}

// Caller renders a registration site as "dir/file:line" for diagnostics.
func Caller(skip int) string {
	_, file, line, ok := runtime.Caller(skip + 1)
	if !ok {
		return "unknown"
	}
	return fmt.Sprintf("%s:%d", trimDir(file), line)
}

// Site renders a known source location as "dir/file:line", in the same form as
// Caller, for a caller that found the frame itself.
func Site(file string, line int) string {
	if file == "" {
		return "unknown"
	}
	return fmt.Sprintf("%s:%d", trimDir(file), line)
}

// trimDir shortens an absolute source path to "dir/file".
func trimDir(file string) string {
	i := strings.LastIndexAny(file, `/\`)
	if i < 0 {
		return file
	}
	// Keep the parent directory: two router files with the same base name in
	// different packages are common, and the directory is what distinguishes
	// them.
	j := strings.LastIndexAny(file[:i], `/\`)
	if j < 0 {
		return file[i+1:]
	}
	return file[j+1:]
}
