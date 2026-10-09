package tree

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
)

// ErrConflict is the sentinel matched by errors.Is for any registration
// conflict.
var ErrConflict = errors.New("photon: route conflict")

// ErrMalformed is the sentinel matched by errors.Is for a malformed pattern.
var ErrMalformed = errors.New("photon: malformed route pattern")

// ConflictError reports that a route cannot be registered because an existing
// route already claims part of the same request space.
//
// Registration panics on this error rather than returning it, because a
// conflicting route is a programming mistake that must surface in a test, not
// a runtime condition to be handled.
type ConflictError struct {
	// New is the route being registered.
	New *Route
	// Existing is the route already registered.
	Existing *Route
	// Reason explains the conflict in terms of the two patterns.
	Reason string
}

func (e *ConflictError) Error() string {
	var b strings.Builder
	b.WriteString("photon: conflicting routes for ")
	b.WriteString(strconv.Quote(e.New.Method))
	b.WriteByte(' ')
	b.WriteString(strconv.Quote(e.New.Path))
	b.WriteString("\n  new:       ")
	b.WriteString(callerOr(e.New, "this registration"))
	b.WriteString("\n  existing:  ")
	b.WriteString(callerOr(e.Existing, "a previous registration"))
	b.WriteString("\n  reason:    ")
	b.WriteString(e.Reason)
	if e.Existing != nil {
		b.WriteString("\n  existing pattern: ")
		b.WriteString(strconv.Quote(e.Existing.Path))
		b.WriteString("\n  new pattern:      ")
		b.WriteString(strconv.Quote(e.New.Path))
	}
	return b.String()
}

// Unwrap makes errors.Is(err, ErrConflict) true.
func (e *ConflictError) Unwrap() error { return ErrConflict }

// checkConflict reports whether a and b compete for the same request space.
//
// Two patterns compete when some path matches both, or when one terminates
// where the other continues past that point. Registering both would make the
// served handler depend on the request path, which is how a typo becomes an
// authorization bug.
//
// A shorter route that simply ends early, such as "/api" beside "/api/users",
// does not compete: the two are distinguishable by segment count, and both are
// unambiguously servable.
func checkConflict(a, b *Route) error {
	ap, bp := &a.Pattern, &b.Pattern

	// Two patterns of different depth can only both match a path if one of
	// them has a catch-all, whose remainder can be any length.
	if len(ap.kinds) != len(bp.kinds) && !ap.hasCatchAll() && !bp.hasCatchAll() {
		return nil
	}
	if ap.Path == bp.Path {
		return &ConflictError{
			New: a, Existing: b,
			Reason: "the same path is already registered for this method",
		}
	}
	if reason, ok := overlapReason(ap, bp, 0); ok {
		return &ConflictError{New: a, Existing: b, Reason: reason}
	}
	return nil
}

// overlapReason reports whether a and b can both match one path, and why.
//
// i is the segment position being compared. The reason returned is the
// innermost one found, which is the most specific explanation.
func overlapReason(a, b *Pattern, i int) (string, bool) {
	// Both patterns have ended: identical shape, caught by the caller.
	if i >= len(a.kinds) && i >= len(b.kinds) {
		return "both patterns match the same paths", true
	}

	// The root pattern "/" has no segments, so it never overlaps anything:
	// it matches only the exact path "/", which no other pattern can reach.
	// Returning here also keeps the "ends where the other continues" rule
	// below from flagging every route in the tree as shadowed by "/".
	if len(a.kinds) == 0 || len(b.kinds) == 0 {
		return "", false
	}

	// One pattern has ended while the other continues. No path matches both:
	// the longer pattern needs at least one more non-empty segment - a
	// catch-all requires a non-empty remainder, so "/files/*path" does not
	// match "/files" - and the shorter one, having no catch-all of its own
	// before this point (that case is decided below), matches nothing longer.
	// So "/files" beside "/files/*path" is allowed, as "/api" beside
	// "/api/users" is, and it is what lets Mount serve a prefix and its
	// subtree with one handler.
	if i >= len(a.kinds) || i >= len(b.kinds) {
		return "", false
	}
	ak, bk := a.kinds[i], b.kinds[i]
	switch {
	case ak == kindCatchAll && bk == kindCatchAll:
		// Two catch-alls at the same position: identical match language.
		return fmt.Sprintf(
			"both catch-alls at position %d match the same remaining paths, "+
				"so one would always win", i+1), true

	case ak == kindCatchAll || bk == kindCatchAll:
		// A catch-all matches every remaining path, including the literal
		// segments of the other pattern, unless the literal differs at some
		// earlier position. So a catch-all under a shared prefix always
		// competes, which is why "/admin/settings" and "/admin/*" conflict.
		caught, other := a, b
		if ak != kindCatchAll {
			caught, other = b, a
		}
		return fmt.Sprintf(
			"%q's catch-all also matches %q, so which one serves it would depend on registration order",
			caught.Path, other.Path), true

	case ak == kindStatic && bk == kindStatic:
		if a.segments[i] != b.segments[i] {
			return "", false
		}

	case ak == kindParam && bk == kindParam:
		// Same match language, possibly different names. Which name a value
		// is stored under would depend on which route was registered first.

	default:
		// One literal, one wildcard. The wildcard matches the literal too.
		literal, _ := a, b
		if ak != kindStatic {
			literal = b
		}
		return fmt.Sprintf(
			"the wildcard in one pattern matches the literal segment %q in the other, "+
				"so %q would be served by whichever was registered first",
			literal.segments[i], literal.Path), true
	}
	return overlapReason(a, b, i+1)
}

// callerOr renders a route's registration site, or a fallback if unknown.
func callerOr(r *Route, fallback string) string {
	if r == nil || r.Caller == "" {
		return fallback
	}
	return r.Caller
}
