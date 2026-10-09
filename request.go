package photon

import (
	"net/http"
	"net/netip"
	"strings"
	"sync"

	"github.com/agenticmarket/photon/internal/tree"
)

// Param is one captured path parameter.
type Param = tree.Param

// Path parameters live in the request itself, through the standard library's
// Request.SetPathValue (Go 1.22) and Request.Pattern (Go 1.23), not in a
// context value.
//
// That choice does three things. It avoids http.Request.WithContext, which
// copies the whole request - the single largest allocation photon used to make
// per parametrised request. It makes r.PathValue("id") work, so a handler
// written for net/http's ServeMux runs unchanged under photon, and the other
// way round. And it puts the matched route template in r.Pattern, which is
// where OpenTelemetry's otelhttp and most metrics middleware look for a
// low-cardinality route label.

// patternNames maps a registered route pattern to its parameter names in
// order, so Params can rebuild the ordered list from r.Pattern. It is written
// at registration and only read afterwards.
var patternNames sync.Map // string -> []string

// rememberPattern records a pattern's parameter names for Params.
func rememberPattern(pattern string, names []string) {
	if len(names) > 0 {
		patternNames.Store(pattern, names)
	}
}

// PathParam returns the value of the named path parameter, or "" if the
// matched route has no such parameter.
//
//	photon.PathParam(r, "id") // the same as r.PathValue("id")
//
// The value is a substring of the request path, not a copy. Go strings are
// immutable, so keeping it is memory-safe; what keeping it does is hold the
// request's whole path in memory for as long as the value lives. The API's
// contract is that the value is for use during the handler call, which leaves
// room for a zero-copy optimisation later; use CopyParam for a value that has
// to outlive the request, such as one stored in a cache or handed to a
// background job.
func PathParam(r *http.Request, name string) string {
	if r == nil {
		return ""
	}
	return r.PathValue(name)
}

// Params returns the captured path parameters in pattern order, or nil if the
// request did not match a parametrised photon route.
//
// The strings are substrings of the request path; see PathParam. Use
// CopyParams to retain them.
func Params(r *http.Request) []Param {
	if r == nil || r.Pattern == "" {
		return nil
	}
	v, ok := patternNames.Load(r.Pattern)
	if !ok {
		return nil
	}
	names := v.([]string)
	out := make([]Param, len(names))
	for i, n := range names {
		out[i] = Param{Name: n, Value: r.PathValue(n)}
	}
	return out
}

// CopyParam returns an independent copy of a path parameter, safe to retain
// past the end of the handler. It allocates, so use it only when a value
// genuinely has to outlive the request.
func CopyParam(r *http.Request, name string) string {
	return strings.Clone(PathParam(r, name))
}

// CopyParams returns an independent copy of the captured path parameters, safe
// to retain past the end of the handler. Both the slice and the strings are
// copied.
func CopyParams(r *http.Request) []Param {
	out := Params(r)
	for i := range out {
		out[i].Value = strings.Clone(out[i].Value)
	}
	return out
}

// ClientIP returns the address of the immediate peer.
//
// Forwarded headers are deliberately ignored. X-Forwarded-For and its relatives
// are set by the client, so trusting them by default lets anyone forge their
// apparent address and defeat rate limiting, access logging, and IP-based
// authorization. Only a server configured with a trusted proxy should consider
// them, and such a server is responsible for replacing this behaviour.
func ClientIP(r *http.Request) netip.Addr {
	if r == nil {
		return netip.Addr{}
	}
	// The port is stripped: it is a per-connection detail, and every consumer
	// of a client address wants the address alone.
	addr, _ := netip.ParseAddrPort(r.RemoteAddr)
	return addr.Addr()
}

// AcceptsGzip reports whether the client advertised gzip support in
// Accept-Encoding. It does not consider an explicit "gzip;q=0", which is a
// rare but correct way to refuse an encoding.
func AcceptsGzip(r *http.Request) bool {
	if r == nil {
		return false
	}
	for _, part := range splitComma(r.Header.Get("Accept-Encoding")) {
		if part == "gzip" || part == "*" {
			return true
		}
	}
	return false
}

// splitComma splits a comma-separated header value, trimming surrounding
// spaces. It allocates only when the header has more than one value, which is
// not the case for the common single-token encodings.
func splitComma(v string) []string {
	if v == "" {
		return nil
	}
	var out []string
	start := 0
	for i := 0; i <= len(v); i++ {
		if i == len(v) || v[i] == ',' {
			tok := trimSpace(v[start:i])
			if tok != "" {
				out = append(out, tok)
			}
			start = i + 1
		}
	}
	return out
}

// trimSpace trims spaces and tabs from s. Header values may not contain other
// whitespace in the tokens this function inspects, and using strings.TrimSpace
// would import strings for a job three lines do.
func trimSpace(s string) string {
	i, j := 0, len(s)
	for i < j && isSpace(s[i]) {
		i++
	}
	for j > i && isSpace(s[j-1]) {
		j--
	}
	return s[i:j]
}

// HandlerFunc adapts a function to http.Handler.
//
// Go's http.HandlerFunc already does this, and any existing code can be passed
// directly. This alias exists so the package's own godoc has one name for the
// concept, and so an example can read as photon.HandlerFunc without importing
// net/http for a single symbol.
type HandlerFunc = http.HandlerFunc

func isSpace(c byte) bool { return c == ' ' || c == '\t' }
