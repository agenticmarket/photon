package photon

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"

	"github.com/agenticmarket/photon/photonerr"
)

// DecodeJSON reads a JSON request body into v.
//
//	var req ChatRequest
//	if err := photon.DecodeJSON(r, &req); err != nil {
//		photon.Error(w, r, err) // 400, 413, or 415, with a safe message
//		return
//	}
//
// Every error it returns is a *photonerr.Error carrying the right status and a
// message that is safe to send, so the usual handling is to pass it straight to
// Error. The underlying decoder error is kept as the cause, for logs.
//
// What it checks, and why:
//
//   - A Content-Type other than JSON is refused with 415. This is a CSRF
//     defence, not pedantry: a cross-site HTML form can POST text/plain or
//     form-encoded bodies without a preflight, but not application/json. A
//     request with no Content-Type at all is accepted.
//   - The body is read through the server's MaxRequestBodyBytes limit, and an
//     oversized body is reported as 413 rather than as malformed JSON.
//   - Exactly one JSON value is accepted. Trailing data is refused, so
//     `{"a":1}{"a":2}` cannot smuggle a second object past a check that only
//     looked at the first.
//   - Error messages never quote the request body. A field name is included
//     only when it is a plain identifier.
//
// Unknown fields are accepted, which is what OpenAI-compatible and most other
// JSON APIs expect from a server. For strict decoding, use a json.Decoder with
// DisallowUnknownFields directly; the body limit still applies.
func DecodeJSON(r *http.Request, v any) error {
	if ct := r.Header.Get("Content-Type"); ct != "" && !isJSONMediaType(ct) {
		return photonerr.UnsupportedMediaType("the request body must be JSON; set Content-Type: application/json")
	}
	if r.Body == nil || r.Body == http.NoBody {
		return photonerr.BadRequest("the request body is empty")
	}
	dec := json.NewDecoder(r.Body)
	if err := dec.Decode(v); err != nil {
		return jsonDecodeError(err)
	}
	var extra json.RawMessage
	switch err := dec.Decode(&extra); {
	case errors.Is(err, io.EOF):
		return nil
	case err == nil:
		return photonerr.BadRequest("the request body must contain exactly one JSON value")
	default:
		var tooLarge *http.MaxBytesError
		if errors.As(err, &tooLarge) {
			return jsonDecodeError(err)
		}
		return photonerr.BadRequest("the request body must contain exactly one JSON value").WithCause(err)
	}
}

// jsonDecodeError maps a decoder error to a client-safe *photonerr.Error.
func jsonDecodeError(err error) error {
	var (
		tooLarge  *http.MaxBytesError
		syntaxErr *json.SyntaxError
		typeErr   *json.UnmarshalTypeError
		invalid   *json.InvalidUnmarshalError
	)
	switch {
	case errors.As(err, &tooLarge):
		return photonerr.LimitExceeded("MaxRequestBodyBytes").WithCause(err)
	case errors.Is(err, io.EOF):
		return photonerr.BadRequest("the request body is empty").WithCause(err)
	case errors.As(err, &syntaxErr), errors.Is(err, io.ErrUnexpectedEOF):
		return photonerr.BadRequest("the request body is not valid JSON").WithCause(err)
	case errors.As(err, &typeErr):
		if isPlainIdentifier(typeErr.Field) {
			return photonerr.BadRequest("field " + typeErr.Field + " has the wrong type").WithCause(err)
		}
		return photonerr.BadRequest("a field has the wrong type").WithCause(err)
	case errors.As(err, &invalid):
		// v was not a non-nil pointer: a bug in the handler, not the client.
		return photonerr.Internal(err)
	default:
		return photonerr.BadRequest("the request body could not be decoded").WithCause(err)
	}
}

// isJSONMediaType reports whether a Content-Type names JSON: application/json
// or any +json suffix type, with or without parameters. It does not allocate.
func isJSONMediaType(ct string) bool {
	if i := strings.IndexByte(ct, ';'); i >= 0 {
		ct = ct[:i]
	}
	ct = strings.TrimSpace(ct)
	if strings.EqualFold(ct, "application/json") {
		return true
	}
	return len(ct) > len("+json") && strings.EqualFold(ct[len(ct)-len("+json"):], "+json")
}

// isPlainIdentifier reports whether s is safe to echo in an error message: a
// short dotted path of ASCII letters, digits, and underscores. The field path
// of a type error is built from the request's own keys when decoding into a
// map, so it is request content and must be vetted before it is reflected.
func isPlainIdentifier(s string) bool {
	if s == "" || len(s) > 64 {
		return false
	}
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		case c >= 'a' && c <= 'z', c >= 'A' && c <= 'Z', c >= '0' && c <= '9', c == '_', c == '.':
		default:
			return false
		}
	}
	return true
}
