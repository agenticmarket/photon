package main

import (
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
)

// errBodyTooLarge is returned by decodeJSON when the body exceeds the limit. It
// is a sentinel so a caller can distinguish a rejected body from malformed JSON
// without string-matching an error.
var errBodyTooLarge = errors.New("request body too large")

// jsonString quotes a string as a JSON literal.
//
// encoding/json is not used for this because it would mean marshalling a
// one-field struct on every error path, and the alternative - string
// concatenation - is how a server ends up with an injection sink. An error
// message can contain a quote, a backslash, or a control character from
// attacker-controlled input, and any of them breaks the response body.
//
// The escape list is the one from RFC 8259: quote, backslash, and the control
// characters. Everything else is passed through, which is safe because a
// multi-byte UTF-8 sequence never contains a byte that needs escaping here.
func jsonString(s string) string {
	var b []byte
	b = append(b, '"')
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		case c == '"':
			b = append(b, '\\', '"')
		case c == '\\':
			b = append(b, '\\', '\\')
		case c == '\n':
			b = append(b, '\\', 'n')
		case c == '\r':
			b = append(b, '\\', 'r')
		case c == '\t':
			b = append(b, '\\', 't')
		case c < 0x20:
			// The remaining control characters, as \u00XX. Written out rather
			// than with fmt.Sprintf, because this is on the error path and the
			// point of the function is to not allocate more than necessary.
			const hex = "0123456789abcdef"
			b = append(b, '\\', 'u', '0', '0', hex[c>>4], hex[c&0xF])
		default:
			// Everything else passes through unchanged, including bytes >= 0x80.
			// A UTF-8 continuation byte is emitted as-is, which is correct:
			// escaping it would corrupt the character it belongs to.
			b = append(b, c)
		}
	}
	b = append(b, '"')
	return string(b)
}

// writeJSON writes v as a JSON response with the given status.
func writeJSON(w http.ResponseWriter, status int, v any) {
	body, err := json.Marshal(v)
	if err != nil {
		// Marshalling our own types cannot fail, because they contain only
		// strings, ints, and slices of those. If it ever does, something is wrong
		// with the value rather than with the encoder, and a 500 with a constant
		// body is the honest response.
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte(`{"error":{"message":"could not encode response","type":"server_error"}}`))
		return
	}
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_, _ = w.Write(body)
}

// writeError writes an OpenAI-shaped error response.
func writeError(w http.ResponseWriter, status int, errType, code, msg string) {
	writeJSON(w, status, errorResponse{Error: errorDetail{
		Message: msg,
		Type:    errType,
		Code:    code,
	}})
}

// decodeJSON reads a JSON body into v.
//
// The size check is on the declared length first so an oversized body is refused
// before a single byte is read, and MaxBytesReader is applied second so a client
// that lies about Content-Length, or sends chunked, is still bounded. Both are
// needed: either alone is defeatable.
func decodeJSON(w http.ResponseWriter, r *http.Request, maxBytes int64, v any) error {
	if r.ContentLength > maxBytes {
		writeError(w, http.StatusRequestEntityTooLarge,
			"invalid_request_error", "context_length_exceeded",
			"request body is too large")
		return errBodyTooLarge
	}
	r.Body = http.MaxBytesReader(w, r.Body, maxBytes)

	dec := json.NewDecoder(r.Body)
	// A typo in a field name becomes an error rather than a silently ignored
	// value. A client sending `max_token` instead of `max_tokens` would otherwise
	// get a request that quietly does not do what it says.
	dec.DisallowUnknownFields()

	if err := dec.Decode(v); err != nil {
		return err
	}
	return nil
}

// itoa avoids strconv for the hot paths where a small int is written into a
// header or a log line.
func itoa(n int) string { return strconv.Itoa(n) }
