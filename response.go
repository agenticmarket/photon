package photon

import (
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"strconv"

	"github.com/agenticmarket/photon/photonerr"
)

// JSON writes v as a JSON response with the given status.
//
// It is a free function rather than a method on a context type because it works
// on any http.ResponseWriter and cannot be misused: there is no way to forget
// to set the content type or to write a body without a status.
//
// The body is written in one Write so a handler cannot accidentally send a
// 200 followed by an error, and the encoder escapes HTML by default, so a
// string containing "<script>" is not a reflected-XSS vector.
func JSON(w http.ResponseWriter, status int, v any) error {
	body, err := json.Marshal(v)
	if err != nil {
		// The value could not be encoded, which is a programming error and not
		// the client's fault. The caller decides what to do with the error; a
		// nil-returning convenience wrapper would hide it.
		return err
	}
	h := w.Header()
	h.Set("Content-Type", "application/json; charset=utf-8")
	h.Set("X-Content-Type-Options", "nosniff")
	h.Set("Content-Length", strconv.Itoa(len(body)))
	w.WriteHeader(status)
	_, _ = w.Write(body)
	return nil
}

// Text writes s as a plain-text response with the given status.
func Text(w http.ResponseWriter, status int, s string) error {
	h := w.Header()
	h.Set("Content-Type", "text/plain; charset=utf-8")
	h.Set("X-Content-Type-Options", "nosniff")
	h.Set("Content-Length", strconv.Itoa(len(s)))
	w.WriteHeader(status)
	// io.WriteString uses the ResponseWriter's own WriteString, which
	// net/http implements, so the body is not copied into a []byte first.
	_, _ = io.WriteString(w, s)
	return nil
}

// NoContent writes a 204 with no body.
func NoContent(w http.ResponseWriter) error {
	w.WriteHeader(http.StatusNoContent)
	return nil
}

// HTML writes s as an HTML response.
//
// It sets X-Content-Type-Options, which stops a browser from sniffing a
// mislabelled response into HTML. It does not set a CSP: a content security
// policy is an application decision, and a default that is wrong for an
// application is worse than none.
func HTML(w http.ResponseWriter, status int, s string) error {
	h := w.Header()
	h.Set("Content-Type", "text/html; charset=utf-8")
	h.Set("X-Content-Type-Options", "nosniff")
	h.Set("Content-Length", strconv.Itoa(len(s)))
	w.WriteHeader(status)
	_, _ = io.WriteString(w, s)
	return nil
}

// Redirect sends a redirect to url. A 303 is used by default because it
// converts a POST into a GET, which is what a redirect after a write almost
// always means; replaying the body on a 307 surprises people.
func Redirect(w http.ResponseWriter, r *http.Request, url string) error {
	http.Redirect(w, r, url, http.StatusSeeOther)
	return nil
}

// Error writes err as an RFC 9457 problem document and returns.
//
//	if err := photon.DecodeJSON(r, &req); err != nil {
//		photon.Error(w, r, err)
//		return
//	}
//
// A *photonerr.Error is rendered with its own status and safe message. Any
// other error is a 500 with a constant body, because an arbitrary error's text
// can contain a connection string or an internal hostname. Server errors (5xx)
// are logged through the server's logger with the full error, so the operator
// sees the cause the client does not.
func Error(w http.ResponseWriter, r *http.Request, err error) {
	if err == nil {
		return
	}
	var pe *photonerr.Error
	if !errors.As(err, &pe) {
		pe = photonerr.Internal(err)
	}
	if pe.Status >= 500 {
		logFor(r).Error("request failed",
			"method", r.Method, "path", r.URL.Path, "status", pe.Status, "err", err)
	}
	_ = photonerr.Render(w, pe, photonerr.RenderOptions{})
}

// logFor returns the logger of the server handling r, or the errors-only
// default when r did not come through a Server.
func logFor(r *http.Request) *slog.Logger {
	if s := serverFromContext(r); s != nil {
		return s.logger
	}
	return defaultLogger
}
