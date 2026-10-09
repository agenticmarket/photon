// Package photonerr is photon's error model.
//
// The design goal is that a handler can return a meaningful error and have it
// rendered correctly, without every handler repeating status codes and JSON
// boilerplate, and without an error type that can accidentally leak internals
// to a client.
//
// The three rules that matter:
//
//  1. An Error carries a safe Message for clients and a separate Detail for
//     operators. Detail is never sent unless explicitly opted into.
//  2. No request content is ever reflected into a response body. An error that
//     echoes a path or a query is a route-enumeration oracle and an XSS vector
//     if the content type is mis-sniffed.
//  3. errors.Is and errors.As work throughout, so middleware distinguishes
//     cases by type rather than by matching on strings.
package photonerr

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
)

// Error is a structured HTTP error.
type Error struct {
	// Status is the HTTP status code to send.
	Status int

	// Code is a stable, machine-readable identifier such as "not_found". It is
	// part of the API contract: clients may branch on it, so it changes only
	// with a major version.
	Code string

	// Message is safe to send to a client. It must not contain request
	// content, internal paths, or anything the caller would not want an
	// attacker to learn.
	Message string

	// Detail is the internal explanation. It is logged, never sent, unless
	// IncludeDetail is set on the renderer.
	Detail string

	// Limit names which configured limit was exceeded, when the status is
	// 413, 431, or 503. It identifies a configuration knob, not request data.
	Limit string

	// cause is the wrapped internal error.
	cause error
}

func (e *Error) Error() string {
	switch {
	case e.Detail != "" && e.cause != nil:
		return fmt.Sprintf("photon: %s: %s: %v", e.Code, e.Detail, e.cause)
	case e.Detail != "":
		return fmt.Sprintf("photon: %s: %s", e.Code, e.Detail)
	case e.cause != nil:
		return fmt.Sprintf("photon: %s: %v", e.Code, e.cause)
	default:
		return "photon: " + e.Code
	}
}

// Unwrap returns the wrapped cause, so errors.Is and errors.As reach it.
func (e *Error) Unwrap() error { return e.cause }

// Is matches on Code, so a caller can test for a specific condition with
// errors.Is(err, photonerr.ErrNotFound) without holding the Error value.
func (e *Error) Is(target error) bool {
	var t *Error
	if !errors.As(target, &t) {
		return false
	}
	return t.Code == e.Code
}

// WithDetail attaches an internal explanation. It returns the same error for
// chaining, and the detail is never sent to a client.
func (e *Error) WithDetail(format string, args ...any) *Error {
	e.Detail = fmt.Sprintf(format, args...)
	return e
}

// WithCause wraps an underlying error for errors.Is and errors.As.
func (e *Error) WithCause(err error) *Error {
	e.cause = err
	return e
}

// WithLimit names the limit that was exceeded.
func (e *Error) WithLimit(name string) *Error {
	e.Limit = name
	return e
}

// The sentinels a caller can test with errors.Is. Each corresponds to a Code.
var (
	ErrBadRequest           = &Error{Status: 400, Code: "bad_request", Message: "bad request"}
	ErrUnauthorized         = &Error{Status: 401, Code: "unauthorized", Message: "unauthorized"}
	ErrForbidden            = &Error{Status: 403, Code: "forbidden", Message: "forbidden"}
	ErrNotFound             = &Error{Status: 404, Code: "not_found", Message: "not found"}
	ErrNotAllowed           = &Error{Status: 405, Code: "method_not_allowed", Message: "method not allowed"}
	ErrConflict             = &Error{Status: 409, Code: "conflict", Message: "conflict"}
	ErrLimitExceeded        = &Error{Status: 413, Code: "limit_exceeded", Message: "limit exceeded"}
	ErrUnsupportedMediaType = &Error{Status: 415, Code: "unsupported_media_type", Message: "unsupported media type"}
	ErrUnprocessable        = &Error{Status: 422, Code: "unprocessable", Message: "unprocessable request"}
	ErrTooManyRequests      = &Error{Status: 429, Code: "too_many_requests", Message: "too many requests"}
	ErrTimeout              = &Error{Status: 504, Code: "timeout", Message: "timeout"}
	ErrCanceled             = &Error{Status: 499, Code: "canceled", Message: "canceled"}
	ErrUpstream             = &Error{Status: 502, Code: "upstream_error", Message: "upstream error"}
	ErrInternal             = &Error{Status: 500, Code: "internal_error", Message: "internal server error"}
	ErrUnavailable          = &Error{Status: 503, Code: "unavailable", Message: "service unavailable"}
)

// clone returns a copy, so a caller mutating a returned error cannot corrupt
// the package-level sentinels. This is the bug that a shared sentinel invites:
// one handler setting Detail on ErrInternal would leak it everywhere.
func clone(e *Error, code, message string, status int) *Error {
	return &Error{Status: status, Code: code, Message: message}
}

// BadRequest returns a 400 with a caller-chosen safe message.
//
// The message is caller-written, so it must not be built from request content.
// photon.NotFound("user") is safe; photon.NotFound(r.URL.Path) is not.
func BadRequest(message string) *Error {
	return clone(ErrBadRequest, ErrBadRequest.Code, message, http.StatusBadRequest)
}

// Unauthorized returns a 401: the request has no valid credentials. The
// message is caller-written and must not echo the credential that failed.
func Unauthorized(message string) *Error {
	if message == "" {
		message = ErrUnauthorized.Message
	}
	return clone(ErrUnauthorized, ErrUnauthorized.Code, message, http.StatusUnauthorized)
}

// Forbidden returns a 403: the caller is known but not allowed.
func Forbidden(message string) *Error {
	if message == "" {
		message = ErrForbidden.Message
	}
	return clone(ErrForbidden, ErrForbidden.Code, message, http.StatusForbidden)
}

// UnsupportedMediaType returns a 415, for a body in a format the endpoint does
// not accept.
func UnsupportedMediaType(message string) *Error {
	if message == "" {
		message = ErrUnsupportedMediaType.Message
	}
	return clone(ErrUnsupportedMediaType, ErrUnsupportedMediaType.Code, message, http.StatusUnsupportedMediaType)
}

// UnprocessableEntity returns a 422: the body is well-formed but its content
// is invalid - a missing field, a value out of range. FastAPI and many JSON
// APIs use it for validation failures; 400 is equally correct, so pick one and
// use it consistently.
func UnprocessableEntity(message string) *Error {
	if message == "" {
		message = ErrUnprocessable.Message
	}
	return clone(ErrUnprocessable, ErrUnprocessable.Code, message, http.StatusUnprocessableEntity)
}

// TooManyRequests returns a 429, for a caller over a rate or quota. Set a
// Retry-After header before rendering it if the caller can usefully wait.
func TooManyRequests(message string) *Error {
	if message == "" {
		message = ErrTooManyRequests.Message
	}
	return clone(ErrTooManyRequests, ErrTooManyRequests.Code, message, http.StatusTooManyRequests)
}

// NotFound returns a 404 naming what was not found, for example "user".
func NotFound(what string) *Error {
	if what == "" {
		return clone(ErrNotFound, ErrNotFound.Code, "not found", http.StatusNotFound)
	}
	return clone(ErrNotFound, ErrNotFound.Code, what+" not found", http.StatusNotFound)
}

// NotAllowed returns a 405.
func NotAllowed() *Error {
	return clone(ErrNotAllowed, ErrNotAllowed.Code, ErrNotAllowed.Message, http.StatusMethodNotAllowed)
}

// Conflict returns a 409 with a safe message.
func Conflict(message string) *Error {
	return clone(ErrConflict, ErrConflict.Code, message, http.StatusConflict)
}

// LimitExceeded returns a 413 naming the limit that was hit.
func LimitExceeded(limit string) *Error {
	e := clone(ErrLimitExceeded, ErrLimitExceeded.Code, ErrLimitExceeded.Message, http.StatusRequestEntityTooLarge)
	e.Limit = limit
	return e
}

// HeaderTooLarge returns a 431 naming the header limit.
func HeaderTooLarge(limit string) *Error {
	e := clone(ErrLimitExceeded, ErrLimitExceeded.Code, "header too large", http.StatusRequestHeaderFieldsTooLarge)
	e.Limit = limit
	return e
}

// Timeout returns a 504.
func Timeout() *Error {
	return clone(ErrTimeout, ErrTimeout.Code, ErrTimeout.Message, http.StatusGatewayTimeout)
}

// Canceled returns a 499 for a request the client abandoned.
//
// 499 is nginx's non-standard "client closed request". It is used because it is
// widely understood by operators and log pipelines, and it is never sent to a
// client that has already gone away.
func Canceled() *Error {
	return clone(ErrCanceled, ErrCanceled.Code, ErrCanceled.Message, 499)
}

// Upstream returns a 502 for a failed upstream call. The cause is wrapped for
// the operator and never sent to the client.
func Upstream(cause error) *Error {
	return clone(ErrUpstream, ErrUpstream.Code, ErrUpstream.Message, http.StatusBadGateway).
		WithCause(cause)
}

// Internal returns a 500. The cause is wrapped and logged, never sent.
func Internal(cause error) *Error {
	return clone(ErrInternal, ErrInternal.Code, ErrInternal.Message, http.StatusInternalServerError).
		WithCause(cause)
}

// Unavailable returns a 503, optionally naming the limit or resource that is
// exhausted.
func Unavailable(reason string) *Error {
	e := clone(ErrUnavailable, ErrUnavailable.Code, ErrUnavailable.Message, http.StatusServiceUnavailable)
	e.Limit = reason
	return e
}

// problem is the RFC 9457 response body.
type problem struct {
	Type   string `json:"type"`
	Title  string `json:"title"`
	Status int    `json:"status"`
	// Detail is omitted unless the caller opted in, so it cannot leak by
	// default.
	Detail string `json:"detail,omitempty"`
	// Code is a photon extension to RFC 9457, for clients that want a stable
	// identifier without parsing the title.
	Code string `json:"code,omitempty"`
	// Limit names the limit, which is configuration, not request data.
	Limit string `json:"limit,omitempty"`
}

// RenderOptions configures how an error is written.
type RenderOptions struct {
	// IncludeDetail sends Error.Detail to the client. Off by default: the
	// detail is written for an operator reading a log, and it usually contains
	// something an attacker should not see, such as an internal hostname.
	IncludeDetail bool
}

// Render writes err as an RFC 9457 problem document.
//
// A nil err writes nothing and returns nil, so a handler can do
// return Render(w, err) unconditionally. An error that is not a photonerr.Error
// is rendered as a 500 with a constant body: an unknown error may be a
// database error containing a connection string, and its text is not safe to
// send.
func Render(w http.ResponseWriter, err error, opts RenderOptions) error {
	if err == nil {
		return nil
	}
	var e *Error
	if !errors.As(err, &e) {
		e = Internal(err)
	}

	p := problem{
		Type:   "https://photon.agenticmarket.dev/errors/" + e.Code,
		Title:  e.Message,
		Status: e.Status,
		Code:   e.Code,
		Limit:  e.Limit,
	}
	if opts.IncludeDetail && e.Detail != "" {
		p.Detail = e.Detail
	}
	if p.Title == "" {
		p.Title = http.StatusText(e.Status)
	}

	body, mErr := json.Marshal(p)
	if mErr != nil {
		// Marshalling a struct of strings cannot fail, but a fallback is
		// still better than a nil write on an error path.
		writeFallback(w, e.Status)
		return mErr
	}
	h := w.Header()
	h.Set("Content-Type", "application/problem+json; charset=utf-8")
	h.Set("X-Content-Type-Options", "nosniff")
	h.Set("Content-Length", strconv.Itoa(len(body)+1))
	w.WriteHeader(e.Status)
	_, _ = w.Write(append(body, '\n'))
	return nil
}

// writeFallback sends a constant problem document, used only if the encoder
// somehow fails.
func writeFallback(w http.ResponseWriter, status int) {
	w.Header().Set("Content-Type", "application/problem+json; charset=utf-8")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.WriteHeader(status)
	_, _ = w.Write([]byte(`{"type":"https://photon.agenticmarket.dev/errors/internal_error","title":"internal server error"}` + "\n"))
}
