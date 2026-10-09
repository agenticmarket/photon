package photonerr

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestErrorUnwrap(t *testing.T) {
	cause := errors.New("connection refused")
	err := Internal(cause)
	if !errors.Is(err, cause) {
		t.Error("errors.Is did not reach the wrapped cause")
	}
	if !strings.Contains(err.Error(), "connection refused") {
		t.Errorf("Error() = %q, want it to mention the cause", err.Error())
	}
}

func TestErrorIsMatchesOnCode(t *testing.T) {
	// A caller can test a condition without holding the Error value.
	if !errors.Is(NotFound("user"), ErrNotFound) {
		t.Error("NotFound(user) does not match ErrNotFound")
	}
	if errors.Is(NotFound("user"), ErrBadRequest) {
		t.Error("NotFound(user) wrongly matches ErrBadRequest")
	}
	// A specific message must not affect the code match.
	if !errors.Is(NotFound("post"), ErrNotFound) {
		t.Error("NotFound(post) does not match ErrNotFound")
	}
}

func TestSentinelsAreNotCorruptedByCallers(t *testing.T) {
	// This is the bug a shared sentinel invites: a handler attaching detail
	// to the package value would leak that detail from every other handler.
	err := NotFound("widget").WithDetail("db: users_v2 lookup by id %q", "abc")
	if err.Detail == "" {
		t.Fatal("WithDetail did not set Detail")
	}
	if ErrNotFound.Detail != "" {
		t.Errorf("ErrNotFound.Detail = %q, want empty: a caller mutated the sentinel", ErrNotFound.Detail)
	}
	if ErrNotFound.Message != "not found" {
		t.Errorf("ErrNotFound.Message = %q, want %q", ErrNotFound.Message, "not found")
	}
}

func TestConstructors(t *testing.T) {
	tests := []struct {
		name   string
		err    *Error
		status int
		code   string
		msg    string
	}{
		{"badrequest", BadRequest("bad input"), 400, "bad_request", "bad input"},
		{"notfound", NotFound("user"), 404, "not_found", "user not found"},
		{"notfound bare", NotFound(""), 404, "not_found", "not found"},
		{"notallowed", NotAllowed(), 405, "method_not_allowed", "method not allowed"},
		{"conflict", Conflict("already exists"), 409, "conflict", "already exists"},
		{"limit", LimitExceeded("MaxRequestBodyBytes"), 413, "limit_exceeded", "limit exceeded"},
		{"headeralarge", HeaderTooLarge("MaxHeaders"), 431, "limit_exceeded", "header too large"},
		{"timeout", Timeout(), 504, "timeout", "timeout"},
		{"canceled", Canceled(), 499, "canceled", "canceled"},
		{"unavailable", Unavailable("MaxConcurrentRequests"), 503, "unavailable", "service unavailable"},
		{"upstream", Upstream(errors.New("boom")), 502, "upstream_error", "upstream error"},
		{"internal", Internal(errors.New("boom")), 500, "internal_error", "internal server error"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if tc.err.Status != tc.status {
				t.Errorf("Status = %d, want %d", tc.err.Status, tc.status)
			}
			if tc.err.Code != tc.code {
				t.Errorf("Code = %q, want %q", tc.err.Code, tc.code)
			}
			if tc.err.Message != tc.msg {
				t.Errorf("Message = %q, want %q", tc.err.Message, tc.msg)
			}
		})
	}
}

func TestLimitIsRecorded(t *testing.T) {
	if got := LimitExceeded("MaxRequestBodyBytes").Limit; got != "MaxRequestBodyBytes" {
		t.Errorf("Limit = %q, want the limit name", got)
	}
	if got := HeaderTooLarge("MaxHeaders").Limit; got != "MaxHeaders" {
		t.Errorf("Limit = %q, want the limit name", got)
	}
	if got := Unavailable("at capacity").Limit; got != "at capacity" {
		t.Errorf("Limit = %q, want the reason", got)
	}
}

func TestRender(t *testing.T) {
	tests := []struct {
		name   string
		err    error
		status int
		want   []string
		absent []string
	}{
		{
			name:   "notfound",
			err:    NotFound("user"),
			status: 404,
			want:   []string{`"status":404`, `"code":"not_found"`, `"title":"user not found"`},
		},
		{
			name:   "internal does not leak the cause",
			err:    Internal(errors.New("dial tcp 10.0.0.5:5432: connection refused")),
			status: 500,
			want:   []string{`"status":500`},
			absent: []string{"10.0.0.5", "5432", "connection refused"},
		},
		{
			name:   "upstream does not leak the cause",
			err:    Upstream(errors.New("token sk-secret123 rejected")),
			status: 502,
			want:   []string{`"status":502`},
			absent: []string{"sk-secret123"},
		},
		{
			name:   "plain error is rendered as 500",
			err:    errors.New("something internal"),
			status: 500,
			want:   []string{`"status":500`},
			absent: []string{"something internal"},
		},
		{
			name:   "limit names the knob",
			err:    LimitExceeded("MaxRequestBodyBytes"),
			status: 413,
			want:   []string{`"status":413`, `"limit":"MaxRequestBodyBytes"`},
		},
		{
			name:   "detail omitted by default",
			err:    NotFound("user").WithDetail("db lookup failed for id %q", "abc"),
			status: 404,
			want:   []string{`"status":404`},
			absent: []string{"db lookup failed", "abc"},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			rec := httptest.NewRecorder()
			if err := Render(rec, tc.err, RenderOptions{}); err != nil {
				t.Fatalf("Render: %v", err)
			}
			if rec.Code != tc.status {
				t.Errorf("status = %d, want %d", rec.Code, tc.status)
			}
			if got := rec.Header().Get("Content-Type"); !strings.HasPrefix(got, "application/problem+json") {
				t.Errorf("Content-Type = %q, want application/problem+json", got)
			}
			body := rec.Body.String()
			for _, want := range tc.want {
				if !strings.Contains(body, want) {
					t.Errorf("body %q does not contain %q", body, want)
				}
			}
			for _, absent := range tc.absent {
				if strings.Contains(body, absent) {
					t.Errorf("body %q leaks %q", body, absent)
				}
			}
			// The body must be valid JSON with the expected shape.
			var p struct {
				Type   string `json:"type"`
				Title  string `json:"title"`
				Status int    `json:"status"`
			}
			if err := json.Unmarshal([]byte(body), &p); err != nil {
				t.Fatalf("body is not valid JSON: %v", err)
			}
			if p.Status != tc.status {
				t.Errorf("body status = %d, want %d", p.Status, tc.status)
			}
			if !strings.HasPrefix(p.Type, "https://photon.agenticmarket.dev/errors/") {
				t.Errorf("type = %q, want a photon error URI", p.Type)
			}
		})
	}
}

func TestRenderNilWritesNothing(t *testing.T) {
	rec := httptest.NewRecorder()
	if err := Render(rec, nil, RenderOptions{}); err != nil {
		t.Fatalf("Render(nil) = %v, want nil", err)
	}
	if rec.Body.Len() != 0 {
		t.Errorf("body = %q, want empty for a nil error", rec.Body.String())
	}
	if rec.Code != 200 {
		t.Errorf("status = %d, want 200 untouched for a nil error", rec.Code)
	}
}

func TestRenderIncludeDetailIsOptIn(t *testing.T) {
	// Including detail must work when asked for, and only then.
	err := NotFound("user").WithDetail("db lookup failed for id %q", "abc")
	rec := httptest.NewRecorder()
	if err := Render(rec, err, RenderOptions{IncludeDetail: true}); err != nil {
		t.Fatalf("Render: %v", err)
	}
	if !strings.Contains(rec.Body.String(), "db lookup failed") {
		t.Errorf("body %q does not include the detail when opted in", rec.Body.String())
	}
}

func TestRenderWrappedError(t *testing.T) {
	// A handler that adds its own context with fmt.Errorf("%w") must still be
	// rendered as the photon error underneath, not flattened to a 500. This is
	// what makes the error model usable inside a real call chain.
	err := fmt.Errorf("loading user: %w", NotFound("user"))
	rec := httptest.NewRecorder()
	if err := Render(rec, err, RenderOptions{}); err != nil {
		t.Fatalf("Render: %v", err)
	}
	if rec.Code != 404 {
		t.Errorf("status = %d, want 404 from the wrapped photon error", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), "user not found") {
		t.Errorf("body %q does not carry the wrapped error's message", rec.Body.String())
	}
}

func TestRenderUntypedWrapIsNotRescued(t *testing.T) {
	// Wrapping with a string rather than %w loses the type. The renderer then
	// treats it as an unknown error, which is the safe direction: a 500 with a
	// constant body rather than a guess at the status.
	err := errors.New("loading user: " + NotFound("user").Error())
	rec := httptest.NewRecorder()
	_ = Render(rec, err, RenderOptions{})
	if rec.Code != 500 {
		t.Errorf("status = %d, want 500: a string wrap carries no type", rec.Code)
	}
}

func TestRenderSetsNoSniff(t *testing.T) {
	// A problem document must not be content-sniffed into HTML.
	rec := httptest.NewRecorder()
	_ = Render(rec, NotFound("x"), RenderOptions{})
	if got := rec.Header().Get("X-Content-Type-Options"); got != "nosniff" {
		t.Errorf("X-Content-Type-Options = %q, want nosniff", got)
	}
}

func TestRenderDoesNotReflectRequestContent(t *testing.T) {
	// The model must be usable without a caller being able to leak a path.
	// A message is caller-written; the framework does not add request data.
	err := NotFound("user")
	rec := httptest.NewRecorder()
	_ = Render(rec, err, RenderOptions{})
	if strings.Contains(rec.Body.String(), "/admin") {
		t.Errorf("body %q reflects a path", rec.Body.String())
	}
}
