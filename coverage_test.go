package photon

import (
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// TestEveryVerbDispatches covers the verbs that have no dedicated test, so a
// copy-paste error in one of them cannot ship.
func TestEveryVerbDispatches(t *testing.T) {
	verbs := map[string]func(*Router, string, http.HandlerFunc, ...Middleware){
		"GET":     (*Router).GET,
		"POST":    (*Router).POST,
		"PUT":     (*Router).PUT,
		"PATCH":   (*Router).PATCH,
		"DELETE":  (*Router).DELETE,
		"HEAD":    (*Router).HEAD,
		"OPTIONS": (*Router).OPTIONS,
	}
	rt := NewRouter()
	for method, register := range verbs {
		m := method
		register(rt, "/x", HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Method != m {
				t.Errorf("handler saw method %q, want %q", r.Method, m)
			}
			_, _ = io.WriteString(w, m)
		}))
	}
	for method := range verbs {
		rec := do(t, rt, method, "/x")
		if rec.Code != 200 {
			t.Errorf("%s status = %d, want 200", method, rec.Code)
		}
		if rec.Body.String() != method {
			t.Errorf("%s body = %q, want %q", method, rec.Body.String(), method)
		}
	}
}

func TestServerVerbPassThrough(t *testing.T) {
	s := New()
	s.PUT("/p", HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, "put")
	}))
	s.PATCH("/p", HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, "patch")
	}))
	s.HEAD("/h", HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Probe", "1")
	}))
	s.OPTIONS("/o", HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Allow", "GET, OPTIONS")
		_, _ = io.WriteString(w, "options")
	}))

	rt := s.Router()
	for _, tc := range []struct{ method, target, want string }{
		{"PUT", "/p", "put"},
		{"PATCH", "/p", "patch"},
		{"OPTIONS", "/o", "options"},
	} {
		rec := do(t, rt, tc.method, tc.target)
		if rec.Code != 200 {
			t.Errorf("%s %s status = %d, want 200", tc.method, tc.target, rec.Code)
		}
		if rec.Body.String() != tc.want {
			t.Errorf("%s %s body = %q, want %q", tc.method, tc.target, rec.Body.String(), tc.want)
		}
	}
	// HEAD is served by its own explicit handler, never derived from GET.
	rec := do(t, rt, "HEAD", "/h")
	if rec.Code != 200 {
		t.Errorf("HEAD status = %d, want 200", rec.Code)
	}
	if rec.Header().Get("X-Probe") != "1" {
		t.Error("HEAD did not reach the registered HEAD handler")
	}
}

func TestServerNotFoundAndUsePassThrough(t *testing.T) {
	var ran int
	s := New()
	s.Use(func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			ran++
			next.ServeHTTP(w, r)
		})
	})
	s.GET("/a", HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, "a")
	}))
	s.NotFound(HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, "custom-404")
	}))
	s.MethodNotAllowed(HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, "custom-405")
	}))

	rt := s.Router()
	do(t, rt, "GET", "/a")
	if ran != 1 {
		t.Errorf("middleware ran %d times, want 1", ran)
	}
	if got := do(t, rt, "GET", "/missing").Body.String(); got != "custom-404" {
		t.Errorf("404 body = %q, want custom-404", got)
	}
	if got := do(t, rt, "POST", "/a").Body.String(); got != "custom-405" {
		t.Errorf("405 body = %q, want custom-405", got)
	}
}

func TestServerRoutesAndHandlePassThrough(t *testing.T) {
	s := New()
	s.Handle("GET", "/h", HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, "h")
	}))
	s.GET("/g", HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))

	routes := s.Routes()
	if len(routes) != 2 {
		t.Fatalf("Routes() = %d, want 2", len(routes))
	}
	if routes[0].Path != "/h" || routes[1].Path != "/g" {
		t.Errorf("routes = %+v, want registration order /h, /g", routes)
	}
}

func TestServerUseAfterFreezePanics(t *testing.T) {
	rt := NewRouter()
	rt.Freeze()
	defer func() {
		if r := recover(); r == nil {
			t.Error("Use after Freeze did not panic")
		}
	}()
	rt.Use(func(next http.Handler) http.Handler { return next })
}

func TestBodyLimitStopsAStreamingReadMidBody(t *testing.T) {
	// The limit must hold for a body read in pieces, not only for a
	// Content-Length-declared body. A chunked request has no declared length,
	// so the only enforcement point is the reader.
	var readErr error
	var total int64
	s := New()
	s.POST("/stream", HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		buf := make([]byte, 64<<10)
		for {
			n, err := r.Body.Read(buf)
			total += int64(n)
			if err != nil {
				readErr = err
				return
			}
		}
	}))
	addr := startServer(t, s)

	// Send more than the 1 MiB default in chunks, with no Content-Length.
	pr, pw := io.Pipe()
	go func() {
		chunk := strings.Repeat("x", 64<<10)
		for i := 0; i < 40; i++ { // 2.5 MB
			if _, err := io.WriteString(pw, chunk); err != nil {
				break
			}
		}
		_ = pw.Close()
	}()

	req, _ := http.NewRequest("POST", addr+"/stream", pr)
	req.ContentLength = -1 // force chunked, so the limit is enforced by the reader
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("request: %v", err)
	}
	defer resp.Body.Close()

	// The handler returned early on the error, so it wrote nothing; the
	// important assertion is that it stopped near the limit.
	if total > (1<<20)+128<<10 {
		t.Errorf("handler read %d bytes, want it to stop near the %d byte limit",
			total, 1<<20)
	}
	if readErr == nil {
		t.Error("handler saw no error: an oversized chunked body was not stopped")
	}
}

// TestBodyLimitErrorIsMaxBytesError checks the error a handler sees when a body
// runs past MaxRequestBodyBytes is the standard *http.MaxBytesError, carrying
// the configured limit. That is the type DecodeJSON and every net/http-aware
// library already recognise; a private error type would leave callers no way
// to tell "too large" from "malformed".
func TestBodyLimitErrorIsMaxBytesError(t *testing.T) {
	l := DefaultLimits()
	l.MaxRequestBodyBytes = 10
	s := New(SetLimits(l))
	got := make(chan error, 1)
	s.POST("/u", func(w http.ResponseWriter, r *http.Request) {
		_, err := io.ReadAll(r.Body)
		got <- err
	})
	addr := startServer(t, s)

	req, _ := http.NewRequest("POST", addr+"/u", io.MultiReader(strings.NewReader("0123456789-overflow")))
	req.ContentLength = -1 // chunked: only the reader can enforce the limit
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()

	var tooLarge *http.MaxBytesError
	if err := <-got; !errors.As(err, &tooLarge) {
		t.Fatalf("handler read error = %v, want *http.MaxBytesError", err)
	}
	if tooLarge.Limit != 10 {
		t.Errorf("MaxBytesError.Limit = %d, want the configured 10", tooLarge.Limit)
	}
}

// TestBodyExactlyAtTheLimitIsAccepted is the regression test for a body of
// exactly MaxRequestBodyBytes being reported as too large.
//
// The original reader returned its limit error on the read AFTER the last
// allowed byte, before the underlying body had a chance to report EOF, so a
// chunked upload of exactly the limit failed. A limit is a maximum, not an
// exclusive bound.
func TestBodyExactlyAtTheLimitIsAccepted(t *testing.T) {
	l := DefaultLimits()
	l.MaxRequestBodyBytes = 10
	s := New(SetLimits(l))
	s.POST("/u", func(w http.ResponseWriter, r *http.Request) {
		b, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		_, _ = w.Write(b)
	})
	addr := startServer(t, s)

	for _, chunked := range []bool{true, false} {
		req, _ := http.NewRequest("POST", addr+"/u", strings.NewReader("0123456789"))
		if chunked {
			req.ContentLength = -1
			req.Body = io.NopCloser(io.MultiReader(strings.NewReader("0123456789")))
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if resp.StatusCode != http.StatusOK || string(body) != "0123456789" {
			t.Errorf("chunked=%v: status %d body %q, want 200 and the full 10-byte body",
				chunked, resp.StatusCode, body)
		}
	}
}

func TestRequestPathPrefersRawPath(t *testing.T) {
	// requestPath decides what the router matches on, and the raw form is what
	// stops an encoded slash from becoming a separator.
	req := httptest.NewRequest("GET", "/admin%2Fsettings", nil)
	if got := requestPath(req); got != "/admin%2Fsettings" {
		t.Errorf("requestPath = %q, want the raw path", got)
	}
	req2 := httptest.NewRequest("GET", "/plain/path", nil)
	if got := requestPath(req2); got != "/plain/path" {
		t.Errorf("requestPath = %q, want the plain path", got)
	}
}

func TestParamsAccessorOnPlainRequest(t *testing.T) {
	// A request that did not come from the router has no captures, and the
	// accessors must say so rather than panic.
	req := httptest.NewRequest("GET", "/x", nil)
	if got := PathParam(req, "id"); got != "" {
		t.Errorf("PathParam on a plain request = %q, want empty", got)
	}
	if got := Params(req); got != nil {
		t.Errorf("Params on a plain request = %v, want nil", got)
	}
	if got := CopyParams(req); got != nil {
		t.Errorf("CopyParams on a plain request = %v, want nil", got)
	}
	if got := CopyParam(req, "id"); got != "" {
		t.Errorf("CopyParam on a plain request = %q, want empty", got)
	}
}

func TestParamsOrder(t *testing.T) {
	var got []Param
	s := New()
	s.GET("/a/:x/b/:y/c/:z", HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got = CopyParams(r)
	}))
	rec := httptest.NewRecorder()
	s.Router().ServeHTTP(rec, httptest.NewRequest("GET", "/a/1/b/2/c/3", nil))
	if len(got) != 3 {
		t.Fatalf("captured %d params, want 3", len(got))
	}
	want := []struct{ name, value string }{{"x", "1"}, {"y", "2"}, {"z", "3"}}
	for i, w := range want {
		if got[i].Name != w.name || got[i].Value != w.value {
			t.Errorf("param %d = %s=%s, want %s=%s", i,
				got[i].Name, got[i].Value, w.name, w.value)
		}
	}
}

func TestCatchAllCapturesSlashes(t *testing.T) {
	var got string
	s := New()
	s.GET("/files/*path", HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got = CopyParam(r, "path")
	}))
	rec := httptest.NewRecorder()
	s.Router().ServeHTTP(rec, httptest.NewRequest("GET", "/files/a/b/c/d.txt", nil))
	if got != "a/b/c/d.txt" {
		t.Errorf("captured %q, want the whole remainder a/b/c/d.txt", got)
	}
}
