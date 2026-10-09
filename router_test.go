package photon

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// echo returns a handler that writes name, so a response identifies the route
// that served it.
func echo(name string) http.HandlerFunc {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(name))
	})
}

func newTestRouter(t *testing.T) *Router {
	t.Helper()
	rt := NewRouter()
	rt.GET("/", echo("root"))
	rt.GET("/healthz", echo("healthz"))
	rt.GET("/api/users", echo("users"))
	rt.GET("/users/:id", echo("user"))
	rt.GET("/users/:id/posts/:postID", echo("post"))
	rt.GET("/files/*path", echo("file"))
	rt.POST("/users", echo("create-user"))
	rt.DELETE("/users/:id", echo("delete-user"))
	return rt
}

func do(t *testing.T, rt http.Handler, method, target string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(method, target, nil)
	rec := httptest.NewRecorder()
	rt.ServeHTTP(rec, req)
	return rec
}

func TestRouterDispatch(t *testing.T) {
	rt := newTestRouter(t)
	tests := []struct {
		method, target string
		status         int
		body           string
	}{
		{"GET", "/", 200, "root"},
		{"GET", "/healthz", 200, "healthz"},
		{"GET", "/api/users", 200, "users"},
		{"GET", "/users/42", 200, "user"},
		{"GET", "/users/42/posts/7", 200, "post"},
		{"GET", "/files/a/b.txt", 200, "file"},
		{"POST", "/users", 200, "create-user"},
		{"DELETE", "/users/42", 200, "delete-user"},
	}
	for _, tc := range tests {
		t.Run(tc.method+" "+tc.target, func(t *testing.T) {
			rec := do(t, rt, tc.method, tc.target)
			if rec.Code != tc.status {
				t.Errorf("status = %d, want %d", rec.Code, tc.status)
			}
			if got := rec.Body.String(); got != tc.body {
				t.Errorf("body = %q, want %q", got, tc.body)
			}
		})
	}
}

func TestRouterNotFound(t *testing.T) {
	rt := newTestRouter(t)
	// "/users" is registered for POST only, so a GET is a 405, not a 404.
	for _, target := range []string{"/nope", "/api", "/users/1/extra", "/file"} {
		t.Run(target, func(t *testing.T) {
			rec := do(t, rt, "GET", target)
			if rec.Code != http.StatusNotFound {
				t.Errorf("status = %d, want 404", rec.Code)
			}
			// The body must not reflect the requested path: that is a route
			// enumeration oracle and an XSS vector if the type is mis-sniffed.
			if strings.Contains(rec.Body.String(), strings.TrimPrefix(target, "/")) &&
				target != "/nope" {
				t.Errorf("404 body %q reflects the request path", rec.Body.String())
			}
			if got := rec.Header().Get("Content-Type"); !strings.HasPrefix(got, "text/plain") {
				t.Errorf("Content-Type = %q, want text/plain", got)
			}
			if got := rec.Header().Get("X-Content-Type-Options"); got != "nosniff" {
				t.Errorf("X-Content-Type-Options = %q, want nosniff", got)
			}
		})
	}
}

func TestRouterMethodNotAllowed(t *testing.T) {
	rt := newTestRouter(t)
	rec := do(t, rt, "PUT", "/users/42")
	if rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("status = %d, want 405", rec.Code)
	}
	allow := rec.Header().Get("Allow")
	for _, want := range []string{"DELETE", "GET"} {
		if !strings.Contains(allow, want) {
			t.Errorf("Allow = %q, want it to contain %q", allow, want)
		}
	}
	if strings.Contains(allow, "PUT") {
		t.Errorf("Allow = %q, must not list the requested method", allow)
	}
}

func TestRouterAllowIsSorted(t *testing.T) {
	rt := NewRouter()
	rt.POST("/x", echo("post"))
	rt.DELETE("/x", echo("delete"))
	rt.GET("/x", echo("get"))
	rt.PUT("/x", echo("put"))
	rec := do(t, rt, "HEAD", "/x")
	got := rec.Header().Get("Allow")
	want := "DELETE, GET, POST, PUT"
	if got != want {
		t.Errorf("Allow = %q, want %q (sorted, so the header is stable)", got, want)
	}
}

func TestRouterPathParams(t *testing.T) {
	var gotID, gotPost string
	rt := NewRouter()
	rt.GET("/users/:id/posts/:postID", HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotID = PathParam(r, "id")
		gotPost = PathParam(r, "postID")
	}))
	do(t, rt, "GET", "/users/42/posts/7")
	if gotID != "42" || gotPost != "7" {
		t.Errorf("params = %q, %q, want 42, 7", gotID, gotPost)
	}
}

func TestPathParamUnknownName(t *testing.T) {
	var got string
	rt := NewRouter()
	rt.GET("/users/:id", HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got = PathParam(r, "nope")
	}))
	do(t, rt, "GET", "/users/1")
	if got != "" {
		t.Errorf("PathParam(nope) = %q, want empty", got)
	}
}

func TestPathParamOnStaticRouteIsEmpty(t *testing.T) {
	var got string
	rt := NewRouter()
	rt.GET("/healthz", HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got = PathParam(r, "id")
	}))
	do(t, rt, "GET", "/healthz")
	if got != "" {
		t.Errorf("PathParam on a static route = %q, want empty", got)
	}
}

func TestCopyParams(t *testing.T) {
	var saved []Param
	rt := NewRouter()
	rt.GET("/users/:id", HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Save only from the first request, so the assertion below checks
		// that an earlier copy survives a later request rather than that the
		// handler ran again.
		if saved == nil {
			saved = CopyParams(r)
		}
	}))
	do(t, rt, "GET", "/users/42")
	if len(saved) != 1 || saved[0].Name != "id" || saved[0].Value != "42" {
		t.Fatalf("CopyParams = %+v, want one id=42", saved)
	}
	// A later request on the same router must not disturb the saved copy.
	do(t, rt, "GET", "/users/99")
	if saved[0].Value != "42" {
		t.Errorf("CopyParams was aliased: got %q, want 42", saved[0].Value)
	}
}

func TestCopyParamClonesTheString(t *testing.T) {
	// CopyParam must clone the string, not just the slice. Cloning only the
	// slice would leave every value aliasing the request path, which is the
	// exact lifetime bug the copy exists to prevent.
	var first string
	rt := NewRouter()
	rt.GET("/users/:id", HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if first == "" {
			first = CopyParam(r, "id")
		}
	}))
	do(t, rt, "GET", "/users/42")
	do(t, rt, "GET", "/users/99")
	if first != "42" {
		t.Errorf("CopyParam = %q after a later request, want 42", first)
	}
}

func TestParamsAreScopedToRequest(t *testing.T) {
	// A param read from one request must not be visible in another. This is the
	// cross-request bleed the design's lifetime rule exists to prevent.
	var leaked string
	rt := NewRouter()
	rt.GET("/users/:id", HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		leaked = PathParam(r, "id")
	}))
	rt.GET("/healthz", HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// A different route on the same router must see no params at all.
		if got := PathParam(r, "id"); got != "" {
			t.Errorf("static route saw param %q", got)
		}
	}))
	do(t, rt, "GET", "/users/1")
	if leaked != "1" {
		t.Fatalf("leaked = %q, want 1", leaked)
	}
	do(t, rt, "GET", "/healthz")
}

func TestEncodedSlashIsNotASeparator(t *testing.T) {
	// Routing on the raw path means %2F does not split segments, so
	// "/admin%2Fsettings" cannot reach a handler as "/admin/:page".
	rt := NewRouter()
	rt.GET("/admin/:page", echo("admin-page"))
	rec := do(t, rt, "GET", "/admin%2Fsettings")
	if rec.Code != http.StatusNotFound {
		t.Errorf("status = %d, want 404: an encoded slash must not cross a segment boundary", rec.Code)
	}
	rec = do(t, rt, "GET", "/admin/settings")
	if rec.Code != http.StatusOK {
		t.Errorf("status = %d, want 200 for the decoded form", rec.Code)
	}
}

// panicMessage renders a recovered value as a string, for assertions on a
// panic that may be a string or an error.
func panicMessage(v any) string {
	switch e := v.(type) {
	case string:
		return e
	case error:
		return e.Error()
	default:
		return ""
	}
}

func TestRouterPanicsOnConflict(t *testing.T) {
	tests := []struct {
		name   string
		first  string
		second string
		want   string
	}{
		{"duplicate", "/a", "/a", "same path is already registered"},
		{"static vs param", "/admin/settings", "/admin/:page", "wildcard in one pattern"},
		{"catchall vs literal", "/admin/*rest", "/admin/settings", "catch-all also matches"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			rt := NewRouter()
			rt.GET(tc.first, echo("a"))
			defer func() {
				r := recover()
				if r == nil {
					t.Fatalf("registering %q after %q did not panic", tc.second, tc.first)
				}
				msg := panicMessage(r)
				if !strings.Contains(msg, tc.want) {
					t.Errorf("panic %q does not contain %q", msg, tc.want)
				}
			}()
			rt.GET(tc.second, echo("b"))
		})
	}
}

func TestRouterPanicsOnMalformedPattern(t *testing.T) {
	for _, pattern := range []string{"", "users", "/users/", "/a//b", "/a/:", "/a/*x/b", "/a/:1x"} {
		t.Run(pattern, func(t *testing.T) {
			rt := NewRouter()
			defer func() {
				if recover() == nil {
					t.Errorf("registering %q did not panic", pattern)
				}
			}()
			rt.GET(pattern, echo("h"))
		})
	}
}

func TestRouterPanicsOnNilHandler(t *testing.T) {
	rt := NewRouter()
	defer func() {
		if r := recover(); r == nil {
			t.Error("registering a nil handler did not panic")
		}
	}()
	rt.GET("/x", nil)
}

func TestRegistrationAfterFreezePanics(t *testing.T) {
	rt := NewRouter()
	rt.GET("/a", echo("a"))
	rt.Freeze()
	defer func() {
		r := recover()
		if r == nil {
			t.Fatal("registering after Freeze did not panic")
		}
		if msg, _ := r.(string); !strings.Contains(msg, "frozen") &&
			!strings.Contains(msg, "before Listen or Serve") {
			t.Errorf("panic %q does not explain that the router is frozen", msg)
		}
	}()
	rt.GET("/b", echo("b"))
}

func TestPerRouteMiddleware(t *testing.T) {
	var order []string
	mark := func(name string) Middleware {
		return func(next http.Handler) http.Handler {
			return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				order = append(order, name)
				next.ServeHTTP(w, r)
			})
		}
	}
	rt := NewRouter()
	rt.GET("/a", echo("a"), mark("r1"), mark("r2"))
	do(t, rt, "GET", "/a")
	want := []string{"r1", "r2"}
	if len(order) != 2 || order[0] != want[0] || order[1] != want[1] {
		t.Errorf("order = %v, want %v (outermost first)", order, want)
	}
}

func TestUseAppliesToEveryRoute(t *testing.T) {
	// Use covers routes registered before it as well as after. An earlier
	// version compiled middleware into routes at registration, so a route
	// declared above Use silently ran without it - an auth or rate-limit check
	// that quietly did not run, depending on line order.
	var ran int
	rt := NewRouter()
	rt.GET("/before", echo("before"))
	rt.Use(func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			ran++
			next.ServeHTTP(w, r)
		})
	})
	rt.GET("/after", echo("after"))

	do(t, rt, "GET", "/before")
	do(t, rt, "GET", "/after")
	if ran != 2 {
		t.Errorf("middleware ran %d times for two requests, want 2", ran)
	}
}

func TestCustomNotFoundAndMethodNotAllowed(t *testing.T) {
	rt := NewRouter()
	rt.GET("/a", echo("a"))
	rt.NotFound(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(404)
		_, _ = w.Write([]byte("custom-404"))
	}))
	rt.MethodNotAllowed(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(405)
		_, _ = w.Write([]byte("custom-405"))
	}))

	if got := do(t, rt, "GET", "/missing").Body.String(); got != "custom-404" {
		t.Errorf("404 body = %q, want custom-404", got)
	}
	rec := do(t, rt, "POST", "/a")
	if got := rec.Body.String(); got != "custom-405" {
		t.Errorf("405 body = %q, want custom-405", got)
	}
	if rec.Header().Get("Allow") != "GET" {
		t.Errorf("Allow = %q, want GET even with a custom handler", rec.Header().Get("Allow"))
	}
}

func TestRoutesIntrospection(t *testing.T) {
	rt := NewRouter()
	rt.GET("/healthz", echo("h"))
	rt.GET("/users/:id", echo("u"))
	rt.POST("/users", echo("c"))

	routes := rt.Routes()
	if len(routes) != 3 {
		t.Fatalf("Routes() = %d routes, want 3", len(routes))
	}
	if routes[0].Path != "/healthz" || routes[0].Method != "GET" {
		t.Errorf("route 0 = %+v, want GET /healthz in registration order", routes[0])
	}
	if routes[1].Path != "/users/:id" || len(routes[1].Params) != 1 ||
		routes[1].Params[0] != "id" {
		t.Errorf("route 1 = %+v, want GET /users/:id with param id", routes[1])
	}

	// The returned slice must be a copy.
	routes[0].Path = "/mutated"
	if rt.Routes()[0].Path != "/healthz" {
		t.Error("Routes() returned a slice aliasing the router's state")
	}
}

func TestLookup(t *testing.T) {
	rt := NewRouter()
	rt.GET("/users/:id", echo("u"))
	h, params, allow := rt.Lookup("GET", "/users/7")
	if h == nil {
		t.Fatal("Lookup returned no handler")
	}
	if len(params) != 1 || params[0].Value != "7" {
		t.Errorf("params = %+v, want one 7", params)
	}
	if allow != nil {
		t.Errorf("allow = %v, want nil on a match", allow)
	}
	_, _, allow = rt.Lookup("POST", "/users/7")
	if len(allow) != 1 || allow[0] != "GET" {
		t.Errorf("allow = %v, want [GET]", allow)
	}
}

func TestRouterConcurrentLookups(t *testing.T) {
	rt := newTestRouter(t)
	const goroutines = 16
	done := make(chan struct{}, goroutines)
	for g := 0; g < goroutines; g++ {
		go func(g int) {
			defer func() { done <- struct{}{} }()
			targets := []string{"/", "/healthz", "/users/1", "/users/1/posts/2", "/files/a/b", "/missing"}
			for i := 0; i < 300; i++ {
				do(t, rt, "GET", targets[(g+i)%len(targets)])
			}
		}(g)
	}
	for g := 0; g < goroutines; g++ {
		<-done
	}
}

func TestClientIPIgnoresForwardedHeaders(t *testing.T) {
	req := httptest.NewRequest("GET", "/", nil)
	req.RemoteAddr = "203.0.113.7:1234"
	req.Header.Set("X-Forwarded-For", "1.2.3.4")
	req.Header.Set("X-Real-IP", "5.6.7.8")

	if got := ClientIP(req).String(); got != "203.0.113.7" {
		t.Errorf("ClientIP = %s, want the peer address 203.0.113.7: "+
			"forwarded headers must not be trusted by default", got)
	}
}

func TestClientIPHandlesMalformedRemoteAddr(t *testing.T) {
	req := httptest.NewRequest("GET", "/", nil)
	req.RemoteAddr = "not-an-address"
	if got := ClientIP(req); got.IsValid() {
		t.Errorf("ClientIP = %v, want the zero address for an unparseable RemoteAddr", got)
	}
}

func TestAcceptsGzip(t *testing.T) {
	tests := []struct {
		header string
		want   bool
	}{
		{"", false},
		{"gzip", true},
		{"gzip, deflate", true},
		{"deflate", false},
		{"*", true},
		{" gzip ", true},
	}
	for _, tc := range tests {
		t.Run(tc.header, func(t *testing.T) {
			req := httptest.NewRequest("GET", "/", nil)
			if tc.header != "" {
				req.Header.Set("Accept-Encoding", tc.header)
			}
			if got := AcceptsGzip(req); got != tc.want {
				t.Errorf("AcceptsGzip = %v, want %v", got, tc.want)
			}
		})
	}
}
