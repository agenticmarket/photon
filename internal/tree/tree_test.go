package tree

import (
	"errors"
	"net/http"
	"strings"
	"testing"
)

func handler(name string) http.Handler {
	return http.HandlerFunc(func(http.ResponseWriter, *http.Request) {})
}

// newTree registers patterns for a method, failing the test on conflict.
func newTree(t *testing.T, method string, patterns ...string) *Tree {
	t.Helper()
	tr := New()
	for _, p := range patterns {
		if err := tr.Add(&Route{Method: method, Path: p, Handler: handler(p)}); err != nil {
			t.Fatalf("Add(%q): %v", p, err)
		}
	}
	return tr
}

// find matches path and returns the route pattern plus its captures.
func find(t *testing.T, tr *Tree, path string) (string, map[string]string) {
	t.Helper()
	p := NewParams()
	r := tr.find(path, p)
	if r == nil {
		return "", nil
	}
	got := make(map[string]string, p.Len())
	for _, kv := range p.All() {
		got[kv.Name] = kv.Value
	}
	return r.Path, got
}

func TestParseRejects(t *testing.T) {
	tests := []struct {
		name    string
		pattern string
		wantErr string
	}{
		{"empty", "", "must not be empty"},
		{"no leading slash", "users/:id", "must start with"},
		{"query", "/users?sort=asc", "must not contain"},
		{"fragment", "/users#top", "must not contain"},
		{"double slash", "/users//posts", "empty segment"},
		{"trailing slash", "/users/", "ends with a slash"},
		{"empty segment mid", "/users//", "empty segment"},
		{"unnamed param", "/users/:", "unnamed wildcard"},
		{"unnamed catchall", "/files/*", "unnamed wildcard"},
		{"mixed suffix", "/v:id", "whole segment"},
		{"colon in literal", "/a:b", "whole segment"},
		{"star in literal", "/a*b", "whole segment"},
		{"two catchalls", "/a/*x/*y", "more than one catch-all"},
		{"after catchall", "/a/*x/b", "after its catch-all"},
		{"servemux param", "/users/{id}", "{id} becomes :id"},
		{"duplicate param name", "/a/:id/b/:id", "twice"},
		{"duplicate catchall name", "/a/:p/*p", "twice"},
		{"servemux catchall", "/files/{path...}", "*path"},
		{"param name starts with digit", "/users/:1id", "starting with a digit"},
		{"param name with dash", "/users/:user-id", "invalid wildcard name"},
		{"param name with space", "/users/:user id", "invalid wildcard name"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			tr := New()
			err := tr.Add(&Route{Method: "GET", Path: tc.pattern})
			if err == nil {
				t.Fatalf("Add(%q) succeeded, want error containing %q", tc.pattern, tc.wantErr)
			}
			if !errors.Is(err, ErrMalformed) {
				t.Errorf("error %v does not match ErrMalformed", err)
			}
			if !strings.Contains(err.Error(), tc.wantErr) {
				t.Errorf("error %q does not contain %q", err, tc.wantErr)
			}
		})
	}
}

func TestParseAccepts(t *testing.T) {
	patterns := []string{
		"/",
		"/healthz",
		"/users/:id",
		"/users/:id/posts/:postID",
		"/files/*path",
		"/a/:b/c/:d/e/*f",
		"/v1/users",
		"/_internal/:x",
		"/users/:id2",
		"/users/:id_2",
		"/u/:名前",
		"/a.b.c",
		"/a-b-c",
		"/percent%20literal",
	}
	for _, p := range patterns {
		t.Run(p, func(t *testing.T) {
			tr := New()
			if err := tr.Add(&Route{Method: "GET", Path: p}); err != nil {
				t.Fatalf("Add(%q): %v", p, err)
			}
		})
	}
}

func TestFindStatic(t *testing.T) {
	tr := newTree(t, "GET", "/", "/healthz", "/api/v1/users", "/api/v1/posts")
	tests := []struct{ path, want string }{
		{"/", "/"},
		{"/healthz", "/healthz"},
		{"/api/v1/users", "/api/v1/users"},
		{"/api/v1/posts", "/api/v1/posts"},
		{"/api/v1", ""},
		{"/api", ""},
		{"/healthz/", ""},
		{"/healthz2", ""},
		{"/api/v1/user", ""},
		{"", ""},
		{"healthz", ""},
	}
	for _, tc := range tests {
		t.Run(tc.path, func(t *testing.T) {
			got, _ := find(t, tr, tc.path)
			if got != tc.want {
				t.Errorf("find(%q) = %q, want %q", tc.path, got, tc.want)
			}
		})
	}
}

func TestFindParam(t *testing.T) {
	// A literal segment and a param segment at the same position are a
	// conflict by design, so this test uses patterns that do not collide.
	tr := newTree(t, "GET", "/users/:id", "/users/:id/posts/:postID",
		"/user/profile", "/user/settings")
	tests := []struct {
		path  string
		want  string
		param map[string]string
	}{
		{"/users/42", "/users/:id", map[string]string{"id": "42"}},
		{"/users/abc-def_1.2", "/users/:id", map[string]string{"id": "abc-def_1.2"}},
		{"/user/profile", "/user/profile", nil},
		{"/user/settings", "/user/settings", nil},
		{"/users/42/posts/7", "/users/:id/posts/:postID",
			map[string]string{"id": "42", "postID": "7"}},
		{"/users/42/posts/7/comments", "", nil},
		{"/users/42/posts", "", nil},
		{"/users", "", nil},
		{"/users/", "", nil},
	}
	for _, tc := range tests {
		t.Run(tc.path, func(t *testing.T) {
			got, params := find(t, tr, tc.path)
			if got != tc.want {
				t.Fatalf("find(%q) = %q, want %q", tc.path, got, tc.want)
			}
			if tc.want == "" {
				return
			}
			if len(params) != len(tc.param) {
				t.Fatalf("find(%q) params = %v, want %v", tc.path, params, tc.param)
			}
			for k, v := range tc.param {
				if params[k] != v {
					t.Errorf("param %q = %q, want %q", k, params[k], v)
				}
			}
		})
	}
}

func TestLiteralSiblingWinsOverCatchAll(t *testing.T) {
	// A literal and a catch-all at the same position are a registration
	// conflict, so priority between them is never decided at request time.
	// This test records the finding that matters: a catch-all is NOT allowed
	// to coexist with a literal below it, because the literal would be
	// unreachable and which one served would depend on registration order.
	// Static lookup runs first, so a literal always wins when both exist --
	// but registration makes that situation impossible to create.
	tr := newTree(t, "GET", "/a/*rest")
	if got, _ := find(t, tr, "/a/b"); got != "/a/*rest" {
		t.Errorf("find(/a/b) = %q, want the catch-all", got)
	}
	err := func() (err error) {
		defer func() {
			if r := recover(); r != nil {
				t.Fatalf("Add panicked: %v", r)
			}
		}()
		return tr.Add(&Route{Method: "GET", Path: "/a/b"})
	}()
	if err == nil {
		t.Error("registering a literal beneath a catch-all was allowed; " +
			"it must be a conflict so the shadowed route cannot exist")
	}
}

func TestFindCatchAll(t *testing.T) {
	// A catch-all shadows every other route beneath its prefix, so a literal
	// below it is a registration conflict rather than a priority decision.
	tr := newTree(t, "GET", "/files/*path", "/other")
	tests := []struct {
		path  string
		want  string
		param string
	}{
		{"/files/a", "/files/*path", "a"},
		{"/files/a/b/c", "/files/*path", "a/b/c"},
		{"/files/a%2Fb", "/files/*path", "a%2Fb"},
		{"/files", "", ""},
		{"/other", "/other", ""},
		{"/other/a", "", ""},
		{"/filese", "", ""},
		{"/files/", "", ""},
	}
	for _, tc := range tests {
		t.Run(tc.path, func(t *testing.T) {
			got, params := find(t, tr, tc.path)
			if got != tc.want {
				t.Fatalf("find(%q) = %q, want %q", tc.path, got, tc.want)
			}
			if tc.want == "" || tc.param == "" {
				return
			}
			if p := params["path"]; p != tc.param {
				t.Errorf("path param = %q, want %q", p, tc.param)
			}
		})
	}
}

func TestFindCatchAllRequiresNonEmptyRemainder(t *testing.T) {
	tr := newTree(t, "GET", "/files/*path")
	if got, _ := find(t, tr, "/files"); got != "" {
		t.Errorf("find(/files) = %q, want no match: a catch-all must not match an empty remainder", got)
	}
}

func TestFindDeepAndMaxDepth(t *testing.T) {
	deep := "/" + strings.Repeat("a/", MaxSegments-1) + "b"
	tr := newTree(t, "GET", deep)
	if got, _ := find(t, tr, deep); got != deep {
		t.Errorf("find(%q) = %q, want a match at the depth limit", deep, got)
	}
	// One segment deeper than the limit must not match, and must not panic.
	over := deep + "/c"
	if got, _ := find(t, tr, over); got != "" {
		t.Errorf("find(%q) = %q, want no match beyond MaxSegments", over, got)
	}
}

func TestMaxDepthCountsSegments(t *testing.T) {
	tr := newTree(t, "GET", "/a", "/a/b/c")
	if got, want := tr.MaxDepth(), 3; got != want {
		t.Errorf("MaxDepth = %d, want %d", got, want)
	}
}

func TestAddRejectsConflicts(t *testing.T) {
	tests := []struct {
		name    string
		first   string
		second  string
		wantErr string
	}{
		{"identical", "/users/:id", "/users/:id", "same path is already registered"},
		{"identical static", "/a/b", "/a/b", "same path is already registered"},
		{"identical renamed", "/users/:id", "/users/:name", "both patterns match the same paths"},
		{"static then catchall", "/admin/settings", "/admin/*rest", "catch-all also matches"},
		{"catchall then static", "/admin/*rest", "/admin/settings", "catch-all also matches"},
		{"static then param", "/admin/settings", "/admin/:page", "wildcard in one pattern matches"},
		{"param then static", "/admin/:page", "/admin/settings", "wildcard in one pattern matches"},
		{"catchall then deeper", "/a/*rest", "/a/b/c", "catch-all also matches"},
		{"catchall then same depth", "/a/*rest", "/a/b", "catch-all also matches"},
		{"catchall vs param", "/a/*rest", "/a/:id", "catch-all also matches"},
		{"catchall then root", "/a/*rest", "/", ""},
		{"param diverges late", "/a/:x/c", "/a/:y/d", ""},
		{"static siblings", "/a/b", "/a/c", ""},
		{"prefix then longer", "/a", "/a/b/c", ""},
		{"prefix then catchall", "/files", "/files/*path", ""},
		{"catchall then prefix", "/files/*path", "/files", ""},
		{"prefix then deeper catchall", "/a", "/a/b/*rest", ""},
		{"diverging branches", "/a/b/c", "/x/y/z", ""},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			tr := New()
			if err := tr.Add(&Route{Method: "GET", Path: tc.first}); err != nil {
				t.Fatalf("Add(%q): %v", tc.first, err)
			}
			err := tr.Add(&Route{Method: "GET", Path: tc.second})
			if tc.wantErr == "" {
				if err != nil {
					t.Fatalf("Add(%q) = %v, want no conflict", tc.second, err)
				}
				return
			}
			if err == nil {
				t.Fatalf("Add(%q) succeeded, want conflict containing %q", tc.second, tc.wantErr)
			}
			if !errors.Is(err, ErrConflict) {
				t.Errorf("error %v does not match ErrConflict", err)
			}
			if !strings.Contains(err.Error(), tc.wantErr) {
				t.Errorf("error %q does not contain %q", err, tc.wantErr)
			}
			for _, want := range []string{"new pattern:", "existing pattern:"} {
				if !strings.Contains(err.Error(), want) {
					t.Errorf("conflict error lacks %q: %v", want, err)
				}
			}
		})
	}
}

func TestAddAllowsSiblingAndPrefixRoutes(t *testing.T) {
	// These must all coexist: the shorter routes terminate at a different
	// segment count, and the siblings differ at a literal segment.
	patterns := []string{
		"/", "/api", "/api/users", "/api/users/:id", "/api/users/:id/posts",
		"/api/posts", "/healthz", "/static/*asset",
	}
	newTree(t, "GET", patterns...)
	for _, p := range patterns {
		path := strings.ReplaceAll(strings.ReplaceAll(p, ":id", "7"), "*asset", "x/y")
		if path == "/" {
			path = "/"
		}
		got, _ := find(t, newTree(t, "GET", p), path)
		if got != p {
			t.Errorf("pattern %q did not match its own path %q (got %q)", p, path, got)
		}
	}
}

func TestConflictErrorNamesBothCallSites(t *testing.T) {
	tr := New()
	if err := tr.Add(&Route{Method: "GET", Path: "/a/b", Caller: "first.go:1"}); err != nil {
		t.Fatal(err)
	}
	err := tr.Add(&Route{Method: "GET", Path: "/a/b", Caller: "second.go:2"})
	if err == nil {
		t.Fatal("expected conflict")
	}
	msg := err.Error()
	for _, want := range []string{"first.go:1", "second.go:2", `"GET"`, "/a/b"} {
		if !strings.Contains(msg, want) {
			t.Errorf("error %q missing %q", msg, want)
		}
	}
}

func TestParamsReuseAndCopy(t *testing.T) {
	tr := newTree(t, "GET", "/users/:id/posts/:postID")

	p := NewParams()
	if tr.find("/users/1/posts/2", p) == nil {
		t.Fatal("no match")
	}
	if p.Get("id") != "1" || p.Get("postID") != "2" {
		t.Fatalf("captures = %v", p.All())
	}
	// Reuse the same Params for a second request, as the router does.
	p.Reset()
	if tr.find("/users/9/posts/8", p) == nil {
		t.Fatal("no match on reuse")
	}
	if p.Get("id") != "9" || p.Get("postID") != "8" {
		t.Fatalf("after reuse captures = %v", p.All())
	}
	// A copy taken earlier must be unaffected by the reuse.
	saved := p.Copy()
	p.Reset()
	if tr.find("/users/5/posts/6", p) == nil {
		t.Fatal("no match after copy")
	}
	if saved[0].Value != "9" || saved[1].Value != "8" {
		t.Errorf("Copy was aliased: %v", saved)
	}
}

func TestParamsAtOutOfRange(t *testing.T) {
	p := NewParams()
	if _, ok := p.At(-1); ok {
		t.Error("At(-1) reported ok")
	}
	if _, ok := p.At(0); ok {
		t.Error("At(0) reported ok on empty Params")
	}
}

func TestParamsGetUnknownName(t *testing.T) {
	tr := newTree(t, "GET", "/users/:id")
	p := NewParams()
	if r := tr.find("/users/1", p); r == nil {
		t.Fatal("no match")
	}
	if got := p.Get("missing"); got != "" {
		t.Errorf("Get(missing) = %q, want empty", got)
	}
}

func TestEncodedTraversalDoesNotChangeSegmentation(t *testing.T) {
	// The router receives the raw path. An encoded slash must not act as a
	// segment separator, so "/admin%2Fsettings" is one segment. It cannot
	// match "/admin/:page", because that would make "%2F" a separator and
	// turn an encoded slash into a routing bypass.
	tr := newTree(t, "GET", "/admin/:page", "/other")
	if got, _ := find(t, tr, "/admin%2Fsettings"); got != "" {
		t.Errorf("find(/admin%%2Fsettings) = %q, want no match", got)
	}
	// The decoded form does match, and that is the point: routing on the raw
	// path means the two forms are distinguishable.
	if got, _ := find(t, tr, "/admin/settings"); got != "/admin/:page" {
		t.Errorf("find(/admin/settings) = %q, want a match", got)
	}
}

func TestCaseSensitivity(t *testing.T) {
	tr := newTree(t, "GET", "/Users/:id")
	if got, _ := find(t, tr, "/users/1"); got != "" {
		t.Errorf("find(/users/1) = %q, want no match: paths are case-sensitive", got)
	}
	if got, _ := find(t, tr, "/Users/1"); got != "/Users/:id" {
		t.Errorf("find(/Users/1) = %q, want a match", got)
	}
}

func TestNoRouteDataRaceAcrossGoroutines(t *testing.T) {
	// Matching is read-only after registration; concurrent readers must be
	// safe. Run with -race.
	tr := newTree(t, "GET", "/users/:id", "/healthz", "/files/*path")
	done := make(chan struct{})
	for g := 0; g < 8; g++ {
		go func(g int) {
			defer func() { done <- struct{}{} }()
			p := NewParams()
			for i := 0; i < 500; i++ {
				switch (g + i) % 3 {
				case 0:
					tr.find("/users/7", p)
				case 1:
					tr.find("/healthz", p)
				case 2:
					tr.find("/files/a/b", p)
				}
			}
		}(g)
	}
	for g := 0; g < 8; g++ {
		<-done
	}
}

func FuzzFind(f *testing.F) {
	seeds := []string{
		"/", "/a", "/a/b", "/a/b/c", "//", "///", "/a/", "/a//b",
		"/%2e%2e/%2e%2e/etc/passwd", "/%252e%252e/etc/passwd",
		"/admin%2Fsettings", "/admin/settings", "/admin", "/admin/",
		"/users/1", "/users/1/posts/2", "/users//posts", "/users/1/posts",
		"/files/", "/files/a", "/files/a/b", "/files/a//b",
		strings.Repeat("/a", MaxSegments), strings.Repeat("/", 40),
		"/\x00", "/%00", "/a%2Fb%2ec", "/a:b", "/:a", "/*a",
	}
	for _, s := range seeds {
		f.Add(s)
	}
	tr := New()
	for _, p := range []string{"/a", "/a/b", "/a/b/c", "/admin/:page",
		"/user/settings", "/users/:id", "/users/:id/posts/:pid", "/files/*path"} {
		if err := tr.Add(&Route{Method: "GET", Path: p}); err != nil {
			f.Fatalf("Add(%q): %v", p, err)
		}
	}
	f.Fuzz(func(t *testing.T, path string) {
		p := NewParams()
		r := tr.find(path, p)
		if r == nil {
			return
		}
		// Every capture of a successful match must be a substring of the path
		// as received, and no capture may be longer than the path.
		if len(p.All()) > MaxSegments {
			t.Fatalf("captured %d params, want at most %d", len(p.All()), MaxSegments)
		}
		for _, kv := range p.All() {
			if !strings.Contains(path, kv.Value) && kv.Value != "" {
				t.Errorf("capture %q is not a substring of %q", kv.Value, path)
			}
		}
	})
}
