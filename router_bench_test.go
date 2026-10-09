package photon

import (
	"fmt"
	"github.com/agenticmarket/photon/internal/tree"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"
)

// Benchmark conditions are recorded alongside every result in bench/README.md.
// The numbers below are measurements, not targets.

// benchRouter builds a router with n static routes plus a fixed set of
// parametrised ones, and returns the path of a static route and a
// parametrised route within it.
func benchRouter(n int) (rt *Router, staticPath, paramPath string) {
	rt = NewRouter()
	for i := 0; i < n; i++ {
		p := "/api/v1/resource" + strconv.Itoa(i) + "/item"
		rt.GET(p, http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
		staticPath = p
	}
	rt.GET("/api/v1/users/:id/posts/:postID", http.HandlerFunc(
		func(http.ResponseWriter, *http.Request) {}))
	rt.GET("/files/*path", http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	return rt, staticPath, "/api/v1/users/42/posts/7"
}

// benchServe times a full ServeHTTP dispatch, which is the cost a real request
// pays. The recorder is created once and Reset per iteration: creating it
// inside the loop would add three allocations per request that belong to
// httptest rather than to the router, and would make every number here wrong.
func benchServe(b *testing.B, rt *Router, method, target string) {
	b.Helper()
	req := httptest.NewRequest(method, target, nil)
	rec := httptest.NewRecorder()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rec.Body.Reset()
		rt.ServeHTTP(rec, req)
	}
}

func BenchmarkRouterStatic(b *testing.B) {
	for _, n := range []int{10, 100, 1000, 10000} {
		b.Run(fmt.Sprintf("routes=%d", n), func(b *testing.B) {
			rt, path, _ := benchRouter(n)
			benchServe(b, rt, "GET", path)
		})
	}
}

func BenchmarkRouterParam(b *testing.B) {
	for _, n := range []int{10, 100, 1000, 10000} {
		b.Run(fmt.Sprintf("routes=%d", n), func(b *testing.B) {
			rt, _, path := benchRouter(n)
			benchServe(b, rt, "GET", path)
		})
	}
}

func BenchmarkRouterCatchAll(b *testing.B) {
	rt, _, _ := benchRouter(100)
	benchServe(b, rt, "GET", "/files/a/b/c/d/e.txt")
}

func BenchmarkRouterMiss(b *testing.B) {
	// A miss is the worst case: the static map misses and the tree is walked.
	rt, _, _ := benchRouter(100)
	benchServe(b, rt, "GET", "/api/v1/nonexistent/deep/path")
}

func BenchmarkRouterMethodNotAllowed(b *testing.B) {
	// A 405 walks every method's tree to build Allow, so it is deliberately
	// the most expensive path. If this is ever hot, the Allow header is the
	// thing to make lazy.
	rt, _, _ := benchRouter(100)
	benchServe(b, rt, "PUT", "/api/v1/users/42/posts/7")
}

func BenchmarkRouterParamLookupOnly(b *testing.B) {
	// Lookup without the response writer, isolating routing from the handler
	// call and the recorder.
	rt, _, path := benchRouter(1000)
	req := httptest.NewRequest("GET", path, nil)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _, _ = rt.Lookup("GET", path)
	}
	_ = req
}

// TestRouterZeroAllocations is the gate for the design's central claim.
//
// The claim is that the static hot path allocates nothing, and TestRouter-
// LookupZeroAllocations proves routing itself never allocates for any path
// shape. A parametrised request cannot reach zero end to end, because Go's
// request context is immutable: attaching the captures costs a holder, a value
// context, a request copy, and a copy of the captures out of the pooled buffer.
// There is no way to hand a pointer to a handler that receives only
// *http.Request, so four is the floor for this API shape.
//
// The ceiling is asserted so a regression past it fails the build, and the
// measured value is printed so the cost stays visible. If a future Go version
// makes this cheaper the ceiling fails and gets lowered; that is the intended
// direction.
func TestRouterZeroAllocations(t *testing.T) {
	const (
		staticCeiling = 0.0
		paramCeiling  = 4.0
	)
	cases := []struct {
		name, target string
		ceiling      float64
	}{
		{"static", "/api/v1/resource99/item", staticCeiling},
		{"param", "/api/v1/users/42/posts/7", paramCeiling},
		{"catchall", "/files/a/b/c.txt", paramCeiling},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rt, _, _ := benchRouter(1000)
			req := httptest.NewRequest("GET", tc.target, nil)
			rec := httptest.NewRecorder()

			// Warm the router so any first-use setup is not counted.
			for i := 0; i < 100; i++ {
				rec.Body.Reset()
				rt.ServeHTTP(rec, req)
			}

			allocs := testing.AllocsPerRun(500, func() {
				rec.Body.Reset()
				rt.ServeHTTP(rec, req)
			})
			t.Logf("%s: %.0f allocs/request (ceiling %.0f)", tc.name, allocs, tc.ceiling)
			if raceEnabled {
				t.Skip("allocation budget not meaningful under -race; see race_on_test.go")
			}
			if allocs > tc.ceiling {
				t.Errorf("ServeHTTP allocated %.0f times per request, want at most %.0f",
					allocs, tc.ceiling)
			}
		})
	}
}

// TestRouterLookupZeroAllocations gates the routing step on its own, with no
// response writer and no context, which is the number the design predicted.
func TestRouterLookupZeroAllocations(t *testing.T) {
	cases := []struct{ name, target string }{
		{"static", "/api/v1/resource99/item"},
		{"param", "/api/v1/users/42/posts/7"},
		{"catchall", "/files/a/b/c.txt"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rt, _, _ := benchRouter(1000)
			// The serving path: a pooled capture buffer, borrowed and returned.
			// Lookup itself copies the captures it returns, because handing
			// back a slice of a buffer already returned to the pool - as an
			// earlier version did - lets the next request overwrite them.
			lookup := func() {
				scratch := paramsPool.Get().(*tree.Params)
				_, _, _ = rt.find("GET", tc.target, scratch)
				paramsPool.Put(scratch)
			}
			for i := 0; i < 100; i++ {
				lookup()
			}
			allocs := testing.AllocsPerRun(1000, lookup)
			t.Logf("Lookup %s: %.0f allocs", tc.name, allocs)
			if raceEnabled {
				t.Skip("allocation budget not meaningful under -race; see race_on_test.go")
			}
			if allocs != 0 {
				t.Errorf("Lookup allocated %.0f times, want 0", allocs)
			}
		})
	}
}

// TestRouterMissAllocations bounds the 404 and 405 paths. They are the most
// expensive in the router, because a 405 consults every method's tree to build
// the Allow header, so a regression here matters even though the path is rare.
func TestRouterMissAllocations(t *testing.T) {
	cases := []struct {
		name, method, target string
		ceiling              float64
	}{
		{"notfound", "GET", "/api/v1/nonexistent/deep/path", 2},
		{"methodnotallowed", "PUT", "/api/v1/users/42/posts/7", 4},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rt, _, _ := benchRouter(1000)
			req := httptest.NewRequest(tc.method, tc.target, nil)
			rec := httptest.NewRecorder()
			for i := 0; i < 100; i++ {
				rec.Body.Reset()
				rt.ServeHTTP(rec, req)
			}
			allocs := testing.AllocsPerRun(500, func() {
				rec.Body.Reset()
				rt.ServeHTTP(rec, req)
			})
			t.Logf("%s: %.0f allocs/request", tc.name, allocs)
			if raceEnabled {
				t.Skip("allocation budget not meaningful under -race; see race_on_test.go")
			}
			if allocs > tc.ceiling {
				t.Errorf("%s allocated %.0f times per request, want at most %.0f",
					tc.name, allocs, tc.ceiling)
			}
		})
	}
}
