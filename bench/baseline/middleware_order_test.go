package main

// Middleware-ordering behaviour across frameworks.
//
// A common claim is that "Gin ignores late middleware; chi panics", and that
// users will hit the difference.
//
// That claim was written from memory, and "written from memory" is exactly the
// kind of thing that is right in general and wrong for the version in front of
// you. So it is measured here against the pinned versions, and whatever the
// result is - including "the claim is now wrong" - is what gets written down.
//
// The question: **can middleware registered after a route still wrap that
// route?** Three answers are possible and they are not equivalent for a user:
//
//	applied - late middleware wraps the route (photon since 2026-10)
//	ignored - the route runs without it, silently (gin, by design)
//	panics  - the framework refuses the state outright (chi)
//
// "ignored" is the dangerous one, because nothing tells you. A security check
// added in the wrong place does not fail loudly; it simply does not run.
//
// This is a test rather than a main() so the answer is asserted on every run of
// the suite. A guide that quotes behaviour nobody re-checks is a guide that will
// be wrong by the next major version without anyone noticing.

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/go-chi/chi/v5"

	photon "github.com/agenticmarket/photon"
)

// orderBehaviour is the classification of a framework's response to late
// middleware.
type orderBehaviour string

const (
	orderApplied orderBehaviour = "applied"
	orderIgnored orderBehaviour = "ignored silently"
	orderPanics  orderBehaviour = "panics"
)

// TestLateMiddlewareBehaviour pins each framework's answer.
//
// The expected values are assertions, not documentation. If a gin or chi upgrade
// changes this, the test fails and the guide gets updated in the same commit -
// which is the mechanism that keeps the guide honest.
//
// Photon's answer changed in 2026-10. This test first measured photon as
// "ignored silently", exactly like gin - a security check registered below its
// routes quietly did not run - and the guides said so. That was fixed rather
// than documented: Use now applies to every route whenever it is called, and to
// 404 and 405 responses too. The expectation was changed from ignored to
// applied in the same commit as the fix.
func TestLateMiddlewareBehaviour(t *testing.T) {
	tests := []struct {
		framework string
		want      orderBehaviour
		run       func(t *testing.T) orderBehaviour
	}{
		{"photon", orderApplied, photonLateMiddleware},
		{"chi", orderPanics, chiLateMiddleware},
		{"gin", orderIgnored, ginLateMiddleware},
	}

	for _, tc := range tests {
		t.Run(tc.framework, func(t *testing.T) {
			got := tc.run(t)
			if got != tc.want {
				t.Errorf("late middleware on %s: %s, want %s.\n"+
					"  The guides state %s. If this changed in an upgrade, update "+
					"docs/guides/middleware.md and examples/middleware/README.md in the same commit - "+
					"that documentation is the only thing a user has to go on.",
					tc.framework, got, tc.want, tc.want)
			}
			t.Logf("%s: late middleware is %s", tc.framework, got)
		})
	}
}

// TestNoFrameworkHereIgnoresLateMiddlewareExceptGin is the comparison, stated
// as its own assertion: photon applies it, chi refuses it loudly, and gin is
// the one that silently drops it. If photon ever regresses to gin's behaviour,
// this fails even if someone "fixes" the expectation above.
func TestNoFrameworkHereIgnoresLateMiddlewareExceptGin(t *testing.T) {
	photonBehaviour := photonLateMiddleware(t)
	ginBehaviour := ginLateMiddleware(t)
	chiBehaviour := chiLateMiddleware(t)

	if photonBehaviour == orderIgnored {
		t.Errorf("photon silently ignores middleware registered after a route; " +
			"a security check added below its routes would not run")
	}
	if chiBehaviour == orderIgnored {
		t.Errorf("chi now silently ignores late middleware; the guides' comparison is stale")
	}
	t.Logf("photon=%s  gin=%s  chi=%s", photonBehaviour, ginBehaviour, chiBehaviour)
}

// photonLateMiddleware registers a route, then middleware, then calls it.
func photonLateMiddleware(t *testing.T) orderBehaviour {
	t.Helper()

	reached := false
	app := photon.New()
	app.GET("/x", photon.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	// Registered after the route: does it still wrap?
	app.Use(func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			reached = true
			next.ServeHTTP(w, r)
		})
	})

	rec := httptest.NewRecorder()
	app.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/x", nil))

	if reached {
		return orderApplied
	}
	return orderIgnored
}

// chiLateMiddleware expects chi to refuse the ordering outright.
func chiLateMiddleware(t *testing.T) orderBehaviour {
	t.Helper()

	reached := false
	r := chi.NewRouter()
	r.Get("/x", func(w http.ResponseWriter, req *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	var panicked bool
	func() {
		defer func() {
			if p := recover(); p != nil {
				panicked = true
				t.Logf("chi panicked: %v", p)
			}
		}()
		r.Use(func(next http.Handler) http.Handler {
			return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
				reached = true
				next.ServeHTTP(w, req)
			})
		})
	}()

	if panicked {
		return orderPanics
	}

	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/x", nil))
	if reached {
		return orderApplied
	}
	return orderIgnored
}

// ginLateMiddleware expects gin to accept the call and silently not run it.
func ginLateMiddleware(t *testing.T) orderBehaviour {
	t.Helper()

	gin.SetMode(gin.TestMode)

	reached := false
	g := gin.New()
	g.GET("/x", func(c *gin.Context) { c.Status(http.StatusOK) })

	var panicked bool
	func() {
		defer func() {
			if p := recover(); p != nil {
				panicked = true
				t.Logf("gin panicked: %v", p)
			}
		}()
		// The interesting call. gin's own docs say Use must come before routes;
		// what matters here is whether it warns, panics, or silently no-ops.
		g.Use(func(c *gin.Context) {
			reached = true
			c.Next()
		})
	}()

	if panicked {
		return orderPanics
	}

	g.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/x", nil))
	if reached {
		return orderApplied
	}
	return orderIgnored
}
