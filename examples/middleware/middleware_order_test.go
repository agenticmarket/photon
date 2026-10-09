package main

// Proof that the middleware is actually wired to the routes it protects.
//
// The trap this guards against: forgetting per-route or group middleware on a
// route that needs it. Nothing panics, and nothing in a running server tells
// you a security check is missing - the route just answers. (Global Use covers
// every route, wherever it is called; middleware attached to a route or a group
// covers only that route or group.)
//
// The assertion is therefore behavioural: make a request and look at the status.
// That is the only kind that works, because `RouteInfo` is {Method, Path, Params}
// and carries no middleware, so there is no structural check to build. A
// chain-length assertion on Routes() would be theatre.
//
// The app under test is buildApp() - the real registration, not a copy. A test
// that rebuilt the routes would assert its own copy is correct, which says
// nothing about the ordering mistake this is here to catch.

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/agenticmarket/photon"
)

// do issues one request against the real app.
func do(t *testing.T, method, path string, headers map[string]string) *httptest.ResponseRecorder {
	t.Helper()

	req := httptest.NewRequest(method, path, strings.NewReader("{}"))
	if method == http.MethodPost {
		req.Header.Set("Content-Type", "application/json")
	}
	for k, v := range headers {
		req.Header.Set(k, v)
	}

	rec := httptest.NewRecorder()
	buildApp().ServeHTTP(rec, req)
	return rec
}

// TestProtectedRoutesRefuseWithoutAuth is the check that matters.
//
// Every protected route must refuse an unauthenticated request. If one answers
// 200, its middleware is not attached, and nothing else in the system would have
// said so.
func TestProtectedRoutesRefuseWithoutAuth(t *testing.T) {
	tests := []struct {
		method string
		path   string
	}{
		{http.MethodGet, "/private"},
		{http.MethodPost, "/admin"},
	}

	for _, tc := range tests {
		t.Run(tc.path, func(t *testing.T) {
			rec := do(t, tc.method, tc.path, nil)
			if rec.Code == http.StatusOK {
				t.Errorf("%s %s answered 200 with no credentials; its auth middleware "+
					"is not attached. photon does not report this at registration or at "+
					"runtime - the route simply serves.", tc.method, tc.path)
			}
			if rec.Code != http.StatusUnauthorized {
				t.Errorf("%s %s = %d, want 401", tc.method, tc.path, rec.Code)
			}
		})
	}
}

// TestPublicRouteStaysPublic checks the opposite direction.
//
// A test that only proves routes are locked is satisfied by a global auth that
// locks everything, including /public. This is the assertion that keeps the
// per-route choice in main.go honest.
func TestPublicRouteStaysPublic(t *testing.T) {
	rec := do(t, http.MethodGet, "/public", nil)
	if rec.Code != http.StatusOK {
		t.Errorf("/public = %d with no credentials, want 200; auth has leaked into a "+
			"route that is meant to be public", rec.Code)
	}
}

// TestAuthedRequestSucceeds proves the middleware is not simply refusing
// everything.
//
// Without this, a requireBearer that always returns 401 would pass the test
// above. The 401 tests and this one together are what make the pair meaningful.
func TestAuthedRequestSucceeds(t *testing.T) {
	rec := do(t, http.MethodGet, "/private", map[string]string{
		"Authorization": "Bearer " + token,
	})
	if rec.Code != http.StatusOK {
		t.Errorf("/private with a valid token = %d, want 200: the middleware is refusing "+
			"requests it should admit, so the 401 above proves nothing", rec.Code)
	}
}

// TestRoleCheckIsActuallyAttached is the nested case.
//
// /admin carries two middleware: requireBearer and requireRole("admin"). A valid
// token with the wrong role must still be refused, which is only true if the
// second one is attached. Same silent-failure shape as the others, one level
// deeper.
//
// This is the test that the example's original hardcoded `role != "admin"`
// comparison could not pass, because with one token there was no way to present
// a non-admin identity at all.
func TestRoleCheckIsActuallyAttached(t *testing.T) {
	rec := do(t, http.MethodPost, "/admin", map[string]string{
		"Authorization": "Bearer " + token, // valid token, role "user"
	})
	if rec.Code == http.StatusOK {
		t.Error("/admin accepted a user token; requireRole is not attached, and " +
			"nothing would have reported it")
	}
	if rec.Code != http.StatusForbidden {
		t.Errorf("/admin with a valid user token = %d, want 403", rec.Code)
	}
}

// TestAdminTokenIsAdmitted is the other direction, and it is what makes the 403
// above mean something.
//
// A requireRole that refused everybody would pass the test above.
func TestAdminTokenIsAdmitted(t *testing.T) {
	rec := do(t, http.MethodPost, "/admin", map[string]string{
		"Authorization": "Bearer " + adminToken,
	})
	if rec.Code != http.StatusOK {
		t.Errorf("/admin with the admin token = %d, want 200: requireRole is refusing "+
			"requests it should admit, so the 403 above proves nothing", rec.Code)
	}
}

// TestCORSPreflightNeedsNoCredentials is the ordering claim, asserted.
//
// A browser sends a preflight without credentials. If auth ran before CORS, this
// would get a 401 with no Access-Control-* headers, and the browser would report
// a CORS error instead of the real problem. The middleware order in buildApp is
// the thing under test.
func TestCORSPreflightNeedsNoCredentials(t *testing.T) {
	req := httptest.NewRequest(http.MethodOptions, "/private", nil)
	req.Header.Set("Origin", "https://app.example")
	req.Header.Set("Access-Control-Request-Method", "GET")

	rec := httptest.NewRecorder()
	buildApp().ServeHTTP(rec, req)

	if rec.Code == http.StatusUnauthorized {
		t.Fatal("preflight got 401: cors runs after auth in buildApp, so a browser " +
			"would report a CORS failure rather than the real error")
	}
	if rec.Code != http.StatusNoContent {
		t.Errorf("preflight = %d, want 204", rec.Code)
	}
	if got := rec.Header().Get("Access-Control-Allow-Origin"); got == "" {
		t.Error("preflight returned no Access-Control-Allow-Origin; the browser will " +
			"block the real request and say it is a CORS problem")
	}
}

// TestCORSOrderingIsLoadBearing is the reason the test above is worth having.
//
// It builds the same routes with the middleware in the wrong order - auth before
// cors - and asserts the preflight breaks. Without this, the ordering in
// buildApp could be accidentally reversed and every other test would still pass,
// because the other tests do not send an Origin header.
//
// A test that passes in both the correct and the incorrect configuration is not
// testing the thing it names.
func TestCORSOrderingIsLoadBearing(t *testing.T) {
	build := func(corsFirst bool) *photon.Server {
		app := photon.New()
		if corsFirst {
			app.Use(cors)
			app.Use(requireBearer)
		} else {
			app.Use(requireBearer)
			app.Use(cors)
		}
		app.GET("/private", photon.HandlerFunc(private), requireBearer)
		app.OPTIONS("/private", photon.HandlerFunc(noContent))
		return app
	}

	preflight := func(app *photon.Server) *httptest.ResponseRecorder {
		req := httptest.NewRequest(http.MethodOptions, "/private", nil)
		req.Header.Set("Origin", "https://app.example")
		req.Header.Set("Access-Control-Request-Method", "GET")
		rec := httptest.NewRecorder()
		app.ServeHTTP(rec, req)
		return rec
	}

	// Correct order: cors runs first, sets the header, and the preflight passes.
	if got := preflight(build(true)); got.Header().Get("Access-Control-Allow-Origin") == "" {
		t.Error("with cors first, the preflight has no Access-Control-Allow-Origin")
	}

	// Wrong order: auth rejects the credential-less preflight before cors can
	// answer it, and the browser is told the wrong thing.
	bad := preflight(build(false))
	if bad.Code != http.StatusUnauthorized {
		t.Logf("auth-first preflight = %d, want 401", bad.Code)
	}
	if bad.Header().Get("Access-Control-Allow-Origin") != "" {
		t.Error("with auth first, the preflight still carries CORS headers; the " +
			"ordering is not what this test assumes it is")
	}
	t.Logf("cors-first = %d, auth-first = %d: the order is the whole difference",
		preflight(build(true)).Code, bad.Code)
}
