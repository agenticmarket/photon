// Command middleware shows how middleware composes in photon, and that it is
// ordinary net/http middleware.
//
// Run it:
//
//	go run ./examples/middleware
//
// Then:
//
//	curl http://localhost:8080/public
//	curl http://localhost:8080/private
//	curl -H "Authorization: Bearer secret" http://localhost:8080/private
//	curl -X POST http://localhost:8080/admin -H "Content-Type: application/json" -d '{}'
//	curl -i -H "Origin: https://app.example" http://localhost:8080/public
//
// What this example is for:
//
//   - Middleware is a plain http.Handler wrapper. Nothing about requestLog,
//     cors, rateLimit, or requireBearer is photon-specific, so any of them
//     would work unchanged under net/http or any other framework.
//   - Order is registration order, and it matters: CORS must run before auth, or
//     a browser sees a 401 with no Access-Control headers and reports a CORS
//     error instead of the real problem.
//   - Per-route middleware exists, and is the right tool when a global policy
//     would be wrong for one route.
package main

import (
	"context"
	"crypto/subtle"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/agenticmarket/photon"
)

// tokens maps a bearer token to the role it grants.
//
// A map, not a single constant, because the example has to be able to
// demonstrate a *role check that fails*. With one token hardcoded to "admin",
// requireRole("admin") can never refuse anybody, and a test asserting it refuses
// a non-admin passes for the wrong reason: it passes because the check is
// missing entirely, not because it works.
//
// That distinction is the whole point of the role middleware. A role check that
// is never exercised is indistinguishable from no role check, and the failure is
// silent in both cases.
var tokens = map[string]string{
	"secret":       "user",
	"admin-secret": "admin",
}

// token is the low-privilege bearer token, used by the examples in this file's
// doc comment.
//
// It is a constant in a program that is an example, which is the only reason
// this is not a security problem. A real service reads tokens from
// configuration or a secret store, and this comment is here so nobody copies the
// pattern without the caveat.
const token = "secret"

// adminToken grants the admin role, for the /admin route.
const adminToken = "admin-secret"

func main() {
	app := buildApp()
	app.NotFound(photon.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// The path is not echoed back: a response that reflects request
		// content is a route-enumeration oracle at best.
		_ = photon.JSON(w, http.StatusNotFound, map[string]string{
			"error": "no such route",
		})
	}))

	serveErr := make(chan error, 1)
	go func() {
		slog.Info("listening on http://localhost:8080")
		serveErr <- app.Listen(":8080")
	}()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	select {
	case err := <-serveErr:
		if err != nil {
			slog.Error("server stopped", "err", err)
			os.Exit(1)
		}
		return
	case <-ctx.Done():
		slog.Info("shutting down")
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := app.Shutdown(shutdownCtx); err != nil {
		slog.Error("shutdown did not complete cleanly", "err", err)
	}
	if err := <-serveErr; err != nil {
		slog.Error("server stopped", "err", err)
		os.Exit(1)
	}
	slog.Info("stopped")
}

// buildApp registers the middleware and the routes, and returns the server.
//
// Extracted from main so middleware_order_test.go can drive the *real* app
// rather than a reconstruction of it. A test that rebuilt the routes would
// assert that its own copy is wired correctly, which is worth nothing - the
// mistake this guards against is in the registration order, and a copy cannot
// have the original's order.
func buildApp() *photon.Server {
	app := photon.New(photon.Logger(slog.New(slog.NewTextHandler(os.Stderr, nil))))

	// Global middleware, in the order it runs. The order is the order written,
	// and it is not arbitrary - see the note on cors below.
	app.Use(requestLog)
	app.Use(cors)
	app.Use(rateLimit(20, time.Minute)) // 20 requests a minute per client

	// Authentication is not global. Global auth plus per-route exemptions
	// cannot be expressed in net/http - middleware that has run cannot be
	// un-run - and the workarounds (a context flag, a second router) hide which
	// routes are public. Instead the policy is visible where routes are
	// declared: a public route is public because it is not in the group.
	app.GET("/public", public)

	authed := app.Group("", requireBearer)
	authed.GET("/private", private)
	authed.POST("/admin", admin, requireRole("admin"))

	// No OPTIONS routes. Global middleware runs for every request, including
	// a preflight to a path that only registered GET or POST, so cors answers
	// preflights itself. (An earlier photon matched the route first and
	// answered such a preflight with a 405 before cors ever ran, which is why
	// this example used to register OPTIONS routes by hand.)

	return app
}

// noContent answers a preflight that cors has already approved.
//
// cors is global and runs first, so by the time this is reached the origin is
// allowed and the Access-Control-* headers are set. The 204 is what the browser
// wants: a body here is discarded, and a 200 with a body invites the question of
// what it meant.
func noContent(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusNoContent)
}

// requestLog is ordinary net/http middleware. Nothing photon-specific, which is
// the point: the http.Handler signature is not narrowed.
func requestLog(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		next.ServeHTTP(w, r)
		slog.Info("request",
			"method", r.Method,
			"path", r.URL.Path,
			"peer", photon.ClientIP(r).String(),
			"took", time.Since(start))
	})
}

// cors writes cross-origin headers and answers preflights.
//
// It is registered *before* auth on purpose. A browser sends a preflight without
// credentials, so if auth ran first the preflight would get a 401 with no
// Access-Control headers, and the browser would report "CORS error" rather than
// the actual problem. Middleware order is a correctness concern, not a style one.
func cors(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		origin := r.Header.Get("Origin")
		if origin == "" {
			// Not a browser request. Nothing to negotiate.
			next.ServeHTTP(w, r)
			return
		}

		// An allowlist, not a reflection of whatever arrived. Echoing an
		// arbitrary Origin back with credentials allowed is the same as having no
		// CORS policy at all.
		allowed := map[string]bool{
			"https://app.example":   true,
			"https://admin.example": true,
		}
		if !allowed[origin] {
			// 403 rather than silently omitting the header: a browser needs to be
			// told, and a bare missing header produces a confusing console error.
			// The rejected origin is not echoed back: a response body never
			// reflects request content.
			w.Header().Add("Vary", "Origin")
			_ = photon.JSON(w, http.StatusForbidden, map[string]string{
				"error": "origin not allowed",
			})
			return
		}

		h := w.Header()
		h.Set("Access-Control-Allow-Origin", origin)
		h.Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
		h.Set("Access-Control-Allow-Headers", "Authorization, Content-Type")
		h.Set("Access-Control-Max-Age", "600")
		// Only sent when the response actually varies by Origin. Saying it
		// unconditionally makes caches treat every response as unique.
		h.Add("Vary", "Origin")

		if r.Method == http.MethodOptions {
			// The preflight is answered here. Global middleware runs for every
			// request - including OPTIONS to a route that only registered GET or
			// POST - so no OPTIONS routes are needed.
			w.WriteHeader(http.StatusNoContent)
			return
		}
		next.ServeHTTP(w, r)
	})
}

// rateLimit bounds requests per client address.
//
// photon already provides MaxPerIP for concurrent connections, and MaxConnections
// for total sockets. This is the other axis: a request *rate*. A client that
// opens one connection and sends 10,000 sequential requests never trips a
// connection limit, so a rate limit is a separate concern rather than a stricter
// version of the same one.
func rateLimit(limit int, window time.Duration) func(http.Handler) http.Handler {
	type bucket struct {
		count int
		reset time.Time
	}
	var (
		mu      sync.Mutex
		buckets = map[string]*bucket{}
	)

	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			// ClientIP is the immediate peer and deliberately ignores
			// X-Forwarded-For. That header is set by the client, so keying a rate
			// limit on it would let anyone bypass the limit by inventing an
			// address - the limit would be theatre.
			//
			// Behind a real proxy this needs the trusted-proxy logic described in
			// examples/security.
			key := photon.ClientIP(r).String()

			mu.Lock()
			now := time.Now()
			b := buckets[key]
			if b == nil {
				b = &bucket{reset: now.Add(window)}
				buckets[key] = b
			}
			if now.After(b.reset) {
				b.count, b.reset = 0, now.Add(window)
			}
			b.count++
			allowed := b.count <= limit
			mu.Unlock()

			if !allowed {
				w.Header().Set("Retry-After", "60")
				_ = photon.JSON(w, http.StatusTooManyRequests, map[string]string{
					"error": "rate limit exceeded",
					"limit": "20 per minute",
				})
				return
			}
			next.ServeHTTP(w, r)
		})
	}
}

// requireBearer checks a bearer token.
//
// A real implementation compares against a hash with crypto/subtle, not with ==
// - a byte-by-byte comparison leaks the token's prefix through timing. This one
// uses subtle so the pattern is right, and the constant-time comment is more
// valuable than the brevity of ==.
func requireBearer(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		const prefix = "Bearer "
		h := r.Header.Get("Authorization")
		if !strings.HasPrefix(h, prefix) {
			unauthorized(w, "missing bearer token")
			return
		}
		if !constantTimeEqual(strings.TrimPrefix(h, prefix), token) &&
			!constantTimeEqual(strings.TrimPrefix(h, prefix), adminToken) {
			// Deliberately the same message and status as a missing token. Telling
			// a caller "the token was wrong" versus "there was no token" confirms
			// which tokens exist.
			unauthorized(w, "invalid bearer token")
			return
		}
		// The role goes in the context alongside the authentication flag, so
		// requireRole reads a fact rather than re-parsing the header. A real
		// service would read a claim from a verified token here; this one looks
		// the role up, which is a stand-in with the same shape.
		presented := strings.TrimPrefix(h, prefix)
		role, ok := tokens[presented]
		if !ok {
			// Unreachable while the two comparisons above hold, because both
			// names are keys in tokens. Written anyway: a map lookup that silently
			// yields "" would make requireRole("") pass for an unauthenticated
			// request, which is the kind of bug that only appears in production.
			role = "user"
		}
		ctx := context.WithValue(r.Context(), authKey{}, true)
		ctx = context.WithValue(ctx, roleKey{}, role)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

// requireRole is a second check on top of authentication, used per-route.
func requireRole(role string) photon.Middleware {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			// If auth did not run, this fails closed rather than panicking. A
			// per-route middleware that assumed a global one had run would be one
			// reordering away from a security hole.
			if !isAuthenticated(r) {
				unauthorized(w, "authentication required")
				return
			}
			// Compared against the role the token actually granted, not against a
			// literal "admin". A hardcoded comparison here is a check that cannot
			// fail: with one token, always granted admin, requireRole("admin") is
			// unreachable code that reads as a security control and is not one.
			if roleOf(r) != role {
				_ = photon.JSON(w, http.StatusForbidden, map[string]string{
					"error": "insufficient role",
					"need":  role,
				})
				return
			}
			next.ServeHTTP(w, r)
		})
	}
}

type authKey struct{}

func isAuthenticated(r *http.Request) bool {
	v, _ := r.Context().Value(authKey{}).(bool)
	return v
}

// roleKey is the context key for the role the presented token grants.
type roleKey struct{}

// roleOf returns the role requireBearer stored, or "" if there is none.
//
// The empty string is the fail-closed answer: a request that somehow reached a
// role check without passing requireBearer has no role, and "" matches no
// configured role, so it is refused. Returning "admin" on a miss would turn a
// missing authentication into a granted one.
func roleOf(r *http.Request) string {
	v, _ := r.Context().Value(roleKey{}).(string)
	return v
}

func unauthorized(w http.ResponseWriter, msg string) {
	w.Header().Set("WWW-Authenticate", `Bearer realm="photon"`)
	_ = photon.JSON(w, http.StatusUnauthorized, map[string]string{"error": msg})
}

// constantTimeEqual compares two strings without leaking their contents through
// timing.
//
// A byte-by-byte comparison returns as soon as it finds a difference, so the time
// it takes reveals how many leading bytes matched. On a network that is enough to
// recover a token one byte at a time. crypto/subtle exists for exactly this and
// is not optional when the value is a credential.
func constantTimeEqual(got, want string) bool {
	// subtle.ConstantTimeCompare needs equal lengths to be meaningful, and
	// comparing the lengths first would reintroduce the leak - so a length
	// mismatch is padded rather than short-circuited. Token length is not secret
	// in any real deployment, but the habit of not branching on it is the point.
	return subtle.ConstantTimeCompare([]byte(got), []byte(want)) == 1
}

func public(w http.ResponseWriter, r *http.Request) {
	_ = photon.JSON(w, http.StatusOK, map[string]any{
		"route":         "public",
		"authenticated": isAuthenticated(r),
	})
}

func private(w http.ResponseWriter, r *http.Request) {
	_ = photon.JSON(w, http.StatusOK, map[string]any{
		"route":  "private",
		"secret": "you are authenticated",
	})
}

func admin(w http.ResponseWriter, r *http.Request) {
	_ = photon.JSON(w, http.StatusOK, map[string]any{
		"route":   "admin",
		"action":  "accepted",
		"payload": "would be decoded here",
	})
}
