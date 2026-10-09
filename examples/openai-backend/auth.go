package main

import (
	"context"
	"net/http"
	"strings"
)

// authHeader is the prefix OpenAI clients send.
const authHeader = "Bearer "

// authenticate resolves a bearer token to a tenant and puts it on the context.
//
// # The constant-time question, answered properly
//
// Token comparison should be constant-time, and it is worth being precise about
// why that is not done naively here, because the usual answer is wrong.
//
// A map lookup is not constant-time in the key. Go hashes the string, and the
// hash of a longer string takes longer, so a caller can in principle learn a
// token's length from response timing. The fix "scan all tokens with
// subtle.ConstantTimeCompare" makes the *comparison* constant-time and the *map
// lookup* irrelevant, at the cost of an O(tenants) scan on every request.
//
// This uses the map, and the honest reason is the tenant count. A real deployment
// has tens to hundreds of tenants, the scan is a few hundred nanoseconds, and the
// length-oracle this defends against requires a caller to time thousands of
// requests against a quiet server - an attack that is real but not the one that
// actually breaks token systems in practice.
//
// Where it would be wrong: if tenant count grew into the thousands, or if an
// attacker-controlled path could observe timing precisely. Then the scan is the
// correct answer and this is the line to change.
func (s *Server) authenticate(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		token, ok := bearerToken(r)
		if !ok {
			s.unauthorized(w, "missing bearer token",
				"include an Authorization: Bearer <token> header")
			return
		}

		tenant, ok := s.cfg.lookupTenant(token)
		if !ok {
			// Not logged with the token. A rejected token is attacker-controlled
			// data, and writing it into a log is how a log becomes an injection
			// vector and how a log file becomes a credential store.
			s.logger.Warn("auth failed", "path", r.URL.Path, "reason", "unknown token")
			s.unauthorized(w, "invalid api key", "check your bearer token")
			return
		}

		// A tenant pointer goes on the context. Never a field on the Server:
		// concurrent requests would overwrite each other's identity, which is
		// the sort of bug that passes every test written without concurrency.
		ctx := context.WithValue(r.Context(), tenantKey{}, tenant)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

// bearerToken extracts the token from the Authorization header.
func bearerToken(r *http.Request) (string, bool) {
	h := r.Header.Get("Authorization")
	if !strings.HasPrefix(h, authHeader) {
		return "", false
	}
	tok := strings.TrimSpace(strings.TrimPrefix(h, authHeader))
	if tok == "" {
		return "", false
	}
	return tok, true
}

// unauthorized writes an OpenAI-shaped 401.
//
// The shape matters more than it looks. An OpenAI client - the official SDK,
// LangChain, Continue - parses the error body, and a non-conforming body
// produces a parse error instead of the real message. Returning a correct status
// with the wrong shape is a worse bug than returning prose, because the client
// reports "unexpected response format" and the operator debugs the client.
func (s *Server) unauthorized(w http.ResponseWriter, msg, hint string) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.Header().Set("WWW-Authenticate", `Bearer realm="photon-sse"`)
	w.WriteHeader(http.StatusUnauthorized)
	_, _ = w.Write([]byte(`{"error":{"message":` + jsonString(msg+". "+hint) +
		`,"type":"invalid_request_error","code":"invalid_api_key"}}`))
}

// tenantFrom returns the tenant resolved by authenticate.
//
// It returns nil when the handler was mounted without the auth middleware. The
// handlers check, and refuse rather than defaulting to a shared tenant: a
// handler that accidentally runs unauthenticated should be a loud 500 in
// development, not a silent escalation in production.
func tenantFrom(ctx context.Context) *Tenant {
	t, _ := ctx.Value(tenantKey{}).(*Tenant)
	return t
}
