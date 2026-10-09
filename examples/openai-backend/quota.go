package main

import (
	"net/http"
	"strconv"
	"time"
)

// enforceQuota applies the per-tenant request-rate and stream-concurrency limits.
//
// # Why this is middleware and not handler code
//
// The check must happen *before* the response status is committed, and that is
// the whole reason it lives here. Once a streaming handler writes its first
// token the status line is gone, so a quota check after that point cannot
// produce a 429 - it can only kill the connection. A caller over quota would get
// a 200 followed by a truncated stream, which is a worse failure than a clean
// 429: the client has already started rendering an answer that will never
// finish, and a client cannot tell that from a network blip.
//
// Ordering: rate limit first, then the concurrency slot. The rate limit is
// cheaper and rejects the common case, so the more expensive slot is only
// contested by a caller who is within their rate.
func (s *Server) enforceQuota(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		tenant := tenantFrom(r.Context())
		if tenant == nil {
			// Unreachable through the router, where authenticate always runs first.
			// Failing closed rather than defaulting to a shared tenant: a routing
			// mistake should be a loud 500 in development, not a silent
			// authentication bypass in production.
			writeError(w, http.StatusInternalServerError,
				"server_error", "no_tenant", "internal error")
			return
		}

		now := time.Now()
		nowNS := now.UnixNano()

		// Rate limit, reported in the headers either way so a well-behaved client
		// can back off before it gets a 429.
		if !tenant.rpm.allow(nowNS, int64(s.cfg.PerTenantRPM)) {
			s.metrics.quotaRejected.Add(1)
			s.logger.Warn("rate limited",
				"tenant", tenant.Name, "limit", s.cfg.PerTenantRPM, "path", r.URL.Path)
			w.Header().Set("Retry-After", "60")
			s.setRateHeaders(w, tenant, nowNS, true)
			writeError(w, http.StatusTooManyRequests,
				"rate_limit_error", "rate_limit_exceeded",
				"rate limit exceeded, retry shortly")
			return
		}
		s.setRateHeaders(w, tenant, nowNS, false)

		// Concurrency slot. Taken here, released by the handler's defer, so a
		// panic or a disconnect cannot leak it.
		if !tenant.AcquireStream(int64(s.cfg.PerTenantStreams)) {
			s.metrics.quotaRejected.Add(1)
			s.logger.Warn("stream limit reached",
				"tenant", tenant.Name, "limit", s.cfg.PerTenantStreams)
			w.Header().Set("Retry-After", "5")
			writeError(w, http.StatusTooManyRequests,
				"rate_limit_error", "concurrent_stream_limit",
				"too many concurrent streams for this api key, retry shortly")
			return
		}

		s.metrics.streamsAdmitted.Add(1)
		// The one place a slot is handed out, and therefore the one place it must
		// be handed back. The handler's defer pairs with this; the tests assert
		// the pairing holds across a panic and across a client disconnect.
		defer tenant.ReleaseStream()

		next.ServeHTTP(w, r)
	})
}

// setRateHeaders reports the rate-limit state.
func (s *Server) setRateHeaders(w http.ResponseWriter, tenant *Tenant, nowNS int64, exceeded bool) {
	limit := int64(s.cfg.PerTenantRPM)
	w.Header().Set("X-RateLimit-Limit-Requests", strconv.FormatInt(limit, 10))

	if exceeded {
		w.Header().Set("X-RateLimit-Remaining-Requests", "0")
		w.Header().Set("X-RateLimit-Reset-Requests", "1m0s")
		return
	}
	w.Header().Set("X-RateLimit-Remaining-Requests",
		strconv.FormatInt(tenant.rpm.remaining(nowNS, limit), 10))
	w.Header().Set("X-RateLimit-Reset-Requests", "1m0s")
}
