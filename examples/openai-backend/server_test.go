package main

import (
	"bufio"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// testToken is the bearer token every test uses. Long enough to satisfy the
// parser's 8-character floor, because a test that configures a token the
// production code rejects is testing the wrong thing.
const testToken = "sk-test-token-0000000000"

// newTestServer builds a server with a deterministic provider and no upstream.
func newTestServer(t *testing.T, tune func(*Config)) (*Server, *fakeProvider) {
	t.Helper()

	tenant := &Tenant{Name: "acme"}
	cfg := DefaultConfig()
	cfg.Tenants = map[string]*Tenant{testToken: tenant}
	cfg.ModelName = "test-model"
	// A zero interval, so a stream completes in microseconds rather than seconds.
	// A streaming test that takes 15 seconds is a test nobody runs.
	cfg.StallTimeout = 500 * time.Millisecond
	if tune != nil {
		tune(&cfg)
	}

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	s, err := New(cfg, logger)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return s, s.provider.(*fakeProvider)
}

// chatBody is a minimal valid request.
func chatBody(stream bool) string {
	return `{"model":"test-model","messages":[{"role":"user","content":"hello there"}],"stream":` +
		map[bool]string{true: "true", false: "false"}[stream] + `}`
}

func post(t *testing.T, h http.Handler, path, body, token string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, path, strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

// TestAuthRejectsMissingAndWrongTokens checks the three auth paths.
func TestAuthRejectsMissingAndWrongTokens(t *testing.T) {
	s, _ := newTestServer(t, nil)
	h := s.Handler()

	tests := []struct {
		name  string
		token string
		want  int
	}{
		{"no token", "", http.StatusUnauthorized},
		{"wrong token", "sk-not-a-real-token", http.StatusUnauthorized},
		{"correct token", testToken, http.StatusOK},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			rec := post(t, h, "/v1/chat/completions", chatBody(false), tc.token)
			if rec.Code != tc.want {
				t.Errorf("status = %d, want %d; body %s", rec.Code, tc.want, rec.Body.String())
			}
			if tc.want == http.StatusUnauthorized {
				// The body must be OpenAI-shaped or a real client reports a
				// parse error instead of the actual problem.
				var er errorResponse
				if err := json.Unmarshal(rec.Body.Bytes(), &er); err != nil {
					t.Fatalf("error body is not JSON: %v (%s)", err, rec.Body.String())
				}
				if er.Error.Type != "invalid_request_error" {
					t.Errorf("error type = %q, want invalid_request_error", er.Error.Type)
				}
				if rec.Header().Get("WWW-Authenticate") == "" {
					t.Error("no WWW-Authenticate header on a 401")
				}
			}
		})
	}
}

// TestValidationRejectsBadRequests covers the shape checks a real client hits.
func TestValidationRejectsBadRequests(t *testing.T) {
	s, _ := newTestServer(t, nil)
	h := s.Handler()

	tests := []struct {
		name string
		body string
		want int
	}{
		{"no model", `{"messages":[{"role":"user","content":"hi"}]}`, http.StatusBadRequest},
		{"unknown model", `{"model":"gpt-9","messages":[{"role":"user","content":"hi"}]}`, http.StatusNotFound},
		{"no messages", `{"model":"test-model","messages":[]}`, http.StatusBadRequest},
		{"last message not user", `{"model":"test-model","messages":[{"role":"assistant","content":"hi"}]}`, http.StatusBadRequest},
		{"empty content", `{"model":"test-model","messages":[{"role":"user","content":"  "}]}`, http.StatusBadRequest},
		{"negative max_tokens", `{"model":"test-model","messages":[{"role":"user","content":"hi"}],"max_tokens":-1}`, http.StatusBadRequest},
		{"temperature too high", `{"model":"test-model","messages":[{"role":"user","content":"hi"}],"temperature":9}`, http.StatusBadRequest},
		{"unknown field", `{"model":"test-model","messages":[{"role":"user","content":"hi"}],"max_token":5}`, http.StatusBadRequest},
		{"malformed json", `{oops`, http.StatusBadRequest},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			rec := post(t, h, "/v1/chat/completions", tc.body, testToken)
			if rec.Code != tc.want {
				t.Errorf("status = %d, want %d; body %s", rec.Code, tc.want, rec.Body.String())
			}
		})
	}
}

// TestNonStreamingResponse checks the buffered path returns a complete body.
func TestNonStreamingResponse(t *testing.T) {
	s, _ := newTestServer(t, nil)
	rec := post(t, s.Handler(), "/v1/chat/completions", chatBody(false), testToken)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d: %s", rec.Code, rec.Body.String())
	}

	var resp chatResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("response is not valid JSON: %v", err)
	}
	if resp.Object != "chat.completion" {
		t.Errorf("object = %q, want chat.completion", resp.Object)
	}
	if len(resp.Choices) != 1 {
		t.Fatalf("got %d choices, want 1", len(resp.Choices))
	}
	if resp.Choices[0].Message.Role != "assistant" {
		t.Errorf("role = %q, want assistant", resp.Choices[0].Message.Role)
	}
	if resp.Choices[0].FinishReason == nil || *resp.Choices[0].FinishReason != "stop" {
		t.Errorf("finish_reason = %v, want stop", resp.Choices[0].FinishReason)
	}
	if resp.Usage.TotalTokens == 0 {
		t.Error("usage.total_tokens is 0; a client showing usage would render '0 tokens'")
	}
}

// readSSE reads a chunked response and returns the decoded data payloads.
//
// Hand-written rather than using a client, because the point of these tests is
// to observe the wire format exactly. An HTTP client would buffer the whole
// response, which is precisely the behaviour these tests need to check against.
func readSSE(t *testing.T, rec *httptest.ResponseRecorder) []string {
	t.Helper()
	if ct := rec.Header().Get("Content-Type"); !strings.HasPrefix(ct, "text/event-stream") {
		t.Fatalf("Content-Type = %q, want text/event-stream", ct)
	}

	var payloads []string
	sc := bufio.NewScanner(strings.NewReader(rec.Body.String()))
	for sc.Scan() {
		line := sc.Text()
		if !strings.HasPrefix(line, "data: ") {
			continue
		}
		payloads = append(payloads, strings.TrimPrefix(line, "data: "))
	}
	return payloads
}

// TestStreamingResponseShape checks the three-part chunk sequence OpenAI clients
// depend on: a role chunk, delta chunks, and a finish_reason chunk.
func TestStreamingResponseShape(t *testing.T) {
	s, _ := newTestServer(t, nil)
	rec := post(t, s.Handler(), "/v1/chat/completions", chatBody(true), testToken)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d: %s", rec.Code, rec.Body.String())
	}

	payloads := readSSE(t, rec)
	if len(payloads) < 3 {
		t.Fatalf("got %d data frames, want at least 3 (role, deltas, finish)", len(payloads))
	}

	var first streamChunk
	if err := json.Unmarshal([]byte(payloads[0]), &first); err != nil {
		t.Fatalf("first frame is not JSON: %v", err)
	}
	if first.Object != "chat.completion.chunk" {
		t.Errorf("object = %q, want chat.completion.chunk", first.Object)
	}
	if first.Choices[0].Delta.Role != "assistant" {
		t.Errorf("first chunk role = %q, want assistant; clients key off this",
			first.Choices[0].Delta.Role)
	}
	if !strings.HasPrefix(first.ID, "chatcmpl-") {
		t.Errorf("id = %q, want a chatcmpl- prefix", first.ID)
	}

	// At least one frame must carry content.
	sawContent := false
	for _, p := range payloads[1:] {
		var c streamChunk
		if err := json.Unmarshal([]byte(p), &c); err != nil {
			t.Fatalf("frame is not JSON: %v", err)
		}
		if c.Choices[0].Delta.Content != "" {
			sawContent = true
		}
	}
	if !sawContent {
		t.Error("no frame carried any content")
	}

	// The last frame must carry finish_reason, or a client cannot tell a complete
	// answer from a truncated one.
	var last streamChunk
	if err := json.Unmarshal([]byte(payloads[len(payloads)-1]), &last); err != nil {
		t.Fatalf("last frame is not JSON: %v", err)
	}
	if last.Choices[0].FinishReason == nil {
		t.Error("last frame has no finish_reason; the client cannot tell the answer ended")
	}
}

// TestUpstreamFailureProducesErrorFrame is the test that matters most for
// correctness.
//
// A provider that dies mid-stream is exactly when a proxy silently truncates a
// response. The client then renders half an answer as though it were complete,
// and nothing reports an error.
func TestUpstreamFailureProducesErrorFrame(t *testing.T) {
	s, provider := newTestServer(t, nil)
	provider.failAfter = 3

	rec := post(t, s.Handler(), "/v1/chat/completions", chatBody(true), testToken)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200: the stream had already started", rec.Code)
	}

	payloads := readSSE(t, rec)

	// An error object must appear in the stream.
	sawError := false
	for _, p := range payloads {
		var er errorResponse
		if json.Unmarshal([]byte(p), &er) == nil && er.Error.Message != "" {
			sawError = true
			break
		}
	}
	if !sawError {
		t.Errorf("no error frame in the stream; the client would render a truncated "+
			"answer as complete. frames:\n%s", strings.Join(payloads, "\n"))
	}

	// And the terminal chunk must not claim a normal stop.
	var last streamChunk
	_ = json.Unmarshal([]byte(payloads[len(payloads)-1]), &last)
	if last.Choices[0].FinishReason != nil && *last.Choices[0].FinishReason == "stop" {
		t.Error("terminal chunk says finish_reason=stop after an upstream failure; " +
			"that tells the client the answer completed normally")
	}

	if s.metrics.upstreamErrors.Load() == 0 {
		t.Error("upstreamErrors was not incremented")
	}
}

// TestQuotaReleasesSlotOnClientDisconnect is the leak test.
//
// If the concurrency slot were not released on a disconnect, a client that opens
// and abandons streams would permanently consume that tenant's quota, and the
// tenant would be locked out with no error and no way to recover.
func TestQuotaReleasesSlotOnClientDisconnect(t *testing.T) {
	tenant := &Tenant{Name: "acme"}
	cfg := DefaultConfig()
	cfg.Tenants = map[string]*Tenant{testToken: tenant}
	cfg.ModelName = "test-model"
	cfg.PerTenantStreams = 1 // one at a time, so a leak is immediately visible
	cfg.PerTenantRPM = 0     // unlimited, isolating the concurrency limit
	cfg.StallTimeout = 200 * time.Millisecond

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	s, err := New(cfg, logger)
	if err != nil {
		t.Fatal(err)
	}

	if n := tenant.InFlight(); n != 0 {
		t.Fatalf("InFlight = %d before any request, want 0", n)
	}

	// One successful request, so the slot is taken and released.
	rec := post(t, s.Handler(), "/v1/chat/completions", chatBody(false), testToken)
	if rec.Code != http.StatusOK {
		t.Fatalf("first request = %d, want 200", rec.Code)
	}
	if n := tenant.InFlight(); n != 0 {
		t.Fatalf("InFlight = %d after a completed request, want 0: the slot leaked", n)
	}

	// Now a request whose context is cancelled mid-flight, which is the case that
	// historically leaked.
	ctx, cancel := context.WithCancel(context.Background())
	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions",
		strings.NewReader(chatBody(true))).WithContext(ctx)
	req.Header.Set("Authorization", "Bearer "+testToken)
	req.Header.Set("Content-Type", "application/json")

	done := make(chan struct{})
	go func() {
		defer close(done)
		rec := httptest.NewRecorder()
		s.Handler().ServeHTTP(rec, req)
	}()

	// Let the stream start, then cancel.
	time.Sleep(20 * time.Millisecond)
	cancel()
	<-done

	// The slot must come back. This is the assertion that matters.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if tenant.InFlight() == 0 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if n := tenant.InFlight(); n != 0 {
		t.Fatalf("InFlight = %d after a cancelled stream, want 0: the quota slot leaked "+
			"and this tenant can never stream again", n)
	}
}

// TestRateLimitRejectsOverQuota checks the RPM limit and its headers.
func TestRateLimitRejectsOverQuota(t *testing.T) {
	s, _ := newTestServer(t, func(c *Config) {
		c.PerTenantRPM = 3
		c.PerTenantStreams = 0
	})
	h := s.Handler()

	for i := 0; i < 3; i++ {
		rec := post(t, h, "/v1/chat/completions", chatBody(false), testToken)
		if rec.Code != http.StatusOK {
			t.Fatalf("request %d = %d, want 200", i+1, rec.Code)
		}
	}

	rec := post(t, h, "/v1/chat/completions", chatBody(false), testToken)
	if rec.Code != http.StatusTooManyRequests {
		t.Fatalf("request 4 = %d, want 429", rec.Code)
	}
	if rec.Header().Get("Retry-After") == "" {
		t.Error("no Retry-After header on a 429")
	}
	if got := rec.Header().Get("X-RateLimit-Remaining-Requests"); got != "0" {
		t.Errorf("X-RateLimit-Remaining-Requests = %q, want 0", got)
	}

	var er errorResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &er); err != nil {
		t.Fatalf("429 body is not JSON: %v", err)
	}
	if er.Error.Type != "rate_limit_error" {
		t.Errorf("error type = %q, want rate_limit_error", er.Error.Type)
	}
}

// TestStreamLimitRejectsConcurrency checks the per-tenant stream limit.
func TestStreamLimitRejectsConcurrency(t *testing.T) {
	s, provider := newTestServer(t, func(c *Config) {
		c.PerTenantStreams = 1
		c.PerTenantRPM = 0
	})
	// A slow provider, so the first stream is still open when the second arrives.
	provider.delay = 400 * time.Millisecond

	first := make(chan int, 1)
	go func() {
		rec := post(t, s.Handler(), "/v1/chat/completions", chatBody(true), testToken)
		first <- rec.Code
	}()

	time.Sleep(80 * time.Millisecond)
	rec := post(t, s.Handler(), "/v1/chat/completions", chatBody(false), testToken)
	if rec.Code != http.StatusTooManyRequests {
		t.Errorf("concurrent request = %d, want 429", rec.Code)
	}

	if code := <-first; code != http.StatusOK {
		t.Errorf("first request = %d, want 200", code)
	}
}

// TestHealthAndReadiness checks the operational endpoints.
func TestHealthAndReadiness(t *testing.T) {
	s, _ := newTestServer(t, nil)
	h := s.Handler()

	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/healthz", nil))
	if rec.Code != http.StatusOK {
		t.Errorf("/healthz = %d, want 200", rec.Code)
	}

	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	if rec.Code != http.StatusOK {
		t.Errorf("/readyz = %d, want 200", rec.Code)
	}

	// Draining: readiness must fail, liveness must not.
	s.draining.Store(true)

	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	if rec.Code != http.StatusServiceUnavailable {
		t.Errorf("/readyz while draining = %d, want 503", rec.Code)
	}

	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/healthz", nil))
	if rec.Code != http.StatusOK {
		t.Errorf("/healthz while draining = %d, want 200: a draining server is still alive", rec.Code)
	}
}

// TestMetricsExposition checks the Prometheus output is well-formed.
func TestMetricsExposition(t *testing.T) {
	s, _ := newTestServer(t, nil)
	post(t, s.Handler(), "/v1/chat/completions", chatBody(true), testToken)
	post(t, s.Handler(), "/v1/chat/completions", chatBody(false), "wrong")

	rec := httptest.NewRecorder()
	s.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("/metrics = %d", rec.Code)
	}
	if ct := rec.Header().Get("Content-Type"); !strings.HasPrefix(ct, "text/plain") {
		t.Errorf("Content-Type = %q, want text/plain", ct)
	}

	body := rec.Body.String()
	for _, want := range []string{
		"# HELP ssebackend_requests_total",
		"# TYPE ssebackend_streams_admitted_total counter",
		"ssebackend_tokens_streamed_total",
		"ssebackend_stream_duration_seconds_bucket{le=\"+Inf\"}",
		"ssebackend_quota_rejected_total",
	} {
		if !strings.Contains(body, want) {
			t.Errorf("metrics output is missing %q", want)
		}
	}

	// A histogram without +Inf is invalid to Prometheus, so the check above is
	// not pedantry: scrapers reject the whole target.
	if strings.Contains(body, `le="+Inf"} 0`) && !strings.Contains(body, `le="+Inf"} 1`) {
		// 0 is legitimate when nothing has streamed; nothing to assert here
		// beyond the bucket existing, which the loop above already did.
		_ = body
	}
}

// TestModelsEndpoint checks the OpenAI model list shape.
func TestModelsEndpoint(t *testing.T) {
	s, _ := newTestServer(t, nil)
	req := httptest.NewRequest(http.MethodGet, "/v1/models", nil)
	req.Header.Set("Authorization", "Bearer "+testToken)
	rec := httptest.NewRecorder()
	s.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("/v1/models = %d: %s", rec.Code, rec.Body.String())
	}
	var list modelList
	if err := json.Unmarshal(rec.Body.Bytes(), &list); err != nil {
		t.Fatalf("not JSON: %v", err)
	}
	if list.Object != "list" {
		t.Errorf("object = %q, want list", list.Object)
	}
	if len(list.Data) == 0 || list.Data[0].ID != "test-model" {
		t.Errorf("data = %+v, want one model named test-model", list.Data)
	}
}

// TestRequestIDEchoedAndSanitised checks the request id round trip.
func TestRequestIDEchoedAndSanitised(t *testing.T) {
	s, _ := newTestServer(t, nil)

	// An inbound id is echoed.
	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	req.Header.Set("X-Request-ID", "abc-123")
	rec := httptest.NewRecorder()
	s.Handler().ServeHTTP(rec, req)
	if got := rec.Header().Get("X-Request-ID"); got != "abc-123" {
		t.Errorf("X-Request-ID = %q, want abc-123", got)
	}

	// A forged id with a newline must be sanitised: it is about to be written
	// into a log line, and an unfiltered one forges log entries.
	req = httptest.NewRequest(http.MethodGet, "/healthz", nil)
	req.Header.Set("X-Request-ID", "evil\r\nINFO forged: true")
	rec = httptest.NewRecorder()
	s.Handler().ServeHTTP(rec, req)
	got := rec.Header().Get("X-Request-ID")
	if strings.ContainsAny(got, "\r\n") {
		t.Errorf("X-Request-ID = %q still contains a newline; this forges log lines", got)
	}
}

// TestUpstreamURLWithoutAdapterFailsStartup checks the misconfiguration guard.
func TestUpstreamURLWithoutAdapterFailsStartup(t *testing.T) {
	cfg := DefaultConfig()
	cfg.UpstreamURL = "https://api.openai.com/v1"
	cfg.Tenants = map[string]*Tenant{testToken: {Name: "acme"}}

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	if _, err := New(cfg, logger); err == nil {
		t.Fatal("New succeeded with an upstream URL and no adapter; that server would " +
			"accept requests and return fake output while appearing to proxy a real model")
	}
}
