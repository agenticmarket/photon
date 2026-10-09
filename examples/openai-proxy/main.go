// Command openai-proxy is an OpenAI-compatible gateway: it accepts Chat
// Completions requests, forwards them to an upstream model server, and relays
// the streamed answer to the client token by token.
//
// Run it with no configuration and it starts a built-in fake upstream, so the
// whole path - client, gateway, upstream - works without an API key:
//
//	go run ./examples/openai-proxy
//
//	curl -N localhost:8080/v1/chat/completions \
//	  -H 'Content-Type: application/json' \
//	  -d '{"model":"demo","stream":true,"messages":[{"role":"user","content":"hi"}]}'
//
// Point it at a real provider - OpenAI, vLLM, Ollama, LiteLLM, anything that
// speaks the Chat Completions format:
//
//	UPSTREAM_URL=https://api.openai.com UPSTREAM_API_KEY=sk-... go run ./examples/openai-proxy
//
// Set GATEWAY_KEYS=key1,key2 to require clients to present one of those keys.
//
// What it demonstrates:
//
//   - A client disconnect cancels the upstream request, so the provider stops
//     generating tokens nobody will read.
//   - Back-pressure end to end: a slow client slows the upstream read rather
//     than buffering the answer in the gateway.
//   - Errors before the first token are real HTTP errors in the shape OpenAI
//     SDKs parse; errors after it are sent in-band.
//   - stream:false and stream:true on one route.
package main

import (
	"bufio"
	"bytes"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/agenticmarket/photon"
	"github.com/agenticmarket/photon/photonerr"
)

func main() {
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stderr, nil)))

	upstream := os.Getenv("UPSTREAM_URL")
	if upstream == "" {
		upstream = startFakeUpstream()
		slog.Info("no UPSTREAM_URL set; using the built-in fake upstream", "url", upstream)
	}
	g := &gateway{
		upstream: strings.TrimSuffix(upstream, "/"),
		apiKey:   os.Getenv("UPSTREAM_API_KEY"),
		client: &http.Client{Transport: &http.Transport{
			ResponseHeaderTimeout: 30 * time.Second,
			MaxIdleConnsPerHost:   64,
			IdleConnTimeout:       90 * time.Second,
		}},
	}

	app := photon.New(photon.Logger(slog.Default()))
	app.GET("/healthz", func(w http.ResponseWriter, r *http.Request) {
		_ = photon.JSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})

	v1 := app.Group("/v1", requireKey(os.Getenv("GATEWAY_KEYS")))
	v1.POST("/chat/completions", g.chatCompletions)
	v1.GET("/models", g.models)

	addr := ":" + envOr("PORT", "8080")
	slog.Info("openai-proxy listening", "addr", addr, "upstream", g.upstream)
	if err := app.Run(addr); err != nil {
		slog.Error("server stopped", "err", err)
		os.Exit(1)
	}
}

// gateway forwards Chat Completions requests to one upstream.
type gateway struct {
	upstream string
	apiKey   string
	client   *http.Client
}

// chatRequest holds the fields the gateway inspects. Everything else the
// client sent is forwarded untouched, because raw keeps the original body.
type chatRequest struct {
	Model    string            `json:"model"`
	Stream   bool              `json:"stream"`
	Messages []json.RawMessage `json:"messages"`
	raw      []byte
}

func (g *gateway) chatCompletions(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(r.Body) // bounded by the server's 1 MiB body limit
	if err != nil {
		var tooLarge *http.MaxBytesError
		if errors.As(err, &tooLarge) {
			openAIError(w, http.StatusRequestEntityTooLarge, "the request body is too large")
			return
		}
		openAIError(w, http.StatusBadRequest, "could not read the request body")
		return
	}
	var req chatRequest
	if err := json.Unmarshal(body, &req); err != nil {
		openAIError(w, http.StatusBadRequest, "the request body is not valid JSON")
		return
	}
	if req.Model == "" || len(req.Messages) == 0 {
		openAIError(w, http.StatusBadRequest, "model and messages are required")
		return
	}
	req.raw = body

	if !req.Stream {
		g.forwardJSON(w, r, req)
		return
	}
	photon.SSE(func(s *photon.Stream, r *http.Request) error {
		return g.relayStream(s, r, req)
	})(w, r)
}

// relayStream forwards a streaming request and relays each upstream event.
func (g *gateway) relayStream(s *photon.Stream, r *http.Request, req chatRequest) error {
	// r.Context() is cancelled when our client disconnects. Tying the upstream
	// request to it closes the upstream connection at the same moment, which
	// is the signal a provider uses to stop generating.
	resp, err := g.send(r, req.raw)
	if err != nil {
		return photonerr.Upstream(err) // nothing sent yet: the client gets a 502
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return upstreamStatus(resp)
	}

	sc := bufio.NewScanner(resp.Body)
	sc.Buffer(make([]byte, 0, 64<<10), 1<<20)
	for sc.Scan() {
		data, ok := bytes.CutPrefix(sc.Bytes(), []byte("data:"))
		if !ok {
			continue // blank separators, comments, and fields we do not relay
		}
		data = bytes.TrimPrefix(data, []byte(" "))
		// Unnamed events, as the OpenAI SDKs expect. When the client is slow,
		// this call waits, the loop stops reading, and the upstream's TCP
		// window fills: back-pressure all the way to the provider.
		if err := s.Event("", data); err != nil {
			return err // the client left; the deferred Close stops the upstream
		}
	}
	if err := sc.Err(); err != nil && r.Context().Err() == nil {
		return photonerr.Upstream(err)
	}
	return nil
}

// forwardJSON relays a non-streaming request and its response.
func (g *gateway) forwardJSON(w http.ResponseWriter, r *http.Request, req chatRequest) {
	resp, err := g.send(r, req.raw)
	if err != nil {
		slog.Warn("upstream request failed", "err", err)
		openAIError(w, http.StatusBadGateway, "the upstream model is unavailable")
		return
	}
	defer resp.Body.Close()
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(resp.StatusCode)
	_, _ = io.Copy(w, resp.Body)
}

// models relays the upstream model list.
func (g *gateway) models(w http.ResponseWriter, r *http.Request) {
	req, err := http.NewRequestWithContext(r.Context(), http.MethodGet, g.upstream+"/v1/models", nil)
	if err != nil {
		openAIError(w, http.StatusInternalServerError, "could not build the upstream request")
		return
	}
	g.authorize(req)
	resp, err := g.client.Do(req)
	if err != nil {
		openAIError(w, http.StatusBadGateway, "the upstream model is unavailable")
		return
	}
	defer resp.Body.Close()
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(resp.StatusCode)
	_, _ = io.Copy(w, resp.Body)
}

func (g *gateway) send(r *http.Request, body []byte) (*http.Response, error) {
	req, err := http.NewRequestWithContext(r.Context(), http.MethodPost,
		g.upstream+"/v1/chat/completions", bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "text/event-stream")
	g.authorize(req)
	return g.client.Do(req)
}

func (g *gateway) authorize(req *http.Request) {
	if g.apiKey != "" {
		req.Header.Set("Authorization", "Bearer "+g.apiKey)
	}
}

// upstreamStatus turns a non-200 upstream answer into an error. Rate limits
// keep their status so the client backs off; everything else is a 502,
// because the upstream's own error text can describe its internals.
func upstreamStatus(resp *http.Response) error {
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 64<<10))
	if resp.StatusCode == http.StatusTooManyRequests {
		return photonerr.TooManyRequests("the upstream model is rate limited")
	}
	return photonerr.Upstream(fmt.Errorf("upstream answered %s", resp.Status))
}

// requireKey checks the client's bearer token against GATEWAY_KEYS. With no
// keys configured the gateway is open, which is what a local demo wants and a
// deployment must not ship with.
func requireKey(keys string) photon.Middleware {
	var allowed [][]byte
	for _, k := range strings.Split(keys, ",") {
		if k = strings.TrimSpace(k); k != "" {
			allowed = append(allowed, []byte(k))
		}
	}
	return func(next http.Handler) http.Handler {
		if len(allowed) == 0 {
			return next
		}
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			got, _ := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer ")
			for _, k := range allowed {
				if subtle.ConstantTimeCompare([]byte(got), k) == 1 {
					next.ServeHTTP(w, r)
					return
				}
			}
			openAIError(w, http.StatusUnauthorized, "invalid API key")
		})
	}
}

// openAIError writes the error shape OpenAI SDKs parse into a typed exception.
func openAIError(w http.ResponseWriter, status int, msg string) {
	typ := "invalid_request_error"
	switch {
	case status == http.StatusUnauthorized:
		typ = "authentication_error"
	case status >= 500:
		typ = "api_error"
	}
	_ = photon.JSON(w, status, map[string]any{"error": map[string]any{"message": msg, "type": typ}})
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

// ---------------------------------------------------------------- fake upstream

// startFakeUpstream serves a small Chat Completions imitation on a loopback
// port and returns its base URL. It streams a fixed answer a word at a time,
// at roughly a real model's pace.
func startFakeUpstream() string {
	fake := photon.New()
	fake.GET("/v1/models", func(w http.ResponseWriter, r *http.Request) {
		_ = photon.JSON(w, http.StatusOK, map[string]any{
			"object": "list",
			"data":   []map[string]string{{"id": "demo", "object": "model", "owned_by": "photon"}},
		})
	})
	fake.POST("/v1/chat/completions", func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			Model  string `json:"model"`
			Stream bool   `json:"stream"`
		}
		if err := photon.DecodeJSON(r, &req); err != nil {
			photon.Error(w, r, err)
			return
		}
		words := strings.Fields("Hello from the fake upstream. Close the connection halfway " +
			"and watch the gateway log the upstream request being cancelled.")
		id := fmt.Sprintf("chatcmpl-%d", time.Now().UnixNano())
		if !req.Stream {
			_ = photon.JSON(w, http.StatusOK, completion(id, req.Model, strings.Join(words, " ")))
			return
		}
		photon.SSE(func(s *photon.Stream, r *http.Request) error {
			for i, word := range words {
				if i > 0 {
					word = " " + word
				}
				if err := s.JSON("", chunk(id, req.Model, word)); err != nil {
					slog.Info("fake upstream: stopped generating, the gateway hung up", "after_tokens", i)
					return err
				}
				select {
				case <-time.After(80 * time.Millisecond):
				case <-r.Context().Done():
					slog.Info("fake upstream: request cancelled", "after_tokens", i+1)
					return nil
				}
			}
			return s.Event("", []byte("[DONE]"))
		})(w, r)
	})

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		panic(err)
	}
	go func() { _ = fake.Serve(ln) }()
	return "http://" + ln.Addr().String()
}

func chunk(id, model, text string) map[string]any {
	return map[string]any{
		"id": id, "object": "chat.completion.chunk", "created": time.Now().Unix(), "model": model,
		"choices": []map[string]any{{"index": 0, "delta": map[string]string{"content": text}}},
	}
}

func completion(id, model, text string) map[string]any {
	return map[string]any{
		"id": id, "object": "chat.completion", "created": time.Now().Unix(), "model": model,
		"choices": []map[string]any{{
			"index": 0, "finish_reason": "stop",
			"message": map[string]string{"role": "assistant", "content": text},
		}},
	}
}
