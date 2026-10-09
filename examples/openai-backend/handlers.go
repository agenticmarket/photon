package main

import (
	"errors"
	"net/http"
	"strings"
	"time"

	"github.com/agenticmarket/photon"
)

// chatCompletions serves POST /v1/chat/completions, streaming or not.
//
// The whole handler is the streaming path with a non-streaming branch, and the
// two are kept in one function deliberately. A proxy that implements them
// separately drifts: the validation, the error shapes, and the usage accounting
// are duplicated, and the copies diverge. Everything before the branch is shared
// for exactly that reason.
func (s *Server) chatCompletions(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	tenant := tenantFrom(r.Context())
	reqID := requestIDFrom(r.Context())

	// Logged with a defer so every exit path is recorded, including the ones
	// that return early. A handler that logs only on success has no record of the
	// requests that failed, which are exactly the ones an operator is looking
	// for.
	var status = http.StatusOK
	defer func() {
		s.metrics.recordRequest("POST /v1/chat/completions", status)
		s.logger.Info("request",
			"request_id", reqID,
			"tenant", tenant.Name,
			"path", r.URL.Path,
			"status", status,
			// A string, not a time.Duration. slog's JSON handler renders a
			// Duration as a bare integer of nanoseconds, so a 337 ms request logs
			// as "took":337845100 - unreadable, and unreadable durations are how
			// latency regressions go unnoticed.
			"took", time.Since(start).Round(time.Microsecond).String())
	}()

	var req chatRequest
	if err := decodeJSON(w, r, maxRequestBody, &req); err != nil {
		// The body limit has already been answered with a 413 by decodeJSON.
		if !errors.Is(err, errBodyTooLarge) {
			status = http.StatusBadRequest
			s.logger.Warn("bad request", "request_id", reqID, "err", err)
			writeError(w, http.StatusBadRequest,
				"invalid_request_error", "invalid_json",
				"could not parse the request body as JSON")
		}
		return
	}

	if verr := s.validateRequest(w, r, &req, reqID); verr != nil {
		// validateRequest has already written the response; the status is
		// recovered from the error so the deferred log line records what was
		// actually sent rather than a default 200.
		var re *requestError
		if errors.As(verr, &re) {
			status = re.status
		}
		return
	}

	// The provider call is made with the request context, so a client that
	// disconnects cancels the upstream work rather than only stopping the write.
	// This is the single most important line in the handler: without it the
	// server keeps paying for model output nobody will read.
	ctx := r.Context()
	tokens, upstreamErr, err := s.provider.Stream(ctx, req)
	if err != nil {
		s.metrics.upstreamErrors.Add(1)
		status = http.StatusBadGateway
		s.logger.Error("upstream failed to start", "request_id", reqID, "err", err)
		writeError(w, http.StatusBadGateway,
			"api_error", "upstream_error", "the model provider could not be reached")
		return
	}

	// Disconnect is the overflow policy, and it is the right one here by
	// default. DropOldest on a token stream produces output with a hole in the
	// middle, which reads as a hallucination and which the client cannot
	// distinguish from the model saying it. photon warns if DropOldest is used on
	// a text stream for exactly this reason.
	//
	// The policy is set explicitly rather than left to the default so that a
	// reader can see the decision at the point it matters, instead of having to
	// know what the default is.
	stream := photon.StreamFrom(r)
	stream.Overflow(photon.Disconnect)

	if req.Stream {
		s.streamResponse(ctx, w, r, stream, tokens, upstreamErr, req, reqID, start)
		return
	}

	// Non-streaming: drain the provider into a buffer and answer once. The same
	// channel is read, so the provider implementation is shared and the
	// cancellation behaviour is identical.
	s.bufferedResponse(w, r, tokens, upstreamErr, req, reqID)
	status = http.StatusOK
}

// requestError carries the status a validation failure should produce.
type requestError struct {
	status  int
	errType string
	code    string
	msg     string
}

func (e *requestError) Error() string { return e.msg }

// validateRequest checks the request, answering the client on failure.
func (s *Server) validateRequest(w http.ResponseWriter, r *http.Request, req *chatRequest, reqID string) error {
	fail := func(status int, errType, code, msg string) error {
		s.logger.Warn("rejected", "request_id", reqID, "code", code, "msg", msg)
		writeError(w, status, errType, code, msg)
		return &requestError{status: status, errType: errType, code: code, msg: msg}
	}

	if req.Model == "" {
		return fail(http.StatusBadRequest, "invalid_request_error", "model_required",
			"you must provide a model parameter")
	}
	if req.Model != s.cfg.ModelName {
		return fail(http.StatusNotFound, "invalid_request_error", "model_not_found",
			"the model "+req.Model+" does not exist")
	}
	if len(req.Messages) == 0 {
		return fail(http.StatusBadRequest, "invalid_request_error", "messages_required",
			"you must provide at least one message")
	}
	// The last message must be from the user. A conversation ending in an
	// assistant turn is not a request for a completion, and silently accepting it
	// produces a model continuing its own output - which looks like a bug in the
	// proxy rather than a mistake in the request.
	last := req.Messages[len(req.Messages)-1]
	if last.Role != "user" {
		return fail(http.StatusBadRequest, "invalid_request_error", "invalid_role",
			"the last message must have role 'user'")
	}
	if strings.TrimSpace(last.Content) == "" {
		return fail(http.StatusBadRequest, "invalid_request_error", "empty_content",
			"the last message must not be empty")
	}
	if req.MaxTokens < 0 {
		return fail(http.StatusBadRequest, "invalid_request_error", "invalid_max_tokens",
			"max_tokens must not be negative")
	}
	if req.Temperature != nil && (*req.Temperature < 0 || *req.Temperature > 2) {
		return fail(http.StatusBadRequest, "invalid_request_error", "invalid_temperature",
			"temperature must be between 0 and 2")
	}
	return nil
}
