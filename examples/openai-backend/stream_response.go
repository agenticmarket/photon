package main

import (
	"context"
	"net/http"
	"time"

	"github.com/agenticmarket/photon"
)

// streamResponse writes an OpenAI-shaped SSE stream.
//
// # The one thing that is easy to get wrong
//
// Once the first byte is written the status line is gone. The headers say 200
// and the status cannot be changed for the life of the connection. So an error
// that happens *after* the stream has started cannot be reported as an HTTP
// status - the only way to tell the client is an SSE frame carrying an error
// object, followed by the stream ending.
//
// This is why there are two error paths in this function and they look
// different: a failure before the first token is a real HTTP status, and a
// failure after it is a data frame. Getting this wrong is the classic OpenAI
// proxy bug - a client receives a truncated stream with no indication that
// anything went wrong, and renders a half-answer as if it were complete.
func (s *Server) streamResponse(
	ctx context.Context,
	w http.ResponseWriter,
	r *http.Request,
	stream *photon.Stream,
	tokens <-chan string,
	upstreamErr <-chan error,
	req chatRequest,
	reqID string,
	start time.Time,
) {
	// A comment frame goes out immediately, before any token. It commits the
	// headers and flushes them, so the client's connection is established and its
	// time-to-first-byte is now measured - which is what a UI shows as "connected"
	// rather than "waiting". Without it a slow provider looks like a hung server.
	if err := stream.Comment("open"); err != nil {
		s.metrics.streamsAborted.Add(1)
		return
	}

	// Heartbeats keep the connection alive through nginx and mobile networks while
	// the provider thinks. A model that takes 10 seconds to produce its first
	// token would otherwise be indistinguishable from a dead connection to every
	// proxy in the path.
	stopHeartbeat := stream.StartHeartbeats()
	defer stopHeartbeat()

	id := newCompletionID(reqID)
	created := time.Now().Unix()

	// The first chunk carries the role and empty content. OpenAI clients expect
	// this three-part shape - role, then deltas, then finish_reason - and a
	// stream that starts directly with content is mis-parsed by the official SDK.
	if err := s.writeChunk(stream, id, created, req.Model,
		streamDelta{Role: "assistant"}, nil, nil); err != nil {
		s.abortStream(stream, r, reqID, start, tokens, upstreamErr, "first chunk")
		return
	}

	sent := 0
	upstreamFailed := false

	// The select reads both channels at once. A `for range tokens` would miss an
	// upstream error entirely, because the provider closes tokens when it fails -
	// so the loop would end cleanly and the client would be told the answer
	// finished when in fact it was truncated.
	for tokens != nil || upstreamErr != nil {
		select {
		case <-ctx.Done():
			// The client disconnected. Nothing to write and nothing to log as an
			// error: a user closing a tab is not a server problem.
			s.metrics.streamsAborted.Add(1)
			s.logger.Info("client disconnected",
				"request_id", reqID, "tokens_sent", sent,
				"took", time.Since(start).Round(time.Microsecond).String())
			return

		case tok, ok := <-tokens:
			if !ok {
				tokens = nil
				continue
			}
			// A space between tokens is what makes a token stream readable. It is
			// part of the wire format, not decoration, and omitting it produces
			// "helloworld" in every client.
			payload := tok + " "
			if err := s.writeChunk(stream, id, created, req.Model,
				streamDelta{Content: payload}, nil, nil); err != nil {
				s.abortStream(stream, r, reqID, start, tokens, upstreamErr, "write token")
				return
			}
			sent++
			s.metrics.tokensStreamed.Add(1)

		case err, ok := <-upstreamErr:
			if !ok {
				upstreamErr = nil
				continue
			}
			if err != nil {
				upstreamFailed = true
				s.metrics.upstreamErrors.Add(1)
				s.logger.Error("upstream failed mid-stream",
					"request_id", reqID, "err", err, "tokens_sent", sent)
				// The error frame, so the client learns the answer is incomplete
				// rather than assuming it is finished.
				s.writeErrorChunk(stream, id, created, req.Model, err)
			}
			upstreamErr = nil
		}
	}

	// The terminal chunk. A client that sees finish_reason knows the answer is
	// complete; a client that sees the connection close has to guess.
	finish := "stop"
	if upstreamFailed {
		// Not a normal stop. Saying "stop" after a failed stream tells the client
		// the answer finished normally, which is the lie this whole design is
		// avoiding.
		finish = "error"
	}
	usage := &usage{
		PromptTokens:     estimatePromptTokens(req),
		CompletionTokens: sent,
		TotalTokens:      estimatePromptTokens(req) + sent,
	}
	if err := s.writeChunk(stream, id, created, req.Model, streamDelta{}, &finish, usage); err != nil {
		s.abortStream(stream, r, reqID, start, nil, nil, "final chunk")
		return
	}
	// A last chunk carrying usage, which is what stream_options.include_usage
	// asks for. Sent only when the client asked, because it is an extra frame
	// every client must tolerate.
	if req.StreamOptions != nil && req.StreamOptions.IncludeUsage {
		_ = s.writeChunk(stream, id, created, req.Model, streamDelta{}, nil, usage)
	}

	_ = stream.Close()

	elapsed := time.Since(start)
	if upstreamFailed {
		s.metrics.streamsAborted.Add(1)
	} else {
		s.metrics.observeStream(elapsed)
	}
	s.logger.Info("stream complete",
		"request_id", reqID, "tokens", sent,
		"took", elapsed.Round(time.Microsecond).String(),
		"upstream_failed", upstreamFailed)
}

// abortStream handles a write that failed because the client is gone.
//
// It is separate from the main path because the actions differ: there is no
// error frame to send (the socket is gone) and the metric is an abort, not a
// close.
func (s *Server) abortStream(
	stream *photon.Stream,
	r *http.Request,
	reqID string,
	start time.Time,
	tokens <-chan string,
	upstreamErr <-chan error,
	where string,
) {
	_ = tokens
	_ = upstreamErr
	s.metrics.streamsAborted.Add(1)
	s.logger.Info("stream aborted",
		"request_id", reqID, "at", where, "err", stream.Err(),
		"took", time.Since(start).Round(time.Microsecond).String())
	_ = stream.Close()
}

// writeChunk writes one SSE chunk.
func (s *Server) writeChunk(
	stream *photon.Stream,
	id string, created int64, model string,
	delta streamDelta, finish *string, usage *usage,
) error {
	chunk := streamChunk{
		ID:      id,
		Object:  "chat.completion.chunk",
		Created: created,
		Model:   model,
		Choices: []streamChoice{{
			Index:        0,
			Delta:        delta,
			FinishReason: finish,
		}},
		Usage: usage,
	}
	// Event is used rather than Write, so the framing is the SSE the client
	// expects. Hand-formatting "data: {...}\n\n" here would be one line that
	// every future edit has to keep correct.
	return stream.Event("", marshalChunk(chunk))
}

// writeErrorChunk sends a mid-stream error as a data frame.
//
// This is the only mechanism available once the status line is committed, and it
// is why the frame is an ordinary data frame containing an error object rather
// than an SSE event with an error type: OpenAI clients parse data frames, and an
// event type they do not expect is dropped.
func (s *Server) writeErrorChunk(
	stream *photon.Stream,
	id string, created int64, model string, cause error,
) {
	payload := errorResponse{Error: errorDetail{
		Message: "the model provider failed: " + cause.Error(),
		Type:    "api_error",
		Code:    "upstream_error",
	}}
	chunk := streamChunk{
		ID:      id,
		Object:  "chat.completion.chunk",
		Created: created,
		Model:   model,
		Choices: []streamChoice{{
			Index:        0,
			Delta:        streamDelta{Content: ""},
			FinishReason: strPtr("error"),
		}},
	}
	// Two frames: the error object, then a terminal chunk. A client that reads
	// only the first knows something went wrong; one that reads both also knows
	// the stream is over.
	_ = stream.Event("", marshalChunk(chunk))
	_ = stream.Event("", marshalError(payload))
}

// bufferedResponse answers a non-streaming request.
//
// It drains the same channel the streaming path reads, so a provider
// implementation works for both without a second code path, and the cancellation
// behaviour is identical: a client that disconnects mid-request stops the
// provider.
func (s *Server) bufferedResponse(
	w http.ResponseWriter,
	r *http.Request,
	tokens <-chan string,
	upstreamErr <-chan error,
	req chatRequest,
	reqID string,
) {
	ctx := r.Context()
	var b []byte
	var failed bool

	for tokens != nil || upstreamErr != nil {
		select {
		case <-ctx.Done():
			s.metrics.streamsAborted.Add(1)
			return
		case tok, ok := <-tokens:
			if !ok {
				tokens = nil
				continue
			}
			b = append(b, tok...)
			b = append(b, ' ')
			s.metrics.tokensStreamed.Add(1)
		case err, ok := <-upstreamErr:
			if !ok {
				upstreamErr = nil
				continue
			}
			if err != nil {
				failed = true
				s.metrics.upstreamErrors.Add(1)
				s.logger.Error("upstream failed", "request_id", reqID, "err", err)
			}
			upstreamErr = nil
		}
	}

	if failed {
		// Nothing has been written yet, so a real status is still available. This
		// is the other half of the two-error-path design: buffered can use HTTP
		// because it has not committed.
		writeError(w, http.StatusBadGateway,
			"api_error", "upstream_error", "the model provider failed")
		return
	}

	promptTokens := estimatePromptTokens(req)
	resp := chatResponse{
		ID:      newCompletionID(reqID),
		Object:  "chat.completion",
		Created: time.Now().Unix(),
		Model:   req.Model,
		Choices: []chatChoice{{
			Index:        0,
			Message:      chatMessage{Role: "assistant", Content: string(b)},
			FinishReason: strPtr("stop"),
		}},
		Usage: usage{
			PromptTokens:     promptTokens,
			CompletionTokens: len(b) / 4, // rough, and labelled as such
			TotalTokens:      promptTokens + len(b)/4,
		},
	}
	writeJSON(w, http.StatusOK, resp)
}
