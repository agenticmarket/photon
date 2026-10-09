package main

import (
	"encoding/json"
	"net/http"
	"time"
)

// newCompletionID returns an OpenAI-shaped completion id.
//
// The "chatcmpl-" prefix is not decoration: some client libraries key
// telemetry or retry logic off the id format, and a bare UUID trips code paths
// that expect a known prefix.
func newCompletionID(requestID string) string {
	// The request id is folded in rather than generated fresh, so one request's
	// chunks share an id derived from a value that is already unique for it.
	// That also means a single log search finds every frame of one stream.
	sum := 0
	for _, c := range requestID {
		sum = (sum*31 + int(c)) & 0xFFFFFF
	}
	return "chatcmpl-" + time.Now().UTC().Format("20060102150405") + hex6(sum)
}

func hex6(n int) string {
	const digits = "0123456789abcdef"
	out := make([]byte, 6)
	for i := 5; i >= 0; i-- {
		out[i] = digits[n&0xF]
		n >>= 4
	}
	return string(out)
}

// marshalChunk encodes a stream chunk.
//
// json.Marshal on a struct of strings and ints cannot fail, so an error here
// would be a bug rather than a condition to handle. Returning the error anyway
// means a future field that can fail is handled rather than panicking in
// production - and the caller, which has no way to report a 500 at this point,
// treats it as a stream abort.
func marshalChunk(c streamChunk) []byte {
	b, err := json.Marshal(c)
	if err != nil {
		return []byte(`{"error":{"message":"could not encode chunk","type":"server_error"}}`)
	}
	return b
}

func marshalError(e errorResponse) []byte {
	b, err := json.Marshal(e)
	if err != nil {
		return []byte(`{"error":{"message":"could not encode error","type":"server_error"}}`)
	}
	return b
}

func strPtr(s string) *string { return &s }

// estimatePromptTokens approximates the prompt size.
//
// A word is roughly four tokens in English text, and every tokenizer in use
// disagrees at the edges. This number exists so a client's usage accounting is
// plausible rather than absent; it is not billing-grade, and the field name in a
// real deployment would be a computed count from the provider's own response.
//
// What would be wrong is reporting 0, because a client that displays usage to a
// user shows "0 tokens" and concludes the proxy is broken.
func estimatePromptTokens(req chatRequest) int {
	total := 0
	for _, m := range req.Messages {
		total += len(m.Content)/4 + 4 // +4 per message for role and delimiters
	}
	return total + 3 // priming
}

// models serves GET /v1/models.
//
// The shape matters: an OpenAI client enumerates models to populate a picker, and
// a response that is not this exact shape is either ignored or reported as a
// parse error.
func (s *Server) models(w http.ResponseWriter, r *http.Request) {
	s.metrics.recordRequest("GET /v1/models", http.StatusOK)
	writeJSON(w, http.StatusOK, modelList{
		Object: "list",
		Data: []modelInfo{{
			ID:      s.cfg.ModelName,
			Object:  "model",
			Created: time.Now().Unix(),
			OwnedBy: "photon",
		}},
	})
}
