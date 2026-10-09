package main

// OpenAI wire types.
//
// The shapes here are the contract. An OpenAI client - the official SDK,
// LangChain, Continue, or a curl loop - parses these exactly, so a field that is
// renamed or omitted produces a client-side parse error rather than a message
// about the thing that actually went wrong.
//
// Field names are snake_case with explicit json tags. Go's default would be to
// emit the Go name, and `MaxTokens` becoming `MaxTokens` instead of `max_tokens`
// is precisely the kind of mismatch that costs an afternoon.

// chatRequest is the POST /v1/chat/completions body.
type chatRequest struct {
	Model    string        `json:"model"`
	Messages []chatMessage `json:"messages"`
	Stream   bool          `json:"stream,omitempty"`

	MaxTokens   int      `json:"max_tokens,omitempty"`
	Temperature *float64 `json:"temperature,omitempty"`
	TopP        *float64 `json:"top_p,omitempty"`
	Stop        []string `json:"stop,omitempty"`

	// User is OpenAI's end-user identifier, used by OpenAI for abuse
	// monitoring. It is accepted and ignored here, which is stated rather than
	// silently dropped: a client sending it is not confused about whether it
	// arrived, and an operator reading the log can see it was never used.
	User string `json:"user,omitempty"`

	// StreamOptions carries usage-in-stream settings. Accepted and ignored, with
	// the same reasoning.
	StreamOptions *streamOptions `json:"stream_options,omitempty"`
}

type streamOptions struct {
	IncludeUsage bool `json:"include_usage,omitempty"`
}

type chatMessage struct {
	Role    string `json:"role"`
	Content string `json:"content"`

	// Name is an optional participant identifier. Accepted and ignored.
	Name string `json:"name,omitempty"`
}

// chatResponse is the non-streaming response body.
type chatResponse struct {
	ID      string       `json:"id"`
	Object  string       `json:"object"`
	Created int64        `json:"created"`
	Model   string       `json:"model"`
	Choices []chatChoice `json:"choices"`
	Usage   usage        `json:"usage"`
}

type chatChoice struct {
	Index        int         `json:"index"`
	Message      chatMessage `json:"message"`
	FinishReason *string     `json:"finish_reason"`
}

type usage struct {
	PromptTokens     int `json:"prompt_tokens"`
	CompletionTokens int `json:"completion_tokens"`
	TotalTokens      int `json:"total_tokens"`
}

// streamChunk is one SSE payload in a streamed completion.
//
// The first chunk carries role with empty content, intermediate chunks carry the
// delta, and the last carries finish_reason with an empty delta. That three-part
// shape is what OpenAI clients expect, and a proxy that emits only deltas is a
// proxy whose clients mis-parse the stream.
type streamChunk struct {
	ID      string         `json:"id"`
	Object  string         `json:"object"`
	Created int64          `json:"created"`
	Model   string         `json:"model"`
	Choices []streamChoice `json:"choices"`
	Usage   *usage         `json:"usage,omitempty"`
}

type streamChoice struct {
	Index        int         `json:"index"`
	Delta        streamDelta `json:"delta"`
	FinishReason *string     `json:"finish_reason"`
}

type streamDelta struct {
	// Role is set on the first chunk only.
	Role string `json:"role,omitempty"`
	// Content is empty on the first and last chunks.
	Content string `json:"content"`
}

// errorResponse is the OpenAI error envelope.
//
// Every non-2xx response uses this shape, including the error frames sent *after*
// a stream has started. See the streaming handler for why that second case
// matters.
type errorResponse struct {
	Error errorDetail `json:"error"`
}

type errorDetail struct {
	Message string `json:"message"`
	Type    string `json:"type"`
	Param   string `json:"param,omitempty"`
	Code    string `json:"code,omitempty"`
}

// modelList is the GET /v1/models response.
type modelList struct {
	Object string      `json:"object"`
	Data   []modelInfo `json:"data"`
}

type modelInfo struct {
	ID      string `json:"id"`
	Object  string `json:"object"`
	Created int64  `json:"created"`
	OwnedBy string `json:"owned_by"`
}
