package main

import (
	"context"
	"errors"
	"math/rand"
	"strings"
	"sync"
	"time"
)

// Provider is the upstream model this server fronts.
//
// One method, returning a channel of tokens. The channel shape is the whole
// design: it means the handler can stream a token as soon as it arrives rather
// than waiting for a complete response, and it means a test can supply a
// deterministic fake with no network and no model.
//
// # The real adapter is deliberately absent
//
// This file contains a fake, not an OpenAI client. That is a deliberate gap and
// the README says so plainly, because a demo that quietly pretends to talk to
// OpenAI is worse than one that admits it does not.
//
// The reason it is a gap rather than an omission: an HTTP adapter is mechanical
// (build a body, POST it, parse the SSE) and every decision worth making here -
// cancellation, back-pressure, error framing, quota - is upstream of it. Writing
// the adapter would add 150 lines that a reader must skip to reach the parts
// that teach anything.
//
// To wire a real provider: implement Provider, and set PHOTON_UPSTREAM_URL. The
// interface is the contract.
type Provider interface {
	// Stream returns a channel of tokens for req, and a channel of one error if
	// the upstream fails.
	//
	// The contract, which is not obvious and is where most implementations go
	// wrong:
	//
	//   - It must respect ctx and return promptly when it is cancelled. The
	//     handler relies on this to release the request's goroutine.
	//   - It must close the token channel when it is done, including on error.
	//     A provider that returns an error without closing leaves the handler
	//     blocked forever.
	//   - It must not write to either channel after closing either one. The
	//     handler is the only reader, and a send on a closed channel panics.
	//
	// Naming the failure modes in the interface is cheaper than rediscovering
	// them in production.
	Stream(ctx context.Context, req chatRequest) (<-chan string, <-chan error, error)
}

// fakeProvider is a deterministic provider for tests and for the demo.
//
// Deterministic on purpose: it uses a fixed seed, so a test asserting on the
// number of tokens does not fail one run in twenty. Random output in a test is a
// test that will be blamed for a bug it did not have.
type fakeProvider struct {
	// words is the vocabulary. Real output has structure - punctuation, casing,
	// varying word length - because a client that renders tokens will be
	// sensitive to it and a lumpy stream hides buffering bugs.
	words []string

	// delay is the per-token interval, standing in for a model's cadence.
	delay time.Duration

	// failAfter makes the provider fail mid-stream, after n tokens. This is the
	// case that matters: an upstream that dies after the first token is exactly
	// when a proxy can silently truncate a response and the client never finds
	// out.
	failAfter int

	// mu guards the call counters, which tests assert on.
	mu        sync.Mutex
	calls     int
	cancelled int
}

// newFakeProvider returns a provider with a fixed vocabulary and seed.
func newFakeProvider(delay time.Duration) *fakeProvider {
	return &fakeProvider{
		words: []string{
			"Streaming", "means", "the", "first", "token", "reaches", "the",
			"client", "before", "the", "last", "one", "is", "generated.",
			"That", "is", "the", "whole", "point", "of", "this", "server.",
		},
		delay: delay,
	}
}

func (f *fakeProvider) Stream(ctx context.Context, req chatRequest) (<-chan string, <-chan error, error) {
	f.mu.Lock()
	f.calls++
	f.mu.Unlock()

	tokens := make(chan string)
	errc := make(chan error, 1)

	// The generated answer reflects the request, so a developer can see that the
	// right thing happened without reading logs.
	answer := f.compose(req)

	go func() {
		// Both channels are closed on every exit path. A provider that returns
		// without closing them hangs the handler, and the hang appears as a
		// stream that simply stops - the worst possible failure mode to debug.
		defer close(tokens)
		defer close(errc)

		for i, tok := range answer {
			if f.failAfter > 0 && i >= f.failAfter {
				errc <- errors.New("upstream provider failed mid-stream")
				return
			}

			// Cancellation checked before every send, not once at the top. A
			// provider that only checks at the start keeps generating for a client
			// that left minutes ago, which is the single most expensive bug in
			// this whole file.
			select {
			case <-ctx.Done():
				f.mu.Lock()
				f.cancelled++
				f.mu.Unlock()
				return
			case <-time.After(f.delay):
			}

			// The send is in the select too, so a cancelled context does not leave
			// this goroutine blocked on an unread channel.
			select {
			case <-ctx.Done():
				f.mu.Lock()
				f.cancelled++
				f.mu.Unlock()
				return
			case tokens <- tok:
			}
		}
	}()

	return tokens, errc, nil
}

// compose builds the fake answer from the request, so it visibly depends on the
// input.
func (f *fakeProvider) compose(req chatRequest) []string {
	//nolint:gosec // Deterministic jitter for a fake, not a security decision.
	rng := rand.New(rand.NewSource(int64(len(req.Messages))*31 + 7))

	out := make([]string, 0, 24)
	for i, w := range f.words {
		switch {
		case i == 0:
			out = append(out, strings.ToLower(w)+",")
		case i%9 == 8:
			out = append(out, w+string([]rune{'.', ',', ';'}[rng.Intn(3)]))
		default:
			out = append(out, w)
		}
	}
	return out
}

func (f *fakeProvider) Calls() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls
}

func (f *fakeProvider) Cancelled() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.cancelled
}

// echoProvider reflects the prompt back.
//
// Useful in the demo because it makes the round trip obviously correct: what you
// sent is what came back, token by token.
type echoProvider struct {
	delay time.Duration
}

func (e *echoProvider) Stream(ctx context.Context, req chatRequest) (<-chan string, <-chan error, error) {
	if len(req.Messages) == 0 {
		return nil, nil, errors.New("no messages")
	}
	last := req.Messages[len(req.Messages)-1].Content
	words := strings.Fields(last)
	if len(words) == 0 {
		words = []string{"(empty", "prompt)"}
	}

	tokens := make(chan string)
	errc := make(chan error, 1)
	go func() {
		defer close(tokens)
		defer close(errc)
		for i, w := range words {
			tok := w
			if i == len(words)-1 {
				tok += "."
			}
			select {
			case <-ctx.Done():
				return
			case <-time.After(e.delay):
			}
			select {
			case <-ctx.Done():
				return
			case tokens <- tok:
			}
		}
	}()
	return tokens, errc, nil
}
