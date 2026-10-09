# Photon development tasks. Every target is a plain Go command, so nothing here
# is required: read the recipe and run it by hand if you prefer.

GO ?= go
MODULES := . examples/openai-backend bench/baseline bench/stream

.PHONY: all test race check fmt vet lint vuln fuzz bench all-modules templates clean

all: check test

test: ## run the root module's tests
	$(GO) test ./...

race: ## the race detector, three times - the gate that matters for concurrency changes
	$(GO) test -race -count=3 ./...

check: fmt vet ## formatting, vet, and the docs coverage test
	$(GO) test -run TestDocsMentionEveryExportedSymbol .

fmt:
	@out=$$(gofmt -l $$(git ls-files '*.go')); if [ -n "$$out" ]; then echo "gofmt needed:"; echo "$$out"; exit 1; fi

vet:
	$(GO) vet ./...

lint: ## staticcheck
	GOTOOLCHAIN=go1.27.1 $(GO) run honnef.co/go/tools/cmd/staticcheck@latest ./...

vuln: ## known vulnerabilities in the code and the Go toolchain
	$(GO) run golang.org/x/vuln/cmd/govulncheck@latest ./...

fuzz: ## the routing and SSE framing fuzzers, 30 s each
	$(GO) test -run '^$$' -fuzz FuzzFind -fuzztime 30s ./internal/tree
	$(GO) test -run '^$$' -fuzz FuzzSSEEventRoundTrip -fuzztime 30s .

bench: ## microbenchmarks
	$(GO) test -run '^$$' -bench . -benchmem .

all-modules: ## test every module in the repository
	@for m in $(MODULES); do echo "== $$m"; (cd $$m && $(GO) test ./...) || exit 1; done

templates: ## scaffold every `photon init` template and build it
	@tmp=$$(mktemp -d); $(GO) build -o $$tmp/photon ./cmd/photon; \
	for t in minimal api agent openai-proxy; do \
		$$tmp/photon init $$tmp/$$t -template $$t -photon . >/dev/null && \
		(cd $$tmp/$$t && $(GO) mod tidy >/dev/null && $(GO) build -o /dev/null .) && echo "template $$t: ok" || exit 1; \
	done; rm -rf $$tmp

clean:
	$(GO) clean -testcache
