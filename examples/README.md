# Photon examples

Runnable programs, each one a single `main.go` you can read in a sitting. They
use only Photon and the standard library.

```bash
go run ./examples/hello        # from the repository root
```

All of them listen on `:8080` (one at a time), and stop gracefully on Ctrl-C.

| Example | What it shows | Start here if... |
|---|---|---|
| [`hello`](hello) | Routes, a path parameter, JSON, a token stream, `Run` | you are new to Photon |
| [`stream`](stream) | Every streaming technique: tokens, resume with `Last-Event-ID`, `DropOldest` metrics, back-pressure, errors after the first event, shutdown with `Done()` | you are building anything that streams |
| [`openai-proxy`](openai-proxy) | An OpenAI-compatible gateway that relays an upstream model and stops it when the client leaves. Runs without an API key. | you put a server in front of a model |
| [`agent`](agent) | An agent's thinking, tool calls, and answer as typed events, with a browser UI | you are streaming agent steps to a frontend |
| [`api`](api) | Groups, `DecodeJSON`, `Error`, `photonerr`, auth on part of the tree, request logging | you are building a JSON API |
| [`middleware`](middleware) | CORS, rate limiting, bearer auth, roles - and the tests that prove the ordering | you are porting middleware |
| [`security`](security) | What the defaults stop, demonstrated with requests you can send | you want to see the limits work |
| [`production`](production) | Health vs readiness, JSON logs, timeouts, graceful drain | you are deploying |
| [`openai-backend`](openai-backend) | A complete OpenAI-compatible backend: tenants, quotas, metrics, Dockerfile. A separate module. | you want a larger, realistic codebase |

## Start a project from an example

The `photon` CLI copies an example into a new module:

```bash
go install github.com/agenticmarket/photon/cmd/photon@latest

photon init my-agent -template agent         # agent, openai-proxy, api, or minimal
cd my-agent && go mod tidy && go run .
```

## Quick tour

```bash
go run ./examples/hello
curl localhost:8080/greet/Ada            # {"message":"Hello, Ada!"}
curl -N localhost:8080/count             # five events, then done
```

```bash
go run ./examples/openai-proxy
curl -N localhost:8080/v1/chat/completions -H 'Content-Type: application/json' \
  -d '{"model":"demo","stream":true,"messages":[{"role":"user","content":"hi"}]}'
# Press Ctrl-C halfway: the gateway logs the upstream request being cancelled.
```

```bash
go run ./examples/agent
# open http://localhost:8080
```

```bash
go run ./examples/stream
curl -N localhost:8080/tokens
curl -N -H 'Last-Event-ID: 5' localhost:8080/resumable   # starts at token 6
```

Use `curl -N`: without it, curl buffers output and a stream appears all at once
at the end, which looks exactly like a server that does not stream.
