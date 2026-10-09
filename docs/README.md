# Photon documentation

The streaming-first Go server for AI agents.

## Start here

| | |
|---|---|
| [Getting started](getting-started.md) | From `go get` to a tested streaming endpoint in about ten minutes |
| [Examples](../examples/) | Runnable programs: hello, streaming, an OpenAI proxy, an agent UI, a JSON API |
| [API reference](API.md) | Every exported symbol, in one page |

## Guides

| Guide | Read it when |
|---|---|
| [Streaming](guides/streaming.md) | You stream anything: SSE, NDJSON, tokens, progress, feeds |
| [AI backends](guides/ai-backends.md) | You serve a model: OpenAI-compatible endpoints, proxies, agent events, MCP |
| [Routing](guides/routing.md) | You need the pattern rules, groups, mounts, or why a route panicked |
| [Middleware](guides/middleware.md) | You write or port middleware: ordering, CORS, auth, logging, rate limits |
| [Security](guides/security.md) | You want to know what the defaults stop and what is still your job |
| [Configuration](guides/configuration.md) | You need to change a limit, or understand a derived one |
| [Deployment](guides/deployment.md) | You ship it: Docker, systemd, Kubernetes, nginx, TLS |
| [Testing](guides/testing.md) | You test handlers and streams |
| [Benchmarking](guides/benchmarking.md) | You measure a streaming endpoint with `photon bench` |

## Migrating

[Migration overview](migration/README.md) — a concept map across frameworks,
then a guide for each:
[net/http](migration/from-net-http.md) ·
[chi](migration/from-chi.md) ·
[gin](migration/from-gin.md) ·
[echo](migration/from-echo.md) ·
[Fiber](migration/from-fiber.md) ·
[FastAPI and Express](migration/from-fastapi-and-express.md)

## Project

[Changelog](../CHANGELOG.md) · [Contributing](../CONTRIBUTING.md) ·
[Security policy](../SECURITY.md) · [Benchmarks](../BENCHMARKS.md) ·
[License: Apache-2.0](../LICENSE)
