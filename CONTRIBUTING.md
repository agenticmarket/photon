# Contributing to Photon

Thanks for wanting to help. Photon is small on purpose, so the bar for adding
things is high and the bar for fixing things is low. Bug reports, failing tests,
documentation fixes, and measurements are the most valuable contributions.

## Before you start

- **Bugs:** open an issue with a minimal program that reproduces it. A failing
  test is even better.
- **Features:** open an issue first and describe the problem, not the solution.
  Photon's core stays small on purpose: an addition has to be something most
  users need and cannot reasonably build on top. Many good ideas belong in
  your application or in a separate package, and an issue is the cheap way to
  find out.
- **Security issues:** do not open an issue. See [SECURITY.md](SECURITY.md).

## Development

You need Go 1.24 or newer. The race detector also needs a C toolchain with
`CGO_ENABLED=1` (on Windows, MinGW-w64 or LLVM-MinGW).

```bash
make test        # go test ./...
make race        # the race detector, three runs - the gate that matters
make check       # gofmt, go vet, and the docs coverage test
make fuzz        # the routing and SSE framing fuzzers, 30 s each
make bench       # microbenchmarks
make all-modules # also the example backend and the benchmark harness modules
```

No `make`? Every target is one Go command; read the [Makefile](Makefile).

The repository holds four Go modules: the library at the root, and
`examples/openai-backend`, `bench/baseline`, and `bench/stream`, which have
their own dependencies. `go test ./...` from the root covers only the first.

## What a good pull request looks like

- **One change.** A fix and a refactor are two pull requests.
- **A test that fails without the change.** For a bug, the test reproduces it.
  For behaviour, the test pins it. Several tests here were proven by sabotage —
  break the code, watch the test fail — and that is the standard.
- **Measurements for performance claims.** Run the before and after
  interleaved, as described in
  [bench/RESULTS-2026-10.md](bench/RESULTS-2026-10.md), and report medians and
  ranges. A single run on a laptop is not evidence.
- **Documentation updated in the same commit.** Every exported symbol must
  appear in [docs/API.md](docs/API.md) (a test enforces it), and behaviour
  changes go in [CHANGELOG.md](CHANGELOG.md) under *Unreleased*.
- **No new dependencies** in the root module. This is enforced in CI and is
  not negotiable: zero dependencies is part of what Photon promises.

## Code style

- `gofmt` and `go vet` clean.
- Comments explain *why*. The codebase is heavily commented with the reasoning
  behind each decision, and the reviewer will ask for the reason if it is
  missing. A comment that restates the code can go.
- Error messages say what happened and what to do: "MaxStreams is -1: use 0
  for the derived default" rather than "invalid value".
- Security-relevant defaults do not change without discussion; changing one is
  a breaking change.

## Design decisions

Explain significant decisions in the pull request description: what you
chose, what you rejected, and why. If a change reverses an earlier decision,
say what was learned. The reasoning is as useful to future contributors as the
code.

## License

By contributing you agree that your contribution is licensed under the
project's [Apache License 2.0](LICENSE), as described in its section 5.
