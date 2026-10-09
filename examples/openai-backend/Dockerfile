# syntax=docker/dockerfile:1

# Build from the repository root, not from examples/openai-backend/:
#
#	docker build -f examples/openai-backend/Dockerfile -t photon-sse-backend .
#
# The reason is the `replace github.com/agenticmarket/photon => ../..` in this
# directory's go.mod. A local replace points outside the module, so this
# directory alone is not a self-contained build
# context and the image cannot resolve the dependency at all - it fails with
# "reading /go.mod: no such file or directory", which reads like a broken
# Dockerfile rather than a wrong build context.
#
# The alternative - copying photon into the image as a vendored dependency - is
# worse, because the two copies then drift and the image can be built from a
# photon that is not the one in the repository.

# Build stage. The version is pinned to the major line rather than "latest",
# because an unpinned base is an unreproducible build: the same Dockerfile
# produces a different binary next month, and a difference that appears with no
# code change is the hardest kind to investigate.
FROM golang:1.24-bookworm AS build

WORKDIR /src

# The whole repository, because photon is a local replace and has to be present
# for the module graph to resolve.
COPY . .

# Dependencies first would normally be its own layer for caching. It is not
# separated here, and the reason is worth stating rather than leaving as an
# apparent oversight: this module has no third-party dependencies, so `go mod
# download` has nothing to do, and a layer that exists only to run a no-op is a
# layer that invalidates for no benefit.
#
# There is no go.sum either, and its absence is correct rather than missing -
# with no external modules there is nothing to checksum.
ENV CGO_ENABLED=0

# -trimpath removes local filesystem paths from the binary, so a stack trace shows
# "main.go:210" rather than "C:\Users\...\main.go:210" or "/src/examples/openai-backend/main.go:210".
# That leaks the build machine's directory layout and, occasionally, a username.
#
# -ldflags="-s -w" drops the symbol table and DWARF. Neither is needed to run, and
# together they are about 25% of the binary.
RUN go build -C examples/openai-backend -trimpath -ldflags="-s -w" -o /out/server .

# Runtime stage. distroless/static has no shell, no package manager, and no libc
# - just the binary and the CA certificates. A shell in a production image is
# both attack surface and a way for an attacker who does get in to pivot.
#
# The CA bundle is copied explicitly because distroless does not include one, and
# a server that calls an HTTPS upstream without it fails at connect time with a
# message that does not obviously mean "no certificates".
FROM gcr.io/distroless/static-debian12:nonroot

COPY --from=build /out/server /server
COPY --from=build /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt

# 65532 is the nonroot user in the distroless base. Running as root in a
# container is a poor default even when the container is not privileged: a
# process that does escape the container escapes as root.
USER 65532:65532

# 8080 matches the default PHOTON_ADDR. Exposed for documentation; EXPOSE does
# not publish a port, it only records the intent.
EXPOSE 8080

# OCI labels, so the image is identifiable in a registry and in a scanner's
# output.
LABEL org.opencontainers.image.title="photon-sse-backend" \
      org.opencontainers.image.description="OpenAI-compatible SSE backend built on photon" \
      org.opencontainers.image.source="https://github.com/agenticmarket/photon"

ENTRYPOINT ["/server"]

