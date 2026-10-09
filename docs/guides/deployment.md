# Deployment

A photon service is one static binary with no runtime dependencies. This guide
covers building it, what `app.Run` does on shutdown, running it in Docker,
under systemd, and on Kubernetes, configuring the proxies in front of it so
server-sent events arrive token by token, and terminating TLS.

- [Build a single binary](#build-a-single-binary)
- [Graceful shutdown with Run](#graceful-shutdown-with-run)
- [Writing your own lifecycle](#writing-your-own-lifecycle)
- [Docker](#docker)
- [systemd](#systemd)
- [Kubernetes](#kubernetes)
- [Reverse proxies for SSE](#reverse-proxies-for-sse)
- [TLS](#tls)
- [HTTP/2](#http2)

## Build a single binary

```sh
CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o server ./cmd/server
```

- `CGO_ENABLED=0` produces a statically linked binary that runs on a `scratch`
  or distroless image, with Go's own DNS resolver. Photon needs no cgo.
- `-trimpath` removes your machine's file paths from the binary, which makes
  builds reproducible and keeps home directories out of stack traces.
- `-ldflags="-s -w"` drops the symbol table and DWARF debug data. The binary
  is smaller; panics still print full stack traces.

Cross-compile by setting `GOOS` and `GOARCH`, for example
`GOOS=linux GOARCH=arm64`.

## Graceful shutdown with Run

```go
if err := app.Run(":8080"); err != nil {
	log.Fatal(err)
}
```

`Run` is the whole lifecycle in one call:

1. **It binds the address first.** A port that is already in use is returned
   as an error immediately (`photon: listen on :8080: ...`), before anything
   is served.
2. **It starts serving.** The router is frozen, and if `MaxConnections` or
   `MaxPerIP` is set, the listener is wrapped so excess connections are refused
   before a byte is read.
3. **It waits for `SIGINT` or `SIGTERM`.** These are what Ctrl-C, systemd,
   Docker, and Kubernetes send.
4. **On the first signal it begins a graceful shutdown.** Default signal
   handling is restored first, so a second Ctrl-C kills the process instead of
   waiting.
5. **Streams are told.** Every `Stream.Done()` channel closes, so a long-lived
   stream can send a final event (a resume id, a "reconnect" hint) and return.
   The stream is still writable at that point.
6. **New connections stop; in-flight work continues.** The listener closes,
   idle keep-alive connections close, and `Run` waits for active requests and
   streams to finish, up to `ShutdownTimeout` (25 seconds by default).
7. **If the deadline passes, remaining connections are closed** and `Run`
   returns `context.DeadlineExceeded`. A deadline that expired did not give the
   work time to finish, and saying so is more useful than pretending it did.

`Run` returns `nil` after a clean shutdown. With a logger configured it logs
`photon: listening` and `photon: shutting down`; with the default logger it
logs nothing unless something goes wrong.

A finite stream (one answer) normally finishes inside the shutdown window. A
stream that never ends on its own (a notification feed) should watch
`s.Done()`; otherwise it holds shutdown open for the full timeout and is then
cut off. See [streaming](../guides/streaming.md#graceful-shutdown).

## Writing your own lifecycle

`Run` covers most deployments. Write your own with `Serve` and `Shutdown` when
you need something between the signal and the shutdown, such as failing a
readiness check and waiting for load balancers to notice, or when you serve TLS
with your own `tls.Config` ([below](#tls)).

```go
package main

import (
	"context"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/agenticmarket/photon"
)

func main() {
	var draining atomic.Bool

	app := photon.New()
	app.GET("/healthz", func(w http.ResponseWriter, r *http.Request) {
		_ = photon.Text(w, http.StatusOK, "ok\n") // alive, even while draining
	})
	app.GET("/readyz", func(w http.ResponseWriter, r *http.Request) {
		if draining.Load() {
			_ = photon.Text(w, http.StatusServiceUnavailable, "draining\n")
			return
		}
		_ = photon.Text(w, http.StatusOK, "ready\n")
	})

	ln, err := net.Listen("tcp", ":8080")
	if err != nil {
		log.Fatal(err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	errc := make(chan error, 1)
	go func() { errc <- app.Serve(ln) }()

	select {
	case err := <-errc:
		log.Fatalf("serve: %v", err) // Serve returned before any signal: it failed
	case <-ctx.Done():
	}
	stop() // a second signal now kills the process

	// Fail readiness, and keep serving while load balancers notice.
	draining.Store(true)
	time.Sleep(5 * time.Second)

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if err := app.Shutdown(shutdownCtx); err != nil {
		log.Printf("shutdown: %v", err) // the deadline passed; connections were closed
	}
	if err := <-errc; err != nil {
		log.Printf("serve: %v", err)
	}
}
```

Bind the listener and start `Serve` before waiting for the signal, as here. The
tempting alternative, starting a goroutine that calls `Shutdown` and then
calling `Listen`, can run the shutdown before the socket is bound, and the
server then stops immediately on startup.

## Docker

```dockerfile
# syntax=docker/dockerfile:1

FROM golang:1.24 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/server ./cmd/server

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/server /server
USER nonroot:nonroot
EXPOSE 8080
ENTRYPOINT ["/server"]
```

Run it with a memory limit and `GOMEMLIMIT` at about 90% of that limit:

```sh
docker run --rm -p 8080:8080 \
  --memory=512m -e GOMEMLIMIT=460MiB \
  --read-only --stop-timeout=30 \
  example/api:1.0.0
```

Why each piece matters:

- **`GOMEMLIMIT`.** Without it the Go garbage collector does not know about the
  container limit and can let the heap grow until the kernel kills the
  process. With it, the collector works harder as memory approaches the limit.
  The 10% headroom covers memory the Go runtime does not count and the fact
  that the limit is soft. Photon also uses `GOMEMLIMIT` as its memory budget
  and derives `MaxStreams` from it (460 MiB gives 460 streams); see
  [configuration](configuration.md#derived-values-memory-budget-and-stream-limits).
- **Distroless `nonroot`.** No shell, no package manager, and a non-root user
  (uid 65532). It includes CA certificates, which you need to call model
  providers over HTTPS.
- **Exec-form `ENTRYPOINT`.** The binary runs as PID 1 and receives `SIGTERM`
  directly. A shell-form entrypoint would put a shell in between that does not
  forward signals. `app.Run` installs a `SIGTERM` handler; a program that does
  not handle the signal itself is not stopped by it when it runs as PID 1.
- **`--stop-timeout=30`.** `docker stop` waits 10 seconds by default and then
  kills the process, which is shorter than photon's 25-second shutdown. Give it
  more than `ShutdownTimeout`. In Compose, the setting is `stop_grace_period`.

For a `scratch` image, copy the CA certificates yourself and pick a numeric
user:

```dockerfile
FROM scratch
COPY --from=build /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/
COPY --from=build /out/server /server
USER 65532:65532
ENTRYPOINT ["/server"]
```

`scratch` has no time zone database either. If your program uses
`time.LoadLocation`, add `import _ "time/tzdata"` to embed one in the binary.

## systemd

```ini
# /etc/systemd/system/api.service
[Unit]
Description=Photon API
After=network-online.target
Wants=network-online.target

[Service]
ExecStart=/usr/local/bin/server
Environment=PORT=8080
Environment=GOMEMLIMIT=460MiB
MemoryMax=512M
Restart=on-failure
RestartSec=2s

# Run handles SIGTERM with a graceful shutdown of up to 25 s.
# Give it longer than that before systemd sends SIGKILL.
KillSignal=SIGTERM
TimeoutStopSec=35s

DynamicUser=yes
NoNewPrivileges=yes
ProtectSystem=strict
ProtectHome=yes
PrivateTmp=yes
PrivateDevices=yes
ProtectKernelTunables=yes
ProtectKernelModules=yes
ProtectControlGroups=yes
RestrictAddressFamilies=AF_INET AF_INET6 AF_UNIX
# To bind port 80 or 443 directly without root:
# AmbientCapabilities=CAP_NET_BIND_SERVICE

[Install]
WantedBy=multi-user.target
```

`MemoryMax=` sets `memory.max` on the service's cgroup. Photon finds it without
configuration: it reads its own cgroup from `/proc/self/cgroup` and checks
`memory.max` at that level and every level above it, so a limit set by systemd
several levels below the root is found as reliably as a container's. Set
`GOMEMLIMIT` as well, because photon's detection only sizes photon's stream
limit; the garbage collector reads `GOMEMLIMIT` and nothing else.

Photon reads `memory.max` from cgroup v2 only. On a host still on cgroup v1 it
falls back to the machine's total memory; set `GOMEMLIMIT` there.

The program reads `PORT` itself; photon reads no environment variables. See
[configuration](configuration.md#environment-variables).

## Kubernetes

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: api
spec:
  replicas: 3
  selector:
    matchLabels: {app: api}
  template:
    metadata:
      labels: {app: api}
    spec:
      terminationGracePeriodSeconds: 40
      containers:
        - name: api
          image: registry.example.com/api:1.0.0
          ports:
            - containerPort: 8080
          env:
            - name: PORT
              value: "8080"
            - name: GOMEMLIMIT
              value: "460MiB"   # about 90% of the memory limit below
          resources:
            requests: {cpu: 250m, memory: 512Mi}
            limits: {memory: 512Mi}
          readinessProbe:
            httpGet: {path: /readyz, port: 8080}
            periodSeconds: 5
            failureThreshold: 2
          livenessProbe:
            httpGet: {path: /healthz, port: 8080}
            periodSeconds: 10
            failureThreshold: 3
          lifecycle:
            preStop:
              sleep: {seconds: 5}   # Kubernetes 1.30+; see below for older clusters
          securityContext:
            runAsNonRoot: true
            allowPrivilegeEscalation: false
            readOnlyRootFilesystem: true
            capabilities: {drop: ["ALL"]}
```

### Liveness and readiness

They answer different questions, and confusing them causes outages.

- **Liveness (`/healthz`)**: is this process working? Check nothing but the
  process itself. A liveness probe that checks a database or the model
  provider gets every pod restarted during that dependency's outage, turning a
  degraded service into a dead one. A draining server is still alive.
- **Readiness (`/readyz`)**: should traffic be sent here? Fail it while the
  instance cannot serve: during startup before configuration is loaded, or
  while draining if you implement that ([above](#writing-your-own-lifecycle)).
  Do not fail it because the model provider is down; every pod would go
  unready at once and the Service would have no endpoints. Return errors to
  clients instead.

Kubernetes probes use `GET`, which photon routes normally. If another health
checker in your path uses `HEAD`, register `HEAD` on the health routes, since
photon does not derive it from `GET`.

### The shutdown timeline

When a pod is deleted, two things start at once: Kubernetes begins removing it
from Service endpoints, and it runs the `preStop` hook. Endpoint removal
reaches kube-proxy, ingress controllers, and cloud load balancers over the next
few seconds, and until it does they keep sending requests to the pod.

1. `preStop` sleeps 5 seconds. Photon keeps serving, so requests that arrive
   through stale endpoints still succeed.
2. Kubernetes sends `SIGTERM`. `app.Run` begins its graceful shutdown and waits
   up to `ShutdownTimeout`: 25 seconds by default.
3. When `terminationGracePeriodSeconds` runs out, counted from the start of
   step 1, Kubernetes sends `SIGKILL`.

So `preStop` + `ShutdownTimeout` + a little time to exit must fit inside
`terminationGracePeriodSeconds`. With the default 30 seconds, a 5-second
`preStop` and photon's 25-second timeout leave no margin, which is why the
manifest sets 40. Alternatively keep 30 and use
`photon.ShutdownTimeout(20*time.Second)`.

Photon's 25-second default was chosen to fit inside the default 30-second grace
period when there is no `preStop` delay.

The `sleep` action needs Kubernetes 1.30 or later. On older clusters an
`exec` hook running `sleep` fails on distroless and `scratch` images, which
have no `sleep` binary; put the delay in your program instead, as in
[Writing your own lifecycle](#writing-your-own-lifecycle).

Streams longer than the shutdown window are cut when the pod stops. Use
`s.Done()` to send a final event, and support `Last-Event-ID` so clients
resume on another pod; see [streaming](../guides/streaming.md#resuming-with-last-event-id).

### Ingress

With ingress-nginx, raise the timeouts for streaming routes and turn off
buffering:

```yaml
metadata:
  annotations:
    nginx.ingress.kubernetes.io/proxy-read-timeout: "3600"
    nginx.ingress.kubernetes.io/proxy-send-timeout: "3600"
    nginx.ingress.kubernetes.io/proxy-buffering: "off"
```

## Reverse proxies for SSE

Whatever sits in front of photon, a streaming response needs four things from
it:

1. **No response buffering.** A buffering proxy holds the tokens and delivers
   them in one lump when the stream ends, which looks to the user like a hang.
2. **Idle and read timeouts longer than the gaps in the stream.** Photon sends
   a heartbeat comment after 15 seconds of silence, so a started stream is
   never idle longer than that. Before the first event there are no heartbeats
   unless the handler calls `s.StartHeartbeats()` or `s.Flush()`, so the
   timeout must also exceed your slowest time to first token.
3. **No compression of `text/event-stream`**, unless the proxy flushes after
   every chunk. Photon sends `Cache-Control: no-cache, no-transform`, which
   asks intermediaries not to transform the response.
4. **Cancellation when the client leaves.** The proxy should close its
   connection to photon when the client disconnects, so `r.Context()` is
   cancelled and your model call stops. nginx and most load balancers do this
   by default.

Every photon stream also sends `X-Accel-Buffering: no`, which tells nginx not
to buffer that response even when buffering is on.

### nginx

```nginx
upstream photon {
    server 127.0.0.1:8080;
    keepalive 32;
}

server {
    listen 443 ssl;
    http2 on;                        # nginx 1.25.1+; older: "listen 443 ssl http2;"
    server_name api.example.com;

    ssl_certificate     /etc/letsencrypt/live/api.example.com/fullchain.pem;
    ssl_certificate_key /etc/letsencrypt/live/api.example.com/privkey.pem;

    location / {
        proxy_pass http://photon;
        proxy_http_version 1.1;          # needed for keep-alive to the upstream
        proxy_set_header Connection "";  # and do not forward "Connection: close"
        proxy_set_header Host $host;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;

        proxy_buffering off;             # photon's X-Accel-Buffering: no does this per response
        proxy_read_timeout 300s;         # well above the 15 s heartbeat and your slowest first token
        proxy_send_timeout 300s;
        client_max_body_size 1m;         # match MaxRequestBodyBytes
    }
}
```

`proxy_read_timeout` is the longest gap nginx tolerates between two reads from
photon; its default of 60 seconds is enough once heartbeats flow, but not for a
model that thinks longer than that before its first event.
`$proxy_add_x_forwarded_for` appends the address nginx saw, which is what the
[trusted-proxy code](security.md#trusted-proxies-and-the-client-address)
expects.

### Caddy

```caddyfile
api.example.com {
    reverse_proxy 127.0.0.1:8080 {
        flush_interval -1
    }
}
```

Caddy flushes `text/event-stream` responses immediately on its own, so SSE
works without `flush_interval`. Setting it to `-1` forces immediate flushing
for other streaming formats such as NDJSON. Caddy's documentation notes that
in this low-latency mode it does not cancel the request to the backend when the
client disconnects early, so photon does not see the client leave; prefer
leaving it unset for SSE routes.

### Cloudflare

Cloudflare's proxy waits up to 100 seconds for your origin to respond and
answers 524 if it does not; Enterprise plans can raise this. Heartbeats keep a
started stream alive. A model that can think for longer than that before its
first token needs `s.StartHeartbeats()` (or an early `s.Flush()`) so the
response starts in time. Cloudflare passes `text/event-stream` responses
through as they are produced.

### AWS ALB and other load balancers

- **AWS Application Load Balancer**: the idle timeout defaults to 60 seconds.
  Heartbeats every 15 seconds keep streams open. Set photon's `IdleTimeout`
  above the ALB's (for example 75 seconds), so photon never closes a
  keep-alive connection the ALB is about to reuse; that race shows up as
  sporadic 502s.
- **Google Cloud Application Load Balancer**: the backend service timeout
  (30 seconds by default) limits the whole response, not idle time, so
  heartbeats do not help. Raise it above your longest stream.
- **Others**: find the idle (or read) timeout and the response timeout, and
  make sure neither is shorter than your longest gap or your longest stream.

Behind any proxy, `photon.ClientIP` returns the proxy's address and `MaxPerIP`
counts the proxy's connections. See
[trusted proxies](security.md#trusted-proxies-and-the-client-address), and do
not set `MaxPerIP` there.

## TLS

There are three ways to serve HTTPS.

**Terminate at a proxy or load balancer.** This is the usual choice. The proxy
handles certificates, renewal, HTTP/2 to browsers, and HSTS, and photon serves
plain HTTP on a private network.

**`ListenTLS`** loads a certificate and key once and serves HTTPS with TLS 1.2
as the minimum, HTTP/2 negotiated automatically, and no 0-RTT:

```go
go func() { errc <- app.ListenTLS(":8443", "/etc/api/tls.crt", "/etc/api/tls.key") }()
```

It blocks like `Listen` and does not handle signals, so pair it with your own
lifecycle as shown [above](#writing-your-own-lifecycle). It does not reload the
certificate; a renewed certificate needs a restart.

**`Serve` with your own `tls.Config`** for certificate reloading or ACME. This
program reloads its certificate on `SIGHUP` and shuts down gracefully on
`SIGTERM`:

```go
package main

import (
	"context"
	"crypto/tls"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"

	"github.com/agenticmarket/photon"
)

// certStore holds the current certificate, so a renewed one can be swapped in
// without a restart.
type certStore struct {
	certFile, keyFile string
	cert              atomic.Pointer[tls.Certificate]
}

func (c *certStore) load() error {
	cert, err := tls.LoadX509KeyPair(c.certFile, c.keyFile)
	if err != nil {
		return err
	}
	c.cert.Store(&cert)
	return nil
}

func (c *certStore) get(*tls.ClientHelloInfo) (*tls.Certificate, error) {
	return c.cert.Load(), nil
}

func main() {
	certs := &certStore{certFile: "/etc/api/tls.crt", keyFile: "/etc/api/tls.key"}
	if err := certs.load(); err != nil {
		log.Fatal(err)
	}

	app := photon.New()
	app.GET("/healthz", func(w http.ResponseWriter, r *http.Request) {
		_ = photon.Text(w, http.StatusOK, "ok\n")
	})

	cfg := &tls.Config{
		MinVersion:     tls.VersionTLS12,
		NextProtos:     []string{"h2", "http/1.1"}, // without "h2", every client uses HTTP/1.1
		GetCertificate: certs.get,
	}
	ln, err := net.Listen("tcp", ":8443")
	if err != nil {
		log.Fatal(err)
	}

	// SIGHUP reloads the certificate, keeping the old one if the new one is bad.
	hup := make(chan os.Signal, 1)
	signal.Notify(hup, syscall.SIGHUP)
	go func() {
		for range hup {
			if err := certs.load(); err != nil {
				log.Printf("certificate reload failed, keeping the old one: %v", err)
			}
		}
	}()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	errc := make(chan error, 1)
	go func() { errc <- app.Serve(tls.NewListener(ln, cfg)) }()

	select {
	case err := <-errc:
		log.Fatalf("serve: %v", err)
	case <-ctx.Done():
	}
	stop()

	shutdownCtx, cancel := context.WithTimeout(context.Background(), photon.DefaultShutdownTimeout)
	defer cancel()
	if err := app.Shutdown(shutdownCtx); err != nil {
		log.Printf("shutdown: %v", err)
	}
	<-errc
}
```

For automatic certificates from Let's Encrypt, `golang.org/x/crypto/acme/autocert`
produces a ready `tls.Config`. It is a dependency of your program, not of
photon:

```go
m := &autocert.Manager{
	Prompt:     autocert.AcceptTOS,
	HostPolicy: autocert.HostWhitelist("api.example.com"),
	Cache:      autocert.DirCache("/var/lib/api/autocert"),
}
cfg := m.TLSConfig() // includes "h2" and the ACME challenge protocol
cfg.MinVersion = tls.VersionTLS12

ln, err := net.Listen("tcp", ":443")
if err != nil {
	log.Fatal(err)
}
go func() { errc <- app.Serve(tls.NewListener(ln, cfg)) }()
```

When you build the `tls.Config` yourself, you own its settings: keep
`MinVersion` at TLS 1.2 or higher and list `"h2"` in `NextProtos`.

## HTTP/2

Serve browsers over HTTP/2. Over HTTP/1.1, a browser allows only six
connections per host, shared by all tabs, and each open `EventSource` holds
one. A user with a seventh tab open has a stream, and every other request to
your API, waiting for a free connection. HTTP/2 multiplexes many streams over
one connection, so the limit does not apply.

- Photon serves HTTP/2 over TLS only, through `ListenTLS` or a `tls.Config`
  that lists `"h2"`. It does not serve cleartext HTTP/2 (h2c).
- Behind a proxy, what matters is the browser-to-proxy connection. Enable
  HTTP/2 there (`http2 on;` in nginx); the proxy-to-photon leg can stay
  HTTP/1.1.
- SSE works the same over HTTP/2. Photon leaves out the `Connection` header,
  which HTTP/2 forbids.
- With HTTP/2, `MaxConnections` counts connections, not requests; `MaxStreams`
  and `MaxConcurrentRequests` still count streams and requests.

## See also

- [Configuration](configuration.md): limits, memory budget, and `ShutdownTimeout`
- [Security](security.md): the hardening checklist and trusted proxies
- [Streaming](../guides/streaming.md): heartbeats, `Done`, and resuming streams
- [Building AI backends](./ai-backends.md)
- [Getting started](../getting-started.md)
