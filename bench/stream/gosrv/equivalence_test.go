package gosrv_test

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"
)

// TestGoServersEmitIdenticalBytes is the fairness gate for the Go half of the
// streaming audit.
//
// If two servers return different bytes for the same request, every table in the
// report measures the responses rather than the servers, and the whole exercise
// is void. This is therefore run before any benchmark and must not be weakened.
//
// It compares the *body bytes* and the header values that a client can observe,
// and it fails loudly rather than reporting a difference to be investigated
// later: a silently different response would put a wrong number in the report.
func TestGoServersEmitIdenticalBytes(t *testing.T) {
	const events = 5

	// photon first, so it defines the reference. Its frame is the specification:
	// every competitor must match it byte for byte.
	//
	// Paths are relative to the module root because `go test ./gosrv/` runs the
	// binary in that package's directory, not the module's.
	servers := []server{
		{name: "photon", bin: "bin/photon.exe", args: []string{"-flush=coalesced"}},
		{name: "chi", bin: "bin/chi.exe"},
		{name: "gin", bin: "bin/gin.exe"},
		{name: "gin-ctx", bin: "bin/gin.exe", path: "/stream-gin"},
		{name: "echo", bin: "bin/echo.exe"},
		{name: "net/http", bin: "bin/nethttp.exe"},
	}

	var refBody string
	var refName string
	refLen := 0

	for _, s := range servers {
		t.Run(s.name, func(t *testing.T) {
			path := s.path
			if path == "" {
				path = "/stream"
			}

			stop, addr := startServer(t, s.bin, s.args...)
			defer stop()

			body, hdr, err := fetch(addr, path, events)
			if err != nil {
				t.Fatalf("%s: %v", s.name, err)
			}

			// The event count is checked first, because a server that delivered
			// the wrong number of events would be caught here rather than being
			// compared on a byte count that happens to match by coincidence.
			if got := strings.Count(body, "event: token"); got != events {
				t.Errorf("%s delivered %d events, want %d", s.name, got, events)
			}
			if !strings.HasSuffix(body, "\n\n") {
				t.Errorf("%s body does not end with the SSE frame terminator", s.name)
			}
			if ct := hdr.Get("Content-Type"); !strings.HasPrefix(ct, "text/event-stream") {
				t.Errorf("%s Content-Type = %q, want text/event-stream", s.name, ct)
			}
			if v := hdr.Get("X-Accel-Buffering"); v != "no" {
				t.Errorf("%s X-Accel-Buffering = %q, want no", s.name, v)
			}

			if refBody == "" {
				refBody, refName, refLen = body, s.name, len(body)
				t.Logf("reference (%s): %d bytes for %d events", refName, refLen, events)
				return
			}
			if body != refBody {
				t.Errorf("%s body differs from %s:\n  %s: %q\n  %s: %q",
					s.name, refName, s.name, trunc(body), refName, trunc(refBody))
			}
		})
	}
}

// server is one binary under test.
type server struct {
	name string
	bin  string
	args []string
	// path overrides /stream, for a server that exposes more than one variant.
	path string
}

// startServer launches a binary and waits for its READY handshake.
//
// The handshake is the reason this can assert on bytes rather than retrying: a
// server that is not up yet is a connection error, not a slow response, so a
// failure here is a real failure rather than a startup race.
func startServer(t *testing.T, bin string, args ...string) (stop func(), addr string) {
	t.Helper()
	// Resolved against the module root rather than the working directory,
	// because `go test ./gosrv/` runs the binary in gosrv/, not in the module
	// root where the binaries are written.
	bin = filepath.Join("..", bin)
	if _, err := os.Stat(bin); err != nil {
		t.Skipf("binary %s not built; run build.ps1 first", bin)
	}

	cmd := exec.Command(bin, args...)
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatalf("stdout pipe: %v", err)
	}
	// stderr is drained but ignored: a server that logs warnings while starting
	// is normal, and its output is not part of what is being compared.
	cmd.Stderr = io.Discard
	if err := cmd.Start(); err != nil {
		t.Fatalf("start %s: %v", bin, err)
	}

	// The READY line carries the bound address, so no port is guessed.
	// READY is printed only after the listener is bound, so the address is real
	// and no retry is needed. The timeout is a guard against a binary that
	// starts but never reaches its own handshake, which would otherwise hang.
	type readyMsg struct {
		addr string
		err  error
	}
	ch := make(chan readyMsg, 1)
	go func() {
		sc := bufio.NewScanner(stdout)
		for sc.Scan() {
			if _, addr, ok := strings.Cut(sc.Text(), "READY "); ok {
				fields := strings.Fields(addr)
				ch <- readyMsg{addr: fields[len(fields)-1]}
				return
			}
		}
		ch <- readyMsg{err: fmt.Errorf("no READY line from %s", bin)}
	}()

	var bound string
	select {
	case m := <-ch:
		if m.err != nil {
			_ = cmd.Process.Kill()
			t.Fatalf("%v", m.err)
		}
		bound = m.addr
	case <-time.After(15 * time.Second):
		_ = cmd.Process.Kill()
		t.Fatalf("%s did not print READY within 15s", bin)
	}

	return func() {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
	}, bound
}

// fetch issues one request over a raw socket and returns the body and headers.
//
// A hand-written reader rather than net/http's client, for two reasons: the
// response is chunked and has no Content-Length, which net/http's client would
// have to buffer in full before the caller saw a single event; and the driver's
// whole job here is to observe the stream as it arrives.
func fetch(addr, path string, events int) (string, http.Header, error) {
	c, err := net.DialTimeout("tcp", addr, 5*time.Second)
	if err != nil {
		return "", nil, err
	}
	defer c.Close()
	_ = c.SetDeadline(time.Now().Add(30 * time.Second))

	req := "GET " + path + "?n=" + strconv.Itoa(events) + " HTTP/1.1\r\n" +
		"Host: " + addr + "\r\nConnection: keep-alive\r\n\r\n"
	if _, err := c.Write([]byte(req)); err != nil {
		return "", nil, err
	}

	br := bufio.NewReaderSize(c, 64<<10)
	// Status line.
	if _, err := br.ReadString('\n'); err != nil {
		return "", nil, err
	}
	hdr := http.Header{}
	for {
		line, err := br.ReadString('\n')
		if err != nil {
			return "", nil, err
		}
		line = strings.TrimRight(line, "\r\n")
		if line == "" {
			break
		}
		if k, v, ok := strings.Cut(line, ":"); ok {
			hdr.Add(k, strings.TrimSpace(v))
		}
	}

	var body strings.Builder
	for {
		chunk, err := readChunk(br)
		if chunk != "" {
			body.WriteString(chunk)
		}
		if err != nil {
			break
		}
	}
	return body.String(), hdr, nil
}

// readChunk reads one HTTP/1.1 chunk, returning its payload.
func readChunk(br *bufio.Reader) (string, error) {
	line, err := br.ReadString('\n')
	if err != nil {
		return "", err
	}
	line = strings.TrimSpace(line)
	if i := strings.IndexByte(line, ';'); i >= 0 {
		line = line[:i] // chunk extension
	}
	size, err := strconv.ParseInt(line, 16, 64)
	if err != nil {
		return "", err
	}
	if size == 0 {
		return "", io.EOF
	}
	buf := make([]byte, size+2) // payload + CRLF
	if _, err := io.ReadFull(br, buf); err != nil {
		return "", err
	}
	return string(buf[:size]), nil
}

func trunc(s string) string {
	const max = 90
	if len(s) <= max {
		return s
	}
	return s[:max] + "..."
}
