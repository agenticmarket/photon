// Command equivcheck asserts that every server in the audit - Go, Node, and
// Python - emits byte-identical SSE frames.
//
// It is the fairness gate for the whole cross-language comparison, and it runs
// before any benchmark. If a Node or Python server emits one byte differently
// from photon, every table in the report measures the responses rather than the
// servers, and the numbers are void.
//
// A raw socket is used rather than net/http's client because the response is
// chunked with no Content-Length: an HTTP client would have to buffer the entire
// stream before returning, which is exactly what this audit must not do.
package main

import (
	"bufio"
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

func main() {
	binDir := flag.String("bin", "../bin", "directory holding the Go server binaries")
	nodeDir := flag.String("node", "../node", "node server directory")
	pyDir := flag.String("python", "../python", "python server directory")
	py := flag.String("python-bin", "", "python interpreter to use (default: the venv in -python)")
	events := flag.Int("events", 5, "events per request")
	flag.Parse()

	const wantBytes = 65 * 5 // one frame is 65 bytes; scaled below

	type target struct {
		name string
		cmd  *exec.Cmd
		// cwd is where the process is launched, so the Node and Python servers
		// resolve their own dependencies relative to their directory.
		cwd string
	}

	targets := []target{
		{name: "photon", cmd: exec.Command(filepath.Join(*binDir, "photon.exe"), "-flush=coalesced")},
		{name: "photon-tight", cmd: exec.Command(filepath.Join(*binDir, "photon.exe"), "-flush=tight")},
		{name: "chi", cmd: exec.Command(filepath.Join(*binDir, "chi.exe"))},
		{name: "gin", cmd: exec.Command(filepath.Join(*binDir, "gin.exe"))},
		{name: "echo", cmd: exec.Command(filepath.Join(*binDir, "echo.exe"))},
		{name: "net/http", cmd: exec.Command(filepath.Join(*binDir, "nethttp.exe"))},
	}

	for _, fw := range []string{"http", "express", "fastify"} {
		targets = append(targets, target{
			name: "node/" + fw,
			cmd:  exec.Command("node", "server.js", fw),
			cwd:  *nodeDir,
		})
	}

	// Absolute, because the child is started with cmd.Dir set to the Python
	// server's own directory, and exec resolves a relative program path against
	// the child's working directory rather than the driver's. Getting this
	// wrong produces "file not found" for a file that demonstrably exists.
	interp := *py
	if interp == "" {
		interp = filepath.Join(*pyDir, ".venv", "Scripts", "python.exe")
	}
	abs, absErr := filepath.Abs(interp)
	if absErr != nil {
		fmt.Fprintf(os.Stderr, "resolving %s: %v\n", interp, absErr)
		os.Exit(1)
	}
	interp = abs
	if _, statErr := os.Stat(interp); statErr != nil {
		fmt.Fprintf(os.Stderr, "python interpreter not found at %s: %v\n", interp, statErr)
		fmt.Fprintln(os.Stderr, "The Python servers cannot be checked. Create the venv first:")
		fmt.Fprintln(os.Stderr, `  python -m venv python\.venv`)
		fmt.Fprintln(os.Stderr, `  python\.venv\Scripts\python -m pip install -r python\requirements.txt`)
		os.Exit(1)
	}
	for _, fw := range []string{"asgi", "starlette", "fastapi"} {
		targets = append(targets, target{
			name: "python/" + fw,
			cmd:  exec.Command(interp, "server.py", fw),
			cwd:  *pyDir,
		})
	}

	_ = wantBytes

	failed := false
	var ref string
	var refName string
	want := 65 * *events

	for _, t := range targets {
		body, hdr, err := check(t.cmd, t.cwd, *events)
		if err != nil {
			fmt.Printf("%-16s ERROR %v\n", t.name, err)
			failed = true
			continue
		}
		got := len(body)
		status := "ok"
		if got != want {
			status = fmt.Sprintf("BYTES want=%d", want)
			failed = true
		} else if ref != "" && body != ref {
			status = "DIFFERS from " + refName
			failed = true
		}
		if ref == "" {
			ref, refName = body, t.name
		}

		// The headers a client can observe are part of the response, so they are
		// compared too: a server that omitted X-Accel-Buffering would behave
		// differently behind nginx while streaming the same bytes.
		hdrNote := ""
		if ct := hdr.Get("Content-Type"); !strings.HasPrefix(ct, "text/event-stream") {
			hdrNote += " [Content-Type=" + ct + "]"
			failed = true
		}
		if hdr.Get("X-Accel-Buffering") != "no" {
			hdrNote += " [X-Accel-Buffering=" + hdr.Get("X-Accel-Buffering") + "]"
			failed = true
		}

		fmt.Printf("%-16s %5d bytes  %s%s\n", t.name, got, status, hdrNote)
		if status != "ok" {
			fmt.Printf("%-16s   %q\n", "", trunc(body))
		}
	}

	if failed {
		fmt.Fprintln(os.Stderr, "\nequivalence FAILED: the tables would not be comparable")
		os.Exit(1)
	}
	fmt.Printf("\nequivalence OK: every server emitted identical %d-byte frames\n", want)
}

// check launches a server, waits for its handshake, and fetches one stream.
func check(cmd *exec.Cmd, cwd string, events int) (string, headerMap, error) {
	if cwd != "" {
		cmd.Dir = cwd
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return "", nil, err
	}
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		return "", nil, err
	}
	defer func() {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
	}()

	addr, err := readReady(stdout)
	if err != nil {
		return "", nil, err
	}
	body, hdr, err := fetch(addr, "/stream", events)
	return body, hdr, err
}

func readReady(r io.Reader) (string, error) {
	ch := make(chan string, 1)
	go func() {
		sc := bufio.NewScanner(r)
		for sc.Scan() {
			if _, addr, ok := strings.Cut(sc.Text(), "READY "); ok {
				f := strings.Fields(addr)
				ch <- f[len(f)-1]
				return
			}
		}
		ch <- ""
	}()
	select {
	case a := <-ch:
		if a == "" {
			return "", fmt.Errorf("no READY handshake")
		}
		return a, nil
	case <-time.After(60 * time.Second):
		return "", fmt.Errorf("no READY within 60s")
	}
}

// headerMap is a minimal header set, avoiding net/http's Header in the driver.
type headerMap map[string]string

func (h headerMap) Get(k string) string { return h[strings.ToLower(k)] }

func fetch(addr, path string, events int) (string, headerMap, error) {
	c, err := net.DialTimeout("tcp", addr, 10*time.Second)
	if err != nil {
		return "", nil, err
	}
	defer c.Close()
	_ = c.SetDeadline(time.Now().Add(60 * time.Second))

	req := "GET " + path + "?n=" + strconv.Itoa(events) + " HTTP/1.1\r\nHost: " + addr +
		"\r\nConnection: close\r\n\r\n"
	if _, err := c.Write([]byte(req)); err != nil {
		return "", nil, err
	}

	br := bufio.NewReaderSize(c, 256<<10)
	if _, err := br.ReadString('\n'); err != nil {
		return "", nil, err
	}
	hdr := headerMap{}
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
			hdr[strings.ToLower(strings.TrimSpace(k))] = strings.TrimSpace(v)
		}
	}

	var body strings.Builder
	for {
		chunk, err := readChunk(br)
		body.WriteString(chunk)
		if err != nil {
			break
		}
	}
	return body.String(), hdr, nil
}

func readChunk(br *bufio.Reader) (string, error) {
	line, err := br.ReadString('\n')
	if err != nil {
		return "", err
	}
	line = strings.TrimSpace(line)
	if i := strings.IndexByte(line, ';'); i >= 0 {
		line = line[:i]
	}
	size, err := strconv.ParseInt(line, 16, 64)
	if err != nil {
		return "", err
	}
	if size == 0 {
		return "", io.EOF
	}
	buf := make([]byte, size+2)
	if _, err := io.ReadFull(br, buf); err != nil {
		return "", err
	}
	return string(buf[:size]), nil
}

func trunc(s string) string {
	const max = 80
	if len(s) <= max {
		return s
	}
	return s[:max] + "..."
}
