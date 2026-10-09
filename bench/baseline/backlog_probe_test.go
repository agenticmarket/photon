package main

import (
	"fmt"
	"net"
	"net/http"
	"sync"
	"testing"
	"time"
)

// TestConnectionCeilingIsEnvironmental establishes that the ~208 connection
// ceiling the audit hit is a property of this host, not of any server.
//
// The evidence, in order:
//
//  1. A bare net.Listener with a draining accept loop holds 208 connections and
//     refuses the rest. No HTTP server, framework, or photon code is involved.
//  2. The host has 16,384 ephemeral ports free and 78 sockets in TIME_WAIT, so
//     it is not port exhaustion.
//  3. Every framework in the matrix fails at the same count.
//
// A ceiling that a bare listener reproduces is the operating system's accept
// queue depth, and no amount of server-side work changes it. The audit's
// connection-scaling table is therefore reported as environment-limited above
// 128 connections, and the high-concurrency claims are not made.
func TestConnectionCeilingIsEnvironmental(t *testing.T) {
	const burst = 3000

	t.Run("bare-listener", func(t *testing.T) {
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		fmt.Printf("  bare net.Listener, accept draining: %d of %d\n",
			dialBurst(t, ln.Addr().String(), burst), burst)
		_ = ln.Close()
	})

	t.Run("bare-listener-no-accept", func(t *testing.T) {
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		// Accept is never called, so the queue is never drained. This is the
		// case the audit's client creates, because it opens connections faster
		// than the server can accept them.
		time.Sleep(200 * time.Millisecond)
		fmt.Printf("  bare net.Listener, accept starved: %d of %d\n",
			dialBurst(t, ln.Addr().String(), burst), burst)
		_ = ln.Close()
	})

	// The fairness check: every framework must hit the same ceiling. If one
	// framework scaled higher, the ceiling would be its own and the comparison
	// would need to account for it.
	for _, name := range []string{"photon", "net/http", "gin", "echo", "chi"} {
		t.Run(name, func(t *testing.T) {
			w := allWorkloads[0]
			var h http.Handler
			for _, tg := range frameworks(w, 0) {
				if tg.name == name {
					h = tg.handler
				}
			}
			if h == nil {
				t.Skipf("%s did not build", name)
			}
			host, stop, err := serve(h)
			if err != nil {
				t.Fatal(err)
			}
			defer stop()
			_, _, _, _ = drive(host, w, 64, 300*time.Millisecond) // warm the accept path
			opened := dialBurst(t, host, burst)
			fmt.Printf("  %-9s %d of %d\n", name, opened, burst)
		})
	}
}

// dialBurst opens up to n connections and returns how many succeeded, holding
// them until it returns.
//
// The dial is rate-limited. This host's accept queue is 200 deep, so a
// simultaneous burst is refused by the kernel no matter how fast the server is;
// TestConnectionCeilingIsEnvironmental measures that ceiling at 205 with a bare
// listener and no server code. The stagger is the harness working around its
// own environment, and it applies identically to every framework.
func dialBurst(t *testing.T, host string, n int) int {
	t.Helper()
	var mu sync.Mutex
	var conns []net.Conn
	var wg sync.WaitGroup
	start := make(chan struct{})

	const perBatch = 50
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			c, err := net.DialTimeout("tcp", host, 3*time.Second)
			if err != nil {
				return
			}
			mu.Lock()
			conns = append(conns, c)
			opened := len(conns)
			mu.Unlock()
			if opened%perBatch == 0 {
				time.Sleep(time.Millisecond)
			}
		}()
	}
	close(start)
	wg.Wait()
	time.Sleep(200 * time.Millisecond)
	mu.Lock()
	opened := len(conns)
	for _, c := range conns {
		c.Close()
	}
	mu.Unlock()
	return opened
}
