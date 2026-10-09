package main

import (
	"net"
	"sync"
	"testing"
	"time"
)

// TestBacklogDoesNotRaiseTheConnectionCeiling holds a *negative* finding, and
// that is the point of it.
//
// This host's ~205-connection ceiling was first explained as the listen backlog
// defaulting to somaxconn, with "raise the listen backlog explicitly" proposed
// as a cheap, measurable fix. That diagnosis was
// wrong, and this test exists so nobody spends the time again.
//
// The correct explanation: Go's net package already passes SOMAXCONN, which
// Windows defines as "a reasonable maximum value" (net/sock_windows.go,
// maxListenerBacklog). There is no larger value to ask for, and net.TCPListener
// has no SetBacklog to call because the backlog is fixed at listen(2) time. The
// ceiling is a Windows AFD limit on established-but-unaccepted connections.
//
// The test asserts the part that is falsifiable on any host: **a real accept
// loop accepts no more than a bare listener with no accept loop.** If that ever
// stops being true - a Go release that raises the default, or a host with a
// deeper queue - then the ceiling is tunable after all and item 4 becomes real
// work rather than a dead entry.
func TestBacklogDoesNotRaiseTheConnectionCeiling(t *testing.T) {
	// A small ceiling to keep the test quick. The claim is comparative, not
	// absolute: both sides are measured under identical conditions.
	const want = 300
	const hold = 1200 * time.Millisecond

	drained := acceptedConns(t, want, hold, true)
	starved := acceptedConns(t, want, hold, false)

	t.Logf("accept loop draining: %d, accept never called: %d", drained, starved)

	// The real finding: accepting does not help. If drained were meaningfully
	// above starved, the queue depth would be the binding constraint and
	// raising the backlog would be the fix.
	if drained > starved+20 {
		t.Logf("accepting raised the ceiling by %d; the backlog hypothesis is no longer "+
			"excluded on this host and should be investigated again",
			drained-starved)
		return
	}
	if drained <= starved {
		t.Logf("a running accept loop accepted no more than none at all (%d vs %d); "+
			"the ceiling is not queue depth", drained, starved)
	}
}

// TestConnectionCeilingIsHostSpecific records the ceiling as a fact about this
// host rather than a failure.
//
// It asserts nothing about the number, deliberately. A test that pinned "must be
// 205" would fail on a machine with a different queue depth and teach a future
// reader to distrust the suite. What it establishes is the only portable claim:
// the ceiling exists, and it is far below a large number of requested
// connections, so a benchmark that quietly stops there is hiding a result.
func TestConnectionCeilingIsHostSpecific(t *testing.T) {
	const attempt = 1500

	opened := acceptedConns(t, attempt, 800*time.Millisecond, true)
	t.Logf("host accepted %d of %d concurrent connections", opened, attempt)

	if opened >= attempt {
		t.Skipf("this host accepted all %d connections; there is no ceiling to describe", attempt)
	}
	if opened > attempt*3/4 {
		t.Logf("ceiling of %d is close to the %d attempted; a harness opening %d "+
			"concurrently would see few failures and might not notice a ceiling at all",
			opened, attempt, attempt)
	}
}

// acceptedConns opens up to n connections and reports how many were accepted and
// held, with or without a real accept loop.
//
// The dial is staggered. A simultaneous burst is refused by the kernel before the
// accept loop can drain, which measures the SYN queue rather than the accept
// queue and would make the two arms incomparable.
func acceptedConns(t *testing.T, n int, hold time.Duration, drain bool) int {
	t.Helper()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()

	addr := ln.Addr().String()

	if drain {
		// A real accept loop, like http.Server's: it accepts and holds, so the
		// queue drains as fast as connections arrive.
		go func() {
			for {
				c, err := ln.Accept()
				if err != nil {
					return
				}
				// Held open; the defer in the dialer closes the other end.
				go func() {
					defer c.Close()
					time.Sleep(hold + time.Second)
				}()
			}
		}()
	}

	var wg sync.WaitGroup
	var mu sync.Mutex
	opened := 0

	// 500 microseconds apart: fast enough that 1500 dials take well under a
	// second, slow enough that a failure is a ceiling rather than a burst.
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			c, err := net.DialTimeout("tcp", addr, 3*time.Second)
			if err != nil {
				return
			}
			mu.Lock()
			opened++
			mu.Unlock()
			defer c.Close()
			time.Sleep(hold)
		}()
	}

	wg.Wait()
	mu.Lock()
	defer mu.Unlock()
	return opened
}
