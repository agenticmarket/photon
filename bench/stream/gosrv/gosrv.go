// Package gosrv holds the server plumbing shared by the non-photon Go servers:
// the listener handshake and the shutdown path.
package gosrv

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"
)

// ListenReady starts h on a loopback port and prints the handshake the driver
// waits for.
//
// The handshake exists because the obvious alternative - sleep, then dial -
// measures the wrong thing on a loaded machine: a server that is not up yet is
// retried, and the retries land in the throughput figure as if the server were
// slow. Port 0 plus a printed address removes the guess entirely.
func ListenReady(name string, h http.Handler) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		fmt.Fprintf(os.Stderr, "%s: listen: %v\n", name, err)
		os.Exit(1)
	}

	srv := &http.Server{
		Handler: h,
		// Slow-consumer protection is deliberately not set here. The audit's
		// point is how each server behaves when a client does not drain, and a
		// write deadline would be one framework's answer being imposed on all of
		// them.
		ReadHeaderTimeout: 10 * time.Second,
	}

	go func() {
		if err := srv.Serve(ln); err != nil && err != http.ErrServerClosed {
			fmt.Fprintf(os.Stderr, "%s: serve: %v\n", name, err)
		}
	}()

	// Printed on stdout and flushed: the driver blocks on exactly this line, so
	// a buffering stdout would deadlock the run.
	fmt.Printf("READY %s %s\n", name, ln.Addr().String())
	os.Stdout.Sync()

	// Shut down cleanly on the driver's signal, so a killed run does not leave
	// listeners behind and perturb the next configuration.
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt, syscall.SIGTERM)
	<-sig

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	_ = srv.Shutdown(ctx)
}
