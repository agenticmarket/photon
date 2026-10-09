//go:build !plan9

package photon

import (
	"os"
	"syscall"
)

// shutdownSignals are the signals Run treats as a request to shut down:
// Ctrl-C, and the SIGTERM that systemd, Docker, and Kubernetes send. On
// Windows, SIGTERM is delivered for console close, logoff, and shutdown events.
var shutdownSignals = []os.Signal{os.Interrupt, syscall.SIGTERM}
