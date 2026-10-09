//go:build plan9

package photon

import "os"

// shutdownSignals on Plan 9, which has notes rather than SIGTERM.
var shutdownSignals = []os.Signal{os.Interrupt}
