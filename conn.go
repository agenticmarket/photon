package photon

import (
	"net"
	"net/http"
	"runtime"
	"time"
)

// limitListener wraps a listener so connections are admitted or refused before
// net/http reads a single request byte.
//
// The order matters. A flood that is refused here never allocates a read
// buffer, never becomes a goroutine, and never reaches the parser. Refusing
// after the request is read would already have spent the memory the limit
// exists to protect.
//
// An admitted connection is returned exactly as the inner listener produced
// it - never wrapped. net/http recognises a TLS connection by its type (and,
// in newer releases, by its ConnectionState method), and HTTP/2, r.TLS, and the
// TLS handshake timeout all depend on that. An earlier version wrapped every
// admitted connection so it could release the slot on Close, which hid the
// *tls.Conn: a client that negotiated h2 was then handed to the HTTP/1 parser.
// The slot is released from net/http's connection-state hook instead.
type limitListener struct {
	net.Listener
	srv *Server
}

func (l *limitListener) Accept() (net.Conn, error) {
	for {
		c, err := l.Listener.Accept()
		if err != nil {
			return nil, err
		}
		if ok, reason := l.srv.admit(c); !ok {
			_ = c.Close()
			// Rate-limited: admission control is working as intended under a
			// flood, and one log line per refused connection would turn a
			// refused flood into a disk-filling one.
			if log, skipped := l.srv.refusals.allow(time.Second); log {
				l.srv.logger.Warn("connection refused", "reason", reason, "suppressed", skipped)
			}
			// Loop rather than return: returning the error would make
			// http.Server.Serve shut the whole listener down, turning a
			// momentary overload into a server that refuses every
			// connection from then on.
			continue
		}
		l.srv.admitted.Store(c, struct{}{})
		return c, nil
	}
}

// connState is net/http's connection-state hook. It releases a connection's
// admission slot when the connection closes or is hijacked - exactly once,
// because LoadAndDelete hands the slot to a single caller even if net/http
// reports the transition from two goroutines (the serving goroutine and the
// idle sweep inside Shutdown can both close a connection).
func (s *Server) connState(c net.Conn, state http.ConnState) {
	switch state {
	// A hijacked connection - a WebSocket upgrade - leaves net/http's
	// bookkeeping at this point, and nothing reports when it later closes,
	// so its slot is released here. MaxConnections and MaxPerIP therefore do
	// not bound upgraded connections; bound those in the upgrade handler.
	case http.StateClosed, http.StateHijacked:
		if _, ok := s.admitted.LoadAndDelete(c); ok {
			s.release(c)
		}
	}
}

// admit decides whether a connection may proceed, and records it so the count
// is released when the connection closes.
func (s *Server) admit(c net.Conn) (bool, string) {
	ip := peerIP(c)

	s.connMu.Lock()
	defer s.connMu.Unlock()

	if s.limits.MaxConnections > 0 && s.conns >= s.limits.MaxConnections {
		return false, "MaxConnections"
	}
	if s.limits.MaxPerIP > 0 {
		if s.perIP == nil {
			s.perIP = make(map[string]int)
		}
		if s.perIP[ip] >= s.limits.MaxPerIP {
			return false, "MaxPerIP"
		}
		s.perIP[ip]++
	}
	s.conns++
	return true, ""
}

// release returns a connection's admission slot.
func (s *Server) release(c net.Conn) {
	ip := peerIP(c)

	s.connMu.Lock()
	defer s.connMu.Unlock()
	if s.conns > 0 {
		s.conns--
	}
	if s.limits.MaxPerIP > 0 && s.perIP != nil {
		if s.perIP[ip] > 1 {
			s.perIP[ip]--
		} else {
			delete(s.perIP, ip)
		}
	}
}

// peerIP returns the connection's remote address as a string key.
//
// It is the socket peer, never a forwarded header. A header is set by the
// client, so a limit keyed on one could be bypassed by inventing an address,
// which is the whole point of the limit.
func peerIP(c net.Conn) string {
	if a, ok := c.RemoteAddr().(*net.TCPAddr); ok {
		return a.IP.String()
	}
	return c.RemoteAddr().String()
}

// limitListener applies the admission limits to ln when any are set.
func (s *Server) limitListener(ln net.Listener) net.Listener {
	return &limitListener{Listener: ln, srv: s}
}

// stack captures the current goroutine's stack for a recovered panic. 8 KB
// holds the frames that matter - the handler and its callers - without letting
// a deep recursion allocate a huge trace per panic.
func stack() []byte {
	buf := make([]byte, 8<<10)
	n := runtime.Stack(buf, false)
	return buf[:n]
}
