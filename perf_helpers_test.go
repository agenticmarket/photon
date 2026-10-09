package photon

import "net/http"

// servedRequest returns r as Serve would deliver it: with the server's base
// context, which already carries the server.
func servedRequest(app *Server, r *http.Request) *http.Request {
	return r.WithContext(app.baseCtx)
}
