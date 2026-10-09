// Command go_echo serves the benchmark stream with Echo.
package main

import (
	"net/http"

	"github.com/labstack/echo/v4"

	"streambench/gosrv"
)

func main() {
	e := echo.New()
	e.HideBanner = true
	e.HidePort = true
	// Echo's error handler would otherwise add a body to any non-2xx response,
	// which the driver's equivalence gate would count as a mismatch.
	e.HTTPErrorHandler = func(err error, c echo.Context) {
		if c.Response().Committed {
			return
		}
		_ = c.NoContent(http.StatusInternalServerError)
	}

	// Echo routes take a HandlerFunc returning an error, so the shared handler
	// is adapted. The response writer is c.Response(), which is what makes the
	// per-event flush reach the socket.
	e.GET("/stream", func(c echo.Context) error {
		gosrv.NewStreamHandler().ServeHTTP(c.Response(), c.Request())
		return nil
	})
	e.GET("/mem", func(c echo.Context) error {
		gosrv.NewMemHandler().ServeHTTP(c.Response(), c.Request())
		return nil
	})

	gosrv.ListenReady("echo", e)
}
