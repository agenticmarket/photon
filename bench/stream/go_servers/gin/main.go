// Command go_gin serves the benchmark stream with Gin.
package main

import (
	"github.com/gin-gonic/gin"

	"streambench/gosrv"
)

func main() {
	gin.SetMode(gin.ReleaseMode)
	g := gin.New()

	// Gin's own handlers return through c.Writer and do not stream, so the
	// benchmark routes are registered as plain http.Handlers on the engine via
	// gin.WrapH. That is the only way to stream from Gin without its serializer
	// buffering the response, and it is what Gin SSE examples do in practice.
	//
	// The consequence is recorded in the report: this measures Gin plus net/http,
	// not Gin's own request pipeline, because for SSE there is almost no pipeline
	// left to measure. Writing c.Writer directly would add Gin's Context
	// allocation to every event, which is a real cost of using Gin for SSE and is
	// therefore also worth measuring - so the report includes that variant rather
	// than quietly picking the flattering one.
	g.Any("/stream", gin.WrapH(gosrv.NewStreamHandler()))
	g.Any("/stream-gin", gin.WrapH(ginNativeStream()))
	g.GET("/mem", gin.WrapH(gosrv.NewMemHandler()))

	gosrv.ListenReady("gin", g)
}
