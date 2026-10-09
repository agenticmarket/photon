// Command photon is the Photon command-line tool.
//
//	photon init my-agent -template agent   scaffold a new service
//	photon serve                           run the demo streaming service on :8484
//	photon bench http://localhost:8484/agent/stream
//	                                       measure any SSE endpoint: time to first event,
//	                                       gaps between events, events per second
//	photon version                         print the installed release
//
// Install it with:
//
//	go install github.com/agenticmarket/photon/cmd/photon@latest
package main

import (
	"fmt"
	"os"
)

const usage = `photon - tools for the Photon streaming server

Usage:
  photon <command> [arguments]

Commands:
  init     Scaffold a new service from a template
  serve    Run the demo AI streaming service (dashboard, SSE, OpenAI-compatible API)
  bench    Benchmark any SSE endpoint: time to first event, event gaps, throughput
  version  Print the installed version

Examples:
  photon init my-agent -template agent
  photon serve -port 8484
  photon bench -c 50 -n 500 http://localhost:8484/agent/stream?burst=true

Run "photon <command> -h" for a command's flags.
`

func main() {
	if len(os.Args) < 2 {
		fmt.Fprint(os.Stderr, usage)
		os.Exit(2)
	}
	var err error
	switch cmd, args := os.Args[1], os.Args[2:]; cmd {
	case "init":
		err = runInit(args)
	case "serve":
		err = runServe(args)
	case "bench":
		err = runBench(args)
	case "version", "-version", "--version":
		err = runVersion(args)
	case "help", "-h", "-help", "--help":
		fmt.Print(usage)
		return
	default:
		fmt.Fprintf(os.Stderr, "photon: unknown command %q\n\n%s", cmd, usage)
		os.Exit(2)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "photon:", err)
		os.Exit(1)
	}
}
