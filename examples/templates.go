// Package examples holds photon's runnable examples. Each subdirectory is a
// standalone program; see README.md.
//
// The package itself exists for one reason: `photon init` scaffolds new
// projects from these programs, and embedding them here means the templates are
// the examples - compiled and tested with the rest of the module - rather than
// string constants that can quietly stop compiling.
package examples

import "embed"

// Templates maps a `photon init -template` name to the example it copies.
var Templates = map[string]string{
	"minimal":      "hello/main.go",
	"api":          "api/main.go",
	"agent":        "agent/main.go",
	"openai-proxy": "openai-proxy/main.go",
}

// FS holds the source of the template programs.
//
//go:embed hello/main.go api/main.go agent/main.go openai-proxy/main.go
var FS embed.FS
