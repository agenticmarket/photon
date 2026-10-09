package main

import (
	"fmt"
	"runtime"
	"runtime/debug"
	"strings"
)

// version reports the release this binary was built from.
//
// `go install github.com/agenticmarket/photon/cmd/photon@v0.1.0` records
// v0.1.0 in the binary's build info, so no -ldflags step is needed. A binary
// built from a checkout reports "(devel)", plus the commit when the VCS
// information is available.
func version() string {
	info, ok := debug.ReadBuildInfo()
	if !ok {
		return "(unknown)"
	}
	v := info.Main.Version
	if v != "" && v != "(devel)" {
		return v
	}
	var rev, dirty string
	for _, s := range info.Settings {
		switch s.Key {
		case "vcs.revision":
			rev = s.Value
		case "vcs.modified":
			if s.Value == "true" {
				dirty = "-dirty"
			}
		}
	}
	if len(rev) > 12 {
		rev = rev[:12]
	}
	if rev != "" {
		return "(devel) " + rev + dirty
	}
	return "(devel)"
}

// releaseVersion returns the binary's version when it is a tagged release,
// and "" for development builds and pseudo-versions.
func releaseVersion() string {
	if v := version(); isRelease(v) {
		return v
	}
	return ""
}

// isRelease reports whether v is a tag someone can `go get`: "v0.1.0" or
// "v0.2.0-rc.1", but not a pseudo-version ("v0.1.1-0.2026...-abcdef"), a
// modified build ("v0.1.0+dirty"), or "(devel)".
func isRelease(v string) bool {
	return strings.HasPrefix(v, "v") && strings.Count(v, "-") < 2 && !strings.Contains(v, "+")
}

func runVersion(args []string) error {
	if len(args) > 0 {
		return fmt.Errorf("version takes no arguments")
	}
	fmt.Printf("photon %s %s/%s %s\n", version(), runtime.GOOS, runtime.GOARCH, runtime.Version())
	return nil
}
