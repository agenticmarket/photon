package main

import (
	"bytes"
	"errors"
	"flag"
	"fmt"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"runtime/debug"
	"sort"
	"strings"

	"github.com/agenticmarket/photon/examples"
)

// photonModule is the module a scaffolded project depends on.
const photonModule = "github.com/agenticmarket/photon"

func runInit(args []string) error {
	flags := flag.NewFlagSet("init", flag.ContinueOnError)
	tmpl := flags.String("template", "agent", "template: "+strings.Join(templateNames(), ", "))
	module := flags.String("module", "", "module path for go.mod (default: the directory name)")
	local := flags.String("photon", "", "path to a local photon checkout to use via a replace directive")
	flags.Usage = func() {
		fmt.Fprintf(flags.Output(), "Usage: photon init <directory> [flags]\n\n")
		flags.PrintDefaults()
	}
	// Accept the directory before or after the flags.
	var dir string
	if len(args) > 0 && !strings.HasPrefix(args[0], "-") {
		dir, args = args[0], args[1:]
	}
	if err := flags.Parse(args); err != nil {
		return err
	}
	if dir == "" {
		dir = flags.Arg(0)
	}
	if dir == "" {
		flags.Usage()
		return errors.New("init needs a directory name")
	}
	src, ok := examples.Templates[*tmpl]
	if !ok {
		return fmt.Errorf("unknown template %q; choose one of: %s", *tmpl, strings.Join(templateNames(), ", "))
	}
	if *module == "" {
		*module = filepath.Base(filepath.Clean(dir))
	}
	if !validModulePath(*module) {
		return fmt.Errorf("%q is not a valid module path; pass one with -module", *module)
	}

	// Never overwrite: a scaffold that silently replaced someone's main.go
	// would be the most expensive command they ran that day.
	for _, f := range []string{"main.go", "go.mod"} {
		if _, err := os.Stat(filepath.Join(dir, f)); err == nil {
			return fmt.Errorf("%s already exists; choose an empty directory", filepath.Join(dir, f))
		}
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}

	// Copy every file of the template's directory, so multi-file examples
	// (api has a JSON helper) arrive whole.
	tdir := path.Dir(src)
	entries, err := templateFiles(tdir)
	if err != nil {
		return err
	}
	for _, e := range entries {
		b, err := examples.FS.ReadFile(path.Join(tdir, e))
		if err != nil {
			return err
		}
		b = bytes.ReplaceAll(b, []byte("go run ./examples/"+tdir), []byte("go run ."))
		if err := os.WriteFile(filepath.Join(dir, e), b, 0o644); err != nil {
			return err
		}
	}

	replace, err := localPhoton(*local)
	if err != nil {
		return err
	}
	gomod := "module " + *module + "\n\ngo 1.24\n"
	switch v := releaseVersion(); {
	case replace != "":
		gomod += "\nrequire " + photonModule + " v0.0.0\n\nreplace " + photonModule + " => " + replace + "\n"
	case v != "":
		// Pin the release this CLI belongs to, so the template compiles
		// against the API it was written for. A development build leaves the
		// choice to `go mod tidy`, which picks the latest release.
		gomod += "\nrequire " + photonModule + " " + v + "\n"
	}
	if err := os.WriteFile(filepath.Join(dir, "go.mod"), []byte(gomod), 0o644); err != nil {
		return err
	}

	fmt.Printf("Created %s from the %q template.\n\nNext:\n  cd %s\n  go mod tidy\n  go run .\n",
		dir, *tmpl, dir)
	if replace != "" {
		fmt.Printf("\nUsing the photon checkout at %s (replace directive in go.mod).\n", replace)
	}
	return nil
}

// templateFiles lists the regular files embedded for one template directory.
func templateFiles(dir string) ([]string, error) {
	entries, err := fs.ReadDir(examples.FS, dir)
	if err != nil {
		return nil, err
	}
	var out []string
	for _, e := range entries {
		if !e.IsDir() {
			out = append(out, e.Name())
		}
	}
	return out, nil
}

// localPhoton decides whether the new project should use a local checkout of
// photon. An explicit -photon path wins. Otherwise, a photon built from source
// - `go run ./cmd/photon` inside the repository, which reports its version as
// "(devel)" - points the project at that checkout, because the version it was
// built from may not be published. A released binary returns "" and the
// project resolves photon from the module proxy with `go mod tidy`.
func localPhoton(explicit string) (string, error) {
	if explicit != "" {
		abs, err := filepath.Abs(explicit)
		if err != nil {
			return "", err
		}
		if !isPhotonCheckout(abs) {
			return "", fmt.Errorf("%s is not a photon checkout (no go.mod for %s)", abs, photonModule)
		}
		return filepath.ToSlash(abs), nil
	}
	if bi, ok := debug.ReadBuildInfo(); ok && bi.Main.Version != "" && bi.Main.Version != "(devel)" {
		return "", nil
	}
	wd, err := os.Getwd()
	if err != nil {
		return "", nil
	}
	for dir := wd; ; dir = filepath.Dir(dir) {
		if isPhotonCheckout(dir) {
			return filepath.ToSlash(dir), nil
		}
		if filepath.Dir(dir) == dir {
			return "", nil
		}
	}
}

func isPhotonCheckout(dir string) bool {
	b, err := os.ReadFile(filepath.Join(dir, "go.mod"))
	return err == nil && bytes.HasPrefix(bytes.TrimSpace(b), []byte("module "+photonModule+"\n"))
}

// validModulePath is a conservative check for a usable module path.
var modulePathRE = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._~/-]*$`)

func validModulePath(p string) bool {
	return modulePathRE.MatchString(p) && !strings.Contains(p, "//") && !strings.HasSuffix(p, "/")
}

func templateNames() []string {
	names := make([]string, 0, len(examples.Templates))
	for n := range examples.Templates {
		names = append(names, n)
	}
	sort.Strings(names)
	return names
}
