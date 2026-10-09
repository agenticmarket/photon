// Package doc_test verifies that the documentation covers the API.
//
// A README that omits a function is a documentation bug that no compiler and no
// code review catches: the code is correct, the tests pass, and the docs are
// quietly incomplete. This test is the mechanism that prevents that, and it is
// the reason the API section of examples/README.md can be trusted to be complete.
package photon_test

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
)

// TestDocsMentionEveryExportedSymbol asserts that every exported function, type,
// and method in the photon package appears in the documentation.
//
// Three files are searched, because the API is documented across three places by
// design: the main README is the index, examples/README.md is the tour, and
// docs/API.md is the reference. A symbol missing from all three is undocumented.
func TestDocsMentionEveryExportedSymbol(t *testing.T) {
	root := repoRoot(t)
	docs := readAll(t,
		filepath.Join(root, "README.md"),
		filepath.Join(root, "examples", "README.md"),
		filepath.Join(root, "docs", "API.md"),
	)

	symbols := exportedSymbols(t, root)

	var missing []string
	for _, sym := range symbols {
		if !mentions(docs, sym) {
			missing = append(missing, sym)
		}
	}
	sort.Strings(missing)

	if len(missing) > 0 {
		t.Errorf("%d exported symbols are not mentioned in README.md, examples/README.md, or docs/API.md:\n  %s\n\n"+
			"Either document them, or - if a symbol is genuinely internal-facing and "+
			"not worth documenting - add it to the allowlist in this test with a reason.",
			len(missing), strings.Join(missing, "\n  "))
	}
}

// notWorthDocumenting are exported but deliberately absent from the docs, each
// with the reason it is acceptable.
//
// An empty allowlist would be easier to maintain and would be wrong: it would
// mean every exported symbol must be documented, including ones that exist only
// for a test. Each entry here is a decision, and the reason is the part that
// matters.
var notWorthDocumenting = map[string]string{
	// Test-only build-tag files. raceEnabled exists in two files whose build tags
	// make exactly one of them compile, and it is read by allocation-budget tests
	// rather than by a user.
	"raceEnabled": "test-only build-tag constant, not part of the public API",
}

func mentions(docs string, sym string) bool {
	if _, skip := notWorthDocumenting[sym]; skip {
		return true
	}
	// Word-ish boundaries would miss "photon.JSON" matching a bare "JSON", which
	// is the common case. A substring match is looser than ideal and errs toward
	// passing, so the failure mode is a symbol that is technically mentioned in
	// the wrong context - visible on reading, not silent in a way that matters.
	return strings.Contains(docs, sym)
}

// exportedSymbols parses the photon package and returns its exported API names.
func exportedSymbols(t *testing.T, root string) []string {
	t.Helper()

	set := map[string]bool{}
	fset := token.NewFileSet()

	entries, err := os.ReadDir(root)
	if err != nil {
		t.Fatal(err)
	}

	for _, e := range entries {
		name := e.Name()
		if e.IsDir() || !strings.HasSuffix(name, ".go") {
			continue
		}
		// _test.go files define no public API, and build-tagged files are
		// conditionally compiled - a symbol that only exists under a tag is not
		// part of the surface a user sees.
		if strings.HasSuffix(name, "_test.go") || strings.HasSuffix(name, "_windows.go") ||
			strings.HasSuffix(name, "_unix.go") || strings.HasSuffix(name, "_linux.go") ||
			strings.HasSuffix(name, "_other.go") || strings.HasSuffix(name, "_off.go") ||
			strings.HasSuffix(name, "_on.go") {
			continue
		}

		f, err := parser.ParseFile(fset, filepath.Join(root, name), nil, 0)
		if err != nil {
			t.Fatalf("parsing %s: %v", name, err)
		}

		for _, decl := range f.Decls {
			switch d := decl.(type) {
			case *ast.FuncDecl:
				if !d.Name.IsExported() {
					continue
				}
				if d.Recv != nil && len(d.Recv.List) > 0 {
					// Methods are recorded as Type.Method so a doc can mention
					// either the type or the method name.
					//
					// An unexported receiver is skipped: a method on an unexported
					// type is not reachable from outside the package, so
					// documenting it would be documenting an implementation
					// detail. Only the method name was reported before, which
					// made Handle, Read, Flush, and Accept look like missing
					// public API.
					if recv := receiverType(d.Recv.List[0].Type); recv != "" && isExportedName(recv) {
						set[recv] = true
						set[recv+"."+d.Name.Name] = true
					}
				} else {
					set[d.Name.Name] = true
				}
			case *ast.GenDecl:
				for _, spec := range d.Specs {
					switch sp := spec.(type) {
					case *ast.TypeSpec:
						if sp.Name.IsExported() {
							set[sp.Name.Name] = true
						}
					case *ast.ValueSpec:
						for _, n := range sp.Names {
							if n.IsExported() {
								set[n.Name] = true
							}
						}
					}
				}
			}
		}
	}

	out := make([]string, 0, len(set))
	for k := range set {
		out = append(out, k)
	}
	return out
}

// receiverType extracts a method's receiver type name, unwrapping pointers and
// generics so *Stream and Stream both yield "Stream".
func receiverType(expr ast.Expr) string {
	switch t := expr.(type) {
	case *ast.Ident:
		return t.Name
	case *ast.StarExpr:
		return receiverType(t.X)
	case *ast.IndexExpr: // generic receiver, e.g. Foo[T]
		return receiverType(t.X)
	case *ast.IndexListExpr:
		return receiverType(t.X)
	}
	return ""
}

func isExportedName(s string) bool {
	if s == "" {
		return false
	}
	c := s[0]
	return c >= 'A' && c <= 'Z'
}

func readAll(t *testing.T, paths ...string) string {
	t.Helper()
	var b strings.Builder
	for _, p := range paths {
		data, err := os.ReadFile(p)
		if err != nil {
			t.Fatalf("reading %s: %v", p, err)
		}
		b.Write(data)
		b.WriteString("\n")
	}
	return b.String()
}

// repoRoot walks up from the test's directory to the module root, so the test
// works regardless of where `go test` is invoked from.
func repoRoot(t *testing.T) string {
	t.Helper()
	dir, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			t.Fatal("could not find go.mod walking up from " + dir)
		}
		dir = parent
	}
}
