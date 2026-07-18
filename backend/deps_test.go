package backend

import (
	"errors"
	"go/build"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

const backendDriverImport = "github.com/looprig/foreignloop/driver"

var backendAllowedImports = map[string]struct{}{
	"github.com/looprig/core/content":         {},
	"github.com/looprig/core/uuid":            {},
	"github.com/looprig/foreignloop/driver":   {},
	"github.com/looprig/harness/pkg/command":  {},
	"github.com/looprig/harness/pkg/event":    {},
	"github.com/looprig/harness/pkg/foreign":  {},
	"github.com/looprig/harness/pkg/identity": {},
	"github.com/looprig/harness/pkg/loop":     {},
	"golang.org/x/sys/unix":                   {},
}

type backendDependencyViolation struct {
	File       string
	ImportPath string
}

func TestScanBackendProductionDependenciesRecursesAcrossBuildTags(t *testing.T) {
	root := filepath.Join(t.TempDir(), "backend")
	writeBackendDependencyFixture(t, filepath.Join(root, "nested", "allowed.go"), `//go:build fixture_allowed

package nested

import (
	_ "context"
	_ "github.com/looprig/foreignloop/driver"
	_ "github.com/looprig/harness/pkg/event"
)
`)
	writeBackendDependencyFixture(t, filepath.Join(root, "nested", "bad.go"), `//go:build fixture_bad

package nested

import (
	_ "github.com/looprig/foreignloop/driver/claude"
	_ "github.com/looprig/foreignloop/driver/codex/wire"
)
`)

	violations, err := scanBackendProductionDependencies(root)
	if err != nil {
		t.Fatalf("scan nested backend dependencies: %v", err)
	}
	wantFile := filepath.Join("nested", "bad.go")
	for _, importPath := range []string{
		"github.com/looprig/foreignloop/driver/claude",
		"github.com/looprig/foreignloop/driver/codex/wire",
	} {
		if !hasBackendDependencyViolation(violations, wantFile, importPath) {
			t.Errorf("violations = %#v, want %s importing %q", violations, wantFile, importPath)
		}
	}
	if len(violations) != 2 {
		t.Errorf("len(violations) = %d, want 2: %#v", len(violations), violations)
	}
}

func TestBackendProductionDependencies(t *testing.T) {
	violations, err := scanBackendProductionDependencies(".")
	if err != nil {
		t.Fatalf("scan backend production dependencies: %v", err)
	}
	for _, violation := range violations {
		t.Errorf("%s imports %q outside the backend package boundary", violation.File, violation.ImportPath)
	}
}

func scanBackendProductionDependencies(root string) ([]backendDependencyViolation, error) {
	var violations []backendDependencyViolation
	fset := token.NewFileSet()
	err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() {
			if path == root {
				return nil
			}
			if skipBackendDependencyDirectory(entry.Name()) {
				return fs.SkipDir
			}
			_, err := os.Stat(filepath.Join(path, "go.mod"))
			switch {
			case err == nil:
				return fs.SkipDir
			case !errors.Is(err, os.ErrNotExist):
				return err
			default:
				return nil
			}
		}
		if filepath.Ext(path) != ".go" || strings.HasSuffix(path, "_test.go") {
			return nil
		}

		rel, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		parsed, err := parser.ParseFile(fset, path, nil, parser.ImportsOnly)
		if err != nil {
			return err
		}
		for _, imported := range parsed.Imports {
			importPath, err := strconv.Unquote(imported.Path.Value)
			if err != nil {
				return err
			}
			if backendImportAllowed(importPath) {
				continue
			}
			violations = append(violations, backendDependencyViolation{File: rel, ImportPath: importPath})
		}
		return nil
	})
	return violations, err
}

func backendImportAllowed(importPath string) bool {
	if _, ok := backendAllowedImports[importPath]; ok {
		return true
	}
	if strings.HasPrefix(importPath, backendDriverImport+"/") {
		return false
	}
	pkg, err := build.Default.Import(importPath, "", build.FindOnly)
	return err == nil && pkg.Goroot
}

func skipBackendDependencyDirectory(name string) bool {
	return name == "CVS" || name == "vendor" || name == "worktrees" || strings.HasPrefix(name, ".")
}

func hasBackendDependencyViolation(violations []backendDependencyViolation, file, importPath string) bool {
	for _, violation := range violations {
		if violation.File == file && violation.ImportPath == importPath {
			return true
		}
	}
	return false
}

func writeBackendDependencyFixture(t *testing.T, path, source string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o750); err != nil {
		t.Fatalf("create fixture directory: %v", err)
	}
	if err := os.WriteFile(path, []byte(source), 0o600); err != nil {
		t.Fatalf("write fixture %s: %v", path, err)
	}
}
