package driver

import (
	"errors"
	"go/ast"
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

const (
	backendImportRoot           = "github.com/looprig/foreignloops/backend"
	coreContentImport           = "github.com/looprig/core/content"
	coreUUIDImport              = "github.com/looprig/core/uuid"
	driverImportRoot            = "github.com/looprig/foreignloops/driver"
	harnessImportRoot           = "github.com/looprig/harness"
	harnessLoopCredentialImport = "github.com/looprig/harness/pkg/loop"
	acpImportRoot               = "github.com/looprig/acp"
	acpDriverDir                = "acp/"
	acpBuilderFile              = "acp/builder.go"
	acpConfigFile               = "acp/config.go"
)

func forbiddenDriverImport(importPath string) bool {
	if hasImportPathPrefix(importPath, backendImportRoot) || hasImportPathPrefix(importPath, harnessImportRoot) {
		return true
	}
	return importPath != driverImportRoot && hasImportPathPrefix(importPath, driverImportRoot)
}

func hasImportPathPrefix(importPath, root string) bool {
	return importPath == root || strings.HasPrefix(importPath, root+"/")
}

func TestForbiddenDriverImport(t *testing.T) {
	tests := []struct {
		path      string
		forbidden bool
	}{
		{path: "github.com/looprig/foreignloops/backend", forbidden: true},
		{path: "github.com/looprig/foreignloops/backend/internal", forbidden: true},
		{path: "github.com/looprig/foreignloops/backendish", forbidden: false},
		{path: "github.com/looprig/harness", forbidden: true},
		{path: "github.com/looprig/harness/pkg/event", forbidden: true},
		{path: "github.com/looprig/harness/pkg/event/testing", forbidden: true},
		{path: "github.com/looprig/harness/pkg/eventual", forbidden: true},
		{path: "github.com/looprig/harness/pkg/command", forbidden: true},
		{path: "github.com/looprig/harnessed/pkg/event", forbidden: false},
		{path: "github.com/looprig/harness/pkg/session", forbidden: true},
		{path: "github.com/looprig/harness/pkg/session/subpackage", forbidden: true},
		{path: "github.com/looprig/harness/pkg/sessionstore", forbidden: true},
		{path: "github.com/looprig/harness/pkg/sessionstore/sql", forbidden: true},
		{path: "github.com/looprig/harness/pkg/sessionstorage", forbidden: true},
		{path: "github.com/looprig/harness/internal", forbidden: true},
		{path: "github.com/looprig/harness/internal/runtimecontract", forbidden: true},
		{path: "github.com/looprig/harness/internals", forbidden: true},
		{path: "github.com/looprig/foreignloops/driver/claude", forbidden: true},
		{path: "github.com/looprig/foreignloops/driver/claude/wire", forbidden: true},
		{path: "github.com/looprig/foreignloops/driver/claudette", forbidden: true},
		{path: "github.com/looprig/foreignloops/driver/codex", forbidden: true},
		{path: "github.com/looprig/foreignloops/driver/codex/wire", forbidden: true},
		{path: "github.com/looprig/foreignloops/driver/future", forbidden: true},
		{path: "github.com/looprig/foreignloops/driverish/claude", forbidden: false},
		{path: "github.com/looprig/core/content", forbidden: false},
		{path: "context", forbidden: false},
	}

	for _, tt := range tests {
		t.Run(tt.path, func(t *testing.T) {
			if got := forbiddenDriverImport(tt.path); got != tt.forbidden {
				t.Errorf("forbiddenDriverImport(%q) = %t, want %t", tt.path, got, tt.forbidden)
			}
		})
	}
}

func TestDriverImportAllowedForFile(t *testing.T) {
	tests := []struct {
		name       string
		file       string
		importPath string
		allowed    bool
	}{
		{name: "base stdlib", file: "driver.go", importPath: "context", allowed: true},
		{name: "base dotless external", file: "driver.go", importPath: "example/local", allowed: false},
		{name: "base Core", file: "history.go", importPath: "github.com/looprig/core/content", allowed: true},
		{name: "base root driver", file: "driver.go", importPath: "github.com/looprig/foreignloops/driver", allowed: false},
		{name: "base concrete provider", file: "driver.go", importPath: "github.com/looprig/foreignloops/driver/claude", allowed: false},
		{name: "claude nested stdlib", file: "claude/wire/decode.go", importPath: "encoding/json", allowed: true},
		{name: "claude nested Core", file: "claude/wire/decode.go", importPath: "github.com/looprig/core/content", allowed: true},
		{name: "claude nested root driver", file: "claude/wire/decode.go", importPath: "github.com/looprig/foreignloops/driver", allowed: true},
		{name: "claude nested codex", file: "claude/wire/decode.go", importPath: "github.com/looprig/foreignloops/driver/codex", allowed: false},
		{name: "codex nested root driver", file: "codex/wire/decode.go", importPath: "github.com/looprig/foreignloops/driver", allowed: true},
		{name: "codex nested claude", file: "codex/wire/decode.go", importPath: "github.com/looprig/foreignloops/driver/claude", allowed: false},
		{name: "future nested root driver", file: "future/wire/decode.go", importPath: "github.com/looprig/foreignloops/driver", allowed: true},
		{name: "future nested sibling", file: "future/wire/decode.go", importPath: "github.com/looprig/foreignloops/driver/claude", allowed: false},
		{name: "future nested Harness", file: "future/wire/decode.go", importPath: "github.com/looprig/harness/pkg/command", allowed: false},
		{name: "acp config credential mode", file: acpConfigFile, importPath: harnessLoopCredentialImport, allowed: true},
		{name: "acp config unrelated Harness", file: acpConfigFile, importPath: "github.com/looprig/harness/pkg/event", allowed: false},
		{name: "acp nested launch", file: "acp/config.go", importPath: "github.com/looprig/acp/launch", allowed: true},
		{name: "acp nested driver", file: "acp/config.go", importPath: "github.com/looprig/foreignloops/driver", allowed: true},
		{name: "claude nested acp forbidden", file: "claude/wire/decode.go", importPath: "github.com/looprig/acp/launch", allowed: false},
		{name: "base acp forbidden", file: "driver.go", importPath: "github.com/looprig/acp/launch", allowed: false},
		{name: "acp unrelated module not widened", file: "acp/config.go", importPath: "github.com/looprig/acpish/launch", allowed: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := driverImportAllowedForFile(tt.file, tt.importPath); got != tt.allowed {
				t.Errorf("driverImportAllowedForFile(%q, %q) = %t, want %t", tt.file, tt.importPath, got, tt.allowed)
			}
		})
	}
}

func TestScanDriverProductionDependenciesRecursesAcrossBuildTags(t *testing.T) {
	root := filepath.Join(t.TempDir(), "driver")
	writeDependencyFixture(t, filepath.Join(root, "claude", "wire", "allowed.go"), `//go:build fixture_allowed

package wire

import (
	_ "context"
	_ "github.com/looprig/core/content"
	_ "github.com/looprig/foreignloops/driver"
)
`)
	writeDependencyFixture(t, filepath.Join(root, "claude", "wire", "bad.go"), `//go:build fixture_bad

package wire

import (
	_ "github.com/looprig/foreignloops/backend/internal"
	_ "github.com/looprig/harness/pkg/event"
)
`)

	violations, err := scanDriverProductionDependencies(root)
	if err != nil {
		t.Fatalf("scan nested driver dependencies: %v", err)
	}
	wantFile := filepath.Join("claude", "wire", "bad.go")
	for _, importPath := range []string{
		"github.com/looprig/foreignloops/backend/internal",
		"github.com/looprig/harness/pkg/event",
	} {
		if !hasDependencyViolation(violations, wantFile, importPath) {
			t.Errorf("violations = %#v, want %s importing %q", violations, wantFile, importPath)
		}
	}
	if len(violations) != 2 {
		t.Errorf("len(violations) = %d, want 2: %#v", len(violations), violations)
	}
}

func TestScanDriverProductionDependenciesSkipsNonProductionTrees(t *testing.T) {
	root := filepath.Join(t.TempDir(), "driver")
	badSource := `package ignored

import _ "github.com/looprig/harness/pkg/event"
`
	for _, name := range []string{
		filepath.Join("claude", "wire", "ignored_test.go"),
		filepath.Join("vendor", "provider", "bad.go"),
		filepath.Join(".git", "objects", "bad.go"),
		filepath.Join(".hidden", "bad.go"),
		filepath.Join(".worktrees", "branch", "bad.go"),
		filepath.Join("worktrees", "branch", "bad.go"),
		filepath.Join("CVS", "bad.go"),
	} {
		writeDependencyFixture(t, filepath.Join(root, name), badSource)
	}
	writeDependencyFixture(t, filepath.Join(root, "claude", "nestedmodule", "go.mod"), "module example.com/nested\n")
	writeDependencyFixture(t, filepath.Join(root, "claude", "nestedmodule", "bad.go"), badSource)

	violations, err := scanDriverProductionDependencies(root)
	if err != nil {
		t.Fatalf("scan driver dependencies with excluded trees: %v", err)
	}
	if len(violations) != 0 {
		t.Fatalf("violations = %#v, want none from excluded trees", violations)
	}
}

func writeDependencyFixture(t *testing.T, path, source string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o750); err != nil {
		t.Fatalf("create fixture directory: %v", err)
	}
	if err := os.WriteFile(path, []byte(source), 0o600); err != nil {
		t.Fatalf("write fixture %s: %v", path, err)
	}
}

func hasDependencyViolation(violations []dependencyViolation, file, importPath string) bool {
	for _, violation := range violations {
		if violation.File == file && violation.ImportPath == importPath {
			return true
		}
	}
	return false
}

type dependencyViolation struct {
	File       string
	ImportPath string
}

func driverImportAllowedForFile(file, importPath string) bool {
	file = filepath.ToSlash(filepath.Clean(file))
	if file == acpConfigFile && importPath == harnessLoopCredentialImport {
		return true
	}
	if file == acpBuilderFile {
		switch {
		case importPath == coreUUIDImport:
			return true
		case hasImportPathPrefix(importPath, backendImportRoot):
			return true
		case hasImportPathPrefix(importPath, harnessImportRoot):
			return true
		}
	}
	if forbiddenDriverImport(importPath) {
		return false
	}
	if importPath == coreContentImport || isStandardLibraryImport(importPath) {
		return true
	}

	if !strings.Contains(file, "/") {
		return false
	}
	if importPath == driverImportRoot {
		return true
	}
	return strings.HasPrefix(file, acpDriverDir) && hasImportPathPrefix(importPath, acpImportRoot)
}

func isStandardLibraryImport(importPath string) bool {
	pkg, err := build.Default.Import(importPath, "", build.FindOnly)
	return err == nil && pkg.Goroot
}

func scanDriverProductionDependencies(root string) ([]dependencyViolation, error) {
	var violations []dependencyViolation
	fset := token.NewFileSet()
	err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() {
			if path == root {
				return nil
			}
			if skipDriverDependencyDirectory(entry.Name()) {
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
			if !driverImportAllowedForFile(rel, importPath) {
				violations = append(violations, dependencyViolation{File: rel, ImportPath: importPath})
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return violations, nil
}

func skipDriverDependencyDirectory(name string) bool {
	return name == "CVS" || name == "vendor" || name == "worktrees" || strings.HasPrefix(name, ".")
}

func TestDriverProductionDependencies(t *testing.T) {
	violations, err := scanDriverProductionDependencies(".")
	if err != nil {
		t.Fatalf("scan driver production dependencies: %v", err)
	}
	for _, violation := range violations {
		t.Errorf("%s imports %q outside its driver package boundary", violation.File, violation.ImportPath)
	}
}

func TestDriverAPIHasNoProviderOrPathVocabulary(t *testing.T) {
	files, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatalf("glob driver production files: %v", err)
	}

	fset := token.NewFileSet()
	for _, file := range files {
		if strings.HasSuffix(file, "_test.go") {
			continue
		}
		parsed, err := parser.ParseFile(fset, file, nil, 0)
		if err != nil {
			t.Fatalf("parse %s: %v", file, err)
		}
		ast.Inspect(parsed, func(node ast.Node) bool {
			ident, ok := node.(*ast.Ident)
			if !ok {
				return true
			}
			switch ident.Name {
			case "TranscriptPath", "sinceTurn", "SinceTurn":
				t.Errorf("%s exposes forbidden provider/path vocabulary %q", file, ident.Name)
			}
			return true
		})
	}
}
