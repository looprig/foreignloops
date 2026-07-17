package driver

import (
	"go/ast"
	"go/parser"
	"go/token"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

var forbiddenProductionImports = []string{
	"github.com/looprig/foreignloop/backend",
	"github.com/looprig/harness/pkg/event",
	"github.com/looprig/harness/pkg/session",
	"github.com/looprig/harness/pkg/sessionstore",
	"github.com/looprig/harness/internal",
	"github.com/looprig/foreignloop/driver/claude",
	"github.com/looprig/foreignloop/driver/codex",
}

func forbiddenDriverImport(importPath string) bool {
	for _, forbidden := range forbiddenProductionImports {
		if importPath == forbidden || strings.HasPrefix(importPath, forbidden+"/") {
			return true
		}
	}
	return false
}

func TestForbiddenDriverImport(t *testing.T) {
	tests := []struct {
		path      string
		forbidden bool
	}{
		{path: "github.com/looprig/foreignloop/backend", forbidden: true},
		{path: "github.com/looprig/foreignloop/backend/internal", forbidden: true},
		{path: "github.com/looprig/foreignloop/backendish", forbidden: false},
		{path: "github.com/looprig/harness/pkg/event", forbidden: true},
		{path: "github.com/looprig/harness/pkg/event/testing", forbidden: true},
		{path: "github.com/looprig/harness/pkg/eventual", forbidden: false},
		{path: "github.com/looprig/harness/pkg/session", forbidden: true},
		{path: "github.com/looprig/harness/pkg/session/subpackage", forbidden: true},
		{path: "github.com/looprig/harness/pkg/sessionstore", forbidden: true},
		{path: "github.com/looprig/harness/pkg/sessionstore/sql", forbidden: true},
		{path: "github.com/looprig/harness/pkg/sessionstorage", forbidden: false},
		{path: "github.com/looprig/harness/internal", forbidden: true},
		{path: "github.com/looprig/harness/internal/runtimecontract", forbidden: true},
		{path: "github.com/looprig/harness/internals", forbidden: false},
		{path: "github.com/looprig/foreignloop/driver/claude", forbidden: true},
		{path: "github.com/looprig/foreignloop/driver/claude/wire", forbidden: true},
		{path: "github.com/looprig/foreignloop/driver/claudette", forbidden: false},
		{path: "github.com/looprig/foreignloop/driver/codex", forbidden: true},
		{path: "github.com/looprig/foreignloop/driver/codex/wire", forbidden: true},
		{path: "github.com/looprig/foreignloop/driver/codexish", forbidden: false},
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

func TestDriverProductionDependencies(t *testing.T) {
	files, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatalf("glob driver production files: %v", err)
	}

	fset := token.NewFileSet()
	for _, file := range files {
		if strings.HasSuffix(file, "_test.go") {
			continue
		}
		parsed, err := parser.ParseFile(fset, file, nil, parser.ImportsOnly)
		if err != nil {
			t.Fatalf("parse %s: %v", file, err)
		}
		for _, imported := range parsed.Imports {
			path, err := strconv.Unquote(imported.Path.Value)
			if err != nil {
				t.Fatalf("unquote import %s in %s: %v", imported.Path.Value, file, err)
			}
			if forbiddenDriverImport(path) {
				t.Errorf("%s imports forbidden dependency %q", file, path)
			}
			if !isAllowedDriverImport(path) {
				t.Errorf("%s imports %q; driver production code may import only stdlib and github.com/looprig/core/content", file, path)
			}
		}
	}
}

func isAllowedDriverImport(importPath string) bool {
	return importPath == "github.com/looprig/core/content" || !strings.Contains(importPath, ".")
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
