package codex

import (
	"go/parser"
	"go/token"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

func TestCodexProductionDependencies(t *testing.T) {
	t.Parallel()
	files, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatalf("glob production files: %v", err)
	}
	allowed := map[string]bool{
		"github.com/looprig/core/content":       true,
		"github.com/looprig/foreignloops/driver": true,
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
				t.Fatalf("unquote import in %s: %v", file, err)
			}
			if strings.Contains(path, ".") && !allowed[path] {
				t.Errorf("%s imports %q; codex production may import only stdlib, Core content, and driver", file, path)
			}
		}
	}
}
