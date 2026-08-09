package boundary

import (
	"go/ast"
	"go/parser"
	"go/token"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"
)

func TestBackendAndDriverPublicTopLevelSymbols(t *testing.T) {
	t.Parallel()
	root := filepath.Clean(filepath.Join("..", ".."))
	tests := []struct {
		name string
		dir  string
		want []string
	}{
		{
			name: "driver",
			dir:  filepath.Join(root, "driver"),
			want: []string{
				"Agent", "Closer", "DecodeError", "Event", "ExitError", "History", "HistoryError",
				"Kind", "KindInit", "KindModelFacingError", "KindStepComplete", "KindTerminalError", "KindTerminalOK",
				"KindTextDelta", "KindThinkingDelta", "KindToolResult", "KindToolUse",
				"Observation", "ObservationKind", "ObservationPrompt", "ObservationSteer", "ObservationUpdate",
				"OrderedStream", "PermissionPosture", "Posture", "PostureAcceptEdits", "PostureDefault", "PostureReadOnly",
				"PostureWorkspaceWrite", "PromptObservation", "SpawnError", "SteerObservation", "SteerOutcome",
				"SteerOutcomeDeliveredUntrackable", "SteerOutcomeDeliveryUnknown", "SteerOutcomeFallbackRequired",
				"SteerOutcomeInjected", "SteerOutcomeUnsupported", "SteerRequest", "SteerResult", "Steerer",
				"Stream", "Turn", "UpdateObservation", "NewSteerRequest",
			},
		},
		{
			name: "backend",
			dir:  filepath.Join(root, "backend"),
			want: []string{
				"BuildRestoredWith", "BuildRestoredWithServices", "BuildWith", "BuildWithServices", "Config", "ConfigError", "ForeignProtocolError",
				"ForeignResultError", "ForeignSessionBusyError", "LockError", "Loop", "New",
				"SIDLateBound", "SIDMode", "SIDPrebound", "SnapshotContextDone", "SnapshotError",
				"SnapshotErrorReason", "SnapshotLoopExited",
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got, err := exportedTopLevelSymbols(tt.dir)
			if err != nil {
				t.Fatalf("inspect public symbols: %v", err)
			}
			sort.Strings(tt.want)
			if !reflect.DeepEqual(got, tt.want) {
				t.Fatalf("public symbols = %v, want exact manifest set %v", got, tt.want)
			}
		})
	}
}

func TestPublicErrorOwnershipSets(t *testing.T) {
	t.Parallel()
	root := filepath.Clean(filepath.Join("..", ".."))
	want := map[string][]string{
		"driver":        {"DecodeError", "ExitError", "HistoryError", "SpawnError"},
		"driver/claude": {"ConfigError", "PathError", "PlatformError", "SpawnConfigError", "WrapError"},
		"driver/codex":  {"ConfigError", "PlatformError", "SpawnConfigError"},
		"backend": {
			"ConfigError", "ForeignProtocolError", "ForeignResultError",
			"ForeignSessionBusyError", "LockError", "SnapshotError",
		},
	}
	for owner, expected := range want {
		types, err := exportedTypeNames(filepath.Join(root, owner))
		if err != nil {
			t.Fatalf("inspect %s public symbols: %v", owner, err)
		}
		var got []string
		for _, symbol := range types {
			if strings.HasSuffix(symbol, "Error") {
				got = append(got, symbol)
			}
		}
		sort.Strings(expected)
		if !reflect.DeepEqual(got, expected) {
			t.Errorf("%s public error types = %v, want exact manifest set %v", owner, got, expected)
		}
	}
}

func exportedTypeNames(dir string) ([]string, error) {
	entries, err := filepath.Glob(filepath.Join(dir, "*.go"))
	if err != nil {
		return nil, err
	}
	fset := token.NewFileSet()
	var exported []string
	for _, path := range entries {
		if strings.HasSuffix(path, "_test.go") {
			continue
		}
		parsed, err := parser.ParseFile(fset, path, nil, 0)
		if err != nil {
			return nil, err
		}
		for _, decl := range parsed.Decls {
			general, ok := decl.(*ast.GenDecl)
			if !ok {
				continue
			}
			for _, spec := range general.Specs {
				typed, ok := spec.(*ast.TypeSpec)
				if ok && typed.Name.IsExported() {
					exported = append(exported, typed.Name.Name)
				}
			}
		}
	}
	sort.Strings(exported)
	return exported, nil
}

func exportedTopLevelSymbols(dir string) ([]string, error) {
	entries, err := filepath.Glob(filepath.Join(dir, "*.go"))
	if err != nil {
		return nil, err
	}
	fset := token.NewFileSet()
	var exported []string
	for _, path := range entries {
		if strings.HasSuffix(path, "_test.go") {
			continue
		}
		parsed, err := parser.ParseFile(fset, path, nil, 0)
		if err != nil {
			return nil, err
		}
		for _, decl := range parsed.Decls {
			switch typed := decl.(type) {
			case *ast.FuncDecl:
				if typed.Recv == nil && typed.Name.IsExported() {
					exported = append(exported, typed.Name.Name)
				}
			case *ast.GenDecl:
				for _, spec := range typed.Specs {
					switch item := spec.(type) {
					case *ast.TypeSpec:
						if item.Name.IsExported() {
							exported = append(exported, item.Name.Name)
						}
					case *ast.ValueSpec:
						for _, name := range item.Names {
							if name.IsExported() {
								exported = append(exported, name.Name)
							}
						}
					}
				}
			}
		}
	}
	sort.Strings(exported)
	return exported, nil
}
