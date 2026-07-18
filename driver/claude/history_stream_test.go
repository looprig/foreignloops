//go:build darwin || (linux && !android)

package claude

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/looprig/foreignloop/driver"
)

func TestClosedStreamHistoryMatchesGoldenSteps(t *testing.T) {
	t.Parallel()
	fake := newFakeClaude(t)
	home := t.TempDir()
	cwd := t.TempDir()
	path, err := transcriptPath(home, cwd, testSID)
	if err != nil {
		t.Fatalf("transcriptPath() error = %v", err)
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatalf("mkdir transcript parent: %v", err)
	}
	fixture, err := os.ReadFile(filepath.Join("testdata", "transcript", "happy.jsonl"))
	if err != nil {
		t.Fatalf("read fixture: %v", err)
	}
	if err := os.WriteFile(path, fixture, 0o600); err != nil {
		t.Fatalf("write transcript: %v", err)
	}

	parent := []string{
		"PATH=" + os.Getenv("PATH"),
		"ARGV_FILE=" + fake.argvFile,
		"ENV_FILE=" + fake.envFile,
		"CWD_FILE=" + fake.cwdFile,
		"STDIN_FILE=" + fake.stdinFile,
	}
	agent, err := NewAgent(parent, Config{
		ExecPath: fake.path,
		Home:     home,
		Model:    "small",
		EnvAllow: []string{"PATH", "ARGV_FILE", "ENV_FILE", "CWD_FILE", "STDIN_FILE"},
	})
	if err != nil {
		t.Fatalf("NewAgent() error = %v", err)
	}
	stream, err := agent.Spawn(context.Background(), driver.Turn{ForeignSID: testSID, StartNew: true, Cwd: cwd})
	if err != nil {
		t.Fatalf("Spawn() error = %v", err)
	}
	_ = collectEvents(t, stream)
	if err := stream.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	history, err := stream.History()
	if err != nil {
		t.Fatalf("History() error = %v", err)
	}
	want := driver.History{Available: true, Steps: transcriptCases()["happy"]}
	if !reflect.DeepEqual(history, want) {
		t.Fatalf("History() = %#v, want exact golden steps %#v", history, want)
	}
}

func TestStreamHistoryClassifiesDerivationAndReadErrors(t *testing.T) {
	t.Parallel()
	for _, tt := range []struct {
		name         string
		sid          string
		wantPath     bool
		wantNotExist bool
	}{
		{name: "invalid sid derivation", sid: "../bad", wantPath: true},
		{name: "missing transcript", sid: testSID, wantNotExist: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			fake := newFakeClaude(t)
			agent := newFakeAgent(t, fake, Config{Model: "small"})
			stream, err := agent.Spawn(context.Background(), driver.Turn{ForeignSID: tt.sid, StartNew: true, Cwd: t.TempDir()})
			if err != nil {
				t.Fatalf("Spawn() error = %v", err)
			}
			_ = collectEvents(t, stream)
			_ = stream.Close()
			history, err := stream.History()
			if !reflect.DeepEqual(history, driver.History{}) {
				t.Fatalf("History() = %#v, want zero history", history)
			}
			var historyErr *driver.HistoryError
			if !errors.As(err, &historyErr) {
				t.Fatalf("History() error = %T %v, want *driver.HistoryError", err, err)
			}
			if tt.wantPath {
				var pathErr *PathError
				if !errors.As(err, &pathErr) {
					t.Fatalf("History() error = %v, want retained *PathError", err)
				}
			}
			if tt.wantNotExist && !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("History() error = %v, want errors.Is(os.ErrNotExist)", err)
			}
		})
	}
}

func TestStreamHistoryRejectsSymlinkEscapes(t *testing.T) {
	t.Parallel()
	for _, tt := range []struct {
		name string
		link func(t *testing.T, root, path, outsidePath string)
	}{
		{
			name: "intermediate project directory symlink",
			link: func(t *testing.T, root, path, outsidePath string) {
				t.Helper()
				if err := os.MkdirAll(root, 0o755); err != nil {
					t.Fatalf("mkdir projects root: %v", err)
				}
				outsideDir := filepath.Dir(outsidePath)
				if err := os.Rename(outsidePath, filepath.Join(outsideDir, filepath.Base(path))); err != nil {
					t.Fatalf("rename outside transcript: %v", err)
				}
				if err := os.Symlink(outsideDir, filepath.Dir(path)); err != nil {
					t.Fatalf("symlink project directory: %v", err)
				}
			},
		},
		{
			name: "final transcript file symlink",
			link: func(t *testing.T, _ string, path, outsidePath string) {
				t.Helper()
				if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
					t.Fatalf("mkdir project directory: %v", err)
				}
				if err := os.Symlink(outsidePath, path); err != nil {
					t.Fatalf("symlink transcript file: %v", err)
				}
			},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			home := t.TempDir()
			cwd := t.TempDir()
			path, err := transcriptPath(home, cwd, testSID)
			if err != nil {
				t.Fatalf("transcriptPath() error = %v", err)
			}
			root := filepath.Join(home, claudeDir, projectsDir)
			outsideDir := t.TempDir()
			outsidePath := filepath.Join(outsideDir, "outside.jsonl")
			writeHappyTranscript(t, outsidePath)
			tt.link(t, root, path, outsidePath)

			stream := spawnClosedHistoryStream(t, home, cwd)
			history, err := stream.History()
			if !reflect.DeepEqual(history, driver.History{}) {
				t.Fatalf("History() = %#v, want zero history", history)
			}
			var historyErr *driver.HistoryError
			var pathErr *PathError
			if !errors.As(err, &historyErr) || !errors.As(err, &pathErr) {
				t.Fatalf("History() error = %T %v, want HistoryError retaining PathError", err, err)
			}
		})
	}
}

func spawnClosedHistoryStream(t *testing.T, home, cwd string) driver.Stream {
	t.Helper()
	fake := newFakeClaude(t)
	parent := []string{
		"PATH=" + os.Getenv("PATH"),
		"ARGV_FILE=" + fake.argvFile,
		"ENV_FILE=" + fake.envFile,
		"CWD_FILE=" + fake.cwdFile,
		"STDIN_FILE=" + fake.stdinFile,
	}
	agent, err := NewAgent(parent, Config{
		ExecPath: fake.path,
		Home:     home,
		Model:    "small",
		EnvAllow: []string{"PATH", "ARGV_FILE", "ENV_FILE", "CWD_FILE", "STDIN_FILE"},
	})
	if err != nil {
		t.Fatalf("NewAgent() error = %v", err)
	}
	stream, err := agent.Spawn(context.Background(), driver.Turn{ForeignSID: testSID, StartNew: true, Cwd: cwd})
	if err != nil {
		t.Fatalf("Spawn() error = %v", err)
	}
	_ = collectEvents(t, stream)
	_ = stream.Close()
	return stream
}

func writeHappyTranscript(t *testing.T, path string) {
	t.Helper()
	fixture, err := os.ReadFile(filepath.Join("testdata", "transcript", "happy.jsonl"))
	if err != nil {
		t.Fatalf("read happy fixture: %v", err)
	}
	if err := os.WriteFile(path, fixture, 0o600); err != nil {
		t.Fatalf("write transcript: %v", err)
	}
}

func TestStreamHasNoTranscriptPathMethod(t *testing.T) {
	t.Parallel()
	if _, ok := reflect.TypeOf((*stream)(nil)).MethodByName("TranscriptPath"); ok {
		t.Fatal("private stream exposes TranscriptPath method")
	}
}

var _ driver.Stream = (*stream)(nil)
