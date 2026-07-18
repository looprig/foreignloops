//go:build darwin || (linux && !android)

package claude

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"strings"
	"testing"

	"github.com/looprig/foreignloop/driver"
)

func TestCommandWrapperResultIsStarted(t *testing.T) {
	t.Parallel()
	fake := newFakeClaude(t)
	called := false
	agent := newFakeAgent(t, fake, Config{
		Model: "small",
		Wrap: func(cmd *exec.Cmd) (*exec.Cmd, error) {
			called = true
			cmd.Env = append(cmd.Env, "WRAPPED=1")
			return cmd, nil
		},
	})
	stream, err := agent.Spawn(context.Background(), driver.Turn{ForeignSID: testSID, StartNew: true, Cwd: t.TempDir()})
	if err != nil {
		t.Fatalf("Spawn() error = %v", err)
	}
	_ = collectEvents(t, stream)
	if err := stream.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	if !called {
		t.Fatal("wrapper was not called")
	}
	if env := readTestFile(t, fake.envFile); !hasEnvLine(env, "WRAPPED=1") {
		t.Fatalf("wrapped command environment missing marker: %q", env)
	}
}

func TestCommandWrapperFailsClosed(t *testing.T) {
	t.Parallel()
	boom := errors.New("wrap boom")
	for _, tt := range []struct {
		name   string
		wrap   CommandWrapper
		wantIs error
	}{
		{name: "wrapper error", wrap: func(*exec.Cmd) (*exec.Cmd, error) { return nil, boom }, wantIs: boom},
		{name: "nil command", wrap: func(*exec.Cmd) (*exec.Cmd, error) { return nil, nil }, wantIs: errNilWrappedCmd},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			fake := newFakeClaude(t)
			agent := newFakeAgent(t, fake, Config{Model: "small", Wrap: tt.wrap})
			stream, err := agent.Spawn(context.Background(), driver.Turn{ForeignSID: testSID, StartNew: true, Cwd: t.TempDir()})
			if stream != nil {
				_ = stream.Close()
				t.Fatalf("Spawn() stream = %T, want nil", stream)
			}
			var spawnErr *driver.SpawnError
			var wrapErr *WrapError
			if !errors.As(err, &spawnErr) || !errors.As(err, &wrapErr) {
				t.Fatalf("Spawn() error = %T %v, want SpawnError wrapping WrapError", err, err)
			}
			if !errors.Is(err, tt.wantIs) {
				t.Fatalf("Spawn() error does not retain cause %v: %v", tt.wantIs, err)
			}
			if _, statErr := os.Stat(fake.argvFile); !errors.Is(statErr, os.ErrNotExist) {
				t.Fatalf("child boundary file exists after wrapper failure: %v", statErr)
			}
		})
	}
}

func hasEnvLine(env, want string) bool {
	for _, line := range strings.Split(env, "\n") {
		if line == want {
			return true
		}
	}
	return false
}
