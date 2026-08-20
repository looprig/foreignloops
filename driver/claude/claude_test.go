//go:build darwin || (linux && !android)

package claude

import (
	"bytes"
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/looprig/core/content"
	"github.com/looprig/foreignloops/driver"
)

const promptCloseLimit = 1250 * time.Millisecond

func TestAgentCloseAlreadyCompletedReturnsPromptly(t *testing.T) {
	t.Parallel()
	fake := newFakeClaude(t)
	foreignStream := newFakeAgent(t, fake, Config{Model: "small"})
	stream, err := foreignStream.Spawn(context.Background(), driver.Turn{
		Cwd:        t.TempDir(),
		ForeignSID: testSID,
		StartNew:   true,
	})
	if err != nil {
		t.Fatalf("Spawn() error = %v", err)
	}
	_ = collectEvents(t, stream)
	assertClosePromptly(t, stream)
}

func TestAgentSpawnStartExecutesExactProcessBoundary(t *testing.T) {
	t.Parallel()
	fake := newFakeClaude(t)
	cwd := t.TempDir()
	agent := newFakeAgent(t, fake, Config{Model: "claude-small"}, "KEEP_ME=1", "STDERR_LINES=20000")
	turn := driver.Turn{
		SystemPrompt: "system rules",
		ForeignSID:   testSID,
		StartNew:     true,
		Cwd:          cwd,
		Posture:      driver.PostureAcceptEdits,
		Input: []content.Block{
			&content.TextBlock{Text: "write "},
			&content.ToolUseBlock{ID: "ignored"},
			&content.TextBlock{Text: "adapter"},
		},
	}

	stream, err := agent.Spawn(context.Background(), turn)
	if err != nil {
		t.Fatalf("Spawn() error = %v", err)
	}
	events := collectEvents(t, stream)
	if err := stream.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	if err := stream.Close(); err != nil {
		t.Fatalf("second Close() error = %v, want idempotent nil", err)
	}

	wantArgs := []string{
		"-p", "--output-format", "stream-json", "--include-partial-messages", "--verbose",
		"--append-system-prompt", "system rules", "--model", "claude-small",
		"--permission-mode", "acceptEdits", "--add-dir", cwd, "--session-id", testSID,
	}
	if got := fake.argv(t); !reflect.DeepEqual(got, wantArgs) {
		t.Fatalf("argv = %#v, want %#v", got, wantArgs)
	}
	if got := cleanPath(t, strings.TrimSpace(readTestFile(t, fake.cwdFile))); got != cleanPath(t, cwd) {
		t.Fatalf("child cwd = %q, want %q", got, cleanPath(t, cwd))
	}
	if got := readTestFile(t, fake.stdinFile); got != "write adapter" {
		t.Fatalf("stdin = %q, want flattened text-only prompt", got)
	}
	env := readTestFile(t, fake.envFile)
	if !strings.Contains(env, "KEEP_ME=1\n") {
		t.Fatalf("child env missing explicit entry: %q", env)
	}
	if strings.Contains(env, "UNRELATED_PARENT_SHOULD_NOT_LEAK=") {
		t.Fatalf("child env leaked unrelated parent entry: %q", env)
	}
	if got := eventKinds(events); !reflect.DeepEqual(got, []driver.Kind{driver.KindInit, driver.KindTextDelta, driver.KindStepComplete, driver.KindTerminalOK}) {
		t.Fatalf("event kinds = %v", got)
	}
}

func TestAgentSpawnResumeUsesResumeSelector(t *testing.T) {
	t.Parallel()
	fake := newFakeClaude(t)
	cwd := t.TempDir()
	agent := newFakeAgent(t, fake, Config{Model: "claude-small"})
	stream, err := agent.Spawn(context.Background(), driver.Turn{
		ForeignSID: testSID,
		StartNew:   false,
		Cwd:        cwd,
	})
	if err != nil {
		t.Fatalf("Spawn() error = %v", err)
	}
	_ = collectEvents(t, stream)
	if err := stream.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	args := fake.argv(t)
	if got := args[len(args)-2:]; !reflect.DeepEqual(got, []string{"--resume", testSID}) {
		t.Fatalf("argv tail = %#v, want resume selector", got)
	}
}

func TestAgentSpawnContextCancelDoesNotCloseEvents(t *testing.T) {
	t.Parallel()
	fake := newFakeClaude(t)
	agent := newFakeAgent(t, fake, Config{Model: "small"}, "FAKE_MODE=long_running")
	ctx, cancel := context.WithCancel(context.Background())
	stream, err := agent.Spawn(ctx, driver.Turn{ForeignSID: testSID, StartNew: true, Cwd: t.TempDir()})
	if err != nil {
		t.Fatalf("Spawn() error = %v", err)
	}
	defer func() { _ = stream.Close() }()
	select {
	case event, ok := <-stream.Events():
		if !ok || event.Kind != driver.KindInit {
			t.Fatalf("initial event = (%#v, %v), want open stream with KindInit", event, ok)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for initial event")
	}
	cancel()
	select {
	case event, ok := <-stream.Events():
		if !ok {
			t.Fatal("context cancellation closed the stream; Claude Spawn context must remain ignored")
		}
		if event.Kind != driver.KindTextDelta || event.Text != "still-running" {
			t.Fatalf("event after cancellation = %#v, want still-running text delta", event)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for event proving cancellation left the child running")
	}
	err1 := stream.Close()
	if err2 := stream.Close(); err2 != err1 {
		t.Fatalf("second Close() error = %v, want same explicit cleanup error %v", err2, err1)
	}
	assertEventsAlreadyClosed(t, stream)
}

func TestAgentCloseClosesEventsWithoutCallerDrain(t *testing.T) {
	t.Parallel()
	fake := newFakeClaude(t)
	agent := newFakeAgent(t, fake, Config{Model: "small"}, "FAKE_MODE=block_on_event")
	stream, err := agent.Spawn(context.Background(), driver.Turn{ForeignSID: testSID, StartNew: true, Cwd: t.TempDir()})
	if err != nil {
		t.Fatalf("Spawn() error = %v", err)
	}
	waitForForwarderBlock(t)
	err1 := stream.Close()
	assertEventsAlreadyClosed(t, stream)
	if err2 := stream.Close(); err2 != err1 {
		t.Fatalf("second Close() error = %v, want same error %v", err2, err1)
	}
}

func TestAgentCloseIgnoresDecodeError(t *testing.T) {
	t.Parallel()
	for _, mode := range []string{"malformed_json", "oversized_line"} {
		t.Run(mode, func(t *testing.T) {
			t.Parallel()
			fake := newFakeClaude(t)
			agent := newFakeAgent(t, fake, Config{Model: "small"}, "FAKE_MODE="+mode)
			stream, err := agent.Spawn(context.Background(), driver.Turn{ForeignSID: testSID, StartNew: true, Cwd: t.TempDir()})
			if err != nil {
				t.Fatalf("Spawn() error = %v", err)
			}
			_ = collectEvents(t, stream)
			err = stream.Close()
			if err != nil {
				t.Fatalf("Close() error = %T %v, want nil for clean process exit", err, err)
			}
			var decodeErr *driver.DecodeError
			if errors.As(err, &decodeErr) {
				t.Fatalf("Close() error retained *driver.DecodeError: %v", err)
			}
		})
	}
}

func TestAgentCloseReturnsExitError(t *testing.T) {
	t.Parallel()
	fake := newFakeClaude(t)
	agent := newFakeAgent(t, fake, Config{Model: "small"}, "EXIT_CODE=7")
	stream, err := agent.Spawn(context.Background(), driver.Turn{ForeignSID: testSID, StartNew: true, Cwd: t.TempDir()})
	if err != nil {
		t.Fatalf("Spawn() error = %v", err)
	}
	_ = collectEvents(t, stream)
	err = stream.Close()
	var exitErr *driver.ExitError
	if !errors.As(err, &exitErr) || exitErr.Code != 7 {
		t.Fatalf("Close() error = %T %v, want *driver.ExitError code 7", err, err)
	}
}

func TestAgentCloseReturnsOnlyExitErrorWhenDecodeAlsoFails(t *testing.T) {
	t.Parallel()
	fake := newFakeClaude(t)
	agent := newFakeAgent(t, fake, Config{Model: "small"}, "FAKE_MODE=malformed_json", "EXIT_CODE=7")
	stream, err := agent.Spawn(context.Background(), driver.Turn{ForeignSID: testSID, StartNew: true, Cwd: t.TempDir()})
	if err != nil {
		t.Fatalf("Spawn() error = %v", err)
	}
	_ = collectEvents(t, stream)
	err = stream.Close()
	var decodeErr *driver.DecodeError
	var exitErr *driver.ExitError
	if errors.As(err, &decodeErr) {
		t.Fatalf("Close() error retained *driver.DecodeError: %v", err)
	}
	if !errors.As(err, &exitErr) || exitErr.Code != 7 {
		t.Fatalf("Close() error = %T %v, want only *driver.ExitError code 7", err, err)
	}
	if got, want := err.Error(), "foreignloop: agent exited 7"; got != want {
		t.Fatalf("Close() error = %q, want %q", got, want)
	}
}

func TestAgentSpawnConfigurationFailsBeforeChildStart(t *testing.T) {
	t.Parallel()
	fake := newFakeClaude(t)
	for _, tt := range []struct {
		name       string
		agent      *agent
		wantField  string
		wantReason string
	}{
		{name: "empty executable", agent: &agent{model: "small"}, wantField: "ExecPath", wantReason: "empty"},
		{name: "relative executable", agent: &agent{execPath: "claude", model: "small"}, wantField: "ExecPath", wantReason: "must be a clean absolute path"},
		{name: "unclean executable", agent: &agent{execPath: filepath.Dir(fake.path) + "/subdir/../claude", model: "small"}, wantField: "ExecPath", wantReason: "must be a clean absolute path"},
		{name: "empty model", agent: &agent{execPath: fake.path}, wantField: "Model", wantReason: "empty"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			stream, err := tt.agent.Spawn(context.Background(), driver.Turn{ForeignSID: testSID, StartNew: true, Cwd: t.TempDir()})
			if stream != nil {
				_ = stream.Close()
				t.Fatalf("Spawn() stream = %T, want nil", stream)
			}
			var configErr *SpawnConfigError
			if !errors.As(err, &configErr) || configErr.Field != tt.wantField || configErr.Reason != tt.wantReason {
				t.Fatalf("Spawn() error = %T %v, want *SpawnConfigError for %s: %s", err, err, tt.wantField, tt.wantReason)
			}
			if _, statErr := os.Stat(fake.argvFile); !errors.Is(statErr, os.ErrNotExist) {
				t.Fatalf("child boundary file exists after config failure: %v", statErr)
			}
		})
	}
}

func TestAgentSpawnBogusExecutableIsSpawnError(t *testing.T) {
	t.Parallel()
	stream, err := (&agent{execPath: "/nonexistent/claude-binary-xyz", model: "small"}).Spawn(
		context.Background(), driver.Turn{ForeignSID: testSID, StartNew: true, Cwd: t.TempDir()},
	)
	if stream != nil {
		_ = stream.Close()
		t.Fatalf("Spawn() stream = %T, want nil", stream)
	}
	var spawnErr *driver.SpawnError
	if !errors.As(err, &spawnErr) {
		t.Fatalf("Spawn() error = %T %v, want *driver.SpawnError", err, err)
	}
}

func TestAgentSpawnEmptyEnvironmentDoesNotInheritAmbient(t *testing.T) {
	t.Setenv("LOOPRIG_AMBIENT_SENTINEL", "must-not-leak")
	dir := t.TempDir()
	execPath := filepath.Join(dir, "claude")
	script := `#!/bin/sh
if [ "${LOOPRIG_AMBIENT_SENTINEL+x}" = x ]; then
  exit 42
fi
printf '%s\n' '{"type":"result","subtype":"success","result":"done"}'
`
	if err := os.WriteFile(execPath, []byte(script), 0o755); err != nil {
		t.Fatalf("write fake claude: %v", err)
	}
	agent, err := NewAgent(nil, Config{ExecPath: execPath, Model: "small"})
	if err != nil {
		t.Fatalf("NewAgent() error = %v", err)
	}
	stream, err := agent.Spawn(context.Background(), driver.Turn{
		ForeignSID: testSID,
		StartNew:   true,
		Cwd:        dir,
	})
	if err != nil {
		t.Fatalf("Spawn() error = %v", err)
	}
	_ = collectEvents(t, stream)
	if err := stream.Close(); err != nil {
		t.Fatalf("Close() error = %v; empty cmd.Env inherited ambient sentinel", err)
	}
}

type fakeClaude struct {
	path      string
	argvFile  string
	envFile   string
	cwdFile   string
	stdinFile string
}

func newFakeClaude(t *testing.T) fakeClaude {
	t.Helper()
	dir := t.TempDir()
	fake := fakeClaude{
		path:      filepath.Join(dir, "claude"),
		argvFile:  filepath.Join(dir, "argv.bin"),
		envFile:   filepath.Join(dir, "env.txt"),
		cwdFile:   filepath.Join(dir, "cwd.txt"),
		stdinFile: filepath.Join(dir, "stdin.txt"),
	}
	script := `#!/bin/sh
set -eu
: > "$ARGV_FILE"
for arg in "$@"; do
  printf '%s\000' "$arg" >> "$ARGV_FILE"
done
env | sort > "$ENV_FILE"
pwd > "$CWD_FILE"
cat > "$STDIN_FILE"
case "${FAKE_MODE:-happy}" in
  malformed_json)
    printf '%s\n' '{"type":"system"'
    exit "${EXIT_CODE:-0}"
    ;;
  oversized_line)
    head -c 1048577 /dev/zero | tr '\000' x
    printf '\n'
    exit 0
    ;;
  long_running)
    trap 'exit 0' INT TERM
    printf '%s\n' '{"type":"system","subtype":"init","session_id":"fake-session"}'
    sleep 0.2
    printf '%s\n' '{"type":"stream_event","event":{"type":"content_block_delta","delta":{"type":"text_delta","text":"still-running"}}}'
    sleep 60
    exit 0
    ;;
  block_on_event)
    trap 'exit 0' INT TERM
    printf '%s\n' '{"type":"system","subtype":"init","session_id":"fake-session"}'
    printf '%s\n' '{"type":"stream_event","event":{"type":"content_block_delta","delta":{"type":"text_delta","text":"blocked"}}}'
    sleep 60
    exit 0
    ;;
esac
i=0
while [ "$i" -lt "${STDERR_LINES:-0}" ]; do
  printf 'stderr line %s abcdefghijklmnopqrstuvwxyz abcdefghijklmnopqrstuvwxyz\n' "$i" >&2
  i=$((i + 1))
done
printf '%s\n' '{"type":"system","subtype":"init","session_id":"fake-session"}'
printf '%s\n' '{"type":"stream_event","event":{"type":"content_block_delta","delta":{"type":"text_delta","text":"hello"}}}'
printf '%s\n' '{"type":"assistant","message":{"content":[{"type":"text","text":"hello"}]}}'
printf '%s\n' '{"type":"result","subtype":"success","result":"done"}'
exit "${EXIT_CODE:-0}"
`
	if err := os.WriteFile(fake.path, []byte(script), 0o755); err != nil {
		t.Fatalf("write fake claude: %v", err)
	}
	return fake
}

func newFakeAgent(t *testing.T, fake fakeClaude, cfg Config, extra ...string) driver.Agent {
	t.Helper()
	parent := append([]string{
		"PATH=" + os.Getenv("PATH"),
		"ARGV_FILE=" + fake.argvFile,
		"ENV_FILE=" + fake.envFile,
		"CWD_FILE=" + fake.cwdFile,
		"STDIN_FILE=" + fake.stdinFile,
		"UNRELATED_PARENT_SHOULD_NOT_LEAK=secret",
	}, extra...)
	cfg.ExecPath = fake.path
	cfg.Home = t.TempDir()
	cfg.EnvAllow = []string{"PATH", "ARGV_FILE", "ENV_FILE", "CWD_FILE", "STDIN_FILE", "KEEP_ME", "STDERR_LINES", "FAKE_MODE", "EXIT_CODE"}
	agent, err := NewAgent(parent, cfg)
	if err != nil {
		t.Fatalf("NewAgent() error = %v", err)
	}
	return agent
}

func (f fakeClaude) argv(t *testing.T) []string {
	t.Helper()
	raw, err := os.ReadFile(f.argvFile)
	if err != nil {
		t.Fatalf("read argv: %v", err)
	}
	raw = bytes.TrimSuffix(raw, []byte{0})
	if len(raw) == 0 {
		return nil
	}
	parts := bytes.Split(raw, []byte{0})
	out := make([]string, len(parts))
	for i, part := range parts {
		out[i] = string(part)
	}
	return out
}

func collectEvents(t *testing.T, stream driver.Stream) []driver.Event {
	t.Helper()
	timer := time.NewTimer(5 * time.Second)
	defer timer.Stop()
	var events []driver.Event
	for {
		select {
		case event, ok := <-stream.Events():
			if !ok {
				return events
			}
			events = append(events, event)
		case <-timer.C:
			_ = stream.Close()
			t.Fatal("timed out waiting for stream events")
		}
	}
}

func assertClosePromptly(t *testing.T, stream driver.Stream) {
	t.Helper()
	started := time.Now()
	if err := stream.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	if elapsed := time.Since(started); elapsed >= promptCloseLimit {
		t.Fatalf("Close() took %v, want less than %v", elapsed, promptCloseLimit)
	}
}

func assertEventsAlreadyClosed(t *testing.T, stream driver.Stream) {
	t.Helper()
	select {
	case event, ok := <-stream.Events():
		if ok {
			t.Fatalf("Events() yielded %#v after Close", event)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("timed out waiting for Events() to close after Close")
	}
}

func waitForForwarderBlock(t *testing.T) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	buf := make([]byte, 1<<20)
	for time.Now().Before(deadline) {
		n := runtime.Stack(buf, true)
		stacks := string(buf[:n])
		if strings.Contains(stacks, "github.com/looprig/foreignloops/driver/claude.forwardEvents") && strings.Contains(stacks, "[select]") {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("timed out waiting for event forwarder to block")
}

func readTestFile(t *testing.T, path string) string {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	return string(data)
}

func cleanPath(t *testing.T, path string) string {
	t.Helper()
	clean, err := filepath.EvalSymlinks(path)
	if err != nil {
		t.Fatalf("eval symlinks %s: %v", path, err)
	}
	return clean
}

// TestExitErrorIgnoresSignalTermination pins the classification shutdown
// depends on. shutdown always signals the process group before reaping, so a
// leader still running at Close -- for instance one that has not yet finished
// writing a line this driver stopped reading after a decode error -- dies of
// our own SIGINT. os/exec reports a signalled process as exit code -1, and
// reporting that as a foreign-agent exit failure blames the agent for this
// driver's own teardown. A genuine non-zero exit must still surface.
func TestExitErrorIgnoresSignalTermination(t *testing.T) {
	t.Parallel()
	cmd := exec.Command("/bin/sh", "-c", "trap '' INT; sleep 30")
	if err := cmd.Start(); err != nil {
		t.Fatalf("start: %v", err)
	}
	if err := cmd.Process.Signal(syscall.SIGKILL); err != nil {
		t.Fatalf("signal: %v", err)
	}
	waitErr := cmd.Wait()
	var exitErr *exec.ExitError
	if !errors.As(waitErr, &exitErr) {
		t.Fatalf("Wait() error = %T %v, want *exec.ExitError", waitErr, waitErr)
	}
	if code := exitErr.ExitCode(); code != -1 {
		t.Fatalf("ExitCode() = %d, want -1 for a signalled process", code)
	}
	if got := exitError(waitErr); got != nil {
		t.Fatalf("exitError(signalled) = %T %v, want nil", got, got)
	}
}
