package codex

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/looprig/core/content"
	"github.com/looprig/foreignloop/driver"
)

// Agent must satisfy the foreign-agent port.
var _ driver.Agent = (*agent)(nil)

const fakeCodexSID = "0199a213-81c0-7800-8aa1-bbab2a035a53"
const promptCloseLimit = 1250 * time.Millisecond

func TestAgentSpawnFirstTurnExecJSONL(t *testing.T) {
	t.Parallel()
	fake := newFakeCodex(t)
	cwd := t.TempDir()
	agent := &agent{
		execPath:         fake.path,
		model:            "gpt-5",
		profile:          "looprig",
		additionalDirs:   []string{"/deps/one", "/deps/two"},
		sandbox:          SandboxWorkspaceWrite,
		approval:         ApprovalOnRequest,
		env:              fake.env("KEEP_ME=1", "STDERR_LINES=20000"),
		ignoreUserConfig: true,
		ignoreRules:      true,
		skipGitRepoCheck: true,
	}
	turn := driver.Turn{
		Cwd:          cwd,
		StartNew:     true,
		SystemPrompt: "system rules",
		Input:        []content.Block{&content.TextBlock{Text: "write the adapter"}},
	}

	stream, err := agent.Spawn(context.Background(), turn)
	if err != nil {
		t.Fatalf("Spawn() error = %v", err)
	}
	assertHistoryUnavailable(t, stream)
	events := collectEvents(t, stream)
	if err := stream.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	if err := stream.Close(); err != nil {
		t.Fatalf("second Close() error = %v, want idempotent nil", err)
	}
	assertHistoryUnavailable(t, stream)

	wantPrompt := "<looprig-system>system rules</looprig-system>\n\n<user-task>write the adapter</user-task>"
	wantArgv := []string{
		"exec",
		"--json",
		"--cd", cwd,
		"--model", "gpt-5",
		"--profile", "looprig",
		"--sandbox", "workspace-write",
		"-c", "approval_policy=\"on-request\"",
		"--add-dir", "/deps/one",
		"--add-dir", "/deps/two",
		"--ignore-user-config",
		"--ignore-rules",
		"--skip-git-repo-check",
		wantPrompt,
	}
	if got := fake.argv(t); !reflect.DeepEqual(got, wantArgv) {
		t.Fatalf("argv = %#v, want %#v", got, wantArgv)
	}
	if got := cleanPath(t, strings.TrimSpace(readFile(t, fake.cwdFile))); got != cleanPath(t, cwd) {
		t.Fatalf("child cwd = %q, want %q", got, cleanPath(t, cwd))
	}
	env := readFile(t, fake.envFile)
	if !strings.Contains(env, "KEEP_ME=1\n") {
		t.Fatalf("child env missing whitelisted var: %q", env)
	}
	if strings.Contains(env, "UNRELATED_PARENT_SHOULD_NOT_LEAK=") {
		t.Fatalf("child env leaked unrelated parent env: %q", env)
	}
	if stdin := readFile(t, fake.stdinFile); stdin != "" {
		t.Fatalf("stdin = %q, want empty", stdin)
	}
	assertCodexEvents(t, events)
}

func TestAgentCloseAlreadyCompletedReturnsPromptly(t *testing.T) {
	t.Parallel()
	fake := newFakeCodex(t)
	foreignStream, err := (&agent{execPath: fake.path, env: fake.env()}).Spawn(context.Background(), driver.Turn{
		Cwd:      t.TempDir(),
		StartNew: true,
	})
	if err != nil {
		t.Fatalf("Spawn() error = %v", err)
	}
	_ = collectEvents(t, foreignStream)
	assertClosePromptly(t, foreignStream)
}

func TestAgentSpawnEmptyEnvironmentDoesNotInheritAmbient(t *testing.T) {
	const sentinel = "LOOPRIG_CODEX_AMBIENT_SENTINEL"
	t.Setenv(sentinel, "must-not-leak")
	dir := t.TempDir()
	execPath := filepath.Join(dir, "codex")
	resultPath := filepath.Join(dir, "result")
	script := "#!/bin/sh\n" +
		"if /usr/bin/env | /usr/bin/grep -q '^" + sentinel + "='; then\n" +
		"  printf leaked > " + shellQuote(resultPath) + "\n" +
		"else\n" +
		"  printf clean > " + shellQuote(resultPath) + "\n" +
		"fi\n"
	if err := os.WriteFile(execPath, []byte(script), 0o755); err != nil {
		t.Fatalf("write fake codex: %v", err)
	}
	agent, err := NewAgent(os.Environ(), Config{ExecPath: execPath})
	if err != nil {
		t.Fatalf("NewAgent() error = %v", err)
	}
	stream, err := agent.Spawn(context.Background(), driver.Turn{Cwd: dir, StartNew: true})
	if err != nil {
		t.Fatalf("Spawn() error = %v", err)
	}
	_ = collectEvents(t, stream)
	if err := stream.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	if got := readFile(t, resultPath); got != "clean" {
		t.Fatalf("child ambient result = %q, want clean", got)
	}
}

func TestAgentSpawnResumeTurnExecJSONL(t *testing.T) {
	t.Parallel()
	fake := newFakeCodex(t)
	cwd := t.TempDir()
	agent := &agent{
		execPath:         fake.path,
		model:            "gpt-5",
		profile:          "ignored-on-resume",
		additionalDirs:   []string{"/ignored"},
		sandbox:          SandboxDangerFullAccess,
		approval:         ApprovalNever,
		env:              fake.env(),
		ignoreUserConfig: true,
		ignoreRules:      true,
		skipGitRepoCheck: true,
	}
	turn := driver.Turn{
		Cwd:          cwd,
		ForeignSID:   "11111111-2222-3333-4444-555555555555",
		StartNew:     false,
		SystemPrompt: "resume system",
		Input:        []content.Block{&content.TextBlock{Text: "continue"}},
	}

	stream, err := agent.Spawn(context.Background(), turn)
	if err != nil {
		t.Fatalf("Spawn() error = %v", err)
	}
	events := collectEvents(t, stream)
	if err := stream.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	wantPrompt := "<looprig-system>resume system</looprig-system>\n\n<user-task>continue</user-task>"
	wantArgv := []string{
		"exec",
		"resume",
		"--json",
		"--model", "gpt-5",
		"--ignore-user-config",
		"--ignore-rules",
		"--skip-git-repo-check",
		"--",
		"11111111-2222-3333-4444-555555555555",
		wantPrompt,
	}
	if got := fake.argv(t); !reflect.DeepEqual(got, wantArgv) {
		t.Fatalf("argv = %#v, want %#v", got, wantArgv)
	}
	assertCodexEvents(t, events)
}

func TestAgentSpawnRejectsInvalidResumeSIDBeforeProcessStart(t *testing.T) {
	t.Parallel()
	fake := newFakeCodex(t)
	stream, err := (&agent{execPath: fake.path, env: fake.env()}).Spawn(context.Background(), driver.Turn{
		Cwd:        t.TempDir(),
		ForeignSID: "--dangerously-bypass-approvals-and-sandbox",
		StartNew:   false,
	})
	if stream != nil {
		_ = stream.Close()
		t.Fatalf("Spawn() stream = %T, want nil", stream)
	}
	var configErr *SpawnConfigError
	if !errors.As(err, &configErr) {
		t.Fatalf("Spawn() error = %T %v, want *SpawnConfigError", err, err)
	}
	if configErr.Field != "ForeignSID" || configErr.Reason != "must be a UUID" {
		t.Fatalf("SpawnConfigError = %#v, want ForeignSID UUID rejection", configErr)
	}
	if _, statErr := os.Stat(fake.argvFile); !os.IsNotExist(statErr) {
		t.Fatalf("fake argv file stat error = %v, want process not started", statErr)
	}
}

func TestAgentSpawnContextCancelClosesEvents(t *testing.T) {
	t.Parallel()
	fake := newFakeCodex(t)
	ctx, cancel := context.WithCancel(context.Background())
	stream, err := (&agent{
		execPath: fake.path,
		env:      fake.env("FAKE_MODE=long_running"),
	}).Spawn(ctx, driver.Turn{
		Cwd:      t.TempDir(),
		StartNew: true,
		Input:    []content.Block{&content.TextBlock{Text: "wait"}},
	})
	if err != nil {
		t.Fatalf("Spawn() error = %v", err)
	}

	cancel()
	assertEventsClosePromptly(t, stream)
	err1 := stream.Close()
	if err2 := stream.Close(); err2 != err1 {
		t.Fatalf("second Close() error = %v, want same error %v", err2, err1)
	}
}

func TestAgentSpawnPreCanceledContextClosesEvents(t *testing.T) {
	t.Parallel()
	fake := newFakeCodex(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	stream, err := (&agent{
		execPath: fake.path,
		env:      fake.env("FAKE_MODE=long_running"),
	}).Spawn(ctx, driver.Turn{Cwd: t.TempDir(), StartNew: true})
	if err != nil {
		t.Fatalf("Spawn() error = %v", err)
	}
	assertEventsClosePromptly(t, stream)
	err1 := stream.Close()
	if err2 := stream.Close(); err2 != err1 {
		t.Fatalf("second Close() error = %v, want same error %v", err2, err1)
	}
}

func TestAgentSpawnConcurrentCancelAndClose(t *testing.T) {
	t.Parallel()
	fake := newFakeCodex(t)
	ctx, cancel := context.WithCancel(context.Background())
	stream, err := (&agent{
		execPath: fake.path,
		env:      fake.env("FAKE_MODE=long_running"),
	}).Spawn(ctx, driver.Turn{Cwd: t.TempDir(), StartNew: true})
	if err != nil {
		t.Fatalf("Spawn() error = %v", err)
	}
	start := make(chan struct{})
	closeResult := make(chan error, 1)
	cancelDone := make(chan struct{})
	var ready sync.WaitGroup
	ready.Add(2)
	go func() {
		ready.Done()
		<-start
		cancel()
		close(cancelDone)
	}()
	go func() {
		ready.Done()
		<-start
		closeResult <- stream.Close()
	}()
	ready.Wait()
	close(start)
	err1 := <-closeResult
	<-cancelDone
	assertEventsAlreadyClosed(t, stream)
	if err2 := stream.Close(); err2 != err1 {
		t.Fatalf("final Close() error = %v, want same error %v", err2, err1)
	}
}

func TestAgentSpawnCloseClosesEventsWithoutDrain(t *testing.T) {
	t.Parallel()
	fake := newFakeCodex(t)
	stream, err := (&agent{
		execPath: fake.path,
		env:      fake.env("FAKE_MODE=block_on_event"),
	}).Spawn(context.Background(), driver.Turn{
		Cwd:      t.TempDir(),
		StartNew: true,
		Input:    []content.Block{&content.TextBlock{Text: "do not drain"}},
	})
	if err != nil {
		t.Fatalf("Spawn() error = %v", err)
	}
	waitForBlockedDecoderSend(t)

	err1 := stream.Close()
	assertEventsAlreadyClosed(t, stream)
	if err2 := stream.Close(); err2 != err1 {
		t.Fatalf("second Close() error = %v, want same error %v", err2, err1)
	}
}

func TestAgentSpawnCloseReturnsDecodeError(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		mode string
	}{
		{name: "malformed json", mode: "malformed_json"},
		{name: "oversized line", mode: "oversized_line"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			fake := newFakeCodex(t)
			stream, err := (&agent{
				execPath: fake.path,
				env:      fake.env("FAKE_MODE=" + tt.mode),
			}).Spawn(context.Background(), driver.Turn{
				Cwd:      t.TempDir(),
				StartNew: true,
				Input:    []content.Block{&content.TextBlock{Text: "decode"}},
			})
			if err != nil {
				t.Fatalf("Spawn() error = %v", err)
			}
			_ = collectEvents(t, stream)
			err = stream.Close()
			var de *driver.DecodeError
			if !errors.As(err, &de) {
				t.Fatalf("Close() error = %T %[1]v, want *driver.DecodeError", err)
			}
		})
	}
}

func TestAgentSpawnCloseReturnsExitError(t *testing.T) {
	t.Parallel()
	fake := newFakeCodex(t)
	agent := &agent{
		execPath: fake.path,
		env:      fake.env("EXIT_CODE=7"),
	}
	turn := driver.Turn{
		Cwd:      t.TempDir(),
		StartNew: true,
		Input:    []content.Block{&content.TextBlock{Text: "fail after output"}},
	}

	stream, err := agent.Spawn(context.Background(), turn)
	if err != nil {
		t.Fatalf("Spawn() error = %v", err)
	}
	_ = collectEvents(t, stream)
	err = stream.Close()
	var ee *driver.ExitError
	if !errors.As(err, &ee) {
		t.Fatalf("Close() error = %T %[1]v, want *driver.ExitError", err)
	}
	if ee.Code != 7 {
		t.Fatalf("ExitError.Code = %d, want 7", ee.Code)
	}
	if err2 := stream.Close(); err2 != err {
		t.Fatalf("second Close() error = %v, want same error %v", err2, err)
	}
}

func TestAgentSpawnCloseJoinsDecodeAndExitErrors(t *testing.T) {
	t.Parallel()
	fake := newFakeCodex(t)
	stream, err := (&agent{
		execPath: fake.path,
		env:      fake.env("FAKE_MODE=malformed_json", "EXIT_CODE=7"),
	}).Spawn(context.Background(), driver.Turn{
		Cwd:      t.TempDir(),
		StartNew: true,
		Input:    []content.Block{&content.TextBlock{Text: "decode then exit"}},
	})
	if err != nil {
		t.Fatalf("Spawn() error = %v", err)
	}
	_ = collectEvents(t, stream)

	err = stream.Close()
	var de *driver.DecodeError
	if !errors.As(err, &de) {
		t.Fatalf("Close() error = %T %[1]v, want DecodeError", err)
	}
	var ee *driver.ExitError
	if !errors.As(err, &ee) || ee.Code != 7 {
		t.Fatalf("Close() error = %T %[1]v, want ExitError code 7", err)
	}
	if got, want := err.Error(), de.Error()+"\n"+ee.Error(); got != want {
		t.Fatalf("Close() error = %q, want deterministic decode-first ordering %q", got, want)
	}
	if err2 := stream.Close(); err2 != err {
		t.Fatalf("second Close() error = %v, want same error %v", err2, err)
	}
}

func TestAgentSpawnErrorPaths(t *testing.T) {
	t.Parallel()
	t.Run("empty exec path fails closed with config error", func(t *testing.T) {
		t.Parallel()
		stream, err := (&agent{}).Spawn(context.Background(), driver.Turn{StartNew: true})
		if err == nil {
			if stream != nil {
				_ = stream.Close()
			}
			t.Fatal("Spawn() error = nil, want error")
		}
		var se *driver.SpawnError
		if errors.As(err, &se) {
			t.Fatalf("Spawn() error = %T %[1]v, want direct config error", err)
		}
		if got := reflect.TypeOf(err).String(); got != "*codex.SpawnConfigError" {
			t.Fatalf("Spawn() error type = %s, want *codex.SpawnConfigError", got)
		}
		if got, want := err.Error(), "codex: spawn config: ExecPath: empty"; got != want {
			t.Fatalf("Spawn() error = %q, want %q", got, want)
		}
	})
	t.Run("bogus exec path surfaces a spawn error", func(t *testing.T) {
		t.Parallel()
		stream, err := (&agent{execPath: "/nonexistent/codex-binary-xyz-not-here"}).Spawn(context.Background(), driver.Turn{StartNew: true})
		if err == nil {
			if stream != nil {
				_ = stream.Close()
			}
			t.Fatal("Spawn() error = nil, want error")
		}
		var se *driver.SpawnError
		if !errors.As(err, &se) {
			t.Fatalf("Spawn() error = %T %[1]v, want *driver.SpawnError", err)
		}
	})
	t.Run("relative exec path fails before ambient lookup", func(t *testing.T) {
		t.Parallel()
		stream, err := (&agent{execPath: "codex"}).Spawn(context.Background(), driver.Turn{StartNew: true})
		if stream != nil {
			_ = stream.Close()
			t.Fatalf("Spawn() stream = %T, want nil", stream)
		}
		var configErr *SpawnConfigError
		if !errors.As(err, &configErr) {
			t.Fatalf("Spawn() error = %T %v, want *SpawnConfigError", err, err)
		}
		if configErr.Field != "ExecPath" || configErr.Reason != "must be a clean absolute path" {
			t.Fatalf("SpawnConfigError = %#v, want ExecPath clean-absolute rejection", configErr)
		}
	})
}

func assertCodexEvents(t *testing.T, got []driver.Event) {
	t.Helper()
	if len(got) != 3 {
		t.Fatalf("events = %d, want 3: %#v", len(got), got)
	}
	if got[0].Kind != driver.KindInit {
		t.Fatalf("event[0].Kind = %v, want KindInit", got[0].Kind)
	}
	if got[0].SessionID != fakeCodexSID {
		t.Fatalf("event[0].SessionID = %q, want %s", got[0].SessionID, fakeCodexSID)
	}
	assertStepText(t, got[1], "decoded assistant text")
	if got[2].Kind != driver.KindTerminalOK {
		t.Fatalf("event[2].Kind = %v, want KindTerminalOK", got[2].Kind)
	}
}

func assertHistoryUnavailable(t *testing.T, stream driver.Stream) {
	t.Helper()
	history, err := stream.History()
	if err != nil {
		t.Fatalf("History() error = %v, want nil", err)
	}
	if history.Available {
		t.Fatal("History().Available = true, want false")
	}
	if history.Steps != nil {
		t.Fatalf("History().Steps = %#v, want nil", history.Steps)
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

func collectEvents(t *testing.T, stream driver.Stream) []driver.Event {
	t.Helper()
	timer := time.NewTimer(5 * time.Second)
	defer timer.Stop()
	var events []driver.Event
	for {
		select {
		case ev, ok := <-stream.Events():
			if !ok {
				return events
			}
			events = append(events, ev)
		case <-timer.C:
			_ = stream.Close()
			t.Fatal("timed out waiting for stream events")
		}
	}
}

func assertEventsClosePromptly(t *testing.T, stream driver.Stream) {
	t.Helper()
	timer := time.NewTimer(500 * time.Millisecond)
	defer timer.Stop()
	for {
		select {
		case _, ok := <-stream.Events():
			if !ok {
				return
			}
		case <-timer.C:
			_ = stream.Close()
			t.Fatal("timed out waiting for events to close after context cancellation")
		}
	}
}

func assertEventsAlreadyClosed(t *testing.T, stream driver.Stream) {
	t.Helper()
	timer := time.NewTimer(500 * time.Millisecond)
	defer timer.Stop()
	select {
	case ev, ok := <-stream.Events():
		if ok {
			t.Fatalf("Events() yielded %#v after Close without a drain, want closed channel", ev)
		}
	case <-timer.C:
		t.Fatal("timed out waiting for Events() to close after Close without a drain")
	}
}

func waitForBlockedDecoderSend(t *testing.T) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	buf := make([]byte, 1<<20)
	for time.Now().Before(deadline) {
		n := runtime.Stack(buf, true)
		stacks := string(buf[:n])
		if strings.Contains(stacks, "github.com/looprig/foreignloop/driver/codex.decodeJSONL.func") &&
			(strings.Contains(stacks, "[chan send]") || strings.Contains(stacks, "[select]")) {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("timed out waiting for decoder goroutine to block on event send")
}

type fakeCodex struct {
	path      string
	argvFile  string
	envFile   string
	cwdFile   string
	stdinFile string
}

func newFakeCodex(t *testing.T) fakeCodex {
	t.Helper()
	dir := t.TempDir()
	f := fakeCodex{
		path:      filepath.Join(dir, "codex"),
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
    printf '%s\n' '{"type":"thread.started"'
    exit "${EXIT_CODE:-0}"
    ;;
  oversized_line)
    head -c 1048577 /dev/zero | tr '\000' x
    printf '\n'
    exit 0
    ;;
  long_running)
    trap 'exit 0' INT TERM
    printf '%s\n' '{"type":"thread.started","thread_id":"0199a213-81c0-7800-8aa1-bbab2a035a53"}'
    sleep 60
    exit 0
    ;;
  block_on_event)
    trap 'exit 0' INT TERM
    printf '%s\n' '{"type":"thread.started","thread_id":"0199a213-81c0-7800-8aa1-bbab2a035a53"}'
    sleep 60
    exit 0
    ;;
esac
i=0
while [ "$i" -lt "${STDERR_LINES:-0}" ]; do
  printf 'stderr line %s abcdefghijklmnopqrstuvwxyz abcdefghijklmnopqrstuvwxyz abcdefghijklmnopqrstuvwxyz\n' "$i" >&2
  i=$((i + 1))
done
printf '%s\n' '{"type":"thread.started","thread_id":"0199a213-81c0-7800-8aa1-bbab2a035a53"}'
printf '%s\n' '{"type":"turn.started"}'
printf '%s\n' '{"type":"item.completed","item":{"type":"agent_message","text":"decoded assistant text"}}'
printf '%s\n' '{"type":"turn.completed"}'
exit "${EXIT_CODE:-0}"
`
	if err := os.WriteFile(f.path, []byte(script), 0o755); err != nil {
		t.Fatalf("write fake codex: %v", err)
	}
	return f
}

func (f fakeCodex) env(extra ...string) []string {
	env := []string{
		"ARGV_FILE=" + f.argvFile,
		"ENV_FILE=" + f.envFile,
		"CWD_FILE=" + f.cwdFile,
		"STDIN_FILE=" + f.stdinFile,
	}
	return append(env, extra...)
}

func (f fakeCodex) argv(t *testing.T) []string {
	t.Helper()
	raw, err := os.ReadFile(f.argvFile)
	if err != nil {
		t.Fatalf("read argv file: %v", err)
	}
	raw = bytes.TrimSuffix(raw, []byte{0})
	if len(raw) == 0 {
		return nil
	}
	parts := bytes.Split(raw, []byte{0})
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		out = append(out, string(p))
	}
	return out
}

func readFile(t *testing.T, path string) string {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	return string(b)
}

func cleanPath(t *testing.T, path string) string {
	t.Helper()
	clean, err := filepath.EvalSymlinks(path)
	if err != nil {
		t.Fatalf("eval symlinks for %s: %v", path, err)
	}
	return clean
}

func shellQuote(value string) string {
	return "'" + strings.ReplaceAll(value, "'", "'\"'\"'") + "'"
}
