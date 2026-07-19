//go:build darwin || (linux && !android)

package claude

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/looprig/foreignloops/driver"
)

const stubbornHelperStartupTimeout = 10 * time.Second

func TestAgentCloseCooperativeSIGINTReturnsPromptly(t *testing.T) {
	execPath, env := newCooperativeClaude(t)
	foreignStream, err := (&agent{
		execPath: execPath,
		model:    "small",
		env:      env,
	}).Spawn(context.Background(), driver.Turn{Cwd: t.TempDir(), ForeignSID: testSID, StartNew: true})
	if err != nil {
		t.Fatalf("Spawn() error = %v", err)
	}
	select {
	case event, ok := <-foreignStream.Events():
		if !ok || event.Kind != driver.KindInit {
			t.Fatalf("first event = (%#v, %t), want KindInit", event, ok)
		}
	case <-time.After(stubbornHelperStartupTimeout):
		t.Fatal("timed out waiting for cooperative process startup")
	}
	assertClosePromptly(t, foreignStream)
}

func TestClaudeCooperativeLeaderHelper(t *testing.T) {
	if os.Getenv("GO_WANT_CLAUDE_COOPERATIVE_HELPER") != "1" {
		return
	}
	interrupt := make(chan os.Signal, 1)
	signal.Notify(interrupt, os.Interrupt)
	fmt.Printf("%s\n", `{"type":"system","subtype":"init","session_id":"fake-session"}`)
	<-interrupt
	os.Exit(0)
}

func TestAgentCloseKillsStubbornProcessGroupDescendant(t *testing.T) {
	t.Parallel()
	execPath, env, childPIDFile := newStubbornClaude(t)
	foreignStream, err := (&agent{
		execPath: execPath,
		model:    "small",
		env:      env,
	}).Spawn(context.Background(), driver.Turn{Cwd: t.TempDir(), ForeignSID: testSID, StartNew: true})
	if err != nil {
		t.Fatalf("Spawn() error = %v", err)
	}
	impl := foreignStream.(*stream)
	leaderUnreaped := true
	t.Cleanup(func() {
		if leaderUnreaped {
			_ = syscall.Kill(-impl.pgid, syscall.SIGKILL)
		}
	})

	select {
	case event, ok := <-foreignStream.Events():
		if !ok || event.Kind != driver.KindInit {
			t.Fatalf("first event = (%#v, %t), want KindInit", event, ok)
		}
	case <-time.After(stubbornHelperStartupTimeout):
		t.Fatal("timed out waiting for stubborn process startup")
	}
	childPID := readStubbornPID(t, childPIDFile)
	closeErr := foreignStream.Close()
	// Close has reaped the leader, so its numeric PGID is no longer safe to
	// target from cleanup even when Close reported a decode or exit error.
	leaderUnreaped = false
	if closeErr != nil {
		t.Fatalf("Close() error = %v", closeErr)
	}
	assertStubbornProcessGone(t, childPID)
}

func newCooperativeClaude(t *testing.T) (string, []string) {
	t.Helper()
	testBinary, err := os.Executable()
	if err != nil {
		t.Fatalf("test executable: %v", err)
	}
	dir := t.TempDir()
	execPath := filepath.Join(dir, "claude")
	script := "#!/bin/sh\nexec \"$TEST_BINARY\" -test.run='^TestClaudeCooperativeLeaderHelper$' --\n"
	if err := os.WriteFile(execPath, []byte(script), 0o755); err != nil {
		t.Fatalf("write cooperative claude wrapper: %v", err)
	}
	return execPath, []string{
		"GO_WANT_CLAUDE_COOPERATIVE_HELPER=1",
		"TEST_BINARY=" + testBinary,
	}
}

func TestClaudeStubbornLeaderHelper(t *testing.T) {
	if os.Getenv("GO_WANT_CLAUDE_STUBBORN_HELPER") != "1" {
		return
	}
	child := exec.Command("/bin/sh", "-c", `trap '' INT TERM; printf ready > "$CHILD_READY_FILE"; exec /bin/sleep 60`)
	child.Stdout = io.Discard
	child.Stderr = io.Discard
	if err := child.Start(); err != nil {
		fmt.Fprintf(os.Stderr, "start stubborn child: %v\n", err)
		os.Exit(2)
	}
	readyDeadline := time.Now().Add(stubbornHelperStartupTimeout)
	for {
		if _, err := os.Stat(os.Getenv("CHILD_READY_FILE")); err == nil {
			break
		}
		if time.Now().After(readyDeadline) {
			fmt.Fprintln(os.Stderr, "stubborn child did not become ready")
			os.Exit(2)
		}
		time.Sleep(10 * time.Millisecond)
	}
	if err := os.WriteFile(os.Getenv("CHILD_PID_FILE"), []byte(strconv.Itoa(child.Process.Pid)), 0o600); err != nil {
		fmt.Fprintf(os.Stderr, "write child pid: %v\n", err)
		os.Exit(2)
	}
	fmt.Printf("%s\n", `{"type":"system","subtype":"init","session_id":"fake-session"}`)
	interrupt := make(chan os.Signal, 1)
	signal.Notify(interrupt, os.Interrupt)
	<-interrupt
	os.Exit(0)
}

func newStubbornClaude(t *testing.T) (string, []string, string) {
	t.Helper()
	testBinary, err := os.Executable()
	if err != nil {
		t.Fatalf("test executable: %v", err)
	}
	dir := t.TempDir()
	execPath := filepath.Join(dir, "claude")
	childPIDFile := filepath.Join(dir, "child.pid")
	childReadyFile := filepath.Join(dir, "child.ready")
	script := "#!/bin/sh\nexec \"$TEST_BINARY\" -test.run='^TestClaudeStubbornLeaderHelper$' --\n"
	if err := os.WriteFile(execPath, []byte(script), 0o755); err != nil {
		t.Fatalf("write stubborn claude wrapper: %v", err)
	}
	return execPath, []string{
		"GO_WANT_CLAUDE_STUBBORN_HELPER=1",
		"TEST_BINARY=" + testBinary,
		"CHILD_PID_FILE=" + childPIDFile,
		"CHILD_READY_FILE=" + childReadyFile,
	}, childPIDFile
}

func readStubbornPID(t *testing.T, path string) int {
	t.Helper()
	deadline := time.Now().Add(stubbornHelperStartupTimeout)
	for time.Now().Before(deadline) {
		raw, err := os.ReadFile(path)
		if err == nil {
			pid, convErr := strconv.Atoi(strings.TrimSpace(string(raw)))
			if convErr != nil {
				t.Fatalf("parse child pid %q: %v", raw, convErr)
			}
			return pid
		}
		if !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("read child pid: %v", err)
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("timed out waiting for child pid file")
	return 0
}

func assertStubbornProcessGone(t *testing.T, pid int) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		err := syscall.Kill(pid, 0)
		if errors.Is(err, syscall.ESRCH) {
			return
		}
		if err != nil && !errors.Is(err, syscall.EPERM) {
			t.Fatalf("probe child pid %d: %v", pid, err)
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("stubborn descendant pid %d still exists after Close", pid)
}
