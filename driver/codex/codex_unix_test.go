//go:build darwin || (linux && !android)

package codex

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

	"github.com/looprig/foreignloop/driver"
)

const stubbornHelperStartupTimeout = 10 * time.Second

func TestAgentCloseCooperativeSIGINTReturnsPromptly(t *testing.T) {
	execPath, env := newCooperativeCodex(t)
	foreignStream, err := (&agent{
		execPath: execPath,
		env:      env,
	}).Spawn(context.Background(), driver.Turn{Cwd: t.TempDir(), StartNew: true})
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

func TestCodexCooperativeLeaderHelper(t *testing.T) {
	if os.Getenv("GO_WANT_CODEX_COOPERATIVE_HELPER") != "1" {
		return
	}
	interrupt := make(chan os.Signal, 1)
	signal.Notify(interrupt, os.Interrupt)
	fmt.Printf("%s\n", `{"type":"thread.started","thread_id":"0199a213-81c0-7800-8aa1-bbab2a035a53"}`)
	<-interrupt
	os.Exit(0)
}

func TestAgentCloseKillsStubbornProcessGroupDescendant(t *testing.T) {
	execPath, env, childPIDFile := newStubbornCodex(t)
	foreignStream, err := (&agent{
		execPath: execPath,
		env:      env,
	}).Spawn(context.Background(), driver.Turn{Cwd: t.TempDir(), StartNew: true})
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
	childPID := readPID(t, childPIDFile)
	closeErr := foreignStream.Close()
	// Close has reaped the leader, so its numeric PGID is no longer safe to
	// target from cleanup even when Close reported a decode or exit error.
	leaderUnreaped = false
	if closeErr != nil {
		t.Fatalf("Close() error = %v", closeErr)
	}
	assertProcessGone(t, childPID)
}

func TestCodexStubbornLeaderHelper(t *testing.T) {
	if os.Getenv("GO_WANT_CODEX_STUBBORN_HELPER") != "1" {
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
	fmt.Printf("%s\n", `{"type":"thread.started","thread_id":"0199a213-81c0-7800-8aa1-bbab2a035a53"}`)
	interrupt := make(chan os.Signal, 1)
	signal.Notify(interrupt, os.Interrupt)
	<-interrupt
	os.Exit(0)
}

func newStubbornCodex(t *testing.T) (string, []string, string) {
	t.Helper()
	testBinary, err := os.Executable()
	if err != nil {
		t.Fatalf("test executable: %v", err)
	}
	dir := t.TempDir()
	execPath := filepath.Join(dir, "codex")
	childPIDFile := filepath.Join(dir, "child.pid")
	childReadyFile := filepath.Join(dir, "child.ready")
	script := "#!/bin/sh\nexec \"$TEST_BINARY\" -test.run='^TestCodexStubbornLeaderHelper$' --\n"
	if err := os.WriteFile(execPath, []byte(script), 0o755); err != nil {
		t.Fatalf("write stubborn codex wrapper: %v", err)
	}
	return execPath, []string{
		"GO_WANT_CODEX_STUBBORN_HELPER=1",
		"TEST_BINARY=" + testBinary,
		"CHILD_PID_FILE=" + childPIDFile,
		"CHILD_READY_FILE=" + childReadyFile,
	}, childPIDFile
}

func newCooperativeCodex(t *testing.T) (string, []string) {
	t.Helper()
	testBinary, err := os.Executable()
	if err != nil {
		t.Fatalf("test executable: %v", err)
	}
	dir := t.TempDir()
	execPath := filepath.Join(dir, "codex")
	script := "#!/bin/sh\nexec \"$TEST_BINARY\" -test.run='^TestCodexCooperativeLeaderHelper$' --\n"
	if err := os.WriteFile(execPath, []byte(script), 0o755); err != nil {
		t.Fatalf("write cooperative codex wrapper: %v", err)
	}
	return execPath, []string{
		"GO_WANT_CODEX_COOPERATIVE_HELPER=1",
		"TEST_BINARY=" + testBinary,
	}
}

func readPID(t *testing.T, path string) int {
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

func assertProcessGone(t *testing.T, pid int) {
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
