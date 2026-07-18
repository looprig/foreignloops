package backend

import (
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

func deadPID(t *testing.T) int {
	t.Helper()
	cmd := exec.Command("true")
	if err := cmd.Run(); err != nil {
		t.Fatalf("run true: %v", err)
	}
	return cmd.Process.Pid
}

func preWriteLock(t *testing.T, sid, cwd, contents string) {
	t.Helper()
	path := foreignLockPath(sid, cwd)
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		t.Fatalf("mkdir lock dir: %v", err)
	}
	if err := os.WriteFile(path, []byte(contents), 0o600); err != nil {
		t.Fatalf("pre-write lock: %v", err)
	}
	t.Cleanup(func() { _ = os.Remove(path) })
}

func TestForeignLockPath(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name       string
		sidA, cwdA string
		sidB, cwdB string
		wantSame   bool
	}{
		{name: "same inputs are deterministic", sidA: "s", cwdA: "/work/a", sidB: "s", cwdB: "/work/a", wantSame: true},
		{name: "trailing slash cleaned equal", sidA: "s", cwdA: "/work/a/", sidB: "s", cwdB: "/work/a", wantSame: true},
		{name: "different cwd differs", sidA: "s", cwdA: "/work/a", sidB: "s", cwdB: "/work/b"},
		{name: "different sid differs", sidA: "s1", cwdA: "/work/a", sidB: "s2", cwdB: "/work/a"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			a := foreignLockPath(tt.sidA, tt.cwdA)
			b := foreignLockPath(tt.sidB, tt.cwdB)
			if (a == b) != tt.wantSame {
				t.Fatalf("foreignLockPath equality = %v (a=%q b=%q), want %v", a == b, a, b, tt.wantSame)
			}
			if !strings.HasPrefix(a, filepath.Join(os.TempDir(), "looprig-foreign")) {
				t.Fatalf("lock path %q not under looprig-foreign tempdir", a)
			}
			if !strings.HasSuffix(a, ".lock") || !strings.Contains(a, tt.sidA) {
				t.Fatalf("lock path %q missing .lock suffix or sid", a)
			}
		})
	}
}

func TestTemporaryForeignLockNamespaceDoesNotCollideWithDurableSID(t *testing.T) {
	t.Parallel()
	const loopID = "00000000-0000-0000-0000-000000000001"
	cwd := t.TempDir()
	foreignSID := temporaryForeignLockPrefix + loopID

	temporary, err := acquireTemporaryForeignLock(loopID, cwd)
	if err != nil {
		t.Fatalf("acquire temporary lock: %v", err)
	}
	t.Cleanup(temporary.release)
	durable, err := acquireForeignLock(foreignSID, cwd)
	if err != nil {
		t.Fatalf("acquire durable lock: %v", err)
	}
	t.Cleanup(durable.release)

	if temporary.path == durable.path {
		t.Fatalf("temporary and durable lock paths collide at %q", temporary.path)
	}
	if got, want := durable.path, foreignLockPath(foreignSID, cwd); got != want {
		t.Fatalf("durable path = %q, want %q", got, want)
	}
}

func TestProcessAlive(t *testing.T) {
	t.Parallel()
	dead := deadPID(t)
	tests := []struct {
		name string
		pid  int
		want bool
	}{
		{name: "self is alive", pid: os.Getpid(), want: true},
		{name: "zero pid not alive", pid: 0},
		{name: "negative pid not alive", pid: -1},
		{name: "reaped child not alive", pid: dead},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := processAlive(tt.pid); got != tt.want {
				t.Fatalf("processAlive(%d) = %v, want %v", tt.pid, got, tt.want)
			}
		})
	}
}

func TestAcquireForeignLock(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name         string
		pre          func(*testing.T, string, string)
		acquireFirst bool
		wantBusy     bool
	}{
		{name: "fresh path acquires"},
		{name: "second acquire while held is busy", acquireFirst: true, wantBusy: true},
		{name: "live holder is busy", pre: func(t *testing.T, sid, cwd string) { preWriteLock(t, sid, cwd, strconv.Itoa(os.Getpid())) }, wantBusy: true},
		{name: "stale dead holder reclaimed", pre: func(t *testing.T, sid, cwd string) { preWriteLock(t, sid, cwd, strconv.Itoa(deadPID(t))) }},
		{name: "malformed pid reclaimed", pre: func(t *testing.T, sid, cwd string) { preWriteLock(t, sid, cwd, "not-a-pid") }},
		{name: "empty lock reclaimed", pre: func(t *testing.T, sid, cwd string) { preWriteLock(t, sid, cwd, "") }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			const sid = "00000000-0000-0000-0000-000000000001"
			cwd := t.TempDir()
			if tt.pre != nil {
				tt.pre(t, sid, cwd)
			}
			if tt.acquireFirst {
				first, err := acquireForeignLock(sid, cwd)
				if err != nil {
					t.Fatalf("first acquire: %v", err)
				}
				t.Cleanup(first.release)
			}

			lock, err := acquireForeignLock(sid, cwd)
			if tt.wantBusy {
				var busy *ForeignSessionBusyError
				if !errors.As(err, &busy) {
					t.Fatalf("acquire error = %T %v, want ForeignSessionBusyError", err, err)
				}
				if busy.SID != sid || busy.Cwd != cwd {
					t.Fatalf("busy coordinates = %s/%s, want %s/%s", busy.SID, busy.Cwd, sid, cwd)
				}
				return
			}
			if err != nil {
				t.Fatalf("acquire: %v", err)
			}
			t.Cleanup(lock.release)
			contents, err := os.ReadFile(lock.path)
			if err != nil {
				t.Fatalf("read lock: %v", err)
			}
			if got := strings.TrimSpace(string(contents)); got != strconv.Itoa(os.Getpid()) {
				t.Fatalf("lock pid = %q, want %d", got, os.Getpid())
			}
		})
	}
}

func TestForeignLockReleaseIdempotent(t *testing.T) {
	t.Parallel()
	for _, acquire := range []bool{true, false} {
		acquire := acquire
		t.Run(strconv.FormatBool(acquire), func(t *testing.T) {
			t.Parallel()
			sid := "00000000-0000-0000-0000-000000000001"
			cwd := t.TempDir()
			var lock *foreignLock
			if acquire {
				var err error
				lock, err = acquireForeignLock(sid, cwd)
				if err != nil {
					t.Fatalf("acquire: %v", err)
				}
			} else {
				lock = &foreignLock{path: foreignLockPath(sid, cwd)}
			}
			lock.release()
			lock.release()
			if _, err := os.Stat(lock.path); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("lock file still present after release: %v", err)
			}
		})
	}
}
