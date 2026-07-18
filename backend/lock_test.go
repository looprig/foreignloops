package backend

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
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

func cleanupForeignLock(t *testing.T, lock *foreignLock) {
	t.Helper()
	lock.release()
	if err := os.Remove(lock.path); err != nil && !errors.Is(err, os.ErrNotExist) {
		t.Errorf("remove stable lock file: %v", err)
	}
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
			if !strings.HasSuffix(a, ".lock") || !strings.HasPrefix(filepath.Base(a), durableForeignLockPrefix) {
				t.Fatalf("lock path %q missing durable namespace or .lock suffix", a)
			}
		})
	}
}

func TestForeignLockPathContainsHashedOpaqueIdentifiers(t *testing.T) {
	t.Parallel()
	cwd := t.TempDir()
	root := filepath.Clean(filepath.Join(os.TempDir(), "looprig-foreign"))
	identifiers := []string{
		"benign-session",
		"contains/separators\\and-more",
		"../parent/../../escape",
		"/absolute/looking/session",
		strings.Repeat("very-long-session-", 1024),
		"セッション-🔒-данные",
	}
	seen := make(map[string]string)
	for _, identifier := range identifiers {
		identifier := identifier
		t.Run(fmt.Sprintf("length_%d", len(identifier)), func(t *testing.T) {
			path := filepath.Clean(foreignLockPath(identifier, cwd))
			rel, err := filepath.Rel(root, path)
			if err != nil || rel == "." || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
				t.Fatalf("lock path %q escaped root %q (rel=%q err=%v)", path, root, rel, err)
			}
			if filepath.Dir(path) != root {
				t.Fatalf("lock path %q is not an immediate child of %q", path, root)
			}
			base := filepath.Base(path)
			if strings.Contains(base, identifier) {
				t.Fatalf("lock filename embeds opaque identifier %q", identifier)
			}
			if previous, exists := seen[path]; exists {
				t.Fatalf("distinct identifiers %q and %q collided at %q", previous, identifier, path)
			}
			seen[path] = identifier
		})
	}

	if foreignLockPath("benign-a", cwd) == foreignLockPath("benign-b", cwd) {
		t.Fatal("distinct benign identifiers collided")
	}
	if foreignLockPath("same", cwd) != foreignLockPath("same", cwd+string(filepath.Separator)) {
		t.Fatal("clean-equivalent workspaces must produce the same path")
	}
	if foreignLockPath("same", cwd) == temporaryForeignLockPath("same", cwd) {
		t.Fatal("durable and temporary namespaces collided")
	}
}

func TestForeignLockOpaqueIdentifierCannotCreateOutsideRoot(t *testing.T) {
	t.Parallel()
	cwd := t.TempDir()
	outside := t.TempDir()
	root := filepath.Join(os.TempDir(), "looprig-foreign")
	legacyPrefixDir := filepath.Join(root, "legacy-prefix")
	rel, err := filepath.Rel(legacyPrefixDir, filepath.Join(outside, "owned"))
	if err != nil {
		t.Fatal(err)
	}
	maliciousID := "prefix/" + rel
	target := filepath.Join(outside, "owned.lock")

	lock, err := acquireForeignLock(maliciousID, cwd)
	if err != nil {
		t.Fatalf("acquire hashed malicious identifier: %v", err)
	}
	t.Cleanup(func() { cleanupForeignLock(t, lock) })
	if _, err := os.Stat(target); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("opaque identifier created outside lock root at %q: %v", target, err)
	}
	if filepath.Dir(lock.path) != filepath.Clean(root) {
		t.Fatalf("lock path = %q, want immediate child of %q", lock.path, root)
	}
}

func TestForeignLockRejectsSymlinkAndHardlinkFiles(t *testing.T) {
	t.Parallel()
	if err := os.MkdirAll(lockRootPath(), 0o700); err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		name  string
		stage func(*testing.T, string, string)
	}{
		{
			name: "symlink",
			stage: func(t *testing.T, target, path string) {
				if err := os.Symlink(target, path); err != nil {
					t.Fatalf("create symlink: %v", err)
				}
			},
		},
		{
			name: "hardlink",
			stage: func(t *testing.T, target, path string) {
				if err := os.Link(target, path); err != nil {
					t.Fatalf("create hardlink: %v", err)
				}
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			cwd := t.TempDir()
			path := foreignLockPath("link-"+tt.name, cwd)
			t.Cleanup(func() { _ = os.Remove(path) })
			target := filepath.Join(t.TempDir(), "outside-target")
			const sentinel = "must remain unchanged"
			if err := os.WriteFile(target, []byte(sentinel), 0o600); err != nil {
				t.Fatal(err)
			}
			tt.stage(t, target, path)

			lock, err := acquireForeignLock("link-"+tt.name, cwd)
			if lock != nil {
				cleanupForeignLock(t, lock)
				t.Fatal("acquired through linked lock file")
			}
			var lockErr *LockError
			if !errors.As(err, &lockErr) {
				t.Fatalf("error = %T %v, want LockError", err, err)
			}
			contents, readErr := os.ReadFile(target)
			if readErr != nil || string(contents) != sentinel {
				t.Fatalf("outside target changed: %q (%v)", contents, readErr)
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
	t.Cleanup(func() { cleanupForeignLock(t, temporary) })
	durable, err := acquireForeignLock(foreignSID, cwd)
	if err != nil {
		t.Fatalf("acquire durable lock: %v", err)
	}
	t.Cleanup(func() { cleanupForeignLock(t, durable) })

	if temporary.path == durable.path {
		t.Fatalf("temporary and durable lock paths collide at %q", temporary.path)
	}
	if got, want := durable.path, foreignLockPath(foreignSID, cwd); got != want {
		t.Fatalf("durable path = %q, want %q", got, want)
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
		{name: "unlocked live pid metadata is reclaimable", pre: func(t *testing.T, sid, cwd string) { preWriteLock(t, sid, cwd, strconv.Itoa(os.Getpid())) }},
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
				t.Cleanup(func() { cleanupForeignLock(t, first) })
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
			t.Cleanup(func() { cleanupForeignLock(t, lock) })
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
	sid := "00000000-0000-0000-0000-000000000001"
	cwd := t.TempDir()
	lock, err := acquireForeignLock(sid, cwd)
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	lock.release()
	lock.release()
	t.Cleanup(func() { cleanupForeignLock(t, lock) })
	if info, err := os.Stat(lock.path); err != nil || !info.Mode().IsRegular() {
		t.Fatalf("stable lock inode removed or replaced after release: %v (%v)", info, err)
	}
}

func TestForeignLockOldReleaseCannotAffectSuccessor(t *testing.T) {
	t.Parallel()
	sid := "successor-safe"
	cwd := t.TempDir()
	first, err := acquireForeignLock(sid, cwd)
	if err != nil {
		t.Fatalf("first acquire: %v", err)
	}
	first.release()

	successor, err := acquireForeignLock(sid, cwd)
	if err != nil {
		t.Fatalf("successor acquire: %v", err)
	}
	defer cleanupForeignLock(t, successor)

	var releases sync.WaitGroup
	for range 32 {
		releases.Add(1)
		go func() {
			defer releases.Done()
			first.release()
		}()
	}
	releases.Wait()

	contender, err := acquireForeignLock(sid, cwd)
	if contender != nil {
		contender.release()
		t.Fatal("old release unlocked or removed the successor lock")
	}
	var busy *ForeignSessionBusyError
	if !errors.As(err, &busy) {
		t.Fatalf("contender error = %T %v, want ForeignSessionBusyError", err, err)
	}
}

func TestForeignLockOnlyOneConcurrentOwner(t *testing.T) {
	t.Parallel()
	const contenders = 32
	cwd := t.TempDir()
	start := make(chan struct{})
	releaseWinner := make(chan struct{})
	type result struct {
		lock *foreignLock
		err  error
	}
	results := make(chan result, contenders)
	for range contenders {
		go func() {
			<-start
			lock, err := acquireForeignLock("concurrent", cwd)
			results <- result{lock: lock, err: err}
			if lock != nil {
				<-releaseWinner
				lock.release()
			}
		}()
	}
	close(start)
	winners := 0
	for range contenders {
		result := <-results
		if result.lock != nil {
			winners++
			continue
		}
		var busy *ForeignSessionBusyError
		if !errors.As(result.err, &busy) {
			t.Fatalf("loser error = %T %v, want ForeignSessionBusyError", result.err, result.err)
		}
	}
	close(releaseWinner)
	if winners != 1 {
		t.Fatalf("concurrent winners = %d, want exactly 1", winners)
	}
}

func TestForeignLockHelperProcess(t *testing.T) {
	if os.Getenv("FOREIGNLOOP_LOCK_HELPER") != "1" {
		return
	}
	sid := os.Getenv("FOREIGNLOOP_LOCK_SID")
	cwd := os.Getenv("FOREIGNLOOP_LOCK_CWD")
	lock, err := acquireForeignLock(sid, cwd)
	if err != nil {
		fmt.Fprintf(os.Stderr, "acquire: %v\n", err)
		os.Exit(2)
	}
	defer lock.release()
	fmt.Printf("locked %d\n", os.Getpid())
	_, _ = io.Copy(io.Discard, os.Stdin)
}

func TestForeignLockCrashIsNaturallyReclaimable(t *testing.T) {
	if os.Getenv("FOREIGNLOOP_LOCK_HELPER") == "1" {
		return
	}
	cwd := t.TempDir()
	sid := "crash-reclaim"
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	command := exec.Command(executable, "-test.run=^TestForeignLockHelperProcess$")
	command.Env = append(os.Environ(),
		"FOREIGNLOOP_LOCK_HELPER=1",
		"FOREIGNLOOP_LOCK_SID="+sid,
		"FOREIGNLOOP_LOCK_CWD="+cwd,
	)
	stdout, err := command.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	stdin, err := command.StdinPipe()
	if err != nil {
		t.Fatal(err)
	}
	defer stdin.Close()
	command.Stderr = os.Stderr
	if err := command.Start(); err != nil {
		t.Fatal(err)
	}
	scanner := bufio.NewScanner(stdout)
	if !scanner.Scan() || !strings.HasPrefix(scanner.Text(), "locked ") {
		_ = command.Process.Kill()
		_ = command.Wait()
		t.Fatalf("helper did not report lock acquisition: %q (%v)", scanner.Text(), scanner.Err())
	}

	contender, err := acquireForeignLock(sid, cwd)
	if contender != nil {
		contender.release()
		t.Fatal("parent acquired while helper held lock")
	}
	var busy *ForeignSessionBusyError
	if !errors.As(err, &busy) || busy.PID != command.Process.Pid {
		t.Fatalf("busy error = %#v (%v), want helper pid %d", busy, err, command.Process.Pid)
	}
	if err := command.Process.Kill(); err != nil {
		t.Fatalf("kill helper: %v", err)
	}
	if err := command.Wait(); err == nil {
		t.Fatal("killed helper exited successfully")
	}

	reclaimed, err := acquireForeignLock(sid, cwd)
	if err != nil {
		t.Fatalf("reclaim after helper crash: %v", err)
	}
	cleanupForeignLock(t, reclaimed)
}
