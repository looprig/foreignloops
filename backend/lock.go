package backend

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
)

type foreignLock struct {
	path string
}

const temporaryForeignLockPrefix = "unbound-"

func temporaryForeignLockPath(loopID, cwd string) string {
	return filepath.Join(os.TempDir(), "looprig-foreign", "temporary", hash12(cwd)+"-"+loopID+".lock")
}

func foreignLockPath(sid, cwd string) string {
	return filepath.Join(os.TempDir(), "looprig-foreign", hash12(cwd)+"-"+sid+".lock")
}

func hash12(cwd string) string {
	sum := sha256.Sum256([]byte(filepath.Clean(cwd)))
	return hex.EncodeToString(sum[:])[:12]
}

func acquireForeignLock(sid, cwd string) (*foreignLock, error) {
	return acquireForeignLockPath(foreignLockPath(sid, cwd), sid, cwd)
}

func acquireTemporaryForeignLock(loopID, cwd string) (*foreignLock, error) {
	return acquireForeignLockPath(
		temporaryForeignLockPath(loopID, cwd),
		temporaryForeignLockPrefix+loopID,
		cwd,
	)
}

func acquireForeignLockPath(path, sid, cwd string) (*foreignLock, error) {
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, &LockError{Op: "mkdir", Path: dir, Cause: err}
	}
	lock, err := tryCreateLock(path)
	if err == nil {
		return lock, nil
	}
	if !errors.Is(err, os.ErrExist) {
		return nil, err
	}
	if pid := readLockPID(path); pid > 0 && processAlive(pid) {
		return nil, &ForeignSessionBusyError{SID: sid, Cwd: cwd, PID: pid}
	}
	_ = os.Remove(path)
	lock, err = tryCreateLock(path)
	if err == nil {
		return lock, nil
	}
	if errors.Is(err, os.ErrExist) {
		return nil, &ForeignSessionBusyError{SID: sid, Cwd: cwd, PID: readLockPID(path)}
	}
	return nil, err
}

func tryCreateLock(path string) (*foreignLock, error) {
	// #nosec G304 -- path is an app-controlled deterministic lock path under
	// os.TempDir, derived from a hash of the cleaned workspace path.
	file, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		if errors.Is(err, os.ErrExist) {
			return nil, err
		}
		return nil, &LockError{Op: "create", Path: path, Cause: err}
	}
	if _, err := file.WriteString(strconv.Itoa(os.Getpid())); err != nil {
		_ = file.Close()
		_ = os.Remove(path)
		return nil, &LockError{Op: "write", Path: path, Cause: err}
	}
	if err := file.Close(); err != nil {
		_ = os.Remove(path)
		return nil, &LockError{Op: "close", Path: path, Cause: err}
	}
	return &foreignLock{path: path}, nil
}

func readLockPID(path string) int {
	// #nosec G304 -- deterministic, app-owned lock path; see tryCreateLock.
	contents, err := os.ReadFile(path)
	if err != nil {
		return 0
	}
	pid, err := strconv.Atoi(strings.TrimSpace(string(contents)))
	if err != nil {
		return 0
	}
	return pid
}

func (l *foreignLock) release() {
	if err := os.Remove(l.path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return
	}
}

func processAlive(pid int) bool {
	if pid <= 0 {
		return false
	}
	err := syscall.Kill(pid, 0)
	return err == nil || errors.Is(err, syscall.EPERM)
}
