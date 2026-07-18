package backend_test

import (
	"context"
	"errors"
	"testing"

	"github.com/looprig/foreignloop/backend"
)

func TestBackendErrorMessagesAndUnwrap(t *testing.T) {
	t.Parallel()

	cause := errors.New("sentinel")
	tests := []struct {
		name string
		err  error
		want string
	}{
		{name: "config", err: &backend.ConfigError{Field: "Config.Agent", Reason: "required"}, want: "foreignloop: config: Config.Agent: required"},
		{name: "busy", err: &backend.ForeignSessionBusyError{SID: "sid-1", Cwd: "/workspace", PID: 42}, want: "foreignloop: session sid-1 busy (pid 42 holds /workspace lock)"},
		{name: "lock", err: &backend.LockError{Op: "create", Path: "/tmp/session.lock", Cause: cause}, want: "foreignloop: lock create /tmp/session.lock: sentinel"},
		{name: "result", err: &backend.ForeignResultError{Detail: "max turns"}, want: "foreignloop: foreign result error: max turns"},
		{name: "protocol", err: &backend.ForeignProtocolError{Reason: "missing terminal"}, want: "foreignloop: foreign protocol: missing terminal"},
		{name: "snapshot exited", err: &backend.SnapshotError{Reason: backend.SnapshotLoopExited}, want: "foreignloop: snapshot failed: loop exited"},
		{name: "snapshot context", err: &backend.SnapshotError{Reason: backend.SnapshotContextDone, Cause: context.Canceled}, want: "foreignloop: snapshot failed: context done: context canceled"},
		{name: "snapshot context no cause", err: &backend.SnapshotError{Reason: backend.SnapshotContextDone}, want: "foreignloop: snapshot failed: context done"},
		{name: "snapshot unknown", err: &backend.SnapshotError{Reason: backend.SnapshotErrorReason("future")}, want: "foreignloop: snapshot failed"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := tt.err.Error(); got != tt.want {
				t.Fatalf("Error() = %q, want %q", got, tt.want)
			}
		})
	}

	if !errors.Is(&backend.LockError{Op: "write", Path: "/tmp/lock", Cause: cause}, cause) {
		t.Fatal("errors.Is(LockError, cause) = false, want true")
	}
	if !errors.Is(&backend.SnapshotError{Reason: backend.SnapshotContextDone, Cause: cause}, cause) {
		t.Fatal("errors.Is(SnapshotError, cause) = false, want true")
	}
}

func TestSnapshotErrorReasonValues(t *testing.T) {
	t.Parallel()
	if backend.SnapshotLoopExited != "loop_exited" {
		t.Fatalf("SnapshotLoopExited = %q, want loop_exited", backend.SnapshotLoopExited)
	}
	if backend.SnapshotContextDone != "context_done" {
		t.Fatalf("SnapshotContextDone = %q, want context_done", backend.SnapshotContextDone)
	}
}
