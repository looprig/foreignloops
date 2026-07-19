package backend

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/looprig/core/uuid"
	"github.com/looprig/foreignloops/driver"
	"github.com/looprig/harness/pkg/command"
	"github.com/looprig/harness/pkg/event"
	"github.com/looprig/harness/pkg/loop"
)

func newTestLoop(t *testing.T, cfg Config, pub *fakePublisher) (*Loop, string) {
	t.Helper()
	if cfg.Cwd == "" {
		cfg.Cwd = t.TempDir()
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	state, sid, err := New(ctx, mustID(t), mustID(t), loop.Provenance{}, pub, validBoundDefinition(), cfg, seqIDGen(), workingFac())
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return state, sid
}

func shutdown(t *testing.T, state *Loop) {
	t.Helper()
	ack := make(chan error, 1)
	select {
	case state.Commands <- command.Shutdown{Ack: ack}:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out sending shutdown")
	}
	if err := <-ack; err != nil {
		t.Fatalf("shutdown: %v", err)
	}
	select {
	case <-state.Done:
	case <-time.After(2 * time.Second):
		t.Fatal("Done did not close")
	}
}

func TestBackendInterfaceAndConstruction(t *testing.T) {
	t.Parallel()
	var _ loop.Backend = (*Loop)(nil)
	state, sid := newTestLoop(t, Config{Agent: &fakeAgent{}, Posture: driver.PostureDefault, SIDMode: SIDPrebound}, &fakePublisher{})
	if sid == "" {
		t.Fatal("prebound sid is empty")
	}
	shutdown(t, state)
}

func TestNewLateBoundDoesNotMintSID(t *testing.T) {
	t.Parallel()
	calls := 0
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	state, sid, err := New(ctx, mustID(t), mustID(t), loop.Provenance{}, &fakePublisher{}, validBoundDefinition(), Config{Agent: &fakeAgent{}, Cwd: t.TempDir(), SIDMode: SIDLateBound}, func() (uuid.UUID, error) {
		calls++
		return uuid.New()
	}, workingFac())
	if err != nil || sid != "" || calls != 0 {
		t.Fatalf("New late-bound = state %v sid %q calls %d err %v", state != nil, sid, calls, err)
	}
	shutdown(t, state)
}

func TestNewPreboundMintFailureUsesDriverSpawnError(t *testing.T) {
	t.Parallel()
	sentinel := errors.New("mint")
	state, sid, err := New(context.Background(), mustID(t), mustID(t), loop.Provenance{}, &fakePublisher{}, validBoundDefinition(), Config{Agent: &fakeAgent{}, Cwd: t.TempDir(), SIDMode: SIDPrebound}, func() (uuid.UUID, error) {
		return uuid.UUID{}, sentinel
	}, workingFac())
	if state != nil || sid != "" {
		t.Fatalf("error returned state=%v sid=%q", state, sid)
	}
	var spawnErr *driver.SpawnError
	if !errors.As(err, &spawnErr) || !errors.Is(err, sentinel) {
		t.Fatalf("err = %T %v, want SpawnError wrapping sentinel", err, err)
	}
}

func TestInterruptWhileIdleAndSnapshotAfterExit(t *testing.T) {
	t.Parallel()
	state, _ := newTestLoop(t, Config{Agent: &fakeAgent{}, SIDMode: SIDPrebound}, &fakePublisher{})
	ack := make(chan bool, 1)
	state.Commands <- command.Interrupt{Ack: ack}
	if <-ack {
		t.Fatal("idle interrupt reported active")
	}
	shutdown(t, state)
	_, _, err := state.Snapshot(context.Background())
	var snapshotErr *SnapshotError
	if !errors.As(err, &snapshotErr) || snapshotErr.Reason != SnapshotLoopExited {
		t.Fatalf("Snapshot after exit = %T %v", err, err)
	}
}

func TestManagedAcceptanceFailureStartsNoWork(t *testing.T) {
	t.Parallel()
	sentinel := errors.New("append failed")
	agent := &fakeAgent{}
	pub := &fakePublisher{checkedErr: sentinel}
	state, _ := newTestLoop(t, Config{Agent: agent, SIDMode: SIDLateBound}, pub)
	accepted := make(chan error, 1)
	state.Commands <- command.UserInput{Header: command.Header{CommandID: mustID(t)}, NoFold: true, TargetLoopID: state.loopID, Accepted: accepted}
	if got := <-accepted; got != sentinel {
		t.Fatalf("Accepted = %v, want sentinel", got)
	}
	if agent.calls() != 0 {
		t.Fatalf("spawn calls = %d, want 0", agent.calls())
	}
	shutdown(t, state)
}

func TestBuildWithAndRestoredStartActorAndPreserveNilOnError(t *testing.T) {
	t.Parallel()
	cfg := Config{Agent: &fakeAgent{}, Cwd: t.TempDir(), SIDMode: SIDPrebound}
	built, sid, err := BuildWith(cfg)(context.Background(), mustID(t), mustID(t), loop.Provenance{}, &fakePublisher{}, validBoundDefinition(), seqIDGen(), workingFac())
	if err != nil || built == nil || sid == "" {
		t.Fatalf("BuildWith = %T %q %v", built, sid, err)
	}
	shutdown(t, built.(*Loop))

	bad, sid, err := BuildWith(Config{})(context.Background(), uuid.UUID{}, uuid.UUID{}, loop.Provenance{}, nil, nil, nil, nil)
	if err == nil || bad != nil || sid != "" {
		t.Fatalf("invalid BuildWith = %T %q %v", bad, sid, err)
	}
}

func TestSnapshotFreshLoop(t *testing.T) {
	t.Parallel()
	state, _ := newTestLoop(t, Config{Agent: &fakeAgent{}, SIDMode: SIDPrebound}, &fakePublisher{})
	msgs, index, err := state.Snapshot(context.Background())
	if err != nil || len(msgs) != 0 || index != event.TurnIndex(0) {
		t.Fatalf("Snapshot = %v %d %v", msgs, index, err)
	}
	shutdown(t, state)
}
