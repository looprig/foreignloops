package backend

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/looprig/foreignloops/driver"
	"github.com/looprig/harness/pkg/command"
	"github.com/looprig/harness/pkg/event"
	"github.com/looprig/harness/pkg/loop"
)

type recordingCloserAgent struct {
	mu        sync.Mutex
	closeErr  error
	closeCall int
}

var (
	_ driver.Agent  = (*recordingCloserAgent)(nil)
	_ driver.Closer = (*recordingCloserAgent)(nil)
)

func (*recordingCloserAgent) Spawn(context.Context, driver.Turn) (driver.Stream, error) {
	return nil, errors.New("recordingCloserAgent.Spawn must not be called")
}

func (a *recordingCloserAgent) Close() error {
	a.mu.Lock()
	a.closeCall++
	err := a.closeErr
	a.mu.Unlock()
	return err
}

func (a *recordingCloserAgent) closeCalls() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.closeCall
}

type blockingCloserAgent struct {
	fakeAgent
	started     chan struct{}
	release     chan struct{}
	startedOnce sync.Once
	releaseOnce sync.Once
}

var (
	_ driver.Agent  = (*blockingCloserAgent)(nil)
	_ driver.Closer = (*blockingCloserAgent)(nil)
)

func newBlockingCloserAgent(active bool) *blockingCloserAgent {
	return &blockingCloserAgent{
		fakeAgent: fakeAgent{block: active},
		started:   make(chan struct{}),
		release:   make(chan struct{}),
	}
}

func (a *blockingCloserAgent) Close() error {
	a.startedOnce.Do(func() { close(a.started) })
	<-a.release
	return nil
}

func (a *blockingCloserAgent) releaseClose() {
	a.releaseOnce.Do(func() { close(a.release) })
}

func (a *blockingCloserAgent) waitCloseStarted(t *testing.T) {
	t.Helper()
	select {
	case <-a.started:
	case <-time.After(2 * time.Second):
		t.Fatal("agent Close did not start")
	}
}

func assertShutdownAckAfterClose(t *testing.T, state *Loop, agent *blockingCloserAgent) {
	t.Helper()
	ack := make(chan error, 1)
	select {
	case state.Commands <- command.Shutdown{Ack: ack}:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out sending shutdown")
	}
	agent.waitCloseStarted(t)
	select {
	case err := <-ack:
		t.Fatalf("shutdown ack arrived while Close blocked: %v", err)
	default:
	}
	agent.releaseClose()
	select {
	case err := <-ack:
		if err != nil {
			t.Fatalf("shutdown: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("shutdown ack did not arrive after Close returned")
	}
	select {
	case <-state.Done:
	case <-time.After(2 * time.Second):
		t.Fatal("Done did not close after shutdown")
	}
}

func TestBackendClosesCloserAgent(t *testing.T) {
	t.Run("shutdown", func(t *testing.T) {
		agent := &recordingCloserAgent{}
		state, _ := newTestLoop(t, Config{Agent: agent, SIDMode: SIDPrebound}, &fakePublisher{})

		shutdown(t, state)

		if got := agent.closeCalls(); got != 1 {
			t.Fatalf("Close calls = %d, want 1", got)
		}
	})

	t.Run("close error does not fail shutdown", func(t *testing.T) {
		agent := &recordingCloserAgent{closeErr: errors.New("close failed")}
		state, _ := newTestLoop(t, Config{Agent: agent, SIDMode: SIDPrebound}, &fakePublisher{})

		shutdown(t, state)

		if got := agent.closeCalls(); got != 1 {
			t.Fatalf("Close calls = %d, want 1", got)
		}
	})
}

func TestBackendShutdownAckWaitsForCloser(t *testing.T) {
	t.Run("idle", func(t *testing.T) {
		agent := newBlockingCloserAgent(false)
		t.Cleanup(agent.releaseClose)
		state, _ := newTestLoop(t, Config{Agent: agent, SIDMode: SIDPrebound}, &fakePublisher{})

		assertShutdownAckAfterClose(t, state, agent)
	})

	t.Run("active turn", func(t *testing.T) {
		agent := newBlockingCloserAgent(true)
		t.Cleanup(agent.releaseClose)
		pub := &fakePublisher{}
		state, _ := newTestLoop(t, Config{Agent: agent, SIDMode: SIDPrebound}, pub)
		submit(t, state, "active")
		waitFor(t, pub, func(input event.Event) bool {
			_, ok := input.(event.TurnStarted)
			return ok
		})

		assertShutdownAckAfterClose(t, state, agent)
	})
}

func TestBackendShutdownWithoutCloserStillWorks(t *testing.T) {
	state, _ := newTestLoop(t, Config{Agent: &fakeAgent{}, SIDMode: SIDPrebound}, &fakePublisher{})

	shutdown(t, state)
}

func TestBackendClosesCloserAgentOnLoopContextCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	agent := &recordingCloserAgent{}
	state, _, err := New(ctx, mustID(t), mustID(t), loop.Provenance{}, &fakePublisher{}, validBoundDefinition(), Config{
		Agent:   agent,
		Cwd:     t.TempDir(),
		SIDMode: SIDPrebound,
	}, seqIDGen(), workingFac())
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	cancel()
	select {
	case <-state.Done:
	case <-time.After(2 * time.Second):
		t.Fatal("Done did not close after loop context cancellation")
	}

	if got := agent.closeCalls(); got != 1 {
		t.Fatalf("Close calls = %d, want 1", got)
	}
}
