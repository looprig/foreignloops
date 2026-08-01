package backend

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/looprig/foreignloops/driver"
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
