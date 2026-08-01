package acp

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/looprig/acp/client"
	"github.com/looprig/acp/protocol"
	"github.com/looprig/core/content"
	"github.com/looprig/foreignloops/driver"
)

type interruptContextKey struct{}

type interruptSession struct {
	id      protocol.SessionID
	updates chan client.Update

	promptStarts   chan struct{}
	promptContexts chan context.Context
	promptAborted  chan error
	cancelContexts chan context.Context
	cancelPrompt   chan struct{}
	cancelOnce     sync.Once

	promptCalls atomic.Int32
	cancelCalls atomic.Int32
	cancelErr   error
	waitCancel  bool
}

func newInterruptSession(waitCancel bool) *interruptSession {
	return &interruptSession{
		id:             "interrupt-session",
		updates:        make(chan client.Update, 16),
		promptStarts:   make(chan struct{}, 4),
		promptContexts: make(chan context.Context, 4),
		promptAborted:  make(chan error, 4),
		cancelContexts: make(chan context.Context, 4),
		cancelPrompt:   make(chan struct{}),
		waitCancel:     waitCancel,
	}
}

func (s *interruptSession) ID() protocol.SessionID { return s.id }

func (s *interruptSession) ConfigOptions() []protocol.SessionConfigOption { return nil }

func (s *interruptSession) Modes() *protocol.SessionModeState { return nil }

func (s *interruptSession) SetConfigOption(context.Context, protocol.SessionConfigID, protocol.SessionConfigValueID) error {
	return nil
}

func (s *interruptSession) SetMode(context.Context, protocol.SessionModeID) error { return nil }

func (s *interruptSession) Prompt(ctx context.Context, _ []protocol.ContentBlock) (*client.PromptResult, error) {
	call := s.promptCalls.Add(1)
	s.promptContexts <- ctx
	s.promptStarts <- struct{}{}
	if call == 1 && s.waitCancel {
		select {
		case <-s.cancelPrompt:
			s.updates <- textUpdate("partial before interrupt")
			return &client.PromptResult{StopReason: protocol.StopReasonCancelled}, nil
		case <-ctx.Done():
			s.promptAborted <- ctx.Err()
			return nil, ctx.Err()
		}
	}
	s.updates <- textUpdate("successful second turn")
	return &client.PromptResult{StopReason: protocol.StopReasonEndTurn}, nil
}

func (s *interruptSession) Updates() <-chan client.Update { return s.updates }

func (s *interruptSession) Cancel(ctx context.Context) error {
	s.cancelCalls.Add(1)
	s.cancelContexts <- ctx
	if s.cancelErr != nil {
		return s.cancelErr
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	s.cancelOnce.Do(func() { close(s.cancelPrompt) })
	return nil
}

func textUpdate(text string) client.Update {
	return client.Update{SessionUpdate: protocol.SessionUpdate{
		AgentMessageChunk: &protocol.ContentChunk{Content: protocol.ContentBlock{
			Text: &protocol.TextContent{Text: text},
		}},
	}}
}

func TestInterruptWatcherCancelsProtocolAndPreservesSession(t *testing.T) {
	driverCtx := context.WithValue(context.Background(), interruptContextKey{}, "driver-owned")
	sess := newInterruptSession(true)
	d := &Driver{session: sess, driverCtx: driverCtx}

	turnCtx, cancelTurn := context.WithCancel(context.Background())
	first, err := d.Spawn(turnCtx, driver.Turn{
		SystemPrompt: "system",
		Input:        []content.Block{&content.TextBlock{Text: "interrupt me"}},
	})
	if err != nil {
		t.Fatalf("first Spawn() error = %v", err)
	}
	<-sess.promptStarts
	promptCtx := <-sess.promptContexts
	if promptCtx.Value(interruptContextKey{}) != "driver-owned" {
		t.Fatalf("Prompt context did not carry driver-owned value: %v", promptCtx)
	}

	cancelTurn()
	select {
	case cancelCtx := <-sess.cancelContexts:
		if cancelCtx.Value(interruptContextKey{}) != "driver-owned" {
			t.Fatalf("Cancel context did not carry driver-owned value: %v", cancelCtx)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for session/cancel")
	}

	firstEvents := collectTurnEvents(t, first)
	if len(firstEvents) == 0 || firstEvents[len(firstEvents)-1].Kind != driver.KindTerminalOK {
		t.Fatalf("interrupted first events = %#v, want terminal OK after drain", firstEvents)
	}
	if got := sess.cancelCalls.Load(); got != 1 {
		t.Fatalf("session Cancel calls = %d, want 1", got)
	}
	select {
	case err := <-sess.promptAborted:
		t.Fatalf("Prompt was aborted by its context: %v", err)
	default:
	}
	waitTurnDone(t, first)

	second, err := d.Spawn(context.Background(), driver.Turn{
		Input: []content.Block{&content.TextBlock{Text: "continue"}},
	})
	if err != nil {
		t.Fatalf("second Spawn() error = %v", err)
	}
	secondEvents := collectTurnEvents(t, second)
	if len(secondEvents) == 0 || secondEvents[len(secondEvents)-1].Kind != driver.KindTerminalOK {
		t.Fatalf("second events = %#v, want successful terminal", secondEvents)
	}
	waitTurnDone(t, second)
	if got := sess.promptCalls.Load(); got != 2 {
		t.Fatalf("Prompt calls = %d, want 2 on the persistent session", got)
	}
}

func TestInterruptWatcherJoinsAfterNormalTurn(t *testing.T) {
	sess := newInterruptSession(false)
	d := &Driver{session: sess, driverCtx: context.Background()}

	stream, err := d.Spawn(context.Background(), driver.Turn{
		Input: []content.Block{&content.TextBlock{Text: "complete"}},
	})
	if err != nil {
		t.Fatalf("Spawn() error = %v", err)
	}
	_ = collectTurnEvents(t, stream)
	waitTurnDone(t, stream)
	select {
	case <-sess.cancelContexts:
		t.Fatal("normal completion unexpectedly sent session/cancel")
	default:
	}
}

func TestInterruptWatcherLogsCancelFailure(t *testing.T) {
	oldLogger := slog.Default()
	var logs bytes.Buffer
	slog.SetDefault(slog.New(slog.NewTextHandler(&logs, nil)))
	t.Cleanup(func() { slog.SetDefault(oldLogger) })

	sess := newInterruptSession(true)
	sess.cancelErr = errors.New("cancel failed")
	turnCtx, cancelTurn := context.WithCancel(context.Background())
	turnDone := make(chan struct{})
	watcherDone := make(chan struct{})
	go watchTurnCancellation(turnCtx, context.Background(), sess, turnDone, watcherDone, &turnLifecycle{})
	cancelTurn()

	select {
	case <-watcherDone:
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for cancellation watcher")
	}
	if !strings.Contains(logs.String(), "acp: session cancel failed") {
		t.Fatalf("logs = %q, want bounded cancellation warning", logs.String())
	}
}

func waitTurnDone(t *testing.T, value driver.Stream) {
	t.Helper()
	s, ok := value.(*stream)
	if !ok {
		t.Fatalf("stream type = %T, want *stream", value)
	}
	select {
	case <-s.done:
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for turn and cancellation watcher to join")
	}
}

// Task 13 adds Cancel to the live turn seam. Keep Task 12's scripted session
// usable for its existing translation tests without changing those fixtures.
func (s *scriptedSession) Cancel(context.Context) error { return nil }

var _ turnSession = (*interruptSession)(nil)
