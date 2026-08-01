package acp

import (
	"context"
	"errors"
	"log/slog"
	"strings"
	"sync"

	"github.com/looprig/acp/client"
	"github.com/looprig/acp/protocol"
	"github.com/looprig/core/content"
	"github.com/looprig/foreignloops/driver"
)

// turnSession is the live-session portion used after construction. Keeping it
// separate from session lets construction tests use a narrow setup seam while
// the concrete ACP client supplies prompt and update delivery for turns.
type turnSession interface {
	session
	Prompt(context.Context, []protocol.ContentBlock) (*client.PromptResult, error)
	Updates() <-chan client.Update
	Cancel(context.Context) error
}

type promptOutcome struct {
	result *client.PromptResult
	err    error
}

// turnLifecycle closes the small race between a prompt completing and its
// caller context being cancelled. A watcher that wins the race reserves the
// cancellation before calling the ACP session; a completed turn prevents a
// late session/cancel from reaching the next turn.
type turnLifecycle struct {
	mu         sync.Mutex
	finished   bool
	cancelling bool
}

func (l *turnLifecycle) beginCancel() bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.finished || l.cancelling {
		return false
	}
	l.cancelling = true
	return true
}

func (l *turnLifecycle) finish() {
	l.mu.Lock()
	l.finished = true
	l.mu.Unlock()
}

// stream is one prompt view over Driver's persistent ACP session. Its close
// function only cancels forwarding for this turn; the session and its owned
// process remain with Driver.
type stream struct {
	events <-chan driver.Event
	done   <-chan struct{}
	cancel context.CancelFunc

	once     sync.Once
	closeErr error
}

// Spawn starts one prompt on the session created by New. The prompt itself is
// deliberately run under the driver's context. The caller's context controls
// only this stream's forwarding lifetime; protocol cancellation is handled by
// the interrupt watcher in the next ACP turn phase.
func (d *Driver) Spawn(ctx context.Context, turn driver.Turn) (driver.Stream, error) {
	if d == nil || d.session == nil {
		return nil, &driver.SpawnError{Cause: errors.New("acp: session unavailable")}
	}
	sess, ok := d.session.(turnSession)
	if !ok {
		return nil, &driver.SpawnError{Cause: errors.New("acp: session does not support turns")}
	}
	if ctx == nil {
		ctx = context.Background()
	}
	d.turnMu.Lock()

	driverCtx := d.driverCtx
	if driverCtx == nil {
		driverCtx = context.Background()
	}
	turnCtx, cancel := context.WithCancel(ctx)
	events := make(chan driver.Event, 32)
	done := make(chan struct{})
	s := &stream{
		events: events,
		done:   done,
		cancel: cancel,
	}
	go d.runTurn(turnCtx, driverCtx, sess, turn, events, done)
	return s, nil
}

func (d *Driver) runTurn(
	turnCtx, driverCtx context.Context,
	sess turnSession,
	turn driver.Turn,
	events chan<- driver.Event,
	done chan<- struct{},
) {
	turnDone := make(chan struct{})
	watcherDone := make(chan struct{})
	lifecycle := &turnLifecycle{}
	go watchTurnCancellation(turnCtx, driverCtx, sess, turnDone, watcherDone, lifecycle)
	defer func() {
		lifecycle.finish()
		close(turnDone)
		<-watcherDone
		close(done)
		close(events)
		d.turnMu.Unlock()
	}()

	promptDone := make(chan promptOutcome, 1)
	go func() {
		result, err := sess.Prompt(driverCtx, promptBlocks(turn))
		promptDone <- promptOutcome{result: result, err: err}
	}()

	state := &translationState{}
	updates := sess.Updates()
	for {
		select {
		case update, ok := <-updates:
			if !ok {
				updates = nil
				continue
			}
			for _, event := range translateUpdate(update.SessionUpdate, state) {
				sendTurnEvent(turnCtx, events, event)
			}
		case outcome := <-promptDone:
			// ACP sends session updates before the prompt response. Drain any
			// notifications already queued so the terminal remains last.
			drainTurnUpdates(turnCtx, updates, state, events)
			sendPromptTerminal(turnCtx, events, state, outcome)
			return
		}
	}
}

func watchTurnCancellation(
	turnCtx, driverCtx context.Context,
	sess turnSession,
	turnDone <-chan struct{},
	watcherDone chan<- struct{},
	lifecycle *turnLifecycle,
) {
	defer close(watcherDone)
	select {
	case <-turnCtx.Done():
		if !lifecycle.beginCancel() {
			return
		}
		if err := sess.Cancel(driverCtx); err != nil {
			slog.Warn("acp: session cancel failed", "error", err)
		}
	case <-turnDone:
	}
}

func drainTurnUpdates(
	turnCtx context.Context,
	updates <-chan client.Update,
	state *translationState,
	events chan<- driver.Event,
) {
	if updates == nil {
		return
	}
	for {
		select {
		case update, ok := <-updates:
			if !ok {
				return
			}
			for _, event := range translateUpdate(update.SessionUpdate, state) {
				sendTurnEvent(turnCtx, events, event)
			}
		default:
			return
		}
	}
}

func sendTurnEvent(ctx context.Context, events chan<- driver.Event, event driver.Event) {
	select {
	case events <- event:
	case <-ctx.Done():
		// The stream is closed or its caller context has ended. Keep draining
		// the ACP session until Prompt returns, but release this stream's
		// consumer without blocking on an abandoned channel.
	}
}

func sendPromptTerminal(ctx context.Context, events chan<- driver.Event, state *translationState, outcome promptOutcome) {
	if outcome.err != nil || outcome.result == nil {
		sendTerminalEvent(ctx, events, driver.Event{
			Kind:    driver.KindTerminalError,
			ErrText: "acp prompt failed",
		})
		return
	}
	if message := state.message(); message != nil {
		sendTurnEvent(ctx, events, driver.Event{
			Kind:    driver.KindStepComplete,
			Message: message,
		})
	}
	sendTerminalEvent(ctx, events, terminalEvent(outcome.result.StopReason))
}

// sendTerminalEvent preserves the terminal marker when a cancelled turn is
// still being drained. If the caller has abandoned the stream and its buffer
// is full, the non-blocking fallback keeps the persistent session from being
// wedged behind an unobserved stream; the backend already treats the canceled
// turn context as interrupted.
func sendTerminalEvent(ctx context.Context, events chan<- driver.Event, event driver.Event) {
	select {
	case events <- event:
	case <-ctx.Done():
		select {
		case events <- event:
		default:
		}
	}
}

func terminalEvent(reason protocol.StopReason) driver.Event {
	switch reason {
	case protocol.StopReasonEndTurn, protocol.StopReasonCancelled:
		return driver.Event{Kind: driver.KindTerminalOK}
	case protocol.StopReasonMaxTokens:
		return driver.Event{Kind: driver.KindTerminalError, ErrText: "acp prompt reached its token limit"}
	case protocol.StopReasonMaxTurnRequests:
		return driver.Event{Kind: driver.KindTerminalError, ErrText: "acp prompt reached its turn limit"}
	case protocol.StopReasonRefusal:
		return driver.Event{Kind: driver.KindTerminalError, ErrText: "acp prompt was refused"}
	default:
		return driver.Event{Kind: driver.KindTerminalError, ErrText: "acp prompt ended with an unknown stop reason"}
	}
}

func promptBlocks(turn driver.Turn) []protocol.ContentBlock {
	var task strings.Builder
	task.WriteString("<looprig-system>")
	task.WriteString(turn.SystemPrompt)
	task.WriteString("</looprig-system>\n\n<user-task>")
	for _, block := range turn.Input {
		switch typed := block.(type) {
		case *content.TextBlock:
			task.WriteString(typed.Text)
		case *content.DocumentBlock:
			task.WriteString(typed.Text)
		}
	}
	task.WriteString("</user-task>")
	return []protocol.ContentBlock{{Text: &protocol.TextContent{Text: task.String()}}}
}

func (s *stream) Events() <-chan driver.Event { return s.events }

func (s *stream) History() (driver.History, error) {
	return driver.History{Available: false}, nil
}

// Close cancels only this stream's forwarding context. The ACP session remains
// open and the turn goroutine continues draining until Prompt returns so a
// later turn cannot consume updates from an abandoned prompt.
func (s *stream) Close() error {
	if s == nil {
		return nil
	}
	s.once.Do(func() {
		if s.cancel != nil {
			s.cancel()
		}
	})
	return s.closeErr
}

var _ driver.Agent = (*Driver)(nil)
var _ driver.Stream = (*stream)(nil)
