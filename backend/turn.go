package backend

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"

	"github.com/looprig/core/content"
	"github.com/looprig/core/uuid"
	"github.com/looprig/foreignloops/driver"
	"github.com/looprig/harness/pkg/command"
	"github.com/looprig/harness/pkg/event"
	"github.com/looprig/harness/pkg/identity"
	"github.com/looprig/harness/pkg/loop"
)

type turnOutcome struct {
	committed   content.AgenticMessages
	success     bool
	interrupted bool
	spawned     bool
	boundSID    string
}

type drainedTurn struct {
	assistant []*content.AIMessage
	boundSID  string
	termErr   error
	bindErr   error
	terminal  bool
}

type turnLock interface{ release() }

type turnLockOps struct {
	acquireTemporary func(loopID, cwd string) (turnLock, error)
	acquireDurable   func(sid, cwd string) (turnLock, error)
}

func productionTurnLockOps() turnLockOps {
	return turnLockOps{
		acquireTemporary: func(loopID, cwd string) (turnLock, error) { return acquireTemporaryForeignLock(loopID, cwd) },
		acquireDurable:   func(sid, cwd string) (turnLock, error) { return acquireForeignLock(sid, cwd) },
	}
}

func (l *Loop) runTurn(loopCtx context.Context, prepared preparedInput) bool {
	input := prepared.command
	cur := l.turnIndex + 1
	pub := l.publisher(loopCtx, prepared.turnID, prepared.stepID)
	user := &content.UserMessage{Message: content.Message{Role: content.RoleUser, Blocks: input.Blocks}}
	pub(event.TurnStarted{
		Header:    event.Header{Cause: identity.Cause{CommandID: input.Header.CommandID, Agency: input.Header.Agency}},
		TurnIndex: cur,
		Message:   user,
	})
	turn := driver.Turn{
		SystemPrompt: l.cfg.EffectiveSystem(),
		ForeignSID:   l.sid,
		StartNew:     !l.hasSpawned,
		Input:        input.Blocks,
		Cwd:          l.backendCfg.Cwd,
		Posture:      l.backendCfg.Posture,
	}
	turnCtx, cancel := context.WithCancel(loopCtx)
	result := make(chan turnOutcome, 1)
	go l.driveTurn(turnCtx, cancel, turn, cur, l.sidBound, pub, result)
	return l.awaitTurn(loopCtx, cur, input.CommandID, cancel, pub, result)
}

func (l *Loop) awaitTurn(loopCtx context.Context, cur event.TurnIndex, activeCommandID uuid.UUID,
	cancel context.CancelFunc, pub func(event.Event), result chan turnOutcome,
) bool {
	for {
		select {
		case outcome := <-result:
			l.applyOutcome(cur, outcome, pub)
			if !outcome.success && !outcome.interrupted {
				l.cancelPending(pub, event.CancelTurnFailed)
			}
			return false
		case req := <-l.snapshots:
			req.reply <- snapshotResult{msgs: cloneMessages(l.msgs), turnIndex: l.turnIndex}
		case input := <-l.Commands:
			if done, exit := l.handleTurnCommand(loopCtx, input, cur, activeCommandID, cancel, pub, result); done {
				return exit
			}
		case <-loopCtx.Done():
			cancel()
			<-result
			return true
		}
	}
}

func (l *Loop) handleTurnCommand(loopCtx context.Context, input command.Command, cur event.TurnIndex,
	activeCommandID uuid.UUID, cancel context.CancelFunc, pub func(event.Event), result chan turnOutcome,
) (bool, bool) {
	switch typed := input.(type) {
	case command.UserInput:
		if len(l.pending) >= loop.ManagedInputQueueCapacity {
			pub(event.TurnRejected{Header: event.Header{Cause: identity.Cause{CommandID: typed.CommandID}}, Reason: event.RejectQueueFull})
			if typed.Accepted != nil {
				typed.Accepted <- &loop.InputRejectedError{Reason: event.RejectQueueFull}
			}
			return false, false
		}
		prepared, ok := l.prepareInput(loopCtx, typed)
		if !ok {
			return false, false
		}
		l.pending = append(l.pending, prepared)
		pub(event.InputQueued{Header: event.Header{Cause: identity.Cause{CommandID: typed.CommandID}}})
		if typed.Accepted != nil {
			typed.Accepted <- nil
		}
		return false, false
	case command.CancelDelegateRequest:
		if err := typed.Validate(); err != nil {
			slog.Warn("foreignloop: invalid CancelDelegateRequest", "error", err)
			return false, false
		}
		if err := command.ValidateCommand(typed); err != nil {
			typed.Ack <- command.DelegateCancelNoop
			return false, false
		}
		for i, pending := range l.pending {
			if pending.command.CommandID != typed.TargetCommandID {
				continue
			}
			l.pending = append(l.pending[:i], l.pending[i+1:]...)
			queued := pending.command
			pub(event.InputCancelled{
				Header:  event.Header{Cause: identity.Cause{CommandID: queued.CommandID, Agency: queued.Agency}},
				Reason:  event.CancelClientRetracted,
				Message: &content.UserMessage{Message: content.Message{Role: content.RoleUser, Blocks: queued.Blocks}},
			})
			typed.Ack <- command.DelegateCancelQueued
			return false, false
		}
		if typed.TargetCommandID != activeCommandID {
			typed.Ack <- command.DelegateCancelNoop
			return false, false
		}
		select {
		case outcome := <-result:
			l.applyOutcome(cur, outcome, pub)
			if !outcome.success && !outcome.interrupted {
				l.cancelPending(pub, event.CancelTurnFailed)
			}
			typed.Ack <- command.DelegateCancelNoop
			return true, false
		default:
		}
		cancel()
		l.applyOutcome(cur, <-result, pub)
		typed.Ack <- command.DelegateCancelActive
		return true, false
	case command.Interrupt:
		cancel()
		l.applyOutcome(cur, <-result, pub)
		l.cancelPending(pub, event.CancelTurnInterrupted)
		typed.Ack <- true
		return true, false
	case command.Shutdown:
		cancel()
		<-result
		l.cancelPending(pub, event.CancelTurnInterrupted)
		typed.Ack <- nil
		return true, true
	default:
		slog.Warn("foreignloop: dropping un-honorable command during turn", "type", fmt.Sprintf("%T", input))
		return false, false
	}
}

func (l *Loop) cancelPending(pub func(event.Event), reason event.CancelReason) {
	for _, pending := range l.pending {
		input := pending.command
		pub(event.InputCancelled{
			Header:  event.Header{Cause: identity.Cause{CommandID: input.CommandID, Agency: input.Agency}},
			Reason:  reason,
			Message: &content.UserMessage{Message: content.Message{Role: content.RoleUser, Blocks: input.Blocks}},
		})
	}
	l.pending = nil
}

func (l *Loop) applyOutcome(cur event.TurnIndex, outcome turnOutcome, pub func(event.Event)) {
	l.applyBoundSID(outcome.boundSID)
	if outcome.interrupted {
		pub(event.TurnInterrupted{TurnIndex: cur})
		return
	}
	l.msgs = append(l.msgs, outcome.committed...)
	if outcome.spawned {
		l.hasSpawned = true
	}
	if outcome.success {
		l.turnIndex = cur
	}
}

func (l *Loop) applyBoundSID(sid string) {
	if sid == "" {
		return
	}
	l.sid = sid
	l.sidBound = true
	l.hasSpawned = true
}

func (l *Loop) driveTurn(turnCtx context.Context, cancel context.CancelFunc, turn driver.Turn,
	cur event.TurnIndex, sidBound bool, pub func(event.Event), result chan turnOutcome,
) {
	l.driveTurnWithLocks(turnCtx, cancel, turn, cur, sidBound, pub, result, productionTurnLockOps())
}

func (l *Loop) driveTurnWithLocks(turnCtx context.Context, cancel context.CancelFunc, turn driver.Turn,
	cur event.TurnIndex, sidBound bool, pub func(event.Event), result chan turnOutcome, locks turnLockOps,
) {
	defer cancel()
	var (
		lock turnLock
		err  error
	)
	if sidBound {
		lock, err = locks.acquireDurable(turn.ForeignSID, l.backendCfg.Cwd)
	} else {
		lock, err = locks.acquireTemporary(l.loopID.String(), l.backendCfg.Cwd)
	}
	if err != nil {
		pub(event.TurnFailed{TurnIndex: cur, Err: err})
		result <- turnOutcome{}
		return
	}
	var outcome turnOutcome
	defer func() { result <- outcome }()
	defer func() { lock.release() }()
	stream, err := l.backendCfg.Agent.Spawn(turnCtx, turn)
	if err != nil {
		pub(event.TurnFailed{TurnIndex: cur, Err: &driver.SpawnError{Cause: err}})
		return
	}
	bindSID := func(sid string) error {
		boundLock, err := locks.acquireDurable(sid, l.backendCfg.Cwd)
		if err != nil {
			return err
		}
		lock.release()
		lock = boundLock
		return nil
	}
	drained := l.drainStream(stream, cur, sidBound, turn.ForeignSID, bindSID, pub)
	closeErr := stream.Close()
	spawned := sidBound || drained.boundSID != ""
	if drained.bindErr != nil {
		pub(event.TurnFailed{TurnIndex: cur, Err: errors.Join(drained.bindErr, closeErr)})
		outcome = turnOutcome{spawned: spawned, boundSID: drained.boundSID}
		return
	}
	if turnCtx.Err() != nil {
		outcome = turnOutcome{interrupted: true, spawned: spawned, boundSID: drained.boundSID}
		return
	}
	committed := l.commitTurn(stream, drained.assistant, pub)
	if turnErr := errors.Join(drained.termErr, closeErr); turnErr != nil {
		pub(event.TurnFailed{TurnIndex: cur, Err: turnErr})
		outcome = turnOutcome{committed: committed, spawned: spawned, boundSID: drained.boundSID}
		return
	}
	pub(event.TurnDone{TurnIndex: cur, Message: lastOf(drained.assistant)})
	outcome = turnOutcome{committed: committed, success: true, spawned: spawned, boundSID: drained.boundSID}
}

func (l *Loop) drainStream(stream driver.Stream, cur event.TurnIndex, sidBound bool,
	expectedSID string, bindSID func(string) error, pub func(event.Event),
) drainedTurn {
	mapper := newMapper(cur, l.idGen)
	var output drainedTurn
	for input := range stream.Events() {
		switch input.Kind {
		case driver.KindInit:
			if output.terminal {
				continue
			}
			if input.SessionID != "" && !sidBound && output.boundSID == "" {
				output.boundSID = input.SessionID
				expectedSID = input.SessionID
				if err := bindSID(input.SessionID); err != nil {
					pub(event.ForeignSessionBound{ForeignSID: input.SessionID})
					output.bindErr = err
					return output
				}
				pub(event.ForeignSessionBound{ForeignSID: input.SessionID})
			} else if input.SessionID != "" && input.SessionID != expectedSID {
				slog.Warn("foreignloop: foreign session id mismatch", "want", expectedSID, "got", input.SessionID)
			}
		case driver.KindStepComplete:
			if input.Message != nil {
				output.assistant = append(output.assistant, input.Message)
			}
		case driver.KindTerminalOK:
			output.terminal = true
			if input.Message != nil {
				output.assistant = append(output.assistant, input.Message)
			}
		case driver.KindTerminalError:
			output.terminal = true
			output.termErr = &ForeignResultError{Detail: input.ErrText}
		default:
			l.publishMapped(mapper, input, pub)
		}
	}
	if output.bindErr == nil {
		switch {
		case !sidBound && output.boundSID == "":
			output.termErr = errors.Join(output.termErr, &ForeignProtocolError{Reason: "late-bound stream ended without init event"})
		case !output.terminal:
			output.termErr = &ForeignProtocolError{Reason: "stream ended without terminal event"}
		}
	}
	return output
}

func (l *Loop) publishMapped(mapper *mapper, input driver.Event, pub func(event.Event)) {
	events, err := mapper.toEvents(input)
	if err != nil {
		slog.Error("foreignloop: mapping foreign event failed; skipping", "error", err)
		return
	}
	for _, mapped := range events {
		pub(mapped)
	}
}

// commitTurn asks for provider-neutral authoritative history only after the
// caller has closed the stream. Deliberately unavailable or failed history
// degrades to the complete assistant messages observed on the live stream.
func (l *Loop) commitTurn(stream driver.Stream, assistant []*content.AIMessage, pub func(event.Event)) content.AgenticMessages {
	history, err := stream.History()
	if err != nil {
		if !errors.Is(err, os.ErrNotExist) {
			slog.Warn("foreignloop: transcript decode failed; degrading to stream assistant", "error", err)
		}
		return commitFromAssistant(assistant, pub)
	}
	if !history.Available {
		return commitFromAssistant(assistant, pub)
	}
	var committed content.AgenticMessages
	for _, group := range history.Steps {
		pub(event.StepDone{Messages: group})
		committed = append(committed, group...)
	}
	return committed
}

func commitFromAssistant(assistant []*content.AIMessage, pub func(event.Event)) content.AgenticMessages {
	var committed content.AgenticMessages
	for _, message := range assistant {
		pub(event.StepDone{Messages: content.AgenticMessages{message}})
		committed = append(committed, message)
	}
	return committed
}

func lastOf(assistant []*content.AIMessage) *content.AIMessage {
	if len(assistant) == 0 {
		return nil
	}
	return assistant[len(assistant)-1]
}
