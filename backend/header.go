package backend

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/looprig/core/uuid"
	"github.com/looprig/harness/pkg/event"
	"github.com/looprig/harness/pkg/identity"
)

func fillForeignHeader(input event.Event, sessionID, loopID, turnID, stepID uuid.UUID) event.Event {
	switch typed := input.(type) {
	case event.TurnStarted:
		typed.Header.SessionID, typed.Header.LoopID, typed.Header.TurnID = sessionID, loopID, turnID
		return typed
	case event.ForeignSessionBound:
		typed.Header.SessionID, typed.Header.LoopID = sessionID, loopID
		return typed
	case event.DelegateRequestAccepted:
		typed.Header.SessionID, typed.Header.LoopID = sessionID, loopID
		return typed
	case event.InputQueued:
		typed.Header.SessionID, typed.Header.LoopID = sessionID, loopID
		return typed
	case event.InputCancelled:
		typed.Header.SessionID, typed.Header.LoopID, typed.Header.TurnID = sessionID, loopID, turnID
		return typed
	case event.TurnDone:
		typed.Header.SessionID, typed.Header.LoopID, typed.Header.TurnID = sessionID, loopID, turnID
		return typed
	case event.TurnFailed:
		typed.Header.SessionID, typed.Header.LoopID, typed.Header.TurnID = sessionID, loopID, turnID
		return typed
	case event.TurnInterrupted:
		typed.Header.SessionID, typed.Header.LoopID, typed.Header.TurnID = sessionID, loopID, turnID
		return typed
	case event.StepDone:
		typed.Header.SessionID, typed.Header.LoopID, typed.Header.TurnID, typed.Header.StepID = sessionID, loopID, turnID, stepID
		return typed
	case event.TokenDelta:
		typed.Header.SessionID, typed.Header.LoopID, typed.Header.TurnID, typed.Header.StepID = sessionID, loopID, turnID, stepID
		return typed
	case event.ToolCallStarted:
		typed.Header.SessionID, typed.Header.LoopID, typed.Header.TurnID, typed.Header.StepID = sessionID, loopID, turnID, stepID
		return typed
	case event.ToolCallCompleted:
		typed.Header.SessionID, typed.Header.LoopID, typed.Header.TurnID, typed.Header.StepID = sessionID, loopID, turnID, stepID
		return typed
	default:
		return input
	}
}

func withForeignHeader(input event.Event, header event.Header) event.Event {
	switch typed := input.(type) {
	case event.TurnStarted:
		typed.Header = header
		return typed
	case event.ForeignSessionBound:
		typed.Header = header
		return typed
	case event.DelegateRequestAccepted:
		typed.Header = header
		return typed
	case event.InputCancelled:
		typed.Header = header
		return typed
	case event.StepDone:
		typed.Header = header
		return typed
	case event.TurnDone:
		typed.Header = header
		return typed
	case event.TurnFailed:
		typed.Header = header
		return typed
	case event.TurnInterrupted:
		typed.Header = header
		return typed
	default:
		return input
	}
}

func (l *Loop) publisher(ctx context.Context, turnID, stepID uuid.UUID) func(event.Event) {
	return func(input event.Event) {
		input = fillForeignHeader(input, l.sessionID, l.loopID, turnID, stepID)
		if input.Class() == event.Enduring {
			header, err := l.fac.Stamp(input.EventHeader())
			if err != nil {
				slog.Error("foreignloop: event id mint failed; dropping Enduring event (fail-secure)", "event", fmt.Sprintf("%T", input), "error", err)
				return
			}
			input = withForeignHeader(input, header)
		}
		if err := l.pub.PublishEvent(ctx, input); err != nil {
			slog.Error("foreignloop: event publish to session fan-in failed", "error", err)
		}
	}
}

func (l *Loop) publishAcceptance(ctx context.Context, commandID uuid.UUID) error {
	input := fillForeignHeader(event.DelegateRequestAccepted{Header: event.Header{Cause: identity.Cause{CommandID: commandID}}}, l.sessionID, l.loopID, uuid.UUID{}, uuid.UUID{})
	header, err := l.fac.Stamp(input.EventHeader())
	if err != nil {
		return err
	}
	return l.pub.PublishEventChecked(ctx, withForeignHeader(input, header))
}
