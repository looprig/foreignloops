package backend

import (
	"context"
	"reflect"
	"testing"

	"github.com/looprig/core/uuid"
	"github.com/looprig/foreignloops/driver"
	"github.com/looprig/harness/pkg/command"
	"github.com/looprig/harness/pkg/event"
	"github.com/looprig/harness/pkg/foreign"
	"github.com/looprig/harness/pkg/identity"
)

func TestTurnCancellationAdjudicatesReservedSteerBeforeLifecycle(t *testing.T) {
	tests := []struct {
		name       string
		make       func(*testing.T, *Loop, uuid.UUID) (command.Command, func(*testing.T))
		steer      driver.SteerResult
		wantCalls  []string
		wantEvents []string
	}{
		{
			name:       "interrupt injected",
			make:       funcCommandInterrupt,
			steer:      injectedSteerResult(),
			wantCalls:  []string{"reserve", "resolve-injected"},
			wantEvents: []string{"event.TurnFoldedInto", "event.TurnInterrupted"},
		},
		{
			name:       "caller cancel active turn fallback",
			make:       funcCommandCancel,
			steer:      fallbackSteerResult(),
			wantCalls:  []string{"reserve", "fallback"},
			wantEvents: []string{"event.TurnInterrupted"},
		},
		{
			name:       "shutdown admission unknown",
			make:       funcCommandShutdown,
			steer:      driver.SteerResult{Outcome: driver.SteerOutcomeAdmissionUnknown},
			wantCalls:  []string{"reserve", string(foreign.DeliveryResolutionUnknown)},
			wantEvents: []string{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			steerer := &scriptedSteerer{started: make(chan struct{}, 1), results: make(chan scriptedSteerResult, 1)}
			hook := &recordingDeliveryHook{}
			l, machine, pub, cancel := newSteeringUnit(t, steerer, hook)
			t.Cleanup(cancel)
			if err := machine.setStream(orderedUnitStream()); err != nil {
				t.Fatalf("set stream: %v", err)
			}
			input := unitPreparedInput(t, l, "cancel me")
			input.command.CommandID = input.command.Cause.CommandID
			activeCommandID := mustID(t)
			if handled, err := machine.offer(input); !handled || err != nil {
				t.Fatalf("offer = handled %t err %v", handled, err)
			}
			awaitStarted(t, steerer.started)

			steerer.results <- scriptedSteerResult{result: tt.steer}
			completion := takeCompletion(t, machine)
			machine.completions <- completion

			result := make(chan turnOutcome)
			mailbox := make(chan turnObservation)
			close(mailbox)
			inputCommand, verifyAck := tt.make(t, l, activeCommandID)
			cancelStarted := make(chan struct{})
			cancelForCommand := func() {
				close(cancelStarted)
				cancel()
			}
			done := make(chan struct{})
			go func() {
				_, _ = l.handleTurnCommand(
					context.Background(), inputCommand, 1, activeCommandID, input.turnID, input.stepID,
					cancelForCommand, l.publisher(context.Background(), input.turnID, input.stepID), mailbox, result, machine,
				)
				close(done)
			}()
			<-cancelStarted
			result <- turnOutcome{interrupted: true}
			<-done
			verifyAck(t)

			if got, want := hook.snapshot(), tt.wantCalls; !reflect.DeepEqual(got, want) {
				t.Fatalf("delivery calls = %v, want %v before %s completes", got, want, tt.name)
			}
			if machine.active != nil || !machine.terminalReady() {
				t.Fatalf("machine after %s = active %p terminalReady=%t, want no unresolved attempt", tt.name, machine.active, machine.terminalReady())
			}
			if machine.pendingCount() != 0 {
				t.Fatalf("machine pending count after %s = %d, want zero", tt.name, machine.pendingCount())
			}
			if got, want := completion.result.WriteAdmitted, tt.steer.WriteAdmitted; got != want {
				t.Fatalf("cancellation completion writer admission = %t, want provider fact %t", got, want)
			}
			if got, want := eventKinds(pub.snapshot()), tt.wantEvents; !reflect.DeepEqual(got, want) {
				t.Fatalf("lifecycle events after %s = %v, want %v after resolution", tt.name, got, want)
			}
		})
	}
}

func TestCancelReservedSteerIDIsNoop(t *testing.T) {
	steerer := &scriptedSteerer{started: make(chan struct{}, 1), results: make(chan scriptedSteerResult, 1)}
	hook := &recordingDeliveryHook{}
	l, machine, pub, cancel := newSteeringUnit(t, steerer, hook)
	t.Cleanup(cancel)
	if err := machine.setStream(orderedUnitStream()); err != nil {
		t.Fatalf("set stream: %v", err)
	}
	input := unitPreparedInput(t, l, "reserved steer")
	input.command.CommandID = input.command.Cause.CommandID
	if handled, err := machine.offer(input); !handled || err != nil {
		t.Fatalf("offer = handled %t err %v", handled, err)
	}
	awaitStarted(t, steerer.started)

	ack := make(chan command.DelegateCancelResult, 1)
	request := command.CancelDelegateRequest{
		Header:          command.Header{CommandID: mustID(t)},
		Coordinates:     identity.Coordinates{SessionID: l.sessionID, LoopID: l.loopID},
		TargetCommandID: input.command.CommandID,
		Ack:             ack,
	}
	done, exit := l.handleTurnCommand(
		context.Background(), request, 1, mustID(t), input.turnID, input.stepID,
		cancel, l.publisher(context.Background(), input.turnID, input.stepID), nil,
		make(chan turnOutcome), machine,
	)
	if done || exit {
		t.Fatalf("reserved-ID cancel returned done=%t exit=%t, want actor continue", done, exit)
	}
	if got := <-ack; got != command.DelegateCancelNoop {
		t.Fatalf("reserved-ID cancel ack = %v, want DelegateCancelNoop", got)
	}
	if got, want := hook.snapshot(), []string{"reserve"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("delivery calls after reserved-ID cancel = %v, want %v", got, want)
	}
	if machine.active == nil || machine.active.resolved || machine.terminalReady() {
		t.Fatalf("reserved steer after no-op cancel = active=%p resolved=%t terminalReady=%t, want live reservation",
			machine.active, machine.active != nil && machine.active.resolved, machine.terminalReady())
	}
	if got := pub.snapshot(); len(got) != 0 {
		t.Fatalf("events after reserved-ID cancel = %v, want none", eventKinds(got))
	}
}

func TestOrderedPromptMessageIsCommittedOnce(t *testing.T) {
	message := aiMessage("assembled answer")
	inputs := make(chan driver.Observation, 1)
	inputs <- driver.PromptObservation{StopReason: "end_turn", Message: message}
	close(inputs)

	loopState := &Loop{}
	drained := loopState.drainOrderedStream(inputs, 1, true, "", func(string) error {
		return nil
	}, func(turnObservation) bool {
		return true
	})
	if len(drained.assistant) != 1 || drained.assistant[0] != message {
		t.Fatalf("ordered assistant transcript = %#v, want exactly %p", drained.assistant, message)
	}
}

func TestSteeringDeadlineUnknownCompletionResolvesWithoutObservation(t *testing.T) {
	tests := []struct {
		name   string
		result driver.SteerResult
	}{
		{
			name:   "admission unknown",
			result: driver.SteerResult{Outcome: driver.SteerOutcomeAdmissionUnknown},
		},
		{
			name:   "delivery unknown",
			result: driver.SteerResult{Outcome: driver.SteerOutcomeDeliveryUnknown, WriteAdmitted: true},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			steerer := &scriptedSteerer{started: make(chan struct{}, 1), results: make(chan scriptedSteerResult, 1)}
			hook := &recordingDeliveryHook{}
			l, machine, _, cancel := newSteeringUnit(t, steerer, hook)
			t.Cleanup(cancel)
			if err := machine.setStream(orderedUnitStream()); err != nil {
				t.Fatalf("set stream: %v", err)
			}
			input := unitPreparedInput(t, l, "deadline unknown")
			if handled, err := machine.offer(input); !handled || err != nil {
				t.Fatalf("offer = handled %t err %v", handled, err)
			}
			queued := unitPreparedInput(t, l, "queued after unknown")
			if handled, err := machine.offer(queued); !handled || err != nil {
				t.Fatalf("queued offer = handled %t err %v", handled, err)
			}
			awaitStarted(t, steerer.started)

			completion := steeringCompletion{
				attempt: machine.active,
				result:  tt.result,
				err:     context.DeadlineExceeded,
			}
			if err := machine.complete(completion); err != nil {
				t.Fatalf("deadline completion: %v", err)
			}
			if got, want := hook.snapshot(), []string{"reserve", string(foreign.DeliveryResolutionUnknown), "fallback"}; !reflect.DeepEqual(got, want) {
				t.Fatalf("deadline delivery calls = %v, want exactly one unknown resolution then FIFO fallback", got)
			}
			if machine.active != nil || machine.pendingCount() != 0 {
				t.Fatalf("deadline state = active %p pending %d, want released FIFO state", machine.active, machine.pendingCount())
			}
			if got, want := hook.fallbackIDs(), []uuid.UUID{queued.command.CommandID}; !reflect.DeepEqual(got, want) {
				t.Fatalf("deadline fallback IDs = %v, want queued request %v only", got, want)
			}

			late := driver.SteerObservation{SteerResult: injectedSteerResult()}
			if err := machine.observe(late); err != nil {
				t.Fatalf("late observation: %v", err)
			}
			if err := machine.complete(completion); err != nil {
				t.Fatalf("late completion: %v", err)
			}
			if got, want := hook.snapshot(), []string{"reserve", string(foreign.DeliveryResolutionUnknown), "fallback"}; !reflect.DeepEqual(got, want) {
				t.Fatalf("late facts changed delivery calls = %v, want %v", got, want)
			}
		})
	}
}

func TestSteeringValidFallbackCompletionWaitsForOrderedObservation(t *testing.T) {
	steerer := &scriptedSteerer{started: make(chan struct{}, 1), results: make(chan scriptedSteerResult, 1)}
	hook := &recordingDeliveryHook{}
	l, machine, _, cancel := newSteeringUnit(t, steerer, hook)
	t.Cleanup(cancel)
	if err := machine.setStream(orderedUnitStream()); err != nil {
		t.Fatalf("set stream: %v", err)
	}
	input := unitPreparedInput(t, l, "delayed fallback")
	if handled, err := machine.offer(input); !handled || err != nil {
		t.Fatalf("offer = handled %t err %v", handled, err)
	}
	awaitStarted(t, steerer.started)
	result := fallbackSteerResult()
	completion := steeringCompletion{attempt: machine.active, result: result}
	if err := machine.complete(completion); err != nil {
		t.Fatalf("valid fallback completion: %v", err)
	}
	if machine.active == nil || machine.active.resolved {
		t.Fatal("valid fallback completion resolved before its ordered observation")
	}
	if got := hook.snapshot(); !reflect.DeepEqual(got, []string{"reserve"}) {
		t.Fatalf("early fallback delivery calls = %v, want reservation only", got)
	}
	if err := machine.observe(driver.SteerObservation{SteerResult: result}); err != nil {
		t.Fatalf("delayed fallback observation: %v", err)
	}
	if machine.active != nil || machine.pendingCount() != 0 {
		t.Fatalf("delayed fallback state = active %p pending %d, want released", machine.active, machine.pendingCount())
	}
	if got, want := hook.snapshot(), []string{"reserve", "fallback"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("delayed fallback delivery calls = %v, want %v", got, want)
	}
}

func funcCommandInterrupt(*testing.T, *Loop, uuid.UUID) (command.Command, func(*testing.T)) {
	ack := make(chan bool, 1)
	return command.Interrupt{Ack: ack}, func(t *testing.T) {
		t.Helper()
		if !<-ack {
			t.Fatal("interrupt ack = false, want true")
		}
	}
}

func funcCommandCancel(t *testing.T, l *Loop, target uuid.UUID) (command.Command, func(*testing.T)) {
	t.Helper()
	ack := make(chan command.DelegateCancelResult, 1)
	return command.CancelDelegateRequest{
			Header:          command.Header{CommandID: mustID(t)},
			Coordinates:     identity.Coordinates{SessionID: l.sessionID, LoopID: l.loopID},
			TargetCommandID: target,
			Ack:             ack,
		}, func(t *testing.T) {
			t.Helper()
			if got := <-ack; got != command.DelegateCancelActive {
				t.Fatalf("cancel ack = %v, want DelegateCancelActive", got)
			}
		}
}

func funcCommandShutdown(*testing.T, *Loop, uuid.UUID) (command.Command, func(*testing.T)) {
	ack := make(chan error, 1)
	return command.Shutdown{Ack: ack}, func(t *testing.T) {
		t.Helper()
		if err := <-ack; err != nil {
			t.Fatalf("shutdown ack error = %v", err)
		}
	}
}

var _ foreign.DeliveryHook = (*recordingDeliveryHook)(nil)
var _ driver.Steerer = (*scriptedSteerer)(nil)
var _ event.Event = event.TurnInterrupted{}
