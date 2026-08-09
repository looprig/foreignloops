package backend

import (
	"context"
	"errors"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/looprig/core/content"
	"github.com/looprig/core/uuid"
	"github.com/looprig/foreignloops/driver"
	"github.com/looprig/harness/pkg/event"
	"github.com/looprig/harness/pkg/foreign"
	"github.com/looprig/harness/pkg/identity"
	"github.com/looprig/harness/pkg/loop"
)

type orderedObservationStream struct {
	observations chan driver.Observation
}

func (s *orderedObservationStream) Events() <-chan driver.Event { return nil }

func (s *orderedObservationStream) Observations() <-chan driver.Observation {
	return s.observations
}

func (s *orderedObservationStream) History() (driver.History, error) {
	return driver.History{Available: false}, nil
}

func (s *orderedObservationStream) Close() error {
	return nil
}

type orderedObservationAgent struct{ stream driver.Stream }

func (a *orderedObservationAgent) Spawn(context.Context, driver.Turn) (driver.Stream, error) {
	return a.stream, nil
}

func TestOrderedDriverObservationsReachActorInOrder(t *testing.T) {
	t.Parallel()
	observations := make(chan driver.Observation, 3)
	observations <- driver.UpdateObservation{Event: driver.Event{Kind: driver.KindInit, SessionID: "ordered-session"}, ReceiveSequence: 1}
	observations <- driver.UpdateObservation{Event: driver.Event{Kind: driver.KindStepComplete, Message: aiMessage("answer")}, ReceiveSequence: 2}
	observations <- driver.PromptObservation{StopReason: "end_turn", ReceiveSequence: 3, ResponseSequence: 3}
	close(observations)
	stream := &orderedObservationStream{observations: observations}
	agent := &orderedObservationAgent{stream: stream}
	pub := &fakePublisher{}
	state, _ := newTestLoop(t, Config{Agent: agent, SIDMode: SIDLateBound}, pub)
	submit(t, state, "ordered")
	waitTurnIndex(t, state, 1)

	if got, want := eventKinds(pub.snapshot()), []string{
		"event.TurnStarted",
		"event.ForeignSessionBound",
		"event.StepDone",
		"event.TurnDone",
	}; !reflect.DeepEqual(got, want) {
		t.Fatalf("ordered events = %v, want %v", got, want)
	}
	if got, want := eventKinds(pub.checkedSnapshot()), []string{
		"event.TurnStarted",
		"event.ForeignSessionBound",
		"event.StepDone",
		"event.TurnDone",
	}; !reflect.DeepEqual(got, want) {
		t.Fatalf("checked ordered events = %v, want %v", got, want)
	}
	shutdown(t, state)
}

func TestEveryOrderedObservationEntersActorMailbox(t *testing.T) {
	t.Parallel()
	inputs := make(chan driver.Observation, 3)
	inputs <- driver.UpdateObservation{Event: driver.Event{Kind: driver.KindInit, SessionID: "ordered-session"}, ReceiveSequence: 1}
	inputs <- driver.UpdateObservation{Event: driver.Event{Kind: driver.KindStepComplete, Message: aiMessage("step")}, ReceiveSequence: 2}
	inputs <- driver.PromptObservation{StopReason: "end_turn", ReceiveSequence: 3, ResponseSequence: 3}
	close(inputs)

	var raw []driver.Observation
	state := &Loop{idGen: seqIDGen()}
	state.drainOrderedStream(inputs, 1, false, "", func(string) error { return nil }, func(observation turnObservation) bool {
		if observation.raw != nil {
			raw = append(raw, observation.raw)
		}
		return true
	})
	if len(raw) != 3 {
		t.Fatalf("raw observations in actor mailbox = %d, want 3", len(raw))
	}
}

func TestCheckedTerminalPublicationFailureStopsForeignLoop(t *testing.T) {
	t.Parallel()
	sentinel := errors.New("journal unavailable")
	pub := &terminalFailurePublisher{failOn: "event.TurnDone", err: sentinel}
	agent := &fakeAgent{events: []driver.Event{{Kind: driver.KindTerminalOK}}}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	state, _, err := New(ctx, mustID(t), mustID(t), loop.Provenance{}, pub, validBoundDefinition(), Config{Agent: agent, Cwd: t.TempDir(), SIDMode: SIDPrebound}, seqIDGen(), workingFac())
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	submit(t, state, "fault")

	select {
	case <-state.Done:
	case <-time.After(3 * time.Second):
		t.Fatal("foreign loop did not stop after checked terminal publication failure")
	}

	for _, input := range pub.snapshot() {
		if _, ok := input.(event.TurnDone); ok {
			t.Fatal("TurnDone was published after checked publication failure")
		}
	}
}

func TestTurnFoldedIntoForeignHeaderIsValid(t *testing.T) {
	t.Parallel()
	sessionID, loopID, turnID, commandID := mustID(t), mustID(t), mustID(t), mustID(t)
	input := event.TurnFoldedInto{
		Header:    event.Header{Cause: identity.Cause{CommandID: commandID}},
		TurnIndex: 3,
		Message: &content.UserMessage{Message: content.Message{
			Role:   content.RoleUser,
			Blocks: []content.Block{&content.TextBlock{Text: "folded"}},
		}},
	}
	got := fillForeignHeader(input, sessionID, loopID, turnID, uuid.UUID{})
	folded, ok := got.(event.TurnFoldedInto)
	if !ok {
		t.Fatalf("filled event = %T, want TurnFoldedInto", got)
	}
	stampedHeader, err := workingFac().Stamp(folded.EventHeader())
	if err != nil {
		t.Fatalf("stamp TurnFoldedInto: %v", err)
	}
	folded = withForeignHeader(folded, stampedHeader).(event.TurnFoldedInto)
	if err := event.ValidateEvent(folded); err != nil {
		t.Fatalf("TurnFoldedInto validation: %v", err)
	}
	header := folded.EventHeader()
	if header.SessionID != sessionID || header.LoopID != loopID || header.TurnID != turnID || !header.StepID.IsZero() {
		t.Fatalf("TurnFoldedInto coordinates = %+v", header.Coordinates)
	}
}

type terminalFailurePublisher struct {
	mu     sync.Mutex
	events []event.Event
	failOn string
	err    error
}

func (p *terminalFailurePublisher) PublishEvent(_ context.Context, ev event.Event) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.events = append(p.events, ev)
	return nil
}

func (p *terminalFailurePublisher) PublishEventChecked(ctx context.Context, ev event.Event) error {
	if eventKind(ev) == p.failOn {
		return p.err
	}
	return p.PublishEvent(ctx, ev)
}

func (p *terminalFailurePublisher) snapshot() []event.Event {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]event.Event(nil), p.events...)
}

var _ foreign.EventPublisher = (*terminalFailurePublisher)(nil)
