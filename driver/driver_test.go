package driver

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/looprig/core/content"
)

type fakeAgent struct {
	stream Stream
	err    error
	turn   Turn
}

func (a *fakeAgent) Spawn(_ context.Context, turn Turn) (Stream, error) {
	a.turn = turn
	return a.stream, a.err
}

type fakeStream struct {
	events  <-chan Event
	history History
	err     error
	closed  bool
}

func (s *fakeStream) Events() <-chan Event { return s.events }

func (s *fakeStream) History() (History, error) { return s.history, s.err }

func (s *fakeStream) Close() error {
	s.closed = true
	return nil
}

var (
	_ Agent  = (*fakeAgent)(nil)
	_ Stream = (*fakeStream)(nil)

	_ = Turn{"system", "sid", true, []content.Block(nil), "/workspace", PostureDefault}
	_ = Event{KindInit, "sid", "text", "tool-id", "tool", true, "preview", (*content.AIMessage)(nil), "error"}
	_ = History{true, []content.AgenticMessages(nil)}
)

func TestAgentAndStreamContracts(t *testing.T) {
	events := make(chan Event)
	wantHistory := History{
		Available: true,
		Steps: []content.AgenticMessages{
			{&content.AIMessage{Message: content.Message{Role: content.RoleAssistant}}},
		},
	}
	stream := &fakeStream{events: events, history: wantHistory}
	agent := &fakeAgent{stream: stream}

	gotStream, err := agent.Spawn(context.Background(), Turn{ForeignSID: "sid-1"})
	if err != nil {
		t.Fatalf("Spawn() error = %v", err)
	}
	if agent.turn.ForeignSID != "sid-1" {
		t.Fatalf("Spawn() turn ForeignSID = %q, want sid-1", agent.turn.ForeignSID)
	}
	if gotStream.Events() != events {
		t.Fatal("Events() did not return the stream event channel")
	}
	gotHistory, err := gotStream.History()
	if err != nil {
		t.Fatalf("History() error = %v", err)
	}
	if !reflect.DeepEqual(gotHistory, wantHistory) {
		t.Fatalf("History() = %#v, want %#v", gotHistory, wantHistory)
	}
	if err := gotStream.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	if !stream.closed {
		t.Fatal("Close() did not close the stream")
	}
}

func TestPermissionPostureValues(t *testing.T) {
	values := []PermissionPosture{PostureDefault, PostureAcceptEdits}
	for want, got := range values {
		if int(got) != want {
			t.Errorf("posture value %d = %d, want %d", want, got, want)
		}
	}

	var zero PermissionPosture
	if zero != PostureDefault {
		t.Errorf("zero PermissionPosture = %d, want PostureDefault (%d)", zero, PostureDefault)
	}
}

func TestKindValues(t *testing.T) {
	values := []Kind{
		KindInit,
		KindTextDelta,
		KindThinkingDelta,
		KindToolUse,
		KindToolResult,
		KindStepComplete,
		KindTerminalOK,
		KindTerminalError,
		KindModelFacingError,
	}
	for want, got := range values {
		if int(got) != want {
			t.Errorf("kind value %d = %d, want %d", want, got, want)
		}
	}

	var zero Kind
	if zero != KindInit {
		t.Errorf("zero Kind = %d, want KindInit (%d)", zero, KindInit)
	}
}

func TestTurnRetainsEveryField(t *testing.T) {
	input := []content.Block{&content.TextBlock{Text: "hello"}}
	want := Turn{
		SystemPrompt: "system",
		ForeignSID:   "foreign-session",
		StartNew:     true,
		Input:        input,
		Cwd:          "/workspace",
		Posture:      PostureAcceptEdits,
	}
	got := want

	if !reflect.DeepEqual(got, want) {
		t.Fatalf("Turn = %#v, want %#v", got, want)
	}
}

func TestEventRetainsEveryField(t *testing.T) {
	message := &content.AIMessage{
		Message: content.Message{
			Role:   content.RoleAssistant,
			Blocks: []content.Block{&content.TextBlock{Text: "answer"}},
		},
	}
	want := Event{
		Kind:          KindTerminalError,
		SessionID:     "session-id",
		Text:          "delta",
		ToolUseID:     "tool-use-id",
		ToolName:      "tool-name",
		IsError:       true,
		ResultPreview: "preview",
		Message:       message,
		ErrText:       "terminal detail",
	}
	got := want

	if !reflect.DeepEqual(got, want) {
		t.Fatalf("Event = %#v, want %#v", got, want)
	}
}

func TestHistoryRetainsAvailabilityAndSteps(t *testing.T) {
	steps := []content.AgenticMessages{
		{&content.AIMessage{Message: content.Message{Role: content.RoleAssistant}}},
	}
	want := History{Available: true, Steps: steps}
	got := want

	if !reflect.DeepEqual(got, want) {
		t.Fatalf("History = %#v, want %#v", got, want)
	}
}

func TestFakeAgentCanReturnSpawnError(t *testing.T) {
	want := errors.New("spawn failed")
	agent := &fakeAgent{err: want}
	_, got := agent.Spawn(context.Background(), Turn{})
	if !errors.Is(got, want) {
		t.Fatalf("Spawn() error = %v, want %v", got, want)
	}
}
