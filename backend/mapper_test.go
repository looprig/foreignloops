package backend

import (
	"errors"
	"testing"

	"github.com/looprig/core/content"
	"github.com/looprig/core/uuid"
	"github.com/looprig/foreignloops/driver"
	"github.com/looprig/harness/pkg/event"
)

const wantTurn event.TurnIndex = 7

var errTestGen = errors.New("test idgen failure")

func errIDGen() func() (uuid.UUID, error) {
	return func() (uuid.UUID, error) { return uuid.UUID{}, errTestGen }
}

func TestMapperToEvents(t *testing.T) {
	t.Parallel()

	stepMsg := aiMessage("step done")
	okMsg := aiMessage("all done")
	tests := []struct {
		name    string
		input   driver.Event
		genErr  bool
		wantLen int
		wantErr bool
		check   func(*testing.T, []event.Event)
	}{
		{
			name: "text delta -> TokenDelta with text chunk", input: driver.Event{Kind: driver.KindTextDelta, Text: "hello"}, wantLen: 1,
			check: func(t *testing.T, events []event.Event) {
				delta, ok := events[0].(event.TokenDelta)
				if !ok {
					t.Fatalf("event = %T, want event.TokenDelta", events[0])
				}
				if delta.TurnIndex != wantTurn {
					t.Fatalf("TurnIndex = %d, want %d", delta.TurnIndex, wantTurn)
				}
				chunk, ok := delta.Chunk.(*content.TextChunk)
				if !ok || chunk.Text != "hello" {
					t.Fatalf("Chunk = %#v, want text hello", delta.Chunk)
				}
			},
		},
		{
			name: "thinking delta -> TokenDelta with thinking chunk", input: driver.Event{Kind: driver.KindThinkingDelta, Text: "pondering"}, wantLen: 1,
			check: func(t *testing.T, events []event.Event) {
				delta, ok := events[0].(event.TokenDelta)
				if !ok {
					t.Fatalf("event = %T, want event.TokenDelta", events[0])
				}
				chunk, ok := delta.Chunk.(*content.ThinkingChunk)
				if !ok || chunk.Thinking != "pondering" {
					t.Fatalf("Chunk = %#v, want thinking pondering", delta.Chunk)
				}
			},
		},
		{
			name: "tool use -> ToolCallStarted with minted id", input: driver.Event{Kind: driver.KindToolUse, ToolUseID: "toolu_1", ToolName: "Bash"}, wantLen: 1,
			check: func(t *testing.T, events []event.Event) {
				started, ok := events[0].(event.ToolCallStarted)
				if !ok {
					t.Fatalf("event = %T, want event.ToolCallStarted", events[0])
				}
				if started.ToolName != "Bash" || started.ToolExecutionID == (uuid.UUID{}) {
					t.Fatalf("started = %#v, want Bash with non-zero execution id", started)
				}
			},
		},
		{name: "tool result with unknown id -> orphan soft-skip", input: driver.Event{Kind: driver.KindToolResult, ToolUseID: "ghost", IsError: true, ResultPreview: "x"}},
		{
			name: "step complete with message -> StepDone", input: driver.Event{Kind: driver.KindStepComplete, Message: stepMsg}, wantLen: 1,
			check: func(t *testing.T, events []event.Event) {
				done, ok := events[0].(event.StepDone)
				if !ok || len(done.Messages) != 1 || done.Messages[0] != stepMsg {
					t.Fatalf("event = %#v, want StepDone containing source message", events[0])
				}
			},
		},
		{name: "step complete with nil message -> no event", input: driver.Event{Kind: driver.KindStepComplete}},
		{
			name: "terminal ok -> TurnDone", input: driver.Event{Kind: driver.KindTerminalOK, Message: okMsg}, wantLen: 1,
			check: func(t *testing.T, events []event.Event) {
				done, ok := events[0].(event.TurnDone)
				if !ok || done.TurnIndex != wantTurn || done.Message != okMsg {
					t.Fatalf("event = %#v, want TurnDone at turn %d", events[0], wantTurn)
				}
			},
		},
		{
			name: "terminal error -> TurnFailed with ForeignResultError", input: driver.Event{Kind: driver.KindTerminalError, ErrText: "error_max_turns"}, wantLen: 1,
			check: func(t *testing.T, events []event.Event) {
				failed, ok := events[0].(event.TurnFailed)
				if !ok || failed.TurnIndex != wantTurn {
					t.Fatalf("event = %#v, want TurnFailed at turn %d", events[0], wantTurn)
				}
				var resultErr *ForeignResultError
				if !errors.As(failed.Err, &resultErr) || resultErr.Detail != "error_max_turns" {
					t.Fatalf("Err = %T %v, want ForeignResultError(error_max_turns)", failed.Err, failed.Err)
				}
			},
		},
		{name: "init -> no event", input: driver.Event{Kind: driver.KindInit, SessionID: "sess-abc"}},
		{name: "unknown kind -> no event", input: driver.Event{Kind: driver.Kind(250)}},
		{name: "idgen failure on tool use -> error", input: driver.Event{Kind: driver.KindToolUse, ToolUseID: "toolu_1", ToolName: "Bash"}, genErr: true, wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			gen := seqIDGen()
			if tt.genErr {
				gen = errIDGen()
			}
			mapper := newMapper(wantTurn, gen)
			got, err := mapper.toEvents(tt.input)
			if (err != nil) != tt.wantErr {
				t.Fatalf("toEvents error = %v, wantErr %v", err, tt.wantErr)
			}
			if tt.wantErr {
				if !errors.Is(err, errTestGen) {
					t.Fatalf("error = %v, want errTestGen", err)
				}
				return
			}
			if len(got) != tt.wantLen {
				t.Fatalf("len(events) = %d, want %d (%#v)", len(got), tt.wantLen, got)
			}
			if tt.check != nil {
				tt.check(t, got)
			}
		})
	}
}

func TestMapperCorrelation(t *testing.T) {
	t.Parallel()
	mapper := newMapper(3, seqIDGen())

	startedEvents, err := mapper.toEvents(driver.Event{Kind: driver.KindToolUse, ToolUseID: "toolu_1", ToolName: "Bash"})
	if err != nil {
		t.Fatalf("tool use: %v", err)
	}
	started, ok := startedEvents[0].(event.ToolCallStarted)
	if !ok {
		t.Fatalf("event = %T, want event.ToolCallStarted", startedEvents[0])
	}

	completedEvents, err := mapper.toEvents(driver.Event{Kind: driver.KindToolResult, ToolUseID: "toolu_1", IsError: true, ResultPreview: "oops"})
	if err != nil {
		t.Fatalf("tool result: %v", err)
	}
	completed, ok := completedEvents[0].(event.ToolCallCompleted)
	if !ok {
		t.Fatalf("event = %T, want event.ToolCallCompleted", completedEvents[0])
	}
	if started.ToolExecutionID == (uuid.UUID{}) || started.ToolExecutionID != completed.ToolExecutionID {
		t.Fatalf("execution ids = %v/%v, want same non-zero id", started.ToolExecutionID, completed.ToolExecutionID)
	}
	if !completed.IsError || completed.ResultPreview != "oops" {
		t.Fatalf("completed = %#v, want error result oops", completed)
	}
}

func TestMapperModelFacingFailureUsesDedicatedError(t *testing.T) {
	t.Parallel()
	const safeDetail = "ACP error -32000: Usage limit reached; resets at 3:00 PM"
	input := markModelFacing(driver.Event{Kind: driver.KindTerminalError, ErrText: safeDetail})
	mapper := newMapper(wantTurn, seqIDGen())
	mapped, err := mapper.toEvents(input)
	if err != nil {
		t.Fatalf("toEvents() error = %v", err)
	}
	if len(mapped) != 1 {
		t.Fatalf("mapped events = %#v, want one event", mapped)
	}
	failed, ok := mapped[0].(event.TurnFailed)
	if !ok {
		t.Fatalf("mapped event = %T, want event.TurnFailed", mapped[0])
	}
	var modelFacing interface{ ModelFacingError() string }
	if !errors.As(failed.Err, &modelFacing) {
		t.Fatalf("TurnFailed.Err = %T %v, want dedicated model-facing error", failed.Err, failed.Err)
	}
	if got := modelFacing.ModelFacingError(); got != safeDetail {
		t.Fatalf("ModelFacingError() = %q, want %q", got, safeDetail)
	}
	var ordinary *ForeignResultError
	if errors.As(failed.Err, &ordinary) {
		t.Fatalf("TurnFailed.Err = %T, must not also expose ordinary ForeignResultError", failed.Err)
	}
}

func markModelFacing(input driver.Event) driver.Event {
	input.Kind = driver.KindModelFacingError
	return input
}
