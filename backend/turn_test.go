package backend

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/looprig/core/content"
	"github.com/looprig/core/uuid"
	"github.com/looprig/foreignloops/driver"
	"github.com/looprig/harness/pkg/command"
	"github.com/looprig/harness/pkg/event"
	"github.com/looprig/harness/pkg/identity"
	"github.com/looprig/harness/pkg/loop"
)

func eventKind(input event.Event) string {
	return fmt.Sprintf("%T", input)
}

func eventKinds(events []event.Event) []string {
	out := make([]string, len(events))
	for i, input := range events {
		out[i] = eventKind(input)
	}
	return out
}

func firstText(t *testing.T, message content.Conversation) string {
	t.Helper()
	var blocks []content.Block
	switch typed := message.(type) {
	case *content.AIMessage:
		blocks = typed.Blocks
	case *content.UserMessage:
		blocks = typed.Blocks
	default:
		t.Fatalf("message type = %T", message)
	}
	for _, block := range blocks {
		if text, ok := block.(*content.TextBlock); ok {
			return text.Text
		}
	}
	return ""
}

func submit(t *testing.T, state *Loop, text string) uuid.UUID {
	t.Helper()
	id := mustID(t)
	select {
	case state.Commands <- command.UserInput{Header: command.Header{CommandID: id}, Blocks: []content.Block{&content.TextBlock{Text: text}}}:
	case <-time.After(2 * time.Second):
		t.Fatal("submit timed out")
	}
	return id
}

func waitFor(t *testing.T, pub *fakePublisher, predicate func(event.Event) bool) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		for _, input := range pub.snapshot() {
			if predicate(input) {
				return
			}
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("event not observed: %v", eventKinds(pub.snapshot()))
}

func waitTurnIndex(t *testing.T, state *Loop, want event.TurnIndex) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		_, got, err := state.Snapshot(context.Background())
		if err == nil && got == want {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("turn index did not reach %d", want)
}

func sendInterrupt(t *testing.T, state *Loop) {
	t.Helper()
	ack := make(chan bool, 1)
	state.Commands <- command.Interrupt{Ack: ack}
	if !<-ack {
		t.Fatal("interrupt reported idle")
	}
}

func waitLoopIdle(t *testing.T, state *Loop) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		ack := make(chan bool, 1)
		select {
		case state.Commands <- command.Interrupt{Ack: ack}:
		case <-time.After(2 * time.Second):
			t.Fatal("idle probe submit timed out")
		}
		if !<-ack {
			return
		}
	}
	t.Fatal("loop did not become idle")
}

func TestTurnClosePrecedesHistoryAndAvailableHistoryCommitsEveryGroup(t *testing.T) {
	t.Parallel()
	history := driver.History{Available: true, Steps: []content.AgenticMessages{
		{aiMessage("one"), &content.ToolResultMessage{Message: content.Message{Role: content.RoleTool, Blocks: []content.Block{&content.TextBlock{Text: "tool"}}}}},
		{aiMessage("two")},
	}}
	agent := &fakeAgent{history: history, events: []driver.Event{{Kind: driver.KindStepComplete, Message: aiMessage("stream")}, {Kind: driver.KindTerminalOK, Message: aiMessage("done")}}}
	pub := &fakePublisher{}
	state, _ := newTestLoop(t, Config{Agent: agent, SIDMode: SIDPrebound}, pub)
	submit(t, state, "go")
	waitTurnIndex(t, state, 1)
	if got, want := agent.lastStream().lifecycle(), []string{"close", "history"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("stream lifecycle = %v, want %v", got, want)
	}
	var steps []event.StepDone
	for _, input := range pub.snapshot() {
		if step, ok := input.(event.StepDone); ok {
			steps = append(steps, step)
		}
	}
	if len(steps) != len(history.Steps) {
		t.Fatalf("StepDone count = %d, want %d", len(steps), len(history.Steps))
	}
	msgs, _, err := state.Snapshot(context.Background())
	if err != nil || len(msgs) != 3 || firstText(t, msgs[0]) != "one" || firstText(t, msgs[2]) != "two" {
		t.Fatalf("snapshot = %v err %v", msgs, err)
	}
	shutdown(t, state)
}

func TestUnavailableHistorySilentlyFallsBackToAllStreamAssistantMessages(t *testing.T) {
	t.Parallel()
	agent := &fakeAgent{history: driver.History{Available: false}, events: []driver.Event{{Kind: driver.KindStepComplete, Message: aiMessage("one")}, {Kind: driver.KindStepComplete, Message: aiMessage("two")}, {Kind: driver.KindTerminalOK, Message: aiMessage("three")}}}
	pub := &fakePublisher{}
	state, _ := newTestLoop(t, Config{Agent: agent, SIDMode: SIDPrebound}, pub)
	submit(t, state, "go")
	waitTurnIndex(t, state, 1)
	msgs, _, _ := state.Snapshot(context.Background())
	if len(msgs) != 3 || firstText(t, msgs[0]) != "one" || firstText(t, msgs[2]) != "three" {
		t.Fatalf("fallback messages = %v", msgs)
	}
	steps := 0
	for _, input := range pub.snapshot() {
		if _, ok := input.(event.StepDone); ok {
			steps++
		}
	}
	if steps != 3 {
		t.Fatalf("StepDone count = %d, want 3", steps)
	}
	shutdown(t, state)
}

func TestTypedHistoryFailureUsesFallbackAndPreservesSuccessfulTerminal(t *testing.T) {
	t.Parallel()
	historyErr := &driver.HistoryError{Cause: errors.New("bad transcript")}
	agent := &fakeAgent{historyErr: historyErr, events: []driver.Event{{Kind: driver.KindStepComplete, Message: aiMessage("fallback")}, {Kind: driver.KindTerminalOK}}}
	pub := &fakePublisher{}
	state, _ := newTestLoop(t, Config{Agent: agent, SIDMode: SIDPrebound}, pub)
	submit(t, state, "go")
	waitTurnIndex(t, state, 1)
	msgs, _, _ := state.Snapshot(context.Background())
	if len(msgs) != 1 || firstText(t, msgs[0]) != "fallback" {
		t.Fatalf("fallback messages = %v", msgs)
	}
	if got := eventKinds(pub.snapshot()); !reflect.DeepEqual(got, []string{"event.TurnStarted", "event.StepDone", "event.TurnDone"}) {
		t.Fatalf("events = %v", got)
	}
	shutdown(t, state)
}

func TestHistoryFallbackWarningParity(t *testing.T) {
	oldLogger := slog.Default()
	var logs bytes.Buffer
	slog.SetDefault(slog.New(slog.NewTextHandler(&logs, nil)))
	t.Cleanup(func() { slog.SetDefault(oldLogger) })

	tests := []struct {
		name        string
		history     driver.History
		err         error
		wantWarning bool
	}{
		{
			name: "missing transcript stays silent",
			err: &driver.HistoryError{Cause: &os.PathError{
				Op:   "open",
				Path: "/missing/session.jsonl",
				Err:  os.ErrNotExist,
			}},
		},
		{name: "non-missing history failure warns", err: &driver.HistoryError{Cause: errors.New("malformed authoritative history")}, wantWarning: true},
		{name: "deliberately unavailable history stays silent", history: driver.History{Available: false}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			logs.Reset()
			stream := &fakeStream{history: tt.history, historyErr: tt.err}
			var published []event.Event
			committed := (&Loop{}).commitTurn(stream, []*content.AIMessage{aiMessage("fallback")}, func(input event.Event) {
				published = append(published, input)
			})
			if got := strings.Contains(logs.String(), "level=WARN"); got != tt.wantWarning {
				t.Fatalf("warning present = %v, want %v; logs=%q", got, tt.wantWarning, logs.String())
			}
			if tt.wantWarning && !strings.Contains(logs.String(), "foreignloop: transcript decode failed; degrading to stream assistant") {
				t.Fatalf("warning message drifted from predecessor: %q", logs.String())
			}
			if len(committed) != 1 || firstText(t, committed[0]) != "fallback" {
				t.Fatalf("committed fallback = %v", committed)
			}
			if len(published) != 1 {
				t.Fatalf("published events = %v, want one fallback StepDone", eventKinds(published))
			}
			step, ok := published[0].(event.StepDone)
			if !ok || len(step.Messages) != 1 || firstText(t, step.Messages[0]) != "fallback" {
				t.Fatalf("published fallback = %T %+v", published[0], published[0])
			}
		})
	}
}

func TestHistoryGroupingMatchesClaudeGoldenProjection(t *testing.T) {
	t.Parallel()
	type projection struct {
		Groups           []content.AgenticMessages `json:"groups"`
		StepDoneMessages []content.AgenticMessages `json:"step_done_messages"`
		Snapshot         content.AgenticMessages   `json:"snapshot"`
	}
	wantJSON, err := os.ReadFile(filepath.Join("..", "driver", "claude", "testdata", "transcript", "happy.golden.json"))
	if err != nil {
		t.Fatalf("read shared golden: %v", err)
	}
	groups := []content.AgenticMessages{
		{&content.UserMessage{Message: content.Message{Role: content.RoleUser, Blocks: []content.Block{&content.TextBlock{Text: "hi there"}}}}},
		{
			&content.AIMessage{Message: content.Message{Role: content.RoleAssistant, Blocks: []content.Block{
				&content.ThinkingBlock{Thinking: "let me think", Signature: "sig"},
				&content.TextBlock{Text: "Working"},
				&content.ToolUseBlock{ID: "toolu_9", Name: "Read", Input: json.RawMessage(`{"path":"/x"}`)},
			}}},
			&content.ToolResultMessage{Message: content.Message{Role: content.RoleTool, Blocks: []content.Block{&content.TextBlock{Text: "contents"}}}, ToolUseID: "toolu_9"},
		},
		{aiMessage("Done")},
	}
	pub := &fakePublisher{}
	state, _ := newTestLoop(t, Config{Agent: &fakeAgent{history: driver.History{Available: true, Steps: groups}, events: []driver.Event{{Kind: driver.KindTerminalOK}}}, SIDMode: SIDPrebound}, pub)
	submit(t, state, "go")
	waitTurnIndex(t, state, 1)
	var stepDone []content.AgenticMessages
	for _, input := range pub.snapshot() {
		if step, ok := input.(event.StepDone); ok {
			stepDone = append(stepDone, step.Messages)
		}
	}
	snapshot, _, err := state.Snapshot(context.Background())
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	gotJSON, err := json.MarshalIndent(projection{Groups: groups, StepDoneMessages: stepDone, Snapshot: snapshot}, "", "  ")
	if err != nil {
		t.Fatalf("marshal projection: %v", err)
	}
	gotJSON = append(gotJSON, '\n')
	if !bytes.Equal(gotJSON, wantJSON) {
		t.Fatalf("backend commit projection differs from shared Claude golden\ngot:\n%s\nwant:\n%s", gotJSON, wantJSON)
	}
	shutdown(t, state)
}

func TestTurnEventOrderingAndDriverErrorIdentity(t *testing.T) {
	t.Parallel()
	closeErr := &driver.DecodeError{Cause: errors.New("wire")}
	agent := &fakeAgent{closeErr: closeErr, events: []driver.Event{{Kind: driver.KindTextDelta, Text: "hi"}, {Kind: driver.KindTerminalError, ErrText: "provider"}}}
	pub := &fakePublisher{}
	state, _ := newTestLoop(t, Config{Agent: agent, SIDMode: SIDPrebound}, pub)
	submit(t, state, "go")
	waitFor(t, pub, func(input event.Event) bool { _, ok := input.(event.TurnFailed); return ok })
	got := eventKinds(pub.snapshot())
	if want := []string{"event.TurnStarted", "event.TokenDelta", "event.TurnFailed"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("events = %v, want %v", got, want)
	}
	failed := pub.snapshot()[2].(event.TurnFailed)
	var resultErr *ForeignResultError
	var decodeErr *driver.DecodeError
	if !errors.As(failed.Err, &resultErr) || !errors.As(failed.Err, &decodeErr) {
		t.Fatalf("TurnFailed.Err = %T %v", failed.Err, failed.Err)
	}
	shutdown(t, state)
}

func TestUserInputPublishesStartedBeforeSpawnAndHistoryBeforeDone(t *testing.T) {
	t.Parallel()
	agent := &fakeAgent{
		history: driver.History{Available: true, Steps: []content.AgenticMessages{{aiMessage("committed")}}},
		events: []driver.Event{
			{Kind: driver.KindTextDelta, Text: "hi"},
			{Kind: driver.KindToolUse, ToolUseID: "tool-1", ToolName: "Read"},
			{Kind: driver.KindToolResult, ToolUseID: "tool-1", ResultPreview: "ok"},
			{Kind: driver.KindStepComplete, Message: aiMessage("stream step")},
			{Kind: driver.KindTerminalOK, Message: aiMessage("stream done")},
		},
	}
	pub := &fakePublisher{}
	spawnAt := 0
	agent.onSpawn = func() { spawnAt = pub.count() }
	state, _ := newTestLoop(t, Config{Agent: agent, SIDMode: SIDPrebound}, pub)
	commandID := submit(t, state, "do the thing")
	waitTurnIndex(t, state, 1)
	if spawnAt < 1 {
		t.Fatalf("Spawn observed %d events, want TurnStarted first", spawnAt)
	}
	if got, want := eventKinds(pub.snapshot()), []string{"event.TurnStarted", "event.TokenDelta", "event.ToolCallStarted", "event.ToolCallCompleted", "event.StepDone", "event.TurnDone"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("events = %v, want %v", got, want)
	}
	started := pub.snapshot()[0].(event.TurnStarted)
	if started.Cause.CommandID != commandID || firstText(t, started.Message) != "do the thing" {
		t.Fatalf("TurnStarted = %+v", started)
	}
	step := pub.snapshot()[4].(event.StepDone)
	if len(step.Messages) != 1 || firstText(t, step.Messages[0]) != "committed" {
		t.Fatalf("StepDone = %+v", step)
	}
	if turn := agent.lastForeignTurn(); turn.SystemPrompt != validBoundDefinition().EffectiveSystem() || turn.Cwd == "" {
		t.Fatalf("driver turn = %+v", turn)
	}
	shutdown(t, state)
}

func TestSpawnProtocolInterruptAndShutdown(t *testing.T) {
	t.Run("spawn", func(t *testing.T) {
		agent := &fakeAgent{spawnErr: errors.New("spawn")}
		pub := &fakePublisher{}
		state, _ := newTestLoop(t, Config{Agent: agent, SIDMode: SIDPrebound}, pub)
		submit(t, state, "go")
		waitFor(t, pub, func(input event.Event) bool { _, ok := input.(event.TurnFailed); return ok })
		failed := pub.snapshot()[1].(event.TurnFailed)
		var spawnErr *driver.SpawnError
		if !errors.As(failed.Err, &spawnErr) {
			t.Fatalf("err = %T %v", failed.Err, failed.Err)
		}
		shutdown(t, state)
	})
	t.Run("protocol", func(t *testing.T) {
		pub := &fakePublisher{}
		state, _ := newTestLoop(t, Config{Agent: &fakeAgent{}, SIDMode: SIDPrebound}, pub)
		submit(t, state, "go")
		waitFor(t, pub, func(input event.Event) bool { _, ok := input.(event.TurnFailed); return ok })
		var protocolErr *ForeignProtocolError
		if !errors.As(pub.snapshot()[1].(event.TurnFailed).Err, &protocolErr) {
			t.Fatal("missing protocol error")
		}
		shutdown(t, state)
	})
	t.Run("interrupt", func(t *testing.T) {
		pub := &fakePublisher{}
		state, _ := newTestLoop(t, Config{Agent: &fakeAgent{block: true}, SIDMode: SIDPrebound}, pub)
		submit(t, state, "go")
		waitFor(t, pub, func(input event.Event) bool { _, ok := input.(event.TurnStarted); return ok })
		sendInterrupt(t, state)
		waitFor(t, pub, func(input event.Event) bool { _, ok := input.(event.TurnInterrupted); return ok })
		shutdown(t, state)
	})
	t.Run("shutdown", func(t *testing.T) {
		pub := &fakePublisher{}
		state, _ := newTestLoop(t, Config{Agent: &fakeAgent{block: true}, SIDMode: SIDPrebound}, pub)
		submit(t, state, "go")
		waitFor(t, pub, func(input event.Event) bool { _, ok := input.(event.TurnStarted); return ok })
		shutdown(t, state)
	})
}

type queueStream struct {
	ctx    context.Context
	events chan driver.Event
	once   sync.Once
}

func (s *queueStream) Events() <-chan driver.Event    { return s.events }
func (*queueStream) History() (driver.History, error) { return driver.History{Available: false}, nil }
func (*queueStream) Close() error                     { return nil }
func (s *queueStream) finish(inputs ...driver.Event) {
	s.once.Do(func() {
		for _, input := range inputs {
			s.events <- input
		}
		close(s.events)
	})
}

type queueSpawn struct {
	turn   driver.Turn
	stream *queueStream
}
type queueAgent struct{ spawned chan queueSpawn }

func (a *queueAgent) Spawn(ctx context.Context, turn driver.Turn) (driver.Stream, error) {
	stream := &queueStream{ctx: ctx, events: make(chan driver.Event, 4)}
	go func() { <-ctx.Done(); stream.finish() }()
	a.spawned <- queueSpawn{turn: turn, stream: stream}
	return stream, nil
}

func sendManaged(t *testing.T, state *Loop, text string) (uuid.UUID, error) {
	t.Helper()
	id := mustID(t)
	accepted := make(chan error, 1)
	state.Commands <- command.UserInput{Header: command.Header{CommandID: id}, Blocks: []content.Block{&content.TextBlock{Text: text}}, NoFold: true, TargetLoopID: state.loopID, Accepted: accepted}
	return id, <-accepted
}

func nextSpawn(t *testing.T, agent *queueAgent) queueSpawn {
	t.Helper()
	select {
	case spawned := <-agent.spawned:
		return spawned
	case <-time.After(2 * time.Second):
		t.Fatal("spawn timeout")
		return queueSpawn{}
	}
}

func TestManagedQueueFIFOAndExactCapacity(t *testing.T) {
	t.Parallel()
	agent := &queueAgent{spawned: make(chan queueSpawn, loop.ManagedInputQueueCapacity+2)}
	state, _ := newTestLoop(t, Config{Agent: agent, SIDMode: SIDPrebound}, &fakePublisher{})
	submit(t, state, "active")
	active := nextSpawn(t, agent)
	for i := 0; i < loop.ManagedInputQueueCapacity; i++ {
		if _, err := sendManaged(t, state, fmt.Sprintf("queued-%02d", i)); err != nil {
			t.Fatalf("queue %d: %v", i, err)
		}
	}
	_, err := sendManaged(t, state, "overflow")
	var rejected *loop.InputRejectedError
	if !errors.As(err, &rejected) || rejected.Reason != event.RejectQueueFull {
		t.Fatalf("overflow = %T %v", err, err)
	}
	active.stream.finish(driver.Event{Kind: driver.KindTerminalOK})
	first := nextSpawn(t, agent)
	if got := first.turn.Input[0].(*content.TextBlock).Text; got != "queued-00" {
		t.Fatalf("first queued turn = %q", got)
	}
	sendInterrupt(t, state)
	shutdown(t, state)
}

func TestManagedQueueRunsAllAcceptedInputsFIFO(t *testing.T) {
	t.Parallel()
	agent := &queueAgent{spawned: make(chan queueSpawn, 3)}
	state, _ := newTestLoop(t, Config{Agent: agent, SIDMode: SIDPrebound}, &fakePublisher{})
	submit(t, state, "A")
	a := nextSpawn(t, agent)
	if _, err := sendManaged(t, state, "B"); err != nil {
		t.Fatal(err)
	}
	if _, err := sendManaged(t, state, "C"); err != nil {
		t.Fatal(err)
	}
	a.stream.finish(driver.Event{Kind: driver.KindTerminalOK, Message: aiMessage("answer A")})
	b := nextSpawn(t, agent)
	if got := b.turn.Input[0].(*content.TextBlock).Text; got != "B" {
		t.Fatalf("second input = %q", got)
	}
	b.stream.finish(driver.Event{Kind: driver.KindTerminalOK, Message: aiMessage("answer B")})
	c := nextSpawn(t, agent)
	if got := c.turn.Input[0].(*content.TextBlock).Text; got != "C" {
		t.Fatalf("third input = %q", got)
	}
	c.stream.finish(driver.Event{Kind: driver.KindTerminalOK, Message: aiMessage("answer C")})
	waitTurnIndex(t, state, 3)
	shutdown(t, state)
}

func waitCancelled(t *testing.T, pub *fakePublisher, reason event.CancelReason, ids ...uuid.UUID) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		seen := make(map[uuid.UUID]bool)
		for _, input := range pub.snapshot() {
			if cancelled, ok := input.(event.InputCancelled); ok && cancelled.Reason == reason {
				seen[cancelled.Cause.CommandID] = true
			}
		}
		all := true
		for _, id := range ids {
			all = all && seen[id]
		}
		if all {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("cancellations not observed: %v", eventKinds(pub.snapshot()))
}

func TestProviderFailureFlushesAcceptedQueueWithoutSpawningIt(t *testing.T) {
	t.Parallel()
	agent := &queueAgent{spawned: make(chan queueSpawn, 3)}
	pub := &fakePublisher{}
	state, _ := newTestLoop(t, Config{Agent: agent, SIDMode: SIDPrebound}, pub)
	submit(t, state, "active")
	active := nextSpawn(t, agent)
	b, _ := sendManaged(t, state, "B")
	c, _ := sendManaged(t, state, "C")
	active.stream.finish(driver.Event{Kind: driver.KindTerminalError, ErrText: "provider failed"})
	waitCancelled(t, pub, event.CancelTurnFailed, b, c)
	select {
	case unexpected := <-agent.spawned:
		t.Fatalf("failed turn spawned queued input: %+v", unexpected.turn)
	default:
	}
	shutdown(t, state)
}

func TestShutdownFlushesAcceptedQueue(t *testing.T) {
	t.Parallel()
	agent := &queueAgent{spawned: make(chan queueSpawn, 2)}
	pub := &fakePublisher{}
	state, _ := newTestLoop(t, Config{Agent: agent, SIDMode: SIDPrebound}, pub)
	submit(t, state, "active")
	_ = nextSpawn(t, agent)
	queued, _ := sendManaged(t, state, "queued")
	shutdown(t, state)
	waitCancelled(t, pub, event.CancelTurnInterrupted, queued)
}

func TestTargetedCancelActivePreservesNextQueuedInput(t *testing.T) {
	t.Parallel()
	agent := &queueAgent{spawned: make(chan queueSpawn, 2)}
	pub := &fakePublisher{}
	state, _ := newTestLoop(t, Config{Agent: agent, SIDMode: SIDPrebound}, pub)
	activeID := submit(t, state, "active")
	_ = nextSpawn(t, agent)
	_, _ = sendManaged(t, state, "next")
	ack := make(chan command.DelegateCancelResult, 1)
	state.Commands <- command.CancelDelegateRequest{
		Header:          command.Header{CommandID: mustID(t)},
		Coordinates:     identity.Coordinates{SessionID: state.sessionID, LoopID: state.loopID},
		TargetCommandID: activeID,
		Ack:             ack,
	}
	if got := <-ack; got != command.DelegateCancelActive {
		t.Fatalf("cancel active = %v", got)
	}
	next := nextSpawn(t, agent)
	if got := next.turn.Input[0].(*content.TextBlock).Text; got != "next" {
		t.Fatalf("preserved next input = %q", got)
	}
	sendInterrupt(t, state)
	shutdown(t, state)
}

func TestCancelQueuedRequestPreservesOthers(t *testing.T) {
	t.Parallel()
	agent := &queueAgent{spawned: make(chan queueSpawn, 3)}
	pub := &fakePublisher{}
	state, _ := newTestLoop(t, Config{Agent: agent, SIDMode: SIDPrebound}, pub)
	submit(t, state, "active")
	active := nextSpawn(t, agent)
	cancelID, _ := sendManaged(t, state, "cancel")
	_, _ = sendManaged(t, state, "keep")
	ack := make(chan command.DelegateCancelResult, 1)
	state.Commands <- command.CancelDelegateRequest{Header: command.Header{CommandID: mustID(t)}, Coordinates: identity.Coordinates{SessionID: state.sessionID, LoopID: state.loopID}, TargetCommandID: cancelID, Ack: ack}
	if got := <-ack; got != command.DelegateCancelQueued {
		t.Fatalf("cancel = %v", got)
	}
	active.stream.finish(driver.Event{Kind: driver.KindTerminalOK})
	kept := nextSpawn(t, agent)
	if got := kept.turn.Input[0].(*content.TextBlock).Text; got != "keep" {
		t.Fatalf("next = %q", got)
	}
	sendInterrupt(t, state)
	shutdown(t, state)
}

func TestLateBoundLockLifecycleAndResume(t *testing.T) {
	t.Parallel()
	cwd := t.TempDir()
	agent := &fakeAgent{block: true, events: []driver.Event{{Kind: driver.KindInit, SessionID: "learned"}}}
	pub := &fakePublisher{}
	state, sid := newTestLoop(t, Config{Agent: agent, Cwd: cwd, SIDMode: SIDLateBound}, pub)
	if sid != "" {
		t.Fatalf("initial sid = %q", sid)
	}
	submit(t, state, "first")
	waitFor(t, pub, func(input event.Event) bool { _, ok := input.(event.ForeignSessionBound); return ok })
	lock, err := acquireForeignLock("learned", cwd)
	if lock != nil {
		lock.release()
	}
	var busy *ForeignSessionBusyError
	if !errors.As(err, &busy) {
		t.Fatalf("durable lock = %T %v", err, err)
	}
	sendInterrupt(t, state)
	agent.mu.Lock()
	agent.block = false
	agent.events = []driver.Event{{Kind: driver.KindTerminalOK}}
	agent.mu.Unlock()
	submit(t, state, "resume")
	waitTurnIndex(t, state, 1)
	if turn := agent.lastForeignTurn(); turn.StartNew || turn.ForeignSID != "learned" {
		t.Fatalf("resume turn = %+v", turn)
	}
	shutdown(t, state)
}

type orderedLock struct {
	name  string
	held  bool
	trace *[]string
}

func (lock *orderedLock) release() {
	*lock.trace = append(*lock.trace, "release "+lock.name)
	lock.held = false
}

type orderedStream struct {
	events  chan driver.Event
	durable *orderedLock
	trace   *[]string
	once    sync.Once
}

func (stream *orderedStream) Events() <-chan driver.Event { return stream.events }

func (stream *orderedStream) History() (driver.History, error) {
	if stream.durable.held {
		*stream.trace = append(*stream.trace, "history while durable held")
	} else {
		*stream.trace = append(*stream.trace, "history after durable release")
	}
	return driver.History{Available: false}, nil
}

func (stream *orderedStream) Close() error {
	stream.once.Do(func() {
		if stream.durable.held {
			*stream.trace = append(*stream.trace, "close stream while durable held")
		} else {
			*stream.trace = append(*stream.trace, "close stream after durable release")
		}
	})
	return nil
}

type orderedAgent struct{ stream driver.Stream }

func (agent orderedAgent) Spawn(context.Context, driver.Turn) (driver.Stream, error) {
	return agent.stream, nil
}

func TestLateBoundTurnLockLifecycleOrderMatchesPredecessor(t *testing.T) {
	t.Parallel()
	var trace []string
	temporary := &orderedLock{name: "temporary", held: true, trace: &trace}
	durable := &orderedLock{name: "durable", trace: &trace}
	locks := turnLockOps{
		acquireTemporary: func(string, string) (turnLock, error) {
			trace = append(trace, "acquire temporary")
			return temporary, nil
		},
		acquireDurable: func(string, string) (turnLock, error) {
			trace = append(trace, "acquire durable")
			durable.held = true
			return durable, nil
		},
	}
	events := make(chan driver.Event, 2)
	events <- driver.Event{Kind: driver.KindInit, SessionID: "foreign-session"}
	events <- driver.Event{Kind: driver.KindTerminalOK}
	close(events)
	stream := &orderedStream{events: events, durable: durable, trace: &trace}
	state := &Loop{backendCfg: Config{Agent: orderedAgent{stream: stream}, Cwd: t.TempDir()}}
	result := make(chan turnOutcome, 1)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	pub := func(input event.Event) {
		if _, ok := input.(event.ForeignSessionBound); ok {
			trace = append(trace, "publish ForeignSessionBound")
		}
	}

	go state.driveTurnWithLocks(ctx, cancel, driver.Turn{}, 1, false, pub, result, locks)
	select {
	case <-result:
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for turn outcome")
	}
	trace = append(trace, "actor receives outcome")

	want := []string{
		"acquire temporary",
		"acquire durable",
		"release temporary",
		"publish ForeignSessionBound",
		"close stream while durable held",
		"history while durable held",
		"release durable",
		"actor receives outcome",
	}
	if !reflect.DeepEqual(trace, want) {
		t.Fatalf("lifecycle trace = %v, want %v", trace, want)
	}
}

func TestLateBoundSessionBoundSequenceHeaderAndResumeParity(t *testing.T) {
	t.Parallel()
	agent := &fakeAgent{events: []driver.Event{
		{Kind: driver.KindInit, SessionID: "codex-thread-1"},
		{Kind: driver.KindStepComplete, Message: aiMessage("ok")},
		{Kind: driver.KindTerminalOK},
	}}
	pub := &fakePublisher{}
	state, sid := newTestLoop(t, Config{Agent: agent, Cwd: t.TempDir(), SIDMode: SIDLateBound}, pub)
	if sid != "" {
		t.Fatalf("initial sid = %q, want empty", sid)
	}

	submit(t, state, "first")
	waitTurnIndex(t, state, 1)
	wantFirst := []string{"event.TurnStarted", "event.ForeignSessionBound", "event.StepDone", "event.TurnDone"}
	if got := eventKinds(pub.snapshot()); !reflect.DeepEqual(got, wantFirst) {
		t.Fatalf("first-turn events = %v, want %v", got, wantFirst)
	}
	bound := pub.snapshot()[1].(event.ForeignSessionBound)
	if bound.ForeignSID != "codex-thread-1" {
		t.Fatalf("ForeignSID = %q", bound.ForeignSID)
	}
	if err := event.ValidateEvent(bound); err != nil {
		t.Fatalf("ForeignSessionBound validation: %v", err)
	}
	header := bound.EventHeader()
	if header.SessionID != state.sessionID || header.LoopID != state.loopID || !header.TurnID.IsZero() || !header.StepID.IsZero() {
		t.Fatalf("ForeignSessionBound coordinates = %+v", header.Coordinates)
	}

	submit(t, state, "second")
	waitTurnIndex(t, state, 2)
	turn := agent.lastForeignTurn()
	if turn.StartNew || turn.ForeignSID != "codex-thread-1" {
		t.Fatalf("resume turn = {StartNew:%v ForeignSID:%q}", turn.StartNew, turn.ForeignSID)
	}
	boundCount := 0
	for _, input := range pub.snapshot() {
		if _, ok := input.(event.ForeignSessionBound); ok {
			boundCount++
		}
	}
	if boundCount != 1 {
		t.Fatalf("ForeignSessionBound count = %d, want 1", boundCount)
	}
	shutdown(t, state)
}

func TestLateBoundFirstTurnsUseIndependentTemporaryLocks(t *testing.T) {
	t.Parallel()
	cwd := t.TempDir()
	firstPub, secondPub := &fakePublisher{}, &fakePublisher{}
	first, _ := newTestLoop(t, Config{Agent: &fakeAgent{block: true, events: []driver.Event{{Kind: driver.KindInit, SessionID: "one"}}}, Cwd: cwd, SIDMode: SIDLateBound}, firstPub)
	second, _ := newTestLoop(t, Config{Agent: &fakeAgent{block: true, events: []driver.Event{{Kind: driver.KindInit, SessionID: "two"}}}, Cwd: cwd, SIDMode: SIDLateBound}, secondPub)
	submit(t, first, "first")
	waitFor(t, firstPub, func(input event.Event) bool { _, ok := input.(event.ForeignSessionBound); return ok })
	submit(t, second, "second")
	waitFor(t, secondPub, func(input event.Event) bool { _, ok := input.(event.ForeignSessionBound); return ok })
	shutdown(t, first)
	shutdown(t, second)
}

func TestLateBoundTransitionFailurePersistsSIDForBusyResume(t *testing.T) {
	t.Parallel()
	cwd := t.TempDir()
	held, err := acquireForeignLock("busy", cwd)
	if err != nil {
		t.Fatalf("hold durable lock: %v", err)
	}
	t.Cleanup(func() { cleanupForeignLock(t, held) })
	agent := &fakeAgent{events: []driver.Event{{Kind: driver.KindInit, SessionID: "busy"}}}
	pub := &fakePublisher{}
	state, _ := newTestLoop(t, Config{Agent: agent, Cwd: cwd, SIDMode: SIDLateBound}, pub)
	submit(t, state, "first")
	waitFor(t, pub, func(input event.Event) bool { _, ok := input.(event.TurnFailed); return ok })
	wantFirst := []string{"event.TurnStarted", "event.ForeignSessionBound", "event.TurnFailed"}
	if got := eventKinds(pub.snapshot()); !reflect.DeepEqual(got, wantFirst) {
		t.Fatalf("first-turn events = %v, want %v", got, wantFirst)
	}
	firstFailed := pub.snapshot()[2].(event.TurnFailed)
	var firstBusy *ForeignSessionBusyError
	if !errors.As(firstFailed.Err, &firstBusy) || firstBusy.SID != "busy" || firstBusy.Cwd != cwd || firstBusy.PID != os.Getpid() {
		t.Fatalf("first TurnFailed.Err = %T %v, want joined ForeignSessionBusyError", firstFailed.Err, firstFailed.Err)
	}
	waitLoopIdle(t, state)
	submit(t, state, "resume")
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if len(pub.snapshot()) >= 5 {
			break
		}
		time.Sleep(time.Millisecond)
	}
	wantAll := []string{"event.TurnStarted", "event.ForeignSessionBound", "event.TurnFailed", "event.TurnStarted", "event.TurnFailed"}
	if got := eventKinds(pub.snapshot()); !reflect.DeepEqual(got, wantAll) {
		t.Fatalf("all events = %v, want %v", got, wantAll)
	}
	secondFailed := pub.snapshot()[4].(event.TurnFailed)
	var secondBusy *ForeignSessionBusyError
	if !errors.As(secondFailed.Err, &secondBusy) || secondBusy.SID != "busy" || secondBusy.Cwd != cwd || secondBusy.PID != os.Getpid() {
		t.Fatalf("second TurnFailed.Err = %T %v, want ForeignSessionBusyError", secondFailed.Err, secondFailed.Err)
	}
	if agent.calls() != 1 {
		t.Fatalf("spawns = %d, want 1", agent.calls())
	}
	shutdown(t, state)
}
