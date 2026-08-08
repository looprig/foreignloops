package acp

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/looprig/acp/client"
	"github.com/looprig/acp/protocol"
	"github.com/looprig/core/content"
	"github.com/looprig/foreignloops/driver"
)

type scriptedSession struct {
	id      protocol.SessionID
	updates chan client.Update

	promptStarts chan struct{}
	promptCalls  atomic.Int32
	barrierCalls atomic.Int32
	promptHook   func(int, []protocol.ContentBlock) (*client.PromptResult, error)
	barrierHook  func(context.Context) error

	mu      sync.Mutex
	prompts [][]protocol.ContentBlock
}

func newScriptedSession(id string) *scriptedSession {
	return &scriptedSession{
		id:           protocol.SessionID(id),
		updates:      make(chan client.Update, 64),
		promptStarts: make(chan struct{}, 16),
	}
}

func (s *scriptedSession) ID() protocol.SessionID { return s.id }

func (s *scriptedSession) ConfigOptions() []protocol.SessionConfigOption { return nil }

func (s *scriptedSession) Modes() *protocol.SessionModeState { return nil }

func (s *scriptedSession) SetConfigOption(context.Context, protocol.SessionConfigID, protocol.SessionConfigValueID) error {
	return nil
}

func (s *scriptedSession) SetMode(context.Context, protocol.SessionModeID) error { return nil }

func (s *scriptedSession) Prompt(_ context.Context, blocks []protocol.ContentBlock) (*client.PromptResult, error) {
	call := int(s.promptCalls.Add(1))
	s.mu.Lock()
	s.prompts = append(s.prompts, cloneACPBlocks(blocks))
	s.mu.Unlock()
	s.promptStarts <- struct{}{}
	if s.promptHook != nil {
		return s.promptHook(call, blocks)
	}
	return &client.PromptResult{StopReason: protocol.StopReasonEndTurn}, nil
}

func (s *scriptedSession) Updates() <-chan client.Update { return s.updates }

func (s *scriptedSession) WaitForUpdates(ctx context.Context) error {
	s.barrierCalls.Add(1)
	if s.barrierHook != nil {
		return s.barrierHook(ctx)
	}
	return nil
}

func cloneACPBlocks(blocks []protocol.ContentBlock) []protocol.ContentBlock {
	return append([]protocol.ContentBlock(nil), blocks...)
}

func newTurnTestDriver(sess *scriptedSession) *Driver {
	return &Driver{session: sess, driverCtx: context.Background()}
}

func TestACPProtocolPromptErrorProjectsOnlySafeDetail(t *testing.T) {
	t.Parallel()

	const (
		resetMessage  = "Usage limit reached; resets at 3:00 PM"
		dataSentinel  = "data-secret-should-not-escape"
		causeSentinel = "cause path=/private/acp token=token-secret"
		outerSentinel = "outer path=/tmp/acp URL=https://example.test/?key=url-secret"
	)
	tests := []struct {
		name string
		err  error
	}{
		{
			name: "wire error",
			err: fmt.Errorf("%s: %w", outerSentinel, &protocol.Error{
				Code:    protocol.ErrorCodeAuthenticationRequired,
				Message: "Usage limit reached;\nresets at 3:00 PM\t\x00",
				Data:    json.RawMessage(`{"secret":"` + dataSentinel + `"}`),
			}),
		},
		{
			name: "fault with cause and data",
			err: fmt.Errorf("%s: %w", outerSentinel, protocol.AuthRequired(
				"Usage limit reached;\rresets at 3:00 PM\x01",
				errors.New(causeSentinel),
			).WithData(map[string]string{"secret": dataSentinel})),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sess := newScriptedSession("safe-error")
			sess.promptHook = func(int, []protocol.ContentBlock) (*client.PromptResult, error) {
				return nil, tt.err
			}
			stream, err := newTurnTestDriver(sess).Spawn(context.Background(), driver.Turn{})
			if err != nil {
				t.Fatalf("Spawn() error = %v", err)
			}
			events := collectTurnEvents(t, stream)
			if len(events) != 1 || events[0].Kind != driver.KindTerminalError {
				t.Fatalf("events = %#v, want one terminal error", events)
			}
			if got, want := events[0].ErrText, "ACP error -32000: "+resetMessage; got != want {
				t.Fatalf("ErrText = %q, want %q", got, want)
			}
			if !eventIsModelFacing(events[0]) {
				t.Fatalf("event = %#v, want explicit model-facing marker", events[0])
			}
			for _, forbidden := range []string{dataSentinel, causeSentinel, outerSentinel, "token-secret", "url-secret"} {
				if strings.Contains(events[0].ErrText, forbidden) {
					t.Fatalf("ErrText = %q contains forbidden sentinel %q", events[0].ErrText, forbidden)
				}
			}
		})
	}
}

func TestACPNonProtocolPromptErrorRemainsGeneric(t *testing.T) {
	t.Parallel()
	sess := newScriptedSession("generic-error")
	sess.promptHook = func(int, []protocol.ContentBlock) (*client.PromptResult, error) {
		return nil, fmt.Errorf("command=/private/bin/acp token=token-secret: %w", errors.New("stderr secret"))
	}
	stream, err := newTurnTestDriver(sess).Spawn(context.Background(), driver.Turn{})
	if err != nil {
		t.Fatalf("Spawn() error = %v", err)
	}
	events := collectTurnEvents(t, stream)
	if len(events) != 1 || events[0].Kind != driver.KindTerminalError {
		t.Fatalf("events = %#v, want one terminal error", events)
	}
	if events[0].ErrText != "acp prompt failed" {
		t.Fatalf("ErrText = %q, want fixed generic category", events[0].ErrText)
	}
	if eventIsModelFacing(events[0]) {
		t.Fatalf("event = %#v, generic ACP failure must not be model-facing", events[0])
	}
}

func TestACPModelFacingPromptErrorIsBoundedAndValidUTF8(t *testing.T) {
	t.Parallel()
	const maxBytes = 512
	message := strings.Repeat("é", maxBytes) + " resets at 3:00 PM"
	sess := newScriptedSession("bounded-error")
	sess.promptHook = func(int, []protocol.ContentBlock) (*client.PromptResult, error) {
		invalid := string([]byte("prefix ")) + string([]byte{0xff, 0xfe}) + message
		return nil, &protocol.Error{Code: protocol.ErrorCodeAuthenticationRequired, Message: invalid}
	}
	stream, err := newTurnTestDriver(sess).Spawn(context.Background(), driver.Turn{})
	if err != nil {
		t.Fatalf("Spawn() error = %v", err)
	}
	events := collectTurnEvents(t, stream)
	if len(events) != 1 {
		t.Fatalf("events = %#v, want one terminal event", events)
	}
	if !eventIsModelFacing(events[0]) {
		t.Fatalf("event = %#v, want model-facing marker", events[0])
	}
	if len(events[0].ErrText) > maxBytes {
		t.Fatalf("ErrText byte length = %d, want <= %d", len(events[0].ErrText), maxBytes)
	}
	if !utf8.ValidString(events[0].ErrText) {
		t.Fatalf("ErrText = %q is not valid UTF-8", events[0].ErrText)
	}
	if strings.ContainsAny(events[0].ErrText, "\r\n\t\x00") {
		t.Fatalf("ErrText = %q contains control/newline injection", events[0].ErrText)
	}
}

func eventIsModelFacing(input driver.Event) bool {
	return input.ModelFacing
}

func TestSpawnTranslatesACPUpdatesAndLeavesSessionOwned(t *testing.T) {
	sess := newScriptedSession("session-1")
	release := make(chan struct{})
	sess.promptHook = func(int, []protocol.ContentBlock) (*client.PromptResult, error) {
		<-release
		return &client.PromptResult{StopReason: protocol.StopReasonEndTurn}, nil
	}
	d := newTurnTestDriver(sess)
	owned := &fakeDialedClient{}
	d.owned = owned

	stream, err := d.Spawn(context.Background(), driver.Turn{
		SystemPrompt: "system rules",
		Input:        []content.Block{&content.TextBlock{Text: "inspect the tree"}},
	})
	if err != nil {
		t.Fatalf("Spawn() error = %v", err)
	}
	<-sess.promptStarts

	sess.updates <- client.Update{SessionUpdate: protocol.SessionUpdate{
		AgentMessageChunk: &protocol.ContentChunk{Content: protocol.ContentBlock{
			Text: &protocol.TextContent{Text: "hello "},
		}},
	}}
	sess.updates <- client.Update{SessionUpdate: protocol.SessionUpdate{
		AgentThoughtChunk: &protocol.ContentChunk{Content: protocol.ContentBlock{
			Text: &protocol.TextContent{Text: "thinking"},
		}},
	}}
	sess.updates <- client.Update{SessionUpdate: protocol.SessionUpdate{
		ToolCall: &protocol.ToolCall{
			ToolCallID: "tool-1",
			Title:      "Read file",
			Kind:       toolKind(protocol.ToolKindRead),
			RawInput:   []byte(`{"path":"main.go"}`),
		},
	}}
	sess.updates <- client.Update{SessionUpdate: protocol.SessionUpdate{
		ToolCallUpdate: &protocol.ToolCallUpdate{
			ToolCallID: "tool-1",
			Status:     toolStatus(protocol.ToolCallStatusCompleted),
			Content: []protocol.ToolCallContent{{
				Content: &protocol.Content{Content: protocol.ContentBlock{
					Text: &protocol.TextContent{Text: "file contents"},
				}},
			}},
		},
	}}
	// Plan is a known ACP update with no driver event yet; it must be ignored.
	sess.updates <- client.Update{SessionUpdate: protocol.SessionUpdate{Plan: &protocol.Plan{}}}
	close(release)

	events := collectTurnEvents(t, stream)
	wantKinds := []driver.Kind{
		driver.KindTextDelta,
		driver.KindThinkingDelta,
		driver.KindToolUse,
		driver.KindToolResult,
		driver.KindStepComplete,
		driver.KindTerminalOK,
	}
	if got := eventKinds(events); !reflect.DeepEqual(got, wantKinds) {
		t.Fatalf("event kinds = %v, want %v", got, wantKinds)
	}
	if events[0].Text != "hello " || events[1].Text != "thinking" {
		t.Fatalf("text events = %#v, want text and thought chunks", events[:2])
	}
	if events[2].ToolUseID != "tool-1" || events[2].ToolName != "Read file" {
		t.Fatalf("tool use = %#v, want stable id and title", events[2])
	}
	if events[3].ToolUseID != "tool-1" || events[3].ResultPreview != "file contents" || events[3].IsError {
		t.Fatalf("tool result = %#v, want completed preview", events[3])
	}
	if events[4].Message == nil || len(events[4].Message.Blocks) != 3 {
		t.Fatalf("step complete message = %#v, want text/thought/tool blocks", events[4].Message)
	}

	sess.mu.Lock()
	prompts := append([][]protocol.ContentBlock(nil), sess.prompts...)
	sess.mu.Unlock()
	if len(prompts) != 1 || len(prompts[0]) != 1 || prompts[0][0].Text == nil {
		t.Fatalf("prompt blocks = %#v, want one text block", prompts)
	}
	wantPrompt := "<looprig-system>system rules</looprig-system>\n\n<user-task>inspect the tree</user-task>"
	if got := prompts[0][0].Text.Text; got != wantPrompt {
		t.Fatalf("prompt text = %q, want %q", got, wantPrompt)
	}

	history, err := stream.History()
	if err != nil {
		t.Fatalf("History() error = %v", err)
	}
	if history.Available || history.Steps != nil {
		t.Fatalf("History() = %#v, want unavailable history", history)
	}
	if err := stream.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	if owned.closeCalls != 0 {
		t.Fatalf("stream Close() closed persistent client %d times, want 0", owned.closeCalls)
	}
}

func TestSpawnReusesOnePersistentSessionAcrossTurns(t *testing.T) {
	sess := newScriptedSession("session-2")
	sess.promptHook = func(call int, _ []protocol.ContentBlock) (*client.PromptResult, error) {
		sess.updates <- client.Update{SessionUpdate: protocol.SessionUpdate{
			AgentMessageChunk: &protocol.ContentChunk{Content: protocol.ContentBlock{
				Text: &protocol.TextContent{Text: "turn-" + string(rune('0'+call))},
			}},
		}}
		return &client.PromptResult{StopReason: protocol.StopReasonEndTurn}, nil
	}
	d := newTurnTestDriver(sess)

	for i := 1; i <= 2; i++ {
		stream, err := d.Spawn(context.Background(), driver.Turn{Input: []content.Block{&content.TextBlock{Text: "next"}}})
		if err != nil {
			t.Fatalf("Spawn(%d) error = %v", i, err)
		}
		events := collectTurnEvents(t, stream)
		if len(events) != 3 || events[0].Kind != driver.KindTextDelta || events[2].Kind != driver.KindTerminalOK {
			t.Fatalf("turn %d events = %#v, want text, step, terminal", i, events)
		}
		if err := stream.Close(); err != nil {
			t.Fatalf("turn %d Close() error = %v", i, err)
		}
	}
	if got := sess.promptCalls.Load(); got != 2 {
		t.Fatalf("Prompt calls = %d, want 2 on one session", got)
	}
}

func TestSpawnWaitsForOrderedUpdateDeliveryBeforeTerminal(t *testing.T) {
	sess := newScriptedSession("session-barrier")
	promptReturned := make(chan struct{})
	barrierStarted := make(chan struct{})
	releaseBarrier := make(chan struct{})
	sess.promptHook = func(call int, _ []protocol.ContentBlock) (*client.PromptResult, error) {
		if call == 1 {
			close(promptReturned)
		}
		return &client.PromptResult{StopReason: protocol.StopReasonEndTurn}, nil
	}
	sess.barrierHook = func(ctx context.Context) error {
		if sess.barrierCalls.Load() != 1 {
			return nil
		}
		close(barrierStarted)
		select {
		case <-releaseBarrier:
			sess.updates <- client.Update{SessionUpdate: protocol.SessionUpdate{
				AgentMessageChunk: &protocol.ContentChunk{Content: protocol.ContentBlock{
					Text: &protocol.TextContent{Text: "delayed update"},
				}},
			}}
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	d := newTurnTestDriver(sess)

	stream, err := d.Spawn(context.Background(), driver.Turn{})
	if err != nil {
		t.Fatalf("Spawn() error = %v", err)
	}
	<-sess.promptStarts
	<-promptReturned
	select {
	case <-barrierStarted:
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for ordered update barrier")
	}
	select {
	case event := <-stream.Events():
		t.Fatalf("received event before barrier release: %#v", event)
	default:
	}

	close(releaseBarrier)
	events := collectTurnEvents(t, stream)
	wantKinds := []driver.Kind{
		driver.KindTextDelta,
		driver.KindStepComplete,
		driver.KindTerminalOK,
	}
	if got := eventKinds(events); !reflect.DeepEqual(got, wantKinds) {
		t.Fatalf("event kinds = %v, want %v", got, wantKinds)
	}
	if events[0].Text != "delayed update" {
		t.Fatalf("first event = %#v, want delayed update", events[0])
	}

	second, err := d.Spawn(context.Background(), driver.Turn{})
	if err != nil {
		t.Fatalf("second Spawn() error = %v", err)
	}
	secondEvents := collectTurnEvents(t, second)
	if got := eventKinds(secondEvents); !reflect.DeepEqual(got, []driver.Kind{driver.KindTerminalOK}) {
		t.Fatalf("second turn events = %v, want terminal only", got)
	}
	if got := sess.barrierCalls.Load(); got != 2 {
		t.Fatalf("barrier calls = %d, want one per persistent turn", got)
	}
}

func TestSpawnSkipsReplayUpdatesDuringForwardingAndDrain(t *testing.T) {
	sess := newScriptedSession("session-replay")
	release := make(chan struct{})
	sess.promptHook = func(int, []protocol.ContentBlock) (*client.PromptResult, error) {
		<-release
		return &client.PromptResult{StopReason: protocol.StopReasonEndTurn}, nil
	}
	d := newTurnTestDriver(sess)
	stream, err := d.Spawn(context.Background(), driver.Turn{})
	if err != nil {
		t.Fatalf("Spawn() error = %v", err)
	}
	<-sess.promptStarts

	sess.updates <- client.Update{
		Meta: client.UpdateMeta{IsReplay: true},
		SessionUpdate: protocol.SessionUpdate{AgentMessageChunk: &protocol.ContentChunk{Content: protocol.ContentBlock{
			Text: &protocol.TextContent{Text: "replayed text"},
		}}},
	}
	sess.updates <- client.Update{SessionUpdate: protocol.SessionUpdate{AgentMessageChunk: &protocol.ContentChunk{Content: protocol.ContentBlock{
		Text: &protocol.TextContent{Text: "live text"},
	}}}}

	var events []driver.Event
	select {
	case event := <-stream.Events():
		events = append(events, event)
		if event.Kind != driver.KindTextDelta || event.Text != "live text" {
			t.Fatalf("first event = %#v, want live text only", event)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for live update")
	}

	// This replay notification is queued for the prompt-completion drain path.
	sess.updates <- client.Update{
		Meta: client.UpdateMeta{IsReplay: true},
		SessionUpdate: protocol.SessionUpdate{ToolCall: &protocol.ToolCall{
			ToolCallID: "replayed-tool",
			Title:      "replayed tool",
		}},
	}
	close(release)
	events = append(events, collectTurnEvents(t, stream)...)

	wantKinds := []driver.Kind{driver.KindTextDelta, driver.KindStepComplete, driver.KindTerminalOK}
	if got := eventKinds(events); !reflect.DeepEqual(got, wantKinds) {
		t.Fatalf("event kinds = %v, want %v", got, wantKinds)
	}
	if events[1].Message == nil || len(events[1].Message.Blocks) != 1 {
		t.Fatalf("step complete message = %#v, want only live text", events[1].Message)
	}
}

func TestDrainTurnUpdatesSkipsReplayNotifications(t *testing.T) {
	updates := make(chan client.Update, 2)
	updates <- client.Update{
		Meta: client.UpdateMeta{IsReplay: true},
		SessionUpdate: protocol.SessionUpdate{ToolCall: &protocol.ToolCall{
			ToolCallID: "replayed-tool",
			Title:      "replayed tool",
		}},
	}
	updates <- client.Update{SessionUpdate: protocol.SessionUpdate{AgentMessageChunk: &protocol.ContentChunk{Content: protocol.ContentBlock{
		Text: &protocol.TextContent{Text: "live drain text"},
	}}}}

	state := &translationState{}
	events := make(chan driver.Event, 2)
	drainTurnUpdates(context.Background(), updates, state, events)
	close(events)

	var got []driver.Event
	for event := range events {
		got = append(got, event)
	}
	if len(got) != 1 || got[0].Kind != driver.KindTextDelta || got[0].Text != "live drain text" {
		t.Fatalf("drained events = %#v, want live text only", got)
	}
	if state.message() == nil || len(state.message().Blocks) != 1 {
		t.Fatalf("drained state = %#v, want only live text", state.message())
	}
}

func TestDriverCloseDoesNotWaitForAbandonedFullStream(t *testing.T) {
	sess := newScriptedSession("session-close-full")
	release := make(chan struct{})
	sess.promptHook = func(int, []protocol.ContentBlock) (*client.PromptResult, error) {
		<-release
		return &client.PromptResult{StopReason: protocol.StopReasonEndTurn}, nil
	}
	driverCtx, driverCancel := context.WithCancel(context.Background())
	d := &Driver{
		session:      sess,
		driverCtx:    driverCtx,
		driverCancel: driverCancel,
		owned:        &fakeDialedClient{},
	}
	stream, err := d.Spawn(context.Background(), driver.Turn{})
	if err != nil {
		t.Fatalf("Spawn() error = %v", err)
	}
	<-sess.promptStarts

	for i := 0; i < cap(stream.Events())+1; i++ {
		sess.updates <- client.Update{SessionUpdate: protocol.SessionUpdate{AgentMessageChunk: &protocol.ContentChunk{Content: protocol.ContentBlock{
			Text: &protocol.TextContent{Text: "queued"},
		}}}}
	}
	deadline := time.NewTimer(3 * time.Second)
	defer deadline.Stop()
	for len(stream.Events()) < cap(stream.Events()) {
		select {
		case <-deadline.C:
			t.Fatalf("stream buffer did not fill: len=%d cap=%d", len(stream.Events()), cap(stream.Events()))
		default:
			time.Sleep(time.Millisecond)
		}
	}

	closeDone := make(chan error, 1)
	go func() { closeDone <- d.Close() }()
	select {
	case <-driverCtx.Done():
		close(release)
	case <-time.After(3 * time.Second):
		close(release)
		t.Fatal("Driver.Close() did not cancel the driver context")
	}
	select {
	case err := <-closeDone:
		if err != nil {
			t.Fatalf("Driver.Close() error = %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Driver.Close() blocked behind an abandoned full stream")
	}
}

func TestTranslateUnknownACPUpdateIsIgnored(t *testing.T) {
	state := &translationState{}
	events := translateUpdate(protocol.SessionUpdate{Plan: &protocol.Plan{Entries: []protocol.PlanEntry{{Content: "future"}}}}, state)
	if len(events) != 0 {
		t.Fatalf("translateUpdate(plan) = %#v, want no driver events", events)
	}
	if events := translateUpdate(protocol.SessionUpdate{}, state); len(events) != 0 {
		t.Fatalf("translateUpdate(zero) = %#v, want no driver events", events)
	}
}

func TestTranslateToolCallDoesNotExposeRawInput(t *testing.T) {
	state := &translationState{}
	rawInput := []byte(`{"token":"raw-token","password":"raw-password","url":"https://user:pass@example.test/?api_key=url-secret"}`)

	events := translateUpdate(protocol.SessionUpdate{ToolCall: &protocol.ToolCall{
		ToolCallID: "tool-secret",
		Title:      "Inspect",
		RawInput:   rawInput,
	}}, state)
	if len(events) != 1 || events[0].ToolUseID != "tool-secret" || events[0].ToolName != "Inspect" {
		t.Fatalf("tool event = %#v, want stable id/name", events)
	}

	message := state.message()
	if message == nil || len(message.Blocks) != 1 {
		t.Fatalf("translated message = %#v, want one tool block", message)
	}
	tool, ok := message.Blocks[0].(*content.ToolUseBlock)
	if !ok {
		t.Fatalf("translated block = %T, want *content.ToolUseBlock", message.Blocks[0])
	}
	if len(tool.Input) != 0 {
		t.Fatalf("tool input = %q, want no raw payload", tool.Input)
	}

	encoded, err := json.Marshal(message)
	if err != nil {
		t.Fatalf("marshal translated message: %v", err)
	}
	assertDoesNotContain(t, string(encoded), "raw-token", "raw-password", "url-secret", "user:pass")
	assertDoesNotContain(t, string(events[0].ToolName), "raw-token", "raw-password", "url-secret", "user:pass")
}

func TestTranslateToolResultRedactsSecretsAndUnsafeURLs(t *testing.T) {
	state := &translationState{}
	rawOutput := []byte(`{"status":"completed","message":"ordinary output","token":"raw-token","password":"raw-password","authorization":"Bearer raw-authorization","url":"https://user:pass@example.test/result?x=1&api_key=url-secret"}`)

	events := translateUpdate(protocol.SessionUpdate{ToolCallUpdate: &protocol.ToolCallUpdate{
		ToolCallID: "tool-result",
		Status:     toolStatus(protocol.ToolCallStatusCompleted),
		RawOutput:  rawOutput,
	}}, state)
	if len(events) != 1 || events[0].Kind != driver.KindToolResult {
		t.Fatalf("tool result events = %#v, want one KindToolResult", events)
	}
	preview := events[0].ResultPreview
	if !strings.Contains(preview, "ordinary output") || !strings.Contains(preview, "completed") {
		t.Fatalf("preview = %q, want ordinary output preserved", preview)
	}
	assertDoesNotContain(t, preview, "raw-token", "raw-password", "raw-authorization", "url-secret", "user:pass", "example.test", "/result", "x=1")
}

func TestTranslateToolResultRedactsSecretsInHumanText(t *testing.T) {
	state := &translationState{}
	text := "status: ok token=raw-token password: raw-password; visit https://user:pass@example.test/path?x=1&secret=url-secret"
	events := translateUpdate(protocol.SessionUpdate{ToolCallUpdate: &protocol.ToolCallUpdate{
		ToolCallID: "tool-text",
		Content: []protocol.ToolCallContent{{
			Content: &protocol.Content{Content: protocol.ContentBlock{
				Text: &protocol.TextContent{Text: text},
			}},
		}},
	}}, state)
	if len(events) != 1 {
		t.Fatalf("tool result events = %#v, want one event", events)
	}
	if !strings.Contains(events[0].ResultPreview, "status: ok") {
		t.Fatalf("preview = %q, want ordinary text preserved", events[0].ResultPreview)
	}
	assertDoesNotContain(t, events[0].ResultPreview, "raw-token", "raw-password", "url-secret", "user:pass")
}

func TestTranslateToolResultPreservesPlainJSONText(t *testing.T) {
	preview := renderToolResult(nil, json.RawMessage(`"ordinary output"`))
	if preview != "ordinary output" {
		t.Fatalf("preview = %q, want unquoted ordinary text", preview)
	}
}

func TestTranslateToolResultRedactsUnknownAndNestedJSONFields(t *testing.T) {
	rawOutput := json.RawMessage(`{"status":"completed","summary":"safe summary","credential":"raw-credential","provider":"raw-provider","error":"raw-error","unknown":"raw-unknown","detail":{"message":"nested safe summary","credential":"nested-credential","provider":"nested-provider","error":"nested-error"}}`)
	preview := renderToolResult(nil, rawOutput)
	if !strings.Contains(preview, "completed") || !strings.Contains(preview, "safe summary") || !strings.Contains(preview, "nested safe summary") {
		t.Fatalf("preview = %q, want safe bounded summaries preserved", preview)
	}
	assertDoesNotContain(t, preview,
		"raw-credential", "raw-provider", "raw-error", "raw-unknown",
		"nested-credential", "nested-provider", "nested-error",
		`"credential"`, `"provider"`, `"error"`, `"unknown"`,
	)
}

func TestTranslateToolResultRedactsGatewayURLsInJSONAndPlainText(t *testing.T) {
	rawOutput := json.RawMessage(`{"status":"ok","message":"gateway response","url":"https://gateway.internal/v1/private?token=raw-token&x=1","detail":{"link":"https://gateway.internal/nested/path?credential=raw-credential"}}`)
	jsonPreview := renderToolResult(nil, rawOutput)
	if !strings.Contains(jsonPreview, "gateway response") {
		t.Fatalf("JSON preview = %q, want safe message preserved", jsonPreview)
	}
	assertDoesNotContain(t, jsonPreview,
		"gateway.internal", "/v1/private", "/nested/path", "raw-token", "raw-credential", "x=1",
	)

	plain := "status: ok provider=raw-provider error: raw-error credential=raw-credential visit https://gateway.internal/api/private?token=raw-token"
	plainEvents := translateUpdate(protocol.SessionUpdate{ToolCallUpdate: &protocol.ToolCallUpdate{
		ToolCallID: "tool-plain-sensitive",
		Content: []protocol.ToolCallContent{{
			Content: &protocol.Content{Content: protocol.ContentBlock{
				Text: &protocol.TextContent{Text: plain},
			}},
		}},
	}}, &translationState{})
	if len(plainEvents) != 1 || !strings.Contains(plainEvents[0].ResultPreview, "status: ok") {
		t.Fatalf("plain events = %#v, want safe status preserved", plainEvents)
	}
	assertDoesNotContain(t, plainEvents[0].ResultPreview,
		"raw-provider", "raw-error", "raw-credential", "gateway.internal", "/api/private", "raw-token",
	)
}

func TestTranslateToolResultRedactsSensitivePhrasesInsideSafeFields(t *testing.T) {
	rawOutput := json.RawMessage(`{"message":"provider Anthropic credential sk-live-message error raw provider failure","summary":"ordinary safe summary","detail":{"text":"provider OpenAI credential sk-live-nested"}}`)
	preview := renderToolResult(nil, rawOutput)
	if !strings.Contains(preview, "ordinary safe summary") {
		t.Fatalf("preview = %q, want ordinary safe summary preserved", preview)
	}
	assertDoesNotContain(t, preview,
		"Anthropic", "OpenAI", "sk-live-message", "sk-live-nested", "raw provider failure",
	)

	plainPreview := renderToolResult(nil, json.RawMessage(`provider Anthropic credential sk-live-raw`))
	assertDoesNotContain(t, plainPreview, "Anthropic", "sk-live-raw")
}

func TestTranslateToolTitleRedactsUnknownFieldsAndURLs(t *testing.T) {
	title := "Inspect provider Anthropic credential sk-live-title https://gateway.internal/tools/private"
	events := translateUpdate(protocol.SessionUpdate{ToolCall: &protocol.ToolCall{
		ToolCallID: "tool-title-sensitive",
		Title:      title,
	}}, &translationState{})
	if len(events) != 1 || !strings.Contains(events[0].ToolName, "Inspect") {
		t.Fatalf("tool events = %#v, want safe title summary", events)
	}
	assertDoesNotContain(t, events[0].ToolName,
		"Anthropic", "sk-live-title", "gateway.internal", "/tools/private",
	)
}

func TestToolTranslationBoundsAdversarialOutputAndTitle(t *testing.T) {
	title := "Inspect token=title-secret " + strings.Repeat("title ", maxACPPreviewRunes*32)
	state := &translationState{}
	toolEvents := translateUpdate(protocol.SessionUpdate{ToolCall: &protocol.ToolCall{
		ToolCallID: "tool-large",
		Title:      title,
	}}, state)
	if len(toolEvents) != 1 {
		t.Fatalf("tool events = %#v, want one event", toolEvents)
	}
	if len([]rune(toolEvents[0].ToolName)) > maxACPPreviewRunes {
		t.Fatalf("tool name length = %d, want <= %d", len([]rune(toolEvents[0].ToolName)), maxACPPreviewRunes)
	}
	assertDoesNotContain(t, toolEvents[0].ToolName, "title-secret")

	largeOutput := strings.Repeat("ordinary output ", maxACPPreviewRunes*32)
	resultEvents := translateUpdate(protocol.SessionUpdate{ToolCallUpdate: &protocol.ToolCallUpdate{
		ToolCallID: "tool-large",
		Content: []protocol.ToolCallContent{{
			Content: &protocol.Content{Content: protocol.ContentBlock{
				Text: &protocol.TextContent{Text: largeOutput},
			}},
		}, {
			Content: &protocol.Content{Content: protocol.ContentBlock{
				Text: &protocol.TextContent{Text: " password=output-secret"},
			}},
		}},
	}}, state)
	if len(resultEvents) != 1 {
		t.Fatalf("result events = %#v, want one event", resultEvents)
	}
	if len([]rune(resultEvents[0].ResultPreview)) > maxACPPreviewRunes {
		t.Fatalf("result preview length = %d, want <= %d", len([]rune(resultEvents[0].ResultPreview)), maxACPPreviewRunes)
	}
	assertDoesNotContain(t, resultEvents[0].ResultPreview, "output-secret")
}

func TestTranslateToolResultFailsSafelyOnMalformedOrAmbiguousJSON(t *testing.T) {
	for _, raw := range []json.RawMessage{
		[]byte(`{"token":"malformed-token",`),
		[]byte(`{"provider":"malformed-provider",`),
		[]byte(`{"status":"ok"} {"password":"trailing-password"}`),
		[]byte(`{"secret":"first-secret","secret":"second-secret"}`),
	} {
		t.Run(string(raw), func(t *testing.T) {
			preview := renderToolResult(nil, raw)
			if preview == string(raw) {
				t.Fatalf("preview returned raw JSON %q", preview)
			}
			assertDoesNotContain(t, preview, "malformed-token", "malformed-provider", "trailing-password", "first-secret", "second-secret")
		})
	}
}

func assertDoesNotContain(t *testing.T, got string, forbidden ...string) {
	t.Helper()
	for _, value := range forbidden {
		if strings.Contains(got, value) {
			t.Fatalf("value %q leaked in %q", value, got)
		}
	}
}

func TestStreamLifecycleIsRaceSafeDuringConcurrentDelivery(t *testing.T) {
	sess := newScriptedSession("session-race")
	release := make(chan struct{})
	sess.promptHook = func(int, []protocol.ContentBlock) (*client.PromptResult, error) {
		<-release
		return &client.PromptResult{StopReason: protocol.StopReasonEndTurn}, nil
	}
	d := newTurnTestDriver(sess)
	stream, err := d.Spawn(context.Background(), driver.Turn{Input: []content.Block{&content.TextBlock{Text: "race"}}})
	if err != nil {
		t.Fatalf("Spawn() error = %v", err)
	}
	<-sess.promptStarts

	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			sess.updates <- client.Update{SessionUpdate: protocol.SessionUpdate{
				AgentMessageChunk: &protocol.ContentChunk{Content: protocol.ContentBlock{
					Text: &protocol.TextContent{Text: string(rune('a' + i))},
				}},
			}}
		}(i)
	}
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = stream.Close()
		}()
	}
	close(release)
	_ = collectTurnEvents(t, stream)
	wg.Wait()
}

func toolKind(kind protocol.ToolKind) *protocol.ToolKind { return &kind }

func toolStatus(status protocol.ToolCallStatus) *protocol.ToolCallStatus { return &status }

func collectTurnEvents(t *testing.T, stream driver.Stream) []driver.Event {
	t.Helper()
	var events []driver.Event
	timeout := time.NewTimer(3 * time.Second)
	defer timeout.Stop()
	for {
		select {
		case event, ok := <-stream.Events():
			if !ok {
				return events
			}
			events = append(events, event)
		case <-timeout.C:
			t.Fatalf("timed out waiting for stream close; events = %#v", events)
		}
	}
}

func eventKinds(events []driver.Event) []driver.Kind {
	kinds := make([]driver.Kind, 0, len(events))
	for _, event := range events {
		kinds = append(kinds, event.Kind)
	}
	return kinds
}

var _ session = (*scriptedSession)(nil)
var _ turnSession = (*scriptedSession)(nil)
var _ driver.Agent = (*Driver)(nil)
