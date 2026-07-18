package backend

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"

	"github.com/looprig/core/content"
	"github.com/looprig/core/uuid"
	"github.com/looprig/foreignloop/driver"
	"github.com/looprig/harness/pkg/event"
	"github.com/looprig/harness/pkg/foreign"
	"github.com/looprig/harness/pkg/loop"
)

func restoredValidConfig(t *testing.T) Config {
	t.Helper()
	return Config{
		Agent:   &fakeAgent{},
		Cwd:     t.TempDir(),
		Posture: driver.PostureDefault,
		SIDMode: SIDPrebound,
	}
}

func validRestoredSeed() foreign.RestoredForeign {
	return foreign.RestoredForeign{
		ForeignSID: "sid-restored",
		TurnIndex:  3,
		Msgs:       content.AgenticMessages{aiMessage("first"), aiMessage("second")},
	}
}

type typedNilPublisher struct{}

func (*typedNilPublisher) PublishEvent(context.Context, event.Event) error {
	panic("typedNilPublisher.PublishEvent must never be called")
}

func (*typedNilPublisher) PublishEventChecked(context.Context, event.Event) error {
	panic("typedNilPublisher.PublishEventChecked must never be called")
}

var _ foreign.EventPublisher = (*typedNilPublisher)(nil)

func TestBuildRestoredWithReturnsHarnessBuilder(t *testing.T) {
	t.Parallel()
	var _ foreign.RestoredBuilder = BuildRestoredWith(restoredValidConfig(t))
}

func TestBuildRestoredWithFailsClosedOnInvalidConfig(t *testing.T) {
	t.Parallel()
	cfg := restoredValidConfig(t)
	cfg.Cwd = ""
	got, err := BuildRestoredWith(cfg)(
		context.Background(),
		uuid.UUID{},
		uuid.UUID{},
		loop.Provenance{},
		nil,
		nil,
		nil,
		nil,
		foreign.RestoredForeign{},
	)
	if got != nil {
		t.Fatalf("BuildRestoredWith error returned non-nil Backend %T", got)
	}
	var configErr *ConfigError
	if !errors.As(err, &configErr) || configErr.Field != "Config.Cwd" {
		t.Fatalf("error = %T %v, want ConfigError for Config.Cwd", err, err)
	}
}

func TestBuildRestoredWithRejectsMissingForeignSessionID(t *testing.T) {
	t.Parallel()
	got, err := BuildRestoredWith(restoredValidConfig(t))(
		context.Background(),
		mustID(t),
		mustID(t),
		loop.Provenance{},
		&fakePublisher{},
		validBoundDefinition(),
		seqIDGen(),
		workingFac(),
		foreign.RestoredForeign{},
	)
	if got != nil {
		t.Fatalf("BuildRestoredWith error returned non-nil Backend %T", got)
	}
	var configErr *ConfigError
	if !errors.As(err, &configErr) || configErr.Field != "RestoredForeign.ForeignSID" {
		t.Fatalf("error = %T %v, want ConfigError for missing foreign sid", err, err)
	}
}

func TestRestoreConstructionValidation(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name      string
		mutateCfg func(*Config)
		loopCfg   loop.BoundDefinition
		pub       foreign.EventPublisher
		idGen     func() (uuid.UUID, error)
		facNil    bool
		seed      foreign.RestoredForeign
		wantField string
	}{
		{name: "nil agent", mutateCfg: func(cfg *Config) { cfg.Agent = nil }, loopCfg: validBoundDefinition(), pub: &fakePublisher{}, idGen: seqIDGen(), seed: validRestoredSeed(), wantField: "Config.Agent"},
		{name: "empty workspace", mutateCfg: func(cfg *Config) { cfg.Cwd = "" }, loopCfg: validBoundDefinition(), pub: &fakePublisher{}, idGen: seqIDGen(), seed: validRestoredSeed(), wantField: "Config.Cwd"},
		{name: "empty system prompt", loopCfg: nil, pub: &fakePublisher{}, idGen: seqIDGen(), seed: validRestoredSeed(), wantField: "System"},
		{name: "nil publisher", loopCfg: validBoundDefinition(), idGen: seqIDGen(), seed: validRestoredSeed(), wantField: "pub"},
		{name: "nil id generator", loopCfg: validBoundDefinition(), pub: &fakePublisher{}, seed: validRestoredSeed(), wantField: "idGen"},
		{name: "nil factory", loopCfg: validBoundDefinition(), pub: &fakePublisher{}, idGen: seqIDGen(), facNil: true, seed: validRestoredSeed(), wantField: "fac"},
		{name: "empty foreign sid", loopCfg: validBoundDefinition(), pub: &fakePublisher{}, idGen: seqIDGen(), seed: foreign.RestoredForeign{TurnIndex: 1}, wantField: "RestoredForeign.ForeignSID"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			cfg := restoredValidConfig(t)
			if tt.mutateCfg != nil {
				tt.mutateCfg(&cfg)
			}
			fac := workingFac()
			if tt.facNil {
				fac = nil
			}
			got, err := newRestoredState(context.Background(), mustID(t), mustID(t), loop.Provenance{}, tt.pub, tt.loopCfg, cfg, tt.idGen, fac, tt.seed)
			if got != nil {
				t.Fatalf("newRestoredState error returned non-nil state %T", got)
			}
			var configErr *ConfigError
			if !errors.As(err, &configErr) {
				t.Fatalf("error = %T %v, want ConfigError", err, err)
			}
			if configErr.Field != tt.wantField {
				t.Fatalf("ConfigError.Field = %q, want %q", configErr.Field, tt.wantField)
			}
		})
	}
}

func TestBuildRestoredWithRejectsTypedNilPublisher(t *testing.T) {
	t.Parallel()

	var pub *typedNilPublisher
	got, err := BuildRestoredWith(restoredValidConfig(t))(
		context.Background(),
		mustID(t),
		mustID(t),
		loop.Provenance{},
		pub,
		validBoundDefinition(),
		seqIDGen(),
		workingFac(),
		validRestoredSeed(),
	)
	if got != nil {
		t.Fatalf("BuildRestoredWith typed-nil publisher returned non-nil Backend %T", got)
	}
	var configErr *ConfigError
	if !errors.As(err, &configErr) || configErr.Field != "pub" || configErr.Reason != "required" {
		t.Fatalf("BuildRestoredWith typed-nil publisher error = %T %v, want ConfigError pub required", err, err)
	}
}

func TestRestoreConstructionSeedsActorState(t *testing.T) {
	t.Parallel()
	seed := validRestoredSeed()
	sessionID := mustID(t)
	loopID := mustID(t)
	parent := loop.Provenance{LoopID: mustID(t), TurnID: mustID(t), StepID: mustID(t)}
	cfg := restoredValidConfig(t)
	bound := validBoundDefinition()
	pub := &fakePublisher{}
	idGen := seqIDGen()
	fac := workingFac()

	state, err := newRestoredState(context.Background(), sessionID, loopID, parent, pub, bound, cfg, idGen, fac, seed)
	if err != nil {
		t.Fatalf("newRestoredState: %v", err)
	}
	if state.sessionID != sessionID || state.loopID != loopID || state.parent != parent {
		t.Fatalf("identity = %v/%v/%v, want %v/%v/%v", state.sessionID, state.loopID, state.parent, sessionID, loopID, parent)
	}
	if state.sid != seed.ForeignSID || !state.sidBound || !state.hasSpawned {
		t.Fatalf("restore binding = sid %q sidBound %v hasSpawned %v", state.sid, state.sidBound, state.hasSpawned)
	}
	if state.turnIndex != seed.TurnIndex || len(state.msgs) != len(seed.Msgs) {
		t.Fatalf("restore state = turn %d messages %d, want %d/%d", state.turnIndex, len(state.msgs), seed.TurnIndex, len(seed.Msgs))
	}
	if state.Commands == nil || state.Done == nil || state.snapshots == nil {
		t.Fatal("restore construction left actor channels nil")
	}
	if state.cfg != bound || state.backendCfg != cfg || state.pub != pub || state.idGen == nil || state.fac != fac {
		t.Fatal("restore dependencies were not retained exactly")
	}
}

func TestRestoreConstructionClonesSnapshotSeed(t *testing.T) {
	t.Parallel()
	seed := validRestoredSeed()
	state, err := newRestoredState(context.Background(), mustID(t), mustID(t), loop.Provenance{}, &fakePublisher{}, validBoundDefinition(), restoredValidConfig(t), seqIDGen(), workingFac(), seed)
	if err != nil {
		t.Fatalf("newRestoredState: %v", err)
	}

	original := state.msgs[0]
	seed.Msgs[0] = aiMessage("mutated seed")
	if state.msgs[0] != original {
		t.Fatal("restored state aliases seed message slice")
	}

	clone := cloneMessages(state.msgs)
	clone[0] = aiMessage("mutated clone")
	if state.msgs[0] != original {
		t.Fatal("cloneMessages result aliases actor-owned slice")
	}
	if cloneMessages(nil) != nil {
		t.Fatal("cloneMessages(nil) must preserve nil")
	}
}

func TestRestoreConstructionPreservesTopLevelNilAndEmptyMessages(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		msgs    content.AgenticMessages
		wantNil bool
	}{
		{name: "nil", msgs: nil, wantNil: true},
		{name: "non-nil empty", msgs: make(content.AgenticMessages, 0)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			seed := validRestoredSeed()
			seed.Msgs = tt.msgs
			state, err := newRestoredState(context.Background(), mustID(t), mustID(t), loop.Provenance{}, &fakePublisher{}, validBoundDefinition(), restoredValidConfig(t), seqIDGen(), workingFac(), seed)
			if err != nil {
				t.Fatalf("newRestoredState: %v", err)
			}
			if gotNil := state.msgs == nil; gotNil != tt.wantNil {
				t.Fatalf("restored messages nil = %v, want %v", gotNil, tt.wantNil)
			}
			if len(state.msgs) != 0 {
				t.Fatalf("restored messages len = %d, want 0", len(state.msgs))
			}
		})
	}
}

func richMessages() content.AgenticMessages {
	return content.AgenticMessages{
		&content.UserMessage{Message: content.Message{
			Role: content.RoleUser,
			Blocks: []content.Block{
				&content.TextBlock{Text: "original-text"},
				&content.ImageBlock{MediaType: content.MediaTypeImagePNG, Source: content.ImageSource{URL: "https://example.test/image", Data: []byte{1, 2, 3}}},
				&content.AudioBlock{MediaType: content.MediaTypeAudioWAV, Data: []byte{4, 5, 6}},
				&content.DocumentBlock{MediaType: content.MediaTypeDocumentPDF, Name: "original.pdf", Data: []byte{7, 8, 9}, Text: "original-document"},
				&content.ThinkingBlock{Thinking: "original-thinking", Signature: "original-signature"},
				&content.ToolUseBlock{ID: "tool-1", Name: "original-tool", Input: json.RawMessage(`{"map":{"nested":"original"}}`)},
				&content.ToolResultBlock{ToolUseID: "tool-1", Content: []content.Block{
					&content.TextBlock{Text: "original-result"},
					&content.ImageBlock{Source: content.ImageSource{Data: []byte{10, 11}}},
					&content.ToolUseBlock{ID: "nested-tool", Input: json.RawMessage(`{"nested":true}`)},
				}, IsError: true},
			},
		}},
		&content.AIMessage{
			Message: content.Message{Role: content.RoleAssistant, Blocks: []content.Block{&content.TextBlock{Text: "original-ai"}}},
			Usage: &content.Usage{
				InputTokens:         11,
				OutputTokens:        7,
				CacheReadTokens:     3,
				CacheCreationTokens: 2,
				ReasoningTokens:     5,
			},
		},
		&content.SystemMessage{Message: content.Message{Role: content.RoleSystem, Blocks: []content.Block{&content.TextBlock{Text: "original-system"}}}},
		&content.ToolResultMessage{
			Message:   content.Message{Role: content.RoleTool, Blocks: []content.Block{&content.TextBlock{Text: "original-tool-message"}}},
			ToolUseID: "tool-message-1",
			IsError:   true,
		},
	}
}

func mutateRichMessages(messages content.AgenticMessages) {
	user := messages[0].(*content.UserMessage)
	user.Role = content.RoleSystem
	user.Blocks[0].(*content.TextBlock).Text = "mutated-text"
	user.Blocks[1].(*content.ImageBlock).Source.Data[0] = 99
	user.Blocks[2].(*content.AudioBlock).Data[0] = 99
	user.Blocks[3].(*content.DocumentBlock).Data[0] = 99
	user.Blocks[3].(*content.DocumentBlock).Text = "mutated-document"
	user.Blocks[4].(*content.ThinkingBlock).Thinking = "mutated-thinking"
	user.Blocks[5].(*content.ToolUseBlock).Input[0] = '['
	result := user.Blocks[6].(*content.ToolResultBlock)
	result.Content[0].(*content.TextBlock).Text = "mutated-result"
	result.Content[1].(*content.ImageBlock).Source.Data[0] = 99
	result.Content[2].(*content.ToolUseBlock).Input[0] = '['

	ai := messages[1].(*content.AIMessage)
	ai.Blocks[0].(*content.TextBlock).Text = "mutated-ai"
	ai.Usage.InputTokens = 999
	messages[2].(*content.SystemMessage).Blocks[0].(*content.TextBlock).Text = "mutated-system"
	toolMessage := messages[3].(*content.ToolResultMessage)
	toolMessage.Blocks[0].(*content.TextBlock).Text = "mutated-tool-message"
	toolMessage.ToolUseID = "mutated-tool-id"
}

func assertRichMessagesOriginal(t *testing.T, messages content.AgenticMessages) {
	t.Helper()
	if len(messages) != 4 {
		t.Fatalf("messages len = %d, want 4", len(messages))
	}
	user := messages[0].(*content.UserMessage)
	if user.Role != content.RoleUser || user.Blocks[0].(*content.TextBlock).Text != "original-text" {
		t.Fatalf("user message mutated: %#v", user)
	}
	if user.Blocks[1].(*content.ImageBlock).Source.Data[0] != 1 || user.Blocks[2].(*content.AudioBlock).Data[0] != 4 {
		t.Fatal("binary image/audio data mutated")
	}
	document := user.Blocks[3].(*content.DocumentBlock)
	if document.Data[0] != 7 || document.Text != "original-document" {
		t.Fatalf("document mutated: %#v", document)
	}
	if user.Blocks[4].(*content.ThinkingBlock).Thinking != "original-thinking" {
		t.Fatal("thinking block mutated")
	}
	if got := string(user.Blocks[5].(*content.ToolUseBlock).Input); got != `{"map":{"nested":"original"}}` {
		t.Fatalf("tool input mutated: %s", got)
	}
	result := user.Blocks[6].(*content.ToolResultBlock)
	if result.Content[0].(*content.TextBlock).Text != "original-result" || result.Content[1].(*content.ImageBlock).Source.Data[0] != 10 || string(result.Content[2].(*content.ToolUseBlock).Input) != `{"nested":true}` {
		t.Fatalf("nested tool result mutated: %#v", result)
	}
	ai := messages[1].(*content.AIMessage)
	if ai.Blocks[0].(*content.TextBlock).Text != "original-ai" || ai.Usage == nil || ai.Usage.InputTokens != 11 {
		t.Fatalf("AI message or usage mutated: %#v", ai)
	}
	if messages[2].(*content.SystemMessage).Blocks[0].(*content.TextBlock).Text != "original-system" {
		t.Fatal("system message mutated")
	}
	toolMessage := messages[3].(*content.ToolResultMessage)
	if toolMessage.Blocks[0].(*content.TextBlock).Text != "original-tool-message" || toolMessage.ToolUseID != "tool-message-1" {
		t.Fatalf("tool result message mutated: %#v", toolMessage)
	}
}

func TestRestoreConstructionDeepClonesSeed(t *testing.T) {
	t.Parallel()
	seed := foreign.RestoredForeign{ForeignSID: "deep-seed", TurnIndex: 9, Msgs: richMessages()}
	state, err := newRestoredState(context.Background(), mustID(t), mustID(t), loop.Provenance{}, &fakePublisher{}, validBoundDefinition(), restoredValidConfig(t), seqIDGen(), workingFac(), seed)
	if err != nil {
		t.Fatalf("newRestoredState: %v", err)
	}
	mutateRichMessages(seed.Msgs)
	assertRichMessagesOriginal(t, state.msgs)
}

func TestDeepClonePreservesNilAndEmptyShape(t *testing.T) {
	t.Parallel()
	emptyBlocks := []content.Block{}
	emptyRaw := json.RawMessage{}
	emptyBytes := []byte{}
	messages := content.AgenticMessages{
		nil,
		&content.UserMessage{Message: content.Message{Role: content.RoleUser, Blocks: emptyBlocks}},
		&content.AIMessage{Message: content.Message{Role: content.RoleAssistant, Blocks: []content.Block{
			&content.ToolUseBlock{Input: emptyRaw},
			&content.AudioBlock{Data: emptyBytes},
		}}},
	}
	cloned := cloneMessages(messages)
	if cloned[0] != nil {
		t.Fatalf("nil conversation cloned as %T", cloned[0])
	}
	if cloned[1].(*content.UserMessage).Blocks == nil {
		t.Fatal("non-nil empty block slice became nil")
	}
	blocks := cloned[2].(*content.AIMessage).Blocks
	if blocks[0].(*content.ToolUseBlock).Input == nil {
		t.Fatal("non-nil empty json.RawMessage became nil")
	}
	if blocks[1].(*content.AudioBlock).Data == nil {
		t.Fatal("non-nil empty byte slice became nil")
	}
}

func TestSnapshotDeepClonesActorStateAndOtherSnapshots(t *testing.T) {
	t.Parallel()
	seed := foreign.RestoredForeign{ForeignSID: "deep-snapshot", TurnIndex: 9, Msgs: richMessages()}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	built, err := BuildRestoredWith(restoredValidConfig(t))(ctx, mustID(t), mustID(t), loop.Provenance{}, &fakePublisher{}, validBoundDefinition(), seqIDGen(), workingFac(), seed)
	if err != nil {
		t.Fatalf("BuildRestoredWith: %v", err)
	}
	state := built.(*Loop)
	t.Cleanup(func() { shutdown(t, state) })

	first, _, err := state.Snapshot(context.Background())
	if err != nil {
		t.Fatalf("first Snapshot: %v", err)
	}
	mutateRichMessages(first)
	second, _, err := state.Snapshot(context.Background())
	if err != nil {
		t.Fatalf("second Snapshot: %v", err)
	}
	assertRichMessagesOriginal(t, second)
	assertRichMessagesOriginal(t, state.msgs)
}

func TestSnapshotPreservesTopLevelNilAndEmptyMessages(t *testing.T) {
	tests := []struct {
		name    string
		msgs    content.AgenticMessages
		wantNil bool
	}{
		{name: "nil", msgs: nil, wantNil: true},
		{name: "non-nil empty", msgs: make(content.AgenticMessages, 0)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			seed := validRestoredSeed()
			seed.Msgs = tt.msgs
			ctx, cancel := context.WithCancel(context.Background())
			t.Cleanup(cancel)
			built, err := BuildRestoredWith(restoredValidConfig(t))(ctx, mustID(t), mustID(t), loop.Provenance{}, &fakePublisher{}, validBoundDefinition(), seqIDGen(), workingFac(), seed)
			if err != nil {
				t.Fatalf("BuildRestoredWith: %v", err)
			}
			state := built.(*Loop)
			t.Cleanup(func() { shutdown(t, state) })

			first, _, err := state.Snapshot(context.Background())
			if err != nil {
				t.Fatalf("first Snapshot: %v", err)
			}
			if gotNil := first == nil; gotNil != tt.wantNil {
				t.Fatalf("first snapshot nil = %v, want %v", gotNil, tt.wantNil)
			}
			first = append(first, aiMessage("caller mutation"))
			if len(first) != 1 || firstText(t, first[0]) != "caller mutation" {
				t.Fatalf("caller append = %#v, want one caller-owned message", first)
			}

			second, _, err := state.Snapshot(context.Background())
			if err != nil {
				t.Fatalf("second Snapshot: %v", err)
			}
			if gotNil := second == nil; gotNil != tt.wantNil {
				t.Fatalf("second snapshot nil = %v, want %v", gotNil, tt.wantNil)
			}
			if len(second) != 0 || len(state.msgs) != 0 {
				t.Fatalf("caller append mutated snapshot/actor lengths to %d/%d", len(second), len(state.msgs))
			}
		})
	}
}

func TestSnapshotMutationDoesNotRaceActorState(t *testing.T) {
	seed := foreign.RestoredForeign{ForeignSID: "deep-race", TurnIndex: 9, Msgs: richMessages()}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	built, err := BuildRestoredWith(restoredValidConfig(t))(ctx, mustID(t), mustID(t), loop.Provenance{}, &fakePublisher{}, validBoundDefinition(), seqIDGen(), workingFac(), seed)
	if err != nil {
		t.Fatalf("BuildRestoredWith: %v", err)
	}
	state := built.(*Loop)
	t.Cleanup(func() { shutdown(t, state) })

	first, _, err := state.Snapshot(context.Background())
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	start := make(chan struct{})
	var mutator sync.WaitGroup
	mutator.Add(1)
	go func() {
		defer mutator.Done()
		<-start
		for range 200 {
			mutateRichMessages(first)
		}
	}()
	close(start)
	for range 50 {
		snapshot, _, err := state.Snapshot(context.Background())
		if err != nil {
			t.Fatalf("concurrent Snapshot: %v", err)
		}
		assertRichMessagesOriginal(t, snapshot)
	}
	mutator.Wait()
}

func TestUnstartedRestoredStateSnapshotFailsOnCallerContext(t *testing.T) {
	t.Parallel()
	state, err := newRestoredState(context.Background(), mustID(t), mustID(t), loop.Provenance{}, &fakePublisher{}, validBoundDefinition(), restoredValidConfig(t), seqIDGen(), workingFac(), validRestoredSeed())
	if err != nil {
		t.Fatalf("newRestoredState: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	msgs, turnIndex, err := state.Snapshot(ctx)
	if msgs != nil || turnIndex != 0 {
		t.Fatalf("Snapshot context error returned partial state: %v/%d", msgs, turnIndex)
	}
	var snapshotErr *SnapshotError
	if !errors.As(err, &snapshotErr) || snapshotErr.Reason != SnapshotContextDone || !errors.Is(err, context.Canceled) {
		t.Fatalf("Snapshot error = %T %v, want context-done SnapshotError", err, err)
	}
}

func TestBuildRestoredWithStartsRestoredActor(t *testing.T) {
	t.Parallel()
	build := BuildRestoredWith(restoredValidConfig(t))
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	got, err := build(ctx, mustID(t), mustID(t), loop.Provenance{}, &fakePublisher{}, validBoundDefinition(), seqIDGen(), workingFac(), validRestoredSeed())
	if err != nil || got == nil {
		t.Fatalf("BuildRestoredWith = %T %v, want backend", got, err)
	}
	state := got.(*Loop)
	msgs, turnIndex, err := state.Snapshot(context.Background())
	if err != nil || turnIndex != validRestoredSeed().TurnIndex || len(msgs) != len(validRestoredSeed().Msgs) {
		t.Fatalf("restored Snapshot = %v/%d/%v", msgs, turnIndex, err)
	}
	shutdown(t, state)
}

func TestRestoredActorResumesSIDAndAppendsAfterSeed(t *testing.T) {
	t.Parallel()
	agent := &fakeAgent{
		history: driver.History{Available: true, Steps: []content.AgenticMessages{{aiMessage("restored reply")}}},
		events:  []driver.Event{{Kind: driver.KindTerminalOK, Message: aiMessage("restored reply")}},
	}
	cfg := restoredValidConfig(t)
	cfg.Agent = agent
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	built, err := BuildRestoredWith(cfg)(ctx, mustID(t), mustID(t), loop.Provenance{}, &fakePublisher{}, validBoundDefinition(), seqIDGen(), workingFac(), validRestoredSeed())
	if err != nil {
		t.Fatalf("BuildRestoredWith: %v", err)
	}
	state := built.(*Loop)
	submit(t, state, "resume")
	waitTurnIndex(t, state, validRestoredSeed().TurnIndex+1)
	turn := agent.lastForeignTurn()
	if turn.StartNew || turn.ForeignSID != validRestoredSeed().ForeignSID {
		t.Fatalf("restored turn = {StartNew:%v ForeignSID:%q}", turn.StartNew, turn.ForeignSID)
	}
	msgs, _, err := state.Snapshot(context.Background())
	if err != nil || len(msgs) != len(validRestoredSeed().Msgs)+1 || firstText(t, msgs[len(msgs)-1]) != "restored reply" {
		t.Fatalf("restored snapshot = %v err %v", msgs, err)
	}
	shutdown(t, state)
}
