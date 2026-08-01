package acp

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/looprig/acp/launch"
	"github.com/looprig/core/content"
	"github.com/looprig/core/uuid"
	"github.com/looprig/foreignloops/backend"
	"github.com/looprig/foreignloops/driver"
	"github.com/looprig/harness/pkg/command"
	"github.com/looprig/harness/pkg/event"
	"github.com/looprig/harness/pkg/foreign"
	"github.com/looprig/harness/pkg/loop"
	"github.com/looprig/harness/pkg/tool"
	"github.com/looprig/inference"
	model "github.com/looprig/inference/model"
	inferenceStream "github.com/looprig/inference/stream"
)

func TestBuildWithReturnsForeignBuilder(t *testing.T) {
	t.Parallel()
	var _ foreign.Builder = BuildWith(Config{})
}

func TestBuildRestoredWithReturnsForeignBuilder(t *testing.T) {
	t.Parallel()
	var _ foreign.RestoredBuilder = BuildRestoredWith(Config{})
}

func TestBuildWithLiveLoopBindsACPSessionID(t *testing.T) {
	workspace := t.TempDir()
	cfg := validConfig(HarnessCodex)
	cfg.AgentSessionID = ""
	cfg.WorkspaceRoot = workspace
	sess := newScriptedSession("live-agent-session")
	conn := &fakeClient{newSession: sess}
	owned := &fakeDialedClient{acpClient: conn}
	installDial(t, func(context.Context, launch.Config) (dialedClient, error) {
		return owned, nil
	})

	pub := &builderPublisher{}
	loopCtx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	state, sid, err := BuildWith(cfg)(
		loopCtx,
		randomBuilderID(t),
		randomBuilderID(t),
		loop.Provenance{},
		pub,
		builderBoundDefinition(t),
		builderIDGen,
		event.NewFactory(builderIDGen, time.Now),
	)
	if err != nil {
		t.Fatalf("BuildWith() error = %v", err)
	}
	if sid != "live-agent-session" {
		t.Fatalf("BuildWith() sid = %q, want ACP session id", sid)
	}
	builtState, ok := state.(*backend.Loop)
	if !ok {
		t.Fatalf("BuildWith() backend = %T, want *backend.Loop", state)
	}

	builtState.CommandSink() <- command.UserInput{
		Header: command.Header{CommandID: randomBuilderID(t)},
		Blocks: []content.Block{&content.TextBlock{Text: "first turn"}},
	}
	waitBuilderEvent(t, pub, func(input event.Event) bool {
		bound, ok := input.(event.ForeignSessionBound)
		return ok && bound.ForeignSID == "live-agent-session"
	})
	waitBuilderEvent(t, pub, func(input event.Event) bool {
		_, ok := input.(event.TurnDone)
		return ok
	})
	shutdownBuilderLoop(t, builtState)
	if owned.closeCalls != 1 {
		t.Fatalf("owned ACP client close calls = %d, want 1", owned.closeCalls)
	}
}

func TestBuildRestoredLoadsSeedAgentSessionAndPreservesState(t *testing.T) {
	workspace := t.TempDir()
	cfg := validConfig(HarnessCodex)
	cfg.AgentSessionID = "should-be-replaced"
	cfg.WorkspaceRoot = workspace
	sess := newScriptedSession("restored-agent-session")
	conn := &fakeClient{loadSession: sess}
	owned := &fakeDialedClient{acpClient: conn}
	installDial(t, func(context.Context, launch.Config) (dialedClient, error) {
		return owned, nil
	})

	seed := foreign.RestoredForeign{
		ForeignSID: "journaled-agent-session",
		TurnIndex:  4,
		Msgs: content.AgenticMessages{
			&content.AIMessage{Message: content.Message{
				Role:   content.RoleAssistant,
				Blocks: []content.Block{&content.TextBlock{Text: "prior answer"}},
			}},
		},
	}
	loopCtx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	built, err := BuildRestoredWith(cfg)(
		loopCtx,
		randomBuilderID(t),
		randomBuilderID(t),
		loop.Provenance{},
		&builderPublisher{},
		builderBoundDefinition(t),
		builderIDGen,
		event.NewFactory(builderIDGen, time.Now),
		seed,
	)
	if err != nil {
		t.Fatalf("BuildRestoredWith() error = %v", err)
	}
	state, ok := built.(*backend.Loop)
	if !ok {
		t.Fatalf("BuildRestoredWith() backend = %T, want *backend.Loop", built)
	}
	if conn.newCalls != 0 || conn.loadCalls != 1 {
		t.Fatalf("session calls = new:%d load:%d, want new:0 load:1", conn.newCalls, conn.loadCalls)
	}
	if got := conn.loadParams[0].SessionID; got != "journaled-agent-session" {
		t.Fatalf("LoadSession SessionID = %q, want journal seed", got)
	}
	msgs, turnIndex, err := state.Snapshot(context.Background())
	if err != nil {
		t.Fatalf("restored Snapshot() error = %v", err)
	}
	if turnIndex != seed.TurnIndex || len(msgs) != len(seed.Msgs) || msgs[0].(*content.AIMessage).Blocks[0].(*content.TextBlock).Text != "prior answer" {
		t.Fatalf("restored snapshot = turn %d msgs %#v, want seed state", turnIndex, msgs)
	}
	shutdownBuilderLoop(t, state)
	if owned.closeCalls != 1 {
		t.Fatalf("owned ACP client close calls = %d, want 1", owned.closeCalls)
	}
}

func TestBuildClosesDriverWhenBackendConstructionFails(t *testing.T) {
	cfg := validConfig(HarnessCodex)
	cfg.AgentSessionID = ""
	cfg.WorkspaceRoot = t.TempDir()
	sess := newScriptedSession("cleanup-session")
	owned := &fakeDialedClient{acpClient: &fakeClient{newSession: sess}}
	installDial(t, func(context.Context, launch.Config) (dialedClient, error) {
		return owned, nil
	})

	built, sid, err := BuildWith(cfg)(
		context.Background(),
		randomBuilderID(t),
		randomBuilderID(t),
		loop.Provenance{},
		&builderPublisher{},
		nil,
		builderIDGen,
		event.NewFactory(builderIDGen, time.Now),
	)
	if built != nil || sid != "" {
		t.Fatalf("failed BuildWith() = %T %q, want nil backend and empty sid", built, sid)
	}
	if err == nil {
		t.Fatal("BuildWith() error = nil, want backend construction error")
	}
	if owned.closeCalls != 1 {
		t.Fatalf("owned ACP client close calls = %d, want cleanup close", owned.closeCalls)
	}
}

func TestBuildRestoredClosesDriverWhenBackendConstructionFails(t *testing.T) {
	cfg := validConfig(HarnessCodex)
	cfg.AgentSessionID = "caller-value-must-not-win"
	cfg.WorkspaceRoot = t.TempDir()
	sess := newScriptedSession("restored-cleanup-session")
	owned := &fakeDialedClient{acpClient: &fakeClient{loadSession: sess}}
	installDial(t, func(context.Context, launch.Config) (dialedClient, error) {
		return owned, nil
	})

	built, err := BuildRestoredWith(cfg)(
		context.Background(),
		randomBuilderID(t),
		randomBuilderID(t),
		loop.Provenance{},
		&builderPublisher{},
		nil,
		builderIDGen,
		event.NewFactory(builderIDGen, time.Now),
		foreign.RestoredForeign{ForeignSID: "journaled-cleanup-session"},
	)
	if built != nil {
		t.Fatalf("failed BuildRestoredWith() = %T, want nil backend", built)
	}
	if err == nil {
		t.Fatal("BuildRestoredWith() error = nil, want backend construction error")
	}
	if owned.closeCalls != 1 {
		t.Fatalf("owned ACP client close calls = %d, want cleanup close", owned.closeCalls)
	}
}

func TestBuilderMapsNeutralPostureAtBackendBoundary(t *testing.T) {
	t.Parallel()
	if got := legacyPosture(driver.PostureReadOnly); got != driver.PostureDefault {
		t.Fatalf("read-only posture = %v, want default legacy posture", got)
	}
	if got := legacyPosture(driver.PostureWorkspaceWrite); got != driver.PostureAcceptEdits {
		t.Fatalf("workspace-write posture = %v, want accept-edits legacy posture", got)
	}
}

type builderPublisher struct {
	mu     sync.Mutex
	events []event.Event
}

func (p *builderPublisher) PublishEvent(_ context.Context, input event.Event) error {
	p.mu.Lock()
	p.events = append(p.events, input)
	p.mu.Unlock()
	return nil
}

func (p *builderPublisher) PublishEventChecked(ctx context.Context, input event.Event) error {
	return p.PublishEvent(ctx, input)
}

func (p *builderPublisher) has(want func(event.Event) bool) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, input := range p.events {
		if want(input) {
			return true
		}
	}
	return false
}

func waitBuilderEvent(t *testing.T, pub *builderPublisher, want func(event.Event) bool) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if pub.has(want) {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("timed out waiting for builder event")
}

func shutdownBuilderLoop(t *testing.T, state *backend.Loop) {
	t.Helper()
	ack := make(chan error, 1)
	state.CommandSink() <- command.Shutdown{Header: command.Header{CommandID: randomBuilderID(t)}, Ack: ack}
	if err := <-ack; err != nil {
		t.Fatalf("loop shutdown error = %v", err)
	}
	select {
	case <-state.DoneChan():
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for loop shutdown")
	}
}

func randomBuilderID(t *testing.T) uuid.UUID {
	t.Helper()
	id, err := uuid.New()
	if err != nil {
		t.Fatalf("uuid.New() error = %v", err)
	}
	return id
}

func builderIDGen() (uuid.UUID, error) { return uuid.New() }

type builderInferenceClient struct{}

func (builderInferenceClient) Invoke(context.Context, inference.Request) (*inference.Response, error) {
	return nil, errors.New("unused")
}

func (builderInferenceClient) Stream(context.Context, inference.Request) (*inferenceStream.StreamReader[content.Chunk], error) {
	return nil, errors.New("unused")
}

func builderBoundDefinition(t *testing.T) loop.BoundDefinition {
	t.Helper()
	d, err := loop.Define(
		loop.WithName("acp-builder"),
		loop.WithInference(builderInferenceClient{}, model.Model{
			Provider:  "test",
			APIFormat: model.APIFormatOpenAI,
			BaseURL:   "http://127.0.0.1:1",
			Name:      "test-model",
		}),
		loop.WithSystem("builder system"),
	)
	if err != nil {
		t.Fatalf("loop.Define() error = %v", err)
	}
	sessionID := randomBuilderID(t)
	loopID := randomBuilderID(t)
	bound, err := d.Bind(context.Background(), tool.Bindings{SessionID: sessionID, LoopID: loopID})
	if err != nil {
		t.Fatalf("definition.Bind() error = %v", err)
	}
	return bound
}
