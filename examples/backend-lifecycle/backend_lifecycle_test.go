package backendlifecycle_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

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
	"github.com/looprig/inference/model"
	inferencestream "github.com/looprig/inference/stream"
)

func TestExampleBackendLifecycleAndRestore(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	ids := sequentialIDs()
	publisher := &recordingPublisher{}
	agent := &scriptedAgent{}
	cfg := backend.Config{
		Agent:   agent,
		Cwd:     t.TempDir(),
		Posture: driver.PostureDefault,
		SIDMode: backend.SIDPrebound,
	}
	state, foreignSID, err := backend.New(
		ctx,
		mustID("00000000-0000-4000-8000-000000000001"),
		mustID("00000000-0000-4000-8000-000000000002"),
		loop.Provenance{},
		publisher,
		boundLoop(t),
		cfg,
		ids,
		event.NewFactory(ids, func() time.Time { return time.Unix(1, 0).UTC() }),
	)
	if err != nil {
		t.Fatalf("build live backend: %v", err)
	}
	if foreignSID == "" {
		t.Fatal("live backend did not mint its foreign session id")
	}

	accepted := make(chan error, 1)
	state.CommandSink() <- command.UserInput{
		Header:   command.Header{CommandID: mustID("00000000-0000-4000-8000-000000000003")},
		Blocks:   []content.Block{&content.TextBlock{Text: "explain the change"}},
		Accepted: accepted,
	}
	if err := <-accepted; err != nil {
		t.Fatalf("submit input: %v", err)
	}
	waitForEvent[event.TurnDone](t, ctx, publisher)
	// TurnDone is published from the turn goroutine before the actor folds the
	// outcome into l.msgs/l.turnIndex (backend.applyOutcome runs only once the
	// turn returns). Observing the event therefore proves the provider turn
	// finished, not that the committed transcript is readable yet, so wait for
	// the actor-visible fold before snapshotting -- the same readiness the
	// restored path below already uses.
	waitForTurn(t, ctx, state, 1)

	messages, turnIndex, err := state.Snapshot(ctx)
	if err != nil {
		t.Fatalf("snapshot live backend: %v", err)
	}
	if turnIndex != 1 || messageText(messages[0]) != "scripted response" {
		t.Fatalf("live snapshot = turn %d messages %#v", turnIndex, messages)
	}
	turn := agent.lastTurn()
	if !turn.StartNew || turn.ForeignSID != foreignSID || turn.Cwd != cfg.Cwd {
		t.Fatalf("first provider turn = %#v", turn)
	}
	shutdown(t, ctx, state)

	// A restore seed comes from the durable event fold. It preserves both the
	// provider session identity and the committed conversation, then resumes
	// rather than creating a second provider-side session.
	restoredAgent := &scriptedAgent{}
	cfg.Agent = restoredAgent
	restored, err := backend.BuildRestoredWith(cfg)(
		ctx,
		mustID("00000000-0000-4000-8000-000000000004"),
		mustID("00000000-0000-4000-8000-000000000005"),
		loop.Provenance{},
		&recordingPublisher{},
		boundLoop(t),
		ids,
		event.NewFactory(ids, func() time.Time { return time.Unix(2, 0).UTC() }),
		foreign.RestoredForeign{ForeignSID: foreignSID, TurnIndex: turnIndex, Msgs: messages},
	)
	if err != nil {
		t.Fatalf("restore backend: %v", err)
	}
	restoredState := restored.(*backend.Loop)
	restoredState.CommandSink() <- command.UserInput{
		Header: command.Header{CommandID: mustID("00000000-0000-4000-8000-000000000006")},
		Blocks: []content.Block{&content.TextBlock{Text: "continue"}},
	}
	waitForTurn(t, ctx, restoredState, 2)
	resumedTurn := restoredAgent.lastTurn()
	if resumedTurn.StartNew || resumedTurn.ForeignSID != foreignSID {
		t.Fatalf("restored provider turn = %#v", resumedTurn)
	}
	shutdown(t, ctx, restoredState)

	// Construction failures are typed, so composition roots can report the
	// faulty field without matching human-readable text.
	bad := cfg
	bad.Cwd = ""
	_, _, err = backend.New(ctx, uuid.UUID{}, uuid.UUID{}, loop.Provenance{}, publisher, boundLoop(t), bad, ids, event.NewFactory(ids, time.Now))
	var configErr *backend.ConfigError
	if !errors.As(err, &configErr) || configErr.Field != "Config.Cwd" {
		t.Fatalf("invalid config error = %T %v", err, err)
	}
}

type scriptedAgent struct {
	mu   sync.Mutex
	turn driver.Turn
}

func (a *scriptedAgent) Spawn(ctx context.Context, turn driver.Turn) (driver.Stream, error) {
	a.mu.Lock()
	a.turn = turn
	a.mu.Unlock()
	message := &content.AIMessage{Message: content.Message{
		Role:   content.RoleAssistant,
		Blocks: []content.Block{&content.TextBlock{Text: "scripted response"}},
	}}
	return &scriptedStream{
		ctx:     ctx,
		events:  []driver.Event{{Kind: driver.KindStepComplete, Message: message}, {Kind: driver.KindTerminalOK}},
		history: driver.History{Available: true, Steps: []content.AgenticMessages{{message}}},
	}, nil
}

func (a *scriptedAgent) lastTurn() driver.Turn {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.turn
}

type scriptedStream struct {
	ctx     context.Context
	events  []driver.Event
	history driver.History
}

func (s *scriptedStream) Events() <-chan driver.Event {
	out := make(chan driver.Event)
	go func() {
		defer close(out)
		for _, item := range s.events {
			select {
			case out <- item:
			case <-s.ctx.Done():
				return
			}
		}
	}()
	return out
}

func (s *scriptedStream) History() (driver.History, error) { return s.history, nil }
func (s *scriptedStream) Close() error                     { return nil }

type recordingPublisher struct {
	mu      sync.Mutex
	events  []event.Event
	changed chan struct{}
}

func (p *recordingPublisher) PublishEvent(_ context.Context, item event.Event) error {
	p.mu.Lock()
	if p.changed == nil {
		p.changed = make(chan struct{}, 1)
	}
	p.events = append(p.events, item)
	select {
	case p.changed <- struct{}{}:
	default:
	}
	p.mu.Unlock()
	return nil
}

func (p *recordingPublisher) PublishEventChecked(ctx context.Context, item event.Event) error {
	return p.PublishEvent(ctx, item)
}

func waitForEvent[T event.Event](t *testing.T, ctx context.Context, publisher *recordingPublisher) T {
	t.Helper()
	for {
		publisher.mu.Lock()
		for _, item := range publisher.events {
			if typed, ok := item.(T); ok {
				publisher.mu.Unlock()
				return typed
			}
		}
		if publisher.changed == nil {
			publisher.changed = make(chan struct{}, 1)
		}
		changed := publisher.changed
		publisher.mu.Unlock()
		select {
		case <-changed:
		case <-ctx.Done():
			t.Fatalf("wait for event: %v", ctx.Err())
		}
	}
}

func waitForTurn(t *testing.T, ctx context.Context, state *backend.Loop, want event.TurnIndex) {
	t.Helper()
	for {
		_, got, err := state.Snapshot(ctx)
		if err != nil {
			t.Fatalf("snapshot restored backend: %v", err)
		}
		if got == want {
			return
		}
		select {
		case <-time.After(time.Millisecond):
		case <-ctx.Done():
			t.Fatalf("wait for restored turn: %v", ctx.Err())
		}
	}
}

func shutdown(t *testing.T, ctx context.Context, state *backend.Loop) {
	t.Helper()
	ack := make(chan error, 1)
	state.CommandSink() <- command.Shutdown{Ack: ack}
	select {
	case err := <-ack:
		if err != nil {
			t.Fatalf("shutdown: %v", err)
		}
	case <-ctx.Done():
		t.Fatalf("shutdown: %v", ctx.Err())
	}
}

func messageText(message content.Conversation) string {
	return message.(*content.AIMessage).Blocks[0].(*content.TextBlock).Text
}

func sequentialIDs() func() (uuid.UUID, error) {
	var mu sync.Mutex
	var n byte = 10
	return func() (uuid.UUID, error) {
		mu.Lock()
		defer mu.Unlock()
		n++
		var id uuid.UUID
		id[6], id[8], id[15] = 0x40, 0x80, n
		return id, nil
	}
}

func mustID(value string) uuid.UUID { return uuid.MustParse(value) }

type unusedInferenceClient struct{}

func (unusedInferenceClient) Invoke(context.Context, inference.Request) (*inference.Response, error) {
	return nil, errors.New("unused by foreign backend")
}

func (unusedInferenceClient) Stream(context.Context, inference.Request) (*inferencestream.StreamReader[content.Chunk], error) {
	return nil, errors.New("unused by foreign backend")
}

func boundLoop(t *testing.T) loop.BoundDefinition {
	t.Helper()
	definition, err := loop.Define(
		loop.WithName("docs-foreign-agent"),
		loop.WithInference(unusedInferenceClient{}, model.Model{
			Provider:  "lmstudio",
			APIFormat: model.APIFormatOpenAI,
			BaseURL:   "http://127.0.0.1:1234",
			Name:      "unused",
		}),
		loop.WithSystem("Be concise."),
	)
	if err != nil {
		t.Fatalf("define loop: %v", err)
	}
	bound, err := definition.Bind(context.Background(), tool.Bindings{
		SessionID: mustID("00000000-0000-4000-8000-000000000007"),
		LoopID:    mustID("00000000-0000-4000-8000-000000000008"),
	})
	if err != nil {
		t.Fatalf("bind loop: %v", err)
	}
	return bound
}
