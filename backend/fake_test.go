package backend

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/looprig/core/content"
	"github.com/looprig/core/uuid"
	"github.com/looprig/foreignloop/driver"
	"github.com/looprig/harness/pkg/event"
	"github.com/looprig/harness/pkg/loop"
	"github.com/looprig/harness/pkg/tool"
	"github.com/looprig/inference"
	model "github.com/looprig/inference/model"
	stream "github.com/looprig/inference/stream"
)

func seqIDGen() func() (uuid.UUID, error) {
	var n byte
	return func() (uuid.UUID, error) {
		n++
		var id uuid.UUID
		id[15] = n
		return id, nil
	}
}

func mustID(t interface {
	Helper()
	Fatal(args ...any)
}) uuid.UUID {
	t.Helper()
	id, err := uuid.New()
	if err != nil {
		t.Fatal(err)
	}
	return id
}

func workingFac() *event.Factory {
	return event.NewFactory(uuid.New, time.Now)
}

func aiMessage(text string) *content.AIMessage {
	return &content.AIMessage{Message: content.Message{
		Role:   content.RoleAssistant,
		Blocks: []content.Block{&content.TextBlock{Text: text}},
	}}
}

type fakePublisher struct {
	mu     sync.Mutex
	events []event.Event
}

func (p *fakePublisher) PublishEvent(_ context.Context, ev event.Event) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.events = append(p.events, ev)
	return nil
}

func (p *fakePublisher) PublishEventChecked(ctx context.Context, ev event.Event) error {
	return p.PublishEvent(ctx, ev)
}

type fakeAgent struct{}

func (*fakeAgent) Spawn(context.Context, driver.Turn) (driver.Stream, error) {
	panic("fakeAgent.Spawn must not be called before Task 13 moves the actor")
}

type boundTestClient struct{}

func (boundTestClient) Invoke(context.Context, inference.Request) (*inference.Response, error) {
	return nil, errors.New("unused")
}

func (boundTestClient) Stream(context.Context, inference.Request) (*stream.StreamReader[content.Chunk], error) {
	return nil, errors.New("unused")
}

func validBoundDefinition() loop.BoundDefinition {
	definition, err := loop.Define(
		loop.WithName("agent"),
		loop.WithInference(boundTestClient{}, model.Model{
			Provider:  "lmstudio",
			APIFormat: model.APIFormatOpenAI,
			BaseURL:   "http://localhost:1234",
			Name:      "m",
		}),
		loop.WithSystem("system prompt"),
	)
	if err != nil {
		panic(err)
	}
	bound, err := definition.Bind(context.Background(), tool.Bindings{
		SessionID: mustID(panicT{}),
		LoopID:    mustID(panicT{}),
	})
	if err != nil {
		panic(err)
	}
	return bound
}

type panicT struct{}

func (panicT) Helper()           {}
func (panicT) Fatal(args ...any) { panic(args) }
