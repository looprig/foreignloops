package backend

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/looprig/core/content"
	"github.com/looprig/core/uuid"
	"github.com/looprig/foreignloops/driver"
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
	mu            sync.Mutex
	events        []event.Event
	checkedEvents []event.Event
	checkedErr    error
}

func (p *fakePublisher) PublishEvent(_ context.Context, ev event.Event) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.events = append(p.events, ev)
	return nil
}

func (p *fakePublisher) PublishEventChecked(ctx context.Context, ev event.Event) error {
	p.mu.Lock()
	err := p.checkedErr
	if err == nil {
		p.checkedEvents = append(p.checkedEvents, ev)
	}
	p.mu.Unlock()
	if err != nil {
		return err
	}
	return p.PublishEvent(ctx, ev)
}

func (p *fakePublisher) snapshot() []event.Event {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]event.Event(nil), p.events...)
}

func (p *fakePublisher) count() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.events)
}

func (p *fakePublisher) checkedSnapshot() []event.Event {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]event.Event(nil), p.checkedEvents...)
}

type fakeStream struct {
	events     []driver.Event
	history    driver.History
	historyErr error
	closeErr   error
	block      bool
	ctx        context.Context

	mu      sync.Mutex
	order   []string
	ch      chan driver.Event
	stop    chan struct{}
	start   sync.Once
	closeCh sync.Once
}

func (s *fakeStream) Events() <-chan driver.Event {
	s.start.Do(func() {
		s.ch = make(chan driver.Event)
		go func() {
			defer close(s.ch)
			for _, input := range s.events {
				select {
				case s.ch <- input:
				case <-s.stop:
					return
				case <-s.ctx.Done():
					return
				}
			}
			if s.block {
				select {
				case <-s.stop:
				case <-s.ctx.Done():
				}
			}
		}()
	})
	return s.ch
}

func (s *fakeStream) History() (driver.History, error) {
	s.mu.Lock()
	s.order = append(s.order, "history")
	s.mu.Unlock()
	return s.history, s.historyErr
}

func (s *fakeStream) Close() error {
	s.mu.Lock()
	s.order = append(s.order, "close")
	s.mu.Unlock()
	s.closeCh.Do(func() { close(s.stop) })
	return s.closeErr
}

func (s *fakeStream) lifecycle() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.order...)
}

type fakeAgent struct {
	mu sync.Mutex

	spawnErr   error
	events     []driver.Event
	history    driver.History
	historyErr error
	closeErr   error
	block      bool
	onSpawn    func()

	spawnCalls int
	lastTurn   driver.Turn
	streams    []*fakeStream
}

func (a *fakeAgent) Spawn(ctx context.Context, turn driver.Turn) (driver.Stream, error) {
	a.mu.Lock()
	a.spawnCalls++
	a.lastTurn = turn
	callback := a.onSpawn
	err := a.spawnErr
	if err != nil {
		a.mu.Unlock()
		if callback != nil {
			callback()
		}
		return nil, err
	}
	stream := &fakeStream{
		events:     append([]driver.Event(nil), a.events...),
		history:    a.history,
		historyErr: a.historyErr,
		closeErr:   a.closeErr,
		block:      a.block,
		ctx:        ctx,
		stop:       make(chan struct{}),
	}
	a.streams = append(a.streams, stream)
	a.mu.Unlock()
	if callback != nil {
		callback()
	}
	return stream, nil
}

func (a *fakeAgent) calls() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.spawnCalls
}

func (a *fakeAgent) lastForeignTurn() driver.Turn {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.lastTurn
}

func (a *fakeAgent) lastStream() *fakeStream {
	a.mu.Lock()
	defer a.mu.Unlock()
	if len(a.streams) == 0 {
		return nil
	}
	return a.streams[len(a.streams)-1]
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
