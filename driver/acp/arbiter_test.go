package acp

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/looprig/acp/client"
	"github.com/looprig/acp/protocol"
	"github.com/looprig/core/content"
	"github.com/looprig/foreignloops/driver"
)

func TestProjectionOwnerSelectionAndStaleSend(t *testing.T) {
	p := newProjection()
	events := p.eventsView()
	observations := p.observationsView()
	select {
	case _, ok := <-observations:
		if ok {
			t.Fatal("inactive observation projection carried traffic")
		}
	case <-time.After(time.Second):
		t.Fatal("inactive observation projection did not close")
	}
	p.emitEvent(driver.Event{Kind: driver.KindTextDelta, Text: "owned"})
	select {
	case event := <-events:
		if event.Text != "owned" {
			t.Fatalf("event = %#v, want owner-delivered event", event)
		}
	case <-time.After(time.Second):
		t.Fatal("projection owner did not deliver event")
	}
	p.close()
	select {
	case _, ok := <-events:
		if ok {
			t.Fatal("event projection remained open after owner close")
		}
	case <-time.After(time.Second):
		t.Fatal("event projection did not close")
	}
	finished := make(chan struct{})
	go func() {
		p.send(projectionCommand{event: &driver.Event{Kind: driver.KindTextDelta}})
		close(finished)
	}()
	select {
	case <-finished:
	case <-time.After(time.Second):
		t.Fatal("stale projection command blocked after close")
	}
}

func TestProjectionBackpressuresStalledConsumerAndCloseUnblocks(t *testing.T) {
	p := newProjection()
	observations := p.observationsView()

	// The selected output buffer, the owner's pending queue, and the command
	// mailbox are the complete bounded in-flight budget. One more producer
	// must remain blocked while the consumer is deliberately absent.
	budget := cap(observations)*2 + cap(p.commands)
	accepted := make(chan struct{}, budget+1)
	producerDone := make(chan struct{})
	go func() {
		defer close(producerDone)
		for i := 0; i < budget+1; i++ {
			p.emitObservation(driver.SteerObservation{})
			accepted <- struct{}{}
		}
	}()

	for i := 0; i < budget; i++ {
		select {
		case <-accepted:
		case <-time.After(2 * time.Second):
			t.Fatalf("only %d/%d projection producers reached the bounded budget", i, budget)
		}
	}
	select {
	case <-accepted:
		t.Fatal("projection accepted an emission beyond its bounded pending/mailbox budget")
	case <-time.After(100 * time.Millisecond):
	}

	closeDone := make(chan struct{})
	go func() {
		p.close()
		close(closeDone)
	}()
	select {
	case <-closeDone:
	case <-time.After(2 * time.Second):
		// Ensure blocked producers are released before reporting the failure.
		p.abortOwner()
		t.Fatal("projection Close() remained blocked with a stalled selected consumer")
	}
	select {
	case <-producerDone:
	case <-time.After(2 * time.Second):
		t.Fatal("blocked projection producer did not release after Close()")
	}
}

func TestTurnHandleStaleSendIsGuardedByDone(t *testing.T) {
	done := make(chan struct{})
	close(done)
	handle := &turnHandle{commands: make(chan steerCommand, 1), done: done}
	finished := make(chan bool, 1)
	go func() {
		finished <- handle.send(context.Background(), steerCommand{reply: make(chan steerReply, 1)})
	}()
	select {
	case sent := <-finished:
		if sent {
			t.Fatal("stale turn handle accepted a command after retirement")
		}
	case <-time.After(time.Second):
		t.Fatal("stale turn handle send blocked after retirement")
	}
}

func TestTurnHandleAlreadyCanceledSendDoesNotEnqueue(t *testing.T) {
	handle := &turnHandle{commands: make(chan steerCommand, 1), done: make(chan struct{})}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if accepted := handle.send(ctx, steerCommand{reply: make(chan steerReply, 1)}); accepted {
		t.Fatal("already-canceled turn handle accepted a steering command")
	}
	select {
	case <-handle.commands:
		t.Fatal("already-canceled steering command entered the turn mailbox")
	default:
	}
}

func TestDriverSteerAlreadyCanceledIsProvenFallbackWithoutAdmission(t *testing.T) {
	handle := &turnHandle{commands: make(chan steerCommand, 1), done: make(chan struct{})}
	d := &Driver{active: handle}
	request, err := driver.NewSteerRequest([]content.Block{&content.TextBlock{Text: "already canceled"}})
	if err != nil {
		t.Fatalf("NewSteerRequest() error = %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	result, steerErr := d.Steer(ctx, request)
	if !errors.Is(steerErr, context.Canceled) {
		t.Fatalf("Steer() error = %v, want context.Canceled", steerErr)
	}
	if result.Outcome != driver.SteerOutcomeFallbackRequired || result.WriteAdmitted {
		t.Fatalf("Steer() result = %#v, want fallback_required with false admission", result)
	}
	if err := result.Validate(); err != nil {
		t.Fatalf("Steer() result validation error = %v", err)
	}
}

func TestTurnArbiterDispatcherRejectionIsFallbackWithoutAdmission(t *testing.T) {
	sess := newScriptedSession("dispatcher-rejected")
	d := newTurnTestDriver(sess)
	d.steeringOn = true
	request, err := driver.NewSteerRequest([]content.Block{&content.TextBlock{Text: "dispatcher unavailable"}})
	if err != nil {
		t.Fatalf("NewSteerRequest() error = %v", err)
	}
	a := &turnArbiter{
		driver:     d,
		session:    sess,
		steers:     make(map[uint64]steerCommand),
		pending:    make([]arbObservation, 0, 1),
		dispatcher: nil,
	}
	a.acceptSteer(steerCommand{
		ctx:     context.Background(),
		request: request,
		reply:   make(chan steerReply, 1),
	})
	if len(a.pending) != 1 {
		t.Fatalf("pending local steering observations = %d, want 1", len(a.pending))
	}
	result := a.pending[0].result.result
	if result.Outcome != driver.SteerOutcomeFallbackRequired || result.WriteAdmitted {
		t.Fatalf("dispatcher rejection result = %#v, want fallback_required with false admission", result)
	}
	if err := result.Validate(); err != nil {
		t.Fatalf("dispatcher rejection result validation error = %v", err)
	}
}

func TestDriverSteerTimeoutBeforeACPAttemptIsFallbackWithoutAdmission(t *testing.T) {
	handle := &turnHandle{commands: make(chan steerCommand, 1), done: make(chan struct{})}
	d := &Driver{active: handle}
	request, err := driver.NewSteerRequest([]content.Block{&content.TextBlock{Text: "timeout before attempt"}})
	if err != nil {
		t.Fatalf("NewSteerRequest() error = %v", err)
	}
	accepted := make(chan struct{})
	go func() {
		command := <-handle.commands
		close(command.accepted)
		close(accepted)
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()

	result, steerErr := d.Steer(ctx, request)
	if !errors.Is(steerErr, context.DeadlineExceeded) {
		t.Fatalf("Steer() error = %v, want context deadline", steerErr)
	}
	select {
	case <-accepted:
	case <-time.After(time.Second):
		t.Fatal("steering command was not acknowledged")
	}
	if result.Outcome != driver.SteerOutcomeFallbackRequired || result.WriteAdmitted {
		t.Fatalf("Steer() result = %#v, want fallback_required with false admission", result)
	}
	if err := result.Validate(); err != nil {
		t.Fatalf("Steer() result validation error = %v", err)
	}
}

func TestCanceledSteerPublishesOneFallbackObservationWithoutACPAttempt(t *testing.T) {
	sess := &steeringSession{scriptedSession: newScriptedSession("canceled-observation")}
	release := make(chan struct{})
	sess.promptHook = func(int, []protocol.ContentBlock) (*client.PromptResult, error) {
		<-release
		return &client.PromptResult{StopReason: protocol.StopReasonEndTurn}, nil
	}
	d := newTurnTestDriver(sess.scriptedSession)
	d.session, d.steeringOn = sess, true
	stream, err := d.Spawn(context.Background(), driver.Turn{})
	if err != nil {
		t.Fatalf("Spawn() error = %v", err)
	}
	<-sess.promptStarts
	request, err := driver.NewSteerRequest([]content.Block{&content.TextBlock{Text: "cancel before send"}})
	if err != nil {
		t.Fatalf("NewSteerRequest() error = %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	result, steerErr := d.Steer(ctx, request)
	if !errors.Is(steerErr, context.Canceled) {
		t.Fatalf("Steer() error = %v, want context.Canceled", steerErr)
	}
	if result.Outcome != driver.SteerOutcomeFallbackRequired || result.WriteAdmitted {
		t.Fatalf("Steer() result = %#v, want fallback_required with false admission", result)
	}
	if len(sess.steerParams) != 0 {
		t.Fatalf("ACP steer calls = %d, want zero for canceled-before-send", len(sess.steerParams))
	}
	ordered := stream.(driver.OrderedStream).Observations()
	select {
	case observation := <-ordered:
		steer, ok := observation.(driver.SteerObservation)
		if !ok || steer.Outcome != driver.SteerOutcomeFallbackRequired || steer.WriteAdmitted {
			t.Fatalf("canceled steer observation = %#v, want one fallback without admission", observation)
		}
	case <-time.After(time.Second):
		t.Fatal("canceled steering observation did not arrive")
	}
	close(release)
	for range ordered {
	}
}

func TestInitStreamConcurrentSelectionCloseAndLargeForward(t *testing.T) {
	inner := newInitTestStream(4096)
	wrapped := newInitStream(inner, "init").(*initStream)
	var wg sync.WaitGroup
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			if index%2 == 0 {
				_ = wrapped.Events()
			} else {
				_ = wrapped.Observations()
			}
		}(i)
	}
	wg.Wait()
	_ = wrapped.Close()
	select {
	case <-wrapped.done:
	case <-time.After(time.Second):
		t.Fatal("wrapper did not stop after concurrent close")
	}

	largeInner := newInitTestStream(4096)
	large := newInitStream(largeInner, "init").(*initStream)
	out := large.Events()
	for i := 0; i < 3001; i++ {
		largeInner.events <- driver.Event{Kind: driver.KindTextDelta, Text: "x"}
	}
	close(largeInner.events)
	count := 0
	for range out {
		count++
	}
	if count != 3002 {
		t.Fatalf("wrapper event count = %d, want init plus 3001 forwarded events", count)
	}

	closedInner := newInitTestStream(1)
	closed := newInitStream(closedInner, "init").(*initStream)
	if err := closed.Close(); err != nil {
		t.Fatalf("close-before-selection error = %v", err)
	}
	select {
	case _, ok := <-closed.Events():
		if ok {
			t.Fatal("close-before-selection returned an event")
		}
	case <-time.After(time.Second):
		t.Fatal("close-before-selection channel remained open")
	}
}

type initTestStream struct {
	events chan driver.Event
	once   sync.Once
}

func newInitTestStream(capacity int) *initTestStream {
	return &initTestStream{events: make(chan driver.Event, capacity)}
}

func (s *initTestStream) Events() <-chan driver.Event { return s.events }
func (s *initTestStream) History() (driver.History, error) {
	return driver.History{Available: false}, nil
}
func (s *initTestStream) Close() error {
	s.once.Do(func() { close(s.events) })
	return nil
}

type barrierConcurrencySession struct {
	*scriptedSession
	started chan uint64
	release chan struct{}
	active  atomic.Int32
	max     atomic.Int32
}

func (s *barrierConcurrencySession) WaitForUpdatesThrough(ctx context.Context, sequence uint64) error {
	current := s.active.Add(1)
	for {
		old := s.max.Load()
		if current <= old || s.max.CompareAndSwap(old, current) {
			break
		}
	}
	s.started <- sequence
	select {
	case <-s.release:
	case <-ctx.Done():
	}
	s.active.Add(-1)
	return nil
}

func TestBarrierWorkerSerializesWaits(t *testing.T) {
	sess := &barrierConcurrencySession{
		scriptedSession: newScriptedSession("barrier"),
		started:         make(chan uint64, 2),
		release:         make(chan struct{}),
	}
	worker := newBarrierWorker(context.Background(), sess)
	if !worker.request(1) || !worker.request(2) {
		t.Fatal("failed to enqueue barrier requests")
	}
	select {
	case sequence := <-sess.started:
		if sequence != 1 {
			t.Fatalf("first barrier sequence = %d, want 1", sequence)
		}
	case <-time.After(time.Second):
		t.Fatal("first barrier did not start")
	}
	select {
	case sequence := <-sess.started:
		t.Fatalf("barrier %d started before first completed", sequence)
	case <-time.After(10 * time.Millisecond):
	}
	close(sess.release)
	for i := 0; i < 2; i++ {
		select {
		case <-worker.results:
		case <-time.After(time.Second):
			t.Fatal("barrier result did not arrive")
		}
	}
	worker.stop()
	if got := sess.max.Load(); got != 1 {
		t.Fatalf("maximum concurrent barriers = %d, want 1", got)
	}
}

func TestSteerDispatcherIsFIFO(t *testing.T) {
	sess := &steeringSession{scriptedSession: newScriptedSession("fifo")}
	var mu sync.Mutex
	var order []string
	sess.steerHook = func(_ context.Context, params client.SteerParams) (client.SteerResult, error) {
		mu.Lock()
		defer mu.Unlock()
		text := params.Prompt[0].Text.Text
		order = append(order, text)
		sequence := uint64(len(order))
		return client.SteerResult{Outcome: client.SteerOutcomeInjected, WriteAdmitted: true, ReceiveSequence: sequence, ResponseSequence: sequence}, nil
	}
	dispatcher := newSteerDispatcher(context.Background(), sess)
	for i := 1; i <= 3; i++ {
		text := string(rune('0' + i))
		if !dispatcher.submit(steerJob{
			id:     uint64(i),
			ctx:    context.Background(),
			params: client.SteerParams{Prompt: []protocol.ContentBlock{{Text: &protocol.TextContent{Text: text}}}},
		}) {
			t.Fatalf("submit(%d) failed", i)
		}
	}
	defer dispatcher.stop()
	for i := 1; i <= 3; i++ {
		select {
		case event := <-dispatcher.Events():
			if event.id != uint64(i) {
				t.Fatalf("dispatcher event %d id = %d, want %d", i, event.id, i)
			}
		case <-time.After(time.Second):
			t.Fatalf("dispatcher event %d timed out", i)
		}
	}
	mu.Lock()
	got := append([]string(nil), order...)
	mu.Unlock()
	if want := []string{"1", "2", "3"}; len(got) != len(want) || got[0] != want[0] || got[1] != want[1] || got[2] != want[2] {
		t.Fatalf("provider order = %v, want %v", got, want)
	}
}

func TestSteerDispatcherQueuedTerminalResolvesWithoutStartingQueuedCall(t *testing.T) {
	sess := &steeringSession{scriptedSession: newScriptedSession("queued-terminal")}
	started := make(chan struct{})
	release := make(chan struct{})
	sess.steerHook = func(_ context.Context, _ client.SteerParams) (client.SteerResult, error) {
		select {
		case <-started:
		default:
			close(started)
		}
		<-release
		return client.SteerResult{Outcome: client.SteerOutcomeInjected, WriteAdmitted: true, ReceiveSequence: 1, ResponseSequence: 1}, nil
	}
	dispatcher := newSteerDispatcher(context.Background(), sess)
	if !dispatcher.submit(steerJob{id: 1, ctx: context.Background(), params: client.SteerParams{}}) {
		t.Fatal("first submit failed")
	}
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("first steering call did not start")
	}
	if !dispatcher.submit(steerJob{id: 2, ctx: context.Background(), params: client.SteerParams{}}) {
		t.Fatal("queued submit failed")
	}
	if !dispatcher.resolveTerminal() {
		t.Fatal("terminal resolution was not accepted")
	}
	defer func() {
		close(release)
		dispatcher.stop()
	}()
	seenQueued := false
	seenTerminal := false
	deadline := time.After(time.Second)
	for !seenTerminal {
		select {
		case event := <-dispatcher.Events():
			if event.id == 2 {
				seenQueued = true
				if event.err == nil || event.writeAdmitted {
					t.Fatalf("queued event = %#v, want pre-admission terminal resolution", event)
				}
			}
			if event.terminalComplete {
				seenTerminal = true
			}
		case <-deadline:
			t.Fatal("terminal resolution timed out")
		}
	}
	if !seenQueued {
		t.Fatal("queued steering call did not receive terminal resolution")
	}
}

func TestSteerCallerDeadlineIgnoresLateResultAndPublishesUnknown(t *testing.T) {
	sess := &steeringSession{scriptedSession: newScriptedSession("caller-deadline")}
	steerRelease := make(chan struct{})
	promptRelease := make(chan struct{})
	sess.steerHook = func(context.Context, client.SteerParams) (client.SteerResult, error) {
		<-steerRelease
		return client.SteerResult{Outcome: client.SteerOutcomeInjected, WriteAdmitted: true, ReceiveSequence: 2, ResponseSequence: 2}, nil
	}
	sess.promptHook = func(int, []protocol.ContentBlock) (*client.PromptResult, error) {
		<-promptRelease
		return &client.PromptResult{StopReason: protocol.StopReasonEndTurn, ReceiveSequence: 3, ResponseSequence: 3, WriteAdmitted: true}, nil
	}
	d := newTurnTestDriver(sess.scriptedSession)
	d.session, d.steeringOn = sess, true
	stream, err := d.Spawn(context.Background(), driver.Turn{})
	if err != nil {
		t.Fatal(err)
	}
	request, err := driver.NewSteerRequest([]content.Block{&content.TextBlock{Text: "deadline"}})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	result, steerErr := d.Steer(ctx, request)
	cancel()
	if steerErr == nil || result.Outcome != driver.SteerOutcomeDeliveryUnknown {
		t.Fatalf("Steer() = %#v, %v, want caller deadline unknown", result, steerErr)
	}
	close(steerRelease)
	close(promptRelease)
	steers := 0
	for observation := range stream.(driver.OrderedStream).Observations() {
		if steer, ok := observation.(driver.SteerObservation); ok {
			steers++
			if steer.Outcome != driver.SteerOutcomeDeliveryUnknown {
				t.Fatalf("late steer observation = %#v, want unknown", steer)
			}
		}
	}
	if steers != 1 {
		t.Fatalf("steer observations = %d, want exactly one", steers)
	}
}

func TestArbiterPositiveSequenceRegressionFailsClosed(t *testing.T) {
	sess := newScriptedSession("regression")
	release := make(chan struct{})
	sess.promptHook = func(int, []protocol.ContentBlock) (*client.PromptResult, error) {
		<-release
		return &client.PromptResult{StopReason: protocol.StopReasonEndTurn, ReceiveSequence: 3, ResponseSequence: 3, WriteAdmitted: true}, nil
	}
	d := newTurnTestDriver(sess)
	d.steeringOn = true
	stream, err := d.Spawn(context.Background(), driver.Turn{})
	if err != nil {
		t.Fatal(err)
	}
	observations := stream.(driver.OrderedStream).Observations()
	sess.updates <- client.Update{ReceiveSequence: 2, SessionUpdate: protocol.SessionUpdate{AgentMessageChunk: &protocol.ContentChunk{Content: protocol.ContentBlock{Text: &protocol.TextContent{Text: "first"}}}}}
	select {
	case observation := <-observations:
		if observation.Sequence() != 2 {
			t.Fatalf("first observation sequence = %d, want 2", observation.Sequence())
		}
	case <-time.After(time.Second):
		t.Fatal("first observation did not arrive")
	}
	sess.updates <- client.Update{ReceiveSequence: 1, SessionUpdate: protocol.SessionUpdate{AgentMessageChunk: &protocol.ContentChunk{Content: protocol.ContentBlock{Text: &protocol.TextContent{Text: "regression"}}}}}
	close(release)
	found := false
	for observation := range observations {
		if prompt, ok := observation.(driver.PromptObservation); ok && prompt.Err != nil {
			found = true
			break
		}
	}
	if !found {
		t.Fatal("sequence regression did not produce a fail-closed prompt observation")
	}
}
