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

func TestTurnHandleAcceptedAckWinsTerminalWhenBothReady(t *testing.T) {
	done := make(chan struct{})
	handle := &turnHandle{commands: make(chan steerCommand, 1), done: done}
	result := make(chan steerSendResult, 1)
	go func() {
		result <- handle.sendResult(context.Background(), steerCommand{reply: make(chan steerReply, 1)})
	}()

	command := <-handle.commands
	if command.ack == nil {
		t.Fatal("turn handle did not attach an explicit acknowledgement state")
	}
	if !command.ack.resolve(steerSendAccepted) {
		t.Fatal("accepted acknowledgement did not win its linearization point")
	}
	close(done)
	if got := <-result; got != steerSendAccepted {
		t.Fatalf("sendResult() = %s, want accepted after terminal close", got)
	}
}

func TestTurnHandleAcceptedAckWinsHighCountTerminalRace(t *testing.T) {
	for i := 0; i < 10000; i++ {
		done := make(chan struct{})
		handle := &turnHandle{commands: make(chan steerCommand, 1), done: done}
		result := make(chan steerSendResult, 1)
		go func() {
			result <- handle.sendResult(context.Background(), steerCommand{reply: make(chan steerReply, 1)})
		}()
		command := <-handle.commands
		if command.ack == nil || !command.ack.resolve(steerSendAccepted) {
			t.Fatalf("iteration %d: accepted acknowledgement did not resolve exactly once", i)
		}
		close(done)
		if got := <-result; got != steerSendAccepted {
			t.Fatalf("iteration %d: sendResult() = %s, want accepted", i, got)
		}
	}
}

func TestDriverSteerAcceptedAckWinsDoneWithoutCapacityFallback(t *testing.T) {
	done := make(chan struct{})
	handle := &turnHandle{
		commands: make(chan steerCommand, 1),
		done:     done,
		lane:     newSteerObservationLane(1),
	}
	d := &Driver{active: handle}
	request, err := driver.NewSteerRequest([]content.Block{&content.TextBlock{Text: "accepted"}})
	if err != nil {
		t.Fatal(err)
	}
	go func() {
		command := <-handle.commands
		if command.ack == nil || !command.ack.resolve(steerSendAccepted) {
			return
		}
		close(done)
		command.reply <- steerReply{result: driver.SteerResult{
			Outcome:          driver.SteerOutcomeInjected,
			WriteAdmitted:    true,
			ReceiveSequence:  1,
			ResponseSequence: 1,
		}}
	}()
	result, steerErr := d.Steer(context.Background(), request)
	var capacityErr *driver.SteerAdmissionError
	if errors.As(steerErr, &capacityErr) {
		t.Fatalf("Steer() reported capacity after accepted ack: %v", steerErr)
	}
	if steerErr != nil || result.Outcome != driver.SteerOutcomeInjected {
		t.Fatalf("Steer() = %#v, %v, want injected result after accepted ack", result, steerErr)
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

func TestDriverSteerTimeoutAfterMailboxAdmissionIsUnknownWithoutWriterFact(t *testing.T) {
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
	if result.Outcome != driver.SteerOutcomeAdmissionUnknown || result.WriteAdmitted {
		t.Fatalf("Steer() result = %#v, want admission_unknown with false admission", result)
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
				if event.err == nil || event.admission != steerAdmissionNotAdmitted {
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

func TestSteerCallerDeadlineWithAdmissionBlockedPublishesAdmissionUnknown(t *testing.T) {
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
	if steerErr == nil || result.Outcome != driver.SteerOutcomeAdmissionUnknown || result.WriteAdmitted {
		t.Fatalf("Steer() = %#v, %v, want caller deadline admission_unknown without fabricated admission", result, steerErr)
	}
	close(steerRelease)
	close(promptRelease)
	steers := 0
	for observation := range stream.(driver.OrderedStream).Observations() {
		if steer, ok := observation.(driver.SteerObservation); ok {
			steers++
			if steer.Outcome != driver.SteerOutcomeDeliveryUnknown || !steer.WriteAdmitted {
				t.Fatalf("late steer observation = %#v, want delivery_unknown after admitted late result", steer)
			}
		}
	}
	if steers != 1 {
		t.Fatalf("steer observations = %d, want exactly one", steers)
	}
}

func TestSteerCallerDeadlineLateExplicitNotAdmittedFallsBackWithoutTrue(t *testing.T) {
	sess := &steeringSession{scriptedSession: newScriptedSession("late-not-admitted")}
	steerRelease := make(chan struct{})
	sess.steerHook = func(context.Context, client.SteerParams) (client.SteerResult, error) {
		<-steerRelease
		return client.SteerResult{Outcome: client.SteerOutcomePromptRequired}, nil
	}
	d := newTurnTestDriver(sess.scriptedSession)
	d.session, d.steeringOn = sess, true
	stream, err := d.Spawn(context.Background(), driver.Turn{})
	if err != nil {
		t.Fatal(err)
	}
	request, err := driver.NewSteerRequest([]content.Block{&content.TextBlock{Text: "late false"}})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	result, steerErr := d.Steer(ctx, request)
	cancel()
	if steerErr == nil || result.Outcome != driver.SteerOutcomeAdmissionUnknown || result.WriteAdmitted {
		t.Fatalf("caller result = %#v, %v, want admission_unknown without admission", result, steerErr)
	}
	close(steerRelease)
	var found bool
	for observation := range stream.(driver.OrderedStream).Observations() {
		steer, ok := observation.(driver.SteerObservation)
		if !ok {
			continue
		}
		found = true
		if steer.Outcome != driver.SteerOutcomeFallbackRequired || steer.WriteAdmitted {
			t.Fatalf("late observation = %#v, want fallback without admission", steer)
		}
	}
	if !found {
		t.Fatal("late explicit not-admitted result produced no steer observation")
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

func TestSteerAdmissionStateNamesAndFailureClassification(t *testing.T) {
	states := []struct {
		state steerAdmission
		name  string
	}{
		{steerAdmissionPending, "pending"},
		{steerAdmissionNotAdmitted, "not_admitted"},
		{steerAdmissionPendingWriter, "admission_pending"},
		{steerAdmissionAdmitted, "admitted"},
	}
	for _, tt := range states {
		if got := tt.state.String(); got != tt.name {
			t.Errorf("steerAdmission(%d).String() = %q, want %q", tt.state, got, tt.name)
		}
	}

	tests := []struct {
		state steerAdmission
		want  driver.SteerOutcome
		admit bool
	}{
		{steerAdmissionPending, driver.SteerOutcomeAdmissionUnknown, false},
		{steerAdmissionNotAdmitted, driver.SteerOutcomeFallbackRequired, false},
		{steerAdmissionPendingWriter, driver.SteerOutcomeAdmissionUnknown, false},
		{steerAdmissionAdmitted, driver.SteerOutcomeDeliveryUnknown, true},
	}
	for _, tt := range tests {
		got := lateSteerResult(tt.state, client.SteerResult{})
		if got.Outcome != tt.want || got.WriteAdmitted != tt.admit {
			t.Errorf("lateSteerResult(%s) = %#v, want %s admitted=%t", tt.state, got, tt.want, tt.admit)
		}
		if err := got.Validate(); err != nil {
			t.Errorf("lateSteerResult(%s) invalid: %v", tt.state, err)
		}
	}
}

func TestSteerAttemptCancellationLinearizesBeforeStart(t *testing.T) {
	attempt := &steerAttempt{}
	if got := attempt.cancelAndSnapshot(); got != steerAdmissionNotAdmitted {
		t.Fatalf("cancelAndSnapshot() = %s, want not_admitted", got)
	}
	if attempt.beginStart() {
		t.Fatal("beginStart() won after cancellation sealed pending admission")
	}
	if got := attempt.snapshot(); got != steerAdmissionNotAdmitted {
		t.Fatalf("snapshot after canceled start = %s, want not_admitted", got)
	}
}

func TestSteerAttemptStartWinsAdmissionPendingWithoutFabricatingTrue(t *testing.T) {
	attempt := &steerAttempt{}
	if !attempt.beginStart() {
		t.Fatal("beginStart() did not claim pending attempt")
	}
	if got := attempt.cancelAndSnapshot(); got != steerAdmissionPendingWriter {
		t.Fatalf("cancelAndSnapshot() = %s, want admission_pending", got)
	}
	result := callerTimeoutSteerResult(attempt)
	if result.Outcome != driver.SteerOutcomeAdmissionUnknown || result.WriteAdmitted {
		t.Fatalf("caller timeout result = %#v, want admission_unknown without admission", result)
	}
	if err := result.Validate(); err != nil {
		t.Fatalf("caller timeout result invalid: %v", err)
	}
}

func TestSequenceFailureNeverFabricatesAdmission(t *testing.T) {
	pending := &steerAttempt{}
	if !pending.beginStart() {
		t.Fatal("beginStart() failed")
	}
	if got := sequenceFailureResult(pending); got.Outcome != driver.SteerOutcomeAdmissionUnknown || got.WriteAdmitted {
		t.Fatalf("pending sequence failure = %#v, want admission_unknown without admission", got)
	}
	admitted := &steerAttempt{}
	if !admitted.beginStart() {
		t.Fatal("beginStart() failed for admitted attempt")
	}
	admitted.markAdmission(true)
	if got := sequenceFailureResult(admitted); got.Outcome != driver.SteerOutcomeDeliveryUnknown || !got.WriteAdmitted {
		t.Fatalf("admitted sequence failure = %#v, want delivery_unknown with admission", got)
	}
}

func TestSteerDispatcherCanceledBeforeStartDoesNotInvokeProvider(t *testing.T) {
	sess := &steeringSession{scriptedSession: newScriptedSession("cancel-before-start")}
	var calls atomic.Int32
	sess.steerHook = func(context.Context, client.SteerParams) (client.SteerResult, error) {
		calls.Add(1)
		return client.SteerResult{Outcome: client.SteerOutcomeInjected, WriteAdmitted: true, ReceiveSequence: 1, ResponseSequence: 1}, nil
	}
	dispatcher := newSteerDispatcher(context.Background(), sess)
	attempt := &steerAttempt{}
	if got := attempt.cancelAndSnapshot(); got != steerAdmissionNotAdmitted {
		t.Fatalf("cancelAndSnapshot() = %s, want not_admitted", got)
	}
	if !dispatcher.submit(steerJob{
		id:      1,
		ctx:     context.Background(),
		attempt: attempt,
		params: client.SteerParams{Prompt: []protocol.ContentBlock{{
			Text: &protocol.TextContent{Text: "must not send"},
		}}},
	}) {
		t.Fatal("dispatcher.submit() failed")
	}
	defer dispatcher.stop()
	select {
	case event := <-dispatcher.Events():
		if event.admission != steerAdmissionNotAdmitted {
			t.Fatalf("canceled-before-start event admission = %s, want not_admitted", event.admission)
		}
	case <-time.After(time.Second):
		t.Fatal("canceled-before-start event did not arrive")
	}
	if got := calls.Load(); got != 0 {
		t.Fatalf("provider calls = %d, want zero after cancellation won", got)
	}
}

func TestSteerAdmissionCancelStartRaceAlwaysSealsState(t *testing.T) {
	for i := 0; i < 1000; i++ {
		attempt := &steerAttempt{}
		start := make(chan struct{})
		var cancelState, startWon bool
		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			<-start
			cancelState = attempt.cancelAndSnapshot() == steerAdmissionNotAdmitted
		}()
		go func() {
			defer wg.Done()
			<-start
			startWon = attempt.beginStart()
		}()
		close(start)
		wg.Wait()
		state := attempt.snapshot()
		if state == steerAdmissionPending {
			t.Fatalf("iteration %d left attempt pending after cancel/start race", i)
		}
		if state == steerAdmissionNotAdmitted && startWon {
			t.Fatalf("iteration %d both start and cancellation claimed the attempt", i)
		}
		if state == steerAdmissionPendingWriter && cancelState {
			t.Fatalf("iteration %d cancellation reported not-admitted after start won", i)
		}
	}
}

func TestSteerObservationLaneCapacityAndClose(t *testing.T) {
	lane := newSteerObservationLane(1)
	first, status := lane.reserve()
	if status != steerReservationReserved || first == nil {
		t.Fatal("first reservation failed")
	}
	if _, status := lane.reserve(); status != steerReservationCapacityExhausted {
		t.Fatal("reservation lane accepted beyond fixed capacity")
	}
	if got := lane.inUse(); got != 1 {
		t.Fatalf("lane in-use = %d, want 1", got)
	}
	first.release()
	first.release()
	if got := lane.inUse(); got != 0 {
		t.Fatalf("lane in-use after idempotent release = %d, want 0", got)
	}
	second, status := lane.reserve()
	if status != steerReservationReserved || second == nil {
		t.Fatal("reservation lane did not reuse released slot")
	}
	lane.close()
	second.release()
	if got := lane.inUse(); got != 0 {
		t.Fatalf("lane in-use after close = %d, want 0", got)
	}
	if _, status := lane.reserve(); status != steerReservationClosed {
		t.Fatal("closed reservation lane accepted a new reservation")
	}
}

func TestCanceledAcceptedSteerSurvivesSaturatedNonSteerProjection(t *testing.T) {
	sess := &steeringSession{scriptedSession: newScriptedSession("saturated-non-steer")}
	promptRelease := make(chan struct{})
	sess.promptHook = func(int, []protocol.ContentBlock) (*client.PromptResult, error) {
		<-promptRelease
		return &client.PromptResult{StopReason: protocol.StopReasonEndTurn}, nil
	}
	d := newTurnTestDriver(sess.scriptedSession)
	d.session, d.steeringOn = sess, true
	stream, err := d.Spawn(context.Background(), driver.Turn{})
	if err != nil {
		t.Fatalf("Spawn() error = %v", err)
	}
	ordered, ok := stream.(*orderedStream)
	if !ok {
		t.Fatalf("Spawn() stream = %T, want ordered stream", stream)
	}
	p := ordered.stream.projection
	observations := ordered.Observations()
	budget := cap(p.observations)*2 + cap(p.commands)
	accepted := make(chan struct{}, budget+1)
	producerDone := make(chan struct{})
	go func() {
		defer close(producerDone)
		for i := 0; i < budget+1; i++ {
			p.emitObservation(driver.UpdateObservation{})
			accepted <- struct{}{}
		}
	}()
	for i := 0; i < budget; i++ {
		select {
		case <-accepted:
		case <-time.After(2 * time.Second):
			t.Fatalf("only %d/%d non-steer observations were accepted", i, budget)
		}
	}
	select {
	case <-accepted:
		t.Fatal("non-steer projection accepted beyond its bounded budget")
	case <-time.After(25 * time.Millisecond):
	}

	request, err := driver.NewSteerRequest([]content.Block{&content.TextBlock{Text: "cancel while saturated"}})
	if err != nil {
		t.Fatalf("NewSteerRequest() error = %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	result, steerErr := d.Steer(ctx, request)
	if !errors.Is(steerErr, context.Canceled) || result.Outcome != driver.SteerOutcomeFallbackRequired || result.WriteAdmitted {
		t.Fatalf("canceled saturated steer = %#v, %v, want fallback without admission", result, steerErr)
	}

	var steerCount atomic.Int32
	drainDone := make(chan struct{})
	go func() {
		defer close(drainDone)
		for observation := range observations {
			if _, ok := observation.(driver.SteerObservation); ok {
				steerCount.Add(1)
			}
		}
	}()
	close(promptRelease)
	select {
	case <-producerDone:
	case <-time.After(2 * time.Second):
		t.Fatal("saturated non-steer producer did not finish after drain")
	}
	select {
	case <-drainDone:
	case <-time.After(3 * time.Second):
		t.Fatal("ordered stream did not close after draining saturated projection")
	}
	if got := steerCount.Load(); got != 1 {
		t.Fatalf("steering observations = %d, want exactly one for accepted canceled steer", got)
	}
}

func TestSteerReservationExhaustionRejectsBeforeACP(t *testing.T) {
	sess := &steeringSession{scriptedSession: newScriptedSession("reservation-exhausted")}
	promptRelease := make(chan struct{})
	var providerCalls atomic.Int32
	sess.steerHook = func(context.Context, client.SteerParams) (client.SteerResult, error) {
		providerCalls.Add(1)
		return client.SteerResult{Outcome: client.SteerOutcomeInjected, WriteAdmitted: true, ReceiveSequence: 1, ResponseSequence: 1}, nil
	}
	sess.promptHook = func(int, []protocol.ContentBlock) (*client.PromptResult, error) {
		<-promptRelease
		return &client.PromptResult{StopReason: protocol.StopReasonEndTurn}, nil
	}
	d := newTurnTestDriver(sess.scriptedSession)
	d.session, d.steeringOn = sess, true
	stream, err := d.Spawn(context.Background(), driver.Turn{})
	if err != nil {
		t.Fatalf("Spawn() error = %v", err)
	}
	lane := d.activeHandle().lane
	reservations := make([]*steerObservationReservation, 0, steeringObservationCapacity)
	for i := 0; i < steeringObservationCapacity; i++ {
		reservation, status := lane.reserve()
		if status != steerReservationReserved {
			t.Fatalf("reservation %d failed before configured capacity", i)
		}
		reservations = append(reservations, reservation)
	}
	request, err := driver.NewSteerRequest([]content.Block{&content.TextBlock{Text: "must reject"}})
	if err != nil {
		t.Fatalf("NewSteerRequest() error = %v", err)
	}
	result, steerErr := d.Steer(context.Background(), request)
	var admissionErr *driver.SteerAdmissionError
	if !errors.As(steerErr, &admissionErr) {
		t.Fatalf("Steer() error = %v (%T), want SteerAdmissionError", steerErr, steerErr)
	}
	if result.Outcome != driver.SteerOutcomeFallbackRequired || result.WriteAdmitted {
		t.Fatalf("Steer() result = %#v, want retry-safe fallback without admission", result)
	}
	if got := providerCalls.Load(); got != 0 {
		t.Fatalf("provider steering calls = %d, want zero on reservation exhaustion", got)
	}
	for _, reservation := range reservations {
		reservation.release()
	}
	close(promptRelease)
	for range stream.(driver.OrderedStream).Observations() {
	}
}

func TestSteerRetirementNeverLooksLikeCapacityExhaustion(t *testing.T) {
	request, err := driver.NewSteerRequest([]content.Block{&content.TextBlock{Text: "retire"}})
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 100; i++ {
		handle := &turnHandle{
			commands: make(chan steerCommand, 1),
			done:     make(chan struct{}),
			lane:     newSteerObservationLane(1),
		}
		d := &Driver{active: handle}
		start := make(chan struct{})
		result := make(chan error, 1)
		go func() {
			<-start
			_, steerErr := d.Steer(context.Background(), request)
			result <- steerErr
		}()
		go func() {
			<-start
			handle.retire()
		}()
		close(start)
		if steerErr := <-result; steerErr != nil {
			var capacityErr *driver.SteerAdmissionError
			if errors.As(steerErr, &capacityErr) {
				t.Fatalf("iteration %d: retirement reported capacity exhaustion: %v", i, steerErr)
			}
		}
	}
}

func TestSteerObservationLaneReserveDistinguishesCapacityAndClosed(t *testing.T) {
	lane := newSteerObservationLane(1)
	first, status := lane.reserve()
	if status != steerReservationReserved || first == nil {
		t.Fatalf("first reserve() = (%p, %s), want reserved", first, status)
	}
	if second, status := lane.reserve(); second != nil || status != steerReservationCapacityExhausted {
		t.Fatalf("full reserve() = (%p, %s), want capacity_exhausted", second, status)
	}
	lane.close()
	if third, status := lane.reserve(); third != nil || status != steerReservationClosed {
		t.Fatalf("closed reserve() = (%p, %s), want closed", third, status)
	}
	first.release()
}

func TestSteerReservationCloseRaceReleasesOutstandingSlots(t *testing.T) {
	for i := 0; i < 1000; i++ {
		lane := newSteerObservationLane(1)
		reservation, status := lane.reserve()
		if status != steerReservationReserved {
			t.Fatalf("iteration %d: initial reservation failed", i)
		}
		start := make(chan struct{})
		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			<-start
			reservation.release()
		}()
		go func() {
			defer wg.Done()
			<-start
			lane.close()
		}()
		close(start)
		wg.Wait()
		if got := lane.inUse(); got != 0 {
			t.Fatalf("iteration %d: lane in-use = %d after close race, want 0", i, got)
		}
		if _, status := lane.reserve(); status != steerReservationClosed {
			t.Fatalf("iteration %d: closed lane accepted reservation", i)
		}
	}
}
