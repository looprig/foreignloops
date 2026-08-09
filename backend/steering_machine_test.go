package backend

import (
	"context"
	"errors"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/looprig/core/content"
	"github.com/looprig/core/uuid"
	"github.com/looprig/foreignloops/driver"
	"github.com/looprig/harness/pkg/command"
	"github.com/looprig/harness/pkg/event"
	"github.com/looprig/harness/pkg/foreign"
	"github.com/looprig/harness/pkg/identity"
	"github.com/looprig/harness/pkg/loop"
)

// gatedSteerStream keeps the active prompt open while the actor adjudicates a
// steering acknowledgement. The test owns the observation order explicitly;
// no wall-clock synchronization is needed.
type gatedSteerStream struct {
	observations chan driver.Observation
}

func (s *gatedSteerStream) Events() <-chan driver.Event { return nil }

func (s *gatedSteerStream) Observations() <-chan driver.Observation {
	return s.observations
}

func (s *gatedSteerStream) History() (driver.History, error) {
	return driver.History{Available: false}, nil
}

func (s *gatedSteerStream) Close() error { return nil }

type gatedSteerAgent struct {
	stream      *gatedSteerStream
	steerStart  chan struct{}
	steerReturn chan struct{}
	steerResult driver.SteerResult
}

func (a *gatedSteerAgent) Spawn(context.Context, driver.Turn) (driver.Stream, error) {
	return a.stream, nil
}

func (a *gatedSteerAgent) Steer(ctx context.Context, _ driver.SteerRequest) (driver.SteerResult, error) {
	select {
	case <-a.steerStart:
	default:
		close(a.steerStart)
	}
	select {
	case <-a.steerReturn:
		return a.steerResult, nil
	case <-ctx.Done():
		return driver.SteerResult{}, ctx.Err()
	}
}

type recordingDeliveryHook struct {
	mu           sync.Mutex
	calls        []string
	reservations []uuid.UUID
	fallbacks    []uuid.UUID
	resolutions  []foreign.DeliveryResolution
	err          error
}

func (h *recordingDeliveryHook) record(call string) error {
	h.mu.Lock()
	h.calls = append(h.calls, call)
	err := h.err
	h.mu.Unlock()
	return err
}

func (h *recordingDeliveryHook) CreateIntent(context.Context, foreign.DeliveryIntent) error {
	return h.record("intent")
}

func (h *recordingDeliveryHook) Reserve(_ context.Context, reservation foreign.DeliveryReservation) error {
	return h.recordReservation("reserve", reservation)
}

func (h *recordingDeliveryHook) recordReservation(call string, reservation foreign.DeliveryReservation) error {
	h.mu.Lock()
	h.calls = append(h.calls, call)
	h.reservations = append(h.reservations, reservation.RequestID)
	err := h.err
	h.mu.Unlock()
	return err
}

func (h *recordingDeliveryHook) QueueFallback(_ context.Context, fallback foreign.DeliveryFallback) error {
	h.mu.Lock()
	h.calls = append(h.calls, "fallback")
	h.fallbacks = append(h.fallbacks, fallback.RequestID)
	err := h.err
	h.mu.Unlock()
	return err
}

func (h *recordingDeliveryHook) Resolve(_ context.Context, resolution foreign.DeliveryResolution) error {
	h.mu.Lock()
	h.resolutions = append(h.resolutions, resolution)
	h.mu.Unlock()
	if resolution.State == foreign.DeliveryResolutionInjected {
		return h.record("resolve-injected")
	}
	return h.record(string(resolution.State))
}

func (h *recordingDeliveryHook) fallbackIDs() []uuid.UUID {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]uuid.UUID(nil), h.fallbacks...)
}

func (h *recordingDeliveryHook) snapshot() []string {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]string(nil), h.calls...)
}

func TestSteeringInjectionPublishesFoldBeforeDurableResolution(t *testing.T) {
	stream := &gatedSteerStream{observations: make(chan driver.Observation)}
	agent := &gatedSteerAgent{
		stream:      stream,
		steerStart:  make(chan struct{}),
		steerReturn: make(chan struct{}),
		steerResult: driver.SteerResult{
			Outcome:          driver.SteerOutcomeInjected,
			WriteAdmitted:    true,
			ReceiveSequence:  2,
			ResponseSequence: 2,
		},
	}
	hook := &recordingDeliveryHook{}
	pub := &fakePublisher{}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	state, _, err := newWithServices(ctx, mustID(t), mustID(t), loop.Provenance{}, pub,
		validBoundDefinition(), Config{Agent: agent, Cwd: t.TempDir(), SIDMode: SIDPrebound},
		seqIDGen(), workingFac(), foreign.NewServices(foreign.BrokerDescriptor{}, hook))
	if err != nil {
		t.Fatalf("newWithServices: %v", err)
	}

	activeID := submit(t, state, "active")
	waitFor(t, pub, func(input event.Event) bool {
		started, ok := input.(event.TurnStarted)
		return ok && started.Cause.CommandID == activeID
	})

	requestID := mustID(t)
	accepted := make(chan error, 1)
	state.Commands <- commandUserInput(requestID, state.loopID, accepted, "steer")
	if err := <-accepted; err != nil {
		t.Fatalf("steering request acceptance: %v", err)
	}
	select {
	case <-agent.steerStart:
	case <-time.After(time.Second):
		t.Fatal("steering call did not start")
	}

	stream.observations <- driver.SteerObservation{SteerResult: agent.steerResult}
	close(agent.steerReturn)
	stream.observations <- driver.PromptObservation{StopReason: "end_turn", ReceiveSequence: 3, ResponseSequence: 3}
	close(stream.observations)

	waitTurnIndex(t, state, 1)
	wantEvents := []string{"event.TurnStarted", "event.DelegateRequestAccepted", "event.TurnFoldedInto", "event.TurnDone"}
	if got := eventKinds(pub.snapshot()); !reflect.DeepEqual(got, wantEvents) {
		t.Fatalf("events = %v, want %v", got, wantEvents)
	}
	if got, want := hook.snapshot(), []string{"reserve", "resolve-injected"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("delivery calls = %v, want %v", got, want)
	}
	folded := pub.snapshot()[2].(event.TurnFoldedInto)
	if folded.Cause.CommandID != requestID || folded.TurnID.IsZero() {
		t.Fatalf("fold = %+v, want request %v on active turn", folded, requestID)
	}
	shutdown(t, state)
}

type steeringSignalPublisher struct {
	mu      sync.Mutex
	events  []event.Event
	checked []event.Event
	signals chan event.Event
}

func (p *steeringSignalPublisher) PublishEvent(_ context.Context, input event.Event) error {
	p.mu.Lock()
	p.events = append(p.events, input)
	p.mu.Unlock()
	select {
	case p.signals <- input:
	default:
	}
	return nil
}

func (p *steeringSignalPublisher) PublishEventChecked(ctx context.Context, input event.Event) error {
	p.mu.Lock()
	p.checked = append(p.checked, input)
	p.mu.Unlock()
	return p.PublishEvent(ctx, input)
}

func awaitSteeringEvent(t *testing.T, signals <-chan event.Event, predicate func(event.Event) bool) event.Event {
	t.Helper()
	for {
		select {
		case input := <-signals:
			if predicate(input) {
				return input
			}
		case <-time.After(time.Second):
			t.Fatal("expected steering lifecycle event did not arrive")
		}
	}
}

func TestSteeringIdleAndRestoredNormalAdmissionDoesNotCallSteer(t *testing.T) {
	stream := &gatedSteerStream{observations: make(chan driver.Observation, 1)}
	stream.observations <- driver.PromptObservation{StopReason: "end_turn", ReceiveSequence: 1, ResponseSequence: 1}
	close(stream.observations)
	agent := &gatedSteerAgent{
		stream:      stream,
		steerStart:  make(chan struct{}, 1),
		steerReturn: make(chan struct{}),
		steerResult: injectedSteerResult(),
	}
	hook := &recordingDeliveryHook{}
	pub := &steeringSignalPublisher{signals: make(chan event.Event, 16)}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	state, _, err := newWithServices(ctx, mustID(t), mustID(t), loop.Provenance{}, pub,
		validBoundDefinition(), Config{Agent: agent, Cwd: t.TempDir(), SIDMode: SIDPrebound},
		seqIDGen(), workingFac(), foreign.NewServices(foreign.BrokerDescriptor{}, hook))
	if err != nil {
		t.Fatalf("newWithServices: %v", err)
	}
	requestID := mustID(t)
	accepted := make(chan error, 1)
	state.Commands <- commandUserInput(requestID, state.loopID, accepted, "restored normal")
	if err := <-accepted; err != nil {
		t.Fatalf("normal admission: %v", err)
	}
	awaitSteeringEvent(t, pub.signals, func(input event.Event) bool {
		started, ok := input.(event.TurnStarted)
		return ok && started.Cause.CommandID == requestID
	})
	awaitSteeringEvent(t, pub.signals, func(input event.Event) bool {
		_, ok := input.(event.TurnDone)
		return ok
	})
	select {
	case <-agent.steerStart:
		t.Fatal("idle/restored request invoked steering")
	default:
	}
	if got := hook.snapshot(); len(got) != 0 {
		t.Fatalf("idle/restored delivery calls = %v, want none", got)
	}
	shutdown(t, state)
}

type scriptedSteerer struct {
	started chan struct{}
	results chan scriptedSteerResult
}

type scriptedSteerResult struct {
	result driver.SteerResult
	err    error
}

func (s *scriptedSteerer) Steer(ctx context.Context, _ driver.SteerRequest) (driver.SteerResult, error) {
	select {
	case s.started <- struct{}{}:
	default:
	}
	select {
	case output := <-s.results:
		return output.result, output.err
	case <-ctx.Done():
		return driver.SteerResult{}, ctx.Err()
	}
}

func (s *scriptedSteerer) Spawn(context.Context, driver.Turn) (driver.Stream, error) {
	return nil, errors.New("scripted steerer is not a turn agent")
}

func newSteeringUnit(t *testing.T, agent driver.Agent, hook *recordingDeliveryHook) (*Loop, *steeringMachine, *fakePublisher, context.CancelFunc) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	pub := &fakePublisher{}
	l := &Loop{
		sessionID: mustID(t),
		loopID:    mustID(t),
		pub:       pub,
		fac:       workingFac(),
		backendCfg: Config{
			Agent: agent,
		},
		services: foreign.Services{Delivery: hook},
	}
	m := newSteeringMachine(ctx, l, func(event.Event) {}, 1, mustID(t), mustID(t), nil)
	return l, m, pub, cancel
}

func unitPreparedInput(t *testing.T, l *Loop, text string) preparedInput {
	t.Helper()
	id := mustID(t)
	return preparedInput{
		command: command.UserInput{
			Header: command.Header{
				Cause: identity.Cause{CommandID: id, Agency: identity.AgencyMachine},
			},
			Blocks:                []content.Block{&content.TextBlock{Text: text}},
			TargetLoopID:          l.loopID,
			DelegateDeliveryPhase: command.DelegateDeliveryPhaseIntent,
		},
		turnID: mustID(t),
		stepID: mustID(t),
	}
}

func orderedUnitStream() driver.Stream {
	return &orderedObservationStream{observations: make(chan driver.Observation)}
}

func takeCompletion(t *testing.T, m *steeringMachine) steeringCompletion {
	t.Helper()
	select {
	case completion := <-m.completions:
		return completion
	case <-time.After(time.Second):
		t.Fatal("steering completion did not arrive")
		return steeringCompletion{}
	}
}

func awaitStarted(t *testing.T, started <-chan struct{}) {
	t.Helper()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("steering call did not start")
	}
}

func injectedSteerResult() driver.SteerResult {
	return driver.SteerResult{
		Outcome:          driver.SteerOutcomeInjected,
		WriteAdmitted:    true,
		ReceiveSequence:  1,
		ResponseSequence: 1,
	}
}

func fallbackSteerResult() driver.SteerResult {
	return driver.SteerResult{Outcome: driver.SteerOutcomeFallbackRequired}
}

func TestSteeringUnsupportedQueuesWithoutReservation(t *testing.T) {
	hook := &recordingDeliveryHook{}
	legacy := &orderedObservationAgent{stream: orderedUnitStream()}
	l, m, _, cancel := newSteeringUnit(t, legacyAgentAdapter{Agent: legacy}, hook)
	t.Cleanup(cancel)
	input := unitPreparedInput(t, l, "unsupported")
	if handled, err := m.offer(input); !handled || err != nil {
		t.Fatalf("offer unsupported = handled %t err %v, want durable fallback", handled, err)
	}
	if got, want := hook.snapshot(), []string{"fallback"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("unsupported delivery calls = %v, want %v", got, want)
	}
	if len(l.pending) != 1 {
		t.Fatalf("unsupported pending length = %d, want 1", len(l.pending))
	}
}

// legacyAgentAdapter deliberately exposes only driver.Agent to the machine;
// this protects the optional capability boundary in the unsupported path.
type legacyAgentAdapter struct{ driver.Agent }

func TestSteeringProviderUnsupportedQueuesExactlyOnce(t *testing.T) {
	steerer := &scriptedSteerer{started: make(chan struct{}, 1), results: make(chan scriptedSteerResult, 1)}
	hook := &recordingDeliveryHook{}
	l, m, _, cancel := newSteeringUnit(t, steerer, hook)
	t.Cleanup(cancel)
	if err := m.setStream(orderedUnitStream()); err != nil {
		t.Fatalf("set stream: %v", err)
	}
	input := unitPreparedInput(t, l, "provider unsupported")
	if _, err := m.offer(input); err != nil {
		t.Fatalf("offer: %v", err)
	}
	awaitStarted(t, steerer.started)
	result := driver.SteerResult{Outcome: driver.SteerOutcomeUnsupported}
	if err := m.observe(driver.SteerObservation{SteerResult: result}); err != nil {
		t.Fatalf("unsupported acknowledgement: %v", err)
	}
	steerer.results <- scriptedSteerResult{result: result}
	if err := m.complete(takeCompletion(t, m)); err != nil {
		t.Fatalf("complete: %v", err)
	}
	if got, want := hook.snapshot(), []string{"reserve", "fallback"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("provider unsupported calls = %v, want %v", got, want)
	}
	if got := hook.fallbackIDs(); len(got) != 1 || got[0] != input.command.CommandID {
		t.Fatalf("provider unsupported fallback IDs = %v, want %v", got, input.command.CommandID)
	}
}

func TestSteeringExplicitFallbackQueuesExactlyOnce(t *testing.T) {
	steerer := &scriptedSteerer{started: make(chan struct{}, 1), results: make(chan scriptedSteerResult, 1)}
	hook := &recordingDeliveryHook{}
	l, m, _, cancel := newSteeringUnit(t, steerer, hook)
	t.Cleanup(cancel)
	if err := m.setStream(orderedUnitStream()); err != nil {
		t.Fatalf("set stream: %v", err)
	}
	input := unitPreparedInput(t, l, "fallback")
	if handled, err := m.offer(input); !handled || err != nil {
		t.Fatalf("offer fallback = handled %t err %v", handled, err)
	}
	awaitStarted(t, steerer.started)
	result := fallbackSteerResult()
	if err := m.observe(driver.SteerObservation{SteerResult: result}); err != nil {
		t.Fatalf("observe fallback: %v", err)
	}
	steerer.results <- scriptedSteerResult{result: result}
	if err := m.complete(takeCompletion(t, m)); err != nil {
		t.Fatalf("complete fallback: %v", err)
	}
	if got, want := hook.snapshot(), []string{"reserve", "fallback"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("fallback calls = %v, want %v", got, want)
	}
	if got := hook.fallbackIDs(); len(got) != 1 || got[0] != input.command.CommandID {
		t.Fatalf("fallback IDs = %v, want one request %v", got, input.command.CommandID)
	}
	if len(l.pending) != 1 {
		t.Fatalf("normal pending length = %d, want 1", len(l.pending))
	}
	if err := m.observe(driver.SteerObservation{SteerResult: injectedSteerResult()}); err != nil {
		t.Fatalf("late fallback acknowledgement: %v", err)
	}
	if err := m.resolvePendingBeforeTerminal(); err != nil {
		t.Fatalf("repeat terminal adjudication: %v", err)
	}
	if got := hook.fallbackIDs(); len(got) != 1 {
		t.Fatalf("fallback repeated after terminal = %v", got)
	}
}

func TestSteeringTerminalBeforeKnownWriterFallbackWaitsForExplicitNonDelivery(t *testing.T) {
	steerer := &scriptedSteerer{started: make(chan struct{}, 1), results: make(chan scriptedSteerResult, 1)}
	hook := &recordingDeliveryHook{}
	l, m, _, cancel := newSteeringUnit(t, steerer, hook)
	t.Cleanup(cancel)
	if err := m.setStream(orderedUnitStream()); err != nil {
		t.Fatalf("set stream: %v", err)
	}
	input := unitPreparedInput(t, l, "terminal race")
	if _, err := m.offer(input); err != nil {
		t.Fatalf("offer: %v", err)
	}
	awaitStarted(t, steerer.started)
	if err := m.observe(driver.PromptObservation{StopReason: "end_turn"}); err != nil {
		t.Fatalf("prompt observation: %v", err)
	}
	if m.terminalReady() {
		t.Fatal("terminal released without steering acknowledgement")
	}
	result := fallbackSteerResult()
	if err := m.observe(driver.SteerObservation{SteerResult: result}); err != nil {
		t.Fatalf("fallback acknowledgement: %v", err)
	}
	steerer.results <- scriptedSteerResult{result: result}
	if err := m.complete(takeCompletion(t, m)); err != nil {
		t.Fatalf("complete: %v", err)
	}
	if !m.terminalReady() {
		t.Fatal("terminal remained held after proven pre-writer fallback")
	}
	if got := hook.fallbackIDs(); len(got) != 1 || got[0] != input.command.CommandID {
		t.Fatalf("fallback IDs = %v, want one %v", got, input.command.CommandID)
	}
}

func TestSteeringPostWriterTimeoutIsUnknownAndDisables(t *testing.T) {
	steerer := &scriptedSteerer{started: make(chan struct{}, 1), results: make(chan scriptedSteerResult, 1)}
	hook := &recordingDeliveryHook{}
	l, m, _, cancel := newSteeringUnit(t, steerer, hook)
	t.Cleanup(cancel)
	if err := m.setStream(orderedUnitStream()); err != nil {
		t.Fatalf("set stream: %v", err)
	}
	input := unitPreparedInput(t, l, "ambiguous")
	if _, err := m.offer(input); err != nil {
		t.Fatalf("offer: %v", err)
	}
	awaitStarted(t, steerer.started)
	m.active.writerAdmitted = true
	if err := m.observe(driver.PromptObservation{StopReason: "end_turn"}); err != nil {
		t.Fatalf("prompt observation: %v", err)
	}
	if m.terminalReady() {
		t.Fatal("post-writer terminal released before bounded timeout")
	}
	if err := m.timeout(); err != nil {
		t.Fatalf("timeout: %v", err)
	}
	if !m.disabled {
		t.Fatal("steering remained enabled after ambiguity")
	}
	if m.terminalReady() == false {
		t.Fatal("terminal remained held after ambiguity resolution")
	}
	if got := hook.snapshot(); !reflect.DeepEqual(got, []string{"reserve", "unknown"}) {
		t.Fatalf("post-writer calls = %v, want reserve/unknown", got)
	}
	if got := hook.fallbackIDs(); len(got) != 0 {
		t.Fatalf("ambiguous request was retried as fallback: %v", got)
	}
	future := unitPreparedInput(t, l, "disabled fallback")
	if handled, err := m.offer(future); !handled || err != nil {
		t.Fatalf("offer after disable = handled %t err %v", handled, err)
	}
	if got := hook.fallbackIDs(); len(got) != 1 || got[0] != future.command.CommandID {
		t.Fatalf("post-disable fallback IDs = %v, want one %v", got, future.command.CommandID)
	}
	if err := m.complete(takeCompletion(t, m)); err != nil {
		t.Fatalf("late cancelled completion: %v", err)
	}
}

func TestSteeringTimeoutBeforeTerminalDoesNotAdjudicateDelayedPromptRequired(t *testing.T) {
	steerer := &scriptedSteerer{started: make(chan struct{}, 1), results: make(chan scriptedSteerResult, 1)}
	hook := &recordingDeliveryHook{}
	l, m, _, cancel := newSteeringUnit(t, steerer, hook)
	t.Cleanup(cancel)
	if err := m.setStream(orderedUnitStream()); err != nil {
		t.Fatalf("set stream: %v", err)
	}
	input := unitPreparedInput(t, l, "delayed promptRequired")
	if _, err := m.offer(input); err != nil {
		t.Fatalf("offer: %v", err)
	}
	awaitStarted(t, steerer.started)

	// A provider acknowledgement may be delayed by wire scheduling while the
	// active prompt is still running. The terminal-race timeout must not resolve
	// that request as unknown before a terminal boundary exists.
	if err := m.timeout(); err != nil {
		t.Fatalf("pre-terminal timeout: %v", err)
	}
	if m.active == nil || m.active.resolved {
		t.Fatal("pre-terminal timeout resolved the active steering attempt")
	}

	result := fallbackSteerResult()
	if err := m.observe(driver.SteerObservation{SteerResult: result}); err != nil {
		t.Fatalf("delayed fallback acknowledgement: %v", err)
	}
	steerer.results <- scriptedSteerResult{result: result}
	if err := m.complete(takeCompletion(t, m)); err != nil {
		t.Fatalf("complete delayed fallback: %v", err)
	}
	if got, want := hook.snapshot(), []string{"reserve", "fallback"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("delayed promptRequired calls = %v, want %v", got, want)
	}
}

func TestSteeringAdmissionUnknownIsNonRetryable(t *testing.T) {
	steerer := &scriptedSteerer{started: make(chan struct{}, 1), results: make(chan scriptedSteerResult, 1)}
	hook := &recordingDeliveryHook{}
	l, m, _, cancel := newSteeringUnit(t, steerer, hook)
	t.Cleanup(cancel)
	if err := m.setStream(orderedUnitStream()); err != nil {
		t.Fatalf("set stream: %v", err)
	}
	input := unitPreparedInput(t, l, "admission unknown")
	if _, err := m.offer(input); err != nil {
		t.Fatalf("offer: %v", err)
	}
	awaitStarted(t, steerer.started)
	result := driver.SteerResult{Outcome: driver.SteerOutcomeAdmissionUnknown}
	if err := m.observe(driver.SteerObservation{SteerResult: result}); err != nil {
		t.Fatalf("observe admission unknown: %v", err)
	}
	steerer.results <- scriptedSteerResult{result: result}
	if err := m.complete(takeCompletion(t, m)); err != nil {
		t.Fatalf("complete: %v", err)
	}
	if got := hook.snapshot(); !reflect.DeepEqual(got, []string{"reserve", "unknown"}) {
		t.Fatalf("admission-unknown calls = %v, want reserve/unknown", got)
	}
	if got := hook.fallbackIDs(); len(got) != 0 {
		t.Fatalf("admission-unknown retried: %v", got)
	}
}

func TestSteeringUntrackableFaultsWithoutSyntheticTurn(t *testing.T) {
	steerer := &scriptedSteerer{started: make(chan struct{}, 1), results: make(chan scriptedSteerResult, 1)}
	hook := &recordingDeliveryHook{}
	l, m, pub, cancel := newSteeringUnit(t, steerer, hook)
	t.Cleanup(cancel)
	if err := m.setStream(orderedUnitStream()); err != nil {
		t.Fatalf("set stream: %v", err)
	}
	input := unitPreparedInput(t, l, "untrackable")
	if _, err := m.offer(input); err != nil {
		t.Fatalf("offer: %v", err)
	}
	awaitStarted(t, steerer.started)
	result := driver.SteerResult{Outcome: driver.SteerOutcomeDeliveredUntrackable, WriteAdmitted: true, ReceiveSequence: 1, ResponseSequence: 1}
	if err := m.observe(driver.SteerObservation{SteerResult: result}); err == nil {
		t.Fatal("untrackable observation returned nil error")
	}
	if got := hook.snapshot(); !reflect.DeepEqual(got, []string{"reserve", "untrackable"}) {
		t.Fatalf("untrackable calls = %v, want reserve/untrackable", got)
	}
	if len(l.pending) != 0 || len(pub.snapshot()) != 0 {
		t.Fatalf("untrackable synthesized lifecycle: pending=%d events=%v", len(l.pending), eventKinds(pub.snapshot()))
	}
}

func TestSteeringMalformedResponseBecomesUnknownAndLateAckIsIgnored(t *testing.T) {
	steerer := &scriptedSteerer{started: make(chan struct{}, 1), results: make(chan scriptedSteerResult, 1)}
	hook := &recordingDeliveryHook{}
	l, m, pub, cancel := newSteeringUnit(t, steerer, hook)
	t.Cleanup(cancel)
	if err := m.setStream(orderedUnitStream()); err != nil {
		t.Fatalf("set stream: %v", err)
	}
	input := unitPreparedInput(t, l, "malformed")
	if _, err := m.offer(input); err != nil {
		t.Fatalf("offer: %v", err)
	}
	awaitStarted(t, steerer.started)
	malformed := driver.SteerResult{Outcome: driver.SteerOutcomeInjected, WriteAdmitted: true}
	if err := m.observe(driver.SteerObservation{SteerResult: malformed}); err != nil {
		t.Fatalf("malformed observation: %v", err)
	}
	steerer.results <- scriptedSteerResult{result: malformed}
	if err := m.complete(takeCompletion(t, m)); err != nil {
		t.Fatalf("complete malformed: %v", err)
	}
	if got := hook.snapshot(); !reflect.DeepEqual(got, []string{"reserve", "unknown"}) {
		t.Fatalf("malformed calls = %v, want reserve/unknown", got)
	}
	if err := m.observe(driver.SteerObservation{SteerResult: injectedSteerResult()}); err != nil {
		t.Fatalf("late acknowledgement: %v", err)
	}
	if len(pub.snapshot()) != 0 || len(l.pending) != 0 {
		t.Fatalf("late acknowledgement changed lifecycle: events=%v pending=%d", eventKinds(pub.snapshot()), len(l.pending))
	}
}

func TestSteeringFIFOReservesAndResolvesInRequestOrder(t *testing.T) {
	steerer := &scriptedSteerer{started: make(chan struct{}, 2), results: make(chan scriptedSteerResult, 2)}
	hook := &recordingDeliveryHook{}
	l, m, _, cancel := newSteeringUnit(t, steerer, hook)
	t.Cleanup(cancel)
	if err := m.setStream(orderedUnitStream()); err != nil {
		t.Fatalf("set stream: %v", err)
	}
	first, second := unitPreparedInput(t, l, "first"), unitPreparedInput(t, l, "second")
	if _, err := m.offer(first); err != nil {
		t.Fatalf("offer first: %v", err)
	}
	awaitStarted(t, steerer.started)
	if _, err := m.offer(second); err != nil {
		t.Fatalf("offer second: %v", err)
	}
	firstResult := fallbackSteerResult()
	if err := m.observe(driver.SteerObservation{SteerResult: firstResult}); err != nil {
		t.Fatalf("first acknowledgement: %v", err)
	}
	steerer.results <- scriptedSteerResult{result: firstResult}
	if err := m.complete(takeCompletion(t, m)); err != nil {
		t.Fatalf("first completion: %v", err)
	}
	if got, want := hook.snapshot(), []string{"reserve", "fallback", "fallback"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("FIFO calls = %v, want %v", got, want)
	}
	if got := hook.fallbackIDs(); !reflect.DeepEqual(got, []uuid.UUID{first.command.CommandID, second.command.CommandID}) {
		t.Fatalf("FIFO fallback IDs = %v, want [%v %v]", got, first.command.CommandID, second.command.CommandID)
	}
}

func commandUserInput(id, loopID uuid.UUID, accepted chan error, text string) command.UserInput {
	return command.UserInput{
		Header:                command.Header{CommandID: id, Agency: identity.AgencyMachine},
		Blocks:                []content.Block{&content.TextBlock{Text: text}},
		TargetLoopID:          loopID,
		DelegateDeliveryPhase: command.DelegateDeliveryPhaseIntent,
		Accepted:              accepted,
	}
}

var _ driver.Agent = (*gatedSteerAgent)(nil)
var _ driver.Steerer = (*gatedSteerAgent)(nil)
var _ foreign.DeliveryHook = (*recordingDeliveryHook)(nil)
