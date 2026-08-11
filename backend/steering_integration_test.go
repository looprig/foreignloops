package backend_test

import (
	"context"
	"fmt"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/looprig/core/content"
	"github.com/looprig/core/uuid"
	foreignbackend "github.com/looprig/foreignloops/backend"
	"github.com/looprig/foreignloops/driver"
	acpdriver "github.com/looprig/foreignloops/driver/acp"
	"github.com/looprig/foreignloops/internal/steertest"
	"github.com/looprig/harness/pkg/command"
	"github.com/looprig/harness/pkg/event"
	"github.com/looprig/harness/pkg/foreign"
	"github.com/looprig/harness/pkg/identity"
	"github.com/looprig/harness/pkg/loop"
	"github.com/looprig/harness/pkg/tool"
	"github.com/looprig/inference"
	model "github.com/looprig/inference/model"
	inferenceStream "github.com/looprig/inference/stream"
)

type steeringIntegrationPublisher struct {
	mu       sync.Mutex
	all      []event.Event
	checked  []event.Event
	signals  chan event.Event
	checkErr error
}

func newSteeringIntegrationPublisher() *steeringIntegrationPublisher {
	return &steeringIntegrationPublisher{signals: make(chan event.Event, 256)}
}

func (p *steeringIntegrationPublisher) PublishEvent(_ context.Context, input event.Event) error {
	p.mu.Lock()
	p.all = append(p.all, input)
	p.mu.Unlock()
	p.signals <- input
	return nil
}

func (p *steeringIntegrationPublisher) PublishEventChecked(ctx context.Context, input event.Event) error {
	p.mu.Lock()
	p.checked = append(p.checked, input)
	err := p.checkErr
	p.mu.Unlock()
	if err != nil {
		return err
	}
	return p.PublishEvent(ctx, input)
}

func (p *steeringIntegrationPublisher) checkedSnapshot() []event.Event {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]event.Event(nil), p.checked...)
}

func (p *steeringIntegrationPublisher) wait(ctx context.Context, want func(event.Event) bool) event.Event {
	for {
		select {
		case input := <-p.signals:
			if want(input) {
				return input
			}
		case <-ctx.Done():
			checked := p.checkedSnapshot()
			var details []string
			for _, input := range checked {
				if failed, ok := input.(event.TurnFailed); ok {
					details = append(details, fmt.Sprintf("TurnFailed=%v", failed.Err))
				}
			}
			panic("steering integration publisher wait: " + ctx.Err().Error() + " kinds=" + fmt.Sprint(steeringIntegrationEventKinds(checked)) + " details=" + fmt.Sprint(details))
		}
	}
}

type steeringIntegrationDeliveryHook struct {
	mu           sync.Mutex
	calls        []string
	reservations []uuid.UUID
	fallbacks    []uuid.UUID
	resolutions  []foreign.DeliveryResolution
	signals      chan string
}

func steeringIntegrationUUID(t *testing.T) uuid.UUID {
	t.Helper()
	id, err := uuid.New()
	if err != nil {
		t.Fatalf("uuid.New() error = %v", err)
	}
	return id
}

func (h *steeringIntegrationDeliveryHook) signal(call string) {
	if h.signals != nil {
		h.signals <- call
	}
}

func (h *steeringIntegrationDeliveryHook) CreateIntent(context.Context, foreign.DeliveryIntent) error {
	h.mu.Lock()
	h.calls = append(h.calls, "intent")
	h.mu.Unlock()
	h.signal("intent")
	return nil
}

func (h *steeringIntegrationDeliveryHook) Reserve(_ context.Context, reservation foreign.DeliveryReservation) error {
	h.mu.Lock()
	h.calls = append(h.calls, "reserve")
	h.reservations = append(h.reservations, reservation.RequestID)
	h.mu.Unlock()
	h.signal("reserve")
	return nil
}

func (h *steeringIntegrationDeliveryHook) QueueFallback(_ context.Context, fallback foreign.DeliveryFallback) error {
	h.mu.Lock()
	h.calls = append(h.calls, "fallback")
	h.fallbacks = append(h.fallbacks, fallback.RequestID)
	h.mu.Unlock()
	h.signal("fallback")
	return nil
}

func (h *steeringIntegrationDeliveryHook) Resolve(_ context.Context, resolution foreign.DeliveryResolution) error {
	h.mu.Lock()
	h.resolutions = append(h.resolutions, resolution)
	h.calls = append(h.calls, string(resolution.State))
	h.mu.Unlock()
	h.signal(string(resolution.State))
	return nil
}

func (h *steeringIntegrationDeliveryHook) waitCall(ctx context.Context, want string) error {
	for {
		select {
		case got := <-h.signals:
			if got == want {
				return nil
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (h *steeringIntegrationDeliveryHook) snapshot() (calls []string, reservations, fallbacks []uuid.UUID, resolutions []foreign.DeliveryResolution) {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]string(nil), h.calls...), append([]uuid.UUID(nil), h.reservations...), append([]uuid.UUID(nil), h.fallbacks...), append([]foreign.DeliveryResolution(nil), h.resolutions...)
}

func newSteeringIntegrationACP(t *testing.T, ctx context.Context, script steertest.Script, harness acpdriver.Harness) (*acpdriver.Driver, *steertest.Agent) {
	t.Helper()
	fixture := steertest.New(t, script)
	d, err := acpdriver.New(ctx, acpdriver.Config{
		Harness:       harness,
		Executable:    fixture.Executable(),
		Env:           fixture.Env(),
		Credential:    loop.CredentialNativeAuth,
		Posture:       driver.PostureReadOnly,
		WorkspaceRoot: t.TempDir(),
	})
	if err != nil {
		t.Fatalf("acp.New() error = %v", err)
	}
	t.Cleanup(func() { _ = d.Close() })
	return d, fixture
}

func sendSteeringIntegrationInput(t *testing.T, ctx context.Context, state loop.Backend, loopID uuid.UUID, id uuid.UUID, text string, accepted bool) error {
	t.Helper()
	var acceptedCh chan error
	if accepted {
		acceptedCh = make(chan error, 1)
	}
	input := command.UserInput{
		Header:                command.Header{CommandID: id, Agency: identity.AgencyMachine},
		Blocks:                []content.Block{&content.TextBlock{Text: text}},
		TargetLoopID:          loopID,
		DelegateDeliveryPhase: command.DelegateDeliveryPhaseIntent,
		Accepted:              acceptedCh,
	}
	select {
	case state.CommandSink() <- input:
	case <-ctx.Done():
		return ctx.Err()
	}
	if acceptedCh == nil {
		return nil
	}
	select {
	case err := <-acceptedCh:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func waitIntegrationGate(t *testing.T, ctx context.Context, fixture *steertest.Agent, occurrence int, want string) {
	t.Helper()
	got, err := fixture.WaitForNth(ctx, steertest.EventGate, occurrence)
	if err != nil {
		t.Fatalf("gate[%d] = %v", occurrence, err)
	}
	if got.Gate != want {
		t.Fatalf("gate[%d] = %q, want %q", occurrence, got.Gate, want)
	}
}

func waitIntegrationSteerOutcome(t *testing.T, ctx context.Context, fixture *steertest.Agent, callIndex int, want steertest.SteeringOutcome) {
	t.Helper()
	// Each extension call emits one request observation and one response
	// observation in the fixture transcript.
	got, err := fixture.WaitForNth(ctx, steertest.EventSteer, callIndex*2+1)
	if err != nil {
		t.Fatalf("steer[%d] = %v", callIndex, err)
	}
	if got.Outcome != want {
		t.Fatalf("steer[%d] outcome = %q, want %q", callIndex, got.Outcome, want)
	}
}

type steeringIntegrationInferenceClient struct{}

func (steeringIntegrationInferenceClient) Invoke(context.Context, inference.Request) (*inference.Response, error) {
	return nil, fmt.Errorf("unused")
}

func (steeringIntegrationInferenceClient) Stream(context.Context, inference.Request) (*inferenceStream.StreamReader[content.Chunk], error) {
	return nil, fmt.Errorf("unused")
}

func steeringIntegrationBoundDefinition(t *testing.T, sessionID, loopID uuid.UUID) loop.BoundDefinition {
	t.Helper()
	definition, err := loop.Define(
		loop.WithName("agent"),
		loop.WithInference(steeringIntegrationInferenceClient{}, model.Model{
			Provider: "fixture", APIFormat: model.APIFormatOpenAI, BaseURL: "http://127.0.0.1", Name: "fixture",
		}),
		loop.WithSystem("fixture system"),
	)
	if err != nil {
		t.Fatalf("loop.Define() error = %v", err)
	}
	bound, err := definition.Bind(context.Background(), tool.Bindings{SessionID: sessionID, LoopID: loopID})
	if err != nil {
		t.Fatalf("definition.Bind() error = %v", err)
	}
	return bound
}

func steeringIntegrationIDGen() func() (uuid.UUID, error) {
	var next byte
	return func() (uuid.UUID, error) {
		next++
		var id uuid.UUID
		id[15] = next
		return id, nil
	}
}

func steeringIntegrationEventKinds(events []event.Event) []string {
	out := make([]string, len(events))
	for i, input := range events {
		out[i] = fmt.Sprintf("%T", input)
	}
	return out
}

func steeringIntegrationCountEvents(fixture *steertest.Agent, kind steertest.EventKind) int {
	count := 0
	for _, input := range fixture.Transcript().Records {
		if input.Kind == kind {
			count++
		}
	}
	return count
}

func shutdownSteeringIntegration(t *testing.T, state loop.Backend, ctx context.Context) {
	t.Helper()
	ack := make(chan error, 1)
	select {
	case state.CommandSink() <- command.Shutdown{Ack: ack}:
	case <-state.DoneChan():
		return
	case <-ctx.Done():
		t.Fatalf("shutdown send: %v", ctx.Err())
	}
	select {
	case err := <-ack:
		if err != nil {
			t.Fatalf("shutdown: %v", err)
		}
	case <-state.DoneChan():
		return
	case <-ctx.Done():
		t.Fatalf("shutdown ack: %v", ctx.Err())
	}
	select {
	case <-state.DoneChan():
	case <-ctx.Done():
		t.Fatalf("shutdown completion: %v", ctx.Err())
	}
}

func assertSteeringIntegrationChecked(t *testing.T, pub *steeringIntegrationPublisher, want []string) {
	t.Helper()
	if got := steeringIntegrationEventKinds(pub.checkedSnapshot()); !reflect.DeepEqual(got, want) {
		t.Fatalf("checked events = %v, want %v", got, want)
	}
}

type steeringIntegrationCountingAgent struct {
	*acpdriver.Driver
	mu    sync.Mutex
	calls int
}

func (a *steeringIntegrationCountingAgent) Steer(ctx context.Context, request driver.SteerRequest) (driver.SteerResult, error) {
	a.mu.Lock()
	a.calls++
	a.mu.Unlock()
	return a.Driver.Steer(ctx, request)
}

func (a *steeringIntegrationCountingAgent) callCount() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.calls
}

type steeringIntegrationResultAgent struct {
	*acpdriver.Driver
	result driver.SteerResult
	err    error
	called chan struct{}
}

func (a *steeringIntegrationResultAgent) Steer(context.Context, driver.SteerRequest) (driver.SteerResult, error) {
	select {
	case a.called <- struct{}{}:
	default:
	}
	return a.result, a.err
}

type steeringIntegrationLateAckAgent struct {
	*acpdriver.Driver
	started chan struct{}
	done    chan struct{}
}

func (a *steeringIntegrationLateAckAgent) Steer(_ context.Context, request driver.SteerRequest) (driver.SteerResult, error) {
	select {
	case a.started <- struct{}{}:
	default:
	}
	go func() {
		_, _ = a.Driver.Steer(context.Background(), request)
		close(a.done)
	}()
	// Deliberately malformed admitted success: the actor must classify the
	// caller-facing result as ambiguous while the real ACP response arrives
	// later through the ordered fixture stream.
	return driver.SteerResult{Outcome: driver.SteerOutcomeInjected, WriteAdmitted: true}, nil
}

func steeringIntegrationScript(outcome steertest.SteeringOutcome) steertest.Script {
	script := steertest.DefaultScript()
	script.Prompts = []steertest.PromptScript{{Actions: []steertest.Action{
		{Kind: steertest.ActionTerminal, Gate: "active-terminal"},
	}}}
	script.Steers = []steertest.SteerScript{{Actions: []steertest.Action{
		{Kind: steertest.ActionSteerReply, Outcome: outcome, Gate: "steer-ack"},
	}}}
	return script
}

func buildSteeringIntegrationBackend(t *testing.T, ctx context.Context, agent driver.Agent, hook *steeringIntegrationDeliveryHook, pub *steeringIntegrationPublisher) (loop.Backend, uuid.UUID) {
	t.Helper()
	sessionID, loopID := steeringIntegrationUUID(t), steeringIntegrationUUID(t)
	idGen := steeringIntegrationIDGen()
	state, _, err := foreignbackend.BuildWithServices(foreignbackend.Config{Agent: agent, Cwd: t.TempDir(), SIDMode: foreignbackend.SIDPrebound})(
		ctx, sessionID, loopID, loop.Provenance{}, pub, steeringIntegrationBoundDefinition(t, sessionID, loopID),
		idGen, event.NewFactory(idGen, time.Now), foreign.NewServices(foreign.BrokerDescriptor{}, hook))
	if err != nil {
		t.Fatalf("BuildWithServices() error = %v", err)
	}
	t.Cleanup(func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		shutdownSteeringIntegration(t, state, shutdownCtx)
	})
	return state, loopID
}

func TestSteeringIntegrationInjectedFoldCheckedBeforeTerminal(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	t.Cleanup(cancel)
	script := steertest.DefaultScript()
	script.Prompts = []steertest.PromptScript{{Actions: []steertest.Action{
		{Kind: steertest.ActionTerminal, Gate: "active-terminal"},
	}}}
	script.Steers = []steertest.SteerScript{{Actions: []steertest.Action{
		{Kind: steertest.ActionSteerReply, Outcome: steertest.OutcomeInjected, Gate: "steer-ack"},
	}}}
	agent, fixture := newSteeringIntegrationACP(t, ctx, script, acpdriver.HarnessClaudeCode)
	hook := &steeringIntegrationDeliveryHook{}
	pub := newSteeringIntegrationPublisher()
	sessionID, loopID := steeringIntegrationUUID(t), steeringIntegrationUUID(t)
	idGen := steeringIntegrationIDGen()
	state, _, err := foreignbackend.BuildWithServices(foreignbackend.Config{Agent: agent, Cwd: t.TempDir(), SIDMode: foreignbackend.SIDPrebound})(
		ctx, sessionID, loopID, loop.Provenance{}, pub, steeringIntegrationBoundDefinition(t, sessionID, loopID),
		idGen, event.NewFactory(idGen, time.Now), foreign.NewServices(foreign.BrokerDescriptor{}, hook))
	if err != nil {
		t.Fatalf("BuildWithServices() error = %v", err)
	}
	t.Cleanup(func() { shutdownSteeringIntegration(t, state, ctx) })

	activeID := steeringIntegrationUUID(t)
	if err := sendSteeringIntegrationInput(t, ctx, state, loopID, activeID, "active", false); err != nil {
		t.Fatalf("active input: %v", err)
	}
	pub.wait(ctx, func(input event.Event) bool {
		started, ok := input.(event.TurnStarted)
		return ok && started.Cause.CommandID == activeID
	})
	waitIntegrationGate(t, ctx, fixture, 0, "active-terminal")

	requestID := steeringIntegrationUUID(t)
	if err := sendSteeringIntegrationInput(t, ctx, state, loopID, requestID, "inject", true); err != nil {
		t.Fatalf("steering input: %v", err)
	}
	waitIntegrationGate(t, ctx, fixture, 1, "steer-ack")
	if err := fixture.Release("steer-ack"); err != nil {
		t.Fatalf("release steering acknowledgement: %v", err)
	}
	waitIntegrationSteerOutcome(t, ctx, fixture, 0, steertest.OutcomeInjected)
	pub.wait(ctx, func(input event.Event) bool { _, ok := input.(event.TurnFoldedInto); return ok })
	if err := fixture.Release("active-terminal"); err != nil {
		t.Fatalf("release active terminal: %v", err)
	}
	fixtureTerminal := fixture.WaitForKind(ctx, steertest.EventTerminal)
	if fixtureTerminal.Err != nil {
		t.Fatalf("fixture terminal: %v", fixtureTerminal.Err)
	}
	pub.wait(ctx, func(input event.Event) bool { _, ok := input.(event.TurnDone); return ok })

	checked := steeringIntegrationEventKinds(pub.checkedSnapshot())
	wantChecked := []string{
		"event.TurnStarted",
		"event.DelegateRequestAccepted",
		"event.TurnFoldedInto",
		"event.TurnDone",
	}
	if !reflect.DeepEqual(checked, wantChecked) {
		t.Fatalf("checked events = %v, want %v", checked, wantChecked)
	}
	calls, reservations, fallbacks, resolutions := hook.snapshot()
	if !reflect.DeepEqual(calls, []string{"reserve", string(foreign.DeliveryResolutionInjected)}) {
		t.Fatalf("delivery calls = %v, want reserve/resolve-injected", calls)
	}
	if len(reservations) != 1 || reservations[0] != requestID {
		t.Fatalf("reservations = %v, want [%s]", reservations, requestID)
	}
	if len(fallbacks) != 0 || len(resolutions) != 1 || resolutions[0].State != foreign.DeliveryResolutionInjected {
		t.Fatalf("fallbacks/resolutions = %v/%v, want no fallback and one injected resolution", fallbacks, resolutions)
	}
}

func TestSteeringIntegrationPromptRequiredQueuesExactlyOneFallback(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	t.Cleanup(cancel)
	script := steertest.DefaultScript()
	script.Prompts = []steertest.PromptScript{
		{Actions: []steertest.Action{{Kind: steertest.ActionTerminal, Gate: "active-terminal"}}},
		{Actions: []steertest.Action{{Kind: steertest.ActionTerminal, Gate: "fallback-terminal"}}},
	}
	script.Steers = []steertest.SteerScript{{Actions: []steertest.Action{
		{Kind: steertest.ActionSteerReply, Outcome: steertest.OutcomePromptRequired, Gate: "steer-ack"},
	}}}
	base, fixture := newSteeringIntegrationACP(t, ctx, script, acpdriver.HarnessClaudeCode)
	counting := &steeringIntegrationCountingAgent{Driver: base}
	hook := &steeringIntegrationDeliveryHook{}
	pub := newSteeringIntegrationPublisher()
	state, loopID := buildSteeringIntegrationBackend(t, ctx, counting, hook, pub)
	activeID, requestID := steeringIntegrationUUID(t), steeringIntegrationUUID(t)
	if err := sendSteeringIntegrationInput(t, ctx, state, loopID, activeID, "active", false); err != nil {
		t.Fatalf("active input: %v", err)
	}
	pub.wait(ctx, func(input event.Event) bool {
		started, ok := input.(event.TurnStarted)
		return ok && started.Cause.CommandID == activeID
	})
	waitIntegrationGate(t, ctx, fixture, 0, "active-terminal")
	if err := sendSteeringIntegrationInput(t, ctx, state, loopID, requestID, "fallback", true); err != nil {
		t.Fatalf("steering input: %v", err)
	}
	waitIntegrationGate(t, ctx, fixture, 1, "steer-ack")
	if err := fixture.Release("steer-ack"); err != nil {
		t.Fatalf("release steering acknowledgement: %v", err)
	}
	waitIntegrationSteerOutcome(t, ctx, fixture, 0, steertest.OutcomePromptRequired)
	if err := fixture.Release("active-terminal"); err != nil {
		t.Fatalf("release active terminal: %v", err)
	}
	if _, err := fixture.WaitForNth(ctx, steertest.EventTerminal, 0); err != nil {
		t.Fatalf("active terminal: %v", err)
	}
	waitIntegrationGate(t, ctx, fixture, 2, "fallback-terminal")
	if err := fixture.Release("fallback-terminal"); err != nil {
		t.Fatalf("release fallback terminal: %v", err)
	}
	if _, err := fixture.WaitForNth(ctx, steertest.EventTerminal, 1); err != nil {
		t.Fatalf("fallback terminal: %v", err)
	}
	pub.wait(ctx, func(input event.Event) bool { _, ok := input.(event.TurnDone); return ok })
	pub.wait(ctx, func(input event.Event) bool { _, ok := input.(event.TurnDone); return ok })

	assertSteeringIntegrationChecked(t, pub, []string{
		"event.TurnStarted",
		"event.DelegateRequestAccepted",
		"event.TurnDone",
		"event.TurnStarted",
		"event.TurnDone",
	})
	calls, reservations, fallbacks, resolutions := hook.snapshot()
	if !reflect.DeepEqual(calls, []string{"reserve", "fallback"}) {
		t.Fatalf("delivery calls = %v, want reserve/fallback", calls)
	}
	if len(reservations) != 1 || reservations[0] != requestID {
		t.Fatalf("reservations = %v, want [%s]", reservations, requestID)
	}
	if len(fallbacks) != 1 || fallbacks[0] != requestID || len(resolutions) != 0 {
		t.Fatalf("fallbacks/resolutions = %v/%v, want one fallback and no resolution", fallbacks, resolutions)
	}
	if got := counting.callCount(); got != 1 {
		t.Fatalf("ACP steering calls = %d, want 1", got)
	}
	if got := steeringIntegrationCountEvents(fixture, steertest.EventSteer); got != 2 {
		t.Fatalf("fixture steer records = %d, want exactly 2", got)
	}
}

func TestSteeringIntegrationFallbackFIFOAcrossTurnsDoesNotSteerBehindOlderPending(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	t.Cleanup(cancel)
	script := steertest.DefaultScript()
	script.Prompts = []steertest.PromptScript{
		{Actions: []steertest.Action{{Kind: steertest.ActionTerminal, Gate: "active-terminal"}}},
		{Actions: []steertest.Action{{Kind: steertest.ActionTerminal, Gate: "fallback-one"}}},
		{Actions: []steertest.Action{{Kind: steertest.ActionTerminal, Gate: "fallback-two"}}},
		{Actions: []steertest.Action{{Kind: steertest.ActionTerminal, Gate: "fallback-three"}}},
	}
	script.Steers = []steertest.SteerScript{{Actions: []steertest.Action{
		{Kind: steertest.ActionSteerReply, Outcome: steertest.OutcomePromptRequired, Gate: "steer-ack"},
	}}}
	base, fixture := newSteeringIntegrationACP(t, ctx, script, acpdriver.HarnessClaudeCode)
	counting := &steeringIntegrationCountingAgent{Driver: base}
	hook := &steeringIntegrationDeliveryHook{}
	pub := newSteeringIntegrationPublisher()
	state, loopID := buildSteeringIntegrationBackend(t, ctx, counting, hook, pub)
	activeID, firstID, secondID, thirdID := steeringIntegrationUUID(t), steeringIntegrationUUID(t), steeringIntegrationUUID(t), steeringIntegrationUUID(t)

	if err := sendSteeringIntegrationInput(t, ctx, state, loopID, activeID, "active", false); err != nil {
		t.Fatalf("active input: %v", err)
	}
	pub.wait(ctx, func(input event.Event) bool {
		started, ok := input.(event.TurnStarted)
		return ok && started.Cause.CommandID == activeID
	})
	waitIntegrationGate(t, ctx, fixture, 0, "active-terminal")

	if err := sendSteeringIntegrationInput(t, ctx, state, loopID, firstID, "first fallback", true); err != nil {
		t.Fatalf("first fallback input: %v", err)
	}
	waitIntegrationGate(t, ctx, fixture, 1, "steer-ack")
	if err := fixture.Release("steer-ack"); err != nil {
		t.Fatalf("release steering acknowledgement: %v", err)
	}
	waitIntegrationSteerOutcome(t, ctx, fixture, 0, steertest.OutcomePromptRequired)

	// The first fallback is now durably queued. The second request must remain
	// older than a request arriving while that fallback turn is active.
	if err := sendSteeringIntegrationInput(t, ctx, state, loopID, secondID, "second fallback", true); err != nil {
		t.Fatalf("second fallback input: %v", err)
	}
	if err := fixture.Release("active-terminal"); err != nil {
		t.Fatalf("release active terminal: %v", err)
	}
	if _, err := fixture.WaitForNth(ctx, steertest.EventTerminal, 0); err != nil {
		t.Fatalf("active terminal: %v", err)
	}
	waitIntegrationGate(t, ctx, fixture, 2, "fallback-one")

	// While #1 is active, #2 remains in Loop.pending. #3 must be queued behind
	// it instead of being steered into #1.
	if err := sendSteeringIntegrationInput(t, ctx, state, loopID, thirdID, "third fallback", true); err != nil {
		t.Fatalf("third fallback input: %v", err)
	}
	if err := fixture.Release("fallback-one"); err != nil {
		t.Fatalf("release first fallback terminal: %v", err)
	}
	if _, err := fixture.WaitForNth(ctx, steertest.EventTerminal, 1); err != nil {
		t.Fatalf("first fallback terminal: %v", err)
	}
	waitIntegrationGate(t, ctx, fixture, 3, "fallback-two")
	if err := fixture.Release("fallback-two"); err != nil {
		t.Fatalf("release second fallback terminal: %v", err)
	}
	if _, err := fixture.WaitForNth(ctx, steertest.EventTerminal, 2); err != nil {
		t.Fatalf("second fallback terminal: %v", err)
	}
	waitIntegrationGate(t, ctx, fixture, 4, "fallback-three")
	if err := fixture.Release("fallback-three"); err != nil {
		t.Fatalf("release third fallback terminal: %v", err)
	}
	if _, err := fixture.WaitForNth(ctx, steertest.EventTerminal, 3); err != nil {
		t.Fatalf("third fallback terminal: %v", err)
	}

	pub.wait(ctx, func(input event.Event) bool { _, ok := input.(event.TurnDone); return ok })
	pub.wait(ctx, func(input event.Event) bool { _, ok := input.(event.TurnDone); return ok })
	pub.wait(ctx, func(input event.Event) bool { _, ok := input.(event.TurnDone); return ok })
	pub.wait(ctx, func(input event.Event) bool { _, ok := input.(event.TurnDone); return ok })

	_, reservations, fallbacks, resolutions := hook.snapshot()
	if !reflect.DeepEqual(fallbacks, []uuid.UUID{firstID, secondID, thirdID}) {
		t.Fatalf("fallback order = %v, want [%s %s %s]", fallbacks, firstID, secondID, thirdID)
	}
	if !reflect.DeepEqual(reservations, []uuid.UUID{firstID}) {
		t.Fatalf("reservations = %v, want [%s]", reservations, firstID)
	}
	if len(resolutions) != 0 {
		t.Fatalf("resolutions = %v, want none for fallback delivery", resolutions)
	}
	if got := counting.callCount(); got != 1 {
		t.Fatalf("ACP steering calls = %d, want one call for #1 only", got)
	}
	if got := steeringIntegrationCountEvents(fixture, steertest.EventSteer); got != 2 {
		t.Fatalf("fixture steer records = %d, want one request/response pair", got)
	}
}

func TestSteeringIntegrationCurrentCodexQueuesWithoutExtensionFallback(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	t.Cleanup(cancel)
	script := steertest.CodexScript()
	script.Prompts = []steertest.PromptScript{
		{Actions: []steertest.Action{{Kind: steertest.ActionTerminal, Gate: "active-terminal"}}},
		{Actions: []steertest.Action{{Kind: steertest.ActionTerminal, Gate: "fallback-terminal"}}},
	}
	base, fixture := newSteeringIntegrationACP(t, ctx, script, acpdriver.HarnessCodex)
	counting := &steeringIntegrationCountingAgent{Driver: base}
	hook := &steeringIntegrationDeliveryHook{}
	pub := newSteeringIntegrationPublisher()
	state, loopID := buildSteeringIntegrationBackend(t, ctx, counting, hook, pub)
	activeID, requestID := steeringIntegrationUUID(t), steeringIntegrationUUID(t)
	if err := sendSteeringIntegrationInput(t, ctx, state, loopID, activeID, "active", false); err != nil {
		t.Fatalf("active input: %v", err)
	}
	pub.wait(ctx, func(input event.Event) bool {
		started, ok := input.(event.TurnStarted)
		return ok && started.Cause.CommandID == activeID
	})
	waitIntegrationGate(t, ctx, fixture, 0, "active-terminal")
	if err := sendSteeringIntegrationInput(t, ctx, state, loopID, requestID, "codex fallback", true); err != nil {
		t.Fatalf("steering input: %v", err)
	}
	if err := fixture.Release("active-terminal"); err != nil {
		t.Fatalf("release active terminal: %v", err)
	}
	if _, err := fixture.WaitForNth(ctx, steertest.EventTerminal, 0); err != nil {
		t.Fatalf("active terminal: %v", err)
	}
	waitIntegrationGate(t, ctx, fixture, 1, "fallback-terminal")
	if err := fixture.Release("fallback-terminal"); err != nil {
		t.Fatalf("release fallback terminal: %v", err)
	}
	if _, err := fixture.WaitForNth(ctx, steertest.EventTerminal, 1); err != nil {
		t.Fatalf("fallback terminal: %v", err)
	}
	pub.wait(ctx, func(input event.Event) bool { _, ok := input.(event.TurnDone); return ok })
	pub.wait(ctx, func(input event.Event) bool { _, ok := input.(event.TurnDone); return ok })

	assertSteeringIntegrationChecked(t, pub, []string{
		"event.TurnStarted",
		"event.DelegateRequestAccepted",
		"event.TurnDone",
		"event.TurnStarted",
		"event.TurnDone",
	})
	calls, reservations, fallbacks, resolutions := hook.snapshot()
	if !reflect.DeepEqual(calls, []string{"fallback"}) {
		t.Fatalf("delivery calls = %v, want exactly one fallback", calls)
	}
	if len(reservations) != 0 || len(fallbacks) != 1 || fallbacks[0] != requestID || len(resolutions) != 0 {
		t.Fatalf("delivery records = reservations %v fallbacks %v resolutions %v, want one fallback only", reservations, fallbacks, resolutions)
	}
	if got := counting.callCount(); got != 0 {
		t.Fatalf("Codex steering calls = %d, want no extension call", got)
	}
	if got := steeringIntegrationCountEvents(fixture, steertest.EventSteer); got != 0 {
		t.Fatalf("fixture steer records = %d, want 0", got)
	}
}

func TestSteeringIntegrationPostWriteUnknownDoesNotFallback(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	t.Cleanup(cancel)
	base, fixture := newSteeringIntegrationACP(t, ctx, steeringIntegrationScript(steertest.SteeringOutcome("future")), acpdriver.HarnessClaudeCode)
	counting := &steeringIntegrationCountingAgent{Driver: base}
	hook := &steeringIntegrationDeliveryHook{}
	pub := newSteeringIntegrationPublisher()
	state, loopID := buildSteeringIntegrationBackend(t, ctx, counting, hook, pub)
	activeID, requestID := steeringIntegrationUUID(t), steeringIntegrationUUID(t)
	if err := sendSteeringIntegrationInput(t, ctx, state, loopID, activeID, "active", false); err != nil {
		t.Fatalf("active input: %v", err)
	}
	pub.wait(ctx, func(input event.Event) bool {
		started, ok := input.(event.TurnStarted)
		return ok && started.Cause.CommandID == activeID
	})
	waitIntegrationGate(t, ctx, fixture, 0, "active-terminal")
	if err := sendSteeringIntegrationInput(t, ctx, state, loopID, requestID, "unknown", true); err != nil {
		t.Fatalf("steering input: %v", err)
	}
	waitIntegrationGate(t, ctx, fixture, 1, "steer-ack")
	if err := fixture.Release("steer-ack"); err != nil {
		t.Fatalf("release steering acknowledgement: %v", err)
	}
	waitIntegrationSteerOutcome(t, ctx, fixture, 0, steertest.SteeringOutcome("future"))
	if err := fixture.Release("active-terminal"); err != nil {
		t.Fatalf("release active terminal: %v", err)
	}
	if terminal := fixture.WaitForKind(ctx, steertest.EventTerminal); terminal.Err != nil {
		t.Fatalf("terminal: %v", terminal.Err)
	}
	pub.wait(ctx, func(input event.Event) bool { _, ok := input.(event.TurnDone); return ok })

	assertSteeringIntegrationChecked(t, pub, []string{
		"event.TurnStarted",
		"event.DelegateRequestAccepted",
		"event.TurnDone",
	})
	calls, reservations, fallbacks, resolutions := hook.snapshot()
	if !reflect.DeepEqual(calls, []string{"reserve", string(foreign.DeliveryResolutionUnknown)}) {
		t.Fatalf("delivery calls = %v, want reserve/unknown", calls)
	}
	if len(reservations) != 1 || reservations[0] != requestID || len(fallbacks) != 0 || len(resolutions) != 1 || resolutions[0].State != foreign.DeliveryResolutionUnknown {
		t.Fatalf("delivery records = reservations %v fallbacks %v resolutions %v, want one unknown resolution", reservations, fallbacks, resolutions)
	}
	if got := counting.callCount(); got != 1 {
		t.Fatalf("ACP steering calls = %d, want 1", got)
	}
}

func TestSteeringIntegrationAdmissionUnknownIsNonRetryable(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	t.Cleanup(cancel)
	base, fixture := newSteeringIntegrationACP(t, ctx, steeringIntegrationScript(steertest.OutcomeInjected), acpdriver.HarnessClaudeCode)
	wrapper := &steeringIntegrationResultAgent{
		Driver: base,
		result: driver.SteerResult{Outcome: driver.SteerOutcomeAdmissionUnknown},
		called: make(chan struct{}, 1),
	}
	hook := &steeringIntegrationDeliveryHook{}
	pub := newSteeringIntegrationPublisher()
	state, loopID := buildSteeringIntegrationBackend(t, ctx, wrapper, hook, pub)
	activeID, requestID := steeringIntegrationUUID(t), steeringIntegrationUUID(t)
	if err := sendSteeringIntegrationInput(t, ctx, state, loopID, activeID, "active", false); err != nil {
		t.Fatalf("active input: %v", err)
	}
	pub.wait(ctx, func(input event.Event) bool {
		started, ok := input.(event.TurnStarted)
		return ok && started.Cause.CommandID == activeID
	})
	waitIntegrationGate(t, ctx, fixture, 0, "active-terminal")
	if err := sendSteeringIntegrationInput(t, ctx, state, loopID, requestID, "admission unknown", true); err != nil {
		t.Fatalf("steering input: %v", err)
	}
	select {
	case <-wrapper.called:
	case <-ctx.Done():
		t.Fatalf("Steer() was not called: %v", ctx.Err())
	}
	if err := fixture.Release("active-terminal"); err != nil {
		t.Fatalf("release active terminal: %v", err)
	}
	if terminal := fixture.WaitForKind(ctx, steertest.EventTerminal); terminal.Err != nil {
		t.Fatalf("terminal: %v", terminal.Err)
	}
	pub.wait(ctx, func(input event.Event) bool { _, ok := input.(event.TurnDone); return ok })

	assertSteeringIntegrationChecked(t, pub, []string{
		"event.TurnStarted",
		"event.DelegateRequestAccepted",
		"event.TurnDone",
	})
	calls, reservations, fallbacks, resolutions := hook.snapshot()
	if !reflect.DeepEqual(calls, []string{"reserve", string(foreign.DeliveryResolutionUnknown)}) {
		t.Fatalf("delivery calls = %v, want reserve/unknown", calls)
	}
	if len(reservations) != 1 || reservations[0] != requestID || len(fallbacks) != 0 || len(resolutions) != 1 || resolutions[0].State != foreign.DeliveryResolutionUnknown {
		t.Fatalf("delivery records = reservations %v fallbacks %v resolutions %v, want one unknown resolution", reservations, fallbacks, resolutions)
	}
	if got := steeringIntegrationCountEvents(fixture, steertest.EventSteer); got != 0 {
		t.Fatalf("fixture steer records = %d, want 0", got)
	}
}

func TestSteeringIntegrationUntrackableFaultHasNoSyntheticTurn(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	t.Cleanup(cancel)
	base, fixture := newSteeringIntegrationACP(t, ctx, steeringIntegrationScript(steertest.OutcomeStartedNewTurn), acpdriver.HarnessClaudeCode)
	hook := &steeringIntegrationDeliveryHook{}
	pub := newSteeringIntegrationPublisher()
	state, loopID := buildSteeringIntegrationBackend(t, ctx, base, hook, pub)
	activeID, requestID := steeringIntegrationUUID(t), steeringIntegrationUUID(t)
	if err := sendSteeringIntegrationInput(t, ctx, state, loopID, activeID, "active", false); err != nil {
		t.Fatalf("active input: %v", err)
	}
	pub.wait(ctx, func(input event.Event) bool {
		started, ok := input.(event.TurnStarted)
		return ok && started.Cause.CommandID == activeID
	})
	waitIntegrationGate(t, ctx, fixture, 0, "active-terminal")
	if err := sendSteeringIntegrationInput(t, ctx, state, loopID, requestID, "untrackable", true); err != nil {
		t.Fatalf("steering input: %v", err)
	}
	waitIntegrationGate(t, ctx, fixture, 1, "steer-ack")
	if err := fixture.Release("steer-ack"); err != nil {
		t.Fatalf("release steering acknowledgement: %v", err)
	}
	waitIntegrationSteerOutcome(t, ctx, fixture, 0, steertest.OutcomeStartedNewTurn)
	select {
	case <-state.DoneChan():
	case <-ctx.Done():
		t.Fatalf("untrackable loop did not fault: %v", ctx.Err())
	}

	assertSteeringIntegrationChecked(t, pub, []string{
		"event.TurnStarted",
		"event.DelegateRequestAccepted",
	})
	calls, reservations, fallbacks, resolutions := hook.snapshot()
	if !reflect.DeepEqual(calls, []string{"reserve", string(foreign.DeliveryResolutionUntrackable)}) {
		t.Fatalf("delivery calls = %v, want reserve/untrackable", calls)
	}
	if len(reservations) != 1 || reservations[0] != requestID || len(fallbacks) != 0 || len(resolutions) != 1 || resolutions[0].State != foreign.DeliveryResolutionUntrackable {
		t.Fatalf("delivery records = reservations %v fallbacks %v resolutions %v, want one untrackable resolution", reservations, fallbacks, resolutions)
	}
	if got := steeringIntegrationCountEvents(fixture, steertest.EventSteer); got != 2 {
		t.Fatalf("fixture steer records = %d, want one request/response pair", got)
	}
}

func TestSteeringIntegrationTwoMessagesFoldIntoOneTurn(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	t.Cleanup(cancel)
	script := steertest.DefaultScript()
	script.Prompts = []steertest.PromptScript{{Actions: []steertest.Action{
		{Kind: steertest.ActionTerminal, Gate: "active-terminal"},
	}}}
	script.Steers = []steertest.SteerScript{
		{Actions: []steertest.Action{{Kind: steertest.ActionSteerReply, Outcome: steertest.OutcomeInjected, Gate: "steer-1"}}},
		{Actions: []steertest.Action{{Kind: steertest.ActionSteerReply, Outcome: steertest.OutcomeInjected, Gate: "steer-2"}}},
	}
	base, fixture := newSteeringIntegrationACP(t, ctx, script, acpdriver.HarnessClaudeCode)
	hook := &steeringIntegrationDeliveryHook{}
	pub := newSteeringIntegrationPublisher()
	state, loopID := buildSteeringIntegrationBackend(t, ctx, base, hook, pub)
	activeID, firstID, secondID := steeringIntegrationUUID(t), steeringIntegrationUUID(t), steeringIntegrationUUID(t)
	if err := sendSteeringIntegrationInput(t, ctx, state, loopID, activeID, "active", false); err != nil {
		t.Fatalf("active input: %v", err)
	}
	pub.wait(ctx, func(input event.Event) bool {
		started, ok := input.(event.TurnStarted)
		return ok && started.Cause.CommandID == activeID
	})
	waitIntegrationGate(t, ctx, fixture, 0, "active-terminal")
	if err := sendSteeringIntegrationInput(t, ctx, state, loopID, firstID, "first", true); err != nil {
		t.Fatalf("first steering input: %v", err)
	}
	waitIntegrationGate(t, ctx, fixture, 1, "steer-1")
	if err := sendSteeringIntegrationInput(t, ctx, state, loopID, secondID, "second", true); err != nil {
		t.Fatalf("second steering input: %v", err)
	}
	if err := fixture.Release("steer-1"); err != nil {
		t.Fatalf("release first steering acknowledgement: %v", err)
	}
	waitIntegrationSteerOutcome(t, ctx, fixture, 0, steertest.OutcomeInjected)
	waitIntegrationGate(t, ctx, fixture, 2, "steer-2")
	if err := fixture.Release("steer-2"); err != nil {
		t.Fatalf("release second steering acknowledgement: %v", err)
	}
	waitIntegrationSteerOutcome(t, ctx, fixture, 1, steertest.OutcomeInjected)
	pub.wait(ctx, func(input event.Event) bool { _, ok := input.(event.TurnFoldedInto); return ok })
	pub.wait(ctx, func(input event.Event) bool { _, ok := input.(event.TurnFoldedInto); return ok })
	if err := fixture.Release("active-terminal"); err != nil {
		t.Fatalf("release active terminal: %v", err)
	}
	if terminal := fixture.WaitForKind(ctx, steertest.EventTerminal); terminal.Err != nil {
		t.Fatalf("terminal: %v", terminal.Err)
	}
	pub.wait(ctx, func(input event.Event) bool { _, ok := input.(event.TurnDone); return ok })

	assertSteeringIntegrationChecked(t, pub, []string{
		"event.TurnStarted",
		"event.DelegateRequestAccepted",
		"event.DelegateRequestAccepted",
		"event.TurnFoldedInto",
		"event.TurnFoldedInto",
		"event.TurnDone",
	})
	calls, reservations, fallbacks, resolutions := hook.snapshot()
	if !reflect.DeepEqual(calls, []string{
		"reserve", string(foreign.DeliveryResolutionInjected),
		"reserve", string(foreign.DeliveryResolutionInjected),
	}) {
		t.Fatalf("delivery calls = %v, want two ordered reserve/injected pairs", calls)
	}
	if !reflect.DeepEqual(reservations, []uuid.UUID{firstID, secondID}) {
		t.Fatalf("reservations = %v, want [%s %s]", reservations, firstID, secondID)
	}
	if len(fallbacks) != 0 || len(resolutions) != 2 || resolutions[0].State != foreign.DeliveryResolutionInjected || resolutions[1].State != foreign.DeliveryResolutionInjected {
		t.Fatalf("fallbacks/resolutions = %v/%v, want no fallbacks and two injected resolutions", fallbacks, resolutions)
	}
	if got := steeringIntegrationCountEvents(fixture, steertest.EventPrompt); got != 1 {
		t.Fatalf("fixture prompt records = %d, want one active prompt", got)
	}
	if got := steeringIntegrationCountEvents(fixture, steertest.EventSteer); got != 4 {
		t.Fatalf("fixture steer records = %d, want two request/response pairs", got)
	}
}

func TestSteeringIntegrationLateAcknowledgementIsIgnored(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	t.Cleanup(cancel)
	base, fixture := newSteeringIntegrationACP(t, ctx, steeringIntegrationScript(steertest.OutcomeInjected), acpdriver.HarnessClaudeCode)
	wrapper := &steeringIntegrationLateAckAgent{
		Driver:  base,
		started: make(chan struct{}, 1),
		done:    make(chan struct{}),
	}
	hook := &steeringIntegrationDeliveryHook{signals: make(chan string, 32)}
	pub := newSteeringIntegrationPublisher()
	state, loopID := buildSteeringIntegrationBackend(t, ctx, wrapper, hook, pub)
	activeID, requestID := steeringIntegrationUUID(t), steeringIntegrationUUID(t)
	if err := sendSteeringIntegrationInput(t, ctx, state, loopID, activeID, "active", false); err != nil {
		t.Fatalf("active input: %v", err)
	}
	pub.wait(ctx, func(input event.Event) bool {
		started, ok := input.(event.TurnStarted)
		return ok && started.Cause.CommandID == activeID
	})
	waitIntegrationGate(t, ctx, fixture, 0, "active-terminal")
	if err := sendSteeringIntegrationInput(t, ctx, state, loopID, requestID, "late", true); err != nil {
		t.Fatalf("steering input: %v", err)
	}
	select {
	case <-wrapper.started:
	case <-ctx.Done():
		t.Fatalf("late steering call did not start: %v", ctx.Err())
	}
	waitIntegrationGate(t, ctx, fixture, 1, "steer-ack")
	if err := hook.waitCall(ctx, string(foreign.DeliveryResolutionUnknown)); err != nil {
		t.Fatalf("unknown delivery resolution: %v", err)
	}
	if err := fixture.Release("steer-ack"); err != nil {
		t.Fatalf("release late steering acknowledgement: %v", err)
	}
	waitIntegrationSteerOutcome(t, ctx, fixture, 0, steertest.OutcomeInjected)
	select {
	case <-wrapper.done:
	case <-ctx.Done():
		t.Fatalf("late ACP acknowledgement did not complete: %v", ctx.Err())
	}
	if err := fixture.Release("active-terminal"); err != nil {
		t.Fatalf("release active terminal: %v", err)
	}
	if terminal := fixture.WaitForKind(ctx, steertest.EventTerminal); terminal.Err != nil {
		t.Fatalf("terminal: %v", terminal.Err)
	}
	pub.wait(ctx, func(input event.Event) bool { _, ok := input.(event.TurnDone); return ok })

	assertSteeringIntegrationChecked(t, pub, []string{
		"event.TurnStarted",
		"event.DelegateRequestAccepted",
		"event.TurnDone",
	})
	calls, reservations, fallbacks, resolutions := hook.snapshot()
	if !reflect.DeepEqual(calls, []string{"reserve", string(foreign.DeliveryResolutionUnknown)}) {
		t.Fatalf("delivery calls = %v, want reserve/unknown", calls)
	}
	if len(reservations) != 1 || reservations[0] != requestID || len(fallbacks) != 0 || len(resolutions) != 1 || resolutions[0].State != foreign.DeliveryResolutionUnknown {
		t.Fatalf("delivery records = reservations %v fallbacks %v resolutions %v, want one unknown resolution", reservations, fallbacks, resolutions)
	}
	if got := steeringIntegrationCountEvents(fixture, steertest.EventSteer); got != 2 {
		t.Fatalf("fixture steer records = %d, want one request/response pair", got)
	}
}
