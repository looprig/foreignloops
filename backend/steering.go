package backend

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/looprig/core/content"
	"github.com/looprig/core/uuid"
	"github.com/looprig/foreignloops/driver"
	"github.com/looprig/harness/pkg/command"
	"github.com/looprig/harness/pkg/event"
	"github.com/looprig/harness/pkg/foreign"
	"github.com/looprig/harness/pkg/identity"
)

// steeringAckTimeout is the short runtime safety bound for a steering request
// whose provider write has already been admitted. It is deliberately not part
// of a model-facing request. A missing acknowledgement is resolved as
// ambiguous after this bound and is never retried automatically.
const steeringAckTimeout = 100 * time.Millisecond

// steeringCallTimeout bounds a request whose provider admission is not yet
// known. The ACP transport has its own terminal grace period, but the backend
// must retain a separate hard bound for adapters that never produce a
// completion or ordered acknowledgement. This longer bound avoids adjudicating
// a valid wire response merely because the actor was scheduled after terminal.
const steeringCallTimeout = time.Second

type steeringDisposition uint8

const (
	steeringDispositionFallback steeringDisposition = iota + 1
	steeringDispositionInjected
	steeringDispositionUnknown
	steeringDispositionUntrackable
)

type steeringAttempt struct {
	input   preparedInput
	request driver.SteerRequest

	cancel   context.CancelFunc
	done     bool
	seen     bool
	resolved bool

	writerAdmitted bool
	result         driver.SteerResult
	err            error
}

type steeringCompletion struct {
	attempt *steeringAttempt
	result  driver.SteerResult
	err     error
}

// steeringMachine is owned by the foreign-loop actor. It serializes steering
// attempts, retains the FIFO order of accepted requests, and never publishes
// a lifecycle event itself. The actor calls observe/complete and performs all
// checked event publication through Loop.publishActor.
type steeringMachine struct {
	loop *Loop
	ctx  context.Context
	// resolveCtx remains usable after the turn operation is canceled. Durable
	// delivery resolution and checked fold publication must not be lost merely
	// because the caller interrupted the provider operation.
	resolveCtx context.Context
	pub        func(event.Event)
	cur        event.TurnIndex
	turnID     uuid.UUID
	stepID     uuid.UUID

	steerer     driver.Steerer
	ordered     bool
	streamKnown bool
	hook        foreign.DeliveryHook

	completions     chan steeringCompletion
	active          *steeringAttempt
	pending         []steeringAttempt
	terminal        bool
	disabled        bool
	fallbackBarrier bool
	fault           error
	timer           *time.Timer
}

func newSteeringMachine(ctx context.Context, l *Loop, pub func(event.Event), cur event.TurnIndex, turnID, stepID uuid.UUID, stream driver.Stream) *steeringMachine {
	if ctx == nil {
		ctx = context.Background()
	}
	steerer, ok := l.backendCfg.Agent.(driver.Steerer)
	if !ok {
		steerer = nil
	}
	_, ordered := stream.(driver.OrderedStream)
	return &steeringMachine{
		loop:        l,
		ctx:         ctx,
		resolveCtx:  context.WithoutCancel(ctx),
		pub:         pub,
		cur:         cur,
		turnID:      turnID,
		stepID:      stepID,
		steerer:     steerer,
		ordered:     ordered,
		hook:        l.services.Delivery,
		completions: make(chan steeringCompletion, 16),
	}
}

func (m *steeringMachine) resolutionContext() context.Context {
	if m == nil {
		return context.Background()
	}
	if m.resolveCtx != nil {
		return m.resolveCtx
	}
	if m.ctx == nil {
		return context.Background()
	}
	return context.WithoutCancel(m.ctx)
}

func (m *steeringMachine) completionsChan() <-chan steeringCompletion {
	if m == nil {
		return nil
	}
	return m.completions
}

func (m *steeringMachine) setStream(stream driver.Stream) error {
	if m == nil {
		return nil
	}
	_, m.ordered = stream.(driver.OrderedStream)
	m.streamKnown = true
	if !m.ordered {
		for len(m.pending) > 0 {
			attempt := m.pending[0]
			m.pending = m.pending[1:]
			if err := m.queueFallback(attempt.input); err != nil {
				m.fault = err
				return err
			}
		}
		return nil
	}
	return m.pump()
}

func (m *steeringMachine) pendingCount() int {
	if m == nil {
		return 0
	}
	count := len(m.pending)
	if m.active != nil {
		count++
	}
	return count
}

func (m *steeringMachine) timerChan() <-chan time.Time {
	if m == nil || m.timer == nil {
		return nil
	}
	return m.timer.C
}

func (m *steeringMachine) isCandidate(input preparedInput) bool {
	if m == nil || m.hook == nil {
		return false
	}
	if input.command.Agency != identity.AgencyMachine || input.command.TargetLoopID != m.loop.loopID {
		return false
	}
	return input.command.DelegateDeliveryPhase == command.DelegateDeliveryPhaseIntent
}

// offer accepts one actor-admitted request. A true return means the steering
// machine owns the request and has either started steering or durably queued a
// fallback. A false return leaves legacy ordinary-input handling to the actor.
func (m *steeringMachine) offer(input preparedInput) (bool, error) {
	if !m.isCandidate(input) {
		return false, nil
	}
	// A steering machine is recreated for every normal turn. Once an earlier
	// fallback has crossed into the loop-level queue, that older request is a
	// FIFO barrier even though this machine has no local fallbackBarrier yet.
	// Cancellation removes entries from loop.pending on the actor, so a request
	// can steer again only after all older queued work has been retracted or run.
	if len(m.loop.pending) > 0 || m.terminal || m.disabled || m.fallbackBarrier || m.steerer == nil {
		return true, m.queueFallback(input)
	}
	request, err := driver.NewSteerRequest(input.command.Blocks)
	if err != nil {
		return true, m.queueFallback(input)
	}
	attempt := steeringAttempt{input: input, request: request}
	m.pending = append(m.pending, attempt)
	if !m.streamKnown {
		return true, nil
	}
	if !m.ordered {
		m.pending = m.pending[:len(m.pending)-1]
		return true, m.queueFallback(input)
	}
	return true, m.pump()
}

func (m *steeringMachine) pump() error {
	if m == nil || m.fault != nil || m.disabled || m.fallbackBarrier || m.terminal || m.active != nil || len(m.pending) == 0 {
		return m.fault
	}
	next := m.pending[0]
	m.pending = m.pending[1:]
	if err := m.hook.Reserve(m.ctx, foreign.DeliveryReservation{
		LoopID: m.loop.loopID, RequestID: next.input.command.CommandID,
	}); err != nil {
		m.fault = err
		return err
	}
	attempt := &next
	// Do not start the terminal-race clock at admission. The provider response
	// and its ordered observation may legitimately arrive after this actor has
	// launched the call; only a terminal boundary needs a bounded ambiguity
	// decision. The terminal path cancels this context if that bound expires.
	attemptCtx, cancel := context.WithTimeout(m.ctx, steeringCallTimeout)
	attempt.cancel = cancel
	m.active = attempt
	go m.callSteerer(attemptCtx, attempt)
	return nil
}

func (m *steeringMachine) callSteerer(ctx context.Context, attempt *steeringAttempt) {
	result, err := m.steerer.Steer(ctx, attempt.request)
	select {
	case m.completions <- steeringCompletion{attempt: attempt, result: result, err: err}:
	case <-m.ctx.Done():
	}
}

// observe consumes one ordered stream fact. Steer observations are the
// authoritative provider acknowledgement; prompt observations mark the active
// terminal race but do not publish a Harness terminal.
func (m *steeringMachine) observe(observation driver.Observation) error {
	if m == nil || observation == nil {
		return nil
	}
	switch typed := observation.(type) {
	case driver.SteerObservation:
		return m.observeSteer(typed)
	case driver.PromptObservation:
		m.terminal = true
		return m.resolvePendingBeforeTerminal()
	default:
		return nil
	}
}

// beforeTerminal decides whether the actor must hold the terminal event until
// the one in-flight steering attempt is adjudicated. A terminal observation
// alone does not prove that a reserved write stayed before the provider
// boundary, so an unresolved attempt is held until its bounded ack deadline.
func (m *steeringMachine) beforeTerminal() (bool, error) {
	if m == nil {
		return false, nil
	}
	m.terminal = true
	if err := m.resolvePendingBeforeTerminal(); err != nil {
		return false, err
	}
	if m.active != nil && !m.active.resolved {
		return true, nil
	}
	return false, nil
}

func (m *steeringMachine) observeSteer(observation driver.SteerObservation) error {
	attempt := m.active
	if attempt == nil || attempt.resolved {
		// A provider acknowledgement after a terminal/unknown resolution has no
		// request identity at this boundary. The FIFO owner deliberately ignores it.
		return nil
	}
	attempt.seen = true
	if observation.WriteAdmitted {
		attempt.writerAdmitted = true
	}
	attempt.result, attempt.err = observation.SteerResult, observation.Err
	return m.resolveAttempt(attempt)
}

func (m *steeringMachine) complete(completion steeringCompletion) error {
	if m == nil || completion.attempt == nil {
		return nil
	}
	attempt := completion.attempt
	if attempt != m.active {
		return nil
	}
	attempt.done = true
	attempt.result, attempt.err = completion.result, completion.err
	if completion.result.WriteAdmitted {
		attempt.writerAdmitted = true
	}
	// A completed call with no result is an acknowledgement failure. A valid
	// admission/delivery-unknown result paired with the call deadline is also
	// authoritative: the bounded call has exhausted its evidence window, and
	// no ordered observation may arrive to identify a more specific outcome.
	// Other valid provider results still wait for their ordered observation so
	// the actor preserves wire order when the call goroutine wins the scheduler
	// race.
	validBoundedUnknown := errors.Is(completion.err, context.DeadlineExceeded) &&
		completion.result.Validate() == nil &&
		(completion.result.Outcome == driver.SteerOutcomeAdmissionUnknown ||
			completion.result.Outcome == driver.SteerOutcomeDeliveryUnknown)
	if !attempt.seen && (completion.err != nil && completion.result.Outcome == "" ||
		completion.result.Outcome != "" && completion.result.Validate() != nil ||
		validBoundedUnknown) {
		if err := m.resolveAttempt(attempt); err != nil {
			return err
		}
		return m.retireIfDone(attempt)
	}
	if m.ordered && !attempt.seen && !m.terminal {
		// Ordered streams carry the authoritative acknowledgement separately. A
		// completion can win the scheduler race but cannot reorder that fact.
		return nil
	}
	if err := m.resolveAttempt(attempt); err != nil {
		return err
	}
	return m.retireIfDone(attempt)
}

func (m *steeringMachine) resolveAttempt(attempt *steeringAttempt) error {
	if attempt == nil || attempt.resolved {
		return nil
	}
	result := attempt.result
	if attempt.err != nil && result.Outcome == "" {
		return m.resolveDisposition(attempt, steeringDispositionUnknown)
	}
	if err := result.Validate(); err != nil {
		return m.resolveDisposition(attempt, steeringDispositionUnknown)
	}
	switch result.Outcome {
	case driver.SteerOutcomeInjected:
		return m.resolveDisposition(attempt, steeringDispositionInjected)
	case driver.SteerOutcomeFallbackRequired, driver.SteerOutcomeUnsupported:
		return m.resolveDisposition(attempt, steeringDispositionFallback)
	case driver.SteerOutcomeAdmissionUnknown, driver.SteerOutcomeDeliveryUnknown:
		return m.resolveDisposition(attempt, steeringDispositionUnknown)
	case driver.SteerOutcomeDeliveredUntrackable:
		return m.resolveDisposition(attempt, steeringDispositionUntrackable)
	default:
		return m.resolveDisposition(attempt, steeringDispositionUnknown)
	}
}

func (m *steeringMachine) resolveDisposition(attempt *steeringAttempt, disposition steeringDisposition) error {
	if attempt == nil || attempt.resolved {
		return nil
	}
	switch disposition {
	case steeringDispositionInjected:
		folded := event.TurnFoldedInto{
			Header: event.Header{Cause: identity.Cause{
				Coordinates: identity.Coordinates{LoopID: attempt.input.command.Cause.LoopID},
				CommandID:   attempt.input.command.CommandID,
				Agency:      attempt.input.command.Agency,
			}},
			TurnIndex: m.cur,
			Message: &content.UserMessage{Message: content.Message{
				Role:   content.RoleUser,
				Blocks: attempt.input.command.Blocks,
			}},
		}
		if err := m.loop.publishActor(m.resolutionContext(), m.turnID, m.stepID, folded); err != nil {
			m.fault = &ForeignPublicationError{Event: "event.TurnFoldedInto", Cause: err}
			return m.fault
		}
		if err := m.hook.Resolve(m.resolutionContext(), foreign.DeliveryResolution{
			LoopID: m.loop.loopID, RequestID: attempt.input.command.CommandID,
			TurnID: m.turnID, State: foreign.DeliveryResolutionInjected,
		}); err != nil {
			m.fault = err
			return err
		}
	case steeringDispositionFallback:
		m.fallbackBarrier = true
		if err := m.queueFallback(attempt.input); err != nil {
			m.fault = err
			return err
		}
	case steeringDispositionUnknown:
		if err := m.hook.Resolve(m.resolutionContext(), foreign.DeliveryResolution{
			LoopID: m.loop.loopID, RequestID: attempt.input.command.CommandID,
			State: foreign.DeliveryResolutionUnknown,
		}); err != nil {
			m.fault = err
			return err
		}
		m.disabled = true
	case steeringDispositionUntrackable:
		if err := m.hook.Resolve(m.resolutionContext(), foreign.DeliveryResolution{
			LoopID: m.loop.loopID, RequestID: attempt.input.command.CommandID,
			State: foreign.DeliveryResolutionUntrackable,
		}); err != nil {
			m.fault = err
			return err
		}
		m.disabled = true
		m.fault = errors.New("foreignloop: steering delivered an untrackable turn")
		return m.fault
	default:
		m.fault = fmt.Errorf("foreignloop: unknown steering disposition %d", disposition)
		return m.fault
	}
	attempt.resolved = true
	if disposition == steeringDispositionFallback || disposition == steeringDispositionUnknown {
		if err := m.queuePendingFallbacks(); err != nil {
			m.fault = err
			return err
		}
	}
	if attempt.cancel != nil && disposition != steeringDispositionInjected {
		// A fallback/unknown result no longer needs a caller-facing wait. The
		// goroutine remains tracked until its completion so a late ack cannot be
		// assigned to the next FIFO request.
		attempt.cancel()
	}
	return m.retireIfDone(attempt)
}

func (m *steeringMachine) queueFallback(input preparedInput) error {
	if m == nil || m.hook == nil {
		return errors.New("foreignloop: delivery hook unavailable")
	}
	// Once one request has moved to the normal turn queue, keep later
	// requests on that same FIFO path for this active turn. Otherwise a later
	// steer could be delivered before the earlier fallback turn runs.
	m.fallbackBarrier = true
	if err := m.hook.QueueFallback(m.resolutionContext(), foreign.DeliveryFallback{
		LoopID: m.loop.loopID, RequestID: input.command.CommandID,
	}); err != nil {
		return err
	}
	m.loop.pending = append(m.loop.pending, input)
	if m.pub != nil {
		m.pub(event.InputQueued{Header: event.Header{Cause: identity.Cause{
			CommandID: input.command.CommandID, Agency: input.command.Agency,
		}}})
	}
	return nil
}

func (m *steeringMachine) resolvePendingBeforeTerminal() error {
	if m == nil {
		return nil
	}
	if m.active != nil && !m.active.resolved {
		// A terminal observation by itself is not proof that the reserved
		// steering write never crossed its provider boundary. Resolve a
		// completed/observed result immediately; otherwise hold the terminal
		// until the same bounded acknowledgement clock adjudicates the race.
		if m.active.done || m.active.seen {
			if err := m.resolveAttempt(m.active); err != nil {
				return err
			}
		}
		if m.active != nil && !m.active.resolved {
			m.startTerminalTimer()
		}
	}
	for len(m.pending) > 0 {
		attempt := m.pending[0]
		m.pending = m.pending[1:]
		if err := m.queueFallback(attempt.input); err != nil {
			return err
		}
		attempt.resolved = true
	}
	return nil
}

func (m *steeringMachine) queuePendingFallbacks() error {
	if m == nil {
		return nil
	}
	for len(m.pending) > 0 {
		attempt := m.pending[0]
		m.pending = m.pending[1:]
		if err := m.queueFallback(attempt.input); err != nil {
			return err
		}
		attempt.resolved = true
	}
	return nil
}

// startTerminalTimer starts the bounded terminal-race clock for an attempt
// whose provider acknowledgement is still unresolved.
func (m *steeringMachine) startTerminalTimer() {
	if m != nil && m.timer == nil {
		timeout := steeringCallTimeout
		if m.active != nil && m.active.writerAdmitted {
			timeout = steeringAckTimeout
		}
		m.timer = time.NewTimer(timeout)
	}
}

// timeout resolves a post-writer terminal race as unknown. The caller then
// releases its held terminal event and closes/continues the actor according to
// the normal turn outcome; no automatic fallback is attempted.
func (m *steeringMachine) timeout() error {
	if m == nil || !m.terminal || m.active == nil || m.active.resolved {
		return nil
	}
	m.timer = nil
	if err := m.resolveDisposition(m.active, steeringDispositionUnknown); err != nil {
		return err
	}
	return m.retireIfDone(m.active)
}

func (m *steeringMachine) retireIfDone(attempt *steeringAttempt) error {
	if m == nil || attempt == nil || !attempt.resolved || !attempt.done || m.active != attempt {
		return m.fault
	}
	if attempt.cancel != nil {
		attempt.cancel()
	}
	if m.timer != nil {
		if !m.timer.Stop() {
			select {
			case <-m.timer.C:
			default:
			}
		}
		m.timer = nil
	}
	m.active = nil
	if !m.terminal {
		return m.pump()
	}
	return m.fault
}

func (m *steeringMachine) terminalReady() bool {
	return m == nil || m.fault != nil || (m.active == nil || m.active.resolved) && len(m.pending) == 0
}

func (m *steeringMachine) faulted() error {
	if m == nil {
		return nil
	}
	return m.fault
}

func (m *steeringMachine) logFault() {
	if m != nil && m.fault != nil {
		slog.Error("foreignloop: steering state machine faulted", "error", m.fault)
	}
}
