package backend

import (
	"context"
	"errors"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/looprig/core/uuid"
	"github.com/looprig/foreignloops/driver"
	"github.com/looprig/harness/pkg/command"
	"github.com/looprig/harness/pkg/foreign"
	"github.com/looprig/harness/pkg/identity"
)

// manualSteeringTimer gives actor tests a deterministic deadline gate without
// changing the production timer implementation or sleeping for a duration.
type manualSteeringTimer struct {
	mu      sync.Mutex
	ch      chan time.Time
	stopped bool
	fired   bool
}

func newManualSteeringTimer() *manualSteeringTimer {
	return &manualSteeringTimer{ch: make(chan time.Time, 1)}
}

func (t *manualSteeringTimer) Chan() <-chan time.Time { return t.ch }

func (t *manualSteeringTimer) Stop() bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.stopped || t.fired {
		return false
	}
	t.stopped = true
	return true
}

func (t *manualSteeringTimer) fire() {
	t.mu.Lock()
	if t.stopped || t.fired {
		t.mu.Unlock()
		return
	}
	t.fired = true
	t.mu.Unlock()
	t.ch <- time.Time{}
}

type manualSteeringTimerFactory struct {
	mu        sync.Mutex
	timers    []*manualSteeringTimer
	durations []time.Duration
}

func (f *manualSteeringTimerFactory) new(timeout time.Duration) steeringTimer {
	timer := newManualSteeringTimer()
	f.mu.Lock()
	f.timers = append(f.timers, timer)
	f.durations = append(f.durations, timeout)
	f.mu.Unlock()
	return timer
}

func (f *manualSteeringTimerFactory) snapshot() ([]*manualSteeringTimer, []time.Duration) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]*manualSteeringTimer(nil), f.timers...), append([]time.Duration(nil), f.durations...)
}

// ignoringSteerer models an adapter that does not honor the cancellation
// context. The actor deadline must still retire the reservation and resolve it
// exactly once; releasing the adapter later only exercises the late-completion
// path.
type ignoringSteerer struct {
	started chan struct{}
	release chan struct{}
}

func (s *ignoringSteerer) Steer(context.Context, driver.SteerRequest) (driver.SteerResult, error) {
	select {
	case s.started <- struct{}{}:
	default:
	}
	<-s.release
	return fallbackSteerResult(), nil
}

func (s *ignoringSteerer) Spawn(context.Context, driver.Turn) (driver.Stream, error) {
	return nil, errors.New("ignoring steerer does not spawn turns")
}

func TestSteeringDeadlineStartsAtReservation(t *testing.T) {
	steerer := &scriptedSteerer{started: make(chan struct{}, 1), results: make(chan scriptedSteerResult, 1)}
	hook := &recordingDeliveryHook{}
	l, machine, _, cancel := newSteeringUnit(t, steerer, hook)
	t.Cleanup(cancel)
	factory := &manualSteeringTimerFactory{}
	machine.timerFactory = factory.new
	if err := machine.setStream(orderedUnitStream()); err != nil {
		t.Fatalf("set stream: %v", err)
	}
	input := unitPreparedInput(t, l, "reservation deadline")
	if handled, err := machine.offer(input); !handled || err != nil {
		t.Fatalf("offer = handled %t err %v", handled, err)
	}
	awaitStarted(t, steerer.started)
	timers, durations := factory.snapshot()
	if len(timers) != 1 || len(durations) != 1 {
		t.Fatalf("reservation timers = %d durations = %d, want one", len(timers), len(durations))
	}
	if durations[0] != steeringCallTimeout {
		t.Fatalf("reservation deadline = %s, want %s", durations[0], steeringCallTimeout)
	}
	if machine.terminal {
		t.Fatal("reservation deadline started after terminal")
	}
}

func TestSteeringDeadlineRetiresIgnoredAdapter(t *testing.T) {
	steerer := &ignoringSteerer{started: make(chan struct{}, 1), release: make(chan struct{})}
	hook := &recordingDeliveryHook{}
	l, machine, _, cancel := newSteeringUnit(t, steerer, hook)
	t.Cleanup(cancel)
	factory := &manualSteeringTimerFactory{}
	machine.timerFactory = factory.new
	if err := machine.setStream(orderedUnitStream()); err != nil {
		t.Fatalf("set stream: %v", err)
	}
	input := unitPreparedInput(t, l, "deadline")
	if handled, err := machine.offer(input); !handled || err != nil {
		t.Fatalf("offer = handled %t err %v", handled, err)
	}
	awaitStarted(t, steerer.started)
	timers, _ := factory.snapshot()
	if len(timers) != 1 {
		t.Fatalf("reservation timers = %d, want one", len(timers))
	}

	// Keep terminal false: this is the reservation deadline, not the
	// post-terminal acknowledgement timer.
	if machine.terminal {
		t.Fatal("reservation deadline test unexpectedly entered terminal state")
	}
	timers[0].fire()
	select {
	case <-machine.deadlineTimerChan():
	case <-time.After(time.Second):
		t.Fatal("reservation deadline did not reach the actor")
	}
	if err := machine.deadlineTimeout(); err != nil {
		t.Fatalf("deadline timeout: %v", err)
	}
	if machine.active != nil {
		t.Fatalf("active attempt = %p, want retired after deadline", machine.active)
	}
	if !machine.disabled {
		t.Fatal("steering remained enabled after deadline ambiguity")
	}
	if got, want := hook.snapshot(), []string{"reserve", string(foreign.DeliveryResolutionUnknown)}; !reflect.DeepEqual(got, want) {
		t.Fatalf("delivery calls = %v, want %v", got, want)
	}

	// The adapter ignored cancellation and only returns now. Its completion
	// must not revive the retired request or trigger a second decision.
	close(steerer.release)
	completion := takeCompletion(t, machine)
	if err := machine.complete(completion); err != nil {
		t.Fatalf("late completion: %v", err)
	}
	if err := machine.observe(driver.SteerObservation{SteerResult: fallbackSteerResult()}); err != nil {
		t.Fatalf("late fallback observation: %v", err)
	}
	if got, want := hook.snapshot(), []string{"reserve", string(foreign.DeliveryResolutionUnknown)}; !reflect.DeepEqual(got, want) {
		t.Fatalf("late facts changed delivery calls = %v, want %v", got, want)
	}
}

func TestSteeringFallbackWinsDeadlineAndStillRetiresIgnoredAdapter(t *testing.T) {
	steerer := &ignoringSteerer{started: make(chan struct{}, 1), release: make(chan struct{})}
	hook := &recordingDeliveryHook{}
	l, machine, _, cancel := newSteeringUnit(t, steerer, hook)
	t.Cleanup(cancel)
	factory := &manualSteeringTimerFactory{}
	machine.timerFactory = factory.new
	if err := machine.setStream(orderedUnitStream()); err != nil {
		t.Fatalf("set stream: %v", err)
	}
	input := unitPreparedInput(t, l, "fallback before deadline")
	if _, err := machine.offer(input); err != nil {
		t.Fatalf("offer: %v", err)
	}
	awaitStarted(t, steerer.started)
	timers, _ := factory.snapshot()
	if err := machine.observe(driver.SteerObservation{SteerResult: fallbackSteerResult()}); err != nil {
		t.Fatalf("fallback observation: %v", err)
	}
	if got, want := hook.snapshot(), []string{"reserve", "fallback"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("fallback calls = %v, want %v", got, want)
	}
	if machine.active == nil || !machine.active.resolved {
		t.Fatal("fallback did not win the adjudication CAS")
	}

	// The deadline may retire the already-resolved attempt, but it must not
	// publish Unknown or queue a second fallback.
	timers[0].fire()
	select {
	case <-machine.deadlineTimerChan():
	case <-time.After(time.Second):
		t.Fatal("fallback deadline did not reach the actor")
	}
	if err := machine.deadlineTimeout(); err != nil {
		t.Fatalf("fallback deadline retirement: %v", err)
	}
	if machine.active != nil {
		t.Fatalf("fallback active attempt = %p, want retired", machine.active)
	}
	if got, want := hook.snapshot(), []string{"reserve", "fallback"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("deadline after fallback changed calls = %v, want %v", got, want)
	}
	close(steerer.release)
	if err := machine.complete(takeCompletion(t, machine)); err != nil {
		t.Fatalf("late fallback completion: %v", err)
	}
}

func TestSteeringDeadlineQueuesPendingFIFO(t *testing.T) {
	steerer := &ignoringSteerer{started: make(chan struct{}, 1), release: make(chan struct{})}
	hook := &recordingDeliveryHook{}
	l, machine, _, cancel := newSteeringUnit(t, steerer, hook)
	t.Cleanup(cancel)
	factory := &manualSteeringTimerFactory{}
	machine.timerFactory = factory.new
	if err := machine.setStream(orderedUnitStream()); err != nil {
		t.Fatalf("set stream: %v", err)
	}
	first := unitPreparedInput(t, l, "deadline first")
	second := unitPreparedInput(t, l, "queued second")
	if _, err := machine.offer(first); err != nil {
		t.Fatalf("offer first: %v", err)
	}
	if _, err := machine.offer(second); err != nil {
		t.Fatalf("offer second: %v", err)
	}
	awaitStarted(t, steerer.started)
	timers, _ := factory.snapshot()
	if len(timers) != 1 {
		t.Fatalf("reservation timers = %d, want one active timer", len(timers))
	}

	timers[0].fire()
	select {
	case <-machine.deadlineTimerChan():
	case <-time.After(time.Second):
		t.Fatal("deadline did not reach the actor")
	}
	if err := machine.deadlineTimeout(); err != nil {
		t.Fatalf("deadline timeout: %v", err)
	}
	if machine.active != nil || len(machine.pending) != 0 {
		t.Fatalf("machine after deadline = active:%p pending:%d, want no active machine work", machine.active, len(machine.pending))
	}
	if got, want := hook.snapshot(), []string{"reserve", string(foreign.DeliveryResolutionUnknown), "fallback"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("deadline FIFO calls = %v, want %v", got, want)
	}
	if got, want := hook.fallbackIDs(), []uuid.UUID{second.command.CommandID}; !reflect.DeepEqual(got, want) {
		t.Fatalf("deadline FIFO fallback IDs = %v, want queued request %v", got, want)
	}
	if len(l.pending) != 1 || l.pending[0].command.CommandID != second.command.CommandID {
		t.Fatalf("loop pending after deadline = %#v, want second request only", l.pending)
	}
	close(steerer.release)
	if err := machine.complete(takeCompletion(t, machine)); err != nil {
		t.Fatalf("late first completion: %v", err)
	}
}

func TestAwaitTurnStaleDeadlineCannotAdjudicatePumpedNextAttempt(t *testing.T) {
	steerer := &ignoringSteerer{started: make(chan struct{}, 4), release: make(chan struct{})}
	hook := &recordingDeliveryHook{}
	l, machine, _, cancelSteerer := newSteeringUnit(t, steerer, hook)
	factory := &manualSteeringTimerFactory{}
	machine.timerFactory = factory.new
	if err := machine.setStream(orderedUnitStream()); err != nil {
		t.Fatalf("set stream: %v", err)
	}
	first := unitPreparedInput(t, l, "stale deadline first")
	first.command.CommandID = first.command.Cause.CommandID
	second := unitPreparedInput(t, l, "stale deadline second")
	second.command.CommandID = second.command.Cause.CommandID
	if _, err := machine.offer(first); err != nil {
		t.Fatalf("offer first: %v", err)
	}
	if _, err := machine.offer(second); err != nil {
		t.Fatalf("offer second: %v", err)
	}
	awaitStarted(t, steerer.started)
	timers, _ := factory.snapshot()
	if len(timers) != 1 {
		t.Fatalf("initial timers = %d, want one", len(timers))
	}
	firstAttempt := machine.active
	injected := injectedSteerResult()
	machine.completions <- steeringCompletion{attempt: firstAttempt, result: injected}
	mailbox := make(chan turnObservation, 1)
	mailbox <- turnObservation{raw: driver.SteerObservation{SteerResult: injected}}
	loopCtx, stop := context.WithCancel(context.Background())
	awaitDone := make(chan bool, 1)
	go func() {
		awaitDone <- l.awaitTurn(loopCtx, 1, first.command.CommandID, first.turnID, first.stepID,
			cancelSteerer, l.publisher(loopCtx, first.turnID, first.stepID), mailbox, make(chan turnOutcome), nil, machine)
	}()
	timers[0].fire()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		calls := hook.snapshot()
		if len(calls) >= 3 {
			break
		}
		time.Sleep(time.Millisecond)
	}
	if got := hook.snapshot(); !reflect.DeepEqual(got, []string{"reserve", "resolve-injected", "reserve"}) {
		t.Fatalf("stale deadline calls = %v, want first injection and second reservation only", got)
	}
	secondTimers, _ := factory.snapshot()
	if len(secondTimers) < 2 {
		t.Fatalf("timers after pump = %d, want second deadline", len(secondTimers))
	}
	secondTimers[1].fire()
	stop()
	select {
	case <-awaitDone:
	case <-time.After(time.Second):
		t.Fatal("awaitTurn did not finish after second deadline")
	}
	close(steerer.release)
}

func TestAwaitTurnAdjudicationFailureStopsReservationTimer(t *testing.T) {
	sentinel := errors.New("resolution publication failed")
	steerer := &ignoringSteerer{started: make(chan struct{}, 1), release: make(chan struct{})}
	hook := &recordingDeliveryHook{}
	l, machine, _, cancelSteerer := newSteeringUnit(t, steerer, hook)
	factory := &manualSteeringTimerFactory{}
	machine.timerFactory = factory.new
	if err := machine.setStream(orderedUnitStream()); err != nil {
		t.Fatalf("set stream: %v", err)
	}
	input := unitPreparedInput(t, l, "adjudication failure cleanup")
	if _, err := machine.offer(input); err != nil {
		t.Fatalf("offer: %v", err)
	}
	awaitStarted(t, steerer.started)
	timers, _ := factory.snapshot()
	hook.mu.Lock()
	hook.err = sentinel
	hook.mu.Unlock()
	loopCtx, stop := context.WithCancel(context.Background())
	awaitDone := make(chan bool, 1)
	go func() {
		awaitDone <- l.awaitTurn(loopCtx, 1, input.command.CommandID, input.turnID, input.stepID,
			cancelSteerer, l.publisher(loopCtx, input.turnID, input.stepID), nil, make(chan turnOutcome), nil, machine)
	}()
	timers[0].fire()
	select {
	case <-awaitDone:
	case <-time.After(time.Second):
		t.Fatal("awaitTurn did not return after adjudication failure")
	}
	stop()
	if machine.active != nil || machine.deadlineTimerChan() != nil || machine.timerChan() != nil {
		t.Fatalf("failed await cleanup = active:%p deadline:%v terminal:%v, want detached timers", machine.active, machine.deadlineTimerChan(), machine.timerChan())
	}
	close(steerer.release)
}

func TestSteeringAckResolutionStopsReservationDeadline(t *testing.T) {
	steerer := &scriptedSteerer{started: make(chan struct{}, 1), results: make(chan scriptedSteerResult, 1)}
	hook := &recordingDeliveryHook{}
	l, machine, _, cancel := newSteeringUnit(t, steerer, hook)
	t.Cleanup(cancel)
	factory := &manualSteeringTimerFactory{}
	machine.timerFactory = factory.new
	if err := machine.setStream(orderedUnitStream()); err != nil {
		t.Fatalf("set stream: %v", err)
	}
	input := unitPreparedInput(t, l, "ack stops deadline")
	if _, err := machine.offer(input); err != nil {
		t.Fatalf("offer: %v", err)
	}
	awaitStarted(t, steerer.started)
	timers, _ := factory.snapshot()
	if len(timers) != 1 {
		t.Fatalf("reservation timers = %d, want one", len(timers))
	}
	result := injectedSteerResult()
	if err := machine.observe(driver.SteerObservation{SteerResult: result}); err != nil {
		t.Fatalf("injected observation: %v", err)
	}
	steerer.results <- scriptedSteerResult{result: result}
	if err := machine.complete(takeCompletion(t, machine)); err != nil {
		t.Fatalf("injected completion: %v", err)
	}
	if machine.active != nil {
		t.Fatalf("ack-resolved active attempt = %p, want retired", machine.active)
	}
	// A stale fire is suppressed by the stopped timer and cannot invoke the
	// deadline path after the checked acknowledgement resolution.
	timers[0].fire()
	if got, want := hook.snapshot(), []string{"reserve", "resolve-injected"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("stale deadline changed calls = %v, want %v", got, want)
	}
}

func TestSteeringDeadlineWinsLateFallbackIsIgnored(t *testing.T) {
	steerer := &ignoringSteerer{started: make(chan struct{}, 1), release: make(chan struct{})}
	hook := &recordingDeliveryHook{}
	l, machine, _, cancel := newSteeringUnit(t, steerer, hook)
	t.Cleanup(cancel)
	factory := &manualSteeringTimerFactory{}
	machine.timerFactory = factory.new
	if err := machine.setStream(orderedUnitStream()); err != nil {
		t.Fatalf("set stream: %v", err)
	}
	input := unitPreparedInput(t, l, "deadline before fallback")
	if _, err := machine.offer(input); err != nil {
		t.Fatalf("offer: %v", err)
	}
	awaitStarted(t, steerer.started)
	timers, _ := factory.snapshot()
	timers[0].fire()
	select {
	case <-machine.deadlineTimerChan():
	case <-time.After(time.Second):
		t.Fatal("deadline did not reach the actor")
	}
	if err := machine.deadlineTimeout(); err != nil {
		t.Fatalf("deadline timeout: %v", err)
	}
	if err := machine.observe(driver.SteerObservation{SteerResult: fallbackSteerResult()}); err != nil {
		t.Fatalf("late fallback observation: %v", err)
	}
	if got, want := hook.snapshot(), []string{"reserve", string(foreign.DeliveryResolutionUnknown)}; !reflect.DeepEqual(got, want) {
		t.Fatalf("late fallback changed calls = %v, want %v", got, want)
	}
	close(steerer.release)
	if err := machine.complete(takeCompletion(t, machine)); err != nil {
		t.Fatalf("late completion: %v", err)
	}
}

func TestSteeringShutdownFinalizesUnresolvedReservation(t *testing.T) {
	steerer := &ignoringSteerer{started: make(chan struct{}, 1), release: make(chan struct{})}
	hook := &recordingDeliveryHook{}
	l, machine, _, cancel := newSteeringUnit(t, steerer, hook)
	t.Cleanup(cancel)
	if err := machine.setStream(orderedUnitStream()); err != nil {
		t.Fatalf("set stream: %v", err)
	}
	input := unitPreparedInput(t, l, "clean shutdown")
	if _, err := machine.offer(input); err != nil {
		t.Fatalf("offer: %v", err)
	}
	awaitStarted(t, steerer.started)
	if err := machine.shutdown(); err != nil {
		t.Fatalf("shutdown: %v", err)
	}
	if machine.active != nil {
		t.Fatalf("shutdown active attempt = %p, want retired", machine.active)
	}
	if got, want := hook.snapshot(), []string{"reserve", string(foreign.DeliveryResolutionUnknown)}; !reflect.DeepEqual(got, want) {
		t.Fatalf("shutdown calls = %v, want %v", got, want)
	}
	close(steerer.release)
	if err := machine.complete(takeCompletion(t, machine)); err != nil {
		t.Fatalf("late shutdown completion: %v", err)
	}
}

func TestShutdownDoesNotWaitForNoncooperativeTurnOutcome(t *testing.T) {
	steerer := &ignoringSteerer{started: make(chan struct{}, 1), release: make(chan struct{})}
	hook := &recordingDeliveryHook{}
	l, machine, _, cancel := newSteeringUnit(t, steerer, hook)
	factory := &manualSteeringTimerFactory{}
	machine.timerFactory = factory.new
	if err := machine.setStream(orderedUnitStream()); err != nil {
		t.Fatalf("set stream: %v", err)
	}
	input := unitPreparedInput(t, l, "shutdown noncooperative")
	if _, err := machine.offer(input); err != nil {
		t.Fatalf("offer: %v", err)
	}
	awaitStarted(t, steerer.started)
	timers, _ := factory.snapshot()
	if len(timers) != 1 {
		t.Fatalf("reservation timers = %d, want one", len(timers))
	}

	mailbox := make(chan turnObservation)
	result := make(chan turnOutcome)
	ack := make(chan error, 1)
	commandDone := make(chan struct {
		done bool
		exit bool
	}, 1)
	cancelCalled := make(chan struct{})
	var cancelOnce sync.Once
	var releaseOnce sync.Once
	finished := false
	cancelForShutdown := func() {
		cancelOnce.Do(func() { close(cancelCalled) })
		cancel()
	}
	releaseSteerer := func() { releaseOnce.Do(func() { close(steerer.release) }) }
	t.Cleanup(func() {
		releaseSteerer()
		if finished {
			return
		}
		result <- turnOutcome{}
		close(mailbox)
		<-commandDone
	})
	shutdown := command.Shutdown{Ack: ack}
	go func() {
		done, exit := l.handleTurnCommand(
			context.Background(), shutdown, 1, mustID(t), input.turnID, input.stepID,
			cancelForShutdown, l.publisher(context.Background(), input.turnID, input.stepID), mailbox, result, machine,
		)
		commandDone <- struct {
			done bool
			exit bool
		}{done: done, exit: exit}
	}()
	<-cancelCalled
	timers[0].fire()

	// The shutdown path must finish after the actor-owned deadline even though
	// neither the provider turn outcome nor its mailbox ever closes.
	select {
	case outcome := <-commandDone:
		finished = true
		if !outcome.done || !outcome.exit {
			t.Fatalf("shutdown result = done:%t exit:%t, want done/exit", outcome.done, outcome.exit)
		}
	case <-time.After(time.Second):
		t.Fatal("shutdown waited for noncooperative turn outcome after deadline")
	}
	if err := <-ack; err != nil {
		t.Fatalf("shutdown ack = %v, want nil", err)
	}
	if got, want := hook.snapshot(), []string{"reserve", string(foreign.DeliveryResolutionUnknown)}; !reflect.DeepEqual(got, want) {
		t.Fatalf("shutdown calls = %v, want %v", got, want)
	}
	releaseSteerer()
}

func TestActiveCancellationDoesNotWaitForNoncooperativeTurnOutcome(t *testing.T) {
	for _, name := range []string{"cancel", "interrupt"} {
		name := name
		t.Run(name, func(t *testing.T) {
			steerer := &ignoringSteerer{started: make(chan struct{}, 1), release: make(chan struct{})}
			hook := &recordingDeliveryHook{}
			l, machine, _, cancel := newSteeringUnit(t, steerer, hook)
			t.Cleanup(func() { close(steerer.release); cancel() })
			factory := &manualSteeringTimerFactory{}
			machine.timerFactory = factory.new
			if err := machine.setStream(orderedUnitStream()); err != nil {
				t.Fatalf("set stream: %v", err)
			}
			input := unitPreparedInput(t, l, "cancel noncooperative")
			input.command.CommandID = input.command.Cause.CommandID
			if _, err := machine.offer(input); err != nil {
				t.Fatalf("offer: %v", err)
			}
			awaitStarted(t, steerer.started)
			timers, _ := factory.snapshot()
			if len(timers) != 1 {
				t.Fatalf("reservation timers = %d, want one", len(timers))
			}
			mailbox := make(chan turnObservation)
			result := make(chan turnOutcome)
			commandDone := make(chan struct{ done, exit bool }, 1)
			cancelCalled := make(chan struct{})
			var once sync.Once
			cancelForCommand := func() {
				once.Do(func() { close(cancelCalled) })
				cancel()
			}
			var inputCommand command.Command
			var cancelAck chan command.DelegateCancelResult
			var interruptAck chan bool
			if name == "cancel" {
				cancelAck = make(chan command.DelegateCancelResult, 1)
				inputCommand = command.CancelDelegateRequest{
					Header:      command.Header{CommandID: mustID(t)},
					Coordinates: identity.Coordinates{SessionID: l.sessionID, LoopID: l.loopID}, TargetCommandID: input.command.CommandID, Ack: cancelAck,
				}
			} else {
				interruptAck = make(chan bool, 1)
				inputCommand = command.Interrupt{Ack: interruptAck}
			}
			go func() {
				done, exit := l.handleTurnCommand(context.Background(), inputCommand, 1, input.command.CommandID,
					input.turnID, input.stepID, cancelForCommand, l.publisher(context.Background(), input.turnID, input.stepID), mailbox, result, machine)
				commandDone <- struct{ done, exit bool }{done: done, exit: exit}
			}()
			<-cancelCalled
			timers[0].fire()
			select {
			case outcome := <-commandDone:
				if !outcome.done || outcome.exit {
					t.Fatalf("%s result = done:%t exit:%t, want done/continue", name, outcome.done, outcome.exit)
				}
			case <-time.After(time.Second):
				t.Fatalf("%s waited for noncooperative turn outcome", name)
			}
			if name == "cancel" {
				if got := <-cancelAck; got != command.DelegateCancelActive {
					t.Fatalf("cancel ack = %v, want active", got)
				}
			} else if !<-interruptAck {
				t.Fatal("interrupt ack = false, want true")
			}
		})
	}
}

func TestShutdownAdjudicationFailureAcknowledgesError(t *testing.T) {
	sentinel := errors.New("shutdown resolution failed")
	steerer := &ignoringSteerer{started: make(chan struct{}, 1), release: make(chan struct{})}
	hook := &recordingDeliveryHook{}
	l, machine, _, cancel := newSteeringUnit(t, steerer, hook)
	factory := &manualSteeringTimerFactory{}
	machine.timerFactory = factory.new
	if err := machine.setStream(orderedUnitStream()); err != nil {
		t.Fatalf("set stream: %v", err)
	}
	input := unitPreparedInput(t, l, "shutdown error")
	if _, err := machine.offer(input); err != nil {
		t.Fatalf("offer: %v", err)
	}
	awaitStarted(t, steerer.started)
	timers, _ := factory.snapshot()
	hook.mu.Lock()
	hook.err = sentinel
	hook.mu.Unlock()

	mailbox := make(chan turnObservation)
	result := make(chan turnOutcome)
	ack := make(chan error, 1)
	commandDone := make(chan struct {
		done bool
		exit bool
	}, 1)
	cancelCalled := make(chan struct{})
	var cancelOnce sync.Once
	var releaseOnce sync.Once
	finished := false
	cancelForShutdown := func() {
		cancelOnce.Do(func() { close(cancelCalled) })
		cancel()
	}
	releaseSteerer := func() { releaseOnce.Do(func() { close(steerer.release) }) }
	t.Cleanup(func() {
		releaseSteerer()
		if finished {
			return
		}
		result <- turnOutcome{}
		close(mailbox)
		<-commandDone
	})
	shutdown := command.Shutdown{Ack: ack}
	go func() {
		done, exit := l.handleTurnCommand(
			context.Background(), shutdown, 1, mustID(t), input.turnID, input.stepID,
			cancelForShutdown, l.publisher(context.Background(), input.turnID, input.stepID), mailbox, result, machine,
		)
		commandDone <- struct {
			done bool
			exit bool
		}{done: done, exit: exit}
	}()
	<-cancelCalled
	timers[0].fire()
	select {
	case outcome := <-commandDone:
		finished = true
		if !outcome.done || !outcome.exit {
			t.Fatalf("shutdown result = done:%t exit:%t, want done/exit", outcome.done, outcome.exit)
		}
	case <-time.After(time.Second):
		t.Fatal("shutdown resolution failure did not return")
	}
	if err := <-ack; !errors.Is(err, sentinel) {
		t.Fatalf("shutdown ack = %v, want %v", err, sentinel)
	}
	releaseSteerer()
}

func TestAwaitTurnNormalCompletionRetiresResolvedSteeringAttempt(t *testing.T) {
	steerer := &ignoringSteerer{started: make(chan struct{}, 1), release: make(chan struct{})}
	hook := &recordingDeliveryHook{}
	l, machine, _, cancel := newSteeringUnit(t, steerer, hook)
	factory := &manualSteeringTimerFactory{}
	machine.timerFactory = factory.new
	if err := machine.setStream(orderedUnitStream()); err != nil {
		t.Fatalf("set stream: %v", err)
	}
	input := unitPreparedInput(t, l, "normal completion cleanup")
	if _, err := machine.offer(input); err != nil {
		t.Fatalf("offer: %v", err)
	}
	awaitStarted(t, steerer.started)
	timers, _ := factory.snapshot()
	if len(timers) != 1 {
		t.Fatalf("reservation timers = %d, want one", len(timers))
	}
	if err := machine.observe(driver.SteerObservation{SteerResult: fallbackSteerResult()}); err != nil {
		t.Fatalf("fallback observation: %v", err)
	}
	if machine.active == nil || !machine.active.resolved {
		t.Fatal("fallback did not resolve before turn outcome")
	}
	result := make(chan turnOutcome, 1)
	result <- turnOutcome{success: true}
	mailbox := make(chan turnObservation)
	close(mailbox)
	streamReady := make(chan driver.Stream)
	if exited := l.awaitTurn(context.Background(), 1, mustID(t), input.turnID, input.stepID, cancel,
		l.publisher(context.Background(), input.turnID, input.stepID), mailbox, result, streamReady, machine); exited {
		t.Fatal("normal completion unexpectedly exited the loop")
	}
	if machine.active != nil {
		t.Fatalf("normal completion active attempt = %p, want retired", machine.active)
	}
	if machine.deadlineTimerChan() != nil {
		t.Fatal("normal completion left reservation deadline armed")
	}
	timers[0].fire()
	if got, want := hook.snapshot(), []string{"reserve", "fallback"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("stale normal-completion deadline changed calls = %v, want %v", got, want)
	}
	close(steerer.release)
	if err := machine.complete(takeCompletion(t, machine)); err != nil {
		t.Fatalf("late completion: %v", err)
	}
}
