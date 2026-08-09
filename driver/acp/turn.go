package acp

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"reflect"
	"regexp"
	"strings"
	"sync"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/looprig/acp/client"
	"github.com/looprig/acp/protocol"
	"github.com/looprig/core/content"
	"github.com/looprig/foreignloops/driver"
)

// turnSession is the live-session portion used after construction. Keeping it
// separate from session lets construction tests use a narrow setup seam while
// the concrete ACP client supplies prompt and update delivery for turns.
type turnSession interface {
	session
	Prompt(context.Context, []protocol.ContentBlock) (*client.PromptResult, error)
	Updates() <-chan client.Update
	Cancel(context.Context) error
	WaitForUpdates(context.Context) error
}

type orderedUpdateBarrier interface {
	WaitForUpdatesThrough(context.Context, uint64) error
}

// promptSession is the pre-barrier ACP client shape. The checked-out ACP
// client does not expose WaitForUpdates yet, so Spawn wraps this legacy shape
// with the compatibility stub below. Once the client-side ordered barrier
// lands, *client.Session will satisfy turnSession directly and the wrapper is
// bypassed.
type promptSession interface {
	session
	Prompt(context.Context, []protocol.ContentBlock) (*client.PromptResult, error)
	Updates() <-chan client.Update
	Cancel(context.Context) error
}

type legacyTurnSession struct {
	promptSession
}

// WaitForUpdates is an explicit compatibility seam for ACP clients built
// before the ordered-delivery API. It intentionally has no behavior; the
// client implementation that provides the barrier satisfies turnSession
// directly and never uses this fallback.
func (legacyTurnSession) WaitForUpdates(context.Context) error { return nil }

type promptOutcome struct {
	result *client.PromptResult
	err    error
}

// turnLifecycle closes the small race between a prompt completing and its
// caller context being cancelled. A watcher that wins the race reserves the
// cancellation before calling the ACP session; a completed turn prevents a
// late session/cancel from reaching the next turn.
type turnLifecycle struct {
	mu         sync.Mutex
	finished   bool
	cancelling bool
}

func (l *turnLifecycle) beginCancel() bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.finished || l.cancelling {
		return false
	}
	l.cancelling = true
	return true
}

func (l *turnLifecycle) finish() {
	l.mu.Lock()
	l.finished = true
	l.mu.Unlock()
}

// stream is one prompt view over Driver's persistent ACP session. Its close
// function only cancels forwarding for this turn; the session and its owned
// process remain with Driver.
type stream struct {
	events       chan driver.Event
	observations chan driver.Observation
	done         <-chan struct{}
	cancel       context.CancelFunc
	steerIn      chan steerInput

	mu                  sync.Mutex
	closed              bool
	selected            streamView
	closedEvents        bool
	closedObservations  bool
	pendingEvents       []driver.Event
	pendingObservations []driver.Observation
	nextOrder           uint64
	steers              map[*steerCall]struct{}
	terminal            bool

	once     sync.Once
	closeErr error
}

type streamView uint8

const (
	viewUnselected streamView = iota
	viewEvents
	viewObservations
)

type steerInput struct {
	observation driver.SteerObservation
	admitted    chan struct{}
}

type steerCall struct {
	admitted  chan struct{}
	finalized bool
}

// Spawn starts one prompt on the session created by New. The prompt itself is
// deliberately run under the driver's context. The caller's context controls
// this stream's forwarding lifetime, and the driver's context cancels it when
// Driver.Close is called; protocol cancellation is handled by the interrupt
// watcher in the turn phase.
func (d *Driver) Spawn(ctx context.Context, turn driver.Turn) (driver.Stream, error) {
	if d == nil || d.session == nil {
		return nil, &driver.SpawnError{Cause: errors.New("acp: session unavailable")}
	}
	sess, ok := d.session.(turnSession)
	if !ok {
		legacy, legacyOK := d.session.(promptSession)
		if legacyOK {
			sess = legacyTurnSession{promptSession: legacy}
			ok = true
		}
	}
	if !ok {
		return nil, &driver.SpawnError{Cause: errors.New("acp: session does not support turns")}
	}
	if ctx == nil {
		ctx = context.Background()
	}
	d.turnMu.Lock()
	if d.closed {
		d.turnMu.Unlock()
		return nil, &driver.SpawnError{Cause: errors.New("acp: driver is closed")}
	}

	driverCtx := d.driverCtx
	if driverCtx == nil {
		driverCtx = context.Background()
	}
	turnCtx, cancel := context.WithCancel(ctx)
	events := make(chan driver.Event, 2048)
	done := make(chan struct{})
	s := &stream{
		events:       events,
		observations: make(chan driver.Observation, 2048),
		done:         done,
		cancel:       cancel,
		steerIn:      make(chan steerInput, 512),
		steers:       make(map[*steerCall]struct{}),
	}
	d.activeMu.Lock()
	d.active = s
	d.activeMu.Unlock()
	go d.runTurn(turnCtx, cancel, driverCtx, sess, turn, s, events, done)
	return s, nil
}

func (s *stream) registerSteer() (*steerCall, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed || s.terminal {
		return nil, false
	}
	call := &steerCall{admitted: make(chan struct{})}
	s.steers[call] = struct{}{}
	return call, true
}

func (s *stream) completeSteer(call *steerCall) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.steers[call]; !ok {
		return false
	}
	delete(s.steers, call)
	return !call.finalized
}

func (s *stream) holdTerminalSteers() []*steerCall {
	s.mu.Lock()
	s.terminal = true
	calls := make([]*steerCall, 0, len(s.steers))
	for call := range s.steers {
		calls = append(calls, call)
	}
	s.mu.Unlock()
	return calls
}

func (s *stream) finalizeSteer(call *steerCall) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.steers[call]; !ok {
		return false
	}
	call.finalized = true
	delete(s.steers, call)
	return true
}

func (d *Driver) runTurn(
	turnCtx context.Context,
	turnCancel context.CancelFunc,
	driverCtx context.Context,
	sess turnSession,
	turn driver.Turn,
	streamState *stream,
	events chan<- driver.Event,
	done chan<- struct{},
) {
	pendingUpdates := d.takePendingUpdates()
	turnDone := make(chan struct{})
	watcherDone := make(chan struct{})
	lifecycle := &turnLifecycle{}
	go watchTurnCancellation(turnCtx, driverCtx, turnCancel, sess, turnDone, watcherDone, lifecycle)
	defer func() {
		lifecycle.finish()
		close(turnDone)
		<-watcherDone
		turnCancel()
		streamState.mu.Lock()
		streamState.closed = true
		streamState.mu.Unlock()
		d.returnPendingUpdates(pendingUpdates)
		for {
			select {
			case pending := <-streamState.steerIn:
				close(pending.admitted)
			default:
				goto pendingSteersDrained
			}
		}
	pendingSteersDrained:
		close(done)
		streamState.mu.Lock()
		if !streamState.closedEvents {
			close(streamState.events)
			streamState.closedEvents = true
		}
		if !streamState.closedObservations {
			close(streamState.observations)
			streamState.closedObservations = true
		}
		streamState.mu.Unlock()
		d.activeMu.Lock()
		if d.active == streamState {
			d.active = nil
		}
		d.activeMu.Unlock()
		d.turnMu.Unlock()
	}()

	promptDone := make(chan promptOutcome, 1)
	go func() {
		result, err := sess.Prompt(driverCtx, promptBlocks(turn))
		promptDone <- promptOutcome{result: result, err: err}
	}()

	state := &translationState{}
	updates := sess.Updates()
	for {
		if len(pendingUpdates) > 0 {
			update := pendingUpdates[0]
			pendingUpdates = pendingUpdates[1:]
			emitTranslatedUpdate(turnCtx, streamState, update, state, events)
			continue
		}
		select {
		case update, ok := <-updates:
			if !ok {
				updates = nil
				continue
			}
			emitTranslatedUpdate(turnCtx, streamState, update, state, events)
		case steer, ok := <-streamState.steerIn:
			if !ok {
				continue
			}
			processSteerInput(driverCtx, turnCtx, sess, updates, &pendingUpdates, state, events, streamState, steer)
		case outcome := <-promptDone:
			// The prompt response can become visible before the ordered
			// notification handler has delivered the final session/update.
			// Wait for the client barrier in a separate goroutine while this
			// goroutine keeps consuming Updates; the client may need that
			// consumer to make the barrier progress.
			barrierDone := make(chan error, 1)
			go func() {
				barrierDone <- waitForPromptUpdates(driverCtx, sess, promptSequence(outcome))
			}()
			for {
				if len(pendingUpdates) > 0 {
					update := pendingUpdates[0]
					pendingUpdates = pendingUpdates[1:]
					emitTranslatedUpdate(turnCtx, streamState, update, state, events)
					continue
				}
				select {
				case update, ok := <-updates:
					if !ok {
						updates = nil
						continue
					}
					emitTranslatedUpdate(turnCtx, streamState, update, state, events)
				case steer, ok := <-streamState.steerIn:
					if !ok {
						continue
					}
					processSteerInput(driverCtx, turnCtx, sess, updates, &pendingUpdates, state, events, streamState, steer)
				case err := <-barrierDone:
					if err != nil && driverCtx.Err() == nil {
						// Keep the diagnostic fixed-category: ACP errors may
						// contain protocol payloads or credentials.
						slog.Warn("acp: update delivery barrier failed")
					}
					// The barrier covers ordered handler delivery. Drain steering
					// requests already admitted to this stream, but never wait for
					// an ACP call still running in another goroutine: that call is
					// bounded solely by its request context.
					for {
						select {
						case steer, ok := <-streamState.steerIn:
							if !ok {
								continue
							}
							processSteerInput(driverCtx, turnCtx, sess, updates, &pendingUpdates, state, events, streamState, steer)
						default:
							goto pendingSteerInputsDrained
						}
					}
				pendingSteerInputsDrained:
					pendingCalls := streamState.holdTerminalSteers()
					if len(pendingCalls) > 0 {
						timer := time.NewTimer(100 * time.Millisecond)
						<-timer.C
						for _, call := range pendingCalls {
							if streamState.finalizeSteer(call) {
								emitObservation(streamState, driver.SteerObservation{SteerResult: driver.SteerResult{Outcome: driver.SteerOutcomeDeliveryUnknown}, Err: errors.New("acp: steer response unavailable")})
								close(call.admitted)
							}
						}
					}
					drainTurnUpdatesThrough(turnCtx, updates, &pendingUpdates, state, events, streamState, promptSequence(outcome))
					emitPromptObservation(streamState, outcome)
					sendPromptTerminal(turnCtx, streamState, events, state, outcome)
					return
				}
			}
		}
	}
}

func processSteerInput(
	driverCtx, turnCtx context.Context,
	sess turnSession,
	updates <-chan client.Update,
	pending *[]client.Update,
	state *translationState,
	events chan<- driver.Event,
	streamState *stream,
	steer steerInput,
) {
	if sequence := steer.observation.Sequence(); sequence != 0 {
		if err := waitForSteeringUpdates(driverCtx, sess, sequence); err != nil && driverCtx.Err() == nil {
			slog.Warn("acp: steering update barrier failed")
		}
		drainTurnUpdatesThrough(turnCtx, updates, pending, state, events, streamState, sequence)
	}
	emitObservation(streamState, steer.observation)
	close(steer.admitted)
}

func waitForSteeringUpdates(ctx context.Context, sess turnSession, sequence uint64) error {
	if ordered, ok := sess.(orderedUpdateBarrier); ok {
		return ordered.WaitForUpdatesThrough(ctx, sequence)
	}
	return sess.WaitForUpdates(ctx)
}

func waitForPromptUpdates(ctx context.Context, sess turnSession, sequence uint64) error {
	if ordered, ok := sess.(orderedUpdateBarrier); ok && sequence != 0 {
		return ordered.WaitForUpdatesThrough(ctx, sequence)
	}
	return sess.WaitForUpdates(ctx)
}

func emitTranslatedUpdate(ctx context.Context, streamState *stream, update client.Update, state *translationState, events chan<- driver.Event) {
	for _, event := range translateLiveUpdate(update, state) {
		emitObservation(streamState, driver.UpdateObservation{Event: event, ReceiveSequence: update.ReceiveSequence})
		sendTurnEvent(ctx, streamState, events, event)
	}
}

func emitPromptObservation(streamState *stream, outcome promptOutcome) {
	if outcome.result == nil {
		emitObservation(streamState, driver.PromptObservation{Err: outcome.err})
		return
	}
	sequence := outcome.result.ReceiveSequence
	if sequence == 0 {
		sequence = outcome.result.ResponseSequence
	}
	emitObservation(streamState, driver.PromptObservation{
		StopReason:       string(outcome.result.StopReason),
		WriteAdmitted:    outcome.result.WriteAdmitted,
		ReceiveSequence:  sequence,
		ResponseSequence: sequence,
		Err:              outcome.err,
	})
}

func watchTurnCancellation(
	turnCtx, driverCtx context.Context,
	turnCancel context.CancelFunc,
	sess turnSession,
	turnDone <-chan struct{},
	watcherDone chan<- struct{},
	lifecycle *turnLifecycle,
) {
	defer close(watcherDone)
	select {
	case <-turnCtx.Done():
	case <-driverCtx.Done():
		// Driver.Close owns driverCtx, while turnCtx belongs to this stream.
		// Cancel forwarding before attempting protocol cancellation so an
		// abandoned full Events channel cannot hold turnMu indefinitely.
		turnCancel()
	case <-turnDone:
		return
	}
	if !lifecycle.beginCancel() {
		return
	}
	if err := sess.Cancel(driverCtx); err != nil {
		// Keep cancellation diagnostics fixed-category and avoid copying ACP
		// error text, which may contain protocol payloads or credentials.
		slog.Warn("acp: session cancel failed")
	}
}

func drainTurnUpdates(
	turnCtx context.Context,
	updates <-chan client.Update,
	state *translationState,
	events chan<- driver.Event,
) {
	drainTurnUpdatesOrdered(turnCtx, updates, state, events, nil)
}

func drainTurnUpdatesOrdered(
	turnCtx context.Context,
	updates <-chan client.Update,
	state *translationState,
	events chan<- driver.Event,
	streamState *stream,
) {
	var pending []client.Update
	drainTurnUpdatesThrough(turnCtx, updates, &pending, state, events, streamState, 0)
}

func drainTurnUpdatesThrough(
	turnCtx context.Context,
	updates <-chan client.Update,
	pending *[]client.Update,
	state *translationState,
	events chan<- driver.Event,
	streamState *stream,
	boundary uint64,
) {
	if updates == nil {
		return
	}
	for {
		select {
		case update, ok := <-updates:
			if !ok {
				return
			}
			if boundary != 0 && update.ReceiveSequence > boundary {
				*pending = append(*pending, update)
				return
			}
			if streamState == nil {
				for _, event := range translateLiveUpdate(update, state) {
					sendTurnEvent(turnCtx, streamState, events, event)
				}
				continue
			}
			emitTranslatedUpdate(turnCtx, streamState, update, state, events)
		default:
			return
		}
	}
}

func promptSequence(outcome promptOutcome) uint64 {
	if outcome.result == nil {
		return 0
	}
	if outcome.result.ReceiveSequence != 0 {
		return outcome.result.ReceiveSequence
	}
	return outcome.result.ResponseSequence
}

func translateLiveUpdate(update client.Update, state *translationState) []driver.Event {
	if update.Meta.IsReplay {
		slog.Debug("acp: ignored replay session update")
		return nil
	}
	return translateUpdate(update.SessionUpdate, state)
}

func sendTurnEvent(ctx context.Context, streamState *stream, events chan<- driver.Event, event driver.Event) {
	if streamState == nil {
		select {
		case events <- event:
		case <-ctx.Done():
		}
		return
	}
	streamState.mu.Lock()
	selected := streamState.selected
	if selected == viewUnselected {
		streamState.pendingEvents = append(streamState.pendingEvents, event)
		streamState.mu.Unlock()
		return
	}
	streamState.mu.Unlock()
	if selected == viewObservations {
		return
	}
	select {
	case events <- event:
	case <-ctx.Done():
		// The stream is closed or its caller context has ended. Keep draining
		// the ACP session until Prompt returns, but release this stream's
		// consumer without blocking on an abandoned channel.
	}
}

// maxACPModelFacingErrorBytes bounds the complete model-facing projection,
// including its fixed prefix. Keeping this at 512 bytes leaves enough room for
// useful reset-time text while preventing an ACP message from becoming an
// unbounded model-facing payload.
const maxACPModelFacingErrorBytes = 512

const (
	maxACPErrorDepth    = 32
	maxACPErrorNodes    = 128
	maxACPErrorChildren = 64
)

const redactedACPPath = "[REDACTED_PATH]"

var (
	acpMessageURLPattern              = regexp.MustCompile(`(?i)\b[a-z][a-z0-9+.-]*://[^\s<>"']+`)
	acpMessageAuthPattern             = regexp.MustCompile(`(?i)(\b(?:authorization|proxy-authorization)\b\s*["']?\s*[:=]\s*)[^\r\n,;&}\]]+`)
	acpMessageSecretAssignmentPattern = regexp.MustCompile(`(?i)(\b(?:api[\s_-]*key|access[\s_-]*token|refresh[\s_-]*token|token|password|credential|secret)\b\s*["']?\s*[:=]\s*)(?:"[^"]*"|'[^']*'|[^\s,;&}\]]+)`)
	acpMessageBearerPattern           = regexp.MustCompile(`(?i)\bBearer\s+[A-Za-z0-9][A-Za-z0-9._~+/=-]*`)
	acpMessageUnixPathPattern         = regexp.MustCompile(`/[^\s,;)}\]>"']+`)
	acpMessageWindowsPathPattern      = regexp.MustCompile(`(?i)[A-Za-z]:[\\/][^\s,;)}\]>"']*`)
)

func promptFailureEvent(err error) driver.Event {
	if detail, ok := safeACPErrorDetail(err); ok {
		return driver.Event{
			Kind:    driver.KindModelFacingError,
			ErrText: detail,
		}
	}
	return driver.Event{Kind: driver.KindTerminalError, ErrText: "acp prompt failed"}
}

// safeACPErrorDetail intentionally reads only Code and Message from an actual
// ACP protocol failure. It never calls errors.As: an arbitrary error can use
// As(any) bool to fabricate a protocol value, and Error()/Data()/causes may
// contain provider-internal secrets. Only direct protocol values and standard
// Unwrap-shaped wrappers are traversed, with finite bounds for hostile chains.
func safeACPErrorDetail(err error) (string, bool) {
	type node struct {
		err   error
		depth int
	}
	if isNilACPError(err) {
		return "", false
	}
	pending := []node{{err: err}}
	seen := make(map[error]struct{})
	visited := 0
	for len(pending) > 0 && visited < maxACPErrorNodes {
		current := pending[len(pending)-1]
		pending = pending[:len(pending)-1]
		if isNilACPError(current.err) || markACPErrorSeen(seen, current.err) {
			continue
		}
		visited++
		if code, message, ok := directACPErrorFields(current.err); ok {
			return formatACPErrorDetail(code, message), true
		}
		if current.depth >= maxACPErrorDepth {
			continue
		}

		if wrapper, ok := current.err.(interface{ Unwrap() []error }); ok {
			children := safeACPUnwrapMany(wrapper)
			if len(children) > maxACPErrorChildren {
				children = children[:maxACPErrorChildren]
			}
			for index := len(children) - 1; index >= 0; index-- {
				pending = append(pending, node{err: children[index], depth: current.depth + 1})
			}
			continue
		}
		if wrapper, ok := current.err.(interface{ Unwrap() error }); ok {
			pending = append(pending, node{err: safeACPUnwrapOne(wrapper), depth: current.depth + 1})
		}
	}
	return "", false
}

func directACPErrorFields(err error) (protocol.ErrorCode, string, bool) {
	switch typed := any(err).(type) {
	case *protocol.Error:
		if typed == nil {
			return 0, "", false
		}
		return typed.Code, typed.Message, true
	case protocol.Error:
		return typed.Code, typed.Message, true
	case *protocol.Fault:
		if typed == nil {
			return 0, "", false
		}
		return typed.Code, typed.Message, true
	case protocol.Fault:
		return typed.Code, typed.Message, true
	default:
		return 0, "", false
	}
}

func isNilACPError(err error) bool {
	if err == nil {
		return true
	}
	value := reflect.ValueOf(err)
	switch value.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return value.IsNil()
	default:
		return false
	}
}

func markACPErrorSeen(seen map[error]struct{}, err error) bool {
	typeOfError := reflect.TypeOf(err)
	if typeOfError == nil || !typeOfError.Comparable() {
		return false
	}
	if _, ok := seen[err]; ok {
		return true
	}
	seen[err] = struct{}{}
	return false
}

func safeACPUnwrapOne(wrapper interface{ Unwrap() error }) (next error) {
	defer func() {
		if recover() != nil {
			next = nil
		}
	}()
	return wrapper.Unwrap()
}

func safeACPUnwrapMany(wrapper interface{ Unwrap() []error }) (children []error) {
	defer func() {
		if recover() != nil {
			children = nil
		}
	}()
	return wrapper.Unwrap()
}

func formatACPErrorDetail(code protocol.ErrorCode, message string) string {
	message = normalizeACPErrorMessage(message)
	detail := fmt.Sprintf("ACP error %d", code)
	if message != "" {
		detail += ": " + message
	}
	return truncateValidUTF8(detail, maxACPModelFacingErrorBytes)
}

func normalizeACPErrorMessage(message string) string {
	message = strings.ToValidUTF8(message, "\uFFFD")
	var normalized strings.Builder
	normalized.Grow(len(message))
	for _, r := range message {
		if unicode.IsControl(r) || unicode.Is(unicode.Cf, r) || r == '\u2028' || r == '\u2029' {
			normalized.WriteByte(' ')
			continue
		}
		normalized.WriteRune(r)
	}
	return redactACPErrorMessage(strings.Join(strings.Fields(normalized.String()), " "))
}

func redactACPErrorMessage(message string) string {
	message = acpMessageURLPattern.ReplaceAllString(message, redactedURL)
	message = acpMessageAuthPattern.ReplaceAllString(message, "$1"+redactedToolValue)
	message = acpMessageSecretAssignmentPattern.ReplaceAllString(message, "$1"+redactedToolValue)
	message = acpMessageBearerPattern.ReplaceAllString(message, redactedToolValue)
	message = toolCredentialTokenPattern.ReplaceAllString(message, redactedToolValue)
	message = acpMessageWindowsPathPattern.ReplaceAllString(message, redactedACPPath)
	message = acpMessageUnixPathPattern.ReplaceAllString(message, redactedACPPath)
	return message
}

func truncateValidUTF8(input string, maxBytes int) string {
	if maxBytes <= 0 {
		return ""
	}
	if len(input) <= maxBytes {
		return input
	}
	cut := 0
	for cut < len(input) {
		_, size := utf8.DecodeRuneInString(input[cut:])
		if cut+size > maxBytes {
			break
		}
		cut += size
	}
	return input[:cut]
}

func sendPromptTerminal(ctx context.Context, streamState *stream, events chan<- driver.Event, state *translationState, outcome promptOutcome) {
	if outcome.err != nil || outcome.result == nil {
		sendTerminalEvent(ctx, streamState, events, promptFailureEvent(outcome.err))
		return
	}
	if message := state.message(); message != nil {
		sendTurnEvent(ctx, streamState, events, driver.Event{
			Kind:    driver.KindStepComplete,
			Message: message,
		})
	}
	sendTerminalEvent(ctx, streamState, events, terminalEvent(outcome.result.StopReason))
}

// sendTerminalEvent preserves the terminal marker when a cancelled turn is
// still being drained. If the caller has abandoned the stream and its buffer
// is full, the non-blocking fallback keeps the persistent session from being
// wedged behind an unobserved stream; the backend already treats the canceled
// turn context as interrupted.
func sendTerminalEvent(ctx context.Context, streamState *stream, events chan<- driver.Event, event driver.Event) {
	streamState.mu.Lock()
	selected := streamState.selected
	streamState.mu.Unlock()
	if selected == viewObservations {
		return
	}
	select {
	case events <- event:
	case <-ctx.Done():
		select {
		case events <- event:
		default:
		}
	}
}

func terminalEvent(reason protocol.StopReason) driver.Event {
	switch reason {
	case protocol.StopReasonEndTurn, protocol.StopReasonCancelled:
		return driver.Event{Kind: driver.KindTerminalOK}
	case protocol.StopReasonMaxTokens:
		return driver.Event{Kind: driver.KindTerminalError, ErrText: "acp prompt reached its token limit"}
	case protocol.StopReasonMaxTurnRequests:
		return driver.Event{Kind: driver.KindTerminalError, ErrText: "acp prompt reached its turn limit"}
	case protocol.StopReasonRefusal:
		return driver.Event{Kind: driver.KindTerminalError, ErrText: "acp prompt was refused"}
	default:
		return driver.Event{Kind: driver.KindTerminalError, ErrText: "acp prompt ended with an unknown stop reason"}
	}
}

func promptBlocks(turn driver.Turn) []protocol.ContentBlock {
	var task strings.Builder
	task.WriteString("<looprig-system>")
	task.WriteString(turn.SystemPrompt)
	task.WriteString("</looprig-system>\n\n<user-task>")
	for _, block := range turn.Input {
		switch typed := block.(type) {
		case *content.TextBlock:
			task.WriteString(typed.Text)
		case *content.DocumentBlock:
			task.WriteString(typed.Text)
		}
	}
	task.WriteString("</user-task>")
	return []protocol.ContentBlock{{Text: &protocol.TextContent{Text: task.String()}}}
}

func (s *stream) Events() <-chan driver.Event {
	if s == nil {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.selected == viewUnselected {
		s.selected = viewEvents
		for _, event := range s.pendingEvents {
			s.events <- event
		}
		s.pendingEvents = nil
		if !s.closedObservations {
			close(s.observations)
			s.closedObservations = true
		}
	}
	return s.events
}

func (s *stream) Observations() <-chan driver.Observation {
	if s == nil {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.selected == viewUnselected {
		s.selected = viewObservations
		for _, observation := range s.pendingObservations {
			s.observations <- observation
		}
		s.pendingObservations = nil
		if !s.closedEvents {
			close(s.events)
			s.closedEvents = true
		}
	}
	return s.observations
}

func (s *stream) enqueueSteer(observation driver.SteerObservation) (bool, <-chan struct{}) {
	admitted := make(chan struct{})
	if s == nil {
		return false, admitted
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return false, admitted
	}
	s.steerIn <- steerInput{observation: observation, admitted: admitted}
	return true, admitted
}

func emitObservation(streamState *stream, observation driver.Observation) {
	if streamState == nil || observation == nil {
		return
	}
	streamState.mu.Lock()
	selected := streamState.selected
	if observation.Sequence() == 0 {
		streamState.nextOrder++
		switch typed := observation.(type) {
		case driver.PromptObservation:
			typed.OrderSequence = streamState.nextOrder
			observation = typed
		case driver.UpdateObservation:
			typed.OrderSequence = streamState.nextOrder
			observation = typed
		case driver.SteerObservation:
			typed.OrderSequence = streamState.nextOrder
			observation = typed
		}
	}
	if selected == viewUnselected {
		streamState.pendingObservations = append(streamState.pendingObservations, observation)
		streamState.mu.Unlock()
		return
	}
	streamState.mu.Unlock()
	if selected != viewObservations {
		return
	}
	select {
	case streamState.observations <- observation:
	case <-streamState.done:
	}
}

func (s *stream) History() (driver.History, error) {
	return driver.History{Available: false}, nil
}

// Close cancels only this stream's forwarding context. The ACP session remains
// open and the turn goroutine continues draining until Prompt returns so a
// later turn cannot consume updates from an abandoned prompt.
func (s *stream) Close() error {
	if s == nil {
		return nil
	}
	s.once.Do(func() {
		if s.cancel != nil {
			s.cancel()
		}
	})
	return s.closeErr
}

var _ driver.Agent = (*Driver)(nil)
var _ driver.Steerer = (*Driver)(nil)
var _ driver.Stream = (*stream)(nil)
var _ driver.OrderedStream = (*stream)(nil)
var _ turnSession = legacyTurnSession{}
