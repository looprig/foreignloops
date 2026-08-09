package acp

import (
	"context"
	"errors"
	"sync"

	"github.com/looprig/core/uuid"
	"github.com/looprig/foreignloops/backend"
	"github.com/looprig/foreignloops/driver"
	"github.com/looprig/harness/pkg/event"
	"github.com/looprig/harness/pkg/foreign"
	"github.com/looprig/harness/pkg/loop"
)

// BuildWith adapts an ACP configuration to the legacy Harness foreign-loop
// builder. It withholds all scoped capabilities by invoking the additive
// builder with zero Services.
func BuildWith(cfg Config) foreign.Builder {
	build := BuildWithServices(cfg)
	return func(
		loopCtx context.Context,
		sessionID, loopID uuid.UUID,
		parent loop.Provenance,
		pub foreign.EventPublisher,
		loopCfg loop.BoundDefinition,
		idGen func() (uuid.UUID, error),
		fac *event.Factory,
	) (loop.Backend, string, error) {
		return build(loopCtx, sessionID, loopID, parent, pub, loopCfg, idGen, fac, foreign.Services{})
	}
}

// BuildWithServices adapts an ACP configuration to the additive Harness
// services-aware builder. Each invocation creates one ACP driver and one
// backend Loop. The returned identity is the agent-assigned ACP session id,
// which the backend binds on the first live turn through a synthetic KindInit
// event.
func BuildWithServices(cfg Config) foreign.ServicesBuilder {
	cfg.McpServers = cloneMcpServers(cfg.McpServers)
	return func(
		loopCtx context.Context,
		sessionID, loopID uuid.UUID,
		parent loop.Provenance,
		pub foreign.EventPublisher,
		loopCfg loop.BoundDefinition,
		idGen func() (uuid.UUID, error),
		fac *event.Factory,
		services foreign.Services,
	) (loop.Backend, string, error) {
		d, err := New(loopCtx, cfg)
		if err != nil {
			return nil, "", err
		}

		agent := &initAgent{agent: d, sessionID: d.AgentSessionID()}
		state, _, err := backend.BuildWithServices(backend.Config{
			Agent:   agent,
			Cwd:     cfg.WorkspaceRoot,
			Posture: legacyPosture(cfg.Posture),
			SIDMode: backend.SIDLateBound,
		})(loopCtx, sessionID, loopID, parent, pub, loopCfg, idGen, fac, services)
		if err != nil {
			return nil, "", closeAfterBackendFailure(d, err)
		}
		return state, d.AgentSessionID(), nil
	}
}

// BuildRestoredWith adapts ACP resume construction to the legacy Harness
// restored builder. It withholds all scoped capabilities by invoking the
// additive builder with zero Services.
func BuildRestoredWith(cfg Config) foreign.RestoredBuilder {
	build := BuildRestoredWithServices(cfg)
	return func(
		loopCtx context.Context,
		sessionID, loopID uuid.UUID,
		parent loop.Provenance,
		pub foreign.EventPublisher,
		loopCfg loop.BoundDefinition,
		idGen func() (uuid.UUID, error),
		fac *event.Factory,
		seed foreign.RestoredForeign,
	) (loop.Backend, error) {
		return build(loopCtx, sessionID, loopID, parent, pub, loopCfg, idGen, fac, seed, foreign.Services{})
	}
}

// BuildRestoredWithServices adapts ACP resume construction to the additive
// Harness restored builder. The journal's AgentSessionID is authoritative for
// ACP session/load; ForeignSID remains the backend's recovered routing
// identity. An empty AgentSessionID preserves legacy session/new behavior.
func BuildRestoredWithServices(cfg Config) foreign.ServicesRestoredBuilder {
	cfg.McpServers = cloneMcpServers(cfg.McpServers)
	return func(
		loopCtx context.Context,
		sessionID, loopID uuid.UUID,
		parent loop.Provenance,
		pub foreign.EventPublisher,
		loopCfg loop.BoundDefinition,
		idGen func() (uuid.UUID, error),
		fac *event.Factory,
		seed foreign.RestoredForeign,
		services foreign.Services,
	) (loop.Backend, error) {
		if err := validateRestoredSeed(seed); err != nil {
			return nil, err
		}
		resumeCfg := cfg
		resumeCfg.AgentSessionID = seed.AgentSessionID
		d, err := New(loopCtx, resumeCfg)
		if err != nil {
			return nil, err
		}

		state, err := backend.BuildRestoredWithServices(backend.Config{
			Agent:   d,
			Cwd:     resumeCfg.WorkspaceRoot,
			Posture: legacyPosture(resumeCfg.Posture),
			SIDMode: backend.SIDLateBound,
		})(
			loopCtx,
			sessionID,
			loopID,
			parent,
			pub,
			loopCfg,
			idGen,
			fac,
			seed,
			services,
		)
		if err != nil {
			return nil, closeAfterBackendFailure(d, err)
		}
		return state, nil
	}
}

func validateRestoredSeed(seed foreign.RestoredForeign) error {
	if seed.ForeignSID == "" {
		return &backend.ConfigError{Field: "RestoredForeign.ForeignSID", Reason: "required"}
	}
	return nil
}

func legacyPosture(posture driver.Posture) driver.PermissionPosture {
	switch posture {
	case driver.PostureWorkspaceWrite:
		return driver.PostureAcceptEdits
	case driver.PostureReadOnly:
		return driver.PostureDefault
	default:
		// New validates the neutral posture before this boundary is reached.
		// Keep an invalid direct call restrictive rather than widening access.
		return driver.PostureDefault
	}
}

func closeAfterBackendFailure(d *Driver, cause error) error {
	if closeErr := d.Close(); closeErr != nil {
		return errors.Join(cause, closeErr)
	}
	return cause
}

// initAgent gives the existing backend its late-bind event without changing
// ACP's direct driver stream contract. Only the first successful live Spawn
// is prefixed; restored loops already carry a journaled SID.
type initAgent struct {
	agent     *Driver
	sessionID string

	mu          sync.Mutex
	initialized bool
}

func (a *initAgent) Spawn(ctx context.Context, turn driver.Turn) (driver.Stream, error) {
	stream, err := a.agent.Spawn(ctx, turn)
	if err != nil {
		return nil, err
	}

	a.mu.Lock()
	first := !a.initialized
	if first {
		a.initialized = true
	}
	a.mu.Unlock()
	if !first {
		return stream, nil
	}
	if a.agent.steeringOn {
		return newOrderedInitStream(stream, a.sessionID), nil
	}
	return newInitStream(stream, a.sessionID), nil
}

func (a *initAgent) Close() error { return a.agent.Close() }

func (a *initAgent) Steer(ctx context.Context, request driver.SteerRequest) (driver.SteerResult, error) {
	return a.agent.Steer(ctx, request)
}

type initStream struct {
	inner  driver.Stream
	events <-chan driver.Event
	done   <-chan struct{}
	cancel context.CancelFunc

	closeOnce sync.Once
	closeErr  error
}

// orderedInitStream preserves the legacy Events projection for the backend
// while exposing the authoritative ordered observation view without probing
// Events on the wrapped stream.
type orderedInitStream struct {
	inner        driver.Stream
	events       <-chan driver.Event
	observations <-chan driver.Observation
	done         <-chan struct{}
	cancel       context.CancelFunc
	closeOnce    sync.Once
	closeErr     error
}

func newOrderedInitStream(inner driver.Stream, sessionID string) driver.Stream {
	ordered := inner.(driver.OrderedStream)
	ctx, cancel := context.WithCancel(context.Background())
	events := make(chan driver.Event, 2048)
	observations := make(chan driver.Observation, 2048)
	done := make(chan struct{})
	go func() {
		defer close(done)
		defer close(events)
		defer close(observations)
		init := driver.UpdateObservation{Event: driver.Event{Kind: driver.KindInit, SessionID: sessionID}}
		select {
		case observations <- init:
		case <-ctx.Done():
			return
		}
		select {
		case events <- init.Event:
		case <-ctx.Done():
			return
		}
		for {
			select {
			case <-ctx.Done():
				return
			case observation, ok := <-ordered.Observations():
				if !ok {
					return
				}
				select {
				case observations <- observation:
				case <-ctx.Done():
					return
				}
				if update, ok := observation.(driver.UpdateObservation); ok {
					select {
					case events <- update.Event:
					case <-ctx.Done():
						return
					}
				}
			}
		}
	}()
	return &orderedInitStream{inner: inner, events: events, observations: observations, done: done, cancel: cancel}
}

func (s *orderedInitStream) Events() <-chan driver.Event             { return s.events }
func (s *orderedInitStream) Observations() <-chan driver.Observation { return s.observations }
func (s *orderedInitStream) History() (driver.History, error)        { return s.inner.History() }
func (s *orderedInitStream) Close() error {
	s.closeOnce.Do(func() { s.cancel(); s.closeErr = s.inner.Close(); <-s.done })
	return s.closeErr
}

func newInitStream(inner driver.Stream, sessionID string) driver.Stream {
	ctx, cancel := context.WithCancel(context.Background())
	events := make(chan driver.Event, 1)
	done := make(chan struct{})
	go func() {
		defer close(done)
		defer close(events)
		select {
		case events <- driver.Event{Kind: driver.KindInit, SessionID: sessionID}:
		case <-ctx.Done():
			return
		}
		innerEvents := inner.Events()
		for {
			select {
			case <-ctx.Done():
				return
			case event, ok := <-innerEvents:
				if !ok {
					return
				}
				select {
				case events <- event:
				case <-ctx.Done():
					return
				}
			}
		}
	}()
	return &initStream{inner: inner, events: events, done: done, cancel: cancel}
}

func (s *initStream) Events() <-chan driver.Event { return s.events }

func (s *initStream) Observations() <-chan driver.Observation {
	ordered, ok := s.inner.(driver.OrderedStream)
	if !ok {
		return nil
	}
	return ordered.Observations()
}

func (s *initStream) History() (driver.History, error) { return s.inner.History() }

func (s *initStream) Close() error {
	if s == nil {
		return nil
	}
	s.closeOnce.Do(func() {
		s.cancel()
		s.closeErr = s.inner.Close()
		<-s.done
	})
	return s.closeErr
}

var (
	_ foreign.Builder                 = BuildWith(Config{})
	_ foreign.ServicesBuilder         = BuildWithServices(Config{})
	_ foreign.RestoredBuilder         = BuildRestoredWith(Config{})
	_ foreign.ServicesRestoredBuilder = BuildRestoredWithServices(Config{})
	_ driver.Agent                    = (*initAgent)(nil)
	_ driver.Steerer                  = (*initAgent)(nil)
	_ driver.Closer                   = (*initAgent)(nil)
	_ driver.Stream                   = (*initStream)(nil)
	_ driver.OrderedStream            = (*initStream)(nil)
)
