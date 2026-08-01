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

// BuildWith adapts an ACP configuration to the Harness foreign-loop builder.
// Each invocation creates one ACP driver and one backend Loop. The returned
// identity is the agent-assigned ACP session id, which the backend binds on
// the first live turn through a synthetic KindInit event.
func BuildWith(cfg Config) foreign.Builder {
	return func(
		loopCtx context.Context,
		sessionID, loopID uuid.UUID,
		parent loop.Provenance,
		pub foreign.EventPublisher,
		loopCfg loop.BoundDefinition,
		idGen func() (uuid.UUID, error),
		fac *event.Factory,
	) (loop.Backend, string, error) {
		d, err := New(loopCtx, cfg)
		if err != nil {
			return nil, "", err
		}

		agent := &initAgent{agent: d, sessionID: d.AgentSessionID()}
		state, _, err := backend.New(
			loopCtx,
			sessionID,
			loopID,
			parent,
			pub,
			loopCfg,
			backend.Config{
				Agent:   agent,
				Cwd:     cfg.WorkspaceRoot,
				Posture: legacyPosture(cfg.Posture),
				SIDMode: backend.SIDLateBound,
			},
			idGen,
			fac,
		)
		if err != nil {
			return nil, "", closeAfterBackendFailure(d, err)
		}
		return state, d.AgentSessionID(), nil
	}
}

// BuildRestoredWith adapts ACP resume construction to the Harness restored
// builder. The journal's AgentSessionID is authoritative for ACP session/load;
// ForeignSID remains the backend's recovered routing identity. An empty
// AgentSessionID preserves legacy session/new behavior.
func BuildRestoredWith(cfg Config) foreign.RestoredBuilder {
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
		if err := validateRestoredSeed(seed); err != nil {
			return nil, err
		}
		resumeCfg := cfg
		resumeCfg.AgentSessionID = seed.AgentSessionID
		d, err := New(loopCtx, resumeCfg)
		if err != nil {
			return nil, err
		}

		state, err := backend.BuildRestoredWith(backend.Config{
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
	return newInitStream(stream, a.sessionID), nil
}

func (a *initAgent) Close() error { return a.agent.Close() }

type initStream struct {
	inner  driver.Stream
	events <-chan driver.Event
	done   <-chan struct{}
	cancel context.CancelFunc

	closeOnce sync.Once
	closeErr  error
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
	_ foreign.Builder         = BuildWith(Config{})
	_ foreign.RestoredBuilder = BuildRestoredWith(Config{})
	_ driver.Agent            = (*initAgent)(nil)
	_ driver.Closer           = (*initAgent)(nil)
	_ driver.Stream           = (*initStream)(nil)
)
