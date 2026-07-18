package backend

import (
	"context"

	"github.com/looprig/core/content"
	"github.com/looprig/core/uuid"
	"github.com/looprig/harness/pkg/command"
	"github.com/looprig/harness/pkg/event"
	"github.com/looprig/harness/pkg/foreign"
	"github.com/looprig/harness/pkg/loop"
)

// Loop holds the backend actor's immutable dependencies and actor-owned state.
// Task 12 moves and tests its restore seed construction; Task 13 adds the command
// channels, actor lifecycle, and loop.Backend methods.
type Loop struct {
	Commands  chan command.Command
	Done      chan struct{}
	snapshots chan snapshotReq

	sessionID  uuid.UUID
	loopID     uuid.UUID
	sid        string
	parent     loop.Provenance
	pub        foreign.EventPublisher
	cfg        loop.BoundDefinition
	backendCfg Config
	idGen      func() (uuid.UUID, error)
	fac        *event.Factory

	msgs       content.AgenticMessages
	turnIndex  event.TurnIndex
	hasSpawned bool
	sidBound   bool
}

func newRestoredState(
	_ context.Context,
	sessionID, loopID uuid.UUID,
	parent loop.Provenance,
	pub foreign.EventPublisher,
	loopCfg loop.BoundDefinition,
	backendCfg Config,
	idGen func() (uuid.UUID, error),
	fac *event.Factory,
	seed foreign.RestoredForeign,
) (*Loop, error) {
	if err := validateConfig(backendCfg); err != nil {
		return nil, err
	}
	if err := validateRuntimeWiring(loopCfg, idGen, fac, pub); err != nil {
		return nil, err
	}
	if err := validateRestoredSeed(seed); err != nil {
		return nil, err
	}
	return &Loop{
		Commands:   make(chan command.Command),
		Done:       make(chan struct{}),
		snapshots:  make(chan snapshotReq),
		sessionID:  sessionID,
		loopID:     loopID,
		sid:        seed.ForeignSID,
		parent:     parent,
		pub:        pub,
		cfg:        loopCfg,
		backendCfg: backendCfg,
		idGen:      idGen,
		fac:        fac,
		msgs:       cloneMessages(seed.Msgs),
		turnIndex:  seed.TurnIndex,
		hasSpawned: true,
		sidBound:   true,
	}, nil
}

// BuildRestoredWith adapts restored backend construction to the Harness-owned
// seam. It preserves true-nil-on-error behavior while the actor and restored
// state mechanics are moved in the following extraction tasks.
func BuildRestoredWith(backendCfg Config) foreign.RestoredBuilder {
	return func(
		_ context.Context,
		_, _ uuid.UUID,
		_ loop.Provenance,
		pub foreign.EventPublisher,
		loopCfg loop.BoundDefinition,
		idGen func() (uuid.UUID, error),
		fac *event.Factory,
		seed foreign.RestoredForeign,
	) (loop.Backend, error) {
		if err := validateConfig(backendCfg); err != nil {
			return nil, err
		}
		if err := validateRuntimeWiring(loopCfg, idGen, fac, pub); err != nil {
			return nil, err
		}
		if err := validateRestoredSeed(seed); err != nil {
			return nil, err
		}
		return nil, errBackendImplementationPending
	}
}

func validateRestoredSeed(seed foreign.RestoredForeign) error {
	if seed.ForeignSID == "" {
		return &ConfigError{Field: "RestoredForeign.ForeignSID", Reason: "required"}
	}
	return nil
}
