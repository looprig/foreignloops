package backend

import (
	"context"

	"github.com/looprig/core/uuid"
	"github.com/looprig/harness/pkg/event"
	"github.com/looprig/harness/pkg/foreign"
	"github.com/looprig/harness/pkg/loop"
)

// BuildWith adapts actor construction to the narrow Harness-owned seam. On
// construction failure it returns a true nil loop.Backend interface.
func BuildWith(backendCfg Config) foreign.Builder {
	return func(
		loopCtx context.Context,
		sessionID, loopID uuid.UUID,
		parent loop.Provenance,
		pub foreign.EventPublisher,
		loopCfg loop.BoundDefinition,
		idGen func() (uuid.UUID, error),
		fac *event.Factory,
	) (loop.Backend, string, error) {
		state, sid, err := New(loopCtx, sessionID, loopID, parent, pub, loopCfg, backendCfg, idGen, fac)
		if err != nil {
			return nil, "", err
		}
		return state, sid, nil
	}
}

func validateRuntimeWiring(
	loopCfg loop.BoundDefinition,
	idGen func() (uuid.UUID, error),
	fac *event.Factory,
	pub foreign.EventPublisher,
) error {
	switch {
	case loopCfg == nil || loopCfg.EffectiveSystem() == "":
		return &ConfigError{Field: "System", Reason: "required"}
	case idGen == nil:
		return &ConfigError{Field: "idGen", Reason: "required"}
	case fac == nil:
		return &ConfigError{Field: "fac", Reason: "required"}
	case pub == nil:
		return &ConfigError{Field: "pub", Reason: "required"}
	default:
		return nil
	}
}
