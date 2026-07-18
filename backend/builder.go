package backend

import (
	"context"
	"errors"

	"github.com/looprig/core/uuid"
	"github.com/looprig/harness/pkg/event"
	"github.com/looprig/harness/pkg/foreign"
	"github.com/looprig/harness/pkg/loop"
)

var errBackendImplementationPending = errors.New("foreignloop: backend implementation pending")

// BuildWith adapts backend construction to the narrow Harness-owned seam. The
// concrete actor is added in the following extraction tasks; until then a fully
// valid invocation fails closed instead of returning a placeholder Backend.
func BuildWith(backendCfg Config) foreign.Builder {
	return func(
		_ context.Context,
		_, _ uuid.UUID,
		_ loop.Provenance,
		pub foreign.EventPublisher,
		loopCfg loop.BoundDefinition,
		idGen func() (uuid.UUID, error),
		fac *event.Factory,
	) (loop.Backend, string, error) {
		if err := validateConfig(backendCfg); err != nil {
			return nil, "", err
		}
		if err := validateRuntimeWiring(loopCfg, idGen, fac, pub); err != nil {
			return nil, "", err
		}
		return nil, "", errBackendImplementationPending
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
