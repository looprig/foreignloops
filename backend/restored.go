package backend

import (
	"context"

	"github.com/looprig/core/uuid"
	"github.com/looprig/harness/pkg/event"
	"github.com/looprig/harness/pkg/foreign"
	"github.com/looprig/harness/pkg/loop"
)

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
