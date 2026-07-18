package backend

import (
	"context"
	"errors"
	"testing"

	"github.com/looprig/core/uuid"
	"github.com/looprig/foreignloop/driver"
	"github.com/looprig/harness/pkg/foreign"
	"github.com/looprig/harness/pkg/loop"
)

type restoredStubAgent struct{}

func (restoredStubAgent) Spawn(context.Context, driver.Turn) (driver.Stream, error) {
	panic("restoredStubAgent.Spawn must not be called by Task 11 construction")
}

func restoredValidConfig() Config {
	return Config{
		Agent:   restoredStubAgent{},
		Cwd:     "/workspace",
		Posture: driver.PostureDefault,
		SIDMode: SIDPrebound,
	}
}

func TestBuildRestoredWithReturnsHarnessBuilder(t *testing.T) {
	t.Parallel()
	var _ foreign.RestoredBuilder = BuildRestoredWith(restoredValidConfig())
}

func TestBuildRestoredWithFailsClosedOnInvalidConfig(t *testing.T) {
	t.Parallel()

	cfg := restoredValidConfig()
	cfg.Cwd = ""
	build := BuildRestoredWith(cfg)
	got, err := build(
		context.Background(),
		uuid.UUID{},
		uuid.UUID{},
		loop.Provenance{},
		nil,
		nil,
		nil,
		nil,
		foreign.RestoredForeign{},
	)
	if got != nil {
		t.Fatalf("BuildRestoredWith error returned non-nil Backend %T", got)
	}
	var cfgErr *ConfigError
	if !errors.As(err, &cfgErr) {
		t.Fatalf("BuildRestoredWith error = %T %v, want *ConfigError", err, err)
	}
	if cfgErr.Field != "Config.Cwd" {
		t.Fatalf("ConfigError.Field = %q, want Config.Cwd", cfgErr.Field)
	}
}

func TestBuildRestoredWithRejectsMissingForeignSessionID(t *testing.T) {
	t.Parallel()

	err := validateRestoredSeed(foreign.RestoredForeign{})
	var cfgErr *ConfigError
	if !errors.As(err, &cfgErr) {
		t.Fatalf("validateRestoredSeed missing sid error = %T %v, want *ConfigError", err, err)
	}
	if cfgErr.Field != "RestoredForeign.ForeignSID" {
		t.Fatalf("ConfigError.Field = %q, want RestoredForeign.ForeignSID", cfgErr.Field)
	}
}
