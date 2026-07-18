package backend_test

import (
	"context"
	"errors"
	"testing"

	"github.com/looprig/core/uuid"
	"github.com/looprig/foreignloop/backend"
	"github.com/looprig/harness/pkg/foreign"
	"github.com/looprig/harness/pkg/loop"
)

func TestBuildWithReturnsHarnessBuilder(t *testing.T) {
	t.Parallel()
	var _ foreign.Builder = backend.BuildWith(validConfig())
}

func TestBuildWithFailsClosedOnInvalidRuntimeWiring(t *testing.T) {
	t.Parallel()

	build := backend.BuildWith(validConfig())
	got, sid, err := build(context.Background(), uuid.UUID{}, uuid.UUID{}, loop.Provenance{}, nil, nil, nil, nil)
	if got != nil {
		t.Fatalf("BuildWith error returned non-nil Backend %T", got)
	}
	if sid != "" {
		t.Fatalf("BuildWith error sid = %q, want empty", sid)
	}
	var cfgErr *backend.ConfigError
	if !errors.As(err, &cfgErr) {
		t.Fatalf("BuildWith error = %T %v, want *backend.ConfigError", err, err)
	}
	if cfgErr.Field != "System" {
		t.Fatalf("ConfigError.Field = %q, want System", cfgErr.Field)
	}
}
