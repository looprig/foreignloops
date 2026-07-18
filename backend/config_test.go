package backend_test

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/looprig/core/uuid"
	"github.com/looprig/foreignloop/backend"
	"github.com/looprig/foreignloop/driver"
	"github.com/looprig/harness/pkg/loop"
)

type stubAgent struct{}

func (stubAgent) Spawn(context.Context, driver.Turn) (driver.Stream, error) {
	panic("stubAgent.Spawn must not be called while validating configuration")
}

type typedNilAgent struct{}

func (*typedNilAgent) Spawn(context.Context, driver.Turn) (driver.Stream, error) {
	panic("typedNilAgent.Spawn must never be called")
}

var _ driver.Agent = (*typedNilAgent)(nil)

func validConfig() backend.Config {
	return backend.Config{
		Agent:   stubAgent{},
		Cwd:     "/workspace",
		Posture: driver.PostureDefault,
		SIDMode: backend.SIDPrebound,
	}
}

func TestConfigPublicFields(t *testing.T) {
	t.Parallel()

	typ := reflect.TypeOf(backend.Config{})
	want := []string{"Agent", "Cwd", "Posture", "SIDMode"}
	if typ.NumField() != len(want) {
		t.Fatalf("Config has %d fields, want %d", typ.NumField(), len(want))
	}
	for i, name := range want {
		if got := typ.Field(i).Name; got != name {
			t.Fatalf("Config field %d = %q, want %q", i, got, name)
		}
	}
}

func TestBuildWithEagerlyValidatesConfig(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		mutate    func(*backend.Config)
		wantField string
	}{
		{name: "nil agent", mutate: func(cfg *backend.Config) { cfg.Agent = nil }, wantField: "Config.Agent"},
		{name: "empty workspace", mutate: func(cfg *backend.Config) { cfg.Cwd = "" }, wantField: "Config.Cwd"},
		{name: "unknown posture", mutate: func(cfg *backend.Config) { cfg.Posture = driver.PermissionPosture(255) }, wantField: "Config.Posture"},
		{name: "unknown sid mode", mutate: func(cfg *backend.Config) { cfg.SIDMode = backend.SIDMode(255) }, wantField: "Config.SIDMode"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			cfg := validConfig()
			tt.mutate(&cfg)

			got, sid, err := backend.BuildWith(cfg)(context.Background(), uuid.UUID{}, uuid.UUID{}, loop.Provenance{}, nil, nil, nil, nil)
			if got != nil {
				t.Fatalf("BuildWith invalid config returned non-nil Backend %T", got)
			}
			if sid != "" {
				t.Fatalf("BuildWith invalid config sid = %q, want empty", sid)
			}
			var cfgErr *backend.ConfigError
			if !errors.As(err, &cfgErr) {
				t.Fatalf("BuildWith invalid config error = %T %v, want *backend.ConfigError", err, err)
			}
			if cfgErr.Field != tt.wantField {
				t.Fatalf("ConfigError.Field = %q, want %q", cfgErr.Field, tt.wantField)
			}
		})
	}
}

func TestBuildWithRejectsTypedNilAgent(t *testing.T) {
	t.Parallel()

	var agent *typedNilAgent
	cfg := validConfig()
	cfg.Agent = agent

	got, sid, err := backend.BuildWith(cfg)(context.Background(), uuid.UUID{}, uuid.UUID{}, loop.Provenance{}, nil, nil, nil, nil)
	if got != nil {
		t.Fatalf("BuildWith typed-nil agent returned non-nil Backend %T", got)
	}
	if sid != "" {
		t.Fatalf("BuildWith typed-nil agent sid = %q, want empty", sid)
	}
	var cfgErr *backend.ConfigError
	if !errors.As(err, &cfgErr) || cfgErr.Field != "Config.Agent" || cfgErr.Reason != "required" {
		t.Fatalf("BuildWith typed-nil agent error = %T %v, want ConfigError Config.Agent required", err, err)
	}
}

func TestSIDModeValues(t *testing.T) {
	t.Parallel()
	if backend.SIDPrebound != 0 {
		t.Fatalf("SIDPrebound = %d, want 0", backend.SIDPrebound)
	}
	if backend.SIDLateBound != 1 {
		t.Fatalf("SIDLateBound = %d, want 1", backend.SIDLateBound)
	}
}
