package acp

import (
	"errors"
	"strings"
	"testing"

	"github.com/looprig/acp/launch"
	"github.com/looprig/foreignloops/driver"
)

func validConfig(h Harness) Config {
	cfg := Config{
		Harness:         h,
		Executable:      "/opt/looprig/acp-adapter",
		Env:             []string{"TOKEN=env-secret", "LANG=C"},
		Binding:         launch.ProxyBinding{BaseURL: "http://127.0.0.1:4141", Token: "binding-secret"},
		ModelAlias:      "primary",
		SmallModelAlias: "small",
		Posture:         driver.PostureWorkspaceWrite,
		AgentSessionID:  "agent-session-1",
		WorkspaceRoot:   "/workspace/project",
	}
	if h == HarnessCodex {
		cfg.SmallModelAlias = ""
	}
	return cfg
}

func TestConfigValidateAcceptsSupportedHarnesses(t *testing.T) {
	for _, harness := range []Harness{HarnessClaudeCode, HarnessCodex} {
		t.Run(string(harness), func(t *testing.T) {
			if err := validConfig(harness).validate(); err != nil {
				t.Fatalf("validate() error = %v, want nil", err)
			}
		})
	}
}

func TestConfigValidateRejectsInvalidFields(t *testing.T) {
	tests := []struct {
		name   string
		field  string
		mutate func(*Config)
	}{
		{
			name:   "empty harness",
			field:  "Harness",
			mutate: func(cfg *Config) { cfg.Harness = "" },
		},
		{
			name:   "unknown harness",
			field:  "Harness",
			mutate: func(cfg *Config) { cfg.Harness = Harness("unknown") },
		},
		{
			name:   "empty model alias",
			field:  "ModelAlias",
			mutate: func(cfg *Config) { cfg.ModelAlias = "" },
		},
		{
			name:   "invalid posture",
			field:  "Posture",
			mutate: func(cfg *Config) { cfg.Posture = driver.Posture("invalid") },
		},
		{
			name:   "empty executable",
			field:  "Executable",
			mutate: func(cfg *Config) { cfg.Executable = "" },
		},
		{
			name:   "relative executable",
			field:  "Executable",
			mutate: func(cfg *Config) { cfg.Executable = "adapter" },
		},
		{
			name:   "unclean executable",
			field:  "Executable",
			mutate: func(cfg *Config) { cfg.Executable = "/opt/../adapter" },
		},
		{
			name:  "codex with small model alias",
			field: "SmallModelAlias",
			mutate: func(cfg *Config) {
				cfg.Harness = HarnessCodex
				cfg.SmallModelAlias = "small"
			},
		},
		{
			name:  "claude without small model alias",
			field: "SmallModelAlias",
			mutate: func(cfg *Config) {
				cfg.Harness = HarnessClaudeCode
				cfg.SmallModelAlias = ""
			},
		},
		{
			name:   "missing workspace root",
			field:  "WorkspaceRoot",
			mutate: func(cfg *Config) { cfg.WorkspaceRoot = "" },
		},
		{
			name:   "relative workspace root",
			field:  "WorkspaceRoot",
			mutate: func(cfg *Config) { cfg.WorkspaceRoot = "workspace/project" },
		},
		{
			name:   "missing binding url",
			field:  "Binding",
			mutate: func(cfg *Config) { cfg.Binding.BaseURL = "" },
		},
		{
			name:   "missing binding token",
			field:  "Binding",
			mutate: func(cfg *Config) { cfg.Binding.Token = "" },
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := validConfig(HarnessClaudeCode)
			tt.mutate(&cfg)

			err := cfg.validate()
			if err == nil {
				t.Fatal("validate() error = nil, want typed ConfigError")
			}
			var cfgErr *ConfigError
			if !errors.As(err, &cfgErr) {
				t.Fatalf("validate() error = %T, want *ConfigError", err)
			}
			if cfgErr.Field != tt.field {
				t.Fatalf("ConfigError.Field = %q, want %q", cfgErr.Field, tt.field)
			}
			for _, sensitive := range []string{
				cfg.Executable,
				cfg.Env[0],
				cfg.Binding.BaseURL,
				cfg.Binding.Token,
				cfg.WorkspaceRoot,
				cfg.AgentSessionID,
			} {
				if sensitive != "" && strings.Contains(err.Error(), sensitive) {
					t.Errorf("ConfigError.Error() contains sensitive value %q: %q", sensitive, err)
				}
			}
		})
	}
}

func TestConfigValidatePreservesCallerOwnedValues(t *testing.T) {
	cfg := validConfig(HarnessClaudeCode)
	env := append([]string(nil), cfg.Env...)
	binding := cfg.Binding

	if err := cfg.validate(); err != nil {
		t.Fatalf("validate() error = %v, want nil", err)
	}
	if strings.Join(cfg.Env, "\x00") != strings.Join(env, "\x00") {
		t.Fatalf("validate() changed caller environment: got %#v, want %#v", cfg.Env, env)
	}
	if cfg.Binding != binding {
		t.Fatalf("validate() changed caller binding: got %#v, want %#v", cfg.Binding, binding)
	}
}
