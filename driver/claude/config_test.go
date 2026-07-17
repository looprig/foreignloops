package claude

import (
	"errors"
	"os/exec"
	"reflect"
	"testing"

	"github.com/looprig/foreignloop/driver"
)

func TestNewAgentRequiredFields(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name      string
		cfg       Config
		wantField string
	}{
		{name: "empty executable", cfg: Config{Model: "claude-small"}, wantField: "ExecPath"},
		{name: "empty model", cfg: Config{ExecPath: "/bin/claude"}, wantField: "Model"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got, err := NewAgent(nil, tt.cfg)
			if got != nil {
				t.Fatalf("NewAgent() = %T, want nil", got)
			}
			var configErr *ConfigError
			if !errors.As(err, &configErr) {
				t.Fatalf("NewAgent() error = %T %v, want *ConfigError", err, err)
			}
			if configErr.Field != tt.wantField || configErr.Reason != "required" {
				t.Fatalf("ConfigError = %#v, want field %q reason required", configErr, tt.wantField)
			}
		})
	}
}

func TestNewAgentOwnsWhitelistedEnvironment(t *testing.T) {
	t.Parallel()
	parent := []string{"PATH=/usr/bin", "HOME=/home/u", "SECRET_TOKEN=shh"}
	allow := []string{"PATH", "HOME"}
	credential := map[string]string{"PATH": "/credential/bin", "ANTHROPIC_API_KEY": "sk-test"}
	wrapper := CommandWrapper(func(cmd *exec.Cmd) (*exec.Cmd, error) { return cmd, nil })

	got, err := NewAgent(parent, Config{
		ExecPath:   "/usr/local/bin/claude",
		Home:       "/home/u",
		Model:      "claude-opus-4-8",
		EnvAllow:   allow,
		Credential: credential,
		Wrap:       wrapper,
	})
	if err != nil {
		t.Fatalf("NewAgent() error = %v", err)
	}
	impl, ok := got.(*agent)
	if !ok {
		t.Fatalf("NewAgent() = %T, want private *agent implementation", got)
	}
	wantEnv := []string{"HOME=/home/u", "ANTHROPIC_API_KEY=sk-test", "PATH=/credential/bin"}
	if !reflect.DeepEqual(impl.env, wantEnv) {
		t.Fatalf("agent env = %#v, want %#v", impl.env, wantEnv)
	}
	if impl.execPath != "/usr/local/bin/claude" || impl.home != "/home/u" || impl.model != "claude-opus-4-8" {
		t.Fatalf("agent scalar config = %#v", impl)
	}
	if impl.wrap == nil {
		t.Fatal("agent wrapper = nil, want configured wrapper")
	}

	parent[1] = "HOME=/mutated"
	allow[0] = "SECRET_TOKEN"
	credential["PATH"] = "/mutated/bin"
	if !reflect.DeepEqual(impl.env, wantEnv) {
		t.Fatalf("agent env changed after caller mutation: %#v", impl.env)
	}
}

func TestConfigProviderNeutralSurface(t *testing.T) {
	t.Parallel()
	typ := reflect.TypeOf(Config{})
	// Negative compile assertions are not expressible in Go. Keep this guard
	// narrow so it rejects backend-owned fields without freezing future
	// provider-owned additions or struct field order.
	for _, forbidden := range []string{"Cwd", "Posture", "SIDMode"} {
		if _, ok := typ.FieldByName(forbidden); ok {
			t.Errorf("Config exposes backend-owned field %q", forbidden)
		}
	}
}

var _ driver.Agent = (*agent)(nil)
