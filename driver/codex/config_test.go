package codex

import (
	"errors"
	"reflect"
	"testing"

	"github.com/looprig/foreignloops/driver"
)

func TestNewAgentRequiredFields(t *testing.T) {
	t.Parallel()
	got, err := NewAgent(nil, Config{})
	if got != nil {
		t.Fatalf("NewAgent() = %T, want nil", got)
	}
	var configErr *ConfigError
	if !errors.As(err, &configErr) {
		t.Fatalf("NewAgent() error = %T %v, want *ConfigError", err, err)
	}
	if configErr.Field != "ExecPath" || configErr.Reason != "required" {
		t.Fatalf("ConfigError = %#v, want field ExecPath reason required", configErr)
	}
}

func TestNewAgentRejectsNonAbsoluteOrUncleanExecPath(t *testing.T) {
	t.Parallel()
	for _, execPath := range []string{"codex", "/usr/local/../bin/codex"} {
		t.Run(execPath, func(t *testing.T) {
			t.Parallel()
			got, err := NewAgent(nil, Config{ExecPath: execPath})
			if got != nil {
				t.Fatalf("NewAgent() = %T, want nil", got)
			}
			var configErr *ConfigError
			if !errors.As(err, &configErr) {
				t.Fatalf("NewAgent() error = %T %v, want *ConfigError", err, err)
			}
			if configErr.Field != "ExecPath" || configErr.Reason != "must be a clean absolute path" {
				t.Fatalf("ConfigError = %#v, want ExecPath clean-absolute rejection", configErr)
			}
		})
	}
}

func TestNewAgentRetainsOwnedConfiguration(t *testing.T) {
	t.Parallel()
	parent := []string{"PATH=/usr/bin", "HOME=/home/u", "SECRET_TOKEN=shh"}
	allow := []string{"PATH", "HOME"}
	dirs := []string{"/deps/one", "/deps/two"}
	credential := map[string]string{"PATH": "/credential/bin", "CODEX_API_KEY": "sk-test"}

	got, err := NewAgent(parent, Config{
		ExecPath:         "/usr/local/bin/codex",
		Model:            "gpt-5",
		Profile:          "looprig",
		AdditionalDirs:   dirs,
		Sandbox:          SandboxDangerFullAccess,
		Approval:         ApprovalNever,
		EnvAllow:         allow,
		Credential:       credential,
		IgnoreUserConfig: true,
		IgnoreRules:      true,
		SkipGitRepoCheck: true,
	})
	if err != nil {
		t.Fatalf("NewAgent() error = %v", err)
	}
	impl, ok := got.(*agent)
	if !ok {
		t.Fatalf("NewAgent() = %T, want private *agent implementation", got)
	}
	wantEnv := []string{"CODEX_API_KEY=sk-test", "HOME=/home/u", "PATH=/credential/bin"}
	if !reflect.DeepEqual(impl.env, wantEnv) {
		t.Fatalf("agent env = %#v, want %#v", impl.env, wantEnv)
	}
	if impl.execPath != "/usr/local/bin/codex" || impl.model != "gpt-5" || impl.profile != "looprig" {
		t.Fatalf("agent scalar config = %#v", impl)
	}
	if !reflect.DeepEqual(impl.additionalDirs, dirs) || impl.sandbox != SandboxDangerFullAccess || impl.approval != ApprovalNever {
		t.Fatalf("agent spawn config = %#v", impl)
	}
	if !impl.ignoreUserConfig || !impl.ignoreRules || !impl.skipGitRepoCheck {
		t.Fatalf("agent boolean config = %#v", impl)
	}

	parent[0] = "PATH=/mutated/parent"
	allow[0] = "SECRET_TOKEN"
	dirs[0] = "/mutated/dir"
	credential["PATH"] = "/mutated/credential"
	if !reflect.DeepEqual(impl.env, wantEnv) {
		t.Fatalf("agent env changed after caller mutation: %#v", impl.env)
	}
	if !reflect.DeepEqual(impl.additionalDirs, []string{"/deps/one", "/deps/two"}) {
		t.Fatalf("agent additional dirs changed after caller mutation: %#v", impl.additionalDirs)
	}
}

func TestConfigProviderNeutralSurface(t *testing.T) {
	t.Parallel()
	typ := reflect.TypeOf(Config{})
	for _, forbidden := range []string{"Cwd", "Posture", "SIDMode"} {
		if _, ok := typ.FieldByName(forbidden); ok {
			t.Errorf("Config exposes backend-owned field %q", forbidden)
		}
	}
}

var _ driver.Agent = (*agent)(nil)
