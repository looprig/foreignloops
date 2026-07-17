package codex

import (
	"fmt"
	"path/filepath"

	"github.com/looprig/foreignloop/driver"
)

// SandboxMode is the typed Codex CLI sandbox mode.
type SandboxMode uint8

const (
	SandboxReadOnly SandboxMode = iota
	SandboxWorkspaceWrite
	SandboxDangerFullAccess
)

// ApprovalPolicy is the typed Codex CLI approval policy.
type ApprovalPolicy uint8

const (
	ApprovalUntrusted ApprovalPolicy = iota
	ApprovalOnRequest
	ApprovalNever
)

// Config is the provider-owned configuration used to construct a Codex agent.
// Turn-owned cwd and session selection are supplied to Agent.Spawn.
type Config struct {
	ExecPath         string
	Model            string
	Profile          string
	AdditionalDirs   []string
	Sandbox          SandboxMode
	Approval         ApprovalPolicy
	EnvAllow         []string
	Credential       map[string]string
	IgnoreUserConfig bool
	IgnoreRules      bool
	SkipGitRepoCheck bool
}

// ConfigError reports invalid provider configuration at construction time.
type ConfigError struct{ Field, Reason string }

func (e *ConfigError) Error() string {
	return "codex: config: " + e.Field + ": " + e.Reason
}

// SpawnConfigError reports invalid internal agent state discovered before a
// child is started. NewAgent prevents callers from constructing this state.
type SpawnConfigError struct{ Field, Reason string }

func (e *SpawnConfigError) Error() string {
	return "codex: spawn config: " + e.Field + ": " + e.Reason
}

// PlatformError reports an OS without the required process-group supervision.
type PlatformError struct{ GOOS string }

func (e *PlatformError) Error() string {
	return fmt.Sprintf("codex: process supervision is unsupported on %s", e.GOOS)
}

// NewAgent resolves provider configuration and a caller-supplied parent
// environment into an agent. It never reads the ambient process environment.
func NewAgent(parentEnv []string, cfg Config) (driver.Agent, error) {
	if cfg.ExecPath == "" {
		return nil, &ConfigError{Field: "ExecPath", Reason: "required"}
	}
	if !cleanAbsoluteExecPath(cfg.ExecPath) {
		return nil, &ConfigError{Field: "ExecPath", Reason: "must be a clean absolute path"}
	}
	return &agent{
		execPath:         cfg.ExecPath,
		model:            cfg.Model,
		profile:          cfg.Profile,
		additionalDirs:   append([]string(nil), cfg.AdditionalDirs...),
		sandbox:          cfg.Sandbox,
		approval:         cfg.Approval,
		env:              whitelistEnv(parentEnv, cfg.EnvAllow, cfg.Credential),
		ignoreUserConfig: cfg.IgnoreUserConfig,
		ignoreRules:      cfg.IgnoreRules,
		skipGitRepoCheck: cfg.SkipGitRepoCheck,
	}, nil
}

func cleanAbsoluteExecPath(path string) bool {
	return filepath.IsAbs(path) && filepath.Clean(path) == path
}
