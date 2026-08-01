package acp

import (
	"path/filepath"

	"github.com/looprig/acp/launch"
	"github.com/looprig/foreignloops/driver"
)

// Harness identifies the ACP adapter contract selected by Config.
type Harness string

const (
	HarnessClaudeCode Harness = "claude-code"
	HarnessCodex      Harness = "codex"
)

// Config is the caller-owned configuration for an ACP driver.
type Config struct {
	// Harness selects the adapter contract.
	Harness Harness
	// Executable is the clean absolute path to the ACP adapter binary.
	Executable string
	// Env is the complete child environment supplied by the caller.
	Env []string
	// Binding is borrowed caller-owned proxy data. The driver never starts or
	// closes anything represented by this value.
	Binding launch.ProxyBinding
	// ModelAlias is the primary harness-facing model alias.
	ModelAlias string
	// SmallModelAlias is required by Claude Code and forbidden by Codex.
	SmallModelAlias string
	// Posture is the neutral access posture for the child.
	Posture driver.Posture
	// AgentSessionID resumes an existing agent-side session when non-empty.
	AgentSessionID string
	// WorkspaceRoot is the absolute session working directory.
	WorkspaceRoot string
}

// ConfigError reports invalid caller-supplied ACP configuration.
type ConfigError struct {
	Field  string
	Reason string
}

func (e *ConfigError) Error() string {
	if e == nil {
		return "acp: config: invalid"
	}
	if e.Reason == "" {
		return "acp: config: " + e.Field
	}
	return "acp: config: " + e.Field + ": " + e.Reason
}

// validate rejects configuration before any ACP child or proxy lifecycle is
// entered. It only reports fixed field-level reasons; caller values and
// underlying errors never become part of the returned error string.
func (c Config) validate() error {
	switch c.Harness {
	case HarnessClaudeCode, HarnessCodex:
	default:
		return &ConfigError{Field: "Harness", Reason: "must be a supported harness"}
	}

	if !cleanAbsolutePath(c.Executable) {
		return &ConfigError{Field: "Executable", Reason: "must be a clean absolute path"}
	}
	if c.Binding.BaseURL == "" || c.Binding.Token == "" {
		return &ConfigError{Field: "Binding", Reason: "is required"}
	}
	if c.ModelAlias == "" {
		return &ConfigError{Field: "ModelAlias", Reason: "is required"}
	}
	if !c.Posture.Valid() {
		return &ConfigError{Field: "Posture", Reason: "must be a supported posture"}
	}
	if !cleanAbsolutePath(c.WorkspaceRoot) {
		return &ConfigError{Field: "WorkspaceRoot", Reason: "must be a clean absolute path"}
	}

	switch c.Harness {
	case HarnessClaudeCode:
		if c.SmallModelAlias == "" {
			return &ConfigError{Field: "SmallModelAlias", Reason: "is required for Claude Code"}
		}
	case HarnessCodex:
		if c.SmallModelAlias != "" {
			return &ConfigError{Field: "SmallModelAlias", Reason: "is not supported by Codex"}
		}
	}
	return nil
}

func cleanAbsolutePath(path string) bool {
	return path != "" && filepath.IsAbs(path) && filepath.Clean(path) == path
}
