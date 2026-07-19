package claude

import (
	"fmt"
	"os/exec"
	"path/filepath"

	"github.com/looprig/foreignloops/driver"
)

// CommandWrapper confines a fully configured child command before pipe
// attachment and process start.
type CommandWrapper func(*exec.Cmd) (*exec.Cmd, error)

// Config is the provider-owned configuration used to construct a Claude agent.
// Turn-owned cwd, posture, and session selection are supplied to Agent.Spawn.
type Config struct {
	ExecPath   string
	Home       string
	Model      string
	EnvAllow   []string
	Credential map[string]string
	Wrap       CommandWrapper
}

// ConfigError reports invalid provider configuration at construction time.
type ConfigError struct{ Field, Reason string }

func (e *ConfigError) Error() string {
	return "claude: config: " + e.Field + ": " + e.Reason
}

// SpawnConfigError reports invalid internal agent state discovered before a
// child is started. NewAgent prevents callers from constructing this state.
type SpawnConfigError struct{ Field, Reason string }

func (e *SpawnConfigError) Error() string {
	return "claude: spawn config: " + e.Field + ": " + e.Reason
}

// PlatformError reports an OS without the required process-group supervision.
type PlatformError struct{ GOOS string }

func (e *PlatformError) Error() string {
	return fmt.Sprintf("claude: process supervision is unsupported on %s", e.GOOS)
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
	if cfg.Model == "" {
		return nil, &ConfigError{Field: "Model", Reason: "required"}
	}
	return &agent{
		execPath: cfg.ExecPath,
		home:     cfg.Home,
		model:    cfg.Model,
		env:      whitelistEnv(parentEnv, cfg.EnvAllow, cfg.Credential),
		wrap:     cfg.Wrap,
	}, nil
}

func cleanAbsoluteExecPath(path string) bool {
	return filepath.IsAbs(path) && filepath.Clean(path) == path
}
