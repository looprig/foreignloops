// Package codex implements the Codex CLI foreign-agent driver. NewAgent
// resolves provider configuration and an explicitly supplied parent
// environment into a driver.Agent. Per-turn cwd, prompt, and session selection
// remain owned by driver.Turn. Codex v1 streams expose normalized live events
// and explicitly unavailable authoritative history.
package codex
