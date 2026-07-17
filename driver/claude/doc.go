// Package claude implements the Claude CLI foreign-agent driver.
//
// NewAgent resolves provider configuration and an explicitly supplied parent
// environment into a driver.Agent. Per-turn cwd, posture, prompt, and session
// selection remain owned by driver.Turn. Spawned streams expose normalized live
// events and complete authoritative Claude transcript history without exposing
// transcript paths through the public driver contract.
package claude
