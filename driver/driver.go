// Package driver defines provider-neutral contracts for foreign agents.
//
// A concrete provider constructor returns an Agent. The product composition
// root combines that agent with backend configuration and installs the resulting
// builders through Harness. Per-turn prompts, workspace, permission posture,
// session selection, normalized events, and authoritative history cross this
// package boundary; provider wire formats and transcript paths do not.
package driver

import (
	"context"

	"github.com/looprig/core/content"
)

// Agent starts one turn with a foreign agent.
type Agent interface {
	Spawn(context.Context, Turn) (Stream, error)
}

// Closer is optionally implemented by agents that own long-lived resources
// spanning turns. The backend invokes Close exactly once after the command
// pump exits, whether it exits from command.Shutdown or loop-context
// cancellation. Drivers that spawn a new CLI process per turn do not need to
// implement Closer.
type Closer interface {
	Close() error
}

// Turn is one turn's input to a foreign agent.
type Turn struct {
	SystemPrompt string
	ForeignSID   string
	StartNew     bool
	Input        []content.Block
	Cwd          string
	Posture      PermissionPosture
}

// Stream is the live normalized event stream and its authoritative history.
type Stream interface {
	Events() <-chan Event
	History() (History, error)
	Close() error
}

// Kind identifies a normalized foreign-agent event.
type Kind uint8

const (
	KindInit Kind = iota
	KindTextDelta
	KindThinkingDelta
	KindToolUse
	KindToolResult
	KindStepComplete
	KindTerminalOK
	KindTerminalError
)

// Event is the normalized event union emitted by a Stream.
type Event struct {
	Kind          Kind
	SessionID     string
	Text          string
	ToolUseID     string
	ToolName      string
	IsError       bool
	ResultPreview string
	Message       *content.AIMessage
	ErrText       string
	// ModelFacing marks terminal error text that a driver has already reduced
	// to a bounded, safe detail suitable for model display. Ordinary provider
	// failures must leave this false so backend error handling keeps them
	// non-model-facing.
	ModelFacing bool
}

// PermissionPosture is the typed, non-interactive permission mode passed to an
// agent.
type PermissionPosture uint8

const (
	PostureDefault PermissionPosture = iota
	PostureAcceptEdits
)
