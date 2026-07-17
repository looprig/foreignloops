// Package driver defines provider-neutral contracts for foreign agents.
package driver

import (
	"context"

	"github.com/looprig/core/content"
)

// Agent starts one turn with a foreign agent.
type Agent interface {
	Spawn(context.Context, Turn) (Stream, error)
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
}

// PermissionPosture is the typed, non-interactive permission mode passed to an
// agent.
type PermissionPosture uint8

const (
	PostureDefault PermissionPosture = iota
	PostureAcceptEdits
)
