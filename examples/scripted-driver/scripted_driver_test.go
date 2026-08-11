//go:build (darwin || linux) && !android

package scripteddriver_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/looprig/core/content"
	"github.com/looprig/foreignloops/driver"
	"github.com/looprig/foreignloops/driver/codex"
)

// Example demonstrates the provider-neutral stream by putting the production
// Codex driver in front of a deterministic executable. The driver still owns
// argv construction, JSONL decoding, process-group supervision, and cleanup.
func Example() {
	dir, err := os.MkdirTemp("", "foreignloops-scripted-driver-")
	if err != nil {
		panic(err)
	}
	defer os.RemoveAll(dir)

	executable := filepath.Join(dir, "scripted-codex")
	script := `#!/bin/sh
printf '%s\n' '{"type":"thread.started","thread_id":"0199a213-81c0-7800-8aa1-bbab2a035a53"}'
printf '%s\n' '{"type":"item.completed","item":{"type":"agent_message","text":"scripted answer"}}'
printf '%s\n' '{"type":"turn.completed"}'
exit "${EXAMPLE_EXIT_CODE:-0}"
`
	if err := os.WriteFile(executable, []byte(script), 0o700); err != nil {
		panic(err)
	}

	agent, err := codex.NewAgent(nil, codex.Config{
		ExecPath:         executable,
		Sandbox:          codex.SandboxReadOnly,
		Approval:         codex.ApprovalNever,
		IgnoreUserConfig: true,
		IgnoreRules:      true,
		SkipGitRepoCheck: true,
	})
	if err != nil {
		panic(err)
	}
	stream, err := agent.Spawn(context.Background(), driver.Turn{
		SystemPrompt: "Answer from the scripted fixture.",
		StartNew:     true,
		Input:        []content.Block{&content.TextBlock{Text: "hello"}},
		Cwd:          dir,
	})
	if err != nil {
		panic(err)
	}
	for event := range stream.Events() {
		switch event.Kind {
		case driver.KindInit:
			fmt.Println("session:", event.SessionID)
		case driver.KindStepComplete:
			fmt.Println("assistant:", event.Message.Blocks[0].(*content.TextBlock).Text)
		case driver.KindTerminalOK:
			fmt.Println("terminal: ok")
		}
	}
	if err := stream.Close(); err != nil {
		panic(err)
	}
	history, err := stream.History()
	if err != nil {
		panic(err)
	}
	fmt.Println("authoritative history:", history.Available)

	// The same subprocess boundary returns a typed exit status that callers
	// can classify without parsing an error string.
	failingAgent, err := codex.NewAgent(nil, codex.Config{
		ExecPath: executable,
		Credential: map[string]string{
			"EXAMPLE_EXIT_CODE": "7",
		},
	})
	if err != nil {
		panic(err)
	}
	failingStream, err := failingAgent.Spawn(context.Background(), driver.Turn{StartNew: true, Cwd: dir})
	if err != nil {
		panic(err)
	}
	for range failingStream.Events() {
	}
	err = failingStream.Close()
	var exitErr *driver.ExitError
	fmt.Println("typed exit:", errors.As(err, &exitErr), exitErr.Code)

	// Output:
	// session: 0199a213-81c0-7800-8aa1-bbab2a035a53
	// assistant: scripted answer
	// terminal: ok
	// authoritative history: false
	// typed exit: true 7
}
