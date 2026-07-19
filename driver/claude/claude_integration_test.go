//go:build integration && (darwin || (linux && !android))

package claude

import (
	"context"
	"crypto/rand"
	"fmt"
	"os"
	"os/exec"
	"testing"
	"time"

	"github.com/looprig/core/content"
	"github.com/looprig/foreignloops/driver"
)

const integrationModel = "haiku"
const integrationTurnTimeout = 120 * time.Second

func TestAgentSpawnIntegration(t *testing.T) {
	execPath, err := exec.LookPath("claude")
	if err != nil {
		t.Skip("claude binary not on PATH; skipping integration test")
	}
	key := os.Getenv("ANTHROPIC_API_KEY")
	if key == "" {
		t.Skip("ANTHROPIC_API_KEY not set; skipping integration test")
	}
	home, err := os.UserHomeDir()
	if err != nil {
		t.Fatalf("UserHomeDir: %v", err)
	}
	agent, err := NewAgent(os.Environ(), Config{
		ExecPath:   execPath,
		Home:       home,
		Model:      integrationModel,
		EnvAllow:   []string{"PATH", "HOME", "TERM", "LANG"},
		Credential: map[string]string{"ANTHROPIC_API_KEY": key},
	})
	if err != nil {
		t.Fatalf("NewAgent: %v", err)
	}
	sid, err := integrationUUID()
	if err != nil {
		t.Fatalf("generate UUID: %v", err)
	}
	cwd := t.TempDir()
	for _, phase := range []struct {
		name     string
		startNew bool
	}{
		{name: "start new session", startNew: true},
		{name: "resume session", startNew: false},
	} {
		t.Run(phase.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), integrationTurnTimeout)
			defer cancel()
			stream, err := agent.Spawn(ctx, driver.Turn{
				SystemPrompt: "You are a terse test agent.",
				ForeignSID:   sid,
				StartNew:     phase.startNew,
				Input:        []content.Block{&content.TextBlock{Text: "say OK"}},
				Cwd:          cwd,
				Posture:      driver.PostureDefault,
			})
			if err != nil {
				t.Fatalf("Spawn: %v", err)
			}
			var sawTerminal, sawError bool
			for event := range stream.Events() {
				switch event.Kind {
				case driver.KindTerminalOK:
					sawTerminal = true
				case driver.KindTerminalError:
					sawTerminal, sawError = true, true
				}
			}
			if err := stream.Close(); err != nil {
				t.Fatalf("Close: %v", err)
			}
			if !sawTerminal || sawError {
				t.Fatalf("terminal state: sawTerminal=%v sawError=%v", sawTerminal, sawError)
			}
			history, err := stream.History()
			if err != nil {
				t.Fatalf("History: %v", err)
			}
			if !history.Available {
				t.Fatal("History.Available = false, want true")
			}
		})
	}
}

func integrationUUID() (string, error) {
	var id [16]byte
	if _, err := rand.Read(id[:]); err != nil {
		return "", err
	}
	id[6] = (id[6] & 0x0f) | 0x40
	id[8] = (id[8] & 0x3f) | 0x80
	return fmt.Sprintf("%08x-%04x-%04x-%04x-%012x",
		id[0:4], id[4:6], id[6:8], id[8:10], id[10:16]), nil
}
