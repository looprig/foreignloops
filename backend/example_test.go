package backend_test

import (
	"fmt"

	"github.com/looprig/foreignloops/backend"
	"github.com/looprig/foreignloops/driver"
	"github.com/looprig/foreignloops/driver/claude"
	"github.com/looprig/harness/pkg/rig"
)

func ExampleBuildWith() {
	workspace := "/srv/agent-workspace"
	// The product provisions this private directory writable by the provider.
	providerHome := "/srv/looprig/provider-home"
	parentEnv := []string{
		"HOME=" + providerHome,
		"PATH=/usr/local/bin:/usr/bin",
	}
	agent, err := claude.NewAgent(parentEnv, claude.Config{
		ExecPath: "/usr/local/bin/claude",
		Home:     providerHome,
		Model:    "claude-sonnet-4-20250514",
		EnvAllow: []string{"HOME", "PATH"},
	})
	if err != nil {
		fmt.Printf("configure Claude agent: %v\n", err)
		return
	}

	cfg := backend.Config{
		Agent:   agent,
		Cwd:     workspace,
		Posture: driver.PostureAcceptEdits,
		SIDMode: backend.SIDPrebound,
	}
	option := rig.WithForeignBuilders(
		backend.BuildWith(cfg),
		backend.BuildRestoredWith(cfg),
	)
	_ = option // Apply this option at the product's Harness composition root.
}
