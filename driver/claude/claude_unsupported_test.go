//go:build !darwin && !linux

package claude

import (
	"context"
	"errors"
	"runtime"
	"testing"

	"github.com/looprig/foreignloop/driver"
)

func TestUnsupportedPlatformFailsBeforeSpawn(t *testing.T) {
	agent, err := NewAgent(nil, Config{ExecPath: "claude", Model: "small"})
	if err != nil {
		t.Fatalf("NewAgent() error = %v", err)
	}
	stream, err := agent.Spawn(context.Background(), driver.Turn{})
	if stream != nil {
		_ = stream.Close()
		t.Fatalf("Spawn() stream = %T, want nil", stream)
	}
	var platformErr *PlatformError
	if !errors.As(err, &platformErr) || platformErr.GOOS != runtime.GOOS {
		t.Fatalf("Spawn() error = %T %v, want *PlatformError for %s", err, err, runtime.GOOS)
	}
}
