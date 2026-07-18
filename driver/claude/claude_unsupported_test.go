//go:build !darwin && (!linux || android)

package claude

import (
	"context"
	"errors"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/looprig/foreignloop/driver"
)

func TestUnsupportedPlatformFailsBeforeSpawn(t *testing.T) {
	execPath, err := filepath.Abs("claude")
	if err != nil {
		t.Fatalf("absolute executable path: %v", err)
	}
	agent, err := NewAgent(nil, Config{ExecPath: execPath, Model: "small"})
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
