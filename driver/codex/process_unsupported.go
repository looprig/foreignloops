//go:build !darwin && (!linux || android)

package codex

import (
	"os/exec"
	"runtime"
)

func platformSupported() error        { return &PlatformError{GOOS: runtime.GOOS} }
func configureProcessGroup(*exec.Cmd) {}
func interruptProcessGroup(int) error { return &PlatformError{GOOS: runtime.GOOS} }
func killProcessGroup(int) error      { return &PlatformError{GOOS: runtime.GOOS} }
func waitProcessExit(int) error       { return &PlatformError{GOOS: runtime.GOOS} }
