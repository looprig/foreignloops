//go:build !darwin && !linux

package claude

import (
	"os/exec"
	"runtime"
)

func platformSupported() error        { return &PlatformError{GOOS: runtime.GOOS} }
func configureProcessGroup(*exec.Cmd) {}
func interruptProcessGroup(int) error { return &PlatformError{GOOS: runtime.GOOS} }
func killProcessGroup(int) error      { return &PlatformError{GOOS: runtime.GOOS} }
func processGroupMissing(error) bool  { return false }
