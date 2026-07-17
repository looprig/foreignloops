//go:build darwin || linux

package codex

import (
	"os/exec"
	"syscall"
)

func platformSupported() error { return nil }

func configureProcessGroup(cmd *exec.Cmd) {
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
}

func interruptProcessGroup(pgid int) error { return syscall.Kill(-pgid, syscall.SIGINT) }
func killProcessGroup(pgid int) error      { return syscall.Kill(-pgid, syscall.SIGKILL) }
