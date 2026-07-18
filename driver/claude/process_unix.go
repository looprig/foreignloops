//go:build darwin || (linux && !android)

package claude

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
