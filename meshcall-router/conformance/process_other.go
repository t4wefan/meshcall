//go:build !windows

package conformance

import (
	"errors"
	"os"
	"os/exec"
	"syscall"
)

// isolate starts the fixture in a process group of its own, so killFixture
// reaches the program a launcher such as `uv run` or `npx` started.
func isolate(cmd *exec.Cmd) {
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
}

// killFixture kills the fixture and everything it started.
func killFixture(process *os.Process) error {
	err := syscall.Kill(-process.Pid, syscall.SIGKILL)
	if errors.Is(err, syscall.ESRCH) {
		return os.ErrProcessDone
	}
	return err
}
