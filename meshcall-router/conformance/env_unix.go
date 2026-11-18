//go:build unix

package conformance

import (
	"os"
	"os/exec"
	"syscall"
)

// ownGroup starts cmd in a process group of its own, so the fixture's
// children (the program a launcher such as uv or npx runs) get its signals.
func ownGroup(cmd *exec.Cmd) {
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	cmd.Cancel = func() error { return signalGroup(cmd.Process, os.Kill) }
}

// signalGroup signals the fixture's whole process group.
func signalGroup(process *os.Process, sig os.Signal) error {
	s, ok := sig.(syscall.Signal)
	if !ok {
		return process.Signal(sig)
	}
	return syscall.Kill(-process.Pid, s)
}
