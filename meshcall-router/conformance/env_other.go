//go:build !unix

package conformance

import (
	"os"
	"os/exec"
)

// ownGroup: no process groups here; the fixture alone is signalled.
func ownGroup(cmd *exec.Cmd) {}

func signalGroup(process *os.Process, sig os.Signal) error {
	if sig == os.Interrupt {
		// Not deliverable on Windows; the caller then kills.
		return process.Signal(sig)
	}
	return process.Kill()
}
