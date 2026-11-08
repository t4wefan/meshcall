//go:build windows

package conformance

import (
	"os"
	"os/exec"
	"strconv"
)

func isolate(*exec.Cmd) {}

// killFixture kills the fixture and everything it started: killing only
// `uv.exe` would leave its python.exe running with the harness's pipes.
func killFixture(process *os.Process) error {
	err := exec.Command("taskkill", "/T", "/F", "/PID", strconv.Itoa(process.Pid)).Run()
	if killErr := process.Kill(); err != nil {
		return killErr
	}
	return nil
}
