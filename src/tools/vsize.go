package tools

import (
	"fmt"
	"os/exec"
	"strconv"
	"strings"
	"syscall"
)

// GetVSize returns the virtual memory size of the process in KiB (1024-byte units)
func GetVSize(pid int) (uint64, error) {
	command := exec.Command("ps", "-p", fmt.Sprintf("%d", pid), "-o", "vsize")
	rawOutput, err := command.Output()
	if err != nil {
		return 0, err
	}

	// we expect "   VSZ\n 67232\n"
	lines := strings.Split(string(rawOutput), "\n")
	if len(lines) < 2 {
		return 0, fmt.Errorf("unexpected output %q", lines)
	}

	return strconv.ParseUint(strings.TrimSpace(lines[1]), 10, 64)
}

// GetMyVSize returns GetVSize for the current process
func GetMyVSize() (uint64, error) {
	return GetVSize(syscall.Getpid())
}
