package tools

import (
	"os/exec"
	"strings"
)

// CreateUUID returns a strinhg formatted UUID
func CreateUUID() (string, error) {
	command := exec.Command("uuidgen")
	stdout, err := command.Output()
	if err != nil {
		return "", err
	}

	return strings.TrimSpace(string(stdout)), nil
}
