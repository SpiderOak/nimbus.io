package tools

import (
	"fmt"
	"io/ioutil"
	"strconv"
	"strings"
)

// FreeMemory returns the amount of free memory in the system
func FreeMemory() (uint64, error) {
	rawContent, err := ioutil.ReadFile("/proc/meminfo")
	if err != nil {
		return 0, err
	}

	content := strings.Split(string(rawContent), "\n")

	for _, line := range content {
		fields := strings.Fields(line)
		if len(fields) == 0 {
			continue
		}
		if fields[0] == "MemFree:" {
			return strconv.ParseUint(fields[1], 10, 64)
		}
	}

	return 0, fmt.Errorf("Unable to find MemFree:")
}
