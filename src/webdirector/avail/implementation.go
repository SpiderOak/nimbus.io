package avail

import (
	"fmt"
	"os"
)

type availability struct {
	availSet map[string]struct{}
}

// Availability returns a mock entity that implements the Availability
// interface. .
func NewAvailability() Availability {
	var a availability
	reader := fmt.Sprintf("127.0.0.1:%s", os.Getenv("NIMBUSIO_WEB_PUBLIC_READER_PORT"))
	writer := fmt.Sprintf("127.0.0.1:%s", os.Getenv("NIMBUSIO_WEB_WRITER_PORT"))
	a.availSet = map[string]struct{}{
		reader: struct{}{},
		writer: struct{}{}}

	return a
}

func (a availability) AvailableHosts(hostNames []string, destPort string) (
	[]string, error) {

	var availableHostNames []string

	for _, hostName := range hostNames {
		testName := fmt.Sprintf("%s:%s", hostName, destPort)
		if _, ok := a.availSet[testName]; ok {
			availableHostNames = append(availableHostNames, hostName)
		}
	}

	return availableHostNames, nil
}
