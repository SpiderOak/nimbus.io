package avail

import (
	"fmt"
)

type mockAvailability struct {
	availSet map[string]struct{}
}

// NewMockAvailability returns a mock entity that implements the Availability
// interface. It taks a slice of strings of the form <host:port>.
// The mock object returns as available any host, port combination that
// matches.
func NewMockAvailability(availableHostPorts []string) Availability {
	var m mockAvailability
	m.availSet = make(map[string]struct{})

	for _, availableHostPort := range availableHostPorts {
		m.availSet[availableHostPort] = struct{}{}
	}

	return m
}

func (m mockAvailability) AvailableHosts(hostNames []string, destPort string) (
	[]string, error) {

	var availableHostNames []string

	for _, hostName := range hostNames {
		testName := fmt.Sprintf("%s:%s", hostName, destPort)
		if _, ok := m.availSet[testName]; ok {
			availableHostNames = append(availableHostNames, hostName)
		}
	}

	return availableHostNames, nil
}
