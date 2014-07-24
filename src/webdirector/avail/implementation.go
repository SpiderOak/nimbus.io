package avail

import (
	"fmt"
)

type availability struct {
}

// Availability returns a mock entity that implements the Availability
// interface. .
func NewAvailability() Availability {
	return availability{}
}

func (a availability) AvailableHosts(hostNames []string, destPort string) (
	[]string, error) {

	return nil, fmt.Errorf("AvailableHosts not implemented")
}
