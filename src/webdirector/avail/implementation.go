package avail

import (
	"fmt"
)

type availability struct {
	availSet map[string]struct{}
}

// Availability returns a mock entity that implements the Availability
// interface. .
func NewAvailability() Availability {
	var a availability
	a.availSet = map[string]struct{}{
		""
	}
}

func (a availability) AvailableHosts(hostNames []string, destPort string) (
	[]string, error) {

	return nil, fmt.Errorf("AvailableHosts not implemented")
}
