package mgmtapi

import (
	"fmt"
	"os"
	"strings"

	"fog"
)

type managementAPIDestinations struct {
	destHosts []string
	index     int
}

// NewManagementAPIDestinations returns an entity that implements the
// ManagementAPIDestinations interface
func NewManagementAPIDestinations() (ManagementAPIDestinations, error) {
	var d managementAPIDestinations

	destString := os.Getenv("NIMBUSIO_MANAGEMENT_API_REQUEST_DEST")
	if destString == "" {
		return nil, fmt.Errorf("No value for NIMBUSIO_MANAGEMENT_API_REQUEST_DEST")
	}

	d.destHosts = strings.Split(destString, " ")
	if len(d.destHosts) == 0 {
		return nil, fmt.Errorf("too few NIMBUSIO_MANAGEMENT_API_REQUEST_DEST")
	}

	fog.Info("NIMBUSIO_MANAGEMENT_API_REQUEST_DEST = '%s'", destString)

	return &d, nil
}

// Next returns the next destination to be used and advances
func (d *managementAPIDestinations) Next() string {
	next := d.destHosts[d.index]
	d.index = (d.index + 1) % len(d.destHosts)
	return next
}

// Peek returns the next destination that will be used
// This is intended for convenience in testing
func (d managementAPIDestinations) Peek() string {
	return d.destHosts[d.index]
}
