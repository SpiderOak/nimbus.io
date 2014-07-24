package hosts

import (
	"fmt"
)

type hostsForCollectionImple struct {
}

// NewHostsForCollection returns a mock implementation of the
// HostsForCollection interface
func NewHostsForCollection() HostsForCollection {
	return hostsForCollectionImple{}
}

func (h hostsForCollectionImple) GetHostNames(collectionName string) ([]string, error) {
	return nil, fmt.Errorf("GetHostNames not implemented")
}
