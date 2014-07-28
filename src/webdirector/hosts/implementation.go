package hosts

import (
	"fmt"
)

type hostsForCollectionImple struct {
	collectionMap map[string][]string
}

// NewHostsForCollection returns a mock implementation of the
// HostsForCollection interface
func NewHostsForCollection() HostsForCollection {
	h := hostsForCollectionImple{}
	h.collectionMap = map[string][]string{
		"collection01": []string{"127.0.0.1"}}
	return h
}

func (h hostsForCollectionImple) GetHostNames(collectionName string) ([]string, error) {
	hostNames, ok := h.collectionMap[collectionName]
	if !ok {
		return nil, fmt.Errorf("no hosts for %s", collectionName)
	}

	return hostNames, nil
}
